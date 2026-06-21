"""Configurable paper/live trading service.

The live-trading surface is intentionally profile-driven: a strategy must be
whitelisted in ``live_strategy_profiles`` before it can produce executable
signals.  The first adapter supports the current TSMF CashAware strategy assets
stored in the ``strategies`` table, but the API is shaped so future adapters can
be registered without changing the frontend contract.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Sequence

import pandas as pd
from loguru import logger
from sqlalchemy import select, update

from app.core.config import settings
from app.db.models.live_trading import (
    LiveOrderAudit,
    LivePaperAccount,
    LiveStrategyProfile,
    LiveTradingRun,
)
from app.db.models.strategy import Strategy
from app.db.sqlite import async_session_factory
from app.engines.qmt_gateway import qmt_gateway
from app.services.factor_pipeline import FactorPipeline, LinearFactorScorer
from app.services.index_components import load_index_symbols
from app.services.qmt_trading import QmtAccountSnapshot, qmt_trading_service
from app.services.us_market import apply_entry_filter_to_target_weights, us_overnight_entry_filter_state


DEFAULT_ADAPTER = "multi_factor_cash_aware"
PAPER_ACCOUNT_KEY = "default"


@dataclass
class LiveAccountSnapshot:
    cash: float
    total_asset: float
    market_value: float
    positions: dict[str, dict[str, Any]]
    source: str
    error: str | None = None

    @classmethod
    def from_qmt(cls, snapshot: QmtAccountSnapshot) -> "LiveAccountSnapshot":
        return cls(
            cash=snapshot.cash,
            total_asset=snapshot.total_asset,
            market_value=snapshot.market_value,
            positions={symbol: position.as_dict() for symbol, position in snapshot.positions.items()},
            source=snapshot.source,
            error=snapshot.error,
        )


@dataclass
class StrategyProfileBundle:
    profile: LiveStrategyProfile
    strategy: Strategy
    constants: dict[str, Any]
    params: dict[str, Any]


class LiveTradingService:
    """Profile-driven paper/live trading orchestration."""

    def __init__(self) -> None:
        self._runner_task: asyncio.Task[None] | None = None
        self._runner_stop: asyncio.Event | None = None
        self._runner_status: dict[str, Any] = {
            "status": "stopped",
            "mode": None,
            "profile_key": None,
            "run_id": None,
            "last_cycle_at": None,
            "last_signal_hash": None,
            "last_error": None,
            "takeover": False,
        }

    async def ensure_default_profiles(self) -> None:
        """Seed configured strategy IDs as live-trading profiles when present."""
        strategy_ids = self._seed_strategy_ids()
        if not strategy_ids:
            return
        async with async_session_factory() as session:
            existing_rows = await session.execute(select(LiveStrategyProfile))
            existing = {row.profile_key: row for row in existing_rows.scalars().all()}
            default_key = settings.live_trading_default_profile
            for strategy_id in strategy_ids:
                strategy = await session.get(Strategy, strategy_id)
                if strategy is None:
                    continue
                profile_key = self._default_profile_key(strategy_id)
                if profile_key in existing:
                    continue
                profile = LiveStrategyProfile(
                    strategy_id=strategy_id,
                    profile_key=profile_key,
                    display_name=strategy.name,
                    description=strategy.description,
                    enabled=True,
                    is_default=profile_key == default_key,
                    adapter_type=DEFAULT_ADAPTER,
                    params_override={},
                    universe_config={"type": "strategy"},
                    execution_policy={
                        "allow_auto_trade": True,
                        "allow_manual_submit": True,
                        "allow_live_submit": True,
                    },
                )
                session.add(profile)
            await session.commit()

    async def status(self) -> dict[str, Any]:
        await self.ensure_default_profiles()
        qmt = await qmt_trading_service.status()
        profiles = await self.list_profiles(include_disabled=False)
        return {
            **qmt,
            "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
            "auto_execute_enabled": bool(settings.live_trading_auto_execute_enabled),
            "default_profile": settings.live_trading_default_profile,
            "profile_count": len(profiles),
            "runner": dict(self._runner_status),
        }

    async def list_profiles(self, *, include_disabled: bool = True) -> list[dict[str, Any]]:
        await self.ensure_default_profiles()
        async with async_session_factory() as session:
            stmt = select(LiveStrategyProfile, Strategy).join(Strategy, LiveStrategyProfile.strategy_id == Strategy.id)
            if not include_disabled:
                stmt = stmt.where(LiveStrategyProfile.enabled.is_(True))
            stmt = stmt.order_by(LiveStrategyProfile.is_default.desc(), LiveStrategyProfile.id.asc())
            rows = (await session.execute(stmt)).all()
        return [self._profile_dict(profile, strategy) for profile, strategy in rows]

    async def create_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        strategy_id = int(payload.get("strategy_id") or 0)
        profile_key = str(payload.get("profile_key") or "").strip()
        if strategy_id <= 0:
            raise ValueError("strategy_id is required")
        if not profile_key:
            raise ValueError("profile_key is required")
        async with async_session_factory() as session:
            strategy = await session.get(Strategy, strategy_id)
            if strategy is None:
                raise ValueError(f"strategy_id {strategy_id} does not exist")
            exists = await session.scalar(select(LiveStrategyProfile).where(LiveStrategyProfile.profile_key == profile_key))
            if exists is not None:
                raise ValueError(f"profile_key {profile_key} already exists")
            is_default = bool(payload.get("is_default", False))
            if is_default:
                await session.execute(update(LiveStrategyProfile).values(is_default=False))
            profile = LiveStrategyProfile(
                strategy_id=strategy_id,
                profile_key=profile_key,
                display_name=str(payload.get("display_name") or strategy.name),
                description=payload.get("description") or strategy.description,
                enabled=bool(payload.get("enabled", True)),
                is_default=is_default,
                adapter_type=str(payload.get("adapter_type") or DEFAULT_ADAPTER),
                params_override=self._json_dict(payload.get("params_override")),
                universe_config=self._json_dict(payload.get("universe_config")) or {"type": "strategy"},
                execution_policy=self._json_dict(payload.get("execution_policy")),
            )
            session.add(profile)
            await session.commit()
            await session.refresh(profile)
            return self._profile_dict(profile, strategy)

    async def update_profile(self, profile_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        async with async_session_factory() as session:
            row = await session.execute(
                select(LiveStrategyProfile, Strategy)
                .join(Strategy, LiveStrategyProfile.strategy_id == Strategy.id)
                .where(LiveStrategyProfile.profile_key == profile_key)
            )
            pair = row.first()
            if pair is None:
                raise ValueError(f"profile_key {profile_key} does not exist")
            profile, strategy = pair
            if "strategy_id" in payload:
                strategy_id = int(payload["strategy_id"])
                next_strategy = await session.get(Strategy, strategy_id)
                if next_strategy is None:
                    raise ValueError(f"strategy_id {strategy_id} does not exist")
                profile.strategy_id = strategy_id
                strategy = next_strategy
            for field in ("display_name", "description", "adapter_type"):
                if field in payload:
                    setattr(profile, field, str(payload[field]) if payload[field] is not None else None)
            for field in ("enabled", "is_default"):
                if field in payload:
                    setattr(profile, field, bool(payload[field]))
            for field in ("params_override", "universe_config", "execution_policy"):
                if field in payload:
                    setattr(profile, field, self._json_dict(payload[field]))
            if profile.is_default:
                await session.execute(
                    update(LiveStrategyProfile)
                    .where(LiveStrategyProfile.profile_key != profile.profile_key)
                    .values(is_default=False)
                )
            await session.commit()
            await session.refresh(profile)
            return self._profile_dict(profile, strategy)

    async def signals(
        self,
        *,
        profile_key: str | None = None,
        mode: str = "paper",
        params: dict[str, Any] | None = None,
        manual_account: dict[str, Any] | None = None,
        trigger_source: str = "manual",
        run_id: str | None = None,
        write_audit: bool = True,
    ) -> dict[str, Any]:
        bundle = await self._load_profile_bundle(profile_key)
        normalized = {**bundle.params, **self._json_dict(params)}
        trade_date = self._parse_date(normalized.get("trade_date")) or date.today()
        if not bundle.profile.enabled:
            if write_audit:
                await self._write_control_audit(
                    profile_key=bundle.profile.profile_key,
                    strategy_id=bundle.profile.strategy_id,
                    trade_date=trade_date,
                    signal_hash=None,
                    trigger_source=trigger_source,
                    mode=mode,
                    run_id=run_id,
                    status="blocked",
                    reason=f"profile {bundle.profile.profile_key} is disabled",
                    payload={"stage": "signals", "profile_key": bundle.profile.profile_key},
                )
            raise ValueError(f"profile {bundle.profile.profile_key} is disabled")
        if bundle.profile.adapter_type != DEFAULT_ADAPTER:
            if write_audit:
                await self._write_control_audit(
                    profile_key=bundle.profile.profile_key,
                    strategy_id=bundle.profile.strategy_id,
                    trade_date=trade_date,
                    signal_hash=None,
                    trigger_source=trigger_source,
                    mode=mode,
                    run_id=run_id,
                    status="blocked",
                    reason=f"Unsupported adapter_type: {bundle.profile.adapter_type}",
                    payload={"stage": "signals", "adapter_type": bundle.profile.adapter_type},
                )
            raise ValueError(f"Unsupported adapter_type: {bundle.profile.adapter_type}")
        account = await self._account_snapshot(mode=mode, manual=manual_account, params=normalized)
        positions = {
            symbol: float(position.get("quantity", 0.0) or 0.0)
            for symbol, position in account.positions.items()
        }
        symbols = await self._resolve_symbols(bundle.profile, normalized, trade_date)
        if not symbols:
            return await self._empty_signal_response(
                bundle=bundle,
                params=normalized,
                trade_date=trade_date,
                account=account,
                reason="未解析到股票池",
                mode=mode,
                trigger_source=trigger_source,
                run_id=run_id,
                write_audit=write_audit,
            )

        factor_configs = list(normalized.get("factor_configs") or bundle.constants.get("FACTOR_CONFIGS") or [])
        filters = list(normalized.get("filter_factors") or bundle.constants.get("FILTER_FACTORS") or [])
        if not factor_configs:
            raise ValueError("strategy profile has no FACTOR_CONFIGS")

        pipeline = FactorPipeline()
        result = await asyncio.to_thread(
            pipeline.build_cross_section,
            factor_specs=factor_configs,
            trade_date=trade_date,
            symbols=symbols,
            filters=filters,
            min_factor_coverage=float(normalized.get("min_factor_coverage", 0.4) or 0.4),
            scorer=LinearFactorScorer(),
        )
        frame = self._apply_theme_filter(result.frame, normalized)
        frame, heat_note = self._apply_limit_up_heat_filter(frame, normalized)
        if frame.empty:
            return await self._empty_signal_response(
                bundle=bundle,
                params=normalized,
                trade_date=trade_date,
                account=account,
                reason="因子截面为空或过滤后无候选",
                mode=mode,
                trigger_source=trigger_source,
                run_id=run_id,
                write_audit=write_audit,
            )

        target_symbols = self._rank_targets(frame, positions, normalized)
        quote_symbols = sorted(set(target_symbols) | set(positions))
        price_map, quote_error = await self._quote_prices(quote_symbols)
        price_map.update(self._position_price_fallbacks(account))

        target_weight = self._target_weight(target_symbols, normalized)
        target_weights = {symbol: target_weight for symbol in target_symbols if target_weight > 0}
        portfolio_value = account.total_asset or account.cash or float(normalized.get("initial_capital", 1_000_000) or 1_000_000)
        entry_state = self._entry_filter_state(trade_date, normalized)
        filtered_weights, entry_state = apply_entry_filter_to_target_weights(
            target_weights,
            current_positions=positions,
            price_map=price_map,
            portfolio_value=portfolio_value,
            entry_filter_state=entry_state,
        )
        orders, skipped_orders = self._build_cash_aware_orders(
            target_weights=filtered_weights,
            positions=positions,
            price_map=price_map,
            account=account,
            params=normalized,
            portfolio_value=portfolio_value,
            bundle=bundle,
        )
        signal_hash = self._signal_hash(bundle.profile.profile_key, bundle.profile.strategy_id, trade_date, orders)
        for order in orders:
            order["signal_hash"] = signal_hash
        for skipped in skipped_orders:
            skipped["signal_hash"] = signal_hash

        if write_audit:
            await self._write_order_audits(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=trade_date,
                signal_hash=signal_hash,
                trigger_source=trigger_source,
                mode=mode,
                run_id=run_id,
                orders=orders,
                skipped_orders=skipped_orders,
            )

        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "profile": self._profile_dict(bundle.profile, bundle.strategy),
            "mode": mode,
            "strategy_id": bundle.profile.strategy_id,
            "strategy_name": bundle.strategy.name,
            "trade_date": trade_date.isoformat(),
            "account": account.__dict__,
            "universe_size": len(symbols),
            "candidate_count": int(len(frame)),
            "excluded_symbol_count": int(len(result.excluded_symbols)),
            "target_symbols": target_symbols,
            "target_weights": filtered_weights,
            "entry_filter": entry_state,
            "quote_error": quote_error,
            "heat_filter_note": heat_note,
            "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
            "auto_execute_enabled": bool(settings.live_trading_auto_execute_enabled),
            "signal_hash": signal_hash,
            "orders": orders,
            "skipped_orders": skipped_orders,
            "top_candidates": self._top_candidates(frame, limit=30),
        }

    async def submit_orders(
        self,
        orders: Sequence[dict[str, Any]],
        *,
        mode: str = "paper",
        confirm: bool = False,
        trigger_source: str = "manual",
        run_id: str | None = None,
    ) -> dict[str, Any]:
        order_list = [dict(order) for order in orders]
        if not order_list:
            return {"submitted": False, "message": "没有可提交的订单", "results": []}
        if mode not in {"paper", "live"}:
            raise ValueError("mode must be paper or live")

        profile_keys = {
            str(order.get("profile_key") or "").strip()
            for order in order_list
            if str(order.get("profile_key") or "").strip()
        }
        if len(profile_keys) > 1:
            raise ValueError("orders must belong to one profile_key")
        profile_key = next(iter(profile_keys), "")
        bundle = await self._load_profile_bundle(profile_key or None)
        strategy_ids = {
            int(order.get("strategy_id") or 0)
            for order in order_list
            if int(order.get("strategy_id") or 0) > 0
        }
        if any(strategy_id != bundle.profile.strategy_id for strategy_id in strategy_ids):
            raise ValueError("order strategy_id does not match profile strategy_id")

        signal_hashes = {
            str(order.get("signal_hash") or "").strip()
            for order in order_list
            if str(order.get("signal_hash") or "").strip()
        }
        if len(signal_hashes) > 1:
            raise ValueError("orders must share one signal_hash")
        signal_hash = next(iter(signal_hashes), "")
        trade_date = self._parse_date(order_list[0].get("trade_date")) or date.today()
        if not bundle.profile.enabled:
            await self._write_control_audit(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=trade_date,
                signal_hash=signal_hash or None,
                trigger_source=trigger_source,
                mode=mode,
                run_id=run_id,
                status="blocked",
                reason=f"profile {bundle.profile.profile_key} is disabled",
                payload={"stage": "submit", "orders": order_list},
            )
            raise ValueError(f"profile {bundle.profile.profile_key} is disabled")
        for order in order_list:
            order["profile_key"] = bundle.profile.profile_key
            order["strategy_id"] = bundle.profile.strategy_id
            order.setdefault("strategy_name", bundle.strategy.name)
            order.setdefault("trade_date", trade_date.isoformat())
            if signal_hash:
                order["signal_hash"] = signal_hash

        async def blocked(message: str, *, enabled: bool = True, status: str = "blocked") -> dict[str, Any]:
            await self._write_control_audit(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=trade_date,
                signal_hash=signal_hash or None,
                trigger_source=trigger_source,
                mode=mode,
                run_id=run_id,
                status=status,
                reason=message,
                payload={"stage": "submit", "orders": order_list},
            )
            return {
                "enabled": enabled,
                "submitted": False,
                "message": message,
                "orders": order_list,
            }

        policy = self._json_dict(bundle.profile.execution_policy)
        if trigger_source == "auto" and not bool(policy.get("allow_auto_trade", True)):
            raise ValueError(f"profile {bundle.profile.profile_key} does not allow auto trade")
        if trigger_source == "manual" and not bool(policy.get("allow_manual_submit", True)):
            raise ValueError(f"profile {bundle.profile.profile_key} does not allow manual submit")
        if mode == "live" and not bool(policy.get("allow_live_submit", True)):
            raise ValueError(f"profile {bundle.profile.profile_key} does not allow live submit")

        if trigger_source == "auto" and signal_hash and await self._has_submitted_signal(signal_hash):
            message = f"signal_hash {signal_hash} 已提交过，跳过重复自动下单。"
            await blocked(message, status="duplicate")
            return {
                "submitted": False,
                "duplicate": True,
                "message": message,
                "results": [],
            }

        if mode == "paper":
            return await self._submit_paper_orders(order_list, trigger_source=trigger_source, run_id=run_id)
        if not settings.live_trading_enable_order_submit:
            return await blocked("LIVE_TRADING_ENABLE_ORDER_SUBMIT=false，当前仅生成信号。", enabled=False)
        if trigger_source == "auto" and self._runner_status.get("takeover"):
            return await blocked("人工接管状态下禁止自动实盘提交。")
        if trigger_source == "auto" and not settings.live_trading_auto_execute_enabled:
            return await blocked("LIVE_TRADING_AUTO_EXECUTE_ENABLED=false，自动实盘提交被阻止。")
        if not confirm:
            return {
                "enabled": True,
                "submitted": False,
                "message": "真实委托需要 confirm=true。",
                "orders": order_list,
            }
        qmt_status = await qmt_trading_service.status()
        if not qmt_status.get("account_configured"):
            return await blocked("QMT account is not configured.")
        if not qmt_status.get("xttrader_available"):
            return await blocked("xtquant.xttrader is unavailable.")
        if not qmt_status.get("quote_connected"):
            return await blocked("QMT quote connection is not ready.")

        results = []
        for order in order_list:
            if str(order.get("side") or "").upper() not in {"BUY", "SELL"}:
                continue
            result = await qmt_trading_service.submit_order({**order, "confirm": True})
            results.append(result)
            await self._write_submit_audit(order, result, mode=mode, trigger_source=trigger_source, run_id=run_id)
        return {
            "enabled": True,
            "submitted": all(bool(item.get("submitted")) for item in results) if results else False,
            "results": results,
        }

    async def list_audits(self, *, limit: int = 100, profile_key: str | None = None) -> list[dict[str, Any]]:
        async with async_session_factory() as session:
            stmt = select(LiveOrderAudit)
            if profile_key:
                stmt = stmt.where(LiveOrderAudit.profile_key == profile_key)
            stmt = stmt.order_by(LiveOrderAudit.created_at.desc()).limit(max(1, min(500, int(limit or 100))))
            rows = (await session.execute(stmt)).scalars().all()
        return [self._audit_dict(row) for row in rows]

    async def start_runner(
        self,
        *,
        profile_key: str | None,
        mode: str,
        params: dict[str, Any] | None,
        interval_seconds: int = 60,
    ) -> dict[str, Any]:
        if mode not in {"paper", "live"}:
            raise ValueError("mode must be paper or live")
        bundle = await self._load_profile_bundle(profile_key)
        if not bundle.profile.enabled:
            await self._write_control_audit(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=date.today(),
                signal_hash=None,
                trigger_source="manual",
                mode=mode,
                run_id=None,
                status="blocked",
                reason=f"profile {bundle.profile.profile_key} is disabled",
                payload={"stage": "runner", "interval_seconds": interval_seconds},
            )
            raise ValueError(f"profile {bundle.profile.profile_key} is disabled")
        if mode == "live" and not settings.live_trading_enable_order_submit:
            raise ValueError("LIVE_TRADING_ENABLE_ORDER_SUBMIT=false，不能启动实盘自动交易")
        if mode == "live" and not settings.live_trading_auto_execute_enabled:
            raise ValueError("LIVE_TRADING_AUTO_EXECUTE_ENABLED=false，不能启动实盘自动交易")
        if mode == "live":
            qmt_status = await qmt_trading_service.status()
            if not qmt_status.get("account_configured"):
                raise ValueError("QMT account is not configured")
            if not qmt_status.get("xttrader_available"):
                raise ValueError("xtquant.xttrader is unavailable")
            if not qmt_status.get("quote_connected"):
                raise ValueError("QMT quote connection is not ready")
        await self.stop_runner(reason="restart")
        run_id = f"live-{uuid.uuid4().hex[:16]}"
        self._runner_stop = asyncio.Event()
        self._runner_status = {
            "status": "running",
            "mode": mode,
            "profile_key": bundle.profile.profile_key,
            "run_id": run_id,
            "last_cycle_at": None,
            "last_signal_hash": None,
            "last_error": None,
            "takeover": False,
        }
        await self._persist_run(
            run_id=run_id,
            profile_key=bundle.profile.profile_key,
            strategy_id=bundle.profile.strategy_id,
            mode=mode,
            status="running",
            params=self._json_dict(params),
        )
        self._runner_task = asyncio.create_task(
            self._runner_loop(
                run_id=run_id,
                profile_key=bundle.profile.profile_key,
                mode=mode,
                params=self._json_dict(params),
                interval_seconds=max(10, int(interval_seconds or 60)),
            )
        )
        return dict(self._runner_status)

    async def stop_runner(self, *, reason: str = "manual_stop") -> dict[str, Any]:
        if self._runner_stop is not None:
            self._runner_stop.set()
        if self._runner_task is not None and not self._runner_task.done():
            self._runner_task.cancel()
            try:
                await self._runner_task
            except asyncio.CancelledError:
                pass
        run_id = self._runner_status.get("run_id")
        self._runner_task = None
        self._runner_stop = None
        self._runner_status = {
            **self._runner_status,
            "status": "stopped",
            "last_error": None if reason == "manual_stop" else reason,
            "takeover": False,
        }
        if run_id:
            await self._update_run_status(str(run_id), "stopped", last_error=None if reason == "manual_stop" else reason)
        return dict(self._runner_status)

    async def takeover(self, *, reason: str = "human takeover") -> dict[str, Any]:
        previous = dict(self._runner_status)
        await self.stop_runner(reason=reason)
        profile_key = str(previous.get("profile_key") or "")
        if profile_key:
            try:
                bundle = await self._load_profile_bundle(profile_key)
                await self._write_control_audit(
                    profile_key=bundle.profile.profile_key,
                    strategy_id=bundle.profile.strategy_id,
                    trade_date=date.today(),
                    signal_hash=str(previous.get("last_signal_hash") or "") or None,
                    trigger_source="manual",
                    mode=str(previous.get("mode") or "paper"),
                    run_id=str(previous.get("run_id") or "") or None,
                    status="takeover",
                    reason=reason,
                    payload={"stage": "runner", "takeover": True},
                )
            except Exception:
                logger.opt(exception=True).warning("Failed to write takeover audit for profile_key={}", profile_key)
        self._runner_status = {
            **previous,
            "status": "paused_by_human",
            "takeover": True,
            "last_error": reason,
        }
        run_id = previous.get("run_id")
        if run_id:
            await self._update_run_status(str(run_id), "paused_by_human", last_error=reason, takeover_reason=reason)
        return dict(self._runner_status)

    async def _runner_loop(
        self,
        *,
        run_id: str,
        profile_key: str,
        mode: str,
        params: dict[str, Any],
        interval_seconds: int,
    ) -> None:
        assert self._runner_stop is not None
        while not self._runner_stop.is_set():
            try:
                if self._is_trading_window() or bool(params.get("ignore_trading_window")):
                    signal = await self.signals(
                        profile_key=profile_key,
                        mode=mode,
                        params=params,
                        trigger_source="auto",
                        run_id=run_id,
                    )
                    signal_hash = str(signal.get("signal_hash") or "")
                    orders = list(signal.get("orders") or [])
                    if orders:
                        await self.submit_orders(
                            orders,
                            mode=mode,
                            confirm=True,
                            trigger_source="auto",
                            run_id=run_id,
                        )
                    self._runner_status.update(
                        {
                            "last_cycle_at": datetime.now().isoformat(timespec="seconds"),
                            "last_signal_hash": signal_hash,
                            "last_error": None,
                        }
                    )
                    await self._update_run_status(
                        run_id,
                        "running",
                        last_signal_hash=signal_hash,
                        last_cycle_at=datetime.now(),
                        last_error=None,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                logger.opt(exception=True).error("Live trading runner cycle failed: {}", message)
                self._runner_status.update({"last_error": message})
                await self._update_run_status(run_id, "error", last_error=message)
            try:
                await asyncio.wait_for(self._runner_stop.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def _load_profile_bundle(self, profile_key: str | None) -> StrategyProfileBundle:
        await self.ensure_default_profiles()
        async with async_session_factory() as session:
            if profile_key:
                stmt = (
                    select(LiveStrategyProfile, Strategy)
                    .join(Strategy, LiveStrategyProfile.strategy_id == Strategy.id)
                    .where(LiveStrategyProfile.profile_key == profile_key)
                )
            else:
                stmt = (
                    select(LiveStrategyProfile, Strategy)
                    .join(Strategy, LiveStrategyProfile.strategy_id == Strategy.id)
                    .where(LiveStrategyProfile.enabled.is_(True))
                    .order_by(LiveStrategyProfile.is_default.desc(), LiveStrategyProfile.id.asc())
                )
            pair = (await session.execute(stmt)).first()
            if pair is None:
                raise ValueError("No enabled live strategy profile is configured")
            profile, strategy = pair
        constants = self._parse_strategy_constants(strategy.code or "")
        params = self._strategy_params(constants, strategy.parameters, profile.params_override)
        return StrategyProfileBundle(profile=profile, strategy=strategy, constants=constants, params=params)

    def _strategy_params(
        self,
        constants: dict[str, Any],
        strategy_params: Any,
        override: Any,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        for key, value in constants.items():
            params[key.lower()] = value
        if "FACTOR_CONFIGS" in constants:
            params["factor_configs"] = constants["FACTOR_CONFIGS"]
        if "FILTER_FACTORS" in constants:
            params["filter_factors"] = constants["FILTER_FACTORS"]
        params.update(self._json_dict(strategy_params))
        params.update(self._json_dict(override))
        return params

    def _parse_strategy_constants(self, code: str) -> dict[str, Any]:
        try:
            tree = ast.parse(code)
        except SyntaxError as exc:
            raise ValueError(f"strategy code cannot be parsed: {exc}") from exc
        constants: dict[str, Any] = {}
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            targets = [target.id for target in node.targets if isinstance(target, ast.Name)]
            if not targets:
                continue
            value = self._literal_node_value(node.value, constants)
            if value is None:
                continue
            for target in targets:
                constants[target] = value
        return constants

    def _literal_node_value(self, node: ast.AST, constants: dict[str, Any]) -> Any:
        if isinstance(node, ast.Name) and node.id in constants:
            return constants[node.id]
        try:
            return ast.literal_eval(node)
        except Exception:
            return None

    async def _resolve_symbols(self, profile: LiveStrategyProfile, params: dict[str, Any], trade_date: date) -> list[str]:
        raw_symbols = params.get("symbols")
        if isinstance(raw_symbols, list) and raw_symbols:
            return [str(symbol) for symbol in raw_symbols if str(symbol).strip()]
        universe = self._json_dict(profile.universe_config)
        if universe.get("type") == "symbols":
            return [str(symbol) for symbol in universe.get("symbols") or [] if str(symbol).strip()]
        index_symbol = (
            universe.get("index_symbol")
            or params.get("index_symbol")
            or ((params.get("backtest_settings") or {}).get("poolSource") or {}).get("indexSymbol")
            or "399101.SZ"
        )
        return await load_index_symbols(str(index_symbol), trade_date, trade_date)

    async def _account_snapshot(
        self,
        *,
        mode: str,
        manual: dict[str, Any] | None,
        params: dict[str, Any],
    ) -> LiveAccountSnapshot:
        if manual:
            positions = self._json_dict(manual.get("positions"))
            return LiveAccountSnapshot(
                cash=float(manual.get("cash", 0.0) or 0.0),
                total_asset=float(manual.get("total_asset", 0.0) or 0.0),
                market_value=float(manual.get("market_value", 0.0) or 0.0),
                positions={str(key): dict(value or {}) for key, value in positions.items()},
                source="manual",
            )
        if mode == "paper":
            return await self._paper_account_snapshot(params)
        try:
            return LiveAccountSnapshot.from_qmt(await qmt_trading_service.account_snapshot())
        except Exception as exc:
            return LiveAccountSnapshot(cash=0.0, total_asset=0.0, market_value=0.0, positions={}, source="qmt", error=f"{type(exc).__name__}: {exc}")

    async def _paper_account_snapshot(self, params: dict[str, Any]) -> LiveAccountSnapshot:
        async with async_session_factory() as session:
            account = await session.scalar(select(LivePaperAccount).where(LivePaperAccount.account_key == PAPER_ACCOUNT_KEY))
            if account is None:
                initial = Decimal(str(params.get("initial_capital") or (params.get("backtest_settings") or {}).get("capital") or 1_000_000))
                account = LivePaperAccount(
                    account_key=PAPER_ACCOUNT_KEY,
                    cash=initial,
                    total_asset=initial,
                    market_value=Decimal("0"),
                    positions={},
                )
                session.add(account)
                await session.commit()
                await session.refresh(account)
            positions = self._json_dict(account.positions)
            return LiveAccountSnapshot(
                cash=float(account.cash or 0),
                total_asset=float(account.total_asset or account.cash or 0),
                market_value=float(account.market_value or 0),
                positions={str(key): dict(value or {}) for key, value in positions.items()},
                source="paper",
            )

    async def _quote_prices(self, symbols: Sequence[str]) -> tuple[dict[str, float], str | None]:
        symbol_list = [str(symbol) for symbol in symbols if str(symbol).strip()]
        if not symbol_list:
            return {}, None
        try:
            quotes = await qmt_gateway.get_realtime_quotes(symbol_list)
        except Exception as exc:
            return {}, f"{type(exc).__name__}: {exc}"
        prices: dict[str, float] = {}
        for quote in quotes:
            symbol = str(quote.get("symbol") or quote.get("code") or quote.get("stock_code") or "")
            price = self._quote_price(quote)
            if symbol and price > 0:
                prices[symbol] = price
        return prices, None

    def _build_cash_aware_orders(
        self,
        *,
        target_weights: dict[str, float],
        positions: dict[str, float],
        price_map: dict[str, float],
        account: LiveAccountSnapshot,
        params: dict[str, Any],
        portfolio_value: float,
        bundle: StrategyProfileBundle,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        lot_size = int(params.get("lot_size", 100) or 100)
        tolerance = float(params.get("rebalance_tolerance_pct", 0.01) or 0.01)
        execution_reserve = float(params.get("cash_execution_reserve_pct", params.get("cash_buffer_pct", 0.08)) or 0.0)
        buy_fee_buffer = float(params.get("cash_aware_buy_fee_buffer_pct", 0.003) or 0.0)
        require_current = bool(params.get("require_current_market_data_for_orders", True))
        sell_orders: list[dict[str, Any]] = []
        buy_candidates: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        def skip(symbol: str, side: str, reason: str, quantity: int = 0) -> None:
            skipped.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "reason": reason,
                    "profile_key": bundle.profile.profile_key,
                    "strategy_id": bundle.profile.strategy_id,
                }
            )

        for symbol, qty in positions.items():
            current_qty = float(qty or 0.0)
            if current_qty <= 0:
                continue
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if require_current and price <= 0:
                skip(symbol, "SELL", "current_market_data_missing")
                continue
            target_weight = float(target_weights.get(symbol, 0.0) or 0.0)
            target_qty = self._round_lot((portfolio_value * target_weight) / price, lot_size) if price > 0 and target_weight > 0 else 0
            delta = target_qty - current_qty
            if symbol in target_weights and abs(delta) * price < portfolio_value * tolerance:
                continue
            if delta < 0 or symbol not in target_weights:
                available = float(account.positions.get(symbol, {}).get("available", current_qty) or current_qty)
                sell_qty = self._round_lot(min(-delta if symbol in target_weights else current_qty, available), lot_size)
                if sell_qty > 0:
                    sell_orders.append(self._order(bundle, symbol, "SELL", sell_qty, price, "CashAware sell/reduce first"))

        projected_cash = account.cash + sum(float(order["quantity"]) * float(order["reference_price"]) for order in sell_orders)
        available_cash = max(0.0, projected_cash * (1.0 - execution_reserve))
        for symbol, weight in target_weights.items():
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if require_current and price <= 0:
                skip(symbol, "BUY", "current_market_data_missing")
                continue
            if price <= 0:
                continue
            target_qty = self._round_lot((portfolio_value * float(weight or 0.0)) / price, lot_size)
            current_qty = float(positions.get(symbol, 0.0) or 0.0)
            delta = target_qty - current_qty
            if delta <= 0 or abs(delta) * price < portfolio_value * tolerance:
                continue
            buy_qty = self._round_lot(delta, lot_size)
            buy_value = buy_qty * price * (1.0 + buy_fee_buffer)
            if buy_qty <= 0:
                continue
            buy_candidates.append(self._order(bundle, symbol, "BUY", buy_qty, price, "CashAware target buy/add"))
            if buy_value <= available_cash:
                available_cash -= buy_value
            else:
                skip(symbol, "BUY", "cash_aware_deferred_insufficient_cash", buy_qty)
                buy_candidates.pop()
        return [*sell_orders, *buy_candidates], skipped

    async def _submit_paper_orders(
        self,
        orders: list[dict[str, Any]],
        *,
        trigger_source: str,
        run_id: str | None,
    ) -> dict[str, Any]:
        async with async_session_factory() as session:
            account = await session.scalar(select(LivePaperAccount).where(LivePaperAccount.account_key == PAPER_ACCOUNT_KEY))
            if account is None:
                account = LivePaperAccount(
                    account_key=PAPER_ACCOUNT_KEY,
                    cash=Decimal("1000000"),
                    total_asset=Decimal("1000000"),
                    market_value=Decimal("0"),
                    positions={},
                )
                session.add(account)
            cash = float(account.cash or 0)
            positions = self._json_dict(account.positions)
            results: list[dict[str, Any]] = []
            for order in orders:
                symbol = str(order.get("symbol") or "")
                side = str(order.get("side") or "").upper()
                qty = int(float(order.get("quantity") or 0))
                price = float(order.get("reference_price") or order.get("price") or 0.0)
                if not symbol or side not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
                    result = {"submitted": False, "message": "invalid paper order", "order": order}
                    results.append(result)
                    await self._write_submit_audit(order, result, mode="paper", trigger_source=trigger_source, run_id=run_id)
                    continue
                position = dict(positions.get(symbol) or {})
                current_qty = float(position.get("quantity", 0.0) or 0.0)
                if side == "BUY":
                    cost = qty * price
                    if cost > cash:
                        result = {"submitted": False, "message": "paper cash insufficient", "order": order}
                        results.append(result)
                        await self._write_submit_audit(order, result, mode="paper", trigger_source=trigger_source, run_id=run_id)
                        continue
                    cash -= cost
                    next_qty = current_qty + qty
                    prev_cost = float(position.get("avg_cost", price) or price)
                    avg_cost = ((prev_cost * current_qty) + cost) / max(next_qty, 1.0)
                    positions[symbol] = {
                        **position,
                        "symbol": symbol,
                        "quantity": next_qty,
                        "available": next_qty,
                        "avg_cost": avg_cost,
                        "market_value": next_qty * price,
                    }
                else:
                    sell_qty = min(current_qty, float(qty))
                    cash += sell_qty * price
                    next_qty = current_qty - sell_qty
                    if next_qty <= 0:
                        positions.pop(symbol, None)
                    else:
                        positions[symbol] = {
                            **position,
                            "quantity": next_qty,
                            "available": next_qty,
                            "market_value": next_qty * price,
                        }
                result = {"submitted": True, "paper": True, "order": order}
                results.append(result)
                await self._write_submit_audit(order, result, mode="paper", trigger_source=trigger_source, run_id=run_id)
            market_value = sum(float(pos.get("market_value", 0.0) or 0.0) for pos in positions.values())
            account.cash = Decimal(str(round(cash, 2)))
            account.market_value = Decimal(str(round(market_value, 2)))
            account.total_asset = Decimal(str(round(cash + market_value, 2)))
            account.positions = positions
            await session.commit()
        return {"submitted": all(item.get("submitted") for item in results) if results else False, "paper": True, "results": results}

    async def _write_order_audits(
        self,
        *,
        profile_key: str,
        strategy_id: int,
        trade_date: date,
        signal_hash: str,
        trigger_source: str,
        mode: str,
        run_id: str | None,
        orders: Sequence[dict[str, Any]],
        skipped_orders: Sequence[dict[str, Any]],
    ) -> None:
        async with async_session_factory() as session:
            for order in orders:
                session.add(
                    LiveOrderAudit(
                        audit_id=f"audit-{uuid.uuid4().hex}",
                        run_id=run_id,
                        profile_key=profile_key,
                        strategy_id=strategy_id,
                        trade_date=trade_date,
                        signal_hash=signal_hash,
                        trigger_source=trigger_source,
                        mode=mode,
                        status="generated",
                        order_payload=dict(order),
                        result_payload=None,
                        skip_reason=None,
                    )
                )
            for skipped in skipped_orders:
                session.add(
                    LiveOrderAudit(
                        audit_id=f"audit-{uuid.uuid4().hex}",
                        run_id=run_id,
                        profile_key=profile_key,
                        strategy_id=strategy_id,
                        trade_date=trade_date,
                        signal_hash=signal_hash,
                        trigger_source=trigger_source,
                        mode=mode,
                        status="skipped",
                        order_payload=dict(skipped),
                        result_payload=None,
                        skip_reason=str(skipped.get("reason") or ""),
                    )
                )
            await session.commit()

    async def _write_control_audit(
        self,
        *,
        profile_key: str,
        strategy_id: int,
        trade_date: date | None,
        signal_hash: str | None,
        trigger_source: str,
        mode: str,
        run_id: str | None,
        status: str,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        async with async_session_factory() as session:
            session.add(
                LiveOrderAudit(
                    audit_id=f"audit-{uuid.uuid4().hex}",
                    run_id=run_id,
                    profile_key=profile_key,
                    strategy_id=strategy_id,
                    trade_date=trade_date,
                    signal_hash=signal_hash,
                    trigger_source=trigger_source,
                    mode=mode,
                    status=status,
                    order_payload=dict(payload or {"reason": reason}),
                    result_payload=None,
                    skip_reason=reason,
                )
            )
            await session.commit()

    async def _write_submit_audit(
        self,
        order: dict[str, Any],
        result: dict[str, Any],
        *,
        mode: str,
        trigger_source: str,
        run_id: str | None,
    ) -> None:
        async with async_session_factory() as session:
            session.add(
                LiveOrderAudit(
                    audit_id=f"audit-{uuid.uuid4().hex}",
                    run_id=run_id,
                    profile_key=str(order.get("profile_key") or ""),
                    strategy_id=int(order.get("strategy_id") or 0),
                    trade_date=self._parse_date(order.get("trade_date")),
                    signal_hash=str(order.get("signal_hash") or "") or None,
                    trigger_source=trigger_source,
                    mode=mode,
                    status=("paper_filled" if mode == "paper" else "submitted") if result.get("submitted") else "failed",
                    order_payload=dict(order),
                    result_payload=dict(result),
                    skip_reason=None if result.get("submitted") else str(result.get("message") or ""),
                )
            )
            await session.commit()

    async def _has_submitted_signal(self, signal_hash: str) -> bool:
        async with async_session_factory() as session:
            row = await session.scalar(
                select(LiveOrderAudit).where(
                    LiveOrderAudit.signal_hash == signal_hash,
                    LiveOrderAudit.status.in_(["submitted", "paper_filled"]),
                )
            )
        return row is not None

    async def _persist_run(
        self,
        *,
        run_id: str,
        profile_key: str,
        strategy_id: int,
        mode: str,
        status: str,
        params: dict[str, Any],
    ) -> None:
        async with async_session_factory() as session:
            session.add(
                LiveTradingRun(
                    run_id=run_id,
                    profile_key=profile_key,
                    strategy_id=strategy_id,
                    mode=mode,
                    status=status,
                    params=params,
                )
            )
            await session.commit()

    async def _update_run_status(
        self,
        run_id: str,
        status: str,
        *,
        last_signal_hash: str | None = None,
        last_cycle_at: datetime | None = None,
        last_error: str | None = None,
        takeover_reason: str | None = None,
    ) -> None:
        async with async_session_factory() as session:
            run = await session.scalar(select(LiveTradingRun).where(LiveTradingRun.run_id == run_id))
            if run is None:
                return
            run.status = status
            if last_signal_hash is not None:
                run.last_signal_hash = last_signal_hash
            if last_cycle_at is not None:
                run.last_cycle_at = last_cycle_at
            run.last_error = last_error
            if takeover_reason is not None:
                run.takeover_reason = takeover_reason
            await session.commit()

    def _apply_theme_filter(self, frame: pd.DataFrame, params: dict[str, Any]) -> pd.DataFrame:
        include_terms = self._as_list(params.get("theme_include_industries")) + self._as_list(params.get("theme_include_keywords"))
        exclude_terms = self._as_list(params.get("theme_exclude_industries")) + self._as_list(params.get("theme_exclude_keywords"))
        if frame.empty or (not include_terms and not exclude_terms):
            return frame
        filtered = frame
        if exclude_terms:
            filtered = filtered.loc[[not self._row_matches_terms(row, exclude_terms) for _, row in filtered.iterrows()]]
        if include_terms:
            themed = filtered.loc[[self._row_matches_terms(row, include_terms) for _, row in filtered.iterrows()]]
            min_candidates = int(params.get("theme_min_candidates", 0) or 0)
            if bool(params.get("strict_theme_filter", False)) or len(themed) >= min_candidates:
                filtered = themed
        return filtered

    def _apply_limit_up_heat_filter(self, frame: pd.DataFrame, params: dict[str, Any]) -> tuple[pd.DataFrame, str | None]:
        if frame.empty or not bool(params.get("enable_limit_up_heat_filter", False)):
            return frame, None
        candidates = [col for col in ("raw__limit_up_heat", "raw__limit_up_heat_score", "limit_up_heat", "limit_up_heat_score") if col in frame.columns]
        if not candidates:
            return frame, "limit_up_heat_filter_declared_but_no_heat_column"
        pct = max(0.0, min(0.9, float(params.get("limit_up_heat_drop_top_pct", 0.0) or 0.0)))
        if pct <= 0:
            return frame, None
        column = candidates[0]
        values = pd.to_numeric(frame[column], errors="coerce")
        cutoff = values.quantile(1.0 - pct)
        return frame.loc[(values.isna()) | (values < cutoff)], f"drop_top_{pct:.0%}_by_{column}"

    def _rank_targets(self, frame: pd.DataFrame, positions: dict[str, float], params: dict[str, Any]) -> list[str]:
        top_n = max(1, int(params.get("top_n", 20) or 20))
        buy_rank_limit = max(top_n, int(params.get("buy_rank_limit", top_n) or top_n))
        hold_rank_buffer = max(top_n, int(params.get("hold_rank_buffer", top_n) or top_n))
        ranked = list(frame.index.astype(str))
        current = {symbol for symbol, qty in positions.items() if float(qty or 0.0) > 0}
        keep = [symbol for symbol in ranked[:hold_rank_buffer] if symbol in current]
        additions = [symbol for symbol in ranked[:buy_rank_limit] if symbol not in keep]
        return (keep + additions)[:top_n]

    def _target_weight(self, target_symbols: list[str], params: dict[str, Any]) -> float:
        if not target_symbols:
            return 0.0
        return min(
            float(params.get("max_position_pct", 0.06) or 0.06),
            (1.0 - float(params.get("cash_buffer_pct", 0.08) or 0.08)) / float(len(target_symbols)),
        )

    def _entry_filter_state(self, trade_date: date, params: dict[str, Any]) -> dict[str, Any]:
        return us_overnight_entry_filter_state(
            trade_date,
            mode=str(params.get("us_overnight_entry_filter", "none") or "none"),
            data_path=str(params.get("us_overnight_data_path", "") or ""),
            max_lag_days=int(params.get("us_overnight_max_lag_days", 5) or 5),
            caution_exposure=float(params.get("us_overnight_caution_exposure", 0.85) or 0.85),
            defensive_exposure=float(params.get("us_overnight_defensive_exposure", 0.70) or 0.70),
            qqq_caution_ret=float(params.get("us_overnight_qqq_caution_ret", -0.01) or -0.01),
            qqq_defensive_ret=float(params.get("us_overnight_qqq_defensive_ret", -0.02) or -0.02),
            semi_caution_ret=float(params.get("us_overnight_semi_caution_ret", -0.02) or -0.02),
            semi_defensive_ret=float(params.get("us_overnight_semi_defensive_ret", -0.03) or -0.03),
            nvda_caution_ret=float(params.get("us_overnight_nvda_caution_ret", -0.03) or -0.03),
            nvda_defensive_ret=float(params.get("us_overnight_nvda_defensive_ret", -0.04) or -0.04),
        )

    def _order(self, bundle: StrategyProfileBundle, symbol: str, side: str, quantity: int, price: float, remark: str) -> dict[str, Any]:
        return {
            "profile_key": bundle.profile.profile_key,
            "strategy_id": bundle.profile.strategy_id,
            "strategy_name": bundle.strategy.name,
            "symbol": symbol,
            "side": side,
            "quantity": int(quantity),
            "price_type": "latest_reference",
            "reference_price": round(float(price or 0.0), 4),
            "remark": remark,
        }

    async def _empty_signal_response(
        self,
        *,
        bundle: StrategyProfileBundle,
        params: dict[str, Any],
        trade_date: date,
        account: LiveAccountSnapshot,
        reason: str,
        mode: str,
        trigger_source: str = "manual",
        run_id: str | None = None,
        write_audit: bool = True,
    ) -> dict[str, Any]:
        signal_hash = self._signal_hash(bundle.profile.profile_key, bundle.profile.strategy_id, trade_date, [])
        if write_audit:
            await self._write_control_audit(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=trade_date,
                signal_hash=signal_hash,
                trigger_source=trigger_source,
                mode=mode,
                run_id=run_id,
                status="skipped",
                reason=reason,
                payload={"stage": "signal_generation", "reason": reason},
            )
        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "profile": self._profile_dict(bundle.profile, bundle.strategy),
            "mode": mode,
            "strategy_id": bundle.profile.strategy_id,
            "strategy_name": bundle.strategy.name,
            "trade_date": trade_date.isoformat(),
            "account": account.__dict__,
            "universe_size": 0,
            "candidate_count": 0,
            "excluded_symbol_count": 0,
            "target_symbols": [],
            "target_weights": {},
            "entry_filter": {},
            "quote_error": reason,
            "heat_filter_note": None,
            "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
            "auto_execute_enabled": bool(settings.live_trading_auto_execute_enabled),
            "signal_hash": signal_hash,
            "orders": [],
            "skipped_orders": [],
            "top_candidates": [],
        }

    def _top_candidates(self, frame: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
        preview = frame.head(limit).copy()
        preview = preview.where(preview.notna(), None)
        return [{"symbol": str(index), **dict(row)} for index, row in preview.iterrows()]

    def _profile_dict(self, profile: LiveStrategyProfile, strategy: Strategy | None = None) -> dict[str, Any]:
        return {
            "id": profile.id,
            "strategy_id": profile.strategy_id,
            "profile_key": profile.profile_key,
            "display_name": profile.display_name,
            "description": profile.description,
            "enabled": profile.enabled,
            "is_default": profile.is_default,
            "adapter_type": profile.adapter_type,
            "params_override": self._json_dict(profile.params_override),
            "universe_config": self._json_dict(profile.universe_config),
            "execution_policy": self._json_dict(profile.execution_policy),
            "strategy_name": strategy.name if strategy else None,
        }

    def _audit_dict(self, row: LiveOrderAudit) -> dict[str, Any]:
        return {
            "audit_id": row.audit_id,
            "run_id": row.run_id,
            "profile_key": row.profile_key,
            "strategy_id": row.strategy_id,
            "trade_date": row.trade_date.isoformat() if row.trade_date else None,
            "signal_hash": row.signal_hash,
            "trigger_source": row.trigger_source,
            "mode": row.mode,
            "status": row.status,
            "order_payload": self._json_dict(row.order_payload),
            "result_payload": self._json_dict(row.result_payload),
            "skip_reason": row.skip_reason,
            "created_at": row.created_at.isoformat(timespec="seconds") if row.created_at else None,
        }

    def _signal_hash(self, profile_key: str, strategy_id: int, trade_date: date, orders: Sequence[dict[str, Any]]) -> str:
        payload = {
            "profile_key": profile_key,
            "strategy_id": strategy_id,
            "trade_date": trade_date.isoformat(),
            "orders": sorted(
                [
                    {
                        "symbol": order.get("symbol"),
                        "side": order.get("side"),
                        "quantity": int(order.get("quantity") or 0),
                        "reference_price": float(order.get("reference_price") or 0.0),
                    }
                    for order in orders
                ],
                key=lambda item: (str(item["symbol"]), str(item["side"]), int(item["quantity"])),
            ),
        }
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def _seed_strategy_ids(self) -> list[int]:
        result: list[int] = []
        for item in str(settings.live_trading_seed_strategy_ids or "").replace(";", ",").split(","):
            text = item.strip()
            if text.isdigit():
                result.append(int(text))
        return result

    def _default_profile_key(self, strategy_id: int) -> str:
        if strategy_id == 62:
            return "tsmf_cashaware_stable"
        if strategy_id == 63:
            return "tsmf_cashaware_aggressive"
        return f"live_strategy_{strategy_id}"

    def _position_price_fallbacks(self, account: LiveAccountSnapshot) -> dict[str, float]:
        result: dict[str, float] = {}
        for symbol, position in account.positions.items():
            qty = float(position.get("quantity", 0.0) or 0.0)
            market_value = float(position.get("market_value", 0.0) or 0.0)
            avg_cost = float(position.get("avg_cost", 0.0) or 0.0)
            if qty > 0 and market_value > 0:
                result[symbol] = market_value / qty
            elif avg_cost > 0:
                result[symbol] = avg_cost
        return result

    def _quote_price(self, quote: dict[str, Any]) -> float:
        for key in ("lastPrice", "last_price", "price", "close"):
            try:
                value = float(quote.get(key) or 0.0)
            except Exception:
                value = 0.0
            if value > 0:
                return value
        return 0.0

    def _round_lot(self, qty: float, lot_size: int) -> int:
        lot = max(1, int(lot_size or 100))
        return int(float(qty or 0.0) // lot * lot)

    def _is_trading_window(self) -> bool:
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        current = now.time()
        return (time(9, 25) <= current <= time(11, 35)) or (time(12, 55) <= current <= time(15, 5))

    def _row_matches_terms(self, row: Any, terms: list[str]) -> bool:
        text = " ".join(
            self._normalize_text(row.get(column, ""))
            for column in ("industry", "industry2", "industry3", "sector", "concept")
        )
        return bool(text) and any(self._normalize_text(term) in text for term in terms if self._normalize_text(term))

    def _as_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        try:
            return [str(item).strip() for item in value if str(item).strip()]
        except Exception:
            return [str(value).strip()] if str(value).strip() else []

    def _json_dict(self, value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                raise ValueError("JSON value must be an object")
            return parsed
        if isinstance(value, dict):
            return dict(value)
        return dict(value)

    def _normalize_text(self, value: Any) -> str:
        return str(value or "").strip().lower()

    def _parse_date(self, value: Any) -> date | None:
        if isinstance(value, date):
            return value
        if not value:
            return None
        try:
            return date.fromisoformat(str(value)[:10])
        except Exception:
            return None


live_trading_service = LiveTradingService()

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
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from loguru import logger
from sqlalchemy import delete, select, update

from app.core.config import settings
from app.db.models.live_trading import (
    LiveOrderAudit,
    LivePaperAccount,
    LivePositionState,
    LiveStrategyProfile,
    LiveTradeRecord,
    LiveTradingRun,
)
from app.db.models.stock import Stock
from app.db.models.strategy import Strategy
from app.db.sqlite import async_session_factory
from app.data_stores import get_market_data_store
from app.engines.qmt_gateway import qmt_gateway
from app.services.factor_dependency_sync import build_precompute_prepare, execute_factor_dependency_sync
from app.services.factor_pipeline import FactorPipeline, LinearFactorScorer
from app.services.factor_precompute import (
    precompute_high_volume_features,
    precompute_live_timer_status_features,
)
from app.services.factor_value_store import (
    factor_params_hash,
    get_factor_definition,
    get_factor_value_store,
    normalize_factor_time,
)
from app.services.index_components import load_index_symbols
from app.services.qmt_trading import QmtAccountSnapshot, qmt_trading_service
from app.services.sync_service import SyncProgress, SyncService
from app.services.us_market import apply_entry_filter_to_target_weights, us_overnight_entry_filter_state


DEFAULT_ADAPTER = "multi_factor_cash_aware"
PAPER_ACCOUNT_KEY = "default"
STRATEGY_ACCOUNT_SYMBOL = "__account__"
LIVE_TIMER_STATUS_FACTORS = {"is_paused", "is_limit_up", "is_limit_down"}
LIVE_HIGH_VOLUME_FACTORS = {
    "cum_volume_at_time",
    "rolling_max_volume",
    "high_volume_ratio",
    "avoid_high_volume_ratio",
    "high_volume_signal",
}
LIVE_PENDING_STATUSES = {"live_pending", "submitted", "accepted", "partially_filled"}
LIVE_FILLED_STATUSES = {"live_filled", "filled"}
PAPER_FILLED_STATUSES = {"paper_filled"}


@dataclass
class LiveAccountSnapshot:
    cash: float
    total_asset: float
    market_value: float
    positions: dict[str, dict[str, Any]]
    source: str
    error: str | None = None
    meta: dict[str, Any] | None = None

    @classmethod
    def from_qmt(cls, snapshot: QmtAccountSnapshot) -> "LiveAccountSnapshot":
        return cls(
            cash=snapshot.cash,
            total_asset=snapshot.total_asset,
            market_value=snapshot.market_value,
            positions={symbol: position.as_dict() for symbol, position in snapshot.positions.items()},
            source=snapshot.source,
            error=snapshot.error,
            meta=None,
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
            "last_wait_reason": None,
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

    async def account_snapshot(
        self,
        *,
        mode: str = "live",
        profile_key: str | None = None,
        params: dict[str, Any] | None = None,
        include_broker: bool = True,
    ) -> dict[str, Any]:
        if mode not in {"paper", "live"}:
            raise ValueError("mode must be paper or live")
        bundle = await self._load_profile_bundle(profile_key) if profile_key else None
        snapshot = await self._account_snapshot(
            mode=mode,
            manual=None,
            params=dict(params or {}),
            profile_key=bundle.profile.profile_key if bundle else None,
        )
        data = await self._account_dict(snapshot, mode=mode)
        if include_broker and mode == "live":
            data["broker_account"] = await self._broker_account_dict()
        return data

    async def initialize_strategy_account(
        self,
        *,
        profile_key: str | None,
        mode: str,
        capital: float,
        reset_existing: bool = False,
    ) -> dict[str, Any]:
        if mode not in {"paper", "live"}:
            raise ValueError("mode must be paper or live")
        bundle = await self._load_profile_bundle(profile_key)
        capital_value = float(capital or 0.0)
        if capital_value <= 0:
            raise ValueError("capital must be positive")
        if mode == "live":
            try:
                broker = await qmt_trading_service.account_snapshot()
                cash_limit = float(broker.cash or 0.0) + max(100.0, capital_value * 0.005)
                if capital_value > cash_limit:
                    raise ValueError(f"QMT 可用现金不足：当前约 {broker.cash:.2f}，不能圈定 {capital_value:.2f}。")
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(f"初始化实盘资金池前需要读取 QMT 可用现金: {type(exc).__name__}: {exc}") from exc
        async with async_session_factory() as session:
            existing = await session.scalar(
                select(LivePositionState).where(
                    LivePositionState.profile_key == bundle.profile.profile_key,
                    LivePositionState.mode == mode,
                    LivePositionState.symbol == STRATEGY_ACCOUNT_SYMBOL,
                )
            )
            if existing is not None and self._json_dict(existing.state).get("initialized") and not reset_existing:
                position_exists = await session.scalar(
                    select(LivePositionState.id).where(
                        LivePositionState.profile_key == bundle.profile.profile_key,
                        LivePositionState.mode == mode,
                        LivePositionState.symbol != STRATEGY_ACCOUNT_SYMBOL,
                    ).limit(1)
                )
                if position_exists is not None:
                    raise ValueError("策略资金池已产生持仓；如需重置，请勾选重新初始化。")
            if reset_existing:
                await session.execute(
                    delete(LivePositionState).where(
                        LivePositionState.profile_key == bundle.profile.profile_key,
                        LivePositionState.mode == mode,
                    )
                )
                existing = None
            now = datetime.now().isoformat(timespec="seconds")
            state = {
                "initialized": True,
                "account_scope": "strategy_pool",
                "profile_key": bundle.profile.profile_key,
                "mode": mode,
                "initial_capital": round(capital_value, 2),
                "cash": round(capital_value, 2),
                "market_value": 0.0,
                "total_asset": round(capital_value, 2),
                "realized_pnl": 0.0,
                "fee_overdraft_limit": round(max(100.0, capital_value * 0.005), 2),
                "positions_source": "strategy_owned_only",
                "initialized_at": now,
                "updated_at": now,
            }
            account_row = existing or LivePositionState(
                profile_key=bundle.profile.profile_key,
                mode=mode,
                symbol=STRATEGY_ACCOUNT_SYMBOL,
                state=state,
            )
            account_row.state = state
            session.add(account_row)
            await session.commit()
        await self._write_control_audit(
            profile_key=bundle.profile.profile_key,
            strategy_id=bundle.profile.strategy_id,
            trade_date=date.today(),
            signal_hash=None,
            trigger_source="manual",
            mode=mode,
            run_id=None,
            status="strategy_account_initialized",
            reason=f"strategy account initialized with capital={capital_value:.2f}",
            payload={"stage": "strategy_account", "reset_existing": reset_existing, "capital": capital_value},
        )
        return await self.account_snapshot(mode=mode, profile_key=bundle.profile.profile_key)

    async def preflight(
        self,
        *,
        profile_key: str | None = None,
        mode: str = "paper",
        params: dict[str, Any] | None = None,
        manual_account: dict[str, Any] | None = None,
        evaluate_pipeline: bool = True,
    ) -> dict[str, Any]:
        if mode not in {"paper", "live"}:
            raise ValueError("mode must be paper or live")
        bundle = await self._load_profile_bundle(profile_key)
        normalized = self._normalized_params(bundle, params)
        trade_date = self._parse_date(normalized.get("trade_date")) or date.today()
        account = await self._account_snapshot(
            mode=mode,
            manual=manual_account,
            params=normalized,
            profile_key=bundle.profile.profile_key,
        )
        symbols: list[str] = []
        universe_error: str | None = None
        try:
            symbols = await self._resolve_symbols(bundle.profile, normalized, trade_date)
        except Exception as exc:
            universe_error = f"{type(exc).__name__}: {exc}"
        factor_configs = self._factor_configs(bundle, normalized)
        filters = self._filter_configs(bundle, normalized)
        return await self._build_preflight_report(
            bundle=bundle,
            params=normalized,
            trade_date=trade_date,
            account=account,
            symbols=symbols,
            factor_configs=factor_configs,
            filters=filters,
            mode=mode,
            universe_error=universe_error,
            include_factor_coverage=True,
            evaluate_pipeline=evaluate_pipeline,
        )

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
        include_preflight: bool = True,
    ) -> dict[str, Any]:
        bundle = await self._load_profile_bundle(profile_key)
        normalized = self._normalized_params(bundle, params)
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
        account = await self._account_snapshot(
            mode=mode,
            manual=manual_account,
            params=normalized,
            profile_key=bundle.profile.profile_key,
        )
        positions = {
            symbol: float(position.get("quantity", 0.0) or 0.0)
            for symbol, position in account.positions.items()
        }
        symbols = await self._resolve_symbols(bundle.profile, normalized, trade_date)
        factor_configs = self._factor_configs(bundle, normalized)
        filters = self._filter_configs(bundle, normalized)
        requirements, requirement_errors = self._factor_requirements(factor_configs, filters)
        effective_dates = self._factor_effective_dates(requirements, trade_date, symbols)
        preflight = None
        if include_preflight:
            preflight = await self._build_preflight_report(
                bundle=bundle,
                params=normalized,
                trade_date=trade_date,
                account=account,
                symbols=symbols,
                factor_configs=factor_configs,
                filters=filters,
                mode=mode,
                include_factor_coverage=True,
                evaluate_pipeline=False,
            )
            prepare_result = await self._prepare_live_intraday_factors(
                bundle=bundle,
                params=normalized,
                trade_date=trade_date,
                symbols=symbols,
                requirements=requirements,
                preflight=preflight,
                mode=mode,
                trigger_source=trigger_source,
                run_id=run_id,
                write_audit=write_audit,
            )
            if prepare_result.get("attempted"):
                preflight = await self._build_preflight_report(
                    bundle=bundle,
                    params=normalized,
                    trade_date=trade_date,
                    account=account,
                    symbols=symbols,
                    factor_configs=factor_configs,
                    filters=filters,
                    mode=mode,
                    include_factor_coverage=True,
                    evaluate_pipeline=False,
                    intraday_prepare=prepare_result,
                )
                effective_dates = self._factor_effective_dates(requirements, trade_date, symbols)
            signal_blocks = self._signal_blocking_reasons(preflight)
            if signal_blocks:
                return await self._empty_signal_response(
                    bundle=bundle,
                    params=normalized,
                    trade_date=trade_date,
                    account=account,
                    reason="；".join(signal_blocks),
                    mode=mode,
                    trigger_source=trigger_source,
                    run_id=run_id,
                    write_audit=write_audit,
                    universe_size=len(symbols),
                    preflight=preflight,
                    factor_dates=effective_dates,
                )
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
                universe_size=0,
                preflight=preflight,
                factor_dates=effective_dates,
            )

        if not factor_configs:
            raise ValueError("strategy profile has no FACTOR_CONFIGS")
        if requirement_errors:
            raise ValueError("；".join(requirement_errors))

        pipeline = FactorPipeline()
        result = await asyncio.to_thread(
            pipeline.build_cross_section,
            factor_specs=factor_configs,
            trade_date=trade_date,
            symbols=symbols,
            filters=filters,
            min_factor_coverage=float(normalized.get("min_factor_coverage", 0.4) or 0.4),
            scorer=LinearFactorScorer(),
            factor_date_map=effective_dates.get("factor_date_map"),
            filter_date_map=effective_dates.get("filter_date_map"),
        )
        frame = self._apply_theme_filter(result.frame, normalized)
        frame, heat_note = self._apply_limit_up_heat_filter(frame, normalized, trade_date)
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
                universe_size=len(symbols),
                preflight=preflight,
                factor_dates=effective_dates,
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
        await self._attach_stock_names([*orders, *skipped_orders])
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
            "account": self._snapshot_payload(account),
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
            "factor_dates": effective_dates,
            "preflight": self._with_pipeline_preflight(
                preflight,
                universe_size=len(symbols),
                raw_count=int(len(result.raw)),
                candidate_count=int(len(frame)),
                excluded_symbol_count=int(len(result.excluded_symbols)),
                heat_filter_note=heat_note,
            ) if preflight else None,
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
        await self._attach_stock_names(order_list)

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

        scope_check = await self._validate_strategy_account_orders(
            profile_key=bundle.profile.profile_key,
            mode=mode,
            orders=order_list,
        )
        if not scope_check.get("ok"):
            return await blocked(str(scope_check.get("message") or "策略资金池校验失败"))

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
            if result.get("submitted"):
                result["status"] = "live_pending"
                result["pending"] = True
                result["message"] = result.get("message") or "委托已提交，等待 QMT 成交回报确认。"
                result["filled_quantity"] = 0
                result["filled_value"] = 0
                result["realized_pnl"] = 0.0
            results.append(result)
            await self._write_submit_audit(order, result, mode=mode, trigger_source=trigger_source, run_id=run_id)
        return {
            "enabled": True,
            "submitted": all(bool(item.get("submitted")) for item in results) if results else False,
            "pending_count": sum(1 for item in results if item.get("pending")),
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

    async def list_trade_records(
        self,
        *,
        limit: int = 100,
        profile_key: str | None = None,
        mode: str | None = None,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
    ) -> list[dict[str, Any]]:
        start = self._parse_date(start_date) if not isinstance(start_date, date) else start_date
        end = self._parse_date(end_date) if not isinstance(end_date, date) else end_date
        async with async_session_factory() as session:
            stmt = select(LiveTradeRecord)
            if profile_key:
                stmt = stmt.where(LiveTradeRecord.profile_key == profile_key)
            if mode:
                stmt = stmt.where(LiveTradeRecord.mode == mode)
            if start:
                stmt = stmt.where(LiveTradeRecord.trade_date >= start)
            if end:
                stmt = stmt.where(LiveTradeRecord.trade_date <= end)
            stmt = stmt.order_by(LiveTradeRecord.created_at.desc()).limit(max(1, min(1000, int(limit or 100))))
            rows = (await session.execute(stmt)).scalars().all()
        return [self._trade_record_dict(row) for row in rows]

    async def list_pending_orders(
        self,
        *,
        limit: int = 100,
        profile_key: str | None = None,
        mode: str = "live",
    ) -> list[dict[str, Any]]:
        async with async_session_factory() as session:
            stmt = select(LiveTradeRecord).where(LiveTradeRecord.status.in_(sorted(LIVE_PENDING_STATUSES)))
            if profile_key:
                stmt = stmt.where(LiveTradeRecord.profile_key == profile_key)
            if mode:
                stmt = stmt.where(LiveTradeRecord.mode == mode)
            stmt = stmt.order_by(LiveTradeRecord.created_at.desc()).limit(max(1, min(500, int(limit or 100))))
            rows = (await session.execute(stmt)).scalars().all()
        return [self._pending_order_dict(row) for row in rows]

    async def weekly_analysis(
        self,
        *,
        week_start: date | str | None = None,
        profile_key: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        start = self._parse_date(week_start) if not isinstance(week_start, date) else week_start
        if start is None:
            today = date.today()
            start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
        records = await self.list_trade_records(
            start_date=start,
            end_date=end,
            profile_key=profile_key,
            mode=mode,
            limit=1000,
        )
        completed_statuses = PAPER_FILLED_STATUSES | LIVE_FILLED_STATUSES
        buy_notional = 0.0
        sell_notional = 0.0
        paper_realized_pnl = 0.0
        by_day: dict[str, dict[str, Any]] = {}
        by_profile: dict[str, dict[str, Any]] = {}
        by_status: dict[str, int] = {}
        by_side: dict[str, dict[str, Any]] = {}
        by_symbol: dict[str, dict[str, Any]] = {}
        for record in records:
            status = str(record.get("status") or "")
            side = str(record.get("side") or "").upper()
            value = float(record.get("order_value") or 0.0)
            signed_value = value if side == "BUY" else -value if side == "SELL" else 0.0
            if status in completed_statuses and side == "BUY":
                buy_notional += value
            if status in completed_statuses and side == "SELL":
                sell_notional += value
            result = self._json_dict(record.get("result_payload"))
            paper_realized_pnl += float(result.get("realized_pnl", 0.0) or 0.0)

            day_key = str(record.get("trade_date") or "-")
            day_row = by_day.setdefault(
                day_key,
                {"trade_date": day_key, "records": 0, "buy_notional": 0.0, "sell_notional": 0.0, "failed": 0, "paper_realized_pnl": 0.0},
            )
            day_row["records"] += 1
            if status in completed_statuses and side == "BUY":
                day_row["buy_notional"] += value
            if status in completed_statuses and side == "SELL":
                day_row["sell_notional"] += value
            if status not in completed_statuses and status not in LIVE_PENDING_STATUSES:
                day_row["failed"] += 1
            day_row["paper_realized_pnl"] += float(result.get("realized_pnl", 0.0) or 0.0)

            profile = str(record.get("profile_key") or "-")
            profile_row = by_profile.setdefault(profile, {"profile_key": profile, "records": 0, "notional": 0.0})
            profile_row["records"] += 1
            if status in completed_statuses:
                profile_row["notional"] += abs(signed_value)

            side_row = by_side.setdefault(side or "-", {"side": side or "-", "records": 0, "notional": 0.0})
            side_row["records"] += 1
            if status in completed_statuses:
                side_row["notional"] += value

            symbol = str(record.get("symbol") or "-")
            symbol_row = by_symbol.setdefault(
                symbol,
                {"symbol": symbol, "stock_name": record.get("stock_name"), "records": 0, "notional": 0.0, "net_notional": 0.0},
            )
            symbol_row["records"] += 1
            if status in completed_statuses:
                symbol_row["notional"] += value
                symbol_row["net_notional"] += signed_value

            by_status[status] = by_status.get(status, 0) + 1

        completed_count = sum(1 for record in records if str(record.get("status") or "") in completed_statuses)
        live_submitted_count = sum(
            1
            for record in records
            if record.get("mode") == "live" and str(record.get("status") or "") in LIVE_PENDING_STATUSES
        )
        failed_count = sum(
            1
            for record in records
            if str(record.get("status") or "") not in completed_statuses
            and str(record.get("status") or "") not in LIVE_PENDING_STATUSES
        )
        notes = []
        if live_submitted_count:
            notes.append("live_pending 表示 QMT 委托提交成功，但尚未在平台内确认成交；请结合 QMT 当日委托/成交核对。")
        if failed_count:
            notes.append(f"本周有 {failed_count} 条提交失败记录，需要查看 message 或订单审计。")
        if not records:
            notes.append("本周暂无独立交易记录。")

        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "week_start": start.isoformat(),
            "week_end": end.isoformat(),
            "profile_key": profile_key,
            "mode": mode,
            "summary": {
                "total_records": len(records),
                "completed_records": completed_count,
                "failed_records": failed_count,
                "buy_notional": round(buy_notional, 2),
                "sell_notional": round(sell_notional, 2),
                "net_notional": round(buy_notional - sell_notional, 2),
                "paper_realized_pnl": round(paper_realized_pnl, 2),
                "live_submitted_records": live_submitted_count,
            },
            "by_day": sorted(by_day.values(), key=lambda item: item["trade_date"]),
            "by_profile": sorted(by_profile.values(), key=lambda item: item["notional"], reverse=True),
            "by_status": [{"status": key, "records": value} for key, value in sorted(by_status.items())],
            "by_side": list(by_side.values()),
            "top_symbols": sorted(by_symbol.values(), key=lambda item: item["notional"], reverse=True)[:10],
            "notes": notes,
        }

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
        preflight = await self.preflight(
            profile_key=bundle.profile.profile_key,
            mode=mode,
            params=params,
            evaluate_pipeline=False,
        )
        runner_blocks = list(preflight.get("runner_blocking_reasons") or [])
        if runner_blocks:
            message = "；".join(str(item) for item in runner_blocks)
            await self._write_control_audit(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=self._parse_date((params or {}).get("trade_date")) or date.today(),
                signal_hash=None,
                trigger_source="manual",
                mode=mode,
                run_id=None,
                status="blocked",
                reason=message,
                payload={"stage": "runner_preflight", "preflight": preflight},
            )
            raise ValueError(message)
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
            "last_wait_reason": None,
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
        return {**dict(self._runner_status), "preflight": preflight}

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
                gate = self._execution_gate(params)
                if gate.get("can_run"):
                    signal = await self.signals(
                        profile_key=profile_key,
                        mode=mode,
                        params=params,
                        trigger_source="auto",
                        run_id=run_id,
                        include_preflight=True,
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
                            "last_wait_reason": None,
                        }
                    )
                    await self._update_run_status(
                        run_id,
                        "running",
                        last_signal_hash=signal_hash,
                        last_cycle_at=datetime.now(),
                        last_error=None,
                    )
                else:
                    self._runner_status.update(
                        {
                            "last_wait_reason": str(gate.get("reason") or "等待交易窗口"),
                            "last_error": None,
                        }
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

    def _normalized_params(self, bundle: StrategyProfileBundle, override: dict[str, Any] | None) -> dict[str, Any]:
        return {**bundle.params, **self._json_dict(override)}

    def _factor_configs(self, bundle: StrategyProfileBundle, params: dict[str, Any]) -> list[Any]:
        return list(params.get("factor_configs") or bundle.constants.get("FACTOR_CONFIGS") or [])

    def _filter_configs(self, bundle: StrategyProfileBundle, params: dict[str, Any]) -> list[Any]:
        return list(params.get("filter_factors") or bundle.constants.get("FILTER_FACTORS") or [])

    def _factor_effective_dates(
        self,
        requirements: list[dict[str, Any]],
        trade_date: date,
        symbols: list[str],
    ) -> dict[str, Any]:
        factor_date_map: dict[str, date] = {}
        filter_date_map: dict[str, date] = {}
        requirement_dates: list[dict[str, Any]] = []
        daily_names = [str(item["name"]) for item in requirements if not self._is_intraday_requirement(item)]
        daily_date = self._latest_factor_date_before(
            daily_names,
            trade_date,
            symbols,
        ) if daily_names else None

        for req in requirements:
            name = str(req["name"])
            roles = list(req.get("roles") or [])
            if self._is_intraday_requirement(req):
                effective_date = trade_date
                date_policy = "same_day_intraday"
            else:
                effective_date = daily_date or (trade_date - timedelta(days=1))
                date_policy = "previous_available_daily"
            if "factor" in roles:
                factor_date_map[name] = effective_date
            if "filter" in roles:
                filter_date_map[name] = effective_date
            requirement_dates.append(
                {
                    "name": name,
                    "roles": roles,
                    "as_of_time": req.get("as_of_time"),
                    "effective_date": effective_date,
                    "date_policy": date_policy,
                }
            )
        return {
            "trade_date": trade_date,
            "daily_factor_date": daily_date,
            "factor_date_map": factor_date_map,
            "filter_date_map": filter_date_map,
            "requirements": requirement_dates,
        }

    def _latest_factor_date_before(
        self,
        factor_names: list[str],
        trade_date: date,
        symbols: list[str],
    ) -> date | None:
        names = sorted({str(name) for name in factor_names if str(name).strip()})
        if not names:
            return None
        store = get_factor_value_store()
        latest_dates: list[date] = []
        market_cap_date: date | None = None
        for name in names:
            try:
                coverage = store.coverage(
                    name,
                    start_date=trade_date - timedelta(days=370),
                    end_date=trade_date - timedelta(days=1),
                    symbols=symbols or None,
                    include_symbols_sample=False,
                )
            except Exception:
                continue
            value = self._parse_date(coverage.get("max_date"))
            if value is not None:
                latest_dates.append(value)
                if name in {"market_cap", "market_cap_rank"}:
                    market_cap_date = value
        if not latest_dates:
            return None
        if market_cap_date is not None and len(latest_dates) < len(names):
            return market_cap_date
        return min(latest_dates)

    def _is_intraday_requirement(self, req: dict[str, Any]) -> bool:
        name = str(req.get("name") or "")
        if req.get("as_of_time"):
            return True
        if name in LIVE_TIMER_STATUS_FACTORS or name in LIVE_HIGH_VOLUME_FACTORS:
            return True
        definition = get_factor_definition(name) or {}
        frequency = str(definition.get("frequency") or "").lower()
        return frequency in {"timer", "minute", "intraday"}

    def _requirements_by_effective_date(
        self,
        requirements: list[dict[str, Any]],
        effective_dates: dict[str, Any],
    ) -> list[tuple[date, list[dict[str, Any]]]]:
        grouped: dict[date, list[dict[str, Any]]] = {}
        req_dates = {
            str(item.get("name")): item.get("effective_date")
            for item in effective_dates.get("requirements") or []
        }
        for req in requirements:
            effective_date = req_dates.get(str(req["name"]))
            if not isinstance(effective_date, date):
                effective_date = self._parse_date(effective_date)
            if effective_date is None:
                continue
            grouped.setdefault(effective_date, []).append(req)
        return sorted(grouped.items(), key=lambda item: item[0])

    @staticmethod
    def _merge_dependency_prepare(parts: list[dict[str, Any]]) -> dict[str, Any]:
        if not parts:
            return {
                "can_precompute": True,
                "coverage_gaps": [],
                "sync_plan": None,
                "precompute_payload": None,
            }
        coverage_gaps: list[dict[str, Any]] = []
        sync_steps: list[dict[str, Any]] = []
        payloads: list[dict[str, Any]] = []
        for part in parts:
            coverage_gaps.extend(list(part.get("coverage_gaps") or []))
            sync_plan = part.get("sync_plan") or {}
            sync_steps.extend(list(sync_plan.get("steps") or []))
            payload = part.get("precompute_payload")
            if payload:
                payloads.append(dict(payload))
        merged_plan = None
        if sync_steps:
            first_plan = next((part.get("sync_plan") for part in parts if part.get("sync_plan")), {}) or {}
            merged_plan = {
                **first_plan,
                "steps": sync_steps,
                "coverage_gaps": coverage_gaps,
            }
        return {
            "can_precompute": not coverage_gaps,
            "coverage_gaps": coverage_gaps,
            "sync_plan": merged_plan,
            "precompute_payloads": payloads,
        }

    def _serializable_factor_dates(self, effective_dates: dict[str, Any]) -> dict[str, Any]:
        def convert(value: Any) -> Any:
            if isinstance(value, date):
                return value.isoformat()
            if isinstance(value, dict):
                return {str(key): convert(item) for key, item in value.items()}
            if isinstance(value, list):
                return [convert(item) for item in value]
            return value

        return convert(effective_dates)

    async def _build_preflight_report(
        self,
        *,
        bundle: StrategyProfileBundle,
        params: dict[str, Any],
        trade_date: date,
        account: LiveAccountSnapshot,
        symbols: list[str],
        factor_configs: list[Any],
        filters: list[Any],
        mode: str,
        universe_error: str | None = None,
        include_factor_coverage: bool = True,
        evaluate_pipeline: bool = False,
        intraday_prepare: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        qmt_status = await qmt_trading_service.status()
        policy = self._json_dict(bundle.profile.execution_policy)
        phase = self._market_phase(trade_date=trade_date, params=params)
        index_symbol = self._resolve_index_symbol(bundle.profile, params)
        requirements, requirement_errors = self._factor_requirements(factor_configs, filters)
        factor_names = sorted({item["name"] for item in requirements if "factor" in item.get("roles", [])})
        filter_names = sorted({item["name"] for item in requirements if "filter" in item.get("roles", [])})
        required_names = sorted({item["name"] for item in requirements})
        effective_dates = self._factor_effective_dates(requirements, trade_date, symbols)

        dependency_prepare: dict[str, Any] | None = None
        dependency_error: str | None = None
        if required_names:
            prepare_params = dict(params)
            prepare_params.setdefault("time", params.get("rebalance_time") or "10:30")
            prepare_groups = self._requirements_by_effective_date(requirements, effective_dates)
            try:
                prepared_parts: list[dict[str, Any]] = []
                for effective_date, grouped_requirements in prepare_groups:
                    prepared_parts.append(
                        await asyncio.to_thread(
                            build_precompute_prepare,
                            mode="single",
                            factor_names=sorted({str(item["name"]) for item in grouped_requirements}),
                            group_name=None,
                            start_date=effective_date,
                            end_date=effective_date,
                            symbols=symbols or None,
                            index_symbol=index_symbol,
                            params=prepare_params,
                        )
                    )
                dependency_prepare = self._merge_dependency_prepare(prepared_parts)
                dependency_prepare["effective_dates"] = self._serializable_factor_dates(effective_dates)
                dependency_prepare["point_in_time_note"] = (
                    "live trading loads daily factors from the latest available factor date before trade_date; "
                    "same-day timer factors keep trade_date."
                )
            except Exception as exc:
                dependency_error = f"{type(exc).__name__}: {exc}"

        factor_coverage: list[dict[str, Any]] = []
        if include_factor_coverage and requirements and symbols:
            factor_coverage = await asyncio.to_thread(
                self._factor_coverage_snapshot,
                requirements,
                trade_date,
                symbols,
                effective_dates,
            )

        pipeline_probe: dict[str, Any] | None = None
        if evaluate_pipeline and factor_configs and symbols:
            try:
                pipeline_probe = await asyncio.to_thread(
                    self._pipeline_probe,
                    factor_configs,
                    filters,
                    trade_date,
                    symbols,
                    params,
                    effective_dates,
                )
            except Exception as exc:
                pipeline_probe = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

        signal_blocks: list[str] = []
        runner_blocks: list[str] = []
        warnings: list[str] = []

        if not bundle.profile.enabled:
            signal_blocks.append(f"profile {bundle.profile.profile_key} 已停用")
            runner_blocks.append(f"profile {bundle.profile.profile_key} 已停用")
        if bundle.profile.adapter_type != DEFAULT_ADAPTER:
            message = f"Unsupported adapter_type: {bundle.profile.adapter_type}"
            signal_blocks.append(message)
            runner_blocks.append(message)
        if requirement_errors:
            signal_blocks.extend(requirement_errors)
            runner_blocks.extend(requirement_errors)
        if not factor_configs:
            signal_blocks.append("strategy profile has no FACTOR_CONFIGS")
            runner_blocks.append("strategy profile has no FACTOR_CONFIGS")
        if universe_error:
            signal_blocks.append(f"股票池解析失败: {universe_error}")
            runner_blocks.append(f"股票池解析失败: {universe_error}")
        elif not symbols:
            signal_blocks.append("未解析到股票池")
            runner_blocks.append("未解析到股票池")

        account_meta = self._json_dict(account.meta)
        if account_meta.get("account_scope") == "strategy_pool" and not account_meta.get("initialized", False):
            message = "策略资金池未初始化：请先圈定本次量化交易本金，系统不会使用既有持仓调仓。"
            signal_blocks.append(message)
            runner_blocks.append(message)

        if not bool(params.get("ignore_trading_window", False)):
            phase_block = self._phase_signal_block(phase)
            if phase_block:
                signal_blocks.append(phase_block)
            runner_block = self._phase_runner_block(phase)
            if runner_block:
                runner_blocks.append(runner_block)
            elif phase.get("wait_reason"):
                warnings.append(str(phase["wait_reason"]))

        gaps = list((dependency_prepare or {}).get("coverage_gaps") or [])
        blocking_gaps = [gap for gap in gaps if not self._is_waitable_gap(gap, phase)]
        waitable_gaps = [gap for gap in gaps if self._is_waitable_gap(gap, phase)]
        if dependency_error:
            signal_blocks.append(f"数据依赖检查失败: {dependency_error}")
            runner_blocks.append(f"数据依赖检查失败: {dependency_error}")
        if blocking_gaps:
            labels = "、".join(str(gap.get("label") or gap.get("dependency")) for gap in blocking_gaps[:5])
            message = f"数据依赖未就绪: {labels}"
            signal_blocks.append(message)
            runner_blocks.append(message)
        if waitable_gaps:
            labels = "、".join(str(gap.get("label") or gap.get("dependency")) for gap in waitable_gaps[:5])
            warnings.append(f"盘中数据等待中: {labels}")

        missing_filter_rows = [
            row for row in factor_coverage
            if "filter" in row.get("roles", []) and int(row.get("value_count") or 0) == 0 and not self._is_waitable_requirement(row, phase)
        ]
        if missing_filter_rows:
            labels = "、".join(str(row.get("name")) for row in missing_filter_rows[:5])
            message = f"交易过滤因子缓存为空: {labels}"
            signal_blocks.append(message)
            runner_blocks.append(message)

        factor_rows = [row for row in factor_coverage if "factor" in row.get("roles", [])]
        empty_factor_rows = [row for row in factor_rows if int(row.get("value_count") or 0) == 0]
        if factor_rows and len(empty_factor_rows) == len(factor_rows):
            message = "所有排序因子在该交易日缓存为空"
            signal_blocks.append(message)
            runner_blocks.append(message)
        elif empty_factor_rows:
            labels = "、".join(str(row.get("name")) for row in empty_factor_rows[:5])
            warnings.append(f"部分排序因子缓存为空: {labels}")

        if pipeline_probe and not pipeline_probe.get("ok", True):
            signal_blocks.append(str(pipeline_probe.get("error") or "因子截面探测失败"))
        elif pipeline_probe and int(pipeline_probe.get("candidate_count") or 0) == 0:
            signal_blocks.append("因子截面为空或过滤后无候选")

        if mode == "live":
            if not bool(policy.get("allow_live_submit", True)):
                runner_blocks.append(f"profile {bundle.profile.profile_key} does not allow live submit")
            if not settings.live_trading_enable_order_submit:
                runner_blocks.append("LIVE_TRADING_ENABLE_ORDER_SUBMIT=false，不能启动实盘自动交易")
            if not settings.live_trading_auto_execute_enabled:
                runner_blocks.append("LIVE_TRADING_AUTO_EXECUTE_ENABLED=false，不能启动实盘自动交易")
            if not qmt_status.get("account_configured"):
                runner_blocks.append("QMT account is not configured")
            if not qmt_status.get("xttrader_available"):
                runner_blocks.append("xtquant.xttrader is unavailable")
            if not qmt_status.get("quote_connected"):
                runner_blocks.append("QMT quote connection is not ready")
        if not bool(policy.get("allow_auto_trade", True)):
            runner_blocks.append(f"profile {bundle.profile.profile_key} does not allow auto trade")

        signal_blocks = self._dedupe_text(signal_blocks)
        runner_blocks = self._dedupe_text(runner_blocks)
        warnings = self._dedupe_text(warnings)
        can_generate = not signal_blocks
        can_start_runner = not runner_blocks
        can_auto_submit = (
            mode == "live"
            and can_generate
            and can_start_runner
            and bool(settings.live_trading_enable_order_submit)
            and bool(settings.live_trading_auto_execute_enabled)
            and bool(qmt_status.get("account_configured"))
            and bool(qmt_status.get("xttrader_available"))
            and bool(qmt_status.get("quote_connected"))
            and not bool(self._runner_status.get("takeover"))
        )

        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "profile": self._profile_dict(bundle.profile, bundle.strategy),
            "mode": mode,
            "trade_date": trade_date.isoformat(),
            "market_phase": phase,
            "settings": {
                "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
                "auto_execute_enabled": bool(settings.live_trading_auto_execute_enabled),
                "default_profile": settings.live_trading_default_profile,
            },
            "qmt_status": qmt_status,
            "account": self._snapshot_payload(account),
            "policy": policy,
            "universe": {
                "index_symbol": index_symbol,
                "symbol_count": len(symbols),
                "sample_symbols": symbols[:10],
                "error": universe_error,
            },
            "strategy": {
                "factor_names": factor_names,
                "filter_names": filter_names,
                "min_factor_coverage": float(params.get("min_factor_coverage", 0.4) or 0.4),
                "rebalance_time": str(params.get("rebalance_time") or "10:30"),
            },
            "dependency_prepare": dependency_prepare,
            "dependency_error": dependency_error,
            "factor_coverage": factor_coverage,
            "pipeline_probe": pipeline_probe,
            "factor_dates": self._serializable_factor_dates(effective_dates),
            "intraday_prepare": intraday_prepare,
            "blocking_reasons": signal_blocks,
            "runner_blocking_reasons": runner_blocks,
            "warnings": warnings,
            "next_actions": self._preflight_next_actions(
                signal_blocks=signal_blocks,
                runner_blocks=runner_blocks,
                warnings=warnings,
                dependency_prepare=dependency_prepare,
                phase=phase,
            ),
            "can_generate": can_generate,
            "can_start_runner": can_start_runner,
            "can_auto_submit": can_auto_submit,
        }

    def _pipeline_probe(
        self,
        factor_configs: list[Any],
        filters: list[Any],
        trade_date: date,
        symbols: list[str],
        params: dict[str, Any],
        effective_dates: dict[str, Any],
    ) -> dict[str, Any]:
        pipeline = FactorPipeline()
        result = pipeline.build_cross_section(
            factor_specs=factor_configs,
            trade_date=trade_date,
            symbols=symbols,
            filters=filters,
            min_factor_coverage=float(params.get("min_factor_coverage", 0.4) or 0.4),
            scorer=LinearFactorScorer(),
            factor_date_map=effective_dates.get("factor_date_map"),
            filter_date_map=effective_dates.get("filter_date_map"),
        )
        frame = self._apply_theme_filter(result.frame, params)
        frame, heat_note = self._apply_limit_up_heat_filter(frame, params, trade_date)
        return {
            "ok": True,
            "raw_count": int(len(result.raw)),
            "candidate_count": int(len(frame)),
            "excluded_symbol_count": int(len(result.excluded_symbols)),
            "heat_filter_note": heat_note,
        }

    def _resolve_index_symbol(self, profile: LiveStrategyProfile, params: dict[str, Any]) -> str | None:
        universe = self._json_dict(profile.universe_config)
        value = (
            universe.get("index_symbol")
            or params.get("index_symbol")
            or ((params.get("backtest_settings") or {}).get("poolSource") or {}).get("indexSymbol")
        )
        return str(value or "399101.SZ")

    def _factor_requirements(self, factor_configs: list[Any], filters: list[Any]) -> tuple[list[dict[str, Any]], list[str]]:
        rows: dict[tuple[str, str | None, str | None], dict[str, Any]] = {}
        errors: list[str] = []

        def add(raw: Any, role: str) -> None:
            if isinstance(raw, str):
                name = raw.strip()
                params = None
                as_of_time = None
            elif isinstance(raw, dict):
                name = str(raw.get("name") or raw.get("factor_name") or "").strip()
                raw_params = raw.get("params")
                params = dict(raw_params) if isinstance(raw_params, dict) else None
                as_of_time = raw.get("as_of_time") or (params or {}).get("time") or (params or {}).get("as_of_time")
            else:
                name = ""
                params = None
                as_of_time = None
            if not name:
                errors.append(f"{role} 配置缺少 name/factor_name")
                return
            normalized_time = None
            if as_of_time:
                try:
                    normalized_time = normalize_factor_time(str(as_of_time))
                except Exception as exc:
                    errors.append(f"{name} as_of_time 无效: {exc}")
                    return
            params_hash = factor_params_hash(params) if params is not None else None
            key = (name, normalized_time, params_hash)
            item = rows.setdefault(
                key,
                {
                    "name": name,
                    "roles": [],
                    "as_of_time": normalized_time,
                    "params": params,
                    "params_hash": params_hash,
                },
            )
            if role not in item["roles"]:
                item["roles"].append(role)

        for item in factor_configs:
            add(item, "factor")
        for item in filters:
            add(item, "filter")
        return list(rows.values()), errors

    def _factor_coverage_snapshot(
        self,
        requirements: list[dict[str, Any]],
        trade_date: date,
        symbols: list[str],
        effective_dates: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        store = get_factor_value_store()
        rows: list[dict[str, Any]] = []
        denominator = max(1, len(symbols))
        req_dates = {
            str(item.get("name")): item.get("effective_date")
            for item in (effective_dates or {}).get("requirements") or []
        }
        for req in requirements:
            name = str(req["name"])
            params = req.get("params")
            as_of_time = req.get("as_of_time")
            effective_date = req_dates.get(name)
            if not isinstance(effective_date, date):
                effective_date = self._parse_date(effective_date)
            if effective_date is None:
                effective_date = trade_date
            try:
                values = store.load_cross_section(
                    name,
                    effective_date,
                    symbols=symbols,
                    as_of_time=as_of_time,
                    params=params,
                )
                coverage = store.coverage(
                    name,
                    start_date=effective_date,
                    end_date=effective_date,
                    symbols=symbols,
                    as_of_time=as_of_time,
                    params=params,
                    include_symbols_sample=False,
                )
                count = len(values)
                rows.append(
                    {
                        "name": name,
                        "roles": list(req.get("roles") or []),
                        "as_of_time": as_of_time,
                        "trade_date": trade_date.isoformat(),
                        "effective_date": effective_date.isoformat(),
                        "date_policy": "same_day_intraday" if self._is_intraday_requirement(req) else "previous_available_daily",
                        "params_hash": req.get("params_hash"),
                        "value_count": count,
                        "coverage_ratio": round(count / denominator, 4),
                        "status": "ok" if count > 0 else "empty",
                        "min_date": coverage.get("min_date"),
                        "max_date": coverage.get("max_date"),
                        "total_rows": coverage.get("total_rows", 0),
                        "symbol_count": coverage.get("symbol_count", 0),
                    }
                )
            except Exception as exc:
                rows.append(
                    {
                        "name": name,
                        "roles": list(req.get("roles") or []),
                        "as_of_time": as_of_time,
                        "trade_date": trade_date.isoformat(),
                        "effective_date": effective_date.isoformat(),
                        "date_policy": "same_day_intraday" if self._is_intraday_requirement(req) else "previous_available_daily",
                        "params_hash": req.get("params_hash"),
                        "value_count": 0,
                        "coverage_ratio": 0.0,
                        "status": "error",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return rows

    def _market_phase(self, *, trade_date: date, params: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now()
        today = now.date()
        rebalance_time = self._parse_time(params.get("rebalance_time"), default=time(10, 30))
        current = now.time()
        is_weekday = trade_date.weekday() < 5
        same_day = trade_date == today
        if trade_date > today:
            phase = "future"
            note = "交易日是未来日期，runner 不会提前执行。"
            can_run_now = False
        elif not is_weekday:
            phase = "non_trading_day"
            note = "周末不是 A 股交易日。"
            can_run_now = False
        elif not same_day:
            phase = "historical"
            note = "历史交易日可手动生成信号，runner 只执行当天。"
            can_run_now = True
        elif current < time(9, 0):
            phase = "before_market"
            note = "早盘未开市，runner 会等待交易窗口。"
            can_run_now = False
        elif current < rebalance_time:
            phase = "before_rebalance"
            note = f"当前早于 {rebalance_time.strftime('%H:%M')} 调仓时间，runner 会等待。"
            can_run_now = False
        elif time(11, 35) < current < time(12, 55):
            phase = "lunch_break"
            note = "午间休市，runner 会等待下午交易窗口。"
            can_run_now = False
        elif current > time(15, 5):
            phase = "after_close"
            note = "已过收盘后的执行窗口。"
            can_run_now = False
        elif (time(9, 25) <= current <= time(11, 35)) or (time(12, 55) <= current <= time(15, 5)):
            phase = "active"
            note = "处于可执行交易窗口。"
            can_run_now = True
        else:
            phase = "outside_window"
            note = "不在交易执行窗口。"
            can_run_now = False
        return {
            "phase": phase,
            "now": now.isoformat(timespec="seconds"),
            "today": today.isoformat(),
            "trade_date": trade_date.isoformat(),
            "same_day": same_day,
            "is_weekday": is_weekday,
            "is_trading_day": is_weekday,
            "rebalance_time": rebalance_time.strftime("%H:%M"),
            "can_run_now": can_run_now,
            "wait_reason": None if can_run_now else note,
            "note": note,
        }

    def _execution_gate(self, params: dict[str, Any]) -> dict[str, Any]:
        if bool(params.get("ignore_trading_window")):
            return {"can_run": True, "reason": None}
        trade_date = self._parse_date(params.get("trade_date")) or date.today()
        phase = self._market_phase(trade_date=trade_date, params=params)
        return {"can_run": bool(phase.get("can_run_now")), "reason": phase.get("wait_reason"), "phase": phase}

    def _phase_signal_block(self, phase: dict[str, Any]) -> str | None:
        value = str(phase.get("phase") or "")
        if value in {"future", "non_trading_day", "before_market", "before_rebalance", "lunch_break", "after_close", "outside_window"}:
            return str(phase.get("note") or phase.get("wait_reason") or "当前不适合生成实时交易信号")
        return None

    def _phase_runner_block(self, phase: dict[str, Any]) -> str | None:
        value = str(phase.get("phase") or "")
        if value == "future":
            return "runner 只执行当天交易日，不能启动到未来日期"
        if value == "historical":
            return "runner 只执行当天交易日；历史日期请手动生成信号"
        if value == "non_trading_day":
            return str(phase.get("note") or "今天不是交易日")
        return None

    def _signal_blocking_reasons(self, preflight: dict[str, Any]) -> list[str]:
        return [str(item) for item in preflight.get("blocking_reasons") or [] if str(item).strip()]

    async def _prepare_live_intraday_factors(
        self,
        *,
        bundle: StrategyProfileBundle,
        params: dict[str, Any],
        trade_date: date,
        symbols: list[str],
        requirements: list[dict[str, Any]],
        preflight: dict[str, Any],
        mode: str,
        trigger_source: str,
        run_id: str | None,
        write_audit: bool,
    ) -> dict[str, Any]:
        phase = preflight.get("market_phase") or self._market_phase(trade_date=trade_date, params=params)
        if not bool(phase.get("same_day")):
            return {"attempted": False, "status": "skipped", "reason": "not_same_day"}
        if not symbols:
            return {"attempted": False, "status": "skipped", "reason": "empty_universe"}
        missing_intraday = [
            row for row in preflight.get("factor_coverage") or []
            if int(row.get("value_count") or 0) == 0
            and self._is_intraday_requirement(row)
            and not self._is_waitable_requirement(row, phase)
        ]
        if not missing_intraday:
            return {"attempted": False, "status": "ready", "reason": "no_missing_intraday_factor"}

        sync_result: dict[str, Any] | None = None
        sync_plan = self._intraday_sync_plan(preflight)
        if sync_plan:
            sync_result = await self._execute_live_intraday_sync_plan(
                sync_plan,
                run_id=run_id,
            )
            if sync_result.get("status") == "failed":
                if write_audit:
                    await self._write_control_audit(
                        profile_key=bundle.profile.profile_key,
                        strategy_id=bundle.profile.strategy_id,
                        trade_date=trade_date,
                        signal_hash=None,
                        trigger_source=trigger_source,
                        mode=mode,
                        run_id=run_id,
                        status="factor_prepare_failed",
                        reason=str(sync_result.get("error") or "intraday dependency sync failed"),
                        payload={"stage": "intraday_dependency_sync", "sync_result": sync_result},
                    )
                return {
                    "attempted": True,
                    "status": "failed",
                    "trade_date": trade_date.isoformat(),
                    "missing_before": missing_intraday,
                    "sync": sync_result,
                    "results": [],
                    "errors": [str(sync_result.get("error") or "intraday dependency sync failed")],
                }

        by_time: dict[str, set[str]] = {}
        for row in missing_intraday:
            name = str(row.get("name") or "")
            as_of_time = row.get("as_of_time")
            if not as_of_time:
                req = next((item for item in requirements if str(item.get("name")) == name), {})
                as_of_time = req.get("as_of_time")
            if not as_of_time:
                definition = get_factor_definition(name) or {}
                as_of_time = definition.get("as_of_time")
            try:
                timer_text = normalize_factor_time(str(as_of_time or params.get("rebalance_time") or "10:30"))
            except Exception:
                timer_text = "10:30"
            by_time.setdefault(timer_text, set()).add(name)

        results: list[dict[str, Any]] = []
        errors: list[str] = []
        for timer_text, names in sorted(by_time.items()):
            timer_value = self._parse_time(timer_text, default=time(10, 30))
            if datetime.now().time() < timer_value:
                results.append({
                    "timer_time": timer_text,
                    "factor_names": sorted(names),
                    "status": "waiting",
                    "reason": f"当前早于 {timer_text}",
                })
                continue
            status_names = sorted(names & LIVE_TIMER_STATUS_FACTORS)
            high_volume_names = sorted(names & LIVE_HIGH_VOLUME_FACTORS)
            if status_names:
                try:
                    result = await asyncio.to_thread(
                        precompute_live_timer_status_features,
                        start_date=trade_date,
                        end_date=trade_date,
                        symbols=symbols,
                        timer_time=timer_text,
                        factor_names=status_names,
                    )
                    results.append({"timer_time": timer_text, "factor_names": status_names, "status": "completed", "result": result})
                except Exception as exc:
                    message = f"{timer_text} 状态因子准备失败: {type(exc).__name__}: {exc}"
                    errors.append(message)
                    results.append({"timer_time": timer_text, "factor_names": status_names, "status": "failed", "error": message})
            if high_volume_names:
                try:
                    result = await asyncio.to_thread(
                        precompute_high_volume_features,
                        start_date=trade_date,
                        end_date=trade_date,
                        symbols=symbols,
                        as_of_time=timer_text,
                        window=int(params.get("high_volume_window") or params.get("window") or 120),
                        threshold=float(params.get("high_volume_threshold") or params.get("threshold") or 0.9),
                        daily_volume_to_share_multiplier=float(params.get("daily_volume_to_share_multiplier") or 100.0),
                    )
                    results.append({"timer_time": timer_text, "factor_names": high_volume_names, "status": "completed", "result": result})
                except Exception as exc:
                    message = f"{timer_text} 放量因子准备失败: {type(exc).__name__}: {exc}"
                    errors.append(message)
                    results.append({"timer_time": timer_text, "factor_names": high_volume_names, "status": "failed", "error": message})

        status = "failed" if errors else ("waiting" if any(item.get("status") == "waiting" for item in results) else "completed")
        response = {
            "attempted": True,
            "status": status,
            "trade_date": trade_date.isoformat(),
            "missing_before": missing_intraday,
            "sync": sync_result,
            "results": results,
            "errors": errors,
        }
        if write_audit:
            await self._write_control_audit(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=trade_date,
                signal_hash=None,
                trigger_source=trigger_source,
                mode=mode,
                run_id=run_id,
                status="factor_prepare_failed" if errors else "factor_prepare",
                reason="；".join(errors) if errors else f"live intraday factor prepare {status}",
                payload={"stage": "intraday_factor_prepare", **response},
            )
        return response

    def _intraday_sync_plan(self, preflight: dict[str, Any]) -> dict[str, Any] | None:
        plan = dict((preflight.get("dependency_prepare") or {}).get("sync_plan") or {})
        steps = []
        for step in plan.get("steps") or []:
            step_type = str((step or {}).get("type") or "")
            if step_type in {"kline_minute", "cum_timer", "tushare_daily"}:
                next_step = dict(step)
                if step_type == "tushare_daily":
                    datasets = [str(item) for item in next_step.get("datasets") or [] if str(item) == "stock_limit_prices"]
                    if not datasets:
                        continue
                    next_step["datasets"] = datasets
                steps.append(next_step)
        if not steps:
            return None
        gaps = [
            dict(gap)
            for gap in plan.get("coverage_gaps") or []
            if str(gap.get("sync_step") or "") in {"kline_minute", "cum_timer", "tushare_daily"}
        ]
        return {**plan, "steps": steps, "coverage_gaps": gaps}

    async def _execute_live_intraday_sync_plan(self, plan: dict[str, Any], *, run_id: str | None) -> dict[str, Any]:
        progress = SyncProgress(
            sync_type="live_intraday_factor_dependency",
            status="running",
            total=len(plan.get("steps") or []),
            start_time=datetime.now(),
            details={"run_id": run_id, "plan": plan, "step_results": []},
        )
        try:
            async with async_session_factory() as session:
                service = SyncService(session)
                results = await execute_factor_dependency_sync(
                    service,
                    plan,
                    run_id=run_id,
                    task_id=None,
                    failure_strategy="stop",
                    progress=progress,
                )
                await session.commit()
            return {"status": "completed", "results": results}
        except Exception as exc:
            logger.opt(exception=True).warning("Live intraday dependency sync failed")
            return {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}

    def _is_waitable_gap(self, gap: dict[str, Any], phase: dict[str, Any]) -> bool:
        if not bool(phase.get("same_day")):
            return False
        if str(gap.get("sync_step")) not in {"kline_minute", "cum_timer"}:
            return False
        timer_text = gap.get("timer_time")
        if not timer_text:
            return False
        try:
            timer_value = self._parse_time(timer_text)
        except Exception:
            return False
        now_value = datetime.now().time()
        return now_value < timer_value

    def _is_waitable_requirement(self, row: dict[str, Any], phase: dict[str, Any]) -> bool:
        if not bool(phase.get("same_day")):
            return False
        as_of_time = row.get("as_of_time")
        if not as_of_time:
            return False
        try:
            timer_value = self._parse_time(as_of_time)
        except Exception:
            return False
        return datetime.now().time() < timer_value

    def _preflight_next_actions(
        self,
        *,
        signal_blocks: list[str],
        runner_blocks: list[str],
        warnings: list[str],
        dependency_prepare: dict[str, Any] | None,
        phase: dict[str, Any],
    ) -> list[str]:
        actions: list[str] = []
        all_blocks = [*signal_blocks, *runner_blocks]
        if any("资金池" in item or "本金" in item for item in all_blocks):
            actions.append("先在交易台的“量化资金池”区域圈定本次策略本金，再生成信号或启动 runner。")
        if any("QMT" in item or "xtquant" in item for item in all_blocks):
            actions.append("先打开并登录 miniQMT，确认账户、行情和交易模块可用。")
        if any("LIVE_TRADING_ENABLE_ORDER_SUBMIT" in item for item in all_blocks):
            actions.append("真实下单默认关闭；只有确认实盘运行后才在 .env.local 开启 LIVE_TRADING_ENABLE_ORDER_SUBMIT。")
        if any("LIVE_TRADING_AUTO_EXECUTE_ENABLED" in item for item in all_blocks):
            actions.append("自动实盘默认关闭；确认策略和账户无误后再开启 LIVE_TRADING_AUTO_EXECUTE_ENABLED。")
        if dependency_prepare and dependency_prepare.get("sync_plan"):
            actions.append("先执行依赖同步计划；日频因子补上一可用交易日，盘中因子补当天 timer factor_values。")
        if any("因子" in item or "factor" in item.lower() for item in all_blocks):
            actions.append("在因子页或同步任务里补齐策略因子缓存；实盘不会使用当天 daily_basic 市值做盘中排序。")
        if phase.get("wait_reason") and warnings:
            actions.append(str(phase["wait_reason"]))
        if not actions:
            actions.append("预检通过后，可以生成信号；实盘自动提交仍受真实下单和自动执行开关保护。")
        return self._dedupe_text(actions)

    @staticmethod
    def _with_pipeline_preflight(
        preflight: dict[str, Any] | None,
        *,
        universe_size: int,
        raw_count: int,
        candidate_count: int,
        excluded_symbol_count: int,
        heat_filter_note: str | None,
    ) -> dict[str, Any] | None:
        if preflight is None:
            return None
        result = dict(preflight)
        result["pipeline_probe"] = {
            "ok": True,
            "raw_count": raw_count,
            "candidate_count": candidate_count,
            "excluded_symbol_count": excluded_symbol_count,
            "heat_filter_note": heat_filter_note,
        }
        result["universe"] = {**dict(result.get("universe") or {}), "symbol_count": universe_size}
        return result

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

    async def _account_dict(self, account: LiveAccountSnapshot, *, mode: str) -> dict[str, Any]:
        positions = await self._position_rows(account.positions)
        unrealized_pnl = sum(float(row.get("unrealized_pnl") or 0.0) for row in positions)
        total_asset = account.total_asset or (account.cash + account.market_value)
        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "mode": mode,
            "cash": account.cash,
            "total_asset": total_asset,
            "market_value": account.market_value,
            "unrealized_pnl": round(unrealized_pnl, 2),
            "position_count": len(positions),
            "positions": positions,
            "positions_by_symbol": account.positions,
            "source": account.source,
            "error": account.error,
            "meta": self._json_dict(account.meta),
        }

    def _snapshot_payload(self, account: LiveAccountSnapshot) -> dict[str, Any]:
        return {
            "cash": account.cash,
            "total_asset": account.total_asset,
            "market_value": account.market_value,
            "positions": account.positions,
            "source": account.source,
            "error": account.error,
            "meta": self._json_dict(account.meta),
        }

    async def _position_rows(self, positions: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        symbol_list = [str(symbol) for symbol in positions.keys() if str(symbol).strip()]
        name_map = await self._stock_names(symbol_list)
        rows: list[dict[str, Any]] = []
        for symbol in sorted(symbol_list):
            raw = dict(positions.get(symbol) or {})
            quantity = float(raw.get("quantity", raw.get("volume", 0.0)) or 0.0)
            available = float(raw.get("available", raw.get("available_volume", quantity)) or 0.0)
            avg_cost = float(raw.get("avg_cost", raw.get("cost_price", 0.0)) or 0.0)
            market_value = float(raw.get("market_value", 0.0) or 0.0)
            cost_value = quantity * avg_cost if quantity > 0 and avg_cost > 0 else 0.0
            unrealized_pnl = market_value - cost_value if market_value or cost_value else float(raw.get("unrealized_pnl", 0.0) or 0.0)
            unrealized_pnl_pct = (unrealized_pnl / cost_value) if cost_value > 0 else None
            stock_name = raw.get("stock_name") or raw.get("name") or raw.get("stockName") or name_map.get(symbol)
            rows.append(
                {
                    **raw,
                    "symbol": symbol,
                    "stock_name": stock_name,
                    "quantity": quantity,
                    "available": available,
                    "avg_cost": avg_cost,
                    "market_value": market_value,
                    "cost_value": round(cost_value, 2),
                    "unrealized_pnl": round(unrealized_pnl, 2),
                    "unrealized_pnl_pct": round(unrealized_pnl_pct, 4) if unrealized_pnl_pct is not None else None,
                }
            )
        return rows

    async def _attach_stock_names(self, items: Sequence[dict[str, Any]]) -> None:
        symbols = [str(item.get("symbol") or "").strip() for item in items if str(item.get("symbol") or "").strip()]
        if not symbols:
            return
        name_map = await self._stock_names(symbols)
        for item in items:
            symbol = str(item.get("symbol") or "").strip()
            name = name_map.get(symbol)
            if name and not item.get("stock_name"):
                item["stock_name"] = name

    async def _stock_names(self, symbols: Sequence[str]) -> dict[str, str]:
        symbol_list = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
        if not symbol_list:
            return {}
        async with async_session_factory() as session:
            rows = (await session.execute(select(Stock.symbol, Stock.name).where(Stock.symbol.in_(symbol_list)))).all()
        return {str(symbol): str(name) for symbol, name in rows if name}

    async def _account_snapshot(
        self,
        *,
        mode: str,
        manual: dict[str, Any] | None,
        params: dict[str, Any],
        profile_key: str | None = None,
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
        if profile_key:
            return await self._strategy_account_snapshot(profile_key=profile_key, mode=mode, params=params)
        if mode == "paper":
            return await self._paper_account_snapshot(params)
        try:
            return LiveAccountSnapshot.from_qmt(await qmt_trading_service.account_snapshot())
        except Exception as exc:
            return LiveAccountSnapshot(cash=0.0, total_asset=0.0, market_value=0.0, positions={}, source="qmt", error=f"{type(exc).__name__}: {exc}")

    async def _strategy_account_snapshot(
        self,
        *,
        profile_key: str,
        mode: str,
        params: dict[str, Any],
    ) -> LiveAccountSnapshot:
        async with async_session_factory() as session:
            rows = (
                await session.execute(
                    select(LivePositionState).where(
                        LivePositionState.profile_key == profile_key,
                        LivePositionState.mode == mode,
                    )
                )
            ).scalars().all()
        account_row = next((row for row in rows if row.symbol == STRATEGY_ACCOUNT_SYMBOL), None)
        account_state = self._json_dict(account_row.state if account_row else None)
        if not account_state.get("initialized"):
            initial = float(params.get("initial_capital") or (params.get("backtest_settings") or {}).get("capital") or 0.0)
            return LiveAccountSnapshot(
                cash=0.0,
                total_asset=0.0,
                market_value=0.0,
                positions={},
                source=f"{mode}_strategy_pool",
                error="策略资金池未初始化，请先圈定本金；系统不会使用 QMT 既有持仓参与调仓。",
                meta={
                    "initialized": False,
                    "account_scope": "strategy_pool",
                    "profile_key": profile_key,
                    "mode": mode,
                    "suggested_initial_capital": initial,
                    "positions_source": "strategy_owned_only",
                },
            )

        positions = {
            row.symbol: self._json_dict(row.state)
            for row in rows
            if row.symbol != STRATEGY_ACCOUNT_SYMBOL and self._json_dict(row.state).get("quantity", 0)
        }
        await self._refresh_strategy_position_marks(positions, mode=mode)
        market_value = round(sum(float(position.get("market_value", 0.0) or 0.0) for position in positions.values()), 2)
        cash = float(account_state.get("cash", 0.0) or 0.0)
        total_asset = round(cash + market_value, 2)
        meta = {
            **account_state,
            "initialized": True,
            "account_scope": "strategy_pool",
            "profile_key": profile_key,
            "mode": mode,
            "positions_source": "strategy_owned_only",
            "market_value": market_value,
            "total_asset": total_asset,
        }
        return LiveAccountSnapshot(
            cash=round(cash, 2),
            total_asset=total_asset,
            market_value=market_value,
            positions={symbol: dict(position) for symbol, position in positions.items()},
            source=f"{mode}_strategy_pool",
            error=None,
            meta=meta,
        )

    async def _broker_account_dict(self) -> dict[str, Any]:
        try:
            snapshot = LiveAccountSnapshot.from_qmt(await qmt_trading_service.account_snapshot())
            return await self._account_dict(snapshot, mode="live")
        except Exception as exc:
            return {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "mode": "live",
                "cash": 0.0,
                "total_asset": 0.0,
                "market_value": 0.0,
                "unrealized_pnl": 0.0,
                "position_count": 0,
                "positions": [],
                "positions_by_symbol": {},
                "source": "qmt",
                "error": f"{type(exc).__name__}: {exc}",
            }

    async def _refresh_strategy_position_marks(self, positions: dict[str, dict[str, Any]], *, mode: str) -> None:
        if not positions:
            return
        broker_positions: dict[str, dict[str, Any]] = {}
        if mode == "live":
            try:
                broker = LiveAccountSnapshot.from_qmt(await qmt_trading_service.account_snapshot())
                broker_positions = broker.positions
            except Exception as exc:
                for position in positions.values():
                    position["broker_error"] = f"{type(exc).__name__}: {exc}"
        price_map, _ = await self._quote_prices(list(positions.keys()))
        for symbol, position in positions.items():
            quantity = float(position.get("quantity", 0.0) or 0.0)
            avg_cost = float(position.get("avg_cost", 0.0) or 0.0)
            broker_position = dict(broker_positions.get(symbol) or {})
            broker_available = float(broker_position.get("available", quantity) or 0.0) if broker_position else quantity
            broker_quantity = float(broker_position.get("quantity", quantity) or 0.0) if broker_position else quantity
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price <= 0 and broker_quantity > 0 and broker_position:
                price = float(broker_position.get("market_value", 0.0) or 0.0) / broker_quantity
            if price <= 0 and quantity > 0:
                price = float(position.get("reference_price", 0.0) or position.get("last_price", 0.0) or avg_cost)
            position["available"] = max(0.0, min(quantity, broker_available))
            position["broker_quantity"] = broker_quantity if mode == "live" else None
            position["broker_available"] = broker_available if mode == "live" else None
            position["last_price"] = price
            position["market_value"] = round(quantity * price, 2) if quantity > 0 and price > 0 else float(position.get("market_value", 0.0) or 0.0)

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

    async def _validate_strategy_account_orders(
        self,
        *,
        profile_key: str,
        mode: str,
        orders: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        snapshot = await self._strategy_account_snapshot(profile_key=profile_key, mode=mode, params={})
        meta = self._json_dict(snapshot.meta)
        if not meta.get("initialized"):
            return {"ok": False, "message": "策略资金池未初始化，禁止提交订单。"}
        cash = float(snapshot.cash or 0.0)
        fee_overdraft_limit = float(meta.get("fee_overdraft_limit", 0.0) or 0.0)
        positions = {
            symbol: float(position.get("quantity", 0.0) or 0.0)
            for symbol, position in snapshot.positions.items()
        }
        available = {
            symbol: float(position.get("available", position.get("quantity", 0.0)) or 0.0)
            for symbol, position in snapshot.positions.items()
        }
        for order in orders:
            symbol = str(order.get("symbol") or "")
            side = str(order.get("side") or "").upper()
            qty = float(order.get("quantity") or 0.0)
            price = float(order.get("reference_price") or order.get("price") or 0.0)
            if not symbol or qty <= 0 or price <= 0 or side not in {"BUY", "SELL"}:
                continue
            if side == "SELL":
                if qty > available.get(symbol, 0.0) + 1e-6:
                    return {
                        "ok": False,
                        "message": f"{symbol} 卖出数量超过策略资金池可用持仓；不会卖出原有手工持仓。",
                    }
                cash += qty * price
                positions[symbol] = max(0.0, positions.get(symbol, 0.0) - qty)
                available[symbol] = max(0.0, available.get(symbol, 0.0) - qty)
        for order in orders:
            side = str(order.get("side") or "").upper()
            if side != "BUY":
                continue
            qty = float(order.get("quantity") or 0.0)
            price = float(order.get("reference_price") or order.get("price") or 0.0)
            if qty <= 0 or price <= 0:
                continue
            cash -= qty * price
            if cash < -fee_overdraft_limit:
                return {
                    "ok": False,
                    "message": "买入金额超过策略资金池现金；只允许少量手续费级别超出。",
                }
        if mode == "live":
            broker_check = await self._validate_broker_capacity(orders)
            if not broker_check.get("ok"):
                return broker_check
        return {"ok": True, "cash_after": round(cash, 2)}

    async def _validate_broker_capacity(self, orders: Sequence[dict[str, Any]]) -> dict[str, Any]:
        try:
            broker = await qmt_trading_service.account_snapshot()
        except Exception as exc:
            return {"ok": False, "message": f"读取 QMT 账户失败，不能提交真实委托: {type(exc).__name__}: {exc}"}
        cash = float(broker.cash or 0.0)
        positions = {symbol: pos.as_dict() for symbol, pos in broker.positions.items()}
        for order in orders:
            symbol = str(order.get("symbol") or "")
            side = str(order.get("side") or "").upper()
            qty = float(order.get("quantity") or 0.0)
            price = float(order.get("reference_price") or order.get("price") or 0.0)
            if not symbol or qty <= 0 or price <= 0 or side not in {"BUY", "SELL"}:
                continue
            if side == "SELL":
                available = float((positions.get(symbol) or {}).get("available", 0.0) or 0.0)
                if qty > available + 1e-6:
                    return {"ok": False, "message": f"QMT 可用持仓不足，不能卖出 {symbol} {qty:.0f} 股。"}
                cash += qty * price
        for order in orders:
            if str(order.get("side") or "").upper() != "BUY":
                continue
            qty = float(order.get("quantity") or 0.0)
            price = float(order.get("reference_price") or order.get("price") or 0.0)
            if qty <= 0 or price <= 0:
                continue
            cash -= qty * price
            if cash < -100.0:
                return {"ok": False, "message": "QMT 可用现金不足，不能提交真实买入委托。"}
        return {"ok": True}

    async def _apply_strategy_account_fill(
        self,
        *,
        profile_key: str,
        mode: str,
        order: dict[str, Any],
        result: dict[str, Any],
    ) -> dict[str, Any]:
        symbol = str(order.get("symbol") or "")
        side = str(order.get("side") or "").upper()
        qty = float(result.get("filled_quantity") or order.get("quantity") or 0.0)
        price = float(result.get("filled_price") or order.get("reference_price") or order.get("price") or 0.0)
        if not symbol or side not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
            raise ValueError("invalid strategy account fill")
        async with async_session_factory() as session:
            rows = (
                await session.execute(
                    select(LivePositionState).where(
                        LivePositionState.profile_key == profile_key,
                        LivePositionState.mode == mode,
                    )
                )
            ).scalars().all()
            account = next((row for row in rows if row.symbol == STRATEGY_ACCOUNT_SYMBOL), None)
            if account is None or not self._json_dict(account.state).get("initialized"):
                raise ValueError("策略资金池未初始化")
            account_state = self._json_dict(account.state)
            position_row = next((row for row in rows if row.symbol == symbol), None)
            position = self._json_dict(position_row.state if position_row else None)
            cash = float(account_state.get("cash", 0.0) or 0.0)
            realized_pnl = float(account_state.get("realized_pnl", 0.0) or 0.0)
            fill_realized_pnl = 0.0
            current_qty = float(position.get("quantity", 0.0) or 0.0)
            avg_cost = float(position.get("avg_cost", price) or price)
            filled_value = round(qty * price, 2)
            if side == "BUY":
                cash -= filled_value
                next_qty = current_qty + qty
                next_avg = ((avg_cost * current_qty) + filled_value) / max(next_qty, 1.0)
                position = {
                    **position,
                    "symbol": symbol,
                    "stock_name": order.get("stock_name") or position.get("stock_name"),
                    "quantity": next_qty,
                    "available": next_qty,
                    "avg_cost": next_avg,
                    "reference_price": price,
                    "last_price": price,
                    "market_value": round(next_qty * price, 2),
                    "strategy_owned": True,
                    "updated_from": "live_submit" if mode == "live" else "paper_fill",
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                if position_row is None:
                    position_row = LivePositionState(profile_key=profile_key, mode=mode, symbol=symbol, state=position)
                position_row.state = position
                session.add(position_row)
            else:
                sell_qty = min(current_qty, qty)
                if sell_qty <= 0:
                    raise ValueError(f"{symbol} 不在策略资金池持仓中")
                cash += round(sell_qty * price, 2)
                fill_realized_pnl = (price - avg_cost) * sell_qty
                realized_pnl += fill_realized_pnl
                next_qty = current_qty - sell_qty
                if next_qty <= 1e-6:
                    if position_row is not None:
                        await session.delete(position_row)
                elif position_row is not None:
                    position_row.state = {
                        **position,
                        "quantity": next_qty,
                        "available": next_qty,
                        "reference_price": price,
                        "last_price": price,
                        "market_value": round(next_qty * price, 2),
                        "stock_name": order.get("stock_name") or position.get("stock_name"),
                        "strategy_owned": True,
                        "updated_from": "live_submit" if mode == "live" else "paper_fill",
                        "updated_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    session.add(position_row)
            live_positions = [
                self._json_dict(row.state)
                for row in rows
                if row.symbol not in {STRATEGY_ACCOUNT_SYMBOL, symbol}
            ]
            if side == "BUY":
                live_positions.append(position)
            elif next_qty > 1e-6:
                live_positions.append(self._json_dict(position_row.state if position_row else None))
            market_value = round(sum(float(item.get("market_value", 0.0) or 0.0) for item in live_positions), 2)
            account_state.update(
                {
                    "cash": round(cash, 2),
                    "market_value": market_value,
                    "total_asset": round(cash + market_value, 2),
                    "realized_pnl": round(realized_pnl, 2),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            account.state = account_state
            session.add(account)
            await session.commit()
        snapshot = await self._strategy_account_snapshot(profile_key=profile_key, mode=mode, params={})
        payload = await self._account_dict(snapshot, mode=mode)
        payload["last_fill"] = {
            "filled_quantity": qty,
            "filled_price": price,
            "filled_value": round(qty * price, 2),
            "realized_pnl": round(fill_realized_pnl, 2),
        }
        payload["strategy_account_note"] = "live 模式当前按委托提交成功回写策略账本，后续可接 QMT 成交回报校准。"
        return payload

    async def _submit_paper_orders(
        self,
        orders: list[dict[str, Any]],
        *,
        trigger_source: str,
        run_id: str | None,
    ) -> dict[str, Any]:
        profile_keys = {str(order.get("profile_key") or "").strip() for order in orders if str(order.get("profile_key") or "").strip()}
        if len(profile_keys) == 1:
            profile_key = next(iter(profile_keys))
            results: list[dict[str, Any]] = []
            for order in orders:
                symbol = str(order.get("symbol") or "")
                side = str(order.get("side") or "").upper()
                qty = int(float(order.get("quantity") or 0))
                price = float(order.get("reference_price") or order.get("price") or 0.0)
                if not symbol or side not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
                    result = {"submitted": False, "message": "invalid paper order", "order": order}
                else:
                    try:
                        snapshot = await self._apply_strategy_account_fill(
                            profile_key=profile_key,
                            mode="paper",
                            order=order,
                            result={"submitted": True, "paper": True, "order": order},
                        )
                        last_fill = self._json_dict(snapshot.get("last_fill"))
                        result = {
                            "submitted": True,
                            "paper": True,
                            "order": order,
                            "filled_quantity": float(last_fill.get("filled_quantity") or qty),
                            "filled_value": float(last_fill.get("filled_value") or round(qty * price, 2)),
                            "filled_price": float(last_fill.get("filled_price") or price),
                            "realized_pnl": float(last_fill.get("realized_pnl") or 0.0),
                            "account_snapshot": snapshot,
                        }
                    except ValueError as exc:
                        result = {"submitted": False, "paper": True, "message": str(exc), "order": order}
                results.append(result)
                await self._write_submit_audit(order, result, mode="paper", trigger_source=trigger_source, run_id=run_id)
            return {"submitted": all(item.get("submitted") for item in results) if results else False, "paper": True, "results": results}

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
                filled_qty = 0.0
                filled_value = 0.0
                realized_pnl = 0.0
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
                    filled_qty = float(qty)
                    filled_value = float(cost)
                    positions[symbol] = {
                        **position,
                        "symbol": symbol,
                        "stock_name": order.get("stock_name") or position.get("stock_name"),
                        "quantity": next_qty,
                        "available": next_qty,
                        "avg_cost": avg_cost,
                        "market_value": next_qty * price,
                    }
                else:
                    sell_qty = min(current_qty, float(qty))
                    avg_cost = float(position.get("avg_cost", price) or price)
                    filled_qty = float(sell_qty)
                    filled_value = float(sell_qty * price)
                    realized_pnl = float((price - avg_cost) * sell_qty)
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
                            "stock_name": order.get("stock_name") or position.get("stock_name"),
                        }
                result = {
                    "submitted": True,
                    "paper": True,
                    "order": order,
                    "filled_quantity": filled_qty,
                    "filled_value": filled_value,
                    "realized_pnl": realized_pnl,
                    "account_cash": round(cash, 2),
                }
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
            status = ("paper_filled" if mode == "paper" else str(result.get("status") or "live_pending")) if result.get("submitted") else "failed"
            order_payload = dict(order)
            result_payload = dict(result)
            trade_date = self._parse_date(order.get("trade_date"))
            profile_key = str(order.get("profile_key") or "")
            strategy_id = int(order.get("strategy_id") or 0)
            submitted = bool(result.get("submitted"))
            quantity = (
                result.get("filled_quantity")
                if status not in LIVE_PENDING_STATUSES
                else order.get("quantity")
            )
            price = (
                result.get("filled_price")
                if status not in LIVE_PENDING_STATUSES
                else None
            ) or order.get("reference_price") or order.get("price") or 0
            order_value = (
                result.get("filled_value")
                if status not in LIVE_PENDING_STATUSES
                else None
            )
            if order_value is None:
                order_value = float(quantity or 0) * float(price or 0)
            session.add(
                LiveOrderAudit(
                    audit_id=f"audit-{uuid.uuid4().hex}",
                    run_id=run_id,
                    profile_key=profile_key,
                    strategy_id=strategy_id,
                    trade_date=trade_date,
                    signal_hash=str(order.get("signal_hash") or "") or None,
                    trigger_source=trigger_source,
                    mode=mode,
                    status=status,
                    order_payload=order_payload,
                    result_payload=result_payload,
                    skip_reason=None if submitted else str(result.get("message") or ""),
                )
            )
            session.add(
                LiveTradeRecord(
                    record_id=f"trade-{uuid.uuid4().hex}",
                    run_id=run_id,
                    profile_key=profile_key,
                    strategy_id=strategy_id,
                    trade_date=trade_date,
                    signal_hash=str(order.get("signal_hash") or "") or None,
                    trigger_source=trigger_source,
                    mode=mode,
                    status=status,
                    symbol=str(order.get("symbol") or ""),
                    stock_name=str(order.get("stock_name") or "") or None,
                    side=str(order.get("side") or "").upper(),
                    quantity=Decimal(str(quantity or 0)),
                    reference_price=Decimal(str(price or 0)),
                    order_value=Decimal(str(order_value or 0)),
                    order_id=str(result.get("order_id") or "") or None,
                    message=str(result.get("message") or "") or (None if submitted else "提交失败"),
                    order_payload=order_payload,
                    result_payload=result_payload,
                    account_snapshot=(
                        self._json_dict(result.get("account_snapshot"))
                        if result.get("account_snapshot") is not None
                        else None
                    ),
                )
            )
            await session.commit()

    def _trade_record_dict(self, row: LiveTradeRecord) -> dict[str, Any]:
        result_payload = self._json_dict(row.result_payload)
        compact_result = {
            key: result_payload.get(key)
            for key in (
                "enabled",
                "submitted",
                "pending",
                "paper",
                "status",
                "order_id",
                "message",
                "filled_quantity",
                "filled_price",
                "filled_value",
                "realized_pnl",
                "account_cash",
            )
            if key in result_payload
        }
        order_payload = self._json_dict(row.order_payload)
        return {
            "record_id": row.record_id,
            "run_id": row.run_id,
            "profile_key": row.profile_key,
            "strategy_id": row.strategy_id,
            "trade_date": row.trade_date.isoformat() if row.trade_date else None,
            "signal_hash": row.signal_hash,
            "trigger_source": row.trigger_source,
            "mode": row.mode,
            "status": row.status,
            "symbol": row.symbol,
            "stock_name": row.stock_name,
            "side": row.side,
            "quantity": float(row.quantity or 0),
            "reference_price": float(row.reference_price or 0),
            "order_value": float(row.order_value or 0),
            "order_id": row.order_id,
            "message": row.message,
            "order_payload": order_payload,
            "result_payload": compact_result or None,
            "account_snapshot": None,
            "created_at": row.created_at.isoformat(timespec="seconds") if row.created_at else None,
        }

    def _pending_order_dict(self, row: LiveTradeRecord) -> dict[str, Any]:
        result_payload = self._json_dict(row.result_payload)
        compact_result = {
            key: result_payload.get(key)
            for key in ("enabled", "submitted", "pending", "status", "order_id", "message")
            if key in result_payload
        }
        if "message" not in compact_result and row.message:
            compact_result["message"] = row.message
        return {
            "record_id": row.record_id,
            "run_id": row.run_id,
            "profile_key": row.profile_key,
            "strategy_id": row.strategy_id,
            "trade_date": row.trade_date.isoformat() if row.trade_date else None,
            "signal_hash": row.signal_hash,
            "trigger_source": row.trigger_source,
            "mode": row.mode,
            "status": row.status,
            "symbol": row.symbol,
            "stock_name": row.stock_name,
            "side": row.side,
            "quantity": float(row.quantity or 0),
            "reference_price": float(row.reference_price or 0),
            "order_value": float(row.order_value or 0),
            "order_id": row.order_id,
            "message": row.message,
            "result_payload": compact_result or None,
            "created_at": row.created_at.isoformat(timespec="seconds") if row.created_at else None,
        }

    async def _has_submitted_signal(self, signal_hash: str) -> bool:
        async with async_session_factory() as session:
            row = await session.scalar(
                select(LiveOrderAudit).where(
                    LiveOrderAudit.signal_hash == signal_hash,
                    LiveOrderAudit.status.in_(["submitted", "live_pending", "paper_filled", "live_filled", "filled"]),
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

    def _apply_limit_up_heat_filter(
        self,
        frame: pd.DataFrame,
        params: dict[str, Any],
        trade_date: date,
    ) -> tuple[pd.DataFrame, str | None]:
        if frame.empty or not bool(params.get("enable_limit_up_heat_filter", False)):
            return frame, None
        pct = max(0.0, min(0.9, float(params.get("limit_up_heat_drop_top_pct", 0.0) or 0.0)))
        if pct <= 0:
            return frame, None
        candidates = [col for col in ("raw__limit_up_heat", "raw__limit_up_heat_score", "limit_up_heat", "limit_up_heat_score") if col in frame.columns]
        column = candidates[0] if candidates else "limit_up_heat"
        values = pd.to_numeric(frame[column], errors="coerce") if candidates else self._limit_up_heat_series(
            list(frame.index.astype(str)),
            trade_date,
            lookback=int(params.get("limit_up_heat_lookback", 20) or 20),
            min_history=int(params.get("limit_up_heat_min_history_days", 10) or 10),
        ).reindex(frame.index.astype(str))
        if values.empty:
            return frame, "limit_up_heat_data_missing"
        top_n = max(1, int(params.get("top_n", 20) or 20))
        if int(values.notna().sum()) < top_n:
            return frame, f"limit_up_heat_data_insufficient:{int(values.notna().sum())}/{top_n}"
        ranks = values.rank(pct=True, method="average")
        threshold = 1.0 - pct
        keep = ranks.le(threshold) | values.isna()
        filtered = frame.loc[keep.reindex(frame.index.astype(str)).fillna(True).to_numpy()]
        if len(filtered) < top_n:
            return frame, f"limit_up_heat_fallback_after_filter:{len(filtered)}/{top_n}"
        dropped = int(len(frame) - len(filtered))
        return filtered, f"drop_top_{pct:.0%}_by_{column}:dropped={dropped}"

    def _limit_up_heat_series(
        self,
        symbols: Sequence[str],
        trade_date: date,
        *,
        lookback: int,
        min_history: int,
    ) -> pd.Series:
        symbol_list = [str(symbol) for symbol in symbols if str(symbol).strip()]
        if not symbol_list:
            return pd.Series(dtype="float64")
        lookback = max(1, int(lookback or 20))
        min_history = max(1, int(min_history or 10))
        start_date = trade_date - timedelta(days=max(45, lookback * 3))
        try:
            daily = get_market_data_store().load_daily(
                symbol_list,
                start_date,
                trade_date,
                columns=["symbol", "trade_date", "close"],
            )
        except Exception as exc:
            logger.warning("Failed to load daily bars for live limit-up heat filter: {}", exc)
            return pd.Series(dtype="float64")
        daily = self._normalize_limit_up_heat_daily_frame(daily)
        if daily.empty:
            return pd.Series(dtype="float64")
        limits = self._load_limit_prices(symbol_list, start_date, trade_date)
        if limits.empty:
            return pd.Series(dtype="float64")

        daily["symbol"] = daily["symbol"].astype(str)
        daily["trade_date"] = pd.to_datetime(daily["trade_date"], errors="coerce").dt.date
        daily["close"] = pd.to_numeric(daily["close"], errors="coerce")
        limits["symbol"] = limits["symbol"].astype(str)
        limits["trade_date"] = pd.to_datetime(limits["trade_date"], errors="coerce").dt.date
        limits["up_limit"] = pd.to_numeric(limits["up_limit"], errors="coerce")
        merged = daily.merge(limits, on=["symbol", "trade_date"], how="left")
        merged = merged.dropna(subset=["symbol", "trade_date"]).sort_values(["symbol", "trade_date"])
        if merged.empty:
            return pd.Series(dtype="float64")
        merged["raw_is_limit_up"] = (
            (merged["close"] > 0)
            & (merged["up_limit"] > 0)
            & (merged["close"] >= merged["up_limit"] - 1e-4)
        ).astype(float)
        rows: list[tuple[str, float]] = []
        for symbol, group in merged.groupby("symbol", sort=False):
            recent = group.tail(lookback)
            value = float("nan") if len(recent) < min_history else float(recent["raw_is_limit_up"].sum())
            rows.append((str(symbol), value))
        return pd.Series(dict(rows), dtype="float64").reindex(symbol_list)

    @staticmethod
    def _normalize_limit_up_heat_daily_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
        if frame is None or getattr(frame, "empty", True):
            return pd.DataFrame()
        body = frame.reset_index() if "trade_date" not in frame.columns else frame.copy()
        if "trade_date" not in body.columns and body.index.name == "trade_date":
            body = body.reset_index()
        if "symbol" not in body.columns or "trade_date" not in body.columns or "close" not in body.columns:
            return pd.DataFrame()
        return body[["symbol", "trade_date", "close"]].copy()

    @staticmethod
    def _load_limit_prices(symbols: Sequence[str], start_date: date, end_date: date) -> pd.DataFrame:
        db_path = Path(getattr(settings, "sqlite_db_path", None) or Path(settings.data_dir) / "gaoshou.db")
        if not db_path.exists():
            return pd.DataFrame()
        rows: list[tuple[Any, ...]] = []
        symbol_list = [str(symbol) for symbol in symbols if str(symbol).strip()]
        try:
            with sqlite3.connect(db_path) as conn:
                for offset in range(0, len(symbol_list), 800):
                    batch = symbol_list[offset : offset + 800]
                    if not batch:
                        continue
                    placeholders = ",".join(["?"] * len(batch))
                    sql = (
                        "SELECT symbol, trade_date, up_limit "
                        "FROM stock_limit_prices "
                        "WHERE trade_date >= ? AND trade_date <= ? "
                        f"AND symbol IN ({placeholders})"
                    )
                    rows.extend(conn.execute(sql, [start_date, end_date, *batch]).fetchall())
        except sqlite3.Error as exc:
            logger.warning("Failed to load stock_limit_prices for live limit-up heat filter: {}", exc)
            return pd.DataFrame()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=["symbol", "trade_date", "up_limit"])

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
        universe_size: int = 0,
        preflight: dict[str, Any] | None = None,
        factor_dates: dict[str, Any] | None = None,
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
            "account": self._snapshot_payload(account),
            "universe_size": universe_size,
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
            "factor_dates": self._serializable_factor_dates(factor_dates or {}),
            "preflight": preflight,
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

    def _parse_time(self, value: Any, *, default: time | None = None) -> time:
        if isinstance(value, time):
            return value
        if value in (None, ""):
            if default is not None:
                return default
            raise ValueError("time value is required")
        parts = str(value).strip().split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid time value: {value}")
        return time(int(parts[0]), int(parts[1]))

    @staticmethod
    def _dedupe_text(items: Sequence[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for item in items:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            result.append(text)
        return result


live_trading_service = LiveTradingService()

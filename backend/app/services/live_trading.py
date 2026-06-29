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
from time import perf_counter
from typing import Any, AsyncIterator, Callable, Sequence

import pandas as pd
from loguru import logger
from sqlalchemy import delete, select, update

from app.core.blocking import run_blocking
from app.core.config import settings
from app.db.models.live_trading import (
    LiveEquitySnapshot,
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
LIVE_PENDING_STATUSES = {"live_pending", "submitted", "accepted", "partially_filled", "cancel_requested"}
LIVE_FILLED_STATUSES = {"live_filled", "filled", "partially_cancelled"}
LIVE_CANCELLED_STATUSES = {"cancelled", "partially_cancelled"}
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
        self._strategy_account_write_lock = asyncio.Lock()
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
        default_profile = next(
            (str(profile.get("profile_key")) for profile in profiles if profile.get("is_default") and profile.get("profile_key")),
            settings.live_trading_default_profile,
        )
        return {
            **qmt,
            "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
            "auto_execute_enabled": bool(settings.live_trading_auto_execute_enabled),
            "default_profile": default_profile,
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
        record_equity: bool = True,
    ) -> dict[str, Any]:
        if mode not in {"paper", "live"}:
            raise ValueError("mode must be paper or live")
        bundle = await self._load_profile_bundle(profile_key) if profile_key else None
        snapshot = await self._account_snapshot(
            mode=mode,
            manual=None,
            params=dict(params or {}),
            profile_key=bundle.profile.profile_key if bundle else None,
            prefer_local=mode == "live" and bundle is not None,
        )
        data = await self._account_dict(snapshot, mode=mode)
        if (
            bundle is not None
            and record_equity
            and not data.get("error")
            and not self._json_dict(data.get("meta")).get("stale_display_only")
        ):
            await self._record_equity_snapshot(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                mode=mode,
                account=data,
                source="account_snapshot",
            )
        if include_broker and mode == "live" and not self._json_dict(data.get("meta")).get("stale_display_only"):
            data["broker_account"] = await self._broker_account_dict()
        return data

    async def account_stream(
        self,
        *,
        mode: str = "live",
        profile_key: str | None = None,
        interval_seconds: int = 5,
    ) -> AsyncIterator[dict[str, Any]]:
        interval = max(2, min(60, int(interval_seconds or 5)))
        while True:
            started_at = datetime.now()
            try:
                payload = await self.account_snapshot(
                    mode=mode,
                    profile_key=profile_key,
                    include_broker=False,
                    record_equity=False,
                )
                yield {
                    "event": "account",
                    "data": payload,
                    "interval_seconds": interval,
                }
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                yield {
                    "event": "stream-error",
                    "data": {
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                        "message": f"{type(exc).__name__}: {exc}",
                    },
                    "interval_seconds": interval,
                }
            elapsed = (datetime.now() - started_at).total_seconds()
            await asyncio.sleep(max(0.5, interval - elapsed))

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
        async with async_session_factory() as session:
            rows = (
                await session.execute(
                    select(LivePositionState).where(
                        LivePositionState.profile_key == bundle.profile.profile_key,
                        LivePositionState.mode == mode,
                    )
                )
            ).scalars().all()
        existing = next((row for row in rows if row.symbol == STRATEGY_ACCOUNT_SYMBOL), None)
        existing_state = self._json_dict(existing.state if existing else None)
        is_adjustment = existing is not None and bool(existing_state.get("initialized")) and not reset_existing
        position_states = {
            row.symbol: self._json_dict(row.state)
            for row in rows
            if row.symbol != STRATEGY_ACCOUNT_SYMBOL and float(self._json_dict(row.state).get("quantity", 0.0) or 0.0) > 0
        }
        if is_adjustment:
            if mode == "live":
                current_snapshot = await self._strategy_account_snapshot(
                    profile_key=bundle.profile.profile_key,
                    mode=mode,
                    params={},
                    prefer_local=True,
                )
                if current_snapshot.error:
                    raise ValueError(f"调整实盘本金前需要实时读取 QMT 策略持仓和行情: {current_snapshot.error}")
                position_states = {symbol: dict(state) for symbol, state in current_snapshot.positions.items()}
                current_market_value = round(float(current_snapshot.market_value or 0.0), 2)
                current_cash = float(current_snapshot.cash or 0.0)
            else:
                await self._refresh_strategy_position_marks(position_states, mode=mode)
                current_market_value = round(sum(float(item.get("market_value", 0.0) or 0.0) for item in position_states.values()), 2)
                current_cash = float(existing_state.get("cash", 0.0) or 0.0)
            current_position_cost = self._positions_cost_basis(position_states)
            desired_cash = round(capital_value - current_position_cost, 2)
            current_total_asset = round(current_cash + current_market_value, 2)
            adjusted_cash = round(desired_cash, 2)
            adjusted_total_asset = round(adjusted_cash + current_market_value, 2)
            cash_delta = round(adjusted_cash - current_cash, 2)
            live_required_cash = max(0.0, cash_delta)
        else:
            current_market_value = 0.0
            current_position_cost = 0.0
            current_cash = 0.0
            current_total_asset = 0.0
            adjusted_cash = capital_value
            adjusted_total_asset = capital_value
            desired_cash = capital_value
            cash_delta = 0.0
            live_required_cash = capital_value
        if mode == "live" and live_required_cash > 0:
            try:
                broker = await qmt_trading_service.account_snapshot()
                broker_cash = float(broker.cash or 0.0)
                cash_limit = broker_cash + max(100.0, live_required_cash * 0.005)
                if live_required_cash > cash_limit:
                    raise ValueError(
                        f"QMT 可用现金不足：当前约 {broker_cash:.2f}，不能圈定所需现金 {live_required_cash:.2f}。"
                    )
            except ValueError:
                raise
            except Exception as exc:
                raise ValueError(f"初始化/调整实盘资金池前需要读取 QMT 可用现金: {type(exc).__name__}: {exc}") from exc
        if is_adjustment:
            now = datetime.now().isoformat(timespec="seconds")
            cash_adjustment_total = float(existing_state.get("cash_adjustment_total", 0.0) or 0.0) + cash_delta
            updated_state = {
                **existing_state,
                "initialized": True,
                "account_scope": "strategy_pool",
                "profile_key": bundle.profile.profile_key,
                "mode": mode,
                "target_capital": round(capital_value, 2),
                "cash": adjusted_cash,
                "market_value": current_market_value,
                "total_asset": adjusted_total_asset,
                "position_cost_basis": round(current_position_cost, 2),
                "principal_cash": adjusted_cash,
                "principal_basis": "position_cost_basis_plus_cash",
                "cash_adjustment_total": round(cash_adjustment_total, 2),
                "last_capital_adjustment": round(cash_delta, 2),
                "last_capital_adjustment_at": now,
                "positions_source": "qmt_account_strategy_symbols" if mode == "live" else existing_state.get("positions_source", "strategy_owned_only"),
                "price_source": "qmt_realtime_quotes" if mode == "live" else existing_state.get("price_source"),
                "updated_at": now,
            }
            async with async_session_factory() as session:
                account_row = await session.scalar(
                    select(LivePositionState).where(
                        LivePositionState.profile_key == bundle.profile.profile_key,
                        LivePositionState.mode == mode,
                        LivePositionState.symbol == STRATEGY_ACCOUNT_SYMBOL,
                    )
                )
                if account_row is None:
                    raise ValueError("策略资金池状态丢失，请重新打开页面后再试。")
                account_row.state = updated_state
                session.add(account_row)
                for symbol, state in position_states.items():
                    position_row = await session.scalar(
                        select(LivePositionState).where(
                            LivePositionState.profile_key == bundle.profile.profile_key,
                            LivePositionState.mode == mode,
                            LivePositionState.symbol == symbol,
                        )
                    )
                    if position_row is not None:
                        position_row.state = state
                        session.add(position_row)
                await session.commit()
            await self._write_control_audit(
                profile_key=bundle.profile.profile_key,
                strategy_id=bundle.profile.strategy_id,
                trade_date=date.today(),
                signal_hash=None,
                trigger_source="manual",
                mode=mode,
                run_id=None,
                status="strategy_account_adjusted",
                reason=f"strategy account target capital adjusted to {capital_value:.2f}",
                payload={
                    "stage": "strategy_account",
                    "action": "adjust_target_capital",
                    "previous_total_asset": current_total_asset,
                    "target_capital": capital_value,
                    "position_cost_basis": round(current_position_cost, 2),
                    "desired_cash": desired_cash,
                    "cash_delta": cash_delta,
                    "market_value": current_market_value,
                    "cash": adjusted_cash,
                    "total_asset": adjusted_total_asset,
                    "capital_basis": "position_cost_basis_plus_cash",
                },
            )
            return await self.account_snapshot(mode=mode, profile_key=bundle.profile.profile_key)

        async with async_session_factory() as session:
            if reset_existing:
                await session.execute(
                    delete(LivePositionState).where(
                        LivePositionState.profile_key == bundle.profile.profile_key,
                        LivePositionState.mode == mode,
                    )
                )
                existing = None
            elif existing is not None:
                existing = await session.scalar(
                    select(LivePositionState).where(
                        LivePositionState.profile_key == bundle.profile.profile_key,
                        LivePositionState.mode == mode,
                        LivePositionState.symbol == STRATEGY_ACCOUNT_SYMBOL,
                    )
                )
            now = datetime.now().isoformat(timespec="seconds")
            state = {
                "initialized": True,
                "account_scope": "strategy_pool",
                "profile_key": bundle.profile.profile_key,
                "mode": mode,
                "initial_capital": round(capital_value, 2),
                "target_capital": round(capital_value, 2),
                "cash": round(capital_value, 2),
                "market_value": 0.0,
                "total_asset": round(capital_value, 2),
                "position_cost_basis": 0.0,
                "principal_cash": round(capital_value, 2),
                "principal_basis": "position_cost_basis_plus_cash",
                "cash_adjustment_total": 0.0,
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
        normalized, filters = self._apply_live_execution_filter_time(
            params=normalized,
            filters=filters,
            mode=mode,
            trade_date=trade_date,
        )
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
        started_at = perf_counter()

        def log_stage(stage: str, **details: Any) -> None:
            logger.info(
                "Live signals stage={} profile={} mode={} trade_date={} elapsed={:.2f}s details={}",
                stage,
                bundle.profile.profile_key,
                mode,
                trade_date.isoformat(),
                perf_counter() - started_at,
                details,
            )

        log_stage("start", trigger_source=trigger_source, include_preflight=include_preflight)
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
        pending_sync: dict[str, Any] | None = None
        if mode == "live":
            pending_sync_timeout = max(
                1.0,
                min(30.0, float(normalized.get("pending_order_sync_timeout_seconds", 10.0) or 10.0)),
            )
            try:
                log_stage("pending_order_sync.start", timeout_seconds=pending_sync_timeout)
                pending_sync = await asyncio.wait_for(
                    self.sync_order_status(
                        profile_key=bundle.profile.profile_key,
                        mode="live",
                        limit=500,
                    ),
                    timeout=pending_sync_timeout,
                )
                log_stage(
                    "pending_order_sync.done",
                    pending_count=pending_sync.get("pending_count"),
                    updated_count=pending_sync.get("updated_count"),
                    synced=pending_sync.get("synced"),
                )
            except asyncio.TimeoutError:
                message = f"QMT pending order sync timed out after {pending_sync_timeout:.0f}s"
                pending_sync = {"synced": False, "timeout": True, "error": message}
                logger.warning("Live signal generation skips pending order sync: {}", message)
                log_stage("pending_order_sync.timeout", timeout_seconds=pending_sync_timeout)
            except Exception as exc:
                pending_sync = {"synced": False, "error": f"{type(exc).__name__}: {exc}"}
                logger.warning("Live signal generation skips pending order sync: {}", pending_sync["error"])
                log_stage("pending_order_sync.failed", error=pending_sync["error"])
        log_stage("account_snapshot.start")
        account = await self._account_snapshot(
            mode=mode,
            manual=manual_account,
            params=normalized,
            profile_key=bundle.profile.profile_key,
        )
        log_stage("account_snapshot.done", source=account.source, error=account.error, positions=len(account.positions))
        if mode == "live" and account.error:
            return await self._empty_signal_response(
                bundle=bundle,
                params=normalized,
                trade_date=trade_date,
                account=account,
                reason=f"实盘账户实时快照不可用：{account.error}",
                mode=mode,
                trigger_source=trigger_source,
                run_id=run_id,
                write_audit=write_audit,
                universe_size=0,
                preflight=None,
                factor_dates={},
            )
        positions = {
            symbol: float(position.get("quantity", 0.0) or 0.0)
            for symbol, position in account.positions.items()
        }
        pending_effect: dict[str, Any] = {"count": 0, "positions": {}, "cash_effect": 0.0}
        if mode == "live":
            log_stage("pending_order_effect.start")
            pending_effect = await self._pending_order_effect(profile_key=bundle.profile.profile_key, mode="live")
            log_stage("pending_order_effect.done", count=pending_effect.get("count"))
        log_stage("resolve_symbols.start")
        symbols = await self._resolve_symbols(bundle.profile, normalized, trade_date)
        log_stage("resolve_symbols.done", symbol_count=len(symbols))
        factor_configs = self._factor_configs(bundle, normalized)
        filters = self._filter_configs(bundle, normalized)
        normalized, filters = self._apply_live_execution_filter_time(
            params=normalized,
            filters=filters,
            mode=mode,
            trade_date=trade_date,
        )
        requirements, requirement_errors = self._factor_requirements(factor_configs, filters)
        effective_dates = self._factor_effective_dates(requirements, trade_date, symbols)
        preflight = None
        if include_preflight:
            log_stage("preflight.start", factor_count=len(factor_configs), filter_count=len(filters))
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
            log_stage(
                "preflight.done",
                can_generate=preflight.get("can_generate"),
                blocking_count=len(preflight.get("blocking_reasons") or []),
                coverage_count=len(preflight.get("factor_coverage") or []),
            )
            log_stage("intraday_prepare.start")
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
                log_stage=log_stage,
            )
            log_stage(
                "intraday_prepare.done",
                attempted=prepare_result.get("attempted"),
                status=prepare_result.get("status"),
            )
            if prepare_result.get("attempted"):
                log_stage("preflight_refresh.start")
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
                log_stage(
                    "preflight_refresh.done",
                    can_generate=preflight.get("can_generate"),
                    blocking_count=len(preflight.get("blocking_reasons") or []),
                    coverage_count=len(preflight.get("factor_coverage") or []),
                )
                effective_dates = self._factor_effective_dates(requirements, trade_date, symbols)
            signal_blocks = self._signal_blocking_reasons(preflight)
            if signal_blocks:
                log_stage("blocked", reasons=signal_blocks[:5])
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
        log_stage("pipeline.start", symbol_count=len(symbols), factor_count=len(factor_configs), filter_count=len(filters))
        result = await run_blocking(
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
        log_stage("pipeline.done", raw_count=len(result.raw), candidate_count=len(result.frame), excluded_count=len(result.excluded_symbols))
        frame = self._apply_theme_filter(result.frame, normalized)
        frame, heat_note = self._apply_limit_up_heat_filter(frame, normalized, trade_date)
        if frame.empty:
            log_stage("empty_after_filters", raw_count=len(result.raw), excluded_count=len(result.excluded_symbols))
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
        quote_symbols = sorted(set(target_symbols) | set(positions) | set(pending_effect.get("positions", {})))
        log_stage("quote.start", quote_symbol_count=len(quote_symbols), target_count=len(target_symbols))
        price_map, quote_error = await self._quote_prices(quote_symbols)
        log_stage("quote.done", price_count=len(price_map), quote_error=quote_error)
        if mode == "live":
            missing_quote_symbols = self._missing_realtime_price_symbols(quote_symbols, price_map)
            if quote_error or missing_quote_symbols:
                log_stage("quote.blocked", missing_count=len(missing_quote_symbols), quote_error=quote_error)
                reason = quote_error or f"实盘实时行情缺失：{', '.join(missing_quote_symbols[:10])}"
                return await self._empty_signal_response(
                    bundle=bundle,
                    params=normalized,
                    trade_date=trade_date,
                    account=account,
                    reason=reason,
                    mode=mode,
                    trigger_source=trigger_source,
                    run_id=run_id,
                    write_audit=write_audit,
                    universe_size=len(symbols),
                    preflight=preflight,
                    factor_dates=effective_dates,
                )
        else:
            price_map.update(self._position_price_fallbacks(account))
        account_for_orders, positions_for_orders, pending_adjustment = self._apply_pending_order_effect(
            account,
            positions,
            pending_effect,
            price_map,
        )

        target_weight = self._target_weight(target_symbols, normalized)
        target_weights = {symbol: target_weight for symbol in target_symbols if target_weight > 0}
        portfolio_value = account_for_orders.total_asset or account_for_orders.cash or float(normalized.get("initial_capital", 1_000_000) or 1_000_000)
        entry_state = self._entry_filter_state(trade_date, normalized)
        filtered_weights, entry_state = apply_entry_filter_to_target_weights(
            target_weights,
            current_positions=positions_for_orders,
            price_map=price_map,
            portfolio_value=portfolio_value,
            entry_filter_state=entry_state,
        )
        orders, skipped_orders = self._build_cash_aware_orders(
            target_weights=filtered_weights,
            positions=positions_for_orders,
            price_map=price_map,
            account=account_for_orders,
            params=normalized,
            portfolio_value=portfolio_value,
            bundle=bundle,
            ranked_symbols=target_symbols,
        )
        await self._attach_stock_names([*orders, *skipped_orders])
        signal_hash = self._signal_hash(bundle.profile.profile_key, bundle.profile.strategy_id, trade_date, orders)
        for order in orders:
            order["signal_hash"] = signal_hash
        for skipped in skipped_orders:
            skipped["signal_hash"] = signal_hash

        if write_audit:
            log_stage("audit.start", order_count=len(orders), skipped_count=len(skipped_orders))
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
            log_stage("audit.done", order_count=len(orders), skipped_count=len(skipped_orders))

        log_stage("done", order_count=len(orders), skipped_count=len(skipped_orders), candidate_count=len(frame))
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
            "pending_order_sync": pending_sync,
            "pending_order_adjustment": pending_adjustment,
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
        order_list = self._submission_order_sort([dict(order) for order in orders])
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

    async def sync_order_status(
        self,
        *,
        profile_key: str | None = None,
        mode: str = "live",
        limit: int = 200,
        record_ids: Sequence[str] | None = None,
        order_ids: Sequence[str | int] | None = None,
    ) -> dict[str, Any]:
        if mode != "live":
            return {"synced": False, "mode": mode, "updated_count": 0, "pending_count": 0, "orders": [], "trades": []}
        pending_rows = await self._load_live_trade_rows(
            profile_key=profile_key,
            mode=mode,
            limit=limit,
            record_ids=record_ids,
            order_ids=order_ids,
            only_pending=True,
        )
        if not pending_rows:
            return {"synced": True, "mode": mode, "updated_count": 0, "pending_count": 0, "orders": [], "trades": []}

        wanted_order_ids = [row.order_id for row in pending_rows if row.order_id]
        try:
            qmt_updates = await qmt_trading_service.query_order_updates(wanted_order_ids)
        except Exception as exc:
            return {
                "synced": False,
                "mode": mode,
                "updated_count": 0,
                "pending_count": len(pending_rows),
                "error": f"{type(exc).__name__}: {exc}",
                "orders": [],
                "trades": [],
            }

        qmt_by_order = qmt_updates.get("by_order_id") or {}
        updated_count = 0
        fill_events: list[dict[str, Any]] = []
        locally_closed: list[dict[str, Any]] = []
        fills_to_apply: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = []
        profiles_to_reconcile: set[str] = set()
        async with async_session_factory() as session:
            for row in pending_rows:
                if not row.order_id:
                    continue
                update_data = dict(qmt_by_order.get(str(row.order_id)) or {})
                if not update_data:
                    if str(row.status or "") == "cancel_requested":
                        closed = await self._close_live_trade_record_in_session(
                            session,
                            row,
                            status="cancelled",
                            trigger_source="qmt_sync_missing_after_cancel",
                            message="QMT 未返回该撤单中委托，按客户端已撤单关闭。",
                            reason="missing_from_qmt_after_cancel_request",
                        )
                        if closed:
                            locally_closed.append(closed)
                            profiles_to_reconcile.add(row.profile_key)
                            updated_count += 1
                    continue
                order_payload = self._json_dict(row.order_payload)
                result_payload = self._json_dict(row.result_payload)
                previous_filled = float(result_payload.get("filled_quantity") or 0.0)
                qmt_filled = float(update_data.get("filled_quantity") or 0.0)
                delta_filled = max(0.0, qmt_filled - previous_filled)
                update_status = str(update_data.get("status") or "live_pending")
                previous_status = str(row.status or "")
                order_quantity = float(order_payload.get("quantity") or row.quantity or 0.0)
                if qmt_filled >= order_quantity > 0:
                    next_status = "live_filled"
                elif update_status in LIVE_CANCELLED_STATUSES:
                    next_status = "partially_cancelled" if qmt_filled > 0 else "cancelled"
                elif qmt_filled > 0:
                    next_status = "partially_filled"
                else:
                    next_status = update_status
                if previous_status == "cancel_requested" and qmt_filled <= 0 and next_status in LIVE_PENDING_STATUSES:
                    next_status = "cancel_requested"

                filled_price = float(update_data.get("filled_price") or order_payload.get("reference_price") or row.reference_price or 0.0)
                filled_value = float(update_data.get("filled_value") or (qmt_filled * filled_price))
                if delta_filled > 0 and filled_price > 0:
                    fill_order = dict(order_payload)
                    fill_order["quantity"] = delta_filled
                    fill_order["reference_price"] = filled_price
                    fill_order["price"] = filled_price
                    fill_result = {
                        "submitted": True,
                        "paper": False,
                        "status": next_status,
                        "filled_quantity": delta_filled,
                        "filled_price": filled_price,
                        "filled_value": round(delta_filled * filled_price, 2),
                        "realized_pnl": 0.0,
                        "message": "QMT 成交回报同步",
                        "updated_from": "qmt_trade_sync",
                        "account_snapshot": None,
                    }
                    fills_to_apply.append((row.profile_key, row.mode, fill_order, fill_result))
                    fill_events.append(
                        {
                            "record_id": row.record_id,
                            "order_id": row.order_id,
                            "filled_quantity": delta_filled,
                            "filled_price": filled_price,
                            "filled_value": round(delta_filled * filled_price, 2),
                        }
                    )

                db_row = await session.scalar(select(LiveTradeRecord).where(LiveTradeRecord.record_id == row.record_id))
                if db_row is None:
                    continue
                db_row.status = next_status
                db_row.message = (
                    str(update_data.get("status_msg") or "")
                    or ("QMT 已成交" if next_status == "live_filled" else "QMT 部分成交" if next_status == "partially_filled" else "QMT 已撤单" if next_status == "cancelled" else db_row.message)
                )
                if filled_price > 0:
                    db_row.reference_price = Decimal(str(round(filled_price, 4)))
                if next_status in LIVE_FILLED_STATUSES:
                    db_row.quantity = Decimal(str(qmt_filled or order_quantity or row.quantity or 0))
                    db_row.order_value = Decimal(str(round(filled_value, 2)))
                result_payload.update(
                    {
                        "submitted": True,
                        "pending": next_status in LIVE_PENDING_STATUSES,
                        "status": next_status,
                        "order_id": update_data.get("order_id") or db_row.order_id,
                        "message": db_row.message,
                        "filled_quantity": qmt_filled,
                        "filled_price": filled_price,
                        "filled_value": round(filled_value, 2),
                        "remaining_quantity": max(0.0, order_quantity - qmt_filled),
                        "order_status": update_data.get("order_status"),
                        "status_msg": update_data.get("status_msg"),
                        "last_trade_time": update_data.get("last_trade_time"),
                    }
                )
                db_row.result_payload = result_payload
                session.add(db_row)
                profiles_to_reconcile.add(db_row.profile_key)
                session.add(
                    LiveOrderAudit(
                        audit_id=f"audit-{uuid.uuid4().hex}",
                        run_id=db_row.run_id,
                        profile_key=db_row.profile_key,
                        strategy_id=db_row.strategy_id,
                        trade_date=db_row.trade_date,
                        signal_hash=db_row.signal_hash,
                        trigger_source="qmt_sync",
                        mode=db_row.mode,
                        status=str(db_row.status),
                        order_payload=self._json_dict(db_row.order_payload),
                        result_payload=self._json_dict(db_row.result_payload),
                        skip_reason=db_row.message,
                    )
                )
                updated_count += 1
            await session.commit()

        account_reconcile_errors: list[str] = []
        for fill_profile, fill_mode, fill_order, fill_result in fills_to_apply:
            try:
                await self._apply_strategy_account_fill(
                    profile_key=fill_profile,
                    mode=fill_mode,
                    order=fill_order,
                    result=fill_result,
                )
            except Exception as exc:
                message = f"{fill_profile}: {type(exc).__name__}: {exc}"
                account_reconcile_errors.append(message)
                logger.warning("Live order sync applied trade status but failed to update strategy account: {}", message)

        for profile in sorted(profiles_to_reconcile):
            try:
                await self._reconcile_strategy_account_from_trade_records(profile_key=profile, mode=mode)
            except Exception as exc:
                message = f"{profile}: {type(exc).__name__}: {exc}"
                account_reconcile_errors.append(message)
                logger.warning("Live order sync status updated but account reconcile failed: {}", message)

        remaining = await self._load_live_trade_rows(profile_key=profile_key, mode=mode, limit=limit, only_pending=True)
        return {
            "synced": True,
            "mode": mode,
            "updated_count": updated_count,
            "pending_count": len(remaining),
            "filled_count": len(fill_events),
            "orders": qmt_updates.get("orders") or [],
            "trades": qmt_updates.get("trades") or [],
            "fill_events": fill_events,
            "locally_closed": locally_closed,
            "account_reconcile_errors": account_reconcile_errors,
        }

    async def close_local_pending_orders(
        self,
        *,
        profile_key: str | None = None,
        mode: str = "live",
        limit: int = 200,
        record_ids: Sequence[str] | None = None,
        order_ids: Sequence[str | int] | None = None,
        reason: str = "client_cancelled",
        confirm: bool = False,
    ) -> dict[str, Any]:
        if mode != "live":
            raise ValueError("local close is only supported in live mode")
        pending_rows = await self._load_live_trade_rows(
            profile_key=profile_key,
            mode=mode,
            limit=limit,
            record_ids=record_ids,
            order_ids=order_ids,
            only_pending=True,
        )
        if not pending_rows:
            return {"closed": False, "closed_count": 0, "orders": [], "message": "没有待关闭的实盘委托"}
        if not confirm:
            return {
                "closed": False,
                "closed_count": len(pending_rows),
                "orders": [self._pending_order_dict(row) for row in pending_rows],
                "message": "本地关闭需要 confirm=true；请先确认这些委托已在 QMT 客户端撤掉或不会成交。",
            }

        closed: list[dict[str, Any]] = []
        profiles_to_reconcile: set[str] = set()
        message = str(reason or "client_cancelled")
        async with async_session_factory() as session:
            for row in pending_rows:
                item = await self._close_live_trade_record_in_session(
                    session,
                    row,
                    status="cancelled",
                    trigger_source="manual_local_close",
                    message=f"本地关闭待确认委托：{message}",
                    reason=message,
                )
                if item:
                    closed.append(item)
                    profiles_to_reconcile.add(row.profile_key)
            await session.commit()

        account_reconcile_errors: list[str] = []
        for profile in sorted(profiles_to_reconcile):
            try:
                await self._reconcile_strategy_account_from_trade_records(profile_key=profile, mode=mode)
            except Exception as exc:
                error = f"{profile}: {type(exc).__name__}: {exc}"
                account_reconcile_errors.append(error)
                logger.warning("Local close updated trade records but account reconcile failed: {}", error)
        remaining = await self._load_live_trade_rows(profile_key=profile_key, mode=mode, limit=limit, only_pending=True)
        return {
            "closed": bool(closed),
            "closed_count": len(closed),
            "records": closed,
            "orders": [self._pending_order_dict(row) for row in remaining],
            "account_reconcile_errors": account_reconcile_errors,
        }

    async def cancel_pending_orders(
        self,
        *,
        profile_key: str | None = None,
        mode: str = "live",
        limit: int = 200,
        min_age_seconds: int = 0,
        record_ids: Sequence[str] | None = None,
        order_ids: Sequence[str | int] | None = None,
        confirm: bool = False,
    ) -> dict[str, Any]:
        if mode != "live":
            raise ValueError("cancel is only supported in live mode")
        pending_rows = await self._load_live_trade_rows(
            profile_key=profile_key,
            mode=mode,
            limit=limit,
            record_ids=record_ids,
            order_ids=order_ids,
            only_pending=True,
            min_age_seconds=min_age_seconds,
        )
        cancel_targets = [row for row in pending_rows if row.order_id]
        if not cancel_targets:
            return {"cancelled": False, "cancel_count": 0, "orders": [], "message": "没有可撤销的待成交真实委托"}
        if not confirm:
            return {
                "cancelled": False,
                "cancel_count": len(cancel_targets),
                "orders": [self._pending_order_dict(row) for row in cancel_targets],
                "message": "批量撤单需要 confirm=true。",
            }

        results: list[dict[str, Any]] = []
        cancel_requested_records: list[str] = []
        for row in cancel_targets:
            result = await qmt_trading_service.cancel_order(row.order_id)
            if result.get("cancelled"):
                result["status"] = "cancel_requested"
                cancel_requested_records.append(row.record_id)
            results.append(result)
            await self._write_control_audit(
                profile_key=row.profile_key,
                strategy_id=row.strategy_id,
                trade_date=row.trade_date,
                signal_hash=row.signal_hash,
                trigger_source="manual_cancel",
                mode=mode,
                run_id=row.run_id,
                status="cancel_requested" if result.get("cancelled") else "cancel_failed",
                reason=str(result.get("message") or ""),
                payload={"order_id": row.order_id, "record_id": row.record_id, "result": result},
            )
        if cancel_requested_records:
            async with async_session_factory() as session:
                rows = (
                    await session.execute(
                        select(LiveTradeRecord).where(LiveTradeRecord.record_id.in_(cancel_requested_records))
                    )
                ).scalars().all()
                for row in rows:
                    payload = self._json_dict(row.result_payload)
                    payload.update({"status": "cancel_requested", "pending": True, "message": "已向 QMT 发送撤单请求，等待确认。"})
                    row.status = "cancel_requested"
                    row.message = "已向 QMT 发送撤单请求，等待确认。"
                    row.result_payload = payload
                    session.add(row)
                await session.commit()
        sync_result = await self.sync_order_status(
            profile_key=profile_key,
            mode=mode,
            limit=limit,
            record_ids=[row.record_id for row in cancel_targets],
            order_ids=[row.order_id for row in cancel_targets if row.order_id],
        )
        remaining = await self._load_live_trade_rows(
            profile_key=profile_key,
            mode=mode,
            limit=limit,
            only_pending=True,
            min_age_seconds=min_age_seconds,
        )
        return {
            "cancelled": all(bool(item.get("cancelled")) for item in results),
            "cancel_count": len(results),
            "results": results,
            "sync_result": sync_result,
            "orders": [self._pending_order_dict(row) for row in remaining],
        }

    async def cancel_and_resubmit_pending_orders(
        self,
        *,
        profile_key: str | None = None,
        mode: str = "live",
        params: dict[str, Any] | None = None,
        limit: int = 200,
        min_age_seconds: int = 0,
        record_ids: Sequence[str] | None = None,
        order_ids: Sequence[str | int] | None = None,
        confirm_cancel: bool = False,
        confirm_submit: bool = False,
    ) -> dict[str, Any]:
        cancel_result = await self.cancel_pending_orders(
            profile_key=profile_key,
            mode=mode,
            limit=limit,
            min_age_seconds=min_age_seconds,
            record_ids=record_ids,
            order_ids=order_ids,
            confirm=confirm_cancel,
        )
        if not confirm_cancel:
            return {"cancel_result": cancel_result, "submitted": False, "message": cancel_result.get("message")}
        post_cancel_pending = cancel_result.get("orders") or []
        if post_cancel_pending:
            return {
                "cancel_result": cancel_result,
                "submitted": False,
                "message": "QMT 撤单尚未完全确认，先等待状态同步后再重提。",
            }
        signal_result = await self.signals(
            profile_key=profile_key,
            mode=mode,
            params=params or {},
            trigger_source="manual",
            include_preflight=True,
        )
        orders = list(signal_result.get("orders") or [])
        if not orders:
            return {
                "cancel_result": cancel_result,
                "signal_result": signal_result,
                "submitted": False,
                "message": "撤单完成，但当前没有新的差额订单需要提交。",
            }
        submit_result = {"submitted": False, "results": [], "message": "未确认提交"}
        if confirm_submit:
            submit_result = await self.submit_orders(
                orders,
                mode=mode,
                confirm=True,
                trigger_source="manual",
            )
        return {
            "cancel_result": cancel_result,
            "signal_result": signal_result,
            "submit_result": submit_result,
            "submitted": bool(submit_result.get("submitted")),
            "message": submit_result.get("message") or "撤单并重提完成",
        }

    async def list_audits(self, *, limit: int = 100, profile_key: str | None = None, mode: str | None = None) -> list[dict[str, Any]]:
        async with async_session_factory() as session:
            stmt = select(LiveOrderAudit)
            if profile_key:
                stmt = stmt.where(LiveOrderAudit.profile_key == profile_key)
            if mode:
                stmt = stmt.where(LiveOrderAudit.mode == mode)
            stmt = stmt.order_by(LiveOrderAudit.created_at.desc()).limit(max(1, min(500, int(limit or 100))))
            rows = (await session.execute(stmt)).scalars().all()
        items = [self._audit_dict(row) for row in rows]
        missing_name_symbols = [
            str(item.get("symbol") or "")
            for item in items
            if item.get("symbol") and not item.get("stock_name")
        ]
        name_map = await self._stock_names(missing_name_symbols)
        for item in items:
            symbol = str(item.get("symbol") or "")
            if symbol and not item.get("stock_name"):
                item["stock_name"] = name_map.get(symbol)
        return items

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
        sync: bool = True,
    ) -> list[dict[str, Any]]:
        if sync and mode == "live":
            await self.sync_order_status(profile_key=profile_key, mode=mode, limit=limit)
        rows = await self._load_live_trade_rows(profile_key=profile_key, mode=mode, limit=limit, only_pending=True)
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
                {"trade_date": day_key, "records": 0, "buy_notional": 0.0, "sell_notional": 0.0, "cancelled": 0, "failed": 0, "paper_realized_pnl": 0.0},
            )
            day_row["records"] += 1
            if status in completed_statuses and side == "BUY":
                day_row["buy_notional"] += value
            if status in completed_statuses and side == "SELL":
                day_row["sell_notional"] += value
            if status in LIVE_CANCELLED_STATUSES:
                day_row["cancelled"] += 1
            if status not in completed_statuses and status not in LIVE_PENDING_STATUSES and status not in LIVE_CANCELLED_STATUSES:
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
            and str(record.get("status") or "") not in LIVE_CANCELLED_STATUSES
        )
        cancelled_count = sum(1 for record in records if str(record.get("status") or "") in LIVE_CANCELLED_STATUSES)
        notes = []
        if live_submitted_count:
            notes.append("live_pending 表示 QMT 委托提交成功，但尚未在平台内确认成交；请结合 QMT 当日委托/成交核对。")
        if cancelled_count:
            notes.append(f"本周有 {cancelled_count} 条 QMT 撤单/部成撤单记录，未成交部分不会进入策略资金池。")
        if failed_count:
            notes.append(f"本周有 {failed_count} 条提交失败记录，需要查看 message 或订单审计。")
        if not records:
            notes.append("本周暂无独立交易记录。")

        current_account: dict[str, Any] | None = None
        if profile_key:
            try:
                current_account = await self.account_snapshot(
                    mode=mode or "live",
                    profile_key=profile_key,
                    include_broker=False,
                    record_equity=False,
                )
            except Exception as exc:
                notes.append(f"权益快照读取失败：{type(exc).__name__}: {exc}")

        weekly_snapshots_raw = await self._load_equity_snapshots(
            profile_key=profile_key,
            mode=mode,
            start_date=start,
            end_date=end,
        )
        all_time_snapshots_raw = await self._load_equity_snapshots(
            profile_key=profile_key,
            mode=mode,
            end_date=end,
        )
        weekly_snapshots = self._usable_equity_snapshots(weekly_snapshots_raw)
        all_time_snapshots = self._usable_equity_snapshots(all_time_snapshots_raw)
        weekly_equity = self._equity_metrics(weekly_snapshots)
        all_time_equity = self._equity_metrics(all_time_snapshots)
        current_total_pnl = self._float_or_none((current_account or {}).get("total_pnl"))
        current_total_pnl_pct = self._float_or_none((current_account or {}).get("total_pnl_pct"))
        current_meta = self._json_dict((current_account or {}).get("meta"))
        initialized_date = self._parse_date(str(current_meta.get("initialized_at") or "")[:10]) if current_meta.get("initialized_at") else None
        weekly_pnl = weekly_equity.get("pnl")
        weekly_return_pct = weekly_equity.get("return_pct")
        if initialized_date and start <= initialized_date <= end and current_total_pnl is not None:
            weekly_pnl = round(current_total_pnl, 2)
            weekly_return_pct = round(current_total_pnl_pct, 4) if current_total_pnl_pct is not None else None
        filtered_weekly = len(weekly_snapshots_raw) - len(weekly_snapshots)
        filtered_all_time = len(all_time_snapshots_raw) - len(all_time_snapshots)
        if filtered_weekly:
            notes.append(f"已过滤 {filtered_weekly} 个 QMT 半截/异常权益快照，避免把账户读取失败当作真实回撤。")
        if initialized_date and start <= initialized_date <= end:
            notes.append("资金池在本周内建立，本周 PnL 与至今 PnL 使用同一净投入本金口径。")
        if len(weekly_snapshots) < 2:
            notes.append("权益快照不足，周内 PnL 和回撤基于当前可用快照。")
        if len(all_time_snapshots) < 2:
            notes.append("至今回撤快照不足，后续每天/每次刷新账户后会逐步补齐。")

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
                "cancelled_records": cancelled_count,
                "buy_notional": round(buy_notional, 2),
                "sell_notional": round(sell_notional, 2),
                "net_notional": round(buy_notional - sell_notional, 2),
                "paper_realized_pnl": round(paper_realized_pnl, 2),
                "live_submitted_records": live_submitted_count,
                "weekly_pnl": weekly_pnl,
                "weekly_return_pct": weekly_return_pct,
                "weekly_max_drawdown_pct": weekly_equity.get("max_drawdown_pct"),
                "all_time_pnl": round(current_total_pnl, 2) if current_total_pnl is not None else all_time_equity.get("pnl"),
                "all_time_return_pct": (
                    round(current_total_pnl_pct, 4)
                    if current_total_pnl_pct is not None
                    else all_time_equity.get("return_pct")
                ),
                "all_time_max_drawdown_pct": all_time_equity.get("max_drawdown_pct"),
                "weekly_equity_snapshot_points": weekly_equity.get("snapshot_points", 0),
                "all_time_equity_snapshot_points": all_time_equity.get("snapshot_points", 0),
                "weekly_equity_snapshot_raw_points": len(weekly_snapshots_raw),
                "all_time_equity_snapshot_raw_points": len(all_time_snapshots_raw),
                "weekly_equity_snapshot_filtered_points": filtered_weekly,
                "all_time_equity_snapshot_filtered_points": filtered_all_time,
                "equity_snapshot_points": {
                    "weekly": weekly_equity.get("snapshot_points", 0),
                    "all_time": all_time_equity.get("snapshot_points", 0),
                },
            },
            "by_day": sorted(by_day.values(), key=lambda item: item["trade_date"]),
            "by_profile": sorted(by_profile.values(), key=lambda item: item["notional"], reverse=True),
            "by_status": [{"status": key, "records": value} for key, value in sorted(by_status.items())],
            "by_side": list(by_side.values()),
            "top_symbols": sorted(by_symbol.values(), key=lambda item: item["notional"], reverse=True)[:10],
            "equity_curve": self._equity_curve(all_time_snapshots),
            "weekly_equity_curve": self._equity_curve(weekly_snapshots),
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

    def _apply_live_execution_filter_time(
        self,
        *,
        params: dict[str, Any],
        filters: list[Any],
        mode: str,
        trade_date: date,
    ) -> tuple[dict[str, Any], list[Any]]:
        if mode != "live" or bool(params.get("disable_current_execution_filter_time", False)):
            return params, filters

        timer_text = self._live_execution_filter_time(params=params, filters=filters, trade_date=trade_date)
        if not timer_text:
            return params, filters

        adjusted_filters: list[Any] = []
        adjusted = False
        for raw in filters:
            name = self._raw_factor_name(raw)
            if name not in LIVE_TIMER_STATUS_FACTORS:
                adjusted_filters.append(raw)
                continue
            item = dict(raw) if isinstance(raw, dict) else {"name": name}
            item["as_of_time"] = timer_text
            raw_params = item.get("params")
            item_params = dict(raw_params) if isinstance(raw_params, dict) else {}
            item_params["time"] = timer_text
            item["params"] = item_params
            adjusted_filters.append(item)
            adjusted = True

        if not adjusted:
            return params, filters

        adjusted_params = dict(params)
        adjusted_params["live_execution_filter_time"] = timer_text
        adjusted_params["live_execution_filter_time_source"] = "current_intraday"
        return adjusted_params, adjusted_filters

    def _live_execution_filter_time(self, *, params: dict[str, Any], filters: list[Any], trade_date: date) -> str | None:
        override = params.get("live_execution_filter_time") or params.get("execution_filter_time")
        if override:
            return normalize_factor_time(str(override))
        now = datetime.now()
        if trade_date != now.date():
            return None
        minute = self._latest_completed_trading_minute(now)
        if minute is None:
            return None
        timer_text = self._snap_to_nearest_timer_time(minute, params, filters)
        if timer_text is not None:
            return timer_text
        return f"{minute.hour:02d}:{minute.minute:02d}"

    @staticmethod
    def _snap_to_nearest_timer_time(current: time, params: dict[str, Any], filters: list[Any] | None = None) -> str | None:
        raw_values: list[Any] = []
        for key in ("timer_times", "timer_minute_times", "minute_timer_times"):
            raw = params.get(key)
            if raw:
                raw_values.extend(raw if isinstance(raw, (list, tuple)) else [raw])
        raw_values.extend(LiveTradingService._timer_values_from_filters(filters or []))
        if not raw_values:
            return None
        timer_times: list[time] = []
        for item in raw_values:
            try:
                parts = str(item).strip().split(":")
                h, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
                t = time(h, m)
                if t not in timer_times:
                    timer_times.append(t)
            except (ValueError, IndexError):
                continue
        timer_times.sort()
        current_seconds = current.hour * 3600 + current.minute * 60
        best: time | None = None
        for tt in timer_times:
            tt_seconds = tt.hour * 3600 + tt.minute * 60
            if tt_seconds <= current_seconds:
                best = tt
            else:
                break
        if best is not None:
            return f"{best.hour:02d}:{best.minute:02d}"
        return None

    @staticmethod
    def _timer_values_from_filters(filters: list[Any]) -> list[str]:
        values: list[str] = []
        for raw in filters:
            if not isinstance(raw, dict):
                continue
            name = LiveTradingService._raw_factor_name(raw)
            if name not in LIVE_TIMER_STATUS_FACTORS:
                continue
            raw_params = raw.get("params")
            params = raw_params if isinstance(raw_params, dict) else {}
            for value in (raw.get("as_of_time"), params.get("time"), params.get("as_of_time")):
                if value:
                    values.append(str(value))
        return values

    @staticmethod
    def _latest_completed_trading_minute(value: datetime) -> time | None:
        current = value.time()
        if current < time(9, 31):
            return None
        if current <= time(11, 30):
            return time(value.hour, value.minute)
        if current < time(13, 1):
            return time(11, 30)
        if current <= time(15, 0):
            return time(value.hour, value.minute)
        return time(15, 0)

    @staticmethod
    def _raw_factor_name(raw: Any) -> str:
        if isinstance(raw, str):
            return raw.strip()
        if isinstance(raw, dict):
            return str(raw.get("name") or raw.get("factor_name") or "").strip()
        return ""

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
        try:
            coverage_by_name = store.coverage_many(
                names,
                start_date=trade_date - timedelta(days=370),
                end_date=trade_date - timedelta(days=1),
                symbols=symbols or None,
            )
        except Exception as exc:
            logger.warning("Failed to load live factor latest dates in batch: {}", exc)
            return None

        latest_dates: list[date] = []
        market_cap_date: date | None = None
        for name in names:
            coverage = coverage_by_name.get(name) or {}
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
        grouped: dict[tuple[date, str | None], list[dict[str, Any]]] = {}
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
            group_time = str(req.get("as_of_time") or "") or None
            grouped.setdefault((effective_date, group_time), []).append(req)
        return [
            (key[0], value)
            for key, value in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1] or ""))
        ]

    def _prepare_params_for_requirements(
        self,
        params: dict[str, Any],
        requirements: list[dict[str, Any]],
    ) -> dict[str, Any]:
        prepare_params = dict(params)
        times = sorted({
            str(req.get("as_of_time") or "")
            for req in requirements
            if str(req.get("as_of_time") or "").strip()
        })
        if times:
            timer_text = times[0]
            prepare_params["time"] = timer_text
            prepare_params["as_of_time"] = timer_text
        else:
            prepare_params.setdefault("time", params.get("rebalance_time") or "10:30")
        return prepare_params

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
        execution_filter_time = self._execution_filter_time_from_requirements(requirements)
        effective_dates = self._factor_effective_dates(requirements, trade_date, symbols)

        dependency_prepare: dict[str, Any] | None = None
        dependency_error: str | None = None
        if required_names:
            prepare_groups = self._requirements_by_effective_date(requirements, effective_dates)
            try:
                prepared_parts: list[dict[str, Any]] = []
                for effective_date, grouped_requirements in prepare_groups:
                    prepare_params = self._prepare_params_for_requirements(params, grouped_requirements)
                    prepared_parts.append(
                        await run_blocking(
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
            factor_coverage = await run_blocking(
                self._factor_coverage_snapshot,
                requirements,
                trade_date,
                symbols,
                effective_dates,
            )

        pipeline_probe: dict[str, Any] | None = None
        if evaluate_pipeline and factor_configs and symbols:
            try:
                pipeline_probe = await run_blocking(
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
                "execution_filter_time": execution_filter_time,
                "execution_filter_time_source": params.get("live_execution_filter_time_source"),
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

    @staticmethod
    def _execution_filter_time_from_requirements(requirements: list[dict[str, Any]]) -> str | None:
        times = sorted({
            str(item.get("as_of_time") or "")
            for item in requirements
            if str(item.get("name") or "") in LIVE_TIMER_STATUS_FACTORS
            and str(item.get("as_of_time") or "").strip()
        })
        return times[0] if times else None

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
        grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for req in requirements:
            name = str(req["name"])
            params = req.get("params")
            as_of_time = req.get("as_of_time")
            effective_date = req_dates.get(name)
            if not isinstance(effective_date, date):
                effective_date = self._parse_date(effective_date)
            if effective_date is None:
                effective_date = trade_date
            params_hash = str(req.get("params_hash") or "")
            params_key = json.dumps(params or {}, sort_keys=True, ensure_ascii=False, default=str)
            group_key = (effective_date.isoformat(), str(as_of_time or ""), params_hash, params_key)
            group = grouped.setdefault(
                group_key,
                {
                    "effective_date": effective_date,
                    "as_of_time": as_of_time,
                    "params": params,
                    "requirements": [],
                },
            )
            group["requirements"].append(req)

        for group in grouped.values():
            effective_date = group["effective_date"]
            as_of_time = group["as_of_time"]
            params = group["params"]
            grouped_requirements = list(group["requirements"])
            names = sorted({str(req["name"]) for req in grouped_requirements})
            try:
                coverage_by_name = store.coverage_many(
                    names,
                    start_date=effective_date,
                    end_date=effective_date,
                    symbols=symbols or None,
                    as_of_time=as_of_time,
                    params=params,
                )
            except Exception as exc:
                coverage_by_name = {}
                error = f"{type(exc).__name__}: {exc}"
                logger.warning("Failed to load live factor coverage batch for {}: {}", names, error)
            else:
                error = None

            for req in grouped_requirements:
                name = str(req["name"])
                coverage = coverage_by_name.get(name) or {}
                count = int(coverage.get("symbol_count") or coverage.get("total_rows") or 0)
                row = {
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
                if error is not None:
                    row.update(
                    {
                        "value_count": 0,
                        "coverage_ratio": 0.0,
                        "status": "error",
                        "error": error,
                    }
                    )
                rows.append(row)
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
        log_stage: Callable[..., None] | None = None,
    ) -> dict[str, Any]:
        def report(stage: str, **details: Any) -> None:
            if log_stage is not None:
                log_stage(stage, **details)

        def progress_logger(prefix: str, timer_text: str, factor_names: list[str]) -> Callable[[float, str, dict[str, Any]], None]:
            last_reported = -1.0

            def report_progress(progress: float, stage: str, meta: dict[str, Any]) -> None:
                nonlocal last_reported
                if progress >= 1.0 or last_reported < 0 or progress - last_reported >= 0.2:
                    last_reported = progress
                    report(
                        f"intraday_prepare.{prefix}.progress",
                        timer_time=timer_text,
                        factor_names=factor_names,
                        progress=round(progress, 2),
                        precompute_stage=stage,
                        meta=meta,
                    )

            return report_progress

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
                    report(
                        "intraday_prepare.status_precompute.start",
                        timer_time=timer_text,
                        factor_names=status_names,
                        symbol_count=len(symbols),
                    )
                    result = await run_blocking(
                        precompute_live_timer_status_features,
                        start_date=trade_date,
                        end_date=trade_date,
                        symbols=symbols,
                        timer_time=timer_text,
                        factor_names=status_names,
                        progress_callback=progress_logger("status_precompute", timer_text, status_names),
                    )
                    report(
                        "intraday_prepare.status_precompute.done",
                        timer_time=timer_text,
                        factor_names=status_names,
                        rows_written=result.get("rows_written"),
                        rows=result.get("rows"),
                    )
                    results.append({"timer_time": timer_text, "factor_names": status_names, "status": "completed", "result": result})
                except Exception as exc:
                    message = f"{timer_text} 状态因子准备失败: {type(exc).__name__}: {exc}"
                    report(
                        "intraday_prepare.status_precompute.failed",
                        timer_time=timer_text,
                        factor_names=status_names,
                        error=message,
                    )
                    errors.append(message)
                    results.append({"timer_time": timer_text, "factor_names": status_names, "status": "failed", "error": message})
            if high_volume_names:
                try:
                    report(
                        "intraday_prepare.high_volume_precompute.start",
                        timer_time=timer_text,
                        factor_names=high_volume_names,
                        symbol_count=len(symbols),
                    )
                    result = await run_blocking(
                        precompute_high_volume_features,
                        start_date=trade_date,
                        end_date=trade_date,
                        symbols=symbols,
                        as_of_time=timer_text,
                        window=int(params.get("high_volume_window") or params.get("window") or 120),
                        threshold=float(params.get("high_volume_threshold") or params.get("threshold") or 0.9),
                        daily_volume_to_share_multiplier=float(params.get("daily_volume_to_share_multiplier") or 100.0),
                        progress_callback=progress_logger("high_volume_precompute", timer_text, high_volume_names),
                    )
                    report(
                        "intraday_prepare.high_volume_precompute.done",
                        timer_time=timer_text,
                        factor_names=high_volume_names,
                        rows_written=result.get("rows_written"),
                        rows=result.get("rows"),
                    )
                    results.append({"timer_time": timer_text, "factor_names": high_volume_names, "status": "completed", "result": result})
                except Exception as exc:
                    message = f"{timer_text} 放量因子准备失败: {type(exc).__name__}: {exc}"
                    report(
                        "intraday_prepare.high_volume_precompute.failed",
                        timer_time=timer_text,
                        factor_names=high_volume_names,
                        error=message,
                    )
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
                if step_type == "kline_minute":
                    timer_times = [str(item) for item in next_step.get("timer_times") or [] if str(item).strip()]
                    if not timer_times:
                        continue
                    next_step["timer_times"] = timer_times
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
            if (
                str(gap.get("sync_step") or "") in {"cum_timer", "tushare_daily"}
                or (str(gap.get("sync_step") or "") == "kline_minute" and bool(gap.get("timer_time")))
            )
            and not (str(gap.get("sync_step") or "") == "tushare_daily" and str(gap.get("dependency") or "") != "stock_limit_prices")
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
        raw_total_asset = account.total_asset or (account.cash + account.market_value)
        meta = self._json_dict(account.meta)
        positions = await self._position_rows(
            account.positions,
            total_asset=raw_total_asset,
            refresh_quotes=True,
        )
        unrealized_pnl = sum(float(row.get("unrealized_pnl") or 0.0) for row in positions)
        market_value = round(sum(float(row.get("market_value") or 0.0) for row in positions), 2) if positions else account.market_value
        total_asset = round(account.cash + market_value, 2) if positions else raw_total_asset
        capital_base = self._capital_base(meta)
        total_pnl = total_asset - capital_base if capital_base > 0 else unrealized_pnl
        total_pnl_pct = (total_pnl / capital_base) if capital_base > 0 else None
        return {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "mode": mode,
            "cash": account.cash,
            "total_asset": total_asset,
            "market_value": market_value,
            "unrealized_pnl": round(unrealized_pnl, 2),
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl_pct, 4) if total_pnl_pct is not None else None,
            "position_count": len(positions),
            "positions": positions,
            "positions_by_symbol": account.positions,
            "source": account.source,
            "error": account.error,
            "meta": meta,
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

    async def _record_equity_snapshot(
        self,
        *,
        profile_key: str,
        strategy_id: int | None,
        mode: str,
        account: dict[str, Any],
        source: str,
    ) -> None:
        meta = self._json_dict(account.get("meta"))
        if not meta.get("initialized"):
            return
        if meta.get("stale_display_only"):
            return
        total_asset = float(account.get("total_asset") or 0.0)
        if total_asset <= 0:
            return
        target_capital = float(meta.get("target_capital") or meta.get("initial_capital") or 0.0)
        position_count = int(float(account.get("position_count") or 0))
        expected_count = len([symbol for symbol in meta.get("strategy_recognized_symbols") or [] if symbol])
        if expected_count and position_count < expected_count and target_capital > 0 and total_asset < target_capital * 0.95:
            logger.warning(
                "Skip incomplete live equity snapshot profile={} positions={}/{} total_asset={} target={}",
                profile_key,
                position_count,
                expected_count,
                total_asset,
                target_capital,
            )
            return
        snapshot_meta = {
            "capital_base": self._capital_base(meta),
            "target_capital": target_capital or meta.get("target_capital") or meta.get("initial_capital"),
            "position_count": position_count,
            "expected_position_count": expected_count,
            "account_source": account.get("source"),
            "recorded_at": datetime.now().isoformat(timespec="seconds"),
        }
        try:
            async with async_session_factory() as session:
                session.add(
                    LiveEquitySnapshot(
                        snapshot_id=f"equity-{uuid.uuid4().hex}",
                        profile_key=profile_key,
                        strategy_id=strategy_id,
                        mode=mode,
                        trade_date=date.today(),
                        cash=Decimal(str(round(float(account.get("cash") or 0.0), 2))),
                        market_value=Decimal(str(round(float(account.get("market_value") or 0.0), 2))),
                        total_asset=Decimal(str(round(total_asset, 2))),
                        realized_pnl=Decimal(str(round(float(meta.get("realized_pnl") or 0.0), 2))),
                        unrealized_pnl=Decimal(str(round(float(account.get("unrealized_pnl") or 0.0), 2))),
                        source=source,
                        meta=snapshot_meta,
                    )
                )
                await session.commit()
        except Exception:
            logger.opt(exception=True).warning("Failed to record live equity snapshot for {}", profile_key)

    def _usable_equity_snapshots(self, rows: Sequence[LiveEquitySnapshot]) -> list[LiveEquitySnapshot]:
        if not rows:
            return []
        counts: list[int] = []
        totals: list[float] = []
        for row in rows:
            meta = self._json_dict(row.meta)
            count = int(float(meta.get("position_count") or 0))
            total_asset = float(row.total_asset or 0.0)
            if count > 0:
                counts.append(count)
            if total_asset > 0:
                totals.append(total_asset)
        expected_count = max(counts) if counts else 0
        anchor_total = max(totals) if totals else 0.0
        usable: list[LiveEquitySnapshot] = []
        for row in rows:
            meta = self._json_dict(row.meta)
            if meta.get("stale_display_only"):
                continue
            total_asset = float(row.total_asset or 0.0)
            if total_asset <= 0:
                continue
            position_count = int(float(meta.get("position_count") or 0))
            target_capital = float(meta.get("target_capital") or meta.get("capital_base") or 0.0)
            reference_total = target_capital if target_capital > 0 else anchor_total
            if expected_count and position_count < expected_count and reference_total > 0 and total_asset < reference_total * 0.95:
                continue
            usable.append(row)
        return usable

    async def _load_equity_snapshots(
        self,
        *,
        profile_key: str | None,
        mode: str | None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 5000,
    ) -> list[LiveEquitySnapshot]:
        async with async_session_factory() as session:
            stmt = select(LiveEquitySnapshot)
            if profile_key:
                stmt = stmt.where(LiveEquitySnapshot.profile_key == profile_key)
            if mode:
                stmt = stmt.where(LiveEquitySnapshot.mode == mode)
            if start_date:
                stmt = stmt.where(LiveEquitySnapshot.trade_date >= start_date)
            if end_date:
                stmt = stmt.where(LiveEquitySnapshot.trade_date <= end_date)
            stmt = stmt.order_by(LiveEquitySnapshot.created_at.asc(), LiveEquitySnapshot.id.asc()).limit(max(1, min(10000, int(limit or 5000))))
            return (await session.execute(stmt)).scalars().all()

    def _equity_metrics(self, rows: Sequence[LiveEquitySnapshot]) -> dict[str, Any]:
        if not rows:
            return {
                "snapshot_points": 0,
                "start_total_asset": None,
                "end_total_asset": None,
                "pnl": None,
                "return_pct": None,
                "max_drawdown_pct": None,
            }
        values = [float(row.total_asset or 0.0) for row in rows if float(row.total_asset or 0.0) > 0]
        if not values:
            return {
                "snapshot_points": len(rows),
                "start_total_asset": None,
                "end_total_asset": None,
                "pnl": None,
                "return_pct": None,
                "max_drawdown_pct": None,
            }
        peak = values[0]
        max_drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            if peak > 0:
                max_drawdown = min(max_drawdown, (value - peak) / peak)
        start_value = values[0]
        end_value = values[-1]
        pnl = end_value - start_value
        return {
            "snapshot_points": len(values),
            "start_total_asset": round(start_value, 2),
            "end_total_asset": round(end_value, 2),
            "pnl": round(pnl, 2),
            "return_pct": round(pnl / start_value, 4) if start_value > 0 else None,
            "max_drawdown_pct": round(max_drawdown, 4),
        }

    def _equity_curve(self, rows: Sequence[LiveEquitySnapshot]) -> list[dict[str, Any]]:
        values = [float(row.total_asset or 0.0) for row in rows]
        positive_values = [value for value in values if value > 0]
        if not positive_values:
            return []
        base = positive_values[0]
        peak = positive_values[0]
        curve: list[dict[str, Any]] = []
        for row in rows:
            total_asset = float(row.total_asset or 0.0)
            if total_asset <= 0:
                continue
            peak = max(peak, total_asset)
            pnl = total_asset - base
            curve.append(
                {
                    "snapshot_id": row.snapshot_id,
                    "trade_date": row.trade_date.isoformat() if row.trade_date else None,
                    "created_at": row.created_at.isoformat(timespec="seconds") if row.created_at else None,
                    "cash": round(float(row.cash or 0.0), 2),
                    "market_value": round(float(row.market_value or 0.0), 2),
                    "total_asset": round(total_asset, 2),
                    "realized_pnl": round(float(row.realized_pnl or 0.0), 2),
                    "unrealized_pnl": round(float(row.unrealized_pnl or 0.0), 2),
                    "pnl": round(pnl, 2),
                    "return_pct": round(pnl / base, 4) if base > 0 else None,
                    "drawdown_pct": round((total_asset - peak) / peak, 4) if peak > 0 else None,
                    "source": row.source,
                }
            )
        return curve

    def _capital_base(self, meta: dict[str, Any]) -> float:
        initial = float(meta.get("initial_capital") or meta.get("capital") or 0.0)
        adjustment = float(meta.get("cash_adjustment_total") or 0.0)
        target = float(meta.get("target_capital") or 0.0)
        base = initial + adjustment
        if base <= 0:
            base = target
        return base

    def _positions_cost_basis(self, positions: dict[str, dict[str, Any]]) -> float:
        total = 0.0
        for raw in positions.values():
            position = dict(raw or {})
            quantity = float(position.get("quantity", position.get("volume", 0.0)) or 0.0)
            if quantity <= 0:
                continue
            avg_cost = float(position.get("avg_cost", position.get("cost_price", 0.0)) or 0.0)
            if avg_cost > 0:
                total += quantity * avg_cost
                continue
            cost_value = float(position.get("cost_value", 0.0) or 0.0)
            if cost_value > 0:
                total += cost_value
                continue
            total += float(position.get("market_value", 0.0) or 0.0)
        return round(total, 2)

    async def _position_rows(
        self,
        positions: dict[str, dict[str, Any]],
        *,
        total_asset: float | None = None,
        refresh_quotes: bool = True,
    ) -> list[dict[str, Any]]:
        symbol_list = [str(symbol) for symbol in positions.keys() if str(symbol).strip()]
        name_map = await self._stock_names(symbol_list)
        quote_map = await self._quote_detail_map(symbol_list) if refresh_quotes else {}
        volume_metrics = await asyncio.to_thread(self._position_volume_metrics, symbol_list)
        denominator = float(total_asset or 0.0)
        rows: list[dict[str, Any]] = []
        for symbol in sorted(symbol_list):
            raw = dict(positions.get(symbol) or {})
            quote = quote_map.get(symbol) or {}
            metrics = volume_metrics.get(symbol, {})
            quantity = float(raw.get("quantity", raw.get("volume", 0.0)) or 0.0)
            available = float(raw.get("available", raw.get("available_volume", quantity)) or 0.0)
            avg_cost = float(raw.get("avg_cost", raw.get("cost_price", 0.0)) or 0.0)
            last_price = float(
                quote.get("price")
                or raw.get("last_price")
                or raw.get("latest_price")
                or raw.get("reference_price")
                or 0.0
            )
            market_value = float(raw.get("market_value", 0.0) or 0.0)
            if quantity > 0 and last_price > 0:
                market_value = round(quantity * last_price, 2)
            cost_value = quantity * avg_cost if quantity > 0 and avg_cost > 0 else 0.0
            unrealized_pnl = market_value - cost_value if market_value or cost_value else float(raw.get("unrealized_pnl", 0.0) or 0.0)
            unrealized_pnl_pct = (unrealized_pnl / cost_value) if cost_value > 0 else None
            stock_name = raw.get("stock_name") or raw.get("name") or raw.get("stockName") or name_map.get(symbol)
            position_pct = (market_value / denominator) if denominator > 0 else None
            volume_ratio = self._float_or_none(quote.get("volume_ratio") or raw.get("volume_ratio"))
            if volume_ratio is None:
                quote_volume = self._float_or_none(quote.get("volume"))
                avg_volume = self._float_or_none(metrics.get("avg_volume_5"))
                if quote_volume is not None and avg_volume and avg_volume > 0:
                    volume_ratio = quote_volume / avg_volume
            if volume_ratio is None:
                volume_ratio = self._float_or_none(metrics.get("volume_ratio"))
            rows.append(
                {
                    **raw,
                    "symbol": symbol,
                    "stock_name": stock_name,
                    "quantity": quantity,
                    "available": available,
                    "avg_cost": avg_cost,
                    "last_price": last_price or raw.get("last_price"),
                    "latest_price": last_price or raw.get("latest_price") or raw.get("last_price"),
                    "market_value": market_value,
                    "cost_value": round(cost_value, 2),
                    "unrealized_pnl": round(unrealized_pnl, 2),
                    "unrealized_pnl_pct": round(unrealized_pnl_pct, 4) if unrealized_pnl_pct is not None else None,
                    "position_pct": round(position_pct, 4) if position_pct is not None else None,
                    "volume_ratio": round(volume_ratio, 4) if volume_ratio is not None else None,
                    "today_change_pct": self._float_or_none(
                        quote.get("change_pct")
                        or raw.get("today_change_pct")
                        or raw.get("change_pct")
                    ),
                    "turnover_rate": self._float_or_none(quote.get("turnover_rate") or raw.get("turnover_rate")),
                    "amount": self._float_or_none(quote.get("amount") or raw.get("amount")),
                    "total_value": self._float_or_none(quote.get("total_value")),
                    "float_value": self._float_or_none(quote.get("float_value")),
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
        prefer_local: bool = False,
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
            return await self._strategy_account_snapshot(
                profile_key=profile_key,
                mode=mode,
                params=params,
                prefer_local=prefer_local,
            )
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
        prefer_local: bool = False,
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

        local_positions = {
            row.symbol: self._json_dict(row.state)
            for row in rows
            if row.symbol != STRATEGY_ACCOUNT_SYMBOL
        }
        cash = float(account_state.get("cash", 0.0) or 0.0)
        if mode != "live":
            positions = {
                symbol: state
                for symbol, state in local_positions.items()
                if float(state.get("quantity", 0.0) or 0.0) > 0
            }
            await self._refresh_strategy_position_marks(positions, mode=mode)
            market_value = round(sum(float(position.get("market_value", 0.0) or 0.0) for position in positions.values()), 2)
            total_asset = round(cash + market_value, 2)
            position_cost_basis = self._positions_cost_basis(positions)
            meta = {
                **account_state,
                "initialized": True,
                "account_scope": "strategy_pool",
                "profile_key": profile_key,
                "mode": mode,
                "positions_source": account_state.get("positions_source", "strategy_owned_only"),
                "market_value": market_value,
                "total_asset": total_asset,
                "position_cost_basis": position_cost_basis,
                "principal_cash": round(cash, 2),
                "principal_invested": round(position_cost_basis + cash, 2),
                "principal_basis": account_state.get("principal_basis", "position_cost_basis_plus_cash"),
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

        recognized_symbols = sorted(
            symbol
            for symbol, state in local_positions.items()
            if str(symbol).strip() and float(state.get("quantity", 0.0) or 0.0) > 0
        )
        checked_at = datetime.now().isoformat(timespec="seconds")
        base_meta = {
            **account_state,
            "initialized": True,
            "account_scope": "strategy_pool",
            "profile_key": profile_key,
            "mode": mode,
            "positions_source": "qmt_account_strategy_symbols",
            "price_source": "qmt_realtime_quotes",
            "strategy_recognized_symbols": recognized_symbols,
            "broker_position_checked_at": checked_at,
            "cash_source": "strategy_pool_subledger",
            "same_symbol_source_limit": "QMT 持仓按证券代码汇总；若手工和策略同时持有同一代码，系统无法区分来源。",
        }
        stale_snapshot = self._stale_strategy_account_snapshot(
            profile_key=profile_key,
            mode=mode,
            cash=cash,
            positions=local_positions,
            base_meta=base_meta,
        )
        if prefer_local:
            return stale_snapshot
        try:
            broker = LiveAccountSnapshot.from_qmt(await qmt_trading_service.account_snapshot())
        except Exception as exc:
            error = f"读取 QMT 账户失败：{type(exc).__name__}: {exc}"
            stale_snapshot.error = error
            stale_snapshot.meta = {**self._json_dict(stale_snapshot.meta), "last_snapshot_error": error}
            return stale_snapshot
        if broker.error:
            error = f"读取 QMT 账户失败：{broker.error}"
            stale_snapshot.error = error
            stale_snapshot.meta = {**self._json_dict(stale_snapshot.meta), "last_snapshot_error": error}
            return stale_snapshot

        held_symbols: list[str] = []
        missing_broker_symbols: list[str] = []
        for symbol in recognized_symbols:
            broker_position = dict(broker.positions.get(symbol) or {})
            broker_quantity = float(broker_position.get("quantity", 0.0) or 0.0)
            if broker_quantity > 0:
                held_symbols.append(symbol)
            else:
                missing_broker_symbols.append(symbol)

        price_map, quote_error = await self._quote_prices(held_symbols)
        missing_quote_symbols = self._missing_realtime_price_symbols(held_symbols, price_map)
        if quote_error or missing_quote_symbols:
            reason = quote_error or f"实盘实时行情缺失：{', '.join(missing_quote_symbols[:10])}"
            qmt_positions = {}
            for symbol in held_symbols:
                broker_position = dict(broker.positions.get(symbol) or {})
                local = dict(local_positions.get(symbol) or {})
                qmt_positions[symbol] = {
                    **local,
                    "symbol": symbol,
                    "quantity": float(broker_position.get("quantity", 0.0) or 0.0),
                    "available": float(broker_position.get("available", 0.0) or 0.0),
                    "avg_cost": float(broker_position.get("avg_cost", local.get("avg_cost", 0.0)) or 0.0),
                    "stock_name": broker_position.get("name") or local.get("stock_name") or local.get("name"),
                    "market_value": 0.0,
                    "price_source": "qmt_realtime_quotes_missing",
                    "quote_error": reason,
                }
            return LiveAccountSnapshot(
                cash=round(cash, 2),
                total_asset=round(cash, 2),
                market_value=0.0,
                positions=qmt_positions,
                source="live_strategy_pool_qmt",
                error=reason,
                meta={**base_meta, "missing_quote_symbols": missing_quote_symbols, "last_snapshot_error": reason},
            )

        name_map = await self._stock_names(held_symbols)
        positions: dict[str, dict[str, Any]] = {}
        for symbol in held_symbols:
            broker_position = dict(broker.positions.get(symbol) or {})
            local = dict(local_positions.get(symbol) or {})
            quantity = float(broker_position.get("quantity", 0.0) or 0.0)
            available = max(0.0, min(quantity, float(broker_position.get("available", quantity) or 0.0)))
            avg_cost = float(broker_position.get("avg_cost", local.get("avg_cost", 0.0)) or 0.0)
            price = float(price_map.get(symbol, 0.0) or 0.0)
            positions[symbol] = {
                **local,
                "symbol": symbol,
                "stock_name": broker_position.get("name") or local.get("stock_name") or local.get("name") or name_map.get(symbol),
                "quantity": quantity,
                "available": available,
                "avg_cost": avg_cost,
                "reference_price": price,
                "last_price": price,
                "market_value": round(quantity * price, 2),
                "broker_quantity": quantity,
                "broker_available": available,
                "broker_market_value": float(broker_position.get("market_value", 0.0) or 0.0),
                "strategy_owned": True,
                "position_source": "qmt_account",
                "price_source": "qmt_realtime_quotes",
                "updated_from": "qmt_account_snapshot",
                "updated_at": checked_at,
            }
        market_value = round(sum(float(position.get("market_value", 0.0) or 0.0) for position in positions.values()), 2)
        total_asset = round(cash + market_value, 2)
        position_cost_basis = self._positions_cost_basis(positions)
        meta = {
            **base_meta,
            "market_value": market_value,
            "total_asset": total_asset,
            "position_cost_basis": position_cost_basis,
            "principal_cash": round(cash, 2),
            "principal_invested": round(position_cost_basis + cash, 2),
            "principal_basis": account_state.get("principal_basis", "position_cost_basis_plus_cash"),
            "broker_cash": round(float(broker.cash or 0.0), 2),
            "broker_total_asset": round(float(broker.total_asset or 0.0), 2),
            "broker_market_value": round(float(broker.market_value or 0.0), 2),
            "missing_broker_symbols": missing_broker_symbols,
            "ignored_broker_symbols": sorted(set(broker.positions) - set(recognized_symbols)),
        }
        async with self._strategy_account_write_lock:
            async with async_session_factory() as session:
                account = await session.scalar(
                    select(LivePositionState).where(
                        LivePositionState.profile_key == profile_key,
                        LivePositionState.mode == mode,
                        LivePositionState.symbol == STRATEGY_ACCOUNT_SYMBOL,
                    )
                )
                if account is not None:
                    account.state = meta
                    session.add(account)
                for symbol, position in positions.items():
                    row = await session.scalar(
                        select(LivePositionState).where(
                            LivePositionState.profile_key == profile_key,
                            LivePositionState.mode == mode,
                            LivePositionState.symbol == symbol,
                        )
                    )
                    if row is None:
                        row = LivePositionState(profile_key=profile_key, mode=mode, symbol=symbol, state=position)
                    row.state = position
                    session.add(row)
                await session.commit()
        return LiveAccountSnapshot(
            cash=round(cash, 2),
            total_asset=total_asset,
            market_value=market_value,
            positions={symbol: dict(position) for symbol, position in positions.items()},
            source="live_strategy_pool_qmt",
            error=None,
            meta=meta,
        )

    def _stale_strategy_account_snapshot(
        self,
        *,
        profile_key: str,
        mode: str,
        cash: float,
        positions: dict[str, dict[str, Any]],
        base_meta: dict[str, Any],
    ) -> LiveAccountSnapshot:
        active_positions = {
            symbol: {
                **dict(state),
                "symbol": symbol,
                "position_source": state.get("position_source") or "last_successful_snapshot",
                "price_source": state.get("price_source") or "last_successful_snapshot",
                "stale_display_only": True,
            }
            for symbol, state in positions.items()
            if str(symbol).strip() and float(state.get("quantity", 0.0) or 0.0) > 0
        }
        market_value = round(
            sum(float(position.get("market_value", 0.0) or 0.0) for position in active_positions.values()),
            2,
        )
        total_asset = round(float(cash or 0.0) + market_value, 2)
        position_cost_basis = self._positions_cost_basis(active_positions)
        meta = {
            **base_meta,
            "profile_key": profile_key,
            "mode": mode,
            "market_value": market_value,
            "total_asset": total_asset,
            "position_cost_basis": position_cost_basis,
            "principal_cash": round(float(cash or 0.0), 2),
            "principal_invested": round(position_cost_basis + float(cash or 0.0), 2),
            "principal_basis": base_meta.get("principal_basis", "position_cost_basis_plus_cash"),
            "positions_source": "last_successful_strategy_snapshot",
            "price_source": "last_successful_strategy_snapshot",
            "stale_display_only": True,
            "stale_display_reason": "QMT 实时账户读取失败，界面暂时展示最近一次成功同步的策略持仓；生成信号和提交订单仍会被阻断。",
        }
        return LiveAccountSnapshot(
            cash=round(float(cash or 0.0), 2),
            total_asset=total_asset,
            market_value=market_value,
            positions=active_positions,
            source="live_strategy_pool_stale",
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
        price_map, quote_error = await self._quote_prices(list(positions.keys()))
        missing_symbols = self._missing_realtime_price_symbols(positions.keys(), price_map) if mode == "live" else []
        for symbol, position in positions.items():
            quantity = float(position.get("quantity", 0.0) or 0.0)
            avg_cost = float(position.get("avg_cost", 0.0) or 0.0)
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if mode == "live" and (quote_error or symbol in missing_symbols):
                position["quote_error"] = quote_error or "realtime_quote_missing"
                position["last_price"] = 0.0
                position["market_value"] = 0.0
                continue
            if mode != "live" and price <= 0 and quantity > 0:
                price = float(position.get("reference_price", 0.0) or position.get("last_price", 0.0) or avg_cost)
            position["available"] = max(0.0, min(quantity, float(position.get("available", quantity) or quantity)))
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
            quotes = await asyncio.wait_for(qmt_gateway.get_realtime_quotes(symbol_list), timeout=5.0)
        except Exception as exc:
            return {}, f"{type(exc).__name__}: {exc}"
        prices: dict[str, float] = {}
        for quote in quotes:
            symbol = str(quote.get("symbol") or quote.get("code") or quote.get("stock_code") or "")
            price = self._quote_price(quote)
            if symbol and price > 0:
                prices[symbol] = price
        return prices, None

    async def _quote_detail_map(self, symbols: Sequence[str]) -> dict[str, dict[str, float | None]]:
        symbol_list = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
        if not symbol_list:
            return {}
        try:
            quotes = await asyncio.wait_for(qmt_gateway.get_realtime_quotes(symbol_list), timeout=5.0)
        except Exception as exc:
            logger.warning("Failed to read realtime quote details: {}", exc)
            return {}

        result: dict[str, dict[str, float | None]] = {}
        for quote in quotes or []:
            symbol = str(quote.get("symbol") or quote.get("code") or quote.get("stock_code") or "").strip()
            if not symbol:
                continue
            price = self._quote_price(quote)
            pre_close = self._first_float(
                quote,
                (
                    "preClose",
                    "pre_close",
                    "pre_close_price",
                    "lastClose",
                    "last_close",
                    "前收盘",
                ),
            )
            change_pct = self._normalized_ratio(
                self._first_float(
                    quote,
                    (
                        "change_pct",
                        "pct_chg",
                        "changeRatio",
                        "change_rate",
                        "涨跌幅",
                    ),
                )
            )
            if change_pct is None and price > 0 and pre_close and pre_close > 0:
                change_pct = (price - pre_close) / pre_close
            raw_quote = self._json_dict(quote.get("raw"))
            if change_pct is None and raw_quote:
                raw_change = self._normalized_ratio(
                    self._first_float(
                        raw_quote,
                        ("change_pct", "pct_chg", "changeRatio", "change_rate", "涨跌幅"),
                    )
                )
                if raw_change is not None:
                    change_pct = raw_change
            result[symbol] = {
                "price": price if price > 0 else None,
                "amount": self._first_float(quote, ("amount", "turnover_amount", "成交额"))
                or self._first_float(raw_quote, ("amount", "turnover_amount", "成交额")),
                "volume": self._first_float(quote, ("volume", "pvolume", "成交量"))
                or self._first_float(raw_quote, ("volume", "pvolume", "成交量")),
                "volume_ratio": self._normalized_ratio(
                    self._first_float(
                        quote,
                        (
                            "volumeRatio",
                            "volume_ratio",
                            "volRatio",
                            "vol_ratio",
                            "量比",
                        ),
                    ) or self._first_float(raw_quote, ("volumeRatio", "volume_ratio", "volRatio", "vol_ratio", "量比")),
                    percent_like=False,
                ),
                "change_pct": change_pct,
                "turnover_rate": self._normalized_ratio(
                    self._first_float(quote, ("turnover_rate", "turnover", "turnoverRate", "换手率"))
                    or self._first_float(raw_quote, ("turnover_rate", "turnover", "turnoverRate", "换手率"))
                ),
                "total_value": self._first_float(quote, ("total_value", "total_mv", "总市值")),
                "float_value": self._first_float(quote, ("float_value", "circ_mv", "流通市值")),
                "quote_time": quote.get("quote_time") or raw_quote.get("time") or raw_quote.get("timetag"),
            }
        return result

    def _position_volume_metrics(self, symbols: Sequence[str]) -> dict[str, dict[str, float]]:
        symbol_list = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
        if not symbol_list:
            return {}
        end_date = date.today()
        start_date = end_date - timedelta(days=45)
        try:
            daily = get_market_data_store().load_daily(
                symbol_list,
                start_date,
                end_date,
                columns=["symbol", "trade_date", "volume"],
            )
        except Exception as exc:
            logger.warning("Failed to load daily volume metrics for live positions: {}", exc)
            return {}
        if daily is None or daily.empty or "symbol" not in daily.columns or "volume" not in daily.columns:
            return {}
        body = daily.reset_index() if "trade_date" not in daily.columns else daily.copy()
        if "trade_date" not in body.columns:
            return {}
        body["symbol"] = body["symbol"].astype(str)
        body["trade_date"] = pd.to_datetime(body["trade_date"], errors="coerce")
        body["volume"] = pd.to_numeric(body["volume"], errors="coerce")
        body = body.dropna(subset=["symbol", "trade_date", "volume"]).sort_values(["symbol", "trade_date"])
        result: dict[str, dict[str, float]] = {}
        for symbol, group in body.groupby("symbol", sort=False):
            volumes = group["volume"].dropna()
            if len(volumes) < 2:
                continue
            avg_window = volumes.tail(6).iloc[:-1]
            avg_volume = float(avg_window.mean() or 0.0) if len(avg_window) else 0.0
            if avg_volume <= 0:
                continue
            result[str(symbol)] = {
                "avg_volume_5": avg_volume,
                "volume_ratio": float(volumes.iloc[-1]) / avg_volume,
            }
        return result

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
        ranked_symbols: Sequence[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        lot_size = int(params.get("lot_size", 100) or 100)
        tolerance = float(params.get("rebalance_tolerance_pct", 0.01) or 0.01)
        execution_reserve = float(params.get("cash_execution_reserve_pct", params.get("cash_buffer_pct", 0.08)) or 0.0)
        buy_fee_buffer = float(params.get("cash_aware_buy_fee_buffer_pct", 0.003) or 0.0)
        require_current = bool(params.get("require_current_market_data_for_orders", True))
        rank_map = {symbol: index + 1 for index, symbol in enumerate(ranked_symbols or [])}
        sell_orders: list[dict[str, Any]] = []
        buy_candidates: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        def skip(
            symbol: str,
            side: str,
            reason: str,
            quantity: int = 0,
            attribution: dict[str, Any] | None = None,
        ) -> None:
            skipped.append(
                {
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "reason": reason,
                    "attribution": attribution or {"summary": reason},
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
                skip(symbol, "SELL", "current_market_data_missing", attribution={
                    "action": "SELL",
                    "summary": "缺少提交所需的实时行情，卖出跳过。",
                    "trigger": "current_market_data_missing",
                    "current_quantity": current_qty,
                })
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
                    if symbol in target_weights:
                        summary = (
                            f"目标仓位降至 {target_qty:.0f} 股，当前 {current_qty:.0f} 股，"
                            f"卖出 {sell_qty:.0f} 股用于回到目标仓位并释放现金。"
                        )
                        trigger = "reduce_to_target"
                    else:
                        summary = f"该股票不在本轮信号目标池，卖出 {sell_qty:.0f} 股退出持仓并释放现金。"
                        trigger = "exit_not_in_target"
                    sell_orders.append(self._order(
                        bundle,
                        symbol,
                        "SELL",
                        sell_qty,
                        price,
                        summary,
                        attribution={
                            "action": "SELL",
                            "trigger": trigger,
                            "summary": summary,
                            "rank": rank_map.get(symbol),
                            "current_quantity": current_qty,
                            "target_quantity": target_qty,
                            "delta_quantity": delta,
                            "order_quantity": sell_qty,
                            "available_quantity": available,
                            "target_weight": target_weight,
                            "reference_price": price,
                            "notional": round(sell_qty * price, 2),
                            "portfolio_value": round(float(portfolio_value or 0.0), 2),
                            "cash_effect": round(sell_qty * price, 2),
                        },
                    ))

        projected_cash = account.cash + sum(float(order["quantity"]) * float(order["reference_price"]) for order in sell_orders)
        available_cash = max(0.0, projected_cash * (1.0 - execution_reserve))
        for symbol, weight in target_weights.items():
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if require_current and price <= 0:
                skip(symbol, "BUY", "current_market_data_missing", attribution={
                    "action": "BUY",
                    "summary": "缺少提交所需的实时行情，买入跳过。",
                    "trigger": "current_market_data_missing",
                    "rank": rank_map.get(symbol),
                    "target_weight": float(weight or 0.0),
                })
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
            available_before = available_cash
            summary = (
                f"目标排名第 {rank_map.get(symbol, '-') }，目标仓位 {target_qty:.0f} 股，"
                f"当前 {current_qty:.0f} 股，买入 {buy_qty:.0f} 股补足目标仓位。"
            )
            buy_candidates.append(self._order(
                bundle,
                symbol,
                "BUY",
                buy_qty,
                price,
                summary,
                attribution={
                    "action": "BUY",
                    "trigger": "enter_or_add_to_target",
                    "summary": summary,
                    "rank": rank_map.get(symbol),
                    "current_quantity": current_qty,
                    "target_quantity": target_qty,
                    "delta_quantity": delta,
                    "order_quantity": buy_qty,
                    "target_weight": float(weight or 0.0),
                    "reference_price": price,
                    "notional": round(buy_qty * price, 2),
                    "buy_value_with_fee_buffer": round(buy_value, 2),
                    "cash_available_before": round(available_before, 2),
                    "cash_reserve_pct": execution_reserve,
                    "buy_fee_buffer_pct": buy_fee_buffer,
                    "portfolio_value": round(float(portfolio_value or 0.0), 2),
                    "cash_effect": -round(buy_value, 2),
                },
            ))
            if buy_value <= available_cash:
                available_cash -= buy_value
            else:
                skip(symbol, "BUY", "cash_aware_deferred_insufficient_cash", buy_qty, attribution={
                    "action": "BUY",
                    "trigger": "cash_aware_deferred_insufficient_cash",
                    "summary": (
                        f"现金不足，需约 {buy_value:.2f}，可用约 {available_before:.2f}，"
                        "本轮延后买入。"
                    ),
                    "rank": rank_map.get(symbol),
                    "current_quantity": current_qty,
                    "target_quantity": target_qty,
                    "order_quantity": buy_qty,
                    "target_weight": float(weight or 0.0),
                    "reference_price": price,
                    "buy_value_with_fee_buffer": round(buy_value, 2),
                    "cash_available_before": round(available_before, 2),
                    "cash_shortfall": round(max(0.0, buy_value - available_before), 2),
                })
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
        if snapshot.error:
            return {"ok": False, "message": f"策略资金池实时快照不可用：{snapshot.error}"}
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
        submit_price_map: dict[str, float] = {}
        if mode == "live":
            order_symbols = sorted({str(order.get("symbol") or "").strip() for order in orders if str(order.get("symbol") or "").strip()})
            submit_price_map, quote_error = await self._quote_prices(order_symbols)
            missing_symbols = self._missing_realtime_price_symbols(order_symbols, submit_price_map)
            if quote_error or missing_symbols:
                reason = quote_error or f"提交前实盘实时行情缺失：{', '.join(missing_symbols[:10])}"
                return {"ok": False, "message": reason}
            repriced_at = datetime.now().isoformat(timespec="seconds")
            for order in orders:
                symbol = str(order.get("symbol") or "").strip()
                price = float(submit_price_map.get(symbol, 0.0) or 0.0)
                if price > 0:
                    order["reference_price"] = round(price, 4)
                    order["price"] = round(price, 4)
                    order["price_source"] = "qmt_realtime_quotes_at_submit"
                    order["repriced_at"] = repriced_at
        for order in orders:
            symbol = str(order.get("symbol") or "")
            side = str(order.get("side") or "").upper()
            qty = float(order.get("quantity") or 0.0)
            price = float(submit_price_map.get(symbol, 0.0) or order.get("reference_price") or order.get("price") or 0.0)
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
            price = float(submit_price_map.get(str(order.get("symbol") or ""), 0.0) or order.get("reference_price") or order.get("price") or 0.0)
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
        async with self._strategy_account_write_lock:
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
                        "updated_from": str(result.get("updated_from") or ("live_fill" if mode == "live" else "paper_fill")),
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
                            "updated_from": str(result.get("updated_from") or ("live_fill" if mode == "live" else "paper_fill")),
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
                position_cost_basis = self._positions_cost_basis({
                    str(item.get("symbol") or ""): dict(item)
                    for item in live_positions
                    if str(item.get("symbol") or "").strip()
                })
                account_state.update(
                    {
                        "cash": round(cash, 2),
                        "market_value": market_value,
                        "total_asset": round(cash + market_value, 2),
                        "position_cost_basis": position_cost_basis,
                        "principal_cash": round(cash, 2),
                        "principal_invested": round(position_cost_basis + cash, 2),
                        "principal_basis": account_state.get("principal_basis", "position_cost_basis_plus_cash"),
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
        payload["strategy_account_note"] = "live 模式按 QMT 成交回报回写策略账本；未成交委托只作为在途订单参与差额计算。"
        return payload

    async def _reconcile_strategy_account_from_trade_records(self, *, profile_key: str, mode: str) -> None:
        if mode != "live":
            return
        async with self._strategy_account_write_lock:
            await self._reconcile_strategy_account_from_trade_records_unlocked(profile_key=profile_key, mode=mode)

    async def _reconcile_strategy_account_from_trade_records_unlocked(self, *, profile_key: str, mode: str) -> None:
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
            account_state = self._json_dict(account.state if account else None)
            if account is None or not account_state.get("initialized"):
                return
            records = (
                await session.execute(
                    select(LiveTradeRecord)
                    .where(
                        LiveTradeRecord.profile_key == profile_key,
                        LiveTradeRecord.mode == mode,
                        LiveTradeRecord.status.in_(sorted(LIVE_FILLED_STATUSES)),
                    )
                    .order_by(LiveTradeRecord.created_at.asc(), LiveTradeRecord.id.asc())
                )
            ).scalars().all()

            initial_capital = float(account_state.get("initial_capital") or account_state.get("capital") or 0.0)
            if initial_capital <= 0:
                initial_capital = float(account_state.get("target_capital") or account_state.get("cash") or 0.0)
            cash_adjustment_total = float(account_state.get("cash_adjustment_total", 0.0) or 0.0)
            cash = initial_capital + cash_adjustment_total
            realized_pnl = 0.0
            positions: dict[str, dict[str, Any]] = {}
            for record in records:
                symbol = str(record.symbol or "")
                side = str(record.side or "").upper()
                qty = float(record.quantity or 0.0)
                price = float(record.reference_price or 0.0)
                if not symbol or side not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
                    continue
                value = round(qty * price, 2)
                position = dict(positions.get(symbol) or {})
                current_qty = float(position.get("quantity", 0.0) or 0.0)
                avg_cost = float(position.get("avg_cost", price) or price)
                if side == "BUY":
                    cash -= value
                    next_qty = current_qty + qty
                    avg_cost = ((avg_cost * current_qty) + value) / max(next_qty, 1.0)
                    positions[symbol] = {
                        **position,
                        "symbol": symbol,
                        "stock_name": record.stock_name or position.get("stock_name"),
                        "quantity": next_qty,
                        "available": next_qty,
                        "avg_cost": avg_cost,
                        "reference_price": price,
                        "last_price": price,
                        "market_value": round(next_qty * price, 2),
                        "strategy_owned": True,
                        "updated_from": "qmt_trade_reconcile",
                        "updated_at": datetime.now().isoformat(timespec="seconds"),
                    }
                else:
                    sell_qty = min(current_qty, qty)
                    cash += round(sell_qty * price, 2)
                    realized_pnl += (price - avg_cost) * sell_qty
                    next_qty = current_qty - sell_qty
                    if next_qty <= 1e-6:
                        positions.pop(symbol, None)
                    else:
                        positions[symbol] = {
                            **position,
                            "quantity": next_qty,
                            "available": next_qty,
                            "reference_price": price,
                            "last_price": price,
                            "market_value": round(next_qty * price, 2),
                            "stock_name": record.stock_name or position.get("stock_name"),
                            "strategy_owned": True,
                            "updated_from": "qmt_trade_reconcile",
                            "updated_at": datetime.now().isoformat(timespec="seconds"),
                        }

            await session.execute(
                delete(LivePositionState).where(
                    LivePositionState.profile_key == profile_key,
                    LivePositionState.mode == mode,
                    LivePositionState.symbol != STRATEGY_ACCOUNT_SYMBOL,
                )
            )
            market_value = round(sum(float(position.get("market_value", 0.0) or 0.0) for position in positions.values()), 2)
            position_cost_basis = self._positions_cost_basis(positions)
            for symbol, position in positions.items():
                session.add(LivePositionState(profile_key=profile_key, mode=mode, symbol=symbol, state=position))
            account_state.update(
                {
                    "cash": round(cash, 2),
                    "market_value": market_value,
                    "total_asset": round(cash + market_value, 2),
                    "position_cost_basis": position_cost_basis,
                    "principal_cash": round(cash, 2),
                    "principal_invested": round(position_cost_basis + cash, 2),
                    "principal_basis": account_state.get("principal_basis", "position_cost_basis_plus_cash"),
                    "realized_pnl": round(realized_pnl, 2),
                    "positions_source": "qmt_trade_records",
                    "reconciled_at": datetime.now().isoformat(timespec="seconds"),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
            account.state = account_state
            session.add(account)
            await session.commit()

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

    async def _load_live_trade_rows(
        self,
        *,
        profile_key: str | None = None,
        mode: str | None = None,
        limit: int = 200,
        only_pending: bool = False,
        min_age_seconds: int = 0,
        record_ids: Sequence[str] | None = None,
        order_ids: Sequence[str | int] | None = None,
    ) -> list[LiveTradeRecord]:
        async with async_session_factory() as session:
            stmt = select(LiveTradeRecord)
            if only_pending:
                stmt = stmt.where(LiveTradeRecord.status.in_(sorted(LIVE_PENDING_STATUSES)))
            if profile_key:
                stmt = stmt.where(LiveTradeRecord.profile_key == profile_key)
            if mode:
                stmt = stmt.where(LiveTradeRecord.mode == mode)
            if record_ids:
                stmt = stmt.where(LiveTradeRecord.record_id.in_([str(item) for item in record_ids if str(item).strip()]))
            if order_ids:
                normalized_order_ids = [self._normalize_order_id(item) for item in order_ids if self._normalize_order_id(item)]
                if normalized_order_ids:
                    stmt = stmt.where(LiveTradeRecord.order_id.in_(normalized_order_ids))
            if min_age_seconds > 0:
                stmt = stmt.where(LiveTradeRecord.created_at <= datetime.now() - timedelta(seconds=int(min_age_seconds)))
            stmt = stmt.order_by(LiveTradeRecord.created_at.desc()).limit(max(1, min(1000, int(limit or 200))))
            return (await session.execute(stmt)).scalars().all()

    async def _pending_order_effect(self, *, profile_key: str, mode: str) -> dict[str, Any]:
        rows = await self._load_live_trade_rows(
            profile_key=profile_key,
            mode=mode,
            limit=500,
            only_pending=True,
        )
        positions: dict[str, float] = {}
        cash_reserved = 0.0
        details: list[dict[str, Any]] = []
        for row in rows:
            order_payload = self._json_dict(row.order_payload)
            result_payload = self._json_dict(row.result_payload)
            symbol = str(row.symbol or order_payload.get("symbol") or "")
            side = str(row.side or order_payload.get("side") or "").upper()
            if not symbol or side not in {"BUY", "SELL"}:
                continue
            order_quantity = float(order_payload.get("quantity") or row.quantity or 0.0)
            filled_quantity = float(result_payload.get("filled_quantity") or 0.0)
            remaining = result_payload.get("remaining_quantity")
            remaining_quantity = float(remaining if remaining is not None else max(0.0, order_quantity - filled_quantity))
            if remaining_quantity <= 0:
                continue
            price = float(row.reference_price or order_payload.get("reference_price") or order_payload.get("price") or 0.0)
            delta = remaining_quantity if side == "BUY" else -remaining_quantity
            positions[symbol] = positions.get(symbol, 0.0) + delta
            if side == "BUY":
                cash_reserved += remaining_quantity * price
            details.append(
                {
                    "record_id": row.record_id,
                    "order_id": row.order_id,
                    "symbol": symbol,
                    "side": side,
                    "remaining_quantity": remaining_quantity,
                    "reference_price": price,
                    "cash_reserved": remaining_quantity * price if side == "BUY" else 0.0,
                    "status": row.status,
                }
            )
        return {
            "count": len(details),
            "positions": positions,
            "cash_effect": -round(cash_reserved, 2),
            "cash_reserved": round(cash_reserved, 2),
            "details": details,
        }

    def _apply_pending_order_effect(
        self,
        account: LiveAccountSnapshot,
        positions: dict[str, float],
        pending_effect: dict[str, Any],
        price_map: dict[str, float],
    ) -> tuple[LiveAccountSnapshot, dict[str, float], dict[str, Any]]:
        effective_positions = {symbol: float(qty or 0.0) for symbol, qty in positions.items()}
        account_positions = {symbol: dict(position or {}) for symbol, position in account.positions.items()}
        deltas = {str(symbol): float(delta or 0.0) for symbol, delta in self._json_dict(pending_effect.get("positions")).items()}
        for symbol, delta in deltas.items():
            current_qty = float(effective_positions.get(symbol, 0.0) or 0.0)
            next_qty = max(0.0, current_qty + delta)
            if next_qty <= 1e-6:
                effective_positions.pop(symbol, None)
                account_positions.pop(symbol, None)
                continue
            effective_positions[symbol] = next_qty
            position = dict(account_positions.get(symbol) or {"symbol": symbol})
            strict_realtime = self._json_dict(account.meta).get("price_source") == "qmt_realtime_quotes"
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price <= 0 and not strict_realtime:
                price = float(position.get("last_price", 0.0) or position.get("reference_price", 0.0) or position.get("avg_cost", 0.0) or 0.0)
            position["quote_error"] = "realtime_quote_missing" if strict_realtime and price <= 0 else position.get("quote_error")
            available = float(position.get("available", position.get("quantity", current_qty)) or 0.0)
            position.update(
                {
                    "symbol": symbol,
                    "quantity": next_qty,
                    "available": max(0.0, min(next_qty, available + min(delta, 0.0))),
                    "reference_price": price,
                    "last_price": price,
                    "market_value": round(next_qty * price, 2) if price > 0 else float(position.get("market_value", 0.0) or 0.0),
                    "includes_pending_order": True,
                }
            )
            account_positions[symbol] = position
        market_value = round(sum(float(position.get("market_value", 0.0) or 0.0) for position in account_positions.values()), 2)
        cash = round(float(account.cash or 0.0) + float(pending_effect.get("cash_effect") or 0.0), 2)
        adjusted = LiveAccountSnapshot(
            cash=cash,
            total_asset=round(cash + market_value, 2),
            market_value=market_value,
            positions=account_positions,
            source=f"{account.source}+pending_orders" if pending_effect.get("count") else account.source,
            error=account.error,
            meta={**self._json_dict(account.meta), "pending_order_adjusted": bool(pending_effect.get("count"))},
        )
        adjustment = {
            "count": int(pending_effect.get("count") or 0),
            "cash_reserved": float(pending_effect.get("cash_reserved") or 0.0),
            "cash_effect": float(pending_effect.get("cash_effect") or 0.0),
            "position_deltas": deltas,
            "details": pending_effect.get("details") or [],
        }
        return adjusted, effective_positions, adjustment

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

    async def _close_live_trade_record_in_session(
        self,
        session: Any,
        row: LiveTradeRecord,
        *,
        status: str,
        trigger_source: str,
        message: str,
        reason: str,
    ) -> dict[str, Any] | None:
        db_row = await session.scalar(select(LiveTradeRecord).where(LiveTradeRecord.record_id == row.record_id))
        if db_row is None:
            return None
        result_payload = self._json_dict(db_row.result_payload)
        quantity = float(db_row.quantity or 0.0)
        filled_quantity = float(result_payload.get("filled_quantity") or 0.0)
        remaining_quantity = max(0.0, quantity - filled_quantity)
        result_payload.update(
            {
                "submitted": True,
                "pending": False,
                "status": status,
                "order_id": db_row.order_id,
                "message": message,
                "filled_quantity": filled_quantity,
                "remaining_quantity": 0.0,
                "local_close_reason": reason,
                "local_close_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        db_row.status = status
        db_row.message = message
        db_row.result_payload = result_payload
        session.add(db_row)
        session.add(
            LiveOrderAudit(
                audit_id=f"audit-{uuid.uuid4().hex}",
                run_id=db_row.run_id,
                profile_key=db_row.profile_key,
                strategy_id=db_row.strategy_id,
                trade_date=db_row.trade_date,
                signal_hash=db_row.signal_hash,
                trigger_source=trigger_source,
                mode=db_row.mode,
                status=status,
                order_payload=self._json_dict(db_row.order_payload),
                result_payload=self._json_dict(db_row.result_payload),
                skip_reason=message,
            )
        )
        return {
            "record_id": db_row.record_id,
            "order_id": db_row.order_id,
            "symbol": db_row.symbol,
            "status": status,
            "filled_quantity": filled_quantity,
            "remaining_quantity_before_close": remaining_quantity,
            "message": message,
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

    def _order(
        self,
        bundle: StrategyProfileBundle,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        remark: str,
        *,
        attribution: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
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
            "attribution": attribution or {"summary": remark},
        }

    @staticmethod
    def _submission_order_sort(orders: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        side_rank = {"SELL": 0, "BUY": 1}
        return sorted(
            [dict(order) for order in orders],
            key=lambda order: (
                side_rank.get(str(order.get("side") or "").upper(), 9),
                str(order.get("symbol") or ""),
            ),
        )

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
        order_payload = self._json_dict(row.order_payload)
        result_payload = self._json_dict(row.result_payload)
        nested_order = self._json_dict(result_payload.get("order"))
        symbol = str(order_payload.get("symbol") or nested_order.get("symbol") or result_payload.get("symbol") or "")
        side = str(order_payload.get("side") or nested_order.get("side") or result_payload.get("side") or "")
        quantity = self._float_or_none(order_payload.get("quantity") or nested_order.get("quantity") or result_payload.get("quantity"))
        filled_quantity = self._float_or_none(result_payload.get("filled_quantity"))
        reference_price = self._float_or_none(
            result_payload.get("filled_price")
            or order_payload.get("reference_price")
            or order_payload.get("price")
            or nested_order.get("reference_price")
            or nested_order.get("price")
        )
        filled_price = self._float_or_none(result_payload.get("filled_price"))
        order_value = self._float_or_none(result_payload.get("filled_value") or order_payload.get("order_value"))
        if order_value is None and quantity is not None and reference_price is not None:
            order_value = round(quantity * reference_price, 2)
        message = (
            row.skip_reason
            or str(result_payload.get("message") or "")
            or str(order_payload.get("reason") or order_payload.get("remark") or "")
            or None
        )
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
            "symbol": symbol or None,
            "stock_name": (
                str(order_payload.get("stock_name") or nested_order.get("stock_name") or result_payload.get("stock_name") or "")
                or None
            ),
            "side": side.upper() if side else None,
            "quantity": quantity,
            "filled_quantity": filled_quantity,
            "remaining_quantity": self._float_or_none(result_payload.get("remaining_quantity")),
            "reference_price": reference_price,
            "filled_price": filled_price,
            "order_value": order_value,
            "order_id": str(result_payload.get("order_id") or order_payload.get("order_id") or "") or None,
            "message": message,
            "event_type": self._audit_event_type(row.status, row.trigger_source),
            "order_payload": order_payload,
            "result_payload": result_payload,
            "skip_reason": row.skip_reason,
            "created_at": row.created_at.isoformat(timespec="seconds") if row.created_at else None,
        }

    def _audit_event_type(self, status: str | None, trigger_source: str | None) -> str:
        status_value = str(status or "")
        source_value = str(trigger_source or "")
        if status_value == "generated":
            return "signal_generated"
        if status_value == "skipped":
            return "order_skipped"
        if status_value in {"strategy_account_initialized", "strategy_account_adjusted"}:
            return "capital_pool"
        if source_value in {"manual_cancel", "cancel_resubmit"} or status_value.startswith("cancel"):
            return "cancel"
        if source_value == "qmt_sync" or status_value in LIVE_FILLED_STATUSES or status_value in LIVE_PENDING_STATUSES:
            return "execution_update"
        if status_value in {"blocked", "duplicate", "failed"}:
            return "guardrail"
        return "control"

    def _float_or_none(self, value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if pd.notna(number) else None

    def _first_float(self, source: dict[str, Any], keys: Sequence[str]) -> float | None:
        for key in keys:
            value = self._float_or_none(source.get(key))
            if value is not None:
                return value
        return None

    def _normalized_ratio(self, value: Any, *, percent_like: bool = True) -> float | None:
        number = self._float_or_none(value)
        if number is None:
            return None
        if percent_like and abs(number) > 1:
            return number / 100.0
        return number

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

    def _missing_realtime_price_symbols(self, symbols: Sequence[str], price_map: dict[str, float]) -> list[str]:
        missing: list[str] = []
        for symbol in sorted({str(item).strip() for item in symbols if str(item).strip()}):
            try:
                price = float(price_map.get(symbol, 0.0) or 0.0)
            except (TypeError, ValueError):
                price = 0.0
            if price <= 0:
                missing.append(symbol)
        return missing

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

    def _normalize_order_id(self, value: Any) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            numeric = float(raw)
            if numeric.is_integer():
                return str(int(numeric))
        except Exception:
            pass
        return raw

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

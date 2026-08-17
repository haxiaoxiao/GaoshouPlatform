"""Persistent simulated-trading service for the intraday T strategy.

This module intentionally contains no broker order-submission path.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import asdict
from datetime import datetime, time, timedelta
from typing import Any, Awaitable, Callable
from uuid import uuid4

import pandas as pd
from sqlalchemy import delete, select

from app.core.blocking import run_blocking
from app.data_stores import get_market_data_store
from app.db.models.intraday_t import IntradayTSession, IntradayTTrade
from app.db.sqlite import async_session_factory
from app.engines.qmt_gateway import qmt_gateway
from app.services.intraday_t_strategy import (
    SUPPORTED_SYMBOLS,
    CostModel,
    Fill,
    IntradayTStrategy,
    MarketSnapshot,
    OrderIntent,
    StrategyParams,
    SymbolDayState,
    TDirection,
    TState,
    compute_intraday_features,
)
from app.services.qmt_trading import qmt_trading_service

Provider = Callable[..., Awaitable[Any] | Any]


def _quote_datetime(value: Any) -> datetime | None:
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.astimezone().replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            value = float(stripped)
        except ValueError:
            try:
                parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
            except ValueError:
                return None
            return parsed.astimezone().replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    if isinstance(value, (int, float)):
        number = float(value)
        if not pd.notna(number) or number <= 0:
            return None
        if number >= 10_000_000_000:
            number /= 1_000
        try:
            return datetime.fromtimestamp(number)
        except (OSError, OverflowError, ValueError):
            return None
    return None


def _quote_matches_minute(quote: dict[str, Any], minute: datetime) -> bool:
    raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
    candidates = (
        quote.get("quote_time"),
        quote.get("time"),
        quote.get("timetag"),
        quote.get("datetime"),
        raw.get("time"),
        raw.get("timetag"),
        raw.get("datetime"),
    )
    present = [value for value in candidates if value not in (None, "")]
    if not present:
        return True
    quote_time = next((parsed for value in present if (parsed := _quote_datetime(value))), None)
    if quote_time is None:
        return False
    return quote_time.replace(second=0, microsecond=0) == minute.replace(second=0, microsecond=0)


def append_realtime_quote_bars(
    frame: pd.DataFrame,
    quotes: dict[str, dict[str, Any]],
    now: datetime,
) -> pd.DataFrame:
    """Append one synthetic current-minute bar per warmed symbol using quote price only."""
    values = frame.copy()
    if not isinstance(values.index, pd.DatetimeIndex):
        if "datetime" not in values.columns:
            raise ValueError("minute data requires datetime")
        values.index = pd.to_datetime(values.pop("datetime"))
    minute = pd.Timestamp(now.replace(second=0, microsecond=0))
    feature_marker = {"zscore", "previous_zscore", "ready"}
    appended: list[pd.DataFrame] = []
    for symbol, quote in quotes.items():
        if not _quote_matches_minute(quote, now):
            continue
        group = values.loc[values["symbol"] == symbol].sort_index()
        if group.empty or group.index[-1].date() != minute.date():
            continue
        if feature_marker.issubset(group.columns) and group.index[-1] == minute:
            continue
        price = 0.0
        for key in ("lastPrice", "last_price", "price", "close"):
            try:
                price = float(quote.get(key) or 0.0)
            except (TypeError, ValueError):
                continue
            if price > 0:
                break
        if price <= 0 or group.index[-1] > minute:
            continue
        recent = group.tail(5)
        positive_volume = pd.to_numeric(recent["volume"], errors="coerce")
        positive_volume = positive_volume.loc[positive_volume > 0]
        volume = float(positive_volume.median()) if not positive_volume.empty else 1.0
        unit = 1.0
        denominators = pd.to_numeric(recent["close"], errors="coerce") * pd.to_numeric(
            recent["volume"], errors="coerce"
        )
        implied_units = pd.to_numeric(recent.get("amount"), errors="coerce") / denominators
        implied_units = implied_units.loc[implied_units.between(50, 150)]
        if not implied_units.empty:
            unit = 100.0
        row = {
            "symbol": symbol,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
            "amount": price * volume * unit,
        }
        if group.index[-1] == minute:
            for key, value in row.items():
                values.loc[(values.index == minute) & (values["symbol"] == symbol), key] = value
        else:
            appended.append(
                pd.DataFrame([row], index=pd.DatetimeIndex([minute], name=values.index.name))
            )
    if appended:
        values = pd.concat([values, *appended], sort=False)
    return values.sort_index(kind="stable")


class IntradayTPaperService:
    def __init__(
        self,
        *,
        session_factory: Any = async_session_factory,
        market_provider: Provider | None = None,
        quote_provider: Provider | None = None,
        account_provider: Provider | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._market_provider = market_provider or self._default_market_provider
        self._quote_provider = quote_provider or qmt_gateway.get_realtime_quotes
        self._account_provider = account_provider or qmt_trading_service.account_snapshot
        self._runner_tasks: dict[str, asyncio.Task[None]] = {}
        self._evaluation_locks: dict[str, asyncio.Lock] = {}

    async def start(
        self,
        *,
        manual_account: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now()
        strategy_params = self._strategy_params(params or {})
        async with self._session_factory() as db:
            existing = await db.scalar(
                select(IntradayTSession)
                .where(IntradayTSession.status == "RUNNING")
                .order_by(IntradayTSession.created_at.desc())
                .limit(1)
            )
            if existing is not None:
                raise ValueError(f"paper session already running: {existing.session_id}")

        account = manual_account or self._account_dict(
            await self._resolve(self._account_provider())
        )
        source = "manual" if manual_account is not None else str(account.get("source") or "qmt")
        positions = dict(account.get("positions") or {})
        states: dict[str, SymbolDayState] = {}
        normalized_positions: dict[str, dict[str, Any]] = {}
        for symbol in SUPPORTED_SYMBOLS:
            position = self._position_dict(positions.get(symbol))
            quantity = max(0, int(float(position.get("quantity", 0) or 0)))
            available = min(quantity, max(0, int(float(position.get("available", quantity) or 0))))
            normalized_positions[symbol] = {
                "quantity": quantity,
                "available": available,
                "avg_cost": float(position.get("avg_cost", 0.0) or 0.0),
            }
            states[symbol] = SymbolDayState(
                symbol=symbol,
                opening_quantity=quantity,
                opening_sellable=available,
                current_quantity=quantity,
                sellable_remaining=available,
            )
        baseline = {
            "cash": float(account.get("cash", 0.0) or 0.0),
            "positions": normalized_positions,
        }
        runtime = {
            "cash": baseline["cash"],
            "states": {symbol: state.as_dict() for symbol, state in states.items()},
            "pending": {},
            "pair_sequence": 0,
            "pair_tracker": {},
        }
        row = IntradayTSession(
            session_id=f"it-{now.strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}",
            trade_date=now.date(),
            status="RUNNING",
            mode="paper",
            account_source=source,
            strategy_params=self._params_dict(strategy_params),
            baseline=baseline,
            runtime_state=runtime,
            runner_active=False,
        )
        async with self._session_factory() as db:
            db.add(row)
            await db.commit()
            await db.refresh(row)
            return self._session_dict(row)

    async def status(self, session_id: str | None = None) -> dict[str, Any]:
        async with self._session_factory() as db:
            query = select(IntradayTSession)
            if session_id:
                query = query.where(IntradayTSession.session_id == session_id)
            else:
                query = query.order_by(IntradayTSession.created_at.desc()).limit(1)
            row = await db.scalar(query)
            if row is None:
                raise ValueError("paper session not found")
            return self._session_dict(row)

    async def evaluate(
        self,
        session_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        lock = self._evaluation_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            return await self._evaluate_locked(session_id, now=now)

    async def _evaluate_locked(
        self,
        session_id: str,
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        now = now or datetime.now()
        minute = now.replace(second=0, microsecond=0)
        async with self._session_factory() as db:
            row = await db.scalar(
                select(IntradayTSession).where(IntradayTSession.session_id == session_id)
            )
            if row is None:
                raise ValueError("paper session not found")
            if row.status != "RUNNING":
                raise ValueError("paper session is not running")
            if row.trade_date > minute.date():
                raise ValueError(
                    f"paper session trade date {row.trade_date.isoformat()} does not match "
                    f"evaluation date {minute.date().isoformat()}"
                )
            if row.last_evaluated_at is not None and row.last_evaluated_at >= minute:
                result = self._session_dict(row)
                result.update({"duplicate": True, "signals": [], "fills": []})
                return result

            params = self._strategy_params(row.strategy_params)
            strategy = IntradayTStrategy(params, CostModel())
            runtime = dict(row.runtime_state or {})
            states = {
                symbol: self._state_from_dict(symbol, value)
                for symbol, value in dict(runtime.get("states") or {}).items()
            }
            pending = dict(runtime.get("pending") or {})
            pair_tracker = dict(runtime.get("pair_tracker") or {})
            cash = float(runtime.get("cash", 0.0) or 0.0)
            pair_sequence = int(runtime.get("pair_sequence", 0) or 0)
            if row.trade_date < minute.date():
                pending = {}
                for state in states.values():
                    self._roll_state_to_new_trade_date(state)
                pair_tracker = {
                    symbol: value
                    for symbol, value in pair_tracker.items()
                    if states[symbol].active_direction is not None
                }
                row.trade_date = minute.date()

            data = await self._resolve(self._market_provider(minute))
            if not isinstance(data, pd.DataFrame) or data.empty:
                raise ValueError("no current-day minute bars available")
            quotes = await self._resolve(self._quote_provider(list(SUPPORTED_SYMBOLS)))
            raw_quote_map = {
                str(item.get("symbol") or item.get("code") or item.get("stock_code")): item
                for item in (quotes or [])
            }
            quote_map = {
                symbol: quote
                for symbol, quote in raw_quote_map.items()
                if _quote_matches_minute(quote, minute)
            }
            data = append_realtime_quote_bars(data, quote_map, minute)
            latest = self._latest_snapshots(
                data,
                params,
                quote_map=quote_map,
                as_of=minute,
                include_unready_symbols={
                    symbol for symbol, state in states.items() if state.active_direction is not None
                },
            )
            fill_results: list[dict[str, Any]] = []
            signal_results: list[dict[str, Any]] = []

            for symbol in SUPPORTED_SYMBOLS:
                state = states[symbol]
                snapshot = latest.get(symbol)
                pending_value = pending.pop(symbol, None)
                skip_decision = False
                if pending_value is not None:
                    intent = self._intent_from_dict(pending_value)
                    is_entry = state.active_direction is None
                    is_next_minute = minute - intent.signal_at == timedelta(minutes=1)
                    if snapshot is None:
                        pending_value = None
                    elif not is_next_minute:
                        pending_value = None
                        if not is_entry:
                            retry = self._renew_restore_intent(intent, snapshot, minute)
                            pending[symbol] = retry.as_dict()
                            signal_results.append(retry.as_dict())
                            skip_decision = True
                    elif is_entry and (
                        not snapshot.limit_price_available or snapshot.at_price_limit
                    ):
                        pending_value = None
                    elif not self._has_sufficient_top_book_depth(
                        quote_map.get(symbol), intent.side, intent.quantity
                    ):
                        pending_value = None
                        if not is_entry:
                            retry = self._renew_restore_intent(intent, snapshot, minute)
                            pending[symbol] = retry.as_dict()
                            signal_results.append(retry.as_dict())
                            skip_decision = True
                    else:
                        fill_price = self._paper_fill_price(
                            intent,
                            quote_map.get(symbol),
                            snapshot,
                        )
                        up_limit, down_limit = self._current_limit_prices(
                            data,
                            symbol,
                            minute,
                            quote_map.get(symbol),
                        )
                        if self._fill_blocked_by_price_limit(
                            intent.side,
                            fill_price,
                            up_limit=up_limit,
                            down_limit=down_limit,
                        ):
                            pending_value = None
                            skip_decision = True
                            if not is_entry:
                                retry = self._renew_restore_intent(intent, snapshot, minute)
                                pending[symbol] = retry.as_dict()
                                signal_results.append(retry.as_dict())
                if pending_value is not None:
                    intent = self._intent_from_dict(pending_value)
                    fill_price = self._paper_fill_price(intent, quote_map.get(symbol), snapshot)
                    fees = self._fees(intent.side, fill_price, intent.quantity)
                    required_cash = fill_price * intent.quantity + fees
                    if intent.side == "BUY" and required_cash > cash:
                        raise ValueError(f"paper cash is insufficient for {symbol}")
                    is_entry = state.active_direction is None
                    if is_entry:
                        pair_sequence += 1
                        pair_id = f"P{pair_sequence:06d}"
                    else:
                        pair_id = str(pair_tracker[symbol]["pair_id"])
                    fill = Fill.from_intent(intent, fill_price=fill_price, filled_at=minute)
                    strategy.apply_fill(state, fill)
                    if intent.side == "BUY":
                        cash -= required_cash
                    else:
                        cash += fill_price * intent.quantity - fees
                    gross_pnl = 0.0
                    net_pnl = 0.0
                    if is_entry:
                        pair_tracker[symbol] = {
                            "pair_id": pair_id,
                            "direction": intent.direction.value,
                            "price": fill_price,
                            "fees": fees,
                            "quantity": intent.quantity,
                        }
                    else:
                        pair = pair_tracker.pop(symbol)
                        if pair["direction"] == TDirection.POSITIVE.value:
                            gross_pnl = (fill_price - float(pair["price"])) * intent.quantity
                        else:
                            gross_pnl = (float(pair["price"]) - fill_price) * intent.quantity
                        net_pnl = gross_pnl - float(pair["fees"]) - fees
                        state.realized_net_pnl += net_pnl
                    leg = "entry" if is_entry else "restore"
                    trade = IntradayTTrade(
                        trade_id=f"itt-{uuid4().hex}",
                        session_id=session_id,
                        trade_date=minute.date(),
                        symbol=symbol,
                        pair_id=pair_id,
                        direction=intent.direction.value,
                        leg=leg,
                        side=intent.side,
                        quantity=intent.quantity,
                        signal_at=intent.signal_at,
                        fill_at=minute,
                        reference_price=intent.reference_price,
                        fill_price=fill_price,
                        fees=fees,
                        gross_pnl=gross_pnl,
                        net_pnl=net_pnl,
                        reason=intent.reason,
                        status="FILLED",
                        idempotency_key=f"{session_id}:{symbol}:{leg}:{minute.isoformat()}",
                        payload={"simulated": True, "broker_order_submitted": False},
                    )
                    db.add(trade)
                    fill_results.append(self._trade_dict(trade))

                if snapshot is not None and not skip_decision:
                    decision = strategy.decide(snapshot, state, minute)
                    if decision is not None:
                        queue_decision = state.active_direction is not None
                        if not queue_decision and self._has_sufficient_top_book_depth(
                            quote_map.get(symbol), decision.side, decision.quantity
                        ):
                            quoted_price = self._paper_fill_price(
                                decision,
                                quote_map.get(symbol),
                                snapshot,
                            )
                            up_limit, down_limit = self._current_limit_prices(
                                data,
                                symbol,
                                minute,
                                quote_map.get(symbol),
                            )
                            queue_decision = not self._fill_blocked_by_price_limit(
                                decision.side,
                                quoted_price,
                                up_limit=up_limit,
                                down_limit=down_limit,
                            )
                        if queue_decision:
                            pending[symbol] = decision.as_dict()
                            signal_results.append(decision.as_dict())

            runtime.update(
                {
                    "cash": cash,
                    "states": {symbol: state.as_dict() for symbol, state in states.items()},
                    "pending": pending,
                    "pair_sequence": pair_sequence,
                    "pair_tracker": pair_tracker,
                }
            )
            row.runtime_state = runtime
            row.last_evaluated_at = minute
            row.last_error = None
            await db.commit()
            result = self._session_dict(row)
            result.update(
                {
                    "duplicate": False,
                    "signals": signal_results,
                    "fills": fill_results,
                }
            )
            return result

    async def stop(self, session_id: str, *, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now()
        if self._runner_is_active(session_id):
            await self.stop_runner(session_id)
        async with self._session_factory() as db:
            row = await db.scalar(
                select(IntradayTSession).where(IntradayTSession.session_id == session_id)
            )
            if row is None:
                raise ValueError("paper session not found")
            runtime = dict(row.runtime_state or {})
            states = dict(runtime.get("states") or {})
            unrestored = [
                symbol
                for symbol, state in states.items()
                if int(state.get("current_quantity", 0)) != int(state.get("opening_quantity", 0))
                or state.get("active_direction") is not None
            ]
            if unrestored:
                raise ValueError(f"restore base position before stopping: {', '.join(unrestored)}")
            runtime["pending"] = {}
            row.runtime_state = runtime
            row.status = "STOPPED"
            row.runner_active = False
            row.last_evaluated_at = now.replace(second=0, microsecond=0)
            await db.commit()
            return self._session_dict(row)

    async def start_runner(
        self,
        session_id: str,
        *,
        interval_seconds: int = 30,
        immediate: bool = True,
    ) -> dict[str, Any]:
        if not 5 <= interval_seconds <= 3_600:
            raise ValueError("runner interval must be between 5 and 3600 seconds")
        if self._runner_is_active(session_id):
            return await self.status(session_id)
        async with self._session_factory() as db:
            row = await db.scalar(
                select(IntradayTSession).where(IntradayTSession.session_id == session_id)
            )
            if row is None:
                raise ValueError("paper session not found")
            if row.status != "RUNNING":
                raise ValueError("paper session is not running")
            row.runner_active = True
            row.last_error = None
            await db.commit()
        task = asyncio.create_task(
            self._runner_loop(
                session_id,
                interval_seconds=interval_seconds,
                immediate=immediate,
            ),
            name=f"intraday-t:{session_id}",
        )
        self._runner_tasks[session_id] = task
        return await self.status(session_id)

    async def stop_runner(self, session_id: str) -> dict[str, Any]:
        task = self._runner_tasks.pop(session_id, None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        async with self._session_factory() as db:
            row = await db.scalar(
                select(IntradayTSession).where(IntradayTSession.session_id == session_id)
            )
            if row is None:
                raise ValueError("paper session not found")
            row.runner_active = False
            await db.commit()
            return self._session_dict(row)

    async def reset(self, session_id: str) -> dict[str, Any]:
        async with self._session_factory() as db:
            row = await db.scalar(
                select(IntradayTSession).where(IntradayTSession.session_id == session_id)
            )
            if row is None:
                raise ValueError("paper session not found")
            if row.status == "RUNNING":
                raise ValueError("stop paper session before reset")
            states = {}
            for symbol, position in dict(row.baseline.get("positions") or {}).items():
                quantity = int(position.get("quantity", 0))
                available = int(position.get("available", quantity))
                states[symbol] = SymbolDayState(
                    symbol=symbol,
                    opening_quantity=quantity,
                    opening_sellable=available,
                    current_quantity=quantity,
                    sellable_remaining=available,
                ).as_dict()
            row.runtime_state = {
                "cash": float(row.baseline.get("cash", 0.0)),
                "states": states,
                "pending": {},
                "pair_sequence": 0,
                "pair_tracker": {},
            }
            row.last_evaluated_at = None
            row.last_error = None
            await db.execute(delete(IntradayTTrade).where(IntradayTTrade.session_id == session_id))
            await db.commit()
            return self._session_dict(row)

    async def trades(self, session_id: str) -> list[dict[str, Any]]:
        async with self._session_factory() as db:
            rows = (
                await db.scalars(
                    select(IntradayTTrade)
                    .where(IntradayTTrade.session_id == session_id)
                    .order_by(IntradayTTrade.fill_at, IntradayTTrade.id)
                )
            ).all()
            return [self._trade_dict(row) for row in rows]

    async def _runner_loop(
        self,
        session_id: str,
        *,
        interval_seconds: int,
        immediate: bool,
    ) -> None:
        if not immediate:
            await asyncio.sleep(interval_seconds)
        while True:
            try:
                await self.evaluate(session_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._record_runner_error(session_id, f"{type(exc).__name__}: {exc}")
            await asyncio.sleep(interval_seconds)

    async def _record_runner_error(self, session_id: str, message: str) -> None:
        async with self._session_factory() as db:
            row = await db.scalar(
                select(IntradayTSession).where(IntradayTSession.session_id == session_id)
            )
            if row is not None:
                row.last_error = message
                await db.commit()

    def _runner_is_active(self, session_id: str) -> bool:
        task = self._runner_tasks.get(session_id)
        return task is not None and not task.done()

    async def _default_market_provider(self, now: datetime) -> pd.DataFrame:
        start = now.replace(hour=9, minute=0, second=0, microsecond=0)
        return await run_blocking(
            get_market_data_store().load_minute,
            list(SUPPORTED_SYMBOLS),
            start,
            now,
        )

    @staticmethod
    async def _resolve(value: Any) -> Any:
        return await value if inspect.isawaitable(value) else value

    @staticmethod
    def _account_dict(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if hasattr(value, "as_dict"):
            return value.as_dict()
        raise ValueError("account provider returned an unsupported snapshot")

    @staticmethod
    def _position_dict(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if hasattr(value, "as_dict"):
            return value.as_dict()
        raise ValueError("position snapshot has unsupported shape")

    def _latest_snapshots(
        self,
        frame: pd.DataFrame,
        params: StrategyParams,
        *,
        quote_map: dict[str, dict[str, Any]] | None = None,
        as_of: datetime | None = None,
        include_unready_symbols: set[str] | None = None,
    ) -> dict[str, MarketSnapshot]:
        values = frame.copy()
        if not isinstance(values.index, pd.DatetimeIndex):
            if "datetime" not in values.columns:
                raise ValueError("minute data requires datetime")
            values.index = pd.to_datetime(values.pop("datetime"))
        snapshots: dict[str, MarketSnapshot] = {}
        include_unready_symbols = include_unready_symbols or set()
        as_of_minute = (
            pd.Timestamp(as_of.replace(second=0, microsecond=0)) if as_of is not None else None
        )
        feature_columns = {
            "vwap",
            "zscore",
            "previous_zscore",
            "fast_ema",
            "slow_ema",
            "vwap_slope",
            "volume_ratio",
            "estimated_edge_bps",
            "previous_price",
            "realized_vol_bps",
            "session_return_bps",
            "ready",
        }
        for symbol, group in values.groupby("symbol"):
            if not feature_columns.issubset(group.columns):
                group = compute_intraday_features(group, params)
                group["vwap"] = group["session_vwap"]
            group = group.sort_index()
            if as_of_minute is not None:
                group = group.loc[group.index.floor("min") == as_of_minute]
            if group.empty:
                continue
            latest = group.iloc[-1]
            symbol_key = str(symbol)
            if not bool(latest["ready"]) and symbol_key not in include_unready_symbols:
                continue
            quote = (quote_map or {}).get(symbol_key)
            up_limit, down_limit = self._explicit_price_limits(latest, quote)
            snapshots[symbol_key] = MarketSnapshot(
                symbol=symbol_key,
                price=float(latest["close"]),
                vwap=self._float_or_nan(latest.get("vwap")),
                zscore=self._float_or_nan(latest.get("zscore")),
                previous_zscore=self._float_or_nan(latest.get("previous_zscore")),
                fast_ema=self._float_or_nan(latest.get("fast_ema")),
                slow_ema=self._float_or_nan(latest.get("slow_ema")),
                vwap_slope=self._float_or_nan(latest.get("vwap_slope")),
                volume_ratio=self._float_or_nan(latest.get("volume_ratio")),
                estimated_edge_bps=self._float_or_nan(latest.get("estimated_edge_bps")),
                previous_price=self._float_or_nan(latest.get("previous_price")),
                realized_vol_bps=self._float_or_nan(latest.get("realized_vol_bps")),
                session_return_bps=self._float_or_nan(latest.get("session_return_bps")),
                limit_price_available=up_limit is not None and down_limit is not None,
                at_price_limit=self._at_price_limit(
                    float(latest["close"]), up_limit=up_limit, down_limit=down_limit
                ),
            )
        return snapshots

    @classmethod
    def _explicit_price_limits(
        cls,
        row: pd.Series,
        quote: dict[str, Any] | None,
    ) -> tuple[float | None, float | None]:
        quote = quote or {}
        raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
        up = cls._first_positive_number(
            row.get("up_limit"),
            quote.get("upStopPrice"),
            quote.get("UpStopPrice"),
            quote.get("upperLimit"),
            quote.get("limitUp"),
            raw.get("upStopPrice"),
            raw.get("UpStopPrice"),
            raw.get("upperLimit"),
            raw.get("limitUp"),
        )
        down = cls._first_positive_number(
            row.get("down_limit"),
            quote.get("downStopPrice"),
            quote.get("DownStopPrice"),
            quote.get("lowerLimit"),
            quote.get("limitDown"),
            raw.get("downStopPrice"),
            raw.get("DownStopPrice"),
            raw.get("lowerLimit"),
            raw.get("limitDown"),
        )
        return up, down

    @staticmethod
    def _at_price_limit(
        price: float,
        *,
        up_limit: float | None,
        down_limit: float | None,
    ) -> bool:
        half_tick = 0.005
        return bool(
            (up_limit is not None and price >= up_limit - half_tick)
            or (down_limit is not None and price <= down_limit + half_tick)
        )

    @classmethod
    def _current_limit_prices(
        cls,
        frame: pd.DataFrame,
        symbol: str,
        minute: datetime,
        quote: dict[str, Any] | None,
    ) -> tuple[float | None, float | None]:
        values = frame
        if not isinstance(values.index, pd.DatetimeIndex):
            values = values.copy()
            values.index = pd.to_datetime(values.pop("datetime"))
        current = values.loc[
            (values["symbol"] == symbol)
            & (values.index.floor("min") == pd.Timestamp(minute.replace(second=0, microsecond=0)))
        ]
        row = current.sort_index().iloc[-1] if not current.empty else pd.Series(dtype=object)
        return cls._explicit_price_limits(row, quote)

    @staticmethod
    def _fill_blocked_by_price_limit(
        side: str,
        price: float,
        *,
        up_limit: float | None,
        down_limit: float | None,
    ) -> bool:
        half_tick = 0.005
        return bool(
            (side == "BUY" and up_limit is not None and price >= up_limit - half_tick)
            or (side == "SELL" and down_limit is not None and price <= down_limit + half_tick)
        )

    @classmethod
    def _has_sufficient_top_book_depth(
        cls,
        quote: dict[str, Any] | None,
        side: str,
        quantity: int,
    ) -> bool:
        quote = quote or {}
        raw = quote.get("raw") if isinstance(quote.get("raw"), dict) else {}
        keys = ("ask_volume", "askVol") if side == "BUY" else ("bid_volume", "bidVol")
        for source in (quote, raw):
            for key in keys:
                value = source.get(key)
                if isinstance(value, (list, tuple)):
                    value = value[0] if value else None
                try:
                    available = int(float(value))
                except (TypeError, ValueError):
                    continue
                return available >= quantity
        return False

    @staticmethod
    def _first_positive_number(*values: Any) -> float | None:
        for value in values:
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if number > 0:
                return number
        return None

    @staticmethod
    def _float_or_nan(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")

    @staticmethod
    def _renew_restore_intent(
        intent: OrderIntent,
        snapshot: MarketSnapshot,
        minute: datetime,
    ) -> OrderIntent:
        return OrderIntent(
            symbol=intent.symbol,
            side=intent.side,
            quantity=intent.quantity,
            direction=intent.direction,
            reason=intent.reason,
            signal_at=minute,
            reference_price=snapshot.price,
        )

    @staticmethod
    def _paper_fill_price(
        intent: OrderIntent,
        quote: dict[str, Any] | None,
        snapshot: MarketSnapshot | None,
    ) -> float:
        quote = quote or {}
        keys = ("askPrice", "ask_price") if intent.side == "BUY" else ("bidPrice", "bid_price")
        for key in keys:
            value = quote.get(key)
            if isinstance(value, (list, tuple)) and value:
                value = value[0]
            try:
                price = float(value)
            except (TypeError, ValueError):
                continue
            if price > 0:
                return price
        for key in ("lastPrice", "last_price", "price"):
            try:
                price = float(quote.get(key))
            except (TypeError, ValueError):
                continue
            if price > 0:
                direction = 1 if intent.side == "BUY" else -1
                return price * (1 + direction * CostModel().slippage_bps / 10_000)
        if snapshot is None:
            raise ValueError(f"no simulated fill price for {intent.symbol}")
        direction = 1 if intent.side == "BUY" else -1
        return snapshot.price * (1 + direction * CostModel().slippage_bps / 10_000)

    @staticmethod
    def _fees(side: str, price: float, quantity: int) -> float:
        cost = CostModel()
        notional = price * quantity
        return (
            max(cost.min_commission, notional * cost.commission_rate)
            + notional * cost.transfer_fee_rate
            + (notional * cost.stamp_duty_rate if side == "SELL" else 0.0)
        )

    @staticmethod
    def _strategy_params(raw: dict[str, Any]) -> StrategyParams:
        allowed = set(StrategyParams.__dataclass_fields__)
        values = {key: value for key, value in raw.items() if key in allowed}
        for key in (
            "morning_entry_start",
            "morning_entry_end",
            "afternoon_entry_start",
            "afternoon_entry_end",
            "lunch_restore_time",
            "force_restore_time",
        ):
            time_value = values.get(key)
            if isinstance(time_value, str):
                values[key] = time.fromisoformat(time_value)
        return StrategyParams(**values)

    @staticmethod
    def _params_dict(params: StrategyParams) -> dict[str, Any]:
        result = asdict(params)
        for key, value in result.items():
            if isinstance(value, time):
                result[key] = value.isoformat()
        return result

    @staticmethod
    def _roll_state_to_new_trade_date(state: SymbolDayState) -> None:
        state.completed_pairs = 0
        state.last_completed_at = None
        state.realized_net_pnl = 0.0
        state.opening_sellable = state.current_quantity
        state.sellable_remaining = state.current_quantity
        if state.active_direction is not None:
            state.state = TState.FORCE_RESTORE
            return
        state.opening_quantity = state.current_quantity
        state.state = TState.READY

    @staticmethod
    def _state_from_dict(symbol: str, value: dict[str, Any]) -> SymbolDayState:
        return SymbolDayState(
            symbol=symbol,
            opening_quantity=int(value["opening_quantity"]),
            opening_sellable=int(value["opening_sellable"]),
            current_quantity=int(value["current_quantity"]),
            sellable_remaining=int(value["sellable_remaining"]),
            state=TState(value.get("state", TState.READY.value)),
            completed_pairs=int(value.get("completed_pairs", 0)),
            active_quantity=int(value.get("active_quantity", 0)),
            active_direction=(
                TDirection(value["active_direction"]) if value.get("active_direction") else None
            ),
            active_entry_price=value.get("active_entry_price"),
            active_entry_at=(
                datetime.fromisoformat(value["active_entry_at"])
                if value.get("active_entry_at")
                else None
            ),
            last_completed_at=(
                datetime.fromisoformat(value["last_completed_at"])
                if value.get("last_completed_at")
                else None
            ),
            realized_net_pnl=float(value.get("realized_net_pnl", 0.0)),
        )

    @staticmethod
    def _intent_from_dict(value: dict[str, Any]) -> OrderIntent:
        return OrderIntent(
            symbol=str(value["symbol"]),
            side=str(value["side"]),
            quantity=int(value["quantity"]),
            direction=TDirection(value["direction"]),
            reason=str(value["reason"]),
            signal_at=datetime.fromisoformat(value["signal_at"]),
            reference_price=float(value["reference_price"]),
        )

    def _session_dict(self, row: IntradayTSession) -> dict[str, Any]:
        runtime = dict(row.runtime_state or {})
        runner_active = self._runner_is_active(row.session_id)
        return {
            "session_id": row.session_id,
            "trade_date": row.trade_date.isoformat(),
            "status": row.status,
            "mode": row.mode,
            "account_source": row.account_source,
            "params": row.strategy_params,
            "baseline": row.baseline,
            "cash": float(runtime.get("cash", 0.0) or 0.0),
            "states": runtime.get("states") or {},
            "pending": runtime.get("pending") or {},
            "last_evaluated_at": (
                row.last_evaluated_at.isoformat() if row.last_evaluated_at else None
            ),
            "last_error": row.last_error,
            "runner_active": runner_active,
            "recoverable": row.status == "RUNNING" and not runner_active,
            "real_order_submit_enabled": False,
        }

    @staticmethod
    def _trade_dict(row: IntradayTTrade) -> dict[str, Any]:
        return {
            "trade_id": row.trade_id,
            "session_id": row.session_id,
            "trade_date": row.trade_date.isoformat(),
            "symbol": row.symbol,
            "name": SUPPORTED_SYMBOLS.get(row.symbol, row.symbol),
            "pair_id": row.pair_id,
            "direction": row.direction,
            "leg": row.leg,
            "side": row.side,
            "quantity": row.quantity,
            "signal_at": row.signal_at.isoformat(),
            "fill_at": row.fill_at.isoformat(),
            "reference_price": float(row.reference_price),
            "fill_price": float(row.fill_price),
            "fees": float(row.fees),
            "gross_pnl": float(row.gross_pnl),
            "net_pnl": float(row.net_pnl),
            "reason": row.reason,
            "status": row.status,
            "simulated": True,
        }


intraday_t_paper_service = IntradayTPaperService()

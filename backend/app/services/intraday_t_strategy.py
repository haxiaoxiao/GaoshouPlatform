"""Pure signal and state-machine domain for conservative A-share intraday T trading."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, time
from enum import StrEnum
from math import isfinite
from typing import Any

import pandas as pd

SUPPORTED_SYMBOLS = {
    "603629.SH": "利通电子",
    "688008.SH": "澜起科技",
}


class TState(StrEnum):
    READY = "READY"
    POSITIVE_T_OPEN = "POSITIVE_T_OPEN"
    REVERSE_T_OPEN = "REVERSE_T_OPEN"
    FORCE_RESTORE = "FORCE_RESTORE"
    RESTORED = "RESTORED"
    LOCKED = "LOCKED"


class TDirection(StrEnum):
    POSITIVE = "POSITIVE"
    REVERSE = "REVERSE"


@dataclass(frozen=True)
class CostModel:
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_duty_rate: float = 0.0005
    transfer_fee_rate: float = 0.00001
    slippage_bps: float = 2.0


@dataclass(frozen=True)
class StrategyParams:
    warmup_bars: int = 30
    volatility_window: int = 30
    realized_vol_window: int = 10
    fast_ema_span: int = 10
    slow_ema_span: int = 30
    vwap_slope_bars: int = 5
    entry_z: float = 1.75
    max_entry_z: float = 2.4
    exit_z: float = 0.25
    stop_z: float = 3.0
    max_trade_fraction: float = 0.25
    max_pairs_per_day: int = 1
    cooldown_minutes: int = 20
    edge_buffer_bps: float = 12.0
    min_realized_vol_bps: float = 0.0
    max_adverse_day_move_bps: float | None = None
    require_price_reversal: bool = False
    max_daily_loss_bps: float = 45.0
    strong_trend_ema_gap: float = 0.01
    strong_trend_vwap_slope: float = 0.001
    strong_trend_volume_ratio: float = 1.5
    morning_entry_start: time = time(10, 0)
    morning_entry_end: time = time(10, 30)
    allow_afternoon_entries: bool = False
    afternoon_entry_start: time = time(13, 5)
    afternoon_entry_end: time = time(14, 30)
    lunch_restore_time: time = time(11, 29)
    force_restore_time: time = time(14, 49)

    def __post_init__(self) -> None:
        if self.warmup_bars < 2:
            raise ValueError("warmup_bars must be at least 2")
        if self.warmup_bars > self.volatility_window:
            raise ValueError("warmup_bars must not exceed volatility_window")
        if self.realized_vol_window < 2:
            raise ValueError("realized_vol_window must be at least 2")
        if not self.entry_z < self.max_entry_z < self.stop_z:
            raise ValueError("entry_z < max_entry_z < stop_z is required")
        if not 0 < self.max_trade_fraction <= 0.3:
            raise ValueError("max_trade_fraction must be in (0, 0.3]")
        if self.max_pairs_per_day < 1:
            raise ValueError("max_pairs_per_day must be positive")
        if self.max_daily_loss_bps <= 0:
            raise ValueError("max_daily_loss_bps must be positive")
        if self.min_realized_vol_bps < 0:
            raise ValueError("min_realized_vol_bps must be non-negative")
        if self.max_adverse_day_move_bps is not None and self.max_adverse_day_move_bps <= 0:
            raise ValueError("max_adverse_day_move_bps must be positive when configured")
        if self.morning_entry_start >= self.morning_entry_end:
            raise ValueError("morning entry window must be increasing")
        if self.afternoon_entry_start >= self.afternoon_entry_end:
            raise ValueError("afternoon entry window must be increasing")


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    price: float
    vwap: float
    zscore: float
    previous_zscore: float
    fast_ema: float
    slow_ema: float
    vwap_slope: float
    volume_ratio: float
    estimated_edge_bps: float
    previous_price: float
    realized_vol_bps: float
    session_return_bps: float
    limit_price_available: bool
    at_price_limit: bool


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    side: str
    quantity: int
    direction: TDirection
    reason: str
    signal_at: datetime
    reference_price: float

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["direction"] = self.direction.value
        result["signal_at"] = self.signal_at.isoformat()
        return result


@dataclass(frozen=True)
class Fill:
    symbol: str
    side: str
    quantity: int
    direction: TDirection
    reason: str
    fill_price: float
    filled_at: datetime

    @classmethod
    def from_intent(cls, intent: OrderIntent, *, fill_price: float, filled_at: datetime) -> Fill:
        return cls(
            symbol=intent.symbol,
            side=intent.side,
            quantity=intent.quantity,
            direction=intent.direction,
            reason=intent.reason,
            fill_price=fill_price,
            filled_at=filled_at,
        )


@dataclass
class SymbolDayState:
    symbol: str
    opening_quantity: int
    opening_sellable: int
    current_quantity: int
    sellable_remaining: int
    state: TState = TState.READY
    completed_pairs: int = 0
    active_quantity: int = 0
    active_direction: TDirection | None = None
    active_entry_price: float | None = None
    active_entry_at: datetime | None = None
    last_completed_at: datetime | None = None
    realized_net_pnl: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["state"] = self.state.value
        result["active_direction"] = (
            self.active_direction.value if self.active_direction is not None else None
        )
        for key in ("active_entry_at", "last_completed_at"):
            value = result[key]
            result[key] = value.isoformat() if value is not None else None
        return result


def _is_star_market(symbol: str) -> bool:
    return symbol.startswith("688") and symbol.endswith(".SH")


def normalize_buy_quantity(symbol: str, requested: float) -> int:
    quantity = max(0, int(requested))
    if _is_star_market(symbol):
        return quantity if quantity >= 200 else 0
    return quantity // 100 * 100


def normalize_sell_quantity(symbol: str, requested: float, *, available: int) -> int:
    quantity = min(max(0, int(requested)), max(0, int(available)))
    if quantity <= 0:
        return 0
    if _is_star_market(symbol):
        if quantity >= 200 or quantity == int(available):
            return quantity
        return 0
    if quantity == int(available) and quantity < 100:
        return quantity
    return quantity // 100 * 100


def estimate_round_trip_cost_bps(*, price: float, quantity: int, cost: CostModel) -> float:
    if price <= 0 or quantity <= 0:
        return float("inf")
    notional = price * quantity
    commission = max(cost.min_commission, notional * cost.commission_rate) * 2
    stamp_duty = notional * cost.stamp_duty_rate
    transfer_fee = notional * cost.transfer_fee_rate * 2
    slippage = notional * cost.slippage_bps / 10_000 * 2
    return (commission + stamp_duty + transfer_fee + slippage) / notional * 10_000


def compute_intraday_features(frame: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
    required = {"close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"minute frame missing columns: {sorted(missing)}")
    result = frame.sort_index().copy()
    close = pd.to_numeric(result["close"], errors="coerce")
    volume = pd.to_numeric(result["volume"], errors="coerce").fillna(0.0).clip(lower=0.0)
    dates = pd.Series(result.index.date, index=result.index)
    reported_amount = (
        pd.to_numeric(result["amount"], errors="coerce")
        if "amount" in result
        else pd.Series(float("nan"), index=result.index)
    )
    implied_unit = reported_amount / (volume * close).replace(0.0, pd.NA)
    lot_volume = implied_unit.between(50.0, 150.0)
    share_volume = implied_unit.between(0.5, 1.5)
    usable_amount = reported_amount.notna() & (reported_amount > 0) & (lot_volume | share_volume)
    effective_volume = volume.where(~lot_volume, volume * 100.0)
    effective_volume = effective_volume.where(usable_amount, volume)
    effective_amount = reported_amount.where(usable_amount, close * volume)
    cumulative_amount = effective_amount.groupby(dates).cumsum()
    cumulative_volume = effective_volume.groupby(dates).cumsum().replace(0.0, pd.NA)
    result["session_vwap"] = cumulative_amount / cumulative_volume
    result["deviation"] = close / result["session_vwap"] - 1.0
    result["deviation_sigma"] = result.groupby(dates)["deviation"].transform(
        lambda values: values.rolling(
            params.volatility_window,
            min_periods=params.warmup_bars,
        ).std(ddof=0)
    )
    sigma = result["deviation_sigma"].where(result["deviation_sigma"] > 1e-8)
    result["zscore"] = result["deviation"] / sigma
    result["previous_zscore"] = result.groupby(dates)["zscore"].shift(1)
    result["previous_price"] = close.groupby(dates).shift(1)
    minute_returns = close.groupby(dates).pct_change(fill_method=None)
    result["realized_vol_bps"] = minute_returns.groupby(dates).transform(
        lambda values: (
            values.rolling(
                params.realized_vol_window,
                min_periods=min(5, params.realized_vol_window),
            ).std(ddof=0)
            * 10_000
        )
    )
    session_open = close.groupby(dates).transform("first")
    result["session_return_bps"] = (close / session_open - 1.0) * 10_000
    result["fast_ema"] = close.groupby(dates).transform(
        lambda values: values.ewm(span=params.fast_ema_span, adjust=False).mean()
    )
    result["slow_ema"] = close.groupby(dates).transform(
        lambda values: values.ewm(span=params.slow_ema_span, adjust=False).mean()
    )
    result["vwap_slope"] = result.groupby(dates)["session_vwap"].pct_change(
        params.vwap_slope_bars, fill_method=None
    )
    short_volume = volume.groupby(dates).transform(lambda values: values.rolling(5).mean())
    long_volume = volume.groupby(dates).transform(
        lambda values: values.rolling(params.volatility_window).mean()
    )
    result["volume_ratio"] = short_volume / long_volume.replace(0.0, pd.NA)
    result["estimated_edge_bps"] = result["deviation"].abs() * 10_000
    counts = result.groupby(dates).cumcount() + 1
    result["ready"] = counts >= params.warmup_bars
    return result


class IntradayTStrategy:
    def __init__(self, params: StrategyParams | None = None, cost: CostModel | None = None) -> None:
        self.params = params or StrategyParams()
        self.cost = cost or CostModel()

    def decide(
        self,
        snapshot: MarketSnapshot,
        state: SymbolDayState,
        now: datetime,
    ) -> OrderIntent | None:
        if snapshot.symbol != state.symbol or snapshot.symbol not in SUPPORTED_SYMBOLS:
            return None
        if state.state is TState.LOCKED:
            return None
        if state.state in {TState.POSITIVE_T_OPEN, TState.REVERSE_T_OPEN, TState.FORCE_RESTORE}:
            return self._exit_intent(snapshot, state, now)
        daily_loss_limit = (
            state.opening_quantity * snapshot.price * self.params.max_daily_loss_bps / 10_000
        )
        if state.realized_net_pnl <= -daily_loss_limit:
            state.state = TState.LOCKED
            return None
        if state.completed_pairs >= self.params.max_pairs_per_day:
            state.state = TState.LOCKED
            return None
        if not self._in_entry_window(now.time()) or not self._cooldown_complete(state, now):
            return None
        if not self._finite_snapshot(snapshot):
            return None
        if (
            abs(snapshot.zscore) >= self.params.max_entry_z
            or snapshot.realized_vol_bps < self.params.min_realized_vol_bps
            or not snapshot.limit_price_available
            or snapshot.at_price_limit
        ):
            return None

        requested = state.opening_quantity * self.params.max_trade_fraction
        if self._positive_entry(snapshot):
            quantity = normalize_buy_quantity(
                state.symbol,
                min(requested, state.sellable_remaining),
            )
            direction = TDirection.POSITIVE
            side = "BUY"
        elif self._reverse_entry(snapshot):
            quantity = normalize_sell_quantity(
                state.symbol,
                requested,
                available=state.sellable_remaining,
            )
            direction = TDirection.REVERSE
            side = "SELL"
        else:
            return None
        if quantity <= 0:
            return None
        required_edge = (
            estimate_round_trip_cost_bps(
                price=snapshot.price,
                quantity=quantity,
                cost=self.cost,
            )
            + self.params.edge_buffer_bps
        )
        if snapshot.estimated_edge_bps < required_edge:
            return None
        return OrderIntent(
            symbol=state.symbol,
            side=side,
            quantity=quantity,
            direction=direction,
            reason="mean_reversion_entry",
            signal_at=now,
            reference_price=snapshot.price,
        )

    def apply_fill(self, state: SymbolDayState, fill: Fill) -> None:
        if fill.symbol != state.symbol or fill.quantity <= 0:
            raise ValueError("fill does not match state")
        if state.state in {TState.READY, TState.RESTORED}:
            state.active_quantity = fill.quantity
            state.active_direction = fill.direction
            state.active_entry_price = fill.fill_price
            state.active_entry_at = fill.filled_at
            if fill.direction is TDirection.POSITIVE and fill.side == "BUY":
                state.current_quantity += fill.quantity
                state.state = TState.POSITIVE_T_OPEN
                return
            if fill.direction is TDirection.REVERSE and fill.side == "SELL":
                if fill.quantity > state.sellable_remaining:
                    raise ValueError("fill exceeds sellable inventory")
                state.current_quantity -= fill.quantity
                state.sellable_remaining -= fill.quantity
                state.state = TState.REVERSE_T_OPEN
                return
            raise ValueError("invalid entry fill")

        if state.active_direction is TDirection.POSITIVE and fill.side == "SELL":
            if fill.quantity > state.sellable_remaining:
                raise ValueError("fill exceeds sellable inventory")
            state.current_quantity -= fill.quantity
            state.sellable_remaining -= fill.quantity
        elif state.active_direction is TDirection.REVERSE and fill.side == "BUY":
            state.current_quantity += fill.quantity
        else:
            raise ValueError("invalid restoration fill")
        if state.current_quantity != state.opening_quantity:
            raise ValueError("restoration fill did not restore opening position")
        state.completed_pairs += 1
        state.last_completed_at = fill.filled_at
        state.state = TState.RESTORED if fill.reason == "force_restore" else TState.READY
        state.active_quantity = 0
        state.active_direction = None
        state.active_entry_price = None
        state.active_entry_at = None

    def _exit_intent(
        self,
        snapshot: MarketSnapshot,
        state: SymbolDayState,
        now: datetime,
    ) -> OrderIntent | None:
        if state.active_direction is None or state.active_quantity <= 0:
            return None
        current_time = now.time()
        forced = state.state is TState.FORCE_RESTORE or (
            self.params.lunch_restore_time <= current_time < time(13, 0)
            or current_time >= self.params.force_restore_time
        )
        if state.active_direction is TDirection.POSITIVE:
            should_exit = snapshot.zscore >= -self.params.exit_z
            stop_loss = snapshot.zscore <= -self.params.stop_z
            side = "SELL"
            available = state.sellable_remaining
            quantity = normalize_sell_quantity(
                state.symbol,
                state.active_quantity,
                available=available,
            )
        else:
            should_exit = snapshot.zscore <= self.params.exit_z
            stop_loss = snapshot.zscore >= self.params.stop_z
            side = "BUY"
            quantity = normalize_buy_quantity(state.symbol, state.active_quantity)
        if not forced and not should_exit and not stop_loss:
            return None
        if quantity != state.active_quantity:
            state.state = TState.LOCKED
            return None
        if forced:
            state.state = TState.FORCE_RESTORE
        return OrderIntent(
            symbol=state.symbol,
            side=side,
            quantity=quantity,
            direction=state.active_direction,
            reason=(
                "force_restore"
                if forced
                else "risk_restore"
                if stop_loss
                else "mean_reversion_exit"
            ),
            signal_at=now,
            reference_price=snapshot.price,
        )

    def _positive_entry(self, snapshot: MarketSnapshot) -> bool:
        reversing = (
            snapshot.zscore <= -self.params.entry_z and snapshot.zscore > snapshot.previous_zscore
        )
        if self.params.require_price_reversal:
            reversing = reversing and snapshot.price > snapshot.previous_price
        if (
            self.params.max_adverse_day_move_bps is not None
            and snapshot.session_return_bps < -self.params.max_adverse_day_move_bps
        ):
            return False
        strong_downtrend = (
            snapshot.fast_ema < snapshot.slow_ema * (1 - self.params.strong_trend_ema_gap)
            and snapshot.vwap_slope <= -self.params.strong_trend_vwap_slope
            and snapshot.volume_ratio >= self.params.strong_trend_volume_ratio
        )
        return reversing and not strong_downtrend

    def _reverse_entry(self, snapshot: MarketSnapshot) -> bool:
        reversing = (
            snapshot.zscore >= self.params.entry_z and snapshot.zscore < snapshot.previous_zscore
        )
        if self.params.require_price_reversal:
            reversing = reversing and snapshot.price < snapshot.previous_price
        if (
            self.params.max_adverse_day_move_bps is not None
            and snapshot.session_return_bps > self.params.max_adverse_day_move_bps
        ):
            return False
        strong_uptrend = (
            snapshot.fast_ema > snapshot.slow_ema * (1 + self.params.strong_trend_ema_gap)
            and snapshot.vwap_slope >= self.params.strong_trend_vwap_slope
            and snapshot.volume_ratio >= self.params.strong_trend_volume_ratio
        )
        return reversing and not strong_uptrend

    def _cooldown_complete(self, state: SymbolDayState, now: datetime) -> bool:
        if state.last_completed_at is None:
            return True
        elapsed = (now - state.last_completed_at).total_seconds() / 60
        return elapsed >= self.params.cooldown_minutes

    def _in_entry_window(self, value: time) -> bool:
        if self.params.morning_entry_start <= value < self.params.morning_entry_end:
            return True
        return self.params.allow_afternoon_entries and (
            self.params.afternoon_entry_start <= value < self.params.afternoon_entry_end
        )

    @staticmethod
    def _finite_snapshot(snapshot: MarketSnapshot) -> bool:
        return snapshot.price > 0 and all(
            isfinite(value)
            for value in (
                snapshot.vwap,
                snapshot.zscore,
                snapshot.previous_zscore,
                snapshot.fast_ema,
                snapshot.slow_ema,
                snapshot.vwap_slope,
                snapshot.volume_ratio,
                snapshot.estimated_edge_bps,
                snapshot.previous_price,
                snapshot.realized_vol_bps,
                snapshot.session_return_bps,
            )
        )

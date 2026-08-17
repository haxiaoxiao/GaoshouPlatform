"""Minute-bar backtester for the dedicated intraday T strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

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
    normalize_buy_quantity,
    normalize_sell_quantity,
)


@dataclass(frozen=True)
class BacktestConfig:
    initial_capital: float = 1_000_000.0
    base_quantities: dict[str, int] = field(default_factory=dict)
    params: StrategyParams = field(default_factory=StrategyParams)
    cost: CostModel = field(default_factory=CostModel)
    decision_cost: CostModel | None = None
    max_bar_volume_fraction: float = 0.05
    cash_buffer_fraction: float = 0.30
    limit_prices: dict[str, dict[str, float]] = field(default_factory=dict)
    require_exact_limit_prices: bool = False

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if not 0 < self.max_bar_volume_fraction <= 1:
            raise ValueError("max_bar_volume_fraction must be in (0, 1]")
        unknown = set(self.base_quantities) - set(SUPPORTED_SYMBOLS)
        if unknown:
            raise ValueError(f"unsupported symbols: {sorted(unknown)}")


class IntradayTBacktester:
    _FEATURE_COLUMNS = {
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

    def run(
        self, minute_data: pd.DataFrame, config: BacktestConfig | None = None
    ) -> dict[str, Any]:
        config = config or BacktestConfig()
        frame = self._prepare_frame(minute_data, config.params)
        if frame.empty:
            raise ValueError("minute data is empty")
        symbols = list(dict.fromkeys(frame["symbol"].astype(str).tolist()))
        unknown = set(symbols) - set(SUPPORTED_SYMBOLS)
        if unknown:
            raise ValueError(f"unsupported symbols: {sorted(unknown)}")

        initial_prices = {
            symbol: float(frame.loc[frame["symbol"] == symbol, "open"].iloc[0])
            for symbol in symbols
        }
        base_quantities = self._base_quantities(symbols, initial_prices, config)
        base_value = sum(initial_prices[symbol] * base_quantities[symbol] for symbol in symbols)
        if base_value > config.initial_capital:
            raise ValueError("initial capital is below opening base-position value")
        cash = config.initial_capital - base_value
        passive_cash = cash
        holdings = dict(base_quantities)
        last_marks = dict(initial_prices)
        strategy = IntradayTStrategy(config.params, config.decision_cost or config.cost)
        trades: list[dict[str, Any]] = []
        rejections: list[dict[str, Any]] = []
        equity_curve: list[dict[str, Any]] = []
        pair_tracker: dict[str, dict[str, Any]] = {}
        pair_sequence = 0
        total_fees = 0.0
        total_entries = 0
        total_pairs = 0
        restoration_failure_ids: set[str] = set()
        states = {
            symbol: SymbolDayState(
                symbol=symbol,
                opening_quantity=base_quantities[symbol],
                opening_sellable=holdings[symbol],
                current_quantity=holdings[symbol],
                sellable_remaining=holdings[symbol],
            )
            for symbol in symbols
        }

        for trade_date, day_frame in frame.groupby(frame.index.date, sort=True):
            for symbol, state in states.items():
                state.opening_sellable = holdings[symbol]
                state.current_quantity = holdings[symbol]
                state.sellable_remaining = holdings[symbol]
                state.completed_pairs = 0
                state.realized_net_pnl = 0.0
                if state.active_direction is not None:
                    state.state = TState.FORCE_RESTORE
                else:
                    state.state = (
                        TState.READY
                        if holdings[symbol] == base_quantities[symbol]
                        else TState.LOCKED
                    )
            pending: dict[str, OrderIntent] = {}
            for timestamp, row in day_frame.sort_index(kind="stable").iterrows():
                symbol = str(row["symbol"])
                state = states[symbol]
                intent = pending.pop(symbol, None)
                if intent is not None:
                    rejection = self._fill_rejection(intent, row, timestamp, state, config)
                    if rejection is not None:
                        rejections.append(rejection)
                    else:
                        limits = config.limit_prices.get(
                            f"{intent.symbol}|{timestamp.date().isoformat()}",
                            {},
                        )
                        fill_price = self._fill_price(
                            float(row["open"]),
                            intent.side,
                            config.cost,
                            limits,
                        )
                        fees = self._transaction_fees(
                            side=intent.side,
                            price=fill_price,
                            quantity=intent.quantity,
                            cost=config.cost,
                        )
                        required_cash = fill_price * intent.quantity + fees
                        if intent.side == "BUY" and required_cash > cash:
                            rejections.append(
                                self._rejection(intent, timestamp, "insufficient_cash")
                            )
                        else:
                            is_entry = state.active_direction is None
                            if is_entry:
                                pair_sequence += 1
                                pair_id = f"T{pair_sequence:06d}"
                            else:
                                pair_id = str(pair_tracker[symbol]["pair_id"])
                            fill = Fill.from_intent(
                                intent, fill_price=fill_price, filled_at=timestamp
                            )
                            strategy.apply_fill(state, fill)
                            holdings[symbol] = state.current_quantity
                            if intent.side == "BUY":
                                cash -= required_cash
                            else:
                                cash += fill_price * intent.quantity - fees
                            total_fees += fees
                            record = {
                                "pair_id": pair_id,
                                "symbol": symbol,
                                "name": SUPPORTED_SYMBOLS[symbol],
                                "direction": intent.direction.value,
                                "leg": "entry" if is_entry else "restore",
                                "side": intent.side,
                                "quantity": intent.quantity,
                                "signal_at": intent.signal_at.isoformat(),
                                "fill_at": timestamp.isoformat(),
                                "reference_price": round(intent.reference_price, 6),
                                "fill_price": round(fill_price, 6),
                                "fees": round(fees, 6),
                                "reason": intent.reason,
                                "gross_pnl": 0.0,
                                "net_pnl": 0.0,
                            }
                            if is_entry:
                                total_entries += 1
                                pair_tracker[symbol] = {
                                    "pair_id": pair_id,
                                    "direction": intent.direction,
                                    "price": fill_price,
                                    "fees": fees,
                                    "quantity": intent.quantity,
                                }
                            else:
                                pair = pair_tracker.pop(symbol)
                                if pair["direction"] is TDirection.POSITIVE:
                                    gross = (fill_price - float(pair["price"])) * intent.quantity
                                else:
                                    gross = (float(pair["price"]) - fill_price) * intent.quantity
                                net = gross - float(pair["fees"]) - fees
                                state.realized_net_pnl += net
                                record["gross_pnl"] = round(gross, 6)
                                record["net_pnl"] = round(net, 6)
                                total_pairs += 1
                            trades.append(record)

                if not bool(row["ready"]) and state.active_direction is None:
                    continue
                snapshot = self._snapshot(symbol, row, timestamp, config)
                decision = strategy.decide(snapshot, state, timestamp)
                if decision is not None:
                    pending[symbol] = decision

            for symbol, state in states.items():
                holdings[symbol] = state.current_quantity
                if state.active_direction is not None and symbol in pair_tracker:
                    restoration_failure_ids.add(str(pair_tracker[symbol]["pair_id"]))
            for symbol in symbols:
                symbol_closes = day_frame.loc[day_frame["symbol"] == symbol, "close"]
                if not symbol_closes.empty:
                    last_marks[symbol] = float(symbol_closes.iloc[-1])
            marks = dict(last_marks)
            equity = cash + sum(holdings[symbol] * marks[symbol] for symbol in symbols)
            passive = passive_cash + sum(
                base_quantities[symbol] * marks[symbol] for symbol in symbols
            )
            equity_curve.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "equity": round(equity, 4),
                    "passive_equity": round(passive, 4),
                    "incremental_pnl": round(equity - passive, 4),
                }
            )

        final_prices = dict(last_marks)
        final_equity = cash + sum(holdings[symbol] * final_prices[symbol] for symbol in symbols)
        passive_final = passive_cash + sum(
            base_quantities[symbol] * final_prices[symbol] for symbol in symbols
        )
        incremental = final_equity - passive_final
        total_opening_quantity = sum(base_quantities.values())
        completed_trades = [item for item in trades if item["leg"] == "restore"]
        winning_trades = [item for item in completed_trades if float(item["net_pnl"]) > 0]
        losing_trades = [item for item in completed_trades if float(item["net_pnl"]) < 0]
        gross_t_pnl = sum(float(item["gross_pnl"]) for item in completed_trades)
        net_t_pnl = sum(float(item["net_pnl"]) for item in completed_trades)
        gross_wins = sum(float(item["net_pnl"]) for item in winning_trades)
        gross_losses = abs(sum(float(item["net_pnl"]) for item in losing_trades))
        previous_incremental = 0.0
        peak_equity = float("-inf")
        max_drawdown = 0.0
        for point in equity_curve:
            point_incremental = float(point["incremental_pnl"])
            point["daily_t_pnl"] = round(point_incremental - previous_incremental, 4)
            previous_incremental = point_incremental
            point_equity = config.initial_capital + point_incremental
            peak_equity = max(peak_equity, point_equity)
            if peak_equity > 0:
                max_drawdown = min(max_drawdown, point_equity / peak_equity - 1)
        max_daily_t_loss = min(
            (float(point["daily_t_pnl"]) for point in equity_curve),
            default=0.0,
        )
        metrics = {
            "initial_capital": round(config.initial_capital, 4),
            "final_equity": round(final_equity, 4),
            "passive_final_equity": round(passive_final, 4),
            "incremental_pnl": round(incremental, 4),
            "incremental_return": round(incremental / config.initial_capital, 8),
            "cost_reduction_per_share": round(
                incremental / total_opening_quantity if total_opening_quantity else 0.0,
                6,
            ),
            "completed_pairs": total_pairs,
            "entry_count": total_entries,
            "restoration_failures": len(restoration_failure_ids),
            "restoration_rate": (
                round(
                    max(0, total_entries - len(restoration_failure_ids)) / total_entries,
                    6,
                )
                if total_entries
                else 1.0
            ),
            "open_pairs_at_end": len(pair_tracker),
            "total_fees": round(total_fees, 4),
            "rejection_count": len(rejections),
            "gross_t_pnl": round(gross_t_pnl, 4),
            "net_t_pnl": round(net_t_pnl, 4),
            "win_rate": round(len(winning_trades) / total_pairs, 6) if total_pairs else 0.0,
            "profit_loss_ratio": round(gross_wins / gross_losses, 6) if gross_losses else None,
            "max_drawdown": round(max_drawdown, 8),
            "max_daily_t_loss": round(min(0.0, max_daily_t_loss), 4),
        }
        direction_metrics = {}
        for direction in TDirection:
            direction_trades = [
                item for item in completed_trades if item["direction"] == direction.value
            ]
            direction_metrics[direction.value] = {
                "completed_pairs": len(direction_trades),
                "gross_pnl": round(sum(float(item["gross_pnl"]) for item in direction_trades), 4),
                "net_pnl": round(sum(float(item["net_pnl"]) for item in direction_trades), 4),
                "win_rate": round(
                    sum(float(item["net_pnl"]) > 0 for item in direction_trades)
                    / len(direction_trades),
                    6,
                )
                if direction_trades
                else 0.0,
            }
        symbol_summaries = []
        for symbol in symbols:
            restores = [
                item for item in trades if item["symbol"] == symbol and item["leg"] == "restore"
            ]
            symbol_summaries.append(
                {
                    "symbol": symbol,
                    "name": SUPPORTED_SYMBOLS[symbol],
                    "opening_quantity": base_quantities[symbol],
                    "ending_quantity": holdings[symbol],
                    "completed_pairs": len(restores),
                    "net_pnl": round(sum(float(item["net_pnl"]) for item in restores), 4),
                }
            )
        symbol_days = {
            symbol: sorted({value.date() for value in frame.loc[frame["symbol"] == symbol].index})
            for symbol in symbols
        }
        observed_union = set().union(*(set(days) for days in symbol_days.values()))
        common_days = set.intersection(*(set(days) for days in symbol_days.values()))
        return {
            "symbols": symbols,
            "period": {
                "start": frame.index.min().isoformat(),
                "end": frame.index.max().isoformat(),
                "trade_days": len({value.date() for value in frame.index}),
                "common_trade_days": len(common_days),
                "symbol_trade_days": {symbol: len(days) for symbol, days in symbol_days.items()},
                "missing_observed_days": {
                    symbol: [day.isoformat() for day in sorted(observed_union - set(days))]
                    for symbol, days in symbol_days.items()
                },
                "bars": len(frame),
            },
            "parameters": {
                "strategy": self._serialize_dataclass(config.params),
                "cost": self._serialize_dataclass(config.cost),
                "decision_cost": self._serialize_dataclass(config.decision_cost or config.cost),
                "max_bar_volume_fraction": config.max_bar_volume_fraction,
                "base_quantities": base_quantities,
            },
            "metrics": metrics,
            "equity_curve": equity_curve,
            "daily_results": equity_curve,
            "direction_metrics": direction_metrics,
            "symbol_summaries": symbol_summaries,
            "trades": trades,
            "rejections": rejections,
            "data_quality": {
                "limit_prices": self._limit_price_quality(frame, config),
            },
        }

    def _prepare_frame(self, minute_data: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
        frame = minute_data.copy()
        if not isinstance(frame.index, pd.DatetimeIndex):
            if "datetime" not in frame.columns:
                raise ValueError("minute data requires a DatetimeIndex or datetime column")
            frame.index = pd.to_datetime(frame.pop("datetime"))
        if "symbol" not in frame.columns:
            raise ValueError("minute data requires symbol column")
        required_market = {"open", "high", "low", "close", "volume"}
        missing = required_market - set(frame.columns)
        if missing:
            raise ValueError(f"minute data missing columns: {sorted(missing)}")
        frame = frame.sort_index(kind="stable")
        if self._FEATURE_COLUMNS.issubset(frame.columns):
            return frame
        featured: list[pd.DataFrame] = []
        for _, group in frame.groupby([frame["symbol"], frame.index.date], sort=False):
            values = compute_intraday_features(group, params)
            values["vwap"] = values["session_vwap"]
            featured.append(values)
        return pd.concat(featured).sort_index(kind="stable")

    @staticmethod
    def _base_quantities(
        symbols: list[str],
        prices: dict[str, float],
        config: BacktestConfig,
    ) -> dict[str, int]:
        result: dict[str, int] = {}
        allocation = config.initial_capital * (1 - config.cash_buffer_fraction) / len(symbols)
        for symbol in symbols:
            if symbol in config.base_quantities:
                quantity = int(config.base_quantities[symbol])
            else:
                quantity = normalize_buy_quantity(symbol, allocation / prices[symbol])
            if quantity <= 0:
                raise ValueError(f"base quantity is zero for {symbol}")
            result[symbol] = quantity
        return result

    @classmethod
    def _snapshot(
        cls,
        symbol: str,
        row: pd.Series,
        timestamp: datetime,
        config: BacktestConfig,
    ) -> MarketSnapshot:
        return MarketSnapshot(
            symbol=symbol,
            price=float(row["close"]),
            vwap=cls._float_or_nan(row["vwap"]),
            zscore=cls._float_or_nan(row["zscore"]),
            previous_zscore=cls._float_or_nan(row["previous_zscore"]),
            fast_ema=cls._float_or_nan(row["fast_ema"]),
            slow_ema=cls._float_or_nan(row["slow_ema"]),
            vwap_slope=cls._float_or_nan(row["vwap_slope"]),
            volume_ratio=cls._float_or_nan(row["volume_ratio"]),
            estimated_edge_bps=cls._float_or_nan(row["estimated_edge_bps"]),
            previous_price=cls._float_or_nan(row["previous_price"]),
            realized_vol_bps=cls._float_or_nan(row["realized_vol_bps"]),
            session_return_bps=cls._float_or_nan(row["session_return_bps"]),
            limit_price_available=(
                not config.require_exact_limit_prices
                or f"{symbol}|{timestamp.date().isoformat()}" in config.limit_prices
            ),
            at_price_limit=cls._at_price_limit(symbol, row, timestamp, config),
        )

    @staticmethod
    def _float_or_nan(value: Any) -> float:
        return float("nan") if pd.isna(value) else float(value)

    @staticmethod
    def _at_price_limit(
        symbol: str,
        row: pd.Series,
        timestamp: datetime,
        config: BacktestConfig,
    ) -> bool:
        limits = config.limit_prices.get(f"{symbol}|{timestamp.date().isoformat()}", {})
        price = float(row["close"])
        up = limits.get("up")
        down = limits.get("down")
        half_tick = 0.005
        return bool(
            (up is not None and price >= float(up) - half_tick)
            or (down is not None and price <= float(down) + half_tick)
        )

    def _fill_rejection(
        self,
        intent: OrderIntent,
        row: pd.Series,
        timestamp: datetime,
        state: SymbolDayState,
        config: BacktestConfig,
    ) -> dict[str, Any] | None:
        if timestamp != intent.signal_at + timedelta(minutes=1):
            return self._rejection(intent, timestamp, "stale_signal")
        bar_capacity = int(self._bar_volume_shares(row) * config.max_bar_volume_fraction)
        if intent.side == "BUY":
            executable = normalize_buy_quantity(intent.symbol, bar_capacity)
        else:
            executable = normalize_sell_quantity(
                intent.symbol,
                bar_capacity,
                available=state.sellable_remaining,
            )
        if executable < intent.quantity:
            return self._rejection(intent, timestamp, "volume_cap")
        limits = config.limit_prices.get(f"{intent.symbol}|{timestamp.date().isoformat()}", {})
        open_price = float(row["open"])
        if intent.side == "BUY" and limits.get("up") is not None and open_price >= limits["up"]:
            return self._rejection(intent, timestamp, "limit_up")
        if (
            intent.side == "SELL"
            and limits.get("down") is not None
            and open_price <= limits["down"]
        ):
            return self._rejection(intent, timestamp, "limit_down")
        return None

    @staticmethod
    def _bar_volume_shares(row: pd.Series) -> float:
        volume = max(0.0, float(row["volume"]))
        amount = float(row.get("amount", 0.0) or 0.0)
        price = float(row.get("close", 0.0) or 0.0)
        if volume > 0 and amount > 0 and price > 0:
            implied_unit = amount / volume / price
            if 50 <= implied_unit <= 150:
                return volume * 100
        return volume

    @staticmethod
    def _fill_price(
        open_price: float,
        side: str,
        cost: CostModel,
        limits: dict[str, float] | None = None,
    ) -> float:
        direction = 1 if side == "BUY" else -1
        price = open_price * (1 + direction * cost.slippage_bps / 10_000)
        limits = limits or {}
        if side == "BUY" and limits.get("up") is not None:
            price = min(price, float(limits["up"]))
        if side == "SELL" and limits.get("down") is not None:
            price = max(price, float(limits["down"]))
        return price

    @staticmethod
    def _limit_price_quality(
        frame: pd.DataFrame,
        config: BacktestConfig,
    ) -> dict[str, Any]:
        expected = sorted(
            {
                f"{str(row['symbol'])}|{timestamp.date().isoformat()}"
                for timestamp, row in frame[["symbol"]].iterrows()
            }
        )
        missing = [key for key in expected if key not in config.limit_prices]
        return {
            "mode": "fail_closed" if config.require_exact_limit_prices else "best_effort",
            "expected_symbol_days": len(expected),
            "available_symbol_days": len(expected) - len(missing),
            "missing_symbol_days": missing,
        }

    @staticmethod
    def _transaction_fees(*, side: str, price: float, quantity: int, cost: CostModel) -> float:
        notional = price * quantity
        commission = max(cost.min_commission, notional * cost.commission_rate)
        transfer = notional * cost.transfer_fee_rate
        stamp = notional * cost.stamp_duty_rate if side == "SELL" else 0.0
        return commission + transfer + stamp

    @staticmethod
    def _rejection(intent: OrderIntent, timestamp: datetime, reason: str) -> dict[str, Any]:
        return {
            "symbol": intent.symbol,
            "side": intent.side,
            "quantity": intent.quantity,
            "signal_at": intent.signal_at.isoformat(),
            "attempted_at": timestamp.isoformat(),
            "reason": reason,
        }

    @staticmethod
    def _serialize_dataclass(value: Any) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in value.__dict__.items():
            if hasattr(item, "isoformat"):
                result[key] = item.isoformat()
            else:
                result[key] = item
        return result

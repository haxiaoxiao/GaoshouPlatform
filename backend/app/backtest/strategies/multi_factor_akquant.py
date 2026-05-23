"""Generic multi-factor AKQuant strategy template."""

MULTI_FACTOR_STRATEGY_CODE = r'''
from datetime import date, datetime
from typing import Any

import akquant as aq

from app.services.factor_pipeline import FactorPipeline, LinearFactorScorer


FACTOR_CONFIGS = [
    {
        "name": "market_cap",
        "weight": 0.45,
        "direction": "lower_better",
        "transform": "zscore",
        "neutralize_market_cap": False,
        "industry_zscore": True,
    },
    {
        "name": "market_cap_rank",
        "weight": 0.35,
        "direction": "lower_better",
        "transform": "rank_pct",
    },
    {
        "name": "v4gv",
        "weight": 0.20,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
]

FILTER_FACTORS = [
    {"name": "is_st", "operator": ">=", "value": 0.5},
    {"name": "is_paused", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_up", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_down", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
]

# Optional static universe. Leave empty to use the symbols passed by the
# platform/AKQuant, or the symbols discovered from bars.
DEFAULT_UNIVERSE_SYMBOLS = []

TOP_N = 10
MIN_CANDIDATES = 5
MIN_FACTOR_COVERAGE = 0.60
REBALANCE_EVERY_N_DAYS = 1
REBALANCE_TIME = "10:30"
CASH_BUFFER_PCT = 0.05
LOT_SIZE = 100
MAX_POSITION_PCT = 0.12
REBALANCE_TOLERANCE_PCT = 0.01
SCORER_TYPE = "linear"

# Risk and fees are intentionally controlled by the platform backtest config:
# risk_config / max_positions / volume_limit_pct, commission_rate,
# stamp_tax_rate, transfer_fee_rate, min_commission, slippage.


class ModelScorer:
    """Protocol-like placeholder for future ML scorers."""

    def score(self, frame, factor_specs):
        raise NotImplementedError


class CustomModelScorer(ModelScorer):
    """Reserved extension point for sklearn/PyTorch/LightGBM inference."""

    def score(self, frame, factor_specs):
        raise NotImplementedError("Set SCORER_TYPE='linear' or implement model inference here.")


class MultiFactorStrategy(aq.Strategy):
    """Generic long-only multi-factor stock selection strategy."""

    symbols = DEFAULT_UNIVERSE_SYMBOLS

    def __init__(self, **kwargs: Any):
        super().__init__()
        self.factor_configs = list(FACTOR_CONFIGS)
        self.filter_factors = list(FILTER_FACTORS)
        self.top_n = int(TOP_N)
        self.min_candidates = int(MIN_CANDIDATES)
        self.min_factor_coverage = float(MIN_FACTOR_COVERAGE)
        self.rebalance_every_n_days = max(1, int(REBALANCE_EVERY_N_DAYS))
        self.rebalance_time = str(REBALANCE_TIME)
        self.cash_buffer_pct = float(CASH_BUFFER_PCT)
        self.lot_size = int(LOT_SIZE)
        self.max_position_pct = float(MAX_POSITION_PCT)
        self.rebalance_tolerance_pct = float(REBALANCE_TOLERANCE_PCT)
        self.scorer_type = str(SCORER_TYPE)
        self._pipeline = None
        self._seen_symbols = set(DEFAULT_UNIVERSE_SYMBOLS)
        self._latest_price = {}
        self._prices_by_day = {}
        self._last_rebalance_date = None
        self._rebalance_count = 0
        self._daily_rebalanced = set()

    def on_start(self):
        self.set_history_depth(0)
        self._pipeline = FactorPipeline()
        try:
            self.add_daily_timer(self.rebalance_time, "multi_factor_rebalance")
        except Exception:
            pass

    def on_bar(self, bar):
        symbol = str(getattr(bar, "symbol", "") or "")
        if not symbol:
            return
        price = self._bar_price(bar)
        if price <= 0:
            return
        self._seen_symbols.add(symbol)
        self._latest_price[symbol] = price
        trade_date = self._bar_date(bar)
        if trade_date is None:
            return
        day_prices = self._prices_by_day.setdefault(trade_date, {})
        day_prices[symbol] = price

        # Daily bars do not have intraday timers in some feeds. Once we have a
        # usable cross-section for the date, run one rebalance for that date.
        universe = self._universe()
        if trade_date not in self._daily_rebalanced and len(day_prices) >= max(1, int(len(universe) * 0.8)):
            self._rebalance(trade_date, price_map=day_prices)
            self._daily_rebalanced.add(trade_date)

    def on_timer(self, payload):
        if str(payload) != "multi_factor_rebalance":
            return
        trade_date = self._current_trade_date()
        if trade_date is None:
            return
        self._rebalance(trade_date, price_map=dict(self._latest_price))

    def _rebalance(self, trade_date, price_map):
        if not self._should_rebalance(trade_date):
            return
        universe = [symbol for symbol in self._universe() if symbol in price_map and price_map[symbol] > 0]
        if len(universe) < max(1, self.min_candidates):
            self.log(f"MF skip {trade_date}: candidate universe too small ({len(universe)})")
            return

        pipeline = self._pipeline or FactorPipeline()
        result = pipeline.build_cross_section(
            factor_specs=self.factor_configs,
            trade_date=trade_date,
            symbols=universe,
            filters=self.filter_factors,
            min_factor_coverage=self.min_factor_coverage,
            scorer=self._scorer(),
        )
        frame = result.frame
        if frame.empty or len(frame) < max(1, self.min_candidates):
            self.log(f"MF skip {trade_date}: no usable factor scores")
            return

        target_symbols = list(frame.head(self.top_n).index.astype(str))
        if not target_symbols:
            return
        target_weight = min(
            self.max_position_pct,
            (1.0 - self.cash_buffer_pct) / float(len(target_symbols)),
        )
        self._submit_targets(target_symbols, target_weight, price_map)
        self._last_rebalance_date = trade_date
        self._rebalance_count += 1
        self.log(
            f"MF rebalance {trade_date}: selected={target_symbols[:5]} "
            f"excluded={len(result.excluded_symbols)}"
        )

    def _submit_targets(self, target_symbols, target_weight, price_map):
        target_set = set(target_symbols)
        portfolio_value = self._portfolio_value(price_map)
        target_positions = {}
        for symbol in target_symbols:
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price <= 0:
                continue
            target_value = portfolio_value * target_weight
            target_positions[symbol] = self._round_lot(target_value / price)

        current_positions = self._positions()
        for symbol, qty in current_positions.items():
            if qty > 0 and symbol not in target_set:
                self.sell(symbol=symbol, quantity=self._round_lot(qty), price=price_map.get(symbol))

        for symbol, target_qty in target_positions.items():
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price <= 0:
                continue
            current_qty = float(current_positions.get(symbol, 0.0) or 0.0)
            delta = target_qty - current_qty
            if abs(delta) * price < portfolio_value * self.rebalance_tolerance_pct:
                continue
            if delta > 0:
                buy_qty = self._round_lot(delta)
                if buy_qty > 0 and self._can_buy(buy_qty, price):
                    self.buy(symbol=symbol, quantity=buy_qty, price=price)
            elif delta < 0:
                sell_qty = self._round_lot(-delta)
                if sell_qty > 0:
                    self.sell(symbol=symbol, quantity=sell_qty, price=price)

    def _should_rebalance(self, trade_date):
        if self._last_rebalance_date == trade_date:
            return False
        if self._last_rebalance_date is None:
            return True
        try:
            return (trade_date - self._last_rebalance_date).days >= self.rebalance_every_n_days
        except Exception:
            return True

    def _scorer(self):
        if self.scorer_type.lower() == "linear":
            return LinearFactorScorer()
        return CustomModelScorer()

    def _universe(self):
        configured = list(getattr(self, "symbols", None) or [])
        if configured:
            return [str(symbol) for symbol in configured]
        return sorted(self._seen_symbols)

    def _positions(self):
        try:
            return {str(k): float(v or 0.0) for k, v in self.get_positions().items()}
        except Exception:
            return {symbol: float(self.get_position(symbol) or 0.0) for symbol in self._universe()}

    def _portfolio_value(self, price_map):
        try:
            return float(self.get_portfolio_value())
        except Exception:
            value = self._cash()
            for symbol, qty in self._positions().items():
                value += float(qty or 0.0) * float(price_map.get(symbol, 0.0) or 0.0)
            return value

    def _cash(self):
        try:
            return float(self.get_cash())
        except Exception:
            return 0.0

    def _can_buy(self, qty, price):
        return self._cash() * (1.0 - self.cash_buffer_pct) >= float(qty) * float(price)

    def _round_lot(self, qty):
        lot = max(1, int(self.lot_size))
        return int(float(qty or 0.0) // lot * lot)

    def _bar_price(self, bar):
        for field in ("close", "open", "price"):
            value = float(getattr(bar, field, 0.0) or 0.0)
            if value > 0:
                return value
        return 0.0

    def _bar_date(self, bar):
        ts = getattr(bar, "timestamp", None)
        return self._date_from_timestamp(ts)

    def _current_trade_date(self):
        current_bar = getattr(self, "current_bar", None)
        if current_bar is not None:
            return self._bar_date(current_bar)
        ctx = getattr(self, "ctx", None)
        current_time = getattr(ctx, "current_time", None) if ctx is not None else None
        return self._date_from_timestamp(current_time)

    def _date_from_timestamp(self, value):
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        try:
            ts = int(value)
            if ts > 10_000_000_000:
                ts = ts // 1_000_000_000
            return datetime.utcfromtimestamp(ts).date()
        except Exception:
            try:
                return datetime.fromisoformat(str(value)).date()
            except Exception:
                return None
'''


DEFAULT_MULTI_FACTOR_PARAMS = {
    "top_n": 10,
    "min_factor_coverage": 0.60,
    "rebalance_time": "10:30",
    "cash_buffer_pct": 0.05,
    "lot_size": 100,
    "max_position_pct": 0.12,
    "scorer_type": "linear",
}


DEFAULT_MULTI_FACTOR_RISK_CONFIG = {
    "max_position_pct": 0.12,
}

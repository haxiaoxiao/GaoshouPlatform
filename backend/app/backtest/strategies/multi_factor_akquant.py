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
THEME_INCLUDE_INDUSTRIES = []
THEME_INCLUDE_KEYWORDS = []
THEME_EXCLUDE_INDUSTRIES = []
THEME_EXCLUDE_KEYWORDS = []
THEME_MIN_CANDIDATES = 0
STRICT_THEME_FILTER = False
STOP_LOSS_PCT = 0.0
TRAILING_STOP_PCT = 0.0
TAKE_PROFIT_PCT = 0.0
PORTFOLIO_DRAWDOWN_STOP_PCT = 0.0
HIGH_VOLUME_RISK_FACTOR = ""
HIGH_VOLUME_RISK_AS_OF_TIME = "14:30"
HIGH_VOLUME_RISK_MAX = 0.0
HIGH_VOLUME_RISK_PARAMS = {}
RISK_CHECK_TIMES = []
BLOCK_REBUY_AFTER_STOP = True

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
        self.theme_include_industries = list(THEME_INCLUDE_INDUSTRIES)
        self.theme_include_keywords = list(THEME_INCLUDE_KEYWORDS)
        self.theme_exclude_industries = list(THEME_EXCLUDE_INDUSTRIES)
        self.theme_exclude_keywords = list(THEME_EXCLUDE_KEYWORDS)
        self.theme_min_candidates = int(THEME_MIN_CANDIDATES)
        self.strict_theme_filter = bool(STRICT_THEME_FILTER)
        self.stop_loss_pct = float(STOP_LOSS_PCT)
        self.trailing_stop_pct = float(TRAILING_STOP_PCT)
        self.take_profit_pct = float(TAKE_PROFIT_PCT)
        self.portfolio_drawdown_stop_pct = float(PORTFOLIO_DRAWDOWN_STOP_PCT)
        self.high_volume_risk_factor = str(HIGH_VOLUME_RISK_FACTOR)
        self.high_volume_risk_as_of_time = str(HIGH_VOLUME_RISK_AS_OF_TIME)
        self.high_volume_risk_max = float(HIGH_VOLUME_RISK_MAX)
        self.high_volume_risk_params = dict(HIGH_VOLUME_RISK_PARAMS)
        self.risk_check_times = list(RISK_CHECK_TIMES)
        self.block_rebuy_after_stop = bool(BLOCK_REBUY_AFTER_STOP)
        self._pipeline = None
        self._seen_symbols = set(DEFAULT_UNIVERSE_SYMBOLS)
        self._latest_price = {}
        self._prices_by_day = {}
        self._day_open_price = {}
        self._cost_basis = {}
        self._peak_price = {}
        self._portfolio_peak_value = 0.0
        self._risk_exit_dates = set()
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
        for risk_time in self._as_list(self.risk_check_times):
            try:
                self.add_daily_timer(str(risk_time), "multi_factor_risk_check")
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
        open_price = float(getattr(bar, "open", 0.0) or 0.0)
        if open_price > 0:
            self._day_open_price[symbol] = open_price
        trade_date = self._bar_date(bar)
        if trade_date is None:
            return
        day_prices = self._prices_by_day.setdefault(trade_date, {})
        day_prices[symbol] = price
        self._risk_check_symbol(symbol, price, trade_date, bar_time=self._bar_time(bar))

        # Daily bars do not have intraday timers in some feeds. Once we have a
        # usable cross-section for the date, run one rebalance for that date.
        universe = self._universe()
        if trade_date not in self._daily_rebalanced and len(day_prices) >= max(1, int(len(universe) * 0.8)):
            self._rebalance(trade_date, price_map=day_prices)
            self._daily_rebalanced.add(trade_date)

    def on_timer(self, payload):
        if str(payload) == "multi_factor_risk_check":
            trade_date = self._current_trade_date()
            if trade_date is not None:
                self._risk_check_all(trade_date, price_map=dict(self._latest_price))
            return
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
        frame = self._apply_theme_filter(frame)
        if frame.empty or len(frame) < max(1, self.min_candidates):
            self.log(f"MF skip {trade_date}: theme-filtered candidates too small ({len(frame)})")
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
                    self._note_submitted_buy(symbol, buy_qty, price, current_qty)
                    self.buy(symbol=symbol, quantity=buy_qty, price=price)
            elif delta < 0:
                sell_qty = self._round_lot(-delta)
                if sell_qty > 0:
                    self.sell(symbol=symbol, quantity=sell_qty, price=price)

    def _should_rebalance(self, trade_date):
        if trade_date in self._risk_exit_dates:
            return False
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

    def on_trade(self, trade):
        symbol = str(getattr(trade, "symbol", "") or "")
        if not symbol:
            return
        side = str(getattr(trade, "side", "") or "").lower()
        quantity = abs(float(getattr(trade, "quantity", 0.0) or 0.0))
        price = float(getattr(trade, "price", 0.0) or 0.0)
        if quantity <= 0 or price <= 0:
            return
        current_qty = abs(float(self.get_position(symbol) or 0.0))
        if "buy" in side:
            previous_qty = max(0.0, current_qty - quantity)
            previous_cost = float(self._cost_basis.get(symbol, price) or price)
            if current_qty > 0:
                self._cost_basis[symbol] = (
                    previous_cost * previous_qty + price * quantity
                ) / max(current_qty, quantity)
                self._peak_price[symbol] = max(price, float(self._peak_price.get(symbol, price) or price))
            return
        if "sell" in side or "close" in side:
            if current_qty <= 0:
                self._cost_basis.pop(symbol, None)
                self._peak_price.pop(symbol, None)

    def _apply_theme_filter(self, frame):
        include_terms = self._as_list(self.theme_include_industries) + self._as_list(self.theme_include_keywords)
        exclude_terms = self._as_list(self.theme_exclude_industries) + self._as_list(self.theme_exclude_keywords)
        if frame.empty or (not include_terms and not exclude_terms):
            return frame

        filtered = frame
        if exclude_terms:
            keep_mask = [
                not self._row_matches_terms(row, exclude_terms)
                for _, row in filtered.iterrows()
            ]
            filtered = filtered.loc[keep_mask]
        if include_terms:
            themed_mask = [
                self._row_matches_terms(row, include_terms)
                for _, row in filtered.iterrows()
            ]
            themed = filtered.loc[themed_mask]
            min_candidates = max(0, int(getattr(self, "theme_min_candidates", 0) or 0))
            if self.strict_theme_filter or len(themed) >= min_candidates:
                filtered = themed
            else:
                self.log(
                    f"MF theme fallback: themed={len(themed)} below min={min_candidates}; "
                    f"using exclude-only candidates={len(filtered)}"
                )
        return filtered

    def _row_matches_terms(self, row, terms):
        text = " ".join(
            self._normalize_text(row.get(column, ""))
            for column in ("industry", "industry2", "industry3", "sector", "concept")
        )
        if not text:
            return False
        return any(self._normalize_text(term) in text for term in terms if self._normalize_text(term))

    def _risk_check_all(self, trade_date, price_map):
        if not price_map:
            return
        if self._portfolio_drawdown_hit(trade_date, price_map):
            return
        for symbol, qty in self._positions().items():
            if qty <= 0:
                continue
            price = float(price_map.get(symbol, 0.0) or 0.0)
            if price > 0:
                self._risk_check_symbol(symbol, price, trade_date)

    def _risk_check_symbol(self, symbol, price, trade_date, bar_time=None):
        qty = float(self._positions().get(symbol, 0.0) or 0.0)
        if qty <= 0 or price <= 0:
            return False
        if self._portfolio_drawdown_hit(trade_date, dict(self._latest_price)):
            return True

        cost = float(self._cost_basis.get(symbol, 0.0) or 0.0)
        if cost <= 0:
            cost = price
            self._cost_basis[symbol] = cost
        peak = max(price, float(self._peak_price.get(symbol, price) or price))
        self._peak_price[symbol] = peak

        stop_loss = float(getattr(self, "stop_loss_pct", 0.0) or 0.0)
        if stop_loss > 0 and price <= cost * (1.0 - stop_loss):
            return self._risk_exit(symbol, qty, price, trade_date, f"stop_loss {price:.4f}/{cost:.4f}")

        trailing = float(getattr(self, "trailing_stop_pct", 0.0) or 0.0)
        if trailing > 0 and peak > cost and price <= peak * (1.0 - trailing):
            return self._risk_exit(symbol, qty, price, trade_date, f"trailing_stop {price:.4f}/{peak:.4f}")

        take_profit = float(getattr(self, "take_profit_pct", 0.0) or 0.0)
        if take_profit > 0 and price >= cost * (1.0 + take_profit):
            return self._risk_exit(symbol, qty, price, trade_date, f"take_profit {price:.4f}/{cost:.4f}")

        if self._high_volume_risk_hit(symbol, trade_date, bar_time):
            return self._risk_exit(symbol, qty, price, trade_date, "high_volume_risk")
        return False

    def _portfolio_drawdown_hit(self, trade_date, price_map):
        stop = float(getattr(self, "portfolio_drawdown_stop_pct", 0.0) or 0.0)
        if stop <= 0:
            return False
        value = self._portfolio_value(price_map)
        if value <= 0:
            return False
        self._portfolio_peak_value = max(float(self._portfolio_peak_value or 0.0), value)
        if self._portfolio_peak_value <= 0 or value > self._portfolio_peak_value * (1.0 - stop):
            return False
        positions = self._positions()
        for symbol, qty in positions.items():
            if qty <= 0:
                continue
            price = float(price_map.get(symbol, self._latest_price.get(symbol, 0.0)) or 0.0)
            self._risk_exit(symbol, qty, price, trade_date, "portfolio_drawdown_stop", mark_date=False)
        if self.block_rebuy_after_stop:
            self._risk_exit_dates.add(trade_date)
        self.log(f"MF portfolio drawdown stop {trade_date}: value={value:.2f}")
        return True

    def _high_volume_risk_hit(self, symbol, trade_date, bar_time):
        factor_name = str(getattr(self, "high_volume_risk_factor", "") or "")
        threshold = float(getattr(self, "high_volume_risk_max", 0.0) or 0.0)
        if not factor_name or threshold <= 0:
            return False
        risk_time = str(getattr(self, "high_volume_risk_as_of_time", "") or "")
        if risk_time and bar_time and not self._time_at_or_after(bar_time, risk_time):
            return False
        try:
            store = (self._pipeline or FactorPipeline()).store
            values = store.load_cross_section(
                factor_name,
                trade_date,
                symbols=[symbol],
                as_of_time=risk_time or None,
                params=dict(getattr(self, "high_volume_risk_params", {}) or {}),
            )
            value = float(values.get(symbol, 0.0) or 0.0)
        except Exception:
            return False
        return value >= threshold

    def _risk_exit(self, symbol, qty, price, trade_date, reason, mark_date=True):
        sell_qty = self._round_lot(qty)
        if sell_qty <= 0:
            return False
        self.sell(symbol=symbol, quantity=sell_qty, price=price if price > 0 else None)
        if mark_date and self.block_rebuy_after_stop:
            self._risk_exit_dates.add(trade_date)
        self._cost_basis.pop(symbol, None)
        self._peak_price.pop(symbol, None)
        self.log(f"MF risk exit {trade_date}: {symbol} qty={sell_qty} reason={reason}")
        return True

    def _note_submitted_buy(self, symbol, qty, price, current_qty):
        previous_qty = max(0.0, float(current_qty or 0.0))
        previous_cost = float(self._cost_basis.get(symbol, price) or price)
        new_qty = previous_qty + float(qty or 0.0)
        if new_qty <= 0 or price <= 0:
            return
        self._cost_basis[symbol] = (previous_cost * previous_qty + float(price) * float(qty)) / new_qty
        self._peak_price[symbol] = max(float(price), float(self._peak_price.get(symbol, price) or price))

    def _as_list(self, value):
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
        try:
            return [str(item).strip() for item in value if str(item).strip()]
        except Exception:
            return [str(value).strip()] if str(value).strip() else []

    def _normalize_text(self, value):
        return str(value or "").strip().lower()

    def _bar_time(self, bar):
        timestamp_text = str(getattr(bar, "timestamp_str", "") or "")
        if len(timestamp_text) >= 16 and ":" in timestamp_text:
            return timestamp_text[-8:-3]
        ts = getattr(bar, "timestamp", None)
        try:
            raw = int(ts)
            if raw > 10_000_000_000:
                raw = raw // 1_000_000_000
            return datetime.utcfromtimestamp(raw).strftime("%H:%M")
        except Exception:
            return None

    def _time_at_or_after(self, left, right):
        try:
            left_parts = [int(part) for part in str(left)[:5].split(":")]
            right_parts = [int(part) for part in str(right)[:5].split(":")]
            return tuple(left_parts[:2]) >= tuple(right_parts[:2])
        except Exception:
            return True

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

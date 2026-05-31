import os
import sqlite3
from datetime import date, datetime, time, timedelta
from pathlib import Path

import akquant as aq
import akquant.talib as aq_talib
import numpy as np

from app.services.security_symbols import normalize_security_symbol, to_jq_symbol


class IntradayCumulativeVolumeIndicator:
    """External intraday cumulative-volume indicator used by ID=43.

    The strategy consumes this as a point-in-time feature instead of owning the
    minute-volume query path. Values are raw minute cumulative volume; only the
    daily volume side is unit-normalized separately.
    """

    def __init__(self, strategy):
        self.strategy = strategy
        self._cache = {}

    def get(self, symbol, as_of=None):
        trade_date = self.strategy._current_trade_date()
        current_dt = as_of or self.strategy._current_datetime(symbol)
        if trade_date is None or current_dt is None:
            return self.strategy._current_day_volume.get(
                (self.strategy._last_trade_date, symbol),
                0.0,
            )
        cache_key = (str(symbol), current_dt)
        if cache_key in self._cache:
            return self._cache[cache_key]

        value = self._load_from_factor_cache(symbol, trade_date, current_dt)
        if value is None:
            value = self._load_from_market_store(symbol, current_dt)
        if value is None:
            value = self._load_from_clickhouse(symbol, trade_date, current_dt)
        if value is None or value <= 0:
            value = self.strategy._current_day_volume.get((trade_date, symbol), 0.0)
        self._cache[cache_key] = float(value or 0.0)
        return self._cache[cache_key]

    def get_batch(self, symbols, as_of):
        symbols = [str(symbol) for symbol in symbols if symbol]
        if not symbols or as_of is None:
            return {}
        trade_date = self.strategy._current_trade_date()
        if trade_date is None:
            return {}
        missing = [symbol for symbol in symbols if (symbol, as_of) not in self._cache]
        if missing:
            factor_values = self._load_factor_cache_batch(missing, trade_date, as_of)
            unresolved = []
            for symbol in missing:
                value = factor_values.get(symbol)
                if value is None:
                    unresolved.append(symbol)
                else:
                    self._cache[(symbol, as_of)] = float(value or 0.0)
            if unresolved:
                store_values = self._load_market_store_batch(unresolved, as_of)
                for symbol in unresolved:
                    value = store_values.get(symbol)
                    if value is None:
                        value = self.strategy._current_day_volume.get((trade_date, symbol), 0.0)
                    self._cache[(symbol, as_of)] = float(value or 0.0)
        return {symbol: float(self._cache.get((symbol, as_of), 0.0) or 0.0) for symbol in symbols}

    def clear_day(self, trade_date):
        stale = [key for key in self._cache if getattr(key[1], "date", lambda: None)() != trade_date]
        for key in stale:
            self._cache.pop(key, None)

    def _feature_params(self, as_of):
        return {"time": as_of.strftime("%H:%M")}

    def _load_from_factor_cache(self, symbol, trade_date, as_of):
        values = self._load_factor_cache_batch([symbol], trade_date, as_of)
        return values.get(symbol)

    def _load_factor_cache_batch(self, symbols, trade_date, as_of):
        if not getattr(self.strategy, "enable_factor_value_cache", True):
            return {}
        try:
            from app.services.factor_value_store import get_factor_value_store

            data_symbols = [self.strategy._store_data_symbol(symbol) for symbol in symbols]
            values = get_factor_value_store().load_cross_section(
                "cum_volume_at_time",
                trade_date,
                symbols=data_symbols,
                as_of_time=as_of.strftime("%H:%M"),
                params=self._feature_params(as_of),
            )
            reverse = {
                self.strategy._store_data_symbol(strategy_symbol): strategy_symbol
                for strategy_symbol in symbols
            }
            return {
                reverse.get(str(data_symbol), str(data_symbol)): float(value or 0.0)
                for data_symbol, value in values.items()
            }
        except Exception:
            return {}

    def _load_from_market_store(self, symbol, as_of):
        values = self._load_market_store_batch([symbol], as_of)
        return values.get(symbol)

    def _load_market_store_batch(self, symbols, as_of):
        try:
            data_symbols = [self.strategy._store_data_symbol(symbol) for symbol in symbols]
            values = self.strategy._market_store().load_minute_cum_volume(data_symbols, as_of)
            reverse = {
                self.strategy._store_data_symbol(strategy_symbol): strategy_symbol
                for strategy_symbol in symbols
            }
            return {
                reverse.get(str(data_symbol), str(data_symbol)): float(value or 0.0)
                for data_symbol, value in values.items()
            }
        except Exception:
            return {}

    def _load_from_clickhouse(self, symbol, trade_date, as_of):
        if not self.strategy._allow_clickhouse_fallback():
            return None
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                """
                SELECT sum(volume)
                FROM klines_minute
                WHERE symbol = %(symbol)s
                  AND datetime >= %(start)s
                  AND datetime <= %(end)s
                """,
                {
                    "symbol": self.strategy._normalize_data_symbol(symbol),
                    "start": datetime.combine(trade_date, datetime.min.time()),
                    "end": as_of,
                },
            )
            return float(rows[0][0] or 0) if rows else 0.0
        except Exception:
            return None


class V4TechnicalSignalIndicator:
    """External adapter for ID=43 V4GV/V4GV21/MACD point-in-time signals."""

    def __init__(self, strategy):
        self.strategy = strategy
        self._cache = {}

    def get(self, symbol):
        as_of = self.strategy._history_as_of_date() or self.strategy._current_trade_date()
        n, m = self.strategy.get_dynamic_periods()
        cache_key = (str(symbol), as_of, n, m)
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = self._load_cached(symbol, as_of, n, m)
        if result is None:
            v4gv, v4gv21 = self.strategy.calculate_indicator(symbol, n=n, m=m)
            macd_signal = self.strategy.get_macd_signal(symbol)
            if v4gv is not None and v4gv21 is not None:
                result = (float(v4gv), float(v4gv21), bool(macd_signal))
                self._store_cached(symbol, as_of, n, m, result)
        self._cache[cache_key] = result
        return result

    def get_batch(self, symbols):
        return {symbol: self.get(symbol) for symbol in symbols if symbol}

    def clear_day(self, trade_date):
        stale = [key for key in self._cache if key[1] != trade_date]
        for key in stale:
            self._cache.pop(key, None)

    def _load_cached(self, symbol, as_of, n, m):
        if as_of is None:
            return None
        redis_cache = self.strategy._backtest_cache() if getattr(self.strategy, "enable_indicator_redis", False) else None
        if redis_cache is None:
            return None
        try:
            payload = redis_cache.get_json(redis_cache.key("indicator_point", symbol, as_of, n, m))
            if not isinstance(payload, dict):
                return None
            if payload.get("v4gv") is None or payload.get("v4gv21") is None:
                return None
            return (float(payload["v4gv"]), float(payload["v4gv21"]), bool(payload.get("macd", False)))
        except Exception:
            return None

    def _store_cached(self, symbol, as_of, n, m, result):
        if as_of is None or not getattr(self.strategy, "enable_indicator_redis", False):
            return
        redis_cache = self.strategy._backtest_cache()
        if redis_cache is None:
            return
        v4gv, v4gv21, macd_signal = result
        redis_cache.set_json(
            redis_cache.key("indicator_point", symbol, as_of, n, m),
            {"v4gv": round(float(v4gv), 6), "v4gv21": round(float(v4gv21), 6), "macd": bool(macd_signal)},
        )


DEFAULT_SMALL_CAP_PARAMS = {
    "index_symbol": "399101.SZ",
    "pass_april": True,
    "run_stoploss": True,
    "stock_num": 5,
    "enable_indicator": True,
    "enable_indicator_exit": True,
    "v4_indicator_mode": "robust",
    "enable_indicator_redis": False,
    "HV_control": True,
    "daily_volume_to_share_multiplier": 100.0,
    "high_volume_check_time": "14:30",
    "timer_price_adjustment_mode": "previous_close_ratio",
    "limit_price_source": "table",
    "execution_plan_mode": "timer",
    "max_industry_weight": 0.3,
    "enable_industry_filter": True,
    "industry_mode": "local",
    "filter_st": True,
    "cash_buffer_rate": 0.01,
}


class SmallCapV4Strategy(aq.Strategy):
    """AKQuant port of the JoinQuant "small-cap V4" strategy.

    The strategy keeps the original control flow using AKQuant daily timers:
    09:15 prepare_stock_list, weekly Tuesday 10:30 rebalance, 10:00 stoploss,
    14:30 afternoon limit-up/high-volume/indicator checks, 14:50 special-month
    closeout, and Friday 15:10 position logging. It is intended to run with
    bar_type="minute_timer" so timer callbacks use sparse minute bars.
    """

    index_symbol = "399101.SZ"
    jq_index_symbol = "399101.XSHE"
    pass_april = DEFAULT_SMALL_CAP_PARAMS["pass_april"]
    run_stoploss = DEFAULT_SMALL_CAP_PARAMS["run_stoploss"]
    stock_num = DEFAULT_SMALL_CAP_PARAMS["stock_num"]
    enable_indicator = DEFAULT_SMALL_CAP_PARAMS["enable_indicator"]
    enable_indicator_exit = DEFAULT_SMALL_CAP_PARAMS["enable_indicator_exit"]
    v4_indicator_mode = DEFAULT_SMALL_CAP_PARAMS["v4_indicator_mode"]
    enable_indicator_redis = DEFAULT_SMALL_CAP_PARAMS["enable_indicator_redis"]
    HV_control = DEFAULT_SMALL_CAP_PARAMS["HV_control"]
    daily_volume_to_share_multiplier = DEFAULT_SMALL_CAP_PARAMS["daily_volume_to_share_multiplier"]
    high_volume_check_time = DEFAULT_SMALL_CAP_PARAMS["high_volume_check_time"]
    timer_price_adjustment_mode = DEFAULT_SMALL_CAP_PARAMS["timer_price_adjustment_mode"]
    limit_price_source = DEFAULT_SMALL_CAP_PARAMS["limit_price_source"]
    execution_plan_mode = DEFAULT_SMALL_CAP_PARAMS["execution_plan_mode"]
    max_industry_weight = DEFAULT_SMALL_CAP_PARAMS["max_industry_weight"]
    enable_industry_filter = DEFAULT_SMALL_CAP_PARAMS["enable_industry_filter"]
    industry_mode = DEFAULT_SMALL_CAP_PARAMS["industry_mode"]
    filter_st = DEFAULT_SMALL_CAP_PARAMS["filter_st"]
    cash_buffer_rate = DEFAULT_SMALL_CAP_PARAMS["cash_buffer_rate"]

    def on_start(self):
        if getattr(self, "_v4_initialized", False):
            return
        self._v4_initialized = True
        self.set_history_depth(int(getattr(self, "ak_history_depth", 0)))
        self._register_daily_timers()

        self.no_trading_today_signal = False
        self.pass_april = getattr(self, "pass_april", True)
        self.run_stoploss = getattr(self, "run_stoploss", True)
        self.hold_list = []
        self.yesterday_HL_list = []
        self.target_list = []
        self.not_buy_again = []
        self.stock_num = int(getattr(self, "stock_num", 5))
        self.up_price = float(getattr(self, "up_price", 100.0))
        self.reason_to_sell = ""
        self.stoploss_strategy = int(getattr(self, "stoploss_strategy", 3))
        self.stoploss_limit = float(getattr(self, "stoploss_limit", 0.88))
        self.stoploss_market = float(getattr(self, "stoploss_market", 0.94))

        self.enable_indicator = getattr(self, "enable_indicator", True)
        self.enable_indicator_exit = getattr(self, "enable_indicator_exit", True)
        self.v4_indicator_mode = str(getattr(self, "v4_indicator_mode", "robust"))
        self.enable_indicator_redis = getattr(self, "enable_indicator_redis", False)
        self.HV_control = getattr(self, "HV_control", True)
        self.HV_duration = int(getattr(self, "HV_duration", 120))
        self.HV_ratio = float(getattr(self, "HV_ratio", 0.9))
        self.high_volume_mode = str(getattr(self, "high_volume_mode", "daily_include_now"))
        self.enable_factor_value_cache = getattr(self, "enable_factor_value_cache", True)
        self.high_volume_factor_name = str(getattr(self, "high_volume_factor_name", "high_volume_signal"))
        self.daily_volume_to_share_multiplier = float(
            getattr(self, "daily_volume_to_share_multiplier", 100.0)
        )
        self.smart_timer_on_bar = getattr(self, "smart_timer_on_bar", False)
        self.smart_timer_full_universe_times = self._normalize_timer_time_set(
            getattr(self, "smart_timer_full_universe_times", ["10:30"])
        )
        self.dynamic_params = getattr(self, "dynamic_params", True)
        self.volatility_lookback = int(getattr(self, "volatility_lookback", 30))
        self.debug_logging = getattr(self, "debug_logging", True)
        self.enable_timer_snapshots = getattr(self, "enable_timer_snapshots", False)
        self.timer_price_adjustment_mode = str(
            getattr(self, "timer_price_adjustment_mode", "previous_close_ratio")
        )
        self.high_volume_check_time = str(getattr(self, "high_volume_check_time", "14:30"))
        self.limit_price_source = str(getattr(self, "limit_price_source", "table"))
        self.execution_plan_mode = str(getattr(self, "execution_plan_mode", "timer"))

        self.max_industry_weight = float(getattr(self, "max_industry_weight", 0.3))
        self.enable_industry_filter = getattr(self, "enable_industry_filter", True)
        self.industry_mode = str(getattr(self, "industry_mode", "local"))
        self.filter_st = getattr(self, "filter_st", True)

        self.commission_rate = float(getattr(self, "commission_rate", 0.00025))
        self.stamp_tax_rate = float(getattr(self, "stamp_tax_rate", 0.001))
        self.min_commission = float(getattr(self, "min_commission", 5.0))
        self.lot_size = int(getattr(self, "lot_size", 100))
        # AKQuant checks cash before sell-side fees are debited. Keep a small
        # cash reserve so liquidation orders are not rejected by fee checks.
        self.cash_buffer_rate = float(getattr(self, "cash_buffer_rate", 0.01))

        self.index_symbol = getattr(self, "index_symbol", "399101.SZ")
        self.jq_index_symbol = self._to_jq_index_symbol(self.index_symbol)
        self._stock_meta = self._load_stock_meta()
        self._stock_name_changes = self._load_stock_name_changes()
        self._stock_name_change_intervals = {}
        self._current_bars = {}
        self._current_day_volume = {}
        self._daily_cache = {}
        self._trade_date_cache = None
        self._week_trade_day_cache = {}
        self._previous_trade_date_cache = {}
        self._previous_trade_date = None
        self._last_trade_date = None
        self._active_timer_datetime = None
        self._position_cost = {}
        self._position_qty = {}
        self._last_buy_price = {}
        self._pending_sells = set()
        self._pending_sell_cash = 0.0
        self._intraday_volume_indicator = IntradayCumulativeVolumeIndicator(self)
        self._v4_signal_indicator = V4TechnicalSignalIndicator(self)
        self._next_pre_open_plan = None

    def __getstate__(self):
        state = super().__getstate__()
        state.pop("_current_bars", None)
        state.pop("_current_day_volume", None)
        state.pop("_market_store_instance", None)
        state.pop("_intraday_volume_indicator", None)
        return state

    def __setstate__(self, state):
        super().__setstate__(state)
        self._current_bars = {}
        self._current_day_volume = {}
        self._daily_cache = {}
        self._trade_date_cache = None
        self._week_trade_day_cache = {}
        self._previous_trade_date_cache = {}
        self._previous_trade_date = None
        self._active_timer_datetime = None
        if not hasattr(self, "_stock_meta"):
            self._stock_meta = self._load_stock_meta()
        if not hasattr(self, "_stock_name_changes"):
            self._stock_name_changes = self._load_stock_name_changes()
        if not hasattr(self, "_stock_name_change_intervals"):
            self._stock_name_change_intervals = {}
        if not hasattr(self, "_pending_sells"):
            self._pending_sells = set()
        self._pending_sell_cash = 0.0
        self._intraday_volume_indicator = IntradayCumulativeVolumeIndicator(self)
        self._v4_signal_indicator = V4TechnicalSignalIndicator(self)
        self._next_pre_open_plan = None

    def _register_daily_timers(self):
        for timer_time, payload in (
            ("09:15:00", "prepare"),
            ("10:00:00", "sell_stocks"),
            ("10:30:00", "weekly_adjustment"),
            ("14:30:00", "trade_afternoon"),
            ("14:50:00", "close_account"),
            ("15:10:00", "print_position_info"),
        ):
            self.add_daily_timer(timer_time, payload)

    def on_bar(self, bar):
        symbol = str(bar.symbol)
        dt_value = self._bar_datetime(bar)
        if dt_value is None:
            return

        trade_date = dt_value.date()
        if self._last_trade_date != trade_date:
            self._previous_trade_date = self._last_trade_date or self._load_previous_trade_date(trade_date)
            self._last_trade_date = trade_date
            self._intraday_volume_indicator.clear_day(trade_date)
            self._v4_signal_indicator.clear_day(self._history_as_of_date() or trade_date)

        if self._skip_timer_bar(symbol, dt_value):
            return

        self._current_bars[symbol] = bar
        self._current_day_volume[(dt_value.date(), symbol)] = (
            self._current_day_volume.get((dt_value.date(), symbol), 0.0) + float(bar.volume)
        )
        self._sync_position_state(symbol, float(bar.close))

    def _skip_timer_bar(self, symbol, dt_value):
        if not self.smart_timer_on_bar:
            return False
        if self._is_daily_bar(dt_value):
            return False
        timer_key = dt_value.strftime("%H:%M")
        if timer_key in self.smart_timer_full_universe_times:
            return False
        if symbol in {self.index_symbol, self.jq_index_symbol}:
            return False
        if symbol in self.get_positions():
            return False
        if symbol in self.target_list or symbol in self.yesterday_HL_list:
            return False
        return True

    def on_timer(self, payload):
        trade_date = self._current_trade_date()
        if trade_date is None:
            return
        payload_text = str(payload)
        self._active_timer_datetime = self._timer_datetime_for_payload(trade_date, payload_text)
        try:
            self._route_timer(trade_date, payload_text)
        finally:
            self._active_timer_datetime = None

    def _route_timer(self, trade_date, payload):
        if payload == "prepare":
            self.prepare_stock_list(trade_date)
        elif payload == "sell_stocks":
            self.sell_stocks(trade_date)
        elif payload == "weekly_adjustment" and self._is_weekly_trade_day(trade_date, 2):
            self.weekly_adjustment(trade_date)
        elif payload == "trade_afternoon":
            self.trade_afternoon(trade_date)
        elif payload == "close_account":
            self.close_account(trade_date)
        elif payload == "print_position_info" and trade_date.weekday() == 4:
            self.print_position_info(trade_date)

    def on_pre_open(self, event):
        """Optional class-style AKQuant pre-open execution hook.

        Disabled by default while we keep JQ-aligned timer execution. When
        execution_plan_mode="pre_open", weekly_adjustment only stages a plan and
        this hook submits orders on the next session open.
        """

        if self.execution_plan_mode != "pre_open":
            return
        plan = self._next_pre_open_plan
        if not plan:
            return
        trading_date = event.get("trading_date") if isinstance(event, dict) else None
        if trading_date is not None and plan.get("execute_date") not in {None, trading_date}:
            return
        self._active_timer_datetime = datetime.combine(trading_date, time(9, 30)) if trading_date else None
        try:
            self._execute_rebalance_plan(plan)
        finally:
            self._active_timer_datetime = None
            self._next_pre_open_plan = None

    @staticmethod
    def _timer_datetime_for_payload(trade_date, payload):
        timer_map = {
            "prepare": time(9, 15),
            "sell_stocks": time(10, 0),
            "weekly_adjustment": time(10, 30),
            "trade_afternoon": time(14, 30),
            "close_account": time(14, 50),
            "print_position_info": time(15, 10),
        }
        timer_value = timer_map.get(str(payload))
        if trade_date is None or timer_value is None:
            return None
        return datetime.combine(trade_date, timer_value)

    def _is_weekly_trade_day(self, trade_date, ordinal):
        if trade_date is None:
            return False
        week_start = trade_date - timedelta(days=trade_date.weekday())
        cache_key = (week_start, int(ordinal))
        cached = self._week_trade_day_cache.get(cache_key)
        if cached is not None:
            return cached == trade_date
        try:
            store = self._market_store()
            days = store.load_trading_dates(
                [self._store_data_symbol(self.index_symbol)],
                week_start,
                week_start + timedelta(days=6),
            )
            if len(days) >= ordinal:
                target = days[ordinal - 1]
                self._week_trade_day_cache[cache_key] = target
                return target == trade_date
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            fallback = week_start + timedelta(days=ordinal - 1)
            self._week_trade_day_cache[cache_key] = fallback
            return fallback == trade_date
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                """
                SELECT DISTINCT trade_date
                FROM klines_daily
                WHERE symbol = %(symbol)s
                  AND trade_date >= %(start)s
                  AND trade_date <= %(end)s
                ORDER BY trade_date
                """,
                {
                    "symbol": self.jq_index_symbol,
                    "start": week_start,
                    "end": week_start + timedelta(days=6),
                },
            )
            days = [row[0] for row in rows]
            if len(days) >= ordinal:
                target = days[ordinal - 1]
                self._week_trade_day_cache[cache_key] = target
                return target == trade_date
        except Exception:
            pass
        fallback = week_start + timedelta(days=ordinal - 1)
        self._week_trade_day_cache[cache_key] = fallback
        return fallback == trade_date

    def prepare_stock_list(self, trade_date):
        self.hold_list = list(self.get_positions().keys())
        self._pending_sells = set()
        self._pending_sell_cash = 0.0
        self.yesterday_HL_list = [
            symbol for symbol in self.hold_list if self._was_limit_up(symbol)
        ]
        self.no_trading_today_signal = self.today_is_between(trade_date)
        self._event_log(
            "DEBUG_PREPARE",
            hold_list=self.hold_list,
            yesterday_HL_list=self.yesterday_HL_list,
            no_trading_today_signal=self.no_trading_today_signal,
        )

    def weekly_adjustment(self, trade_date):
        if self.no_trading_today_signal:
            self._event_log("今日为特殊月份，不交易")
            return

        self.not_buy_again = []
        self.target_list = self.get_stock_list(trade_date)
        self._event_log("DEBUG_TARGET_LIST", target_list=self.target_list)
        if not self.target_list:
            self._event_log("今日无符合条件股票")
            return

        target_list = self.filter_by_industry(self.target_list[: self.stock_num], self.hold_list)
        self._event_log("DEBUG_FINAL_TARGET", target_list=target_list)
        if self.execution_plan_mode == "pre_open":
            self._next_pre_open_plan = self._build_rebalance_plan(trade_date, target_list)
            self._event_log("DEBUG_STAGE_PRE_OPEN_PLAN", target_list=target_list)
            return

        for stock in list(self.hold_list):
            if stock not in target_list and stock not in self.yesterday_HL_list:
                self._event_log(f"调仓卖出[{stock}]")
                self.close_position_(stock, "rebalance")
            else:
                self._event_log(f"继续持有[{stock}]")

        self.buy_security(target_list)
        self.not_buy_again = [s for s in self.get_positions().keys() if s not in self._pending_sells]

    def _build_rebalance_plan(self, trade_date, target_list):
        return {
            "created_date": trade_date,
            "execute_date": None,
            "hold_list": list(self.hold_list),
            "target_list": list(target_list),
            "yesterday_HL_list": list(self.yesterday_HL_list),
        }

    def _execute_rebalance_plan(self, plan):
        target_list = list(plan.get("target_list") or [])
        hold_list = list(plan.get("hold_list") or self.hold_list)
        yesterday_HL_list = set(plan.get("yesterday_HL_list") or self.yesterday_HL_list)
        for stock in hold_list:
            if stock not in target_list and stock not in yesterday_HL_list:
                self._event_log(f"rebalance sell [{stock}]")
                self.close_position_(stock, "rebalance")
            else:
                self._event_log(f"continue hold [{stock}]")
        self.buy_security(target_list)
        self.not_buy_again = [s for s in self.get_positions().keys() if s not in self._pending_sells]

    def sell_stocks(self, trade_date):
        if not self.run_stoploss:
            return

        if self.stoploss_strategy == 1:
            for stock in list(self.get_positions().keys()):
                cost = self._avg_cost(stock)
                price = self._last_price(stock)
                if cost <= 0 or price <= 0:
                    continue
                if price >= cost * 2:
                    self._event_log(f"收益100%止盈,卖出{stock}")
                    self.close_position_(stock, "take_profit")
                elif price < cost * self.get_dynamic_stoploss(stock):
                    self._event_log(f"动态止损,卖出{stock}")
                    if self.close_position_(stock, "stoploss"):
                        self.reason_to_sell = "stoploss"

        elif self.stoploss_strategy == 2:
            self._market_stoploss_only()

        elif self.stoploss_strategy == 3:
            if self._market_stoploss_only():
                return
            for stock in list(self.get_positions().keys()):
                cost = self._avg_cost(stock)
                price = self._last_price(stock)
                dynamic_stop = self.get_dynamic_stoploss(stock)
                self._event_log(
                    "DEBUG_STOPLOSS",
                    symbol=stock,
                    cost=round(cost, 6),
                    price=round(price, 6),
                    stop=round(dynamic_stop, 6),
                    threshold=round(cost * dynamic_stop, 6),
                )
                if cost > 0 and price > 0 and price < cost * dynamic_stop:
                    self._event_log(f"动态止损,卖出{stock}")
                    if self.close_position_(stock, "stoploss"):
                        self.reason_to_sell = "stoploss"

    def trade_afternoon(self, trade_date):
        if self.no_trading_today_signal:
            return

        self.check_limit_up()
        if self.HV_control:
            self.check_high_volume()
        self.check_trading_signals()
        self.check_remain_amount()

    def close_account(self, trade_date):
        if self.no_trading_today_signal and self.get_positions():
            for stock in list(self.get_positions().keys()):
                self.close_position_(stock, "special_month")

    def check_limit_up(self):
        if not self.yesterday_HL_list:
            return

        for stock in list(self.yesterday_HL_list):
            if stock not in self.get_positions():
                self._safe_remove(self.yesterday_HL_list, stock)
                continue
            if not self._is_limit_up_now(stock):
                self._event_log(f"[{stock}]涨停打开，卖出")
                if self.close_position_(stock, "limitup_open"):
                    self._safe_remove(self.yesterday_HL_list, stock)
                    self.reason_to_sell = "limitup"
            else:
                self._event_log(f"[{stock}]涨停，继续持有")

    def check_remain_amount(self):
        if self.reason_to_sell == "limitup":
            self.hold_list = list(self.get_positions().keys())
            cash = float(self.get_cash())
            if len(self.hold_list) < self.stock_num and cash > 100:
                target_list = [s for s in self.target_list if s not in self.not_buy_again]
                target_list = target_list[: min(self.stock_num - len(self.hold_list), len(target_list))]
                self._event_log(f"有余额可用{cash:.2f}元，买入{target_list}")
                self.buy_security(target_list)
            self.reason_to_sell = ""
        elif self.reason_to_sell == "stoploss":
            self._event_log("止损后余额，下周再交易")
            self.reason_to_sell = ""

    def check_high_volume(self):
        volume_as_of = self._current_timer_datetime(self.high_volume_check_time)
        positions = list(self.get_positions().keys())
        volume_map = self._intraday_volume_indicator.get_batch(positions, volume_as_of) if volume_as_of is not None else {}
        feature_signals = self._high_volume_feature_signal_map(
            positions,
            volume_as_of,
        )
        for stock in positions:
            if stock in self._pending_sells:
                continue
            if self._is_paused(stock):
                continue
            if self._is_limit_up_now(stock):
                continue
            if self.get_available_position(stock) <= 0:
                continue
            feature_signal = feature_signals.get(stock) if feature_signals is not None else None
            if feature_signal is not None:
                if feature_signal:
                    self._event_log(f"[{stock}]放量(FactorValueStore)，卖出")
                    self.close_position_(stock, "high_volume")
                continue
            if self.high_volume_mode == "minute_to_now" and volume_as_of is not None:
                history_count = max(self.HV_duration - 1, 1)
                volumes = self._daily_history(stock, "volume", history_count, include_current=False)
                current_volume = volume_map.get(stock, 0.0)
                if current_volume <= 0:
                    current_volume = self._current_day_volume.get((self._current_trade_date(), stock), 0.0)
                volumes = np.append(volumes, current_volume)
            else:
                volumes = self._daily_history(stock, "volume", self.HV_duration, include_current=True)
            if len(volumes) < self.HV_duration:
                continue
            max_vol = float(np.nanmax(volumes))
            if max_vol > 0 and volumes[-1] > self.HV_ratio * max_vol:
                self._event_log(f"[{stock}]放量({volumes[-1]:.0f} > {self.HV_ratio * max_vol:.0f})，卖出")
                self.close_position_(stock, "high_volume")

    def check_trading_signals(self):
        if not self.enable_indicator or not self.enable_indicator_exit:
            return
        if self.v4_indicator_mode in {"jq_fallback", "fallback"}:
            return
        positions = list(self.get_positions().keys())
        indicator_map = self._v4_signal_indicator.get_batch(positions)
        for stock in positions:
            if stock in self._pending_sells:
                continue
            if self._is_paused(stock):
                continue
            indicator = indicator_map.get(stock)
            if indicator is None:
                continue
            v4gv, v4gv21, macd_signal = indicator
            if v4gv is None or v4gv21 is None:
                continue
            if v4gv < v4gv21 and v4gv21 > 0 and not macd_signal:
                # Match JQ comparison semantics: a sell log means the order
                # passed T+1/available-position checks and was submitted.
                if self.close_position_(stock, "v4gv_dead_cross"):
                    self._event_log(
                        f"检测到死叉信号(V4GV:{v4gv:.2f} < V4GV21:{v4gv21:.2f})，卖出[{stock}]"
                    )

    def get_stock_list(self, trade_date):
        initial_list = self.get_index_stocks(self.index_symbol)
        self._event_log("DEBUG_FILTER", step="index", count=len(initial_list), sample=initial_list[:10])
        initial_list = self.filter_new_stock(initial_list, trade_date)
        self._event_log("DEBUG_FILTER", step="new_stock", count=len(initial_list), sample=initial_list[:10])
        initial_list = self.filter_kcbj_stock(initial_list)
        self._event_log("DEBUG_FILTER", step="board", count=len(initial_list), sample=initial_list[:10])
        initial_list = self.filter_st_stock(initial_list, trade_date)
        self._event_log("DEBUG_FILTER", step="st", count=len(initial_list), sample=initial_list[:10])
        initial_list = self.filter_paused_stock(initial_list)
        self._event_log("DEBUG_FILTER", step="paused", count=len(initial_list), sample=initial_list[:10])
        initial_list = self.filter_limitup_stock(initial_list)
        self._event_log("DEBUG_FILTER", step="limitup", count=len(initial_list), sample=initial_list[:10])
        initial_list = self.filter_limitdown_stock(initial_list)
        self._event_log("DEBUG_FILTER", step="limitdown", count=len(initial_list), sample=initial_list[:10])

        ranked = self._sort_by_market_cap(initial_list, trade_date)
        stock_list = ranked[:100]
        self._event_log("DEBUG_MARKET_CAP_TOP100", stock_list=stock_list[:20])
        stock_list = self.filter_by_industry(stock_list, self.hold_list)
        self._event_log("DEBUG_INDUSTRY_FILTER", count=len(stock_list), stock_list=stock_list[:20])

        if self.enable_indicator:
            self._preload_daily_history(stock_list, 140)
            filtered = []
            if self.v4_indicator_mode not in {"jq_fallback", "fallback"}:
                indicator_map = self._v4_signal_indicator.get_batch(stock_list)
                for stock in stock_list:
                    indicator = indicator_map.get(stock)
                    if indicator is None:
                        continue
                    v4gv, v4gv21, macd_signal = indicator
                    if v4gv is None or v4gv21 is None:
                        continue
                    if v4gv > v4gv21 and v4gv > 0 and macd_signal:
                        filtered.append(stock)
            self._event_log("DEBUG_INDICATOR_FILTER", mode=self.v4_indicator_mode, count=len(filtered), stock_list=filtered[:20])
            if len(filtered) < self.stock_num:
                for stock in stock_list:
                    if stock not in filtered:
                        filtered.append(stock)
                    if len(filtered) >= self.stock_num:
                        break
            final_list = filtered[: 2 * self.stock_num]
            self._event_log(f"今日选股({len(final_list)}只):{final_list[:min(10, len(final_list))]}...")
            return final_list

        final_list = stock_list[: 2 * self.stock_num]
        self._event_log(f"今日选股({len(final_list)}只):{final_list[:min(10, len(final_list))]}...")
        return final_list

    def get_market_volatility(self, days=30):
        end_date = self._history_as_of_date()
        start_date = end_date - timedelta(days=days + 10) if end_date is not None else None
        frame = self._daily_frame_between(self.index_symbol, start_date, end_date)
        close = frame.get("close", np.asarray([], dtype=float))
        if len(close) < days:
            return 0.2
        close = self._fill_nan(close)
        close = close[close > 0]
        if len(close) < days:
            return 0.2
        returns = np.diff(np.log(close))
        return float(np.nanstd(returns[-days:])) if len(returns) else 0.2

    def get_dynamic_periods(self):
        if not self.dynamic_params:
            return 55, 34
        volatility = self.get_market_volatility(self.volatility_lookback)
        if volatility > 0.025:
            return 40, 25
        if volatility > 0.015:
            return 48, 30
        return 55, 34

    def calculate_indicator(self, security, n=None, m=None):
        if n is None or m is None:
            n, m = self.get_dynamic_periods()

        # JQ source loads a calendar-day window ending at context.previous_date.
        # Do not extend it to a fixed trading-day count, otherwise early-2020
        # holiday gaps produce valid indicators locally while JQ returns NaN.
        start_date = None
        end_date = self._history_as_of_date()
        if end_date is not None:
            start_date = end_date - timedelta(days=max(n, m) * 2)
        frame = self._daily_frame_between(security, start_date, end_date)
        close = frame.get("close", np.asarray([], dtype=float))
        high = frame.get("high", np.asarray([], dtype=float))
        low = frame.get("low", np.asarray([], dtype=float))
        if len(close) < max(n, m, 55, 34) + 5:
            return None, None
        if np.isnan(close).all() or np.isnan(high).all() or np.isnan(low).all():
            return None, None

        close = self._fill_nan(close)
        high = self._fill_nan(high)
        low = self._fill_nan(low)

        llv34 = self._talib_min(low, 34)
        hhv34 = self._talib_max(high, 34)
        denominator34 = np.where(hhv34 - llv34 == 0, 1e-6, hhv34 - llv34)
        rsv = 100 * (close - llv34) / denominator34
        var1 = self._talib_sma(rsv, 5) - 20

        llv55 = self._talib_min(low, 55)
        hhv55 = self._talib_max(high, 55)
        denominator55 = np.where(hhv55 - llv55 == 0, 1e-6, hhv55 - llv55)
        rsv2 = 100 * (close - llv55) / denominator55
        sma1 = self._talib_ema(rsv2, 5)
        a1 = 3 * sma1 - 2 * sma1
        a12 = (a1 + var1) / 2

        rsv3 = 100 * (close - llv34) / denominator34
        main_fund = self._talib_ema(rsv3, 3)
        rsv4 = -100 * (hhv34 - close) / denominator34
        d0 = self._talib_ema(rsv4, 4) + 100
        v4gv = ((main_fund + d0) / 2 + a12) / 2
        v4gv21 = (v4gv + self._talib_sma(v4gv, 2)) / 2

        if not np.isfinite(v4gv[-1]) or not np.isfinite(v4gv21[-1]):
            return None, None
        return float(v4gv[-1]), float(v4gv21[-1])

    def get_macd_signal(self, security):
        cached = self._cached_v4gv_macd(security)
        if cached is not None:
            return bool(cached[2])
        end_date = self._history_as_of_date()
        start_date = end_date - timedelta(days=50) if end_date is not None else None
        frame = self._daily_frame_between(security, start_date, end_date)
        close = frame.get("close", np.asarray([], dtype=float))
        if len(close) < 35:
            return False
        close = self._fill_nan(close)
        macd, signal, _hist = aq_talib.MACD(
            close, fastperiod=12, slowperiod=26, signalperiod=9, backend="python"
        )
        return bool(np.isfinite(macd[-1]) and np.isfinite(signal[-1]) and macd[-1] > signal[-1] and macd[-1] > 0)

    def _cached_v4gv_macd(self, security):
        if not getattr(self, "enable_indicator_redis", False):
            return None
        as_of = self._history_as_of_date() or self._current_trade_date()
        if as_of is None:
            return None
        n, m = self.get_dynamic_periods()
        cache_key = ("__v4gv_macd__", security, as_of, n, m)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        redis_cache = self._backtest_cache()
        if redis_cache is None:
            return None
        try:
            payload = redis_cache.get_json(redis_cache.key("indicator_point", security, as_of, n, m))
            if not isinstance(payload, dict):
                return None
            if payload.get("v4gv") is None or payload.get("v4gv21") is None:
                self._daily_cache[cache_key] = None
                return None
            result = (float(payload["v4gv"]), float(payload["v4gv21"]), bool(payload.get("macd", False)))
            self._daily_cache[cache_key] = result
            return result
        except Exception:
            self._daily_cache[cache_key] = None
            return None

    def _set_cached_v4gv_macd(self, security, v4gv, v4gv21, macd_signal):
        if not getattr(self, "enable_indicator_redis", False):
            return
        if v4gv is None or v4gv21 is None:
            return
        as_of = self._history_as_of_date() or self._current_trade_date()
        if as_of is None:
            return
        n, m = self.get_dynamic_periods()
        cache_key = ("__v4gv_macd__", security, as_of, n, m)
        result = (float(v4gv), float(v4gv21), bool(macd_signal))
        self._daily_cache[cache_key] = result
        redis_cache = self._backtest_cache()
        if redis_cache is None:
            return
        redis_cache.set_json(
            redis_cache.key("indicator_point", security, as_of, n, m),
            {"v4gv": round(float(v4gv), 6), "v4gv21": round(float(v4gv21), 6), "macd": bool(macd_signal)},
        )

    def get_stock_industry(self, stock_list):
        if self.industry_mode in {"unknown", "jq_unknown"}:
            return {stock: "unknown" for stock in stock_list}
        result = {}
        for stock in stock_list:
            result[stock] = self._stock_meta.get(stock, {}).get("industry") or "unknown"
        return result

    def filter_by_industry(self, stock_list, current_holdings):
        if not self.enable_industry_filter or not stock_list:
            return stock_list
        hold_industries = self.get_stock_industry(current_holdings)
        industry_count = {}
        for industry in hold_industries.values():
            industry_count[industry] = industry_count.get(industry, 0) + 1

        candidate_industries = self.get_stock_industry(stock_list)
        filtered = []
        denominator = len(current_holdings)
        for stock in stock_list:
            industry = candidate_industries.get(stock, "unknown")
            current_weight = industry_count.get(industry, 0) / denominator if denominator else 0
            if current_weight < self.max_industry_weight:
                filtered.append(stock)
        return filtered

    def get_dynamic_stoploss(self, stock):
        if not self.dynamic_params:
            return self.stoploss_limit
        high = self._daily_history(stock, "high", 20)
        low = self._daily_history(stock, "low", 20)
        close = self._daily_history(stock, "close", 20)
        if len(close) < 14:
            return self.stoploss_limit
        high = self._fill_nan(high)
        low = self._fill_nan(low)
        close = self._fill_nan(close)
        atr = self._atr(high, low, close, 14)
        price = close[-1]
        if price <= 0 or not np.isfinite(atr):
            return self.stoploss_limit
        atr_percentage = atr / price
        return 1 - min(0.12, max(0.08, atr_percentage * 2))

    def filter_paused_stock(self, stock_list):
        return [s for s in stock_list if not self._is_paused(s)]

    def filter_st_stock(self, stock_list, trade_date=None):
        as_of = trade_date or self._history_as_of_date() or self._current_trade_date()
        return [s for s in stock_list if not self._is_st_or_delisted(s, as_of)]

    def filter_kcbj_stock(self, stock_list):
        return [
            s for s in stock_list
            if not (s.startswith("688") or s.startswith(("4", "8", "9")))
        ]

    def filter_limitup_stock(self, stock_list):
        return [
            s for s in stock_list
            if s in self.get_positions() or not self._is_limit_up_now(s)
        ]

    def filter_limitdown_stock(self, stock_list):
        return [
            s for s in stock_list
            if s in self.get_positions() or not self._is_limit_down_now(s)
        ]

    def filter_new_stock(self, stock_list, trade_date):
        filtered = []
        for s in stock_list:
            list_date = self._list_date(s)
            if list_date is None:
                continue
            if trade_date - list_date > timedelta(days=375):
                filtered.append(s)
        return filtered

    def filter_highprice_stock(self, stock_list):
        return [
            s for s in stock_list
            if s in self.get_positions() or self._last_price(s) <= self.up_price
        ]

    def order_target_value_(self, security, value):
        price = self._last_price(security)
        if value == 0:
            return self.close_position_(security, "target_zero")
        before_qty = float(self.get_position(security))
        self.order_target_value(
            float(value),
            symbol=security,
            price=price if price and price > 0 else None,
            order_type="market",
            time_in_force=aq.TimeInForce.Day,
        )
        after_qty = float(self.get_position(security))
        if after_qty > before_qty and price > 0:
            self._record_buy_cost(security, after_qty - before_qty, price)
            return True
        return bool(price and price > 0 and value > 0)

    def open_position(self, security, value):
        return self.order_target_value_(security, value)

    def close_position_(self, security, reason=""):
        if security in self._pending_sells:
            return False
        if security not in self.get_positions():
            return False
        if self.get_available_position(security) <= 0:
            return False
        if self._has_open_sell_order(security):
            self._pending_sells.add(security)
            return False
        price = self._last_price(security)
        quantity = float(self.get_position(security))
        order_id = self.submit_order(
            symbol=security,
            side="Sell",
            quantity=quantity,
            price=price if price and price > 0 else None,
            order_type="market",
            time_in_force=aq.TimeInForce.Day,
            tag=str(reason or ""),
            position_effect="close",
            reduce_only=True,
        )
        self._event_log("DEBUG_ORDER", side="sell", symbol=security, quantity=quantity, price=price, order_type="market", reason=reason)
        self._pending_sells.add(security)
        if order_id and price > 0 and quantity > 0:
            gross = price * quantity
            sell_fee = max(gross * self.commission_rate, self.min_commission) + gross * self.stamp_tax_rate
            self._pending_sell_cash += max(0.0, gross - sell_fee)
        return bool(order_id)

    def _has_open_sell_order(self, security):
        try:
            orders = self.get_open_orders(security)
        except Exception:
            return False
        for order in orders or []:
            side = str(getattr(order, "side", "")).lower()
            if "sell" in side:
                return True
        return False

    def _orderable_limit_price(self, symbol, reference_price, side):
        if self._using_intraday_bars():
            return reference_price if reference_price and reference_price > 0 else None
        if side == "buy":
            limit_price = self._current_limit_price(symbol, "up")
            return limit_price if limit_price > 0 else reference_price if reference_price and reference_price > 0 else None
        if side == "sell":
            limit_price = self._current_limit_price(symbol, "down")
            return limit_price if limit_price > 0 else reference_price if reference_price and reference_price > 0 else None
        return reference_price if reference_price and reference_price > 0 else None

    def buy_security(self, target_list):
        if not target_list:
            return
        current_positions = self.get_positions()
        active_positions = {
            symbol: quantity
            for symbol, quantity in current_positions.items()
            if symbol not in self._pending_sells
        }
        position_count = len(active_positions)
        target_num = min(self.stock_num, len(target_list))
        if target_num <= position_count:
            return
        cash = (float(self.get_cash()) + float(getattr(self, "_pending_sell_cash", 0.0))) * max(
            0.0,
            1.0 - self.cash_buffer_rate,
        )
        value = cash / (target_num - position_count)
        pending_buys = 0
        for stock in target_list:
            current_positions = self.get_positions()
            active_positions = {
                symbol: quantity
                for symbol, quantity in current_positions.items()
                if symbol not in self._pending_sells
            }
            if stock not in active_positions:
                before_qty = float(self.get_position(stock))
                if self.open_position(stock, value):
                    after_qty = float(self.get_position(stock))
                    self._event_log(f"买入[{stock}] {value:.2f}元")
                    self._event_log(
                        "DEBUG_ORDER",
                        side="buy",
                        symbol=stock,
                        quantity=max(0.0, after_qty - before_qty),
                        value=value,
                        price=self._last_price(stock),
                    )
                    self.not_buy_again.append(stock)
                    pending_buys += 1
                    if position_count + pending_buys >= target_num:
                        break

    def today_is_between(self, trade_date):
        if not self.pass_april:
            return False
        md = (trade_date.month, trade_date.day)
        return (md[0] == 4 and 1 <= md[1] <= 30) or (md[0] == 1 and 1 <= md[1] <= 30)

    def print_position_info(self, trade_date):
        total_value = self._portfolio_total_value()
        self._event_log("=" * 50)
        self._event_log(f"日期: {trade_date}")
        self._event_log(f"总资产: {total_value:.2f}")
        self._event_log(f"持仓数量: {len(self.get_positions())}")
        if self.enable_industry_filter:
            industries = self.get_stock_industry(list(self.get_positions().keys()))
            dist = {}
            for industry in industries.values():
                dist[industry] = dist.get(industry, 0) + 1
            self._event_log(f"行业分布: {dist}")
        for stock in self.get_positions().keys():
            cost = self._avg_cost(stock)
            price = self._last_price(stock)
            ret = (price / cost - 1) * 100 if cost > 0 else 0
            self._event_log(f"{stock} | 成本:{cost:.2f} | 现价:{price:.2f} | 收益:{ret:.2f}%")
        if not self.get_positions():
            self._event_log("当前无持仓")
        self._event_log("=" * 50)

    def get_index_stocks(self, index_symbol):
        symbols = self._load_index_components(index_symbol)
        if symbols:
            return symbols
        symbols = [
            symbol for symbol, meta in self._stock_meta.items()
            if not meta.get("is_delist") and meta.get("security_type", "stock") in {"stock", "股票", ""}
        ]
        tradable = set(self.get_instruments().keys())
        return [s for s in symbols if s in tradable] if tradable else symbols

    def _market_stoploss_only(self):
        down_ratio = self._market_down_ratio()
        if down_ratio <= self.stoploss_market:
            self.reason_to_sell = "stoploss"
            self._event_log(f"大盘惨跌,跌幅{down_ratio:.2%}")
            for stock in list(self.get_positions().keys()):
                self._event_log(f"清仓卖出{stock}")
                self.close_position_(stock, "market_stoploss")
            return True
        return False

    def _market_down_ratio(self):
        trade_date = self._current_trade_date()
        if trade_date is not None:
            trade_date = self._load_previous_trade_date(trade_date) or trade_date
        if trade_date is not None:
            open_value = self._daily_value_on(self.index_symbol, "open", trade_date)
            close_value = self._daily_value_on(self.index_symbol, "close", trade_date)
            if open_value > 0 and close_value > 0:
                return float(close_value / open_value)
        open_values = self._index_history("open", 1)
        close_values = self._index_history("close", 1)
        if len(open_values) and len(close_values) and open_values[-1] > 0:
            return float(close_values[-1] / open_values[-1])
        return 1.0

    def _was_limit_up(self, symbol):
        trade_date = self._current_trade_date()
        if trade_date is not None:
            previous = self._load_previous_trade_date(trade_date)
            if previous is not None:
                high_limit = self._limit_price_on(symbol, previous, "up", adjust_to_minute=False)
                close_value = self._daily_value_on(symbol, "close", previous)
                if high_limit > 0 and close_value > 0:
                    return close_value >= high_limit - 1e-4
        close = self._daily_history(symbol, "close", 2, include_current=False)
        if len(close) < 2:
            return False
        high_limit = self._limit_price(symbol, close[-2], "up")
        return close[-1] >= high_limit - 1e-4

    def _is_limit_up_now(self, symbol):
        price = self._last_price(symbol)
        high_limit = self._current_limit_price(symbol, "up")
        if price <= 0 or high_limit <= 0:
            return False
        return price >= high_limit - 1e-4

    def _is_limit_down_now(self, symbol):
        price = self._last_price(symbol)
        low_limit = self._current_limit_price(symbol, "down")
        if price <= 0 or low_limit <= 0:
            return False
        return price <= low_limit + 1e-4

    def _current_limit_price(self, symbol, direction):
        trade_date = self._current_trade_date()
        if trade_date is None:
            return 0.0
        return self._limit_price_on(
            symbol,
            trade_date,
            direction,
            adjust_to_minute=self._using_intraday_bars() and not self._timer_prices_adjusted_to_daily_space(),
        )

    def _limit_price_on(self, symbol, trade_date, direction, adjust_to_minute=True):
        source = str(getattr(self, "limit_price_source", "table") or "table").lower()
        if source in {"table", "auto"}:
            if adjust_to_minute:
                table = self._adjusted_limit_prices_from_table(trade_date)
                value = table.get(self._store_data_symbol(symbol), {}).get(direction, 0.0)
            else:
                value = self._limit_price_from_table(symbol, trade_date, direction)
            if value > 0:
                return float(value)
            if source == "table" and adjust_to_minute:
                value = self._limit_price_from_minute_prev_close(symbol, trade_date, direction)
                if value > 0:
                    return value
        if source in {"minute_prev_close", "auto"}:
            value = self._limit_price_from_minute_prev_close(symbol, trade_date, direction)
            if value > 0:
                return value
        prev_close = self._previous_close_before_date(symbol, trade_date)
        return self._limit_price(symbol, prev_close, direction) if prev_close > 0 else 0.0

    def _adjust_limit_price_to_minute_space(self, symbol, trade_date, limit_price):
        previous = self._load_previous_trade_date(trade_date)
        if previous is None or limit_price <= 0:
            return float(limit_price or 0.0)
        daily_prev_close = self._daily_value_on(symbol, "close", previous)
        minute_prev_close = self._minute_close_at(symbol, datetime.combine(previous, time(15, 0)))
        if daily_prev_close <= 0 or minute_prev_close <= 0:
            return float(limit_price or 0.0)
        ratio = float(minute_prev_close) / float(daily_prev_close)
        if not np.isfinite(ratio) or ratio <= 0:
            return float(limit_price or 0.0)
        if abs(ratio - 1.0) < 1e-4:
            return float(limit_price or 0.0)
        return round(float(limit_price) * ratio + 1e-8, 2)

    def _adjusted_limit_prices_from_table(self, trade_date):
        cache_key = ("__adjusted_limit_prices_table__", trade_date)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        raw = self._limit_prices_from_table(trade_date)
        if not raw:
            self._daily_cache[cache_key] = {}
            return {}
        previous = self._load_previous_trade_date(trade_date)
        if previous is None:
            self._daily_cache[cache_key] = raw
            return raw
        daily_prev_close = self._daily_values_on("close", previous)
        minute_prev_close = self._minute_closes_at(datetime.combine(previous, time(15, 0)))
        if not daily_prev_close or not minute_prev_close:
            self._daily_cache[cache_key] = raw
            return raw

        result = {}
        for symbol, limits in raw.items():
            daily_value = float(daily_prev_close.get(symbol) or 0.0)
            minute_value = float(minute_prev_close.get(symbol) or 0.0)
            ratio = minute_value / daily_value if daily_value > 0 and minute_value > 0 else 1.0
            if not np.isfinite(ratio) or ratio <= 0 or abs(ratio - 1.0) < 1e-4:
                result[symbol] = limits
                continue
            result[symbol] = {
                "up": round(float(limits.get("up") or 0.0) * ratio + 1e-8, 2),
                "down": round(float(limits.get("down") or 0.0) * ratio + 1e-8, 2),
            }
        self._daily_cache[cache_key] = result
        return result

    def _limit_price_from_table(self, symbol, trade_date, direction):
        table = self._limit_prices_from_table(trade_date)
        symbol_key = self._store_data_symbol(symbol)
        value = table.get(symbol_key, {}).get(direction, 0.0)
        return float(value or 0.0)

    def _limit_prices_from_table(self, trade_date):
        cache_key = ("__limit_prices_table__", trade_date)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        db_path = self._db_path()
        if not db_path:
            self._daily_cache[cache_key] = {}
            return {}
        try:
            with sqlite3.connect(db_path) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_limit_prices'"
                ).fetchone()
                if not exists:
                    result = {}
                else:
                    rows = conn.execute(
                        """
                        SELECT symbol, up_limit, down_limit
                        FROM stock_limit_prices
                        WHERE trade_date = ?
                          AND (up_limit > 0 OR down_limit > 0)
                        """,
                        (trade_date.isoformat(),),
                    ).fetchall()
                    result = {
                        str(symbol): {
                            "up": float(up_limit or 0.0),
                            "down": float(down_limit or 0.0),
                        }
                        for symbol, up_limit, down_limit in rows
                    }
        except Exception:
            result = {}
        self._daily_cache[cache_key] = result
        return result

    def _limit_price_from_minute_prev_close(self, symbol, trade_date, direction):
        previous = self._load_previous_trade_date(trade_date)
        if previous is None:
            return 0.0
        prev_close = self._minute_closes_at(
            datetime.combine(previous, time(15, 0))
        ).get(self._store_data_symbol(symbol), 0.0)
        if prev_close <= 0:
            return 0.0
        return self._limit_price(symbol, prev_close, direction)

    def _limit_price(self, symbol, prev_close, direction):
        ratio = self._limit_ratio(symbol)
        multiplier = 1 + ratio if direction == "up" else 1 - ratio
        return round(float(prev_close) * multiplier + 1e-8, 2)

    def _limit_ratio(self, symbol):
        meta = self._stock_meta.get(symbol, {})
        as_of = self._history_as_of_date() or self._current_trade_date()
        if self._is_st_or_delisted(symbol, as_of):
            return 0.05
        sector = meta.get("sector") or ""
        if sector in {"科创板", "创业板"} or symbol.startswith(("300", "301", "688")):
            return 0.20
        if sector == "北交所" or symbol.startswith(("4", "8", "9")):
            return 0.30
        return 0.10

    def _previous_close(self, symbol):
        trade_date = self._current_trade_date()
        if trade_date is not None:
            value = self._daily_value_before(symbol, "close", trade_date)
            if value > 0:
                return value
        close = self._daily_history(symbol, "close", 2, include_current=True)
        if len(close) >= 2:
            return float(close[-2])
        return 0.0

    def _is_paused(self, symbol):
        if self._stock_meta.get(symbol, {}).get("is_suspend"):
            return True
        return self._last_price(symbol) <= 0

    def _last_price(self, symbol):
        if self._active_timer_datetime is not None and self._active_timer_datetime.time() >= time(15, 0):
            close_value = self._daily_value_on(symbol, "close", self._active_timer_datetime.date())
            if close_value > 0:
                return close_value
        if self._active_timer_datetime is not None and self.enable_timer_snapshots:
            snapshot = self._timer_snapshot(symbol)
            if snapshot is not None:
                close_value = float(snapshot.get("close") or 0.0)
                if close_value > 0:
                    return close_value
        bar = self._current_bars.get(symbol)
        if bar is not None:
            dt_value = self._bar_datetime(bar)
            close_value = float(getattr(bar, "close", 0) or 0)
            if dt_value is not None and not self._is_daily_bar(dt_value) and close_value > 0:
                return close_value
            if float(bar.open) > 0:
                return float(bar.open)
        if self.enable_timer_snapshots:
            snapshot = self._timer_snapshot(symbol)
            if snapshot is not None:
                close_value = float(snapshot.get("close") or 0.0)
                if close_value > 0:
                    return close_value
        trade_date = self._current_trade_date()
        if trade_date is not None:
            value = self._daily_value_on(symbol, "open", trade_date)
            if value > 0:
                return value
        open_values = self._daily_history(symbol, "open", 1, include_current=True)
        if len(open_values) and np.isfinite(open_values[-1]) and open_values[-1] > 0:
            return float(open_values[-1])
        close = self._daily_history(symbol, "close", 1, include_current=True)
        return float(close[-1]) if len(close) and np.isfinite(close[-1]) else 0.0

    def _timer_snapshot(self, symbol):
        current_dt = self._current_datetime(symbol)
        if current_dt is None:
            return None
        cache_key = ("__timer_snapshot__", current_dt)
        snapshots = self._daily_cache.get(cache_key)
        if snapshots is None:
            snapshots = self._load_timer_snapshots(current_dt)
            self._daily_cache[cache_key] = snapshots
        return snapshots.get(symbol)

    def _load_timer_snapshots(self, current_dt):
        symbols = set(self.get_positions().keys()) | set(self.target_list or []) | set(self.yesterday_HL_list or [])
        timer_key = current_dt.strftime("%H:%M")
        if timer_key == "10:30":
            symbols.update(self.get_index_stocks(self.index_symbol))
        if self.index_symbol:
            symbols.add(self.index_symbol)
        if not symbols:
            return {}
        try:
            df = self._market_store().load_minute(
                [self._store_data_symbol(s) for s in sorted(symbols)],
                current_dt,
                current_dt + timedelta(microseconds=1),
                columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"],
            )
            if df.empty:
                return {}
            reverse_symbols = {self._store_data_symbol(s): s for s in symbols}
            result = {}
            for data_symbol, group in df.groupby("symbol"):
                row = group.iloc[-1]
                symbol = reverse_symbols.get(str(data_symbol), str(data_symbol))
                result[symbol] = {
                    "open": float(row.get("open") or 0.0),
                    "high": float(row.get("high") or 0.0),
                    "low": float(row.get("low") or 0.0),
                    "close": float(row.get("close") or 0.0),
                    "volume": float(row.get("volume") or 0.0),
                    "amount": float(row.get("amount") or 0.0),
                }
                self._adjust_timer_snapshot_price_space(symbol, current_dt, result[symbol])
            return result
        except Exception:
            return {}

    def _timer_prices_adjusted_to_daily_space(self):
        mode = str(getattr(self, "timer_price_adjustment_mode", "") or "").lower()
        return mode in {"previous_close_ratio", "prev_close_ratio", "daily"}

    def _adjust_timer_snapshot_price_space(self, symbol, current_dt, snapshot):
        if not self._timer_prices_adjusted_to_daily_space():
            return snapshot
        ratio = self._timer_price_adjustment_ratio(symbol, current_dt)
        if not np.isfinite(ratio) or ratio <= 0 or abs(ratio - 1.0) < 1e-4:
            return snapshot
        for field in ("open", "high", "low", "close"):
            value = float(snapshot.get(field) or 0.0)
            if value > 0:
                snapshot[field] = round(value * ratio + 1e-8, 4)
        return snapshot

    def _timer_price_adjustment_ratio(self, symbol, current_dt):
        cache_key = ("__timer_price_adjustment_ratio__", symbol, current_dt.date())
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        previous = self._load_previous_trade_date(current_dt.date())
        if previous is None:
            self._daily_cache[cache_key] = 1.0
            return 1.0
        daily_prev_close = self._daily_value_on(symbol, "close", previous)
        minute_prev_close = self._minute_close_at(symbol, datetime.combine(previous, time(15, 0)))
        if daily_prev_close <= 0 or minute_prev_close <= 0:
            ratio = 1.0
        else:
            ratio = float(daily_prev_close) / float(minute_prev_close)
            if not np.isfinite(ratio) or ratio <= 0:
                ratio = 1.0
        self._daily_cache[cache_key] = ratio
        return ratio

    def _daily_value_on(self, symbol, field, trade_date):
        if field not in {"open", "high", "low", "close", "volume", "amount"}:
            return 0.0
        values = self._daily_values_on(field, trade_date)
        if symbol in values:
            return values[symbol]
        cache_key = ("__daily_value_on__", symbol, field, trade_date)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            df = self._market_store().load_daily(
                [self._store_data_symbol(symbol)],
                trade_date,
                trade_date,
                columns=["symbol", "trade_date", field],
            )
            if not df.empty and field in df.columns:
                value = float(df[field].dropna().iloc[-1] or 0)
                self._daily_cache[cache_key] = value
                return value
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            self._daily_cache[cache_key] = 0.0
            return 0.0
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                f"""
                SELECT anyLast({field})
                FROM klines_daily
                WHERE symbol = %(symbol)s
                  AND trade_date = %(trade_date)s
                """,
                {"symbol": self._normalize_data_symbol(symbol), "trade_date": trade_date},
            )
            value = float(rows[0][0] or 0) if rows else 0.0
        except Exception:
            value = 0.0
        self._daily_cache[cache_key] = value
        return value

    def _daily_value_before(self, symbol, field, trade_date):
        if field not in {"open", "high", "low", "close", "volume", "amount"}:
            return 0.0
        values = self._daily_values_before(field, trade_date)
        if symbol in values:
            return values[symbol]
        cache_key = ("__daily_value_before__", symbol, field, trade_date)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            values = self._market_store().load_latest_daily_values(
                [self._store_data_symbol(symbol)],
                field,
                trade_date - timedelta(days=500),
                trade_date - timedelta(days=1),
            )
            value = float(next(iter(values.values())) or 0.0) if values else 0.0
            if value > 0:
                self._daily_cache[cache_key] = value
                return value
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            self._daily_cache[cache_key] = 0.0
            return 0.0
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                f"""
                SELECT argMax({field}, trade_date)
                FROM klines_daily
                WHERE symbol = %(symbol)s
                  AND trade_date < %(trade_date)s
                """,
                {"symbol": self._normalize_data_symbol(symbol), "trade_date": trade_date},
            )
            value = float(rows[0][0] or 0) if rows else 0.0
        except Exception:
            value = 0.0
        self._daily_cache[cache_key] = value
        return value

    def _previous_close_before_date(self, symbol, trade_date):
        previous = self._load_previous_trade_date(trade_date)
        if previous is not None:
            value = self._daily_value_on(symbol, "close", previous)
            if value > 0:
                return value
        return self._daily_value_before(symbol, "close", trade_date)

    def _minute_close_at(self, symbol, timestamp):
        cache_key = ("__minute_close_at__", symbol, timestamp)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        batch_value = self._minute_closes_at(timestamp).get(self._store_data_symbol(symbol), 0.0)
        if batch_value > 0:
            self._daily_cache[cache_key] = batch_value
            return batch_value
        try:
            df = self._market_store().load_minute(
                [self._store_data_symbol(symbol)],
                timestamp,
                timestamp + timedelta(microseconds=1),
                columns=["symbol", "datetime", "close"],
                timer_times=[timestamp.strftime("%H:%M")],
            )
            if not df.empty and "close" in df.columns:
                value = float(df["close"].dropna().iloc[-1] or 0.0)
                self._daily_cache[cache_key] = value
                return value
        except Exception:
            pass
        self._daily_cache[cache_key] = 0.0
        return 0.0

    def _minute_closes_at(self, timestamp):
        cache_key = ("__minute_closes_at__", timestamp)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        symbols = set(self.get_index_stocks(self.index_symbol))
        symbols.update(str(symbol) for symbol in self.get_positions().keys())
        symbols.update(str(symbol) for symbol in (self.target_list or []))
        symbols.update(str(symbol) for symbol in (self.yesterday_HL_list or []))
        symbols.discard("")
        if not symbols:
            self._daily_cache[cache_key] = {}
            return {}
        try:
            data_symbols = [self._store_data_symbol(s) for s in sorted(symbols)]
            df = self._market_store().load_minute(
                data_symbols,
                timestamp,
                timestamp + timedelta(microseconds=1),
                columns=["symbol", "datetime", "close"],
                timer_times=[timestamp.strftime("%H:%M")],
            )
            if df.empty or "symbol" not in df.columns or "close" not in df.columns:
                result = {}
            else:
                result = {
                    str(symbol): float(group["close"].dropna().iloc[-1] or 0.0)
                    for symbol, group in df.groupby("symbol")
                    if len(group["close"].dropna())
                }
        except Exception:
            result = {}
        self._daily_cache[cache_key] = result
        return result

    def _daily_values_on(self, field, trade_date):
        cache_key = ("__daily_values_on__", field, trade_date)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        symbols = self.get_index_stocks(self.index_symbol)
        if not symbols:
            return {}
        try:
            values = self._market_store().load_latest_daily_values(
                [self._store_data_symbol(s) for s in symbols],
                field,
                trade_date,
                trade_date,
            )
            if values:
                reverse_symbols = {self._store_data_symbol(s): s for s in symbols}
                result = {
                    reverse_symbols.get(str(symbol), str(symbol)): float(value or 0.0)
                    for symbol, value in values.items()
                }
                self._daily_cache[cache_key] = result
                return result
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            self._daily_cache[cache_key] = {}
            return {}
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                f"""
                SELECT symbol, anyLast({field})
                FROM klines_daily
                WHERE symbol IN %(symbols)s
                  AND trade_date = %(trade_date)s
                GROUP BY symbol
                """,
                {"symbols": symbols, "trade_date": trade_date},
            )
            result = {str(symbol): float(value or 0) for symbol, value in rows}
        except Exception:
            result = {}
        self._daily_cache[cache_key] = result
        return result

    def _daily_values_before(self, field, trade_date):
        cache_key = ("__daily_values_before__", field, trade_date)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        symbols = self.get_index_stocks(self.index_symbol)
        if not symbols:
            return {}
        try:
            values = self._market_store().load_latest_daily_values(
                [self._store_data_symbol(s) for s in symbols],
                field,
                trade_date - timedelta(days=500),
                trade_date - timedelta(days=1),
            )
            if values:
                reverse_symbols = {self._store_data_symbol(s): s for s in symbols}
                result = {
                    reverse_symbols.get(str(symbol), str(symbol)): float(value or 0.0)
                    for symbol, value in values.items()
                }
                self._daily_cache[cache_key] = result
                return result
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            self._daily_cache[cache_key] = {}
            return {}
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                f"""
                SELECT symbol, argMax({field}, trade_date)
                FROM klines_daily
                WHERE symbol IN %(symbols)s
                  AND trade_date < %(trade_date)s
                GROUP BY symbol
                """,
                {"symbols": symbols, "trade_date": trade_date},
            )
            result = {str(symbol): float(value or 0) for symbol, value in rows}
        except Exception:
            result = {}
        self._daily_cache[cache_key] = result
        return result

    def _daily_history(self, symbol, field, count, include_current=False):
        queried = self._query_kline_daily(symbol, field, count)
        if field == "volume":
            queried = self._daily_volume_in_minute_unit(queried)
        if include_current and symbol in self._current_bars and field in {"open", "high", "low", "close", "volume"}:
            if field == "volume":
                current = self._current_day_volume_to_now(symbol)
            else:
                current = float(getattr(self._current_bars[symbol], field))
            if len(queried) == 0 or abs(queried[-1] - current) > 1e-9:
                queried = np.append(queried, current)
        return queried[-count:]

    def _daily_frame_between(self, symbol, start_date, end_date):
        if start_date is None or end_date is None:
            return {}
        cache_key = ("__daily_frame_between__", symbol, start_date, end_date)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        query_symbol = self._normalize_data_symbol(symbol)
        global_frame = self._daily_global_frame(query_symbol)
        if global_frame is not None:
            dates = global_frame.get("trade_date")
            if dates is not None and len(dates):
                mask = (dates >= np.datetime64(start_date)) & (dates <= np.datetime64(end_date))
                if mask.any():
                    frame = {
                        key: np.asarray(value)[mask]
                        for key, value in global_frame.items()
                        if key != "trade_date"
                    }
                    self._daily_cache[cache_key] = frame
                    return frame
        try:
            df = self._market_store().load_daily(
                [self._store_data_symbol(symbol)],
                start_date,
                end_date,
                columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
            )
            if not df.empty:
                df = df.sort_index()
                frame = {
                    "open": df["open"].astype(float).to_numpy(),
                    "high": df["high"].astype(float).to_numpy(),
                    "low": df["low"].astype(float).to_numpy(),
                    "close": df["close"].astype(float).to_numpy(),
                    "volume": df["volume"].astype(float).to_numpy(),
                    "amount": df["amount"].astype(float).to_numpy(),
                }
            else:
                frame = {}
        except Exception:
            frame = {}
        self._daily_cache[cache_key] = frame
        return frame

    def _daily_volume_in_minute_unit(self, values):
        if len(values) == 0:
            return values
        multiplier = float(getattr(self, "daily_volume_to_share_multiplier", 100.0))
        if not np.isfinite(multiplier) or multiplier <= 0:
            multiplier = 1.0
        if abs(multiplier - 1.0) < 1e-12:
            return values.astype(float, copy=False)
        return values.astype(float, copy=False) * multiplier

    def _current_day_volume_to_now(self, symbol, as_of=None):
        return self._intraday_volume_indicator.get(symbol, as_of=as_of)

    def _high_volume_feature_signal_map(self, symbols, as_of):
        if not getattr(self, "enable_factor_value_cache", True) or as_of is None:
            return None
        if str(getattr(self, "high_volume_mode", "daily_include_now")) != "minute_to_now":
            return None
        factor_name = str(getattr(self, "high_volume_factor_name", "high_volume_signal"))
        if factor_name != "high_volume_signal":
            return None
        trade_date = self._current_trade_date()
        if trade_date is None:
            return None
        params = {
            "time": str(getattr(self, "high_volume_check_time", "14:30"))[:5],
            "window": int(getattr(self, "HV_duration", 120)),
            "threshold": float(getattr(self, "HV_ratio", 0.9)),
            "daily_volume_to_share_multiplier": float(
                getattr(self, "daily_volume_to_share_multiplier", 100.0)
            ),
        }
        symbols = [symbol for symbol in symbols if symbol]
        if not symbols:
            return None
        cache_key = (
            "__factor_high_volume_signal_map__",
            tuple(sorted(symbols)),
            trade_date,
            tuple(sorted(params.items())),
        )
        if cache_key in self._daily_cache:
            return self._daily_cache[cache_key]
        try:
            from app.services.factor_value_store import get_factor_value_store

            values = get_factor_value_store().load_cross_section(
                factor_name,
                trade_date,
                symbols=[self._store_data_symbol(symbol) for symbol in symbols],
                as_of_time=params["time"],
                params=params,
            )
            data_to_strategy = {
                self._store_data_symbol(strategy_symbol): strategy_symbol
                for strategy_symbol in symbols
            }
            result = {}
            for data_symbol, value in values.items():
                strategy_symbol = data_to_strategy.get(str(data_symbol), str(data_symbol))
                result[strategy_symbol] = bool(float(value) >= 0.5)
        except Exception:
            result = None
        self._daily_cache[cache_key] = result
        return result

    def _current_timer_datetime(self, timer_text=None):
        trade_date = self._current_trade_date()
        if trade_date is None:
            return None
        timer_text = str(timer_text or "").strip()
        if not timer_text:
            return None
        parts = timer_text.split(":")
        try:
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            second = int(parts[2]) if len(parts) > 2 else 0
        except Exception:
            return None
        return datetime.combine(trade_date, time(hour, minute, second))

    def _index_history(self, field, count):
        candidates = [self.index_symbol]
        for symbol in (self.jq_index_symbol, "399101.SZ", "399101.XSHE"):
            if symbol not in candidates:
                candidates.append(symbol)
        for symbol in candidates:
            values = self._query_kline_daily(symbol, field, count)
            if len(values):
                return values[-count:]
        return np.asarray([], dtype=float)

    def _history(self, symbol, field, count):
        try:
            return np.asarray(self.get_history(count, symbol, field), dtype=float)
        except Exception:
            return np.asarray([], dtype=float)

    def _query_kline_daily(self, symbol, field, count):
        if field not in {"open", "high", "low", "close", "volume", "amount"}:
            return np.asarray([], dtype=float)
        as_of = self._history_as_of_date()
        query_symbol = self._normalize_data_symbol(symbol)
        cache_key = (symbol, as_of)
        cached = self._daily_cache.get(cache_key)
        if cached is not None and field in cached:
            return cached[field][-count:]
        global_frame = self._daily_global_frame(query_symbol)
        if global_frame is not None and as_of is not None and field in global_frame:
            dates = global_frame.get("trade_date")
            values = global_frame.get(field)
            if dates is not None and values is not None and len(values):
                mask = dates <= np.datetime64(as_of)
                if mask.any():
                    frame = {
                        key: np.asarray(value)[mask][-260:]
                        for key, value in global_frame.items()
                        if key != "trade_date"
                    }
                    self._daily_cache[cache_key] = frame
                    return frame[field][-count:]
        redis_cache = self._backtest_cache()
        redis_key = None
        redis_window = max(count, 260)
        if redis_cache is not None:
            redis_key = redis_cache.key("daily_window", symbol, as_of or "latest", redis_window)
            frame = self._redis_daily_frame(redis_key)
            if frame is not None:
                self._daily_cache[cache_key] = frame
                if field in frame:
                    return frame[field][-count:]
        try:
            end_date = as_of or self._current_trade_date() or date.today()
            start_date = end_date - timedelta(days=max(800, int(redis_window * 3)))
            df = self._market_store().load_daily(
                [self._store_data_symbol(symbol)],
                start_date,
                end_date,
                columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
            )
            if not df.empty:
                df = df.sort_index().tail(redis_window)
                frame = {
                    "open": df["open"].astype(float).to_numpy(),
                    "high": df["high"].astype(float).to_numpy(),
                    "low": df["low"].astype(float).to_numpy(),
                    "close": df["close"].astype(float).to_numpy(),
                    "volume": df["volume"].astype(float).to_numpy(),
                    "amount": df["amount"].astype(float).to_numpy(),
                }
                self._daily_cache[cache_key] = frame
                return frame[field][-count:]
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            return np.asarray([], dtype=float)
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            where_date = ""
            params = {"symbol": symbol, "limit": redis_window}
            if as_of is not None:
                where_date = "AND trade_date <= %(as_of)s"
                params["as_of"] = as_of
            rows = ch.execute(
                f"""
                SELECT
                    trade_date,
                    anyLast(open) AS open,
                    anyLast(high) AS high,
                    anyLast(low) AS low,
                    anyLast(close) AS close,
                    anyLast(volume) AS volume,
                    anyLast(amount) AS amount
                FROM klines_daily
                WHERE symbol = %(symbol)s
                {where_date}
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT %(limit)s
                """,
                {**params, "symbol": query_symbol},
            )
            rows = list(reversed(rows))
            frame = {
                "open": np.asarray([float(r[1]) for r in rows], dtype=float),
                "high": np.asarray([float(r[2]) for r in rows], dtype=float),
                "low": np.asarray([float(r[3]) for r in rows], dtype=float),
                "close": np.asarray([float(r[4]) for r in rows], dtype=float),
                "volume": np.asarray([float(r[5]) for r in rows], dtype=float),
                "amount": np.asarray([float(r[6]) for r in rows], dtype=float),
            }
            self._daily_cache[cache_key] = frame
            if redis_cache is not None and redis_key is not None and len(rows):
                self._set_redis_daily_frame(redis_key, rows)
            return frame[field][-count:]
        except Exception:
            return np.asarray([], dtype=float)

    def _daily_global_frame(self, symbol):
        cache_key = ("__daily_global_frame__", symbol)
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            end_date = self._parse_date(getattr(self, "backtest_end_date", None)) or self._current_trade_date() or date.today()
            start_date = self._parse_date(getattr(self, "backtest_start_date", None)) or end_date
            start_date = start_date - timedelta(days=900)
            df = self._market_store().load_daily(
                [self._store_data_symbol(symbol)],
                start_date,
                end_date,
                columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
            )
            if df.empty:
                self._daily_cache[cache_key] = None
                return None
            df = df.sort_index()
            frame = {
                "trade_date": df.index.to_numpy(dtype="datetime64[D]"),
                "open": df["open"].astype(float).to_numpy(),
                "high": df["high"].astype(float).to_numpy(),
                "low": df["low"].astype(float).to_numpy(),
                "close": df["close"].astype(float).to_numpy(),
                "volume": df["volume"].astype(float).to_numpy(),
                "amount": df["amount"].astype(float).to_numpy(),
            }
        except Exception:
            frame = None
        self._daily_cache[cache_key] = frame
        return frame

    def _preload_daily_history(self, symbols, count):
        if not symbols:
            return
        as_of = self._history_as_of_date()
        if as_of is None:
            return
        missing = [
            symbol for symbol in symbols
            if (symbol, as_of) not in self._daily_cache
        ]
        if not missing:
            return
        redis_cache = self._backtest_cache()
        redis_window = max(count, 260)
        if redis_cache is not None:
            still_missing = []
            for symbol in missing:
                redis_key = redis_cache.key("daily_window", symbol, as_of, redis_window)
                frame = self._redis_daily_frame(redis_key)
                if frame is not None:
                    self._daily_cache[(symbol, as_of)] = frame
                else:
                    still_missing.append(symbol)
            missing = still_missing
            if not missing:
                return
        start_date = as_of - timedelta(days=max(400, int(redis_window * 2.2)))
        try:
            df = self._market_store().load_daily(
                [self._store_data_symbol(s) for s in missing],
                start_date,
                as_of,
                columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
            )
            if not df.empty:
                loaded = set()
                for sym, group in df.groupby("symbol"):
                    sym_rows = group.sort_index().tail(redis_window)
                    frame = {
                        "open": sym_rows["open"].astype(float).to_numpy(),
                        "high": sym_rows["high"].astype(float).to_numpy(),
                        "low": sym_rows["low"].astype(float).to_numpy(),
                        "close": sym_rows["close"].astype(float).to_numpy(),
                        "volume": sym_rows["volume"].astype(float).to_numpy(),
                        "amount": sym_rows["amount"].astype(float).to_numpy(),
                    }
                    cache_symbol = self.index_symbol if sym == self._store_data_symbol(self.index_symbol) else str(sym)
                    self._daily_cache[(cache_symbol, as_of)] = frame
                    loaded.add(cache_symbol)
                missing = [s for s in missing if s not in loaded]
                if not missing:
                    return
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            return
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                """
                SELECT
                    symbol,
                    trade_date,
                    anyLast(open) AS open,
                    anyLast(high) AS high,
                    anyLast(low) AS low,
                    anyLast(close) AS close,
                    anyLast(volume) AS volume,
                    anyLast(amount) AS amount
                FROM klines_daily
                WHERE symbol IN %(symbols)s
                  AND trade_date >= %(start)s
                  AND trade_date <= %(as_of)s
                GROUP BY symbol, trade_date
                ORDER BY symbol, trade_date
                """,
                {"symbols": [self._normalize_data_symbol(s) for s in missing], "start": start_date, "as_of": as_of},
            )
        except Exception:
            return
        grouped = {}
        for row in rows:
            symbol = str(row[0])
            if symbol == self.jq_index_symbol:
                symbol = self.index_symbol
            grouped.setdefault(symbol, []).append(row)
        for symbol, sym_rows in grouped.items():
            sym_rows = sym_rows[-redis_window:]
            frame = {
                "open": np.asarray([float(r[2]) for r in sym_rows], dtype=float),
                "high": np.asarray([float(r[3]) for r in sym_rows], dtype=float),
                "low": np.asarray([float(r[4]) for r in sym_rows], dtype=float),
                "close": np.asarray([float(r[5]) for r in sym_rows], dtype=float),
                "volume": np.asarray([float(r[6]) for r in sym_rows], dtype=float),
                "amount": np.asarray([float(r[7]) for r in sym_rows], dtype=float),
            }
            self._daily_cache[(symbol, as_of)] = frame
            if redis_cache is not None:
                redis_key = redis_cache.key("daily_window", symbol, as_of, redis_window)
                self._set_redis_daily_frame(redis_key, sym_rows)

    def _normalize_data_symbol(self, symbol):
        normalized = normalize_security_symbol(symbol) or symbol
        if normalized == self.index_symbol:
            return self.jq_index_symbol
        return normalized

    def _store_data_symbol(self, symbol):
        normalized = normalize_security_symbol(symbol) or symbol
        if normalized == self.index_symbol:
            return self.index_symbol
        return normalized

    def _current_trade_date(self):
        current_time = None
        ctx = getattr(self, "ctx", None)
        if ctx is not None:
            current_time = getattr(ctx, "current_time", None)
        if current_time:
            try:
                return self._timestamp_to_local_datetime(current_time).date()
            except Exception:
                pass
        if self._last_trade_date is not None:
            return self._last_trade_date
        return None

    def _current_datetime(self, symbol=None):
        if self._active_timer_datetime is not None:
            return self._active_timer_datetime
        if symbol is not None:
            bar = self._current_bars.get(symbol)
            dt_value = self._bar_datetime(bar) if bar is not None else None
            if dt_value is not None:
                return dt_value
        ctx = getattr(self, "ctx", None)
        current_time = getattr(ctx, "current_time", None) if ctx is not None else None
        if current_time:
            try:
                return self._timestamp_to_local_datetime(current_time)
            except Exception:
                pass
        trade_date = self._current_trade_date()
        return datetime.combine(trade_date, datetime.min.time()) if trade_date is not None else None

    def _history_as_of_date(self):
        trade_date = self._current_trade_date()
        if trade_date is not None:
            previous = self._load_previous_trade_date(trade_date)
            if previous is not None:
                return previous
        return self._previous_trade_date or trade_date

    def _load_index_components(self, index_symbol):
        db_path = self._db_path()
        if not db_path:
            return []
        tables = ("index_components", "index_constituents")
        as_of = self._history_as_of_date() or self._current_trade_date()
        index_symbol = self._normalize_index_symbol(index_symbol)
        jq_symbol = self._to_jq_index_symbol(index_symbol)
        memory_key = ("__index_components__", index_symbol, as_of)
        cached = self._daily_cache.get(memory_key)
        if cached is not None:
            return cached
        redis_cache = self._backtest_cache()
        redis_key = None
        if redis_cache is not None and as_of is not None:
            redis_key = redis_cache.key("index_components", index_symbol, as_of)
            redis_value = redis_cache.get_json(redis_key)
            if isinstance(redis_value, list):
                symbols = [str(symbol) for symbol in redis_value]
                self._daily_cache[memory_key] = symbols
                return symbols
        for table in tables:
            try:
                with sqlite3.connect(db_path) as conn:
                    exists = conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (table,),
                    ).fetchone()
                    if not exists:
                        continue
                    rows = conn.execute(
                        f"""
                        SELECT symbol
                        FROM {table}
                        WHERE (index_symbol IN (?, ?) OR jq_index_symbol IN (?, ?))
                          AND trade_date = (
                              SELECT MAX(trade_date)
                              FROM {table}
                              WHERE (index_symbol IN (?, ?) OR jq_index_symbol IN (?, ?))
                                AND trade_date <= ?
                          )
                        ORDER BY symbol
                        """,
                        (
                            index_symbol,
                            jq_symbol,
                            index_symbol,
                            jq_symbol,
                            index_symbol,
                            jq_symbol,
                            index_symbol,
                            jq_symbol,
                            as_of,
                        ),
                    ).fetchall()
                    symbols = [r[0] for r in rows]
                    if symbols:
                        self._daily_cache[memory_key] = symbols
                        if redis_cache is not None and redis_key is not None:
                            redis_cache.set_json(redis_key, symbols)
                        return symbols
            except Exception:
                continue
        return []

    @staticmethod
    def _normalize_index_symbol(symbol):
        return normalize_security_symbol(symbol) or symbol

    @staticmethod
    def _to_jq_index_symbol(symbol):
        return to_jq_symbol(symbol) or symbol

    def _sort_by_market_cap(self, symbols, trade_date=None):
        as_of = self._market_cap_as_of_date(trade_date)
        if as_of is None:
            self._event_log("DEBUG_MARKET_CAP_ASOF_MISSING", trade_date=trade_date)
            return sorted(symbols)
        daily_basic = self._daily_basic_market_caps(symbols, as_of)
        prices = self._latest_closes(symbols, as_of)

        def mv(symbol):
            point_in_time = daily_basic.get(symbol)
            if point_in_time and point_in_time > 0:
                return float(point_in_time)
            meta = self._stock_meta.get(symbol, {})
            shares = meta.get("total_shares") or 0
            price = prices.get(symbol)
            if shares and price and price > 0:
                return self._shares_to_10k(float(shares)) * float(price)
            value = meta.get("market_cap")
            return float(value) if value and value > 0 else float("inf")

        ranked = sorted(symbols, key=mv)
        if ranked and mv(ranked[0]) == float("inf"):
            return sorted(symbols)
        return ranked

    def _market_cap_as_of_date(self, trade_date):
        if trade_date is not None:
            previous = self._load_previous_trade_date(trade_date)
            if previous is not None and previous < trade_date:
                return previous
            return None
        as_of = self._history_as_of_date()
        current = self._current_trade_date()
        if current is not None and as_of is not None and as_of >= current:
            return None
        return as_of

    def _daily_basic_market_caps(self, symbols, as_of):
        if not symbols or as_of is None:
            return {}
        cache_key = ("__daily_basic_mv__", as_of, tuple(sorted(symbols)))
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        index_key = self._normalize_index_symbol(getattr(self, "index_symbol", "399101.SZ"))
        redis_cache = self._backtest_cache()
        redis_key = None
        if redis_cache is not None:
            redis_key = redis_cache.key("daily_basic_mv", index_key, as_of)
            redis_value = redis_cache.get_json(redis_key)
            if isinstance(redis_value, dict):
                wanted = set(symbols)
                result = {
                    str(symbol): float(value)
                    for symbol, value in redis_value.items()
                    if symbol in wanted and value is not None
                }
                if result:
                    self._daily_cache[cache_key] = result
                    return result
        db_path = self._db_path()
        if not db_path:
            return {}
        placeholders = ",".join("?" for _ in symbols)
        try:
            with sqlite3.connect(db_path) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_daily_basic'"
                ).fetchone()
                if not exists:
                    return {}
                rows = conn.execute(
                    f"""
                    SELECT symbol, trade_date, total_mv
                    FROM stock_daily_basic
                    WHERE symbol IN ({placeholders})
                      AND trade_date <= ?
                      AND total_mv > 0
                    ORDER BY symbol, trade_date DESC
                    """,
                    (*symbols, as_of),
                ).fetchall()
        except Exception:
            return {}
        result = {}
        for symbol, _, total_mv in rows:
            if symbol not in result:
                result[str(symbol)] = float(total_mv)
        self._daily_cache[cache_key] = result
        if redis_cache is not None and redis_key is not None and result:
            redis_cache.set_json(redis_key, result)
        return result

    def _backtest_cache(self):
        if hasattr(self, "__backtest_cache"):
            return self.__backtest_cache
        enabled = str(getattr(self, "enable_indicator_redis", False)).lower()
        if enabled not in {"1", "true", "yes", "on"}:
            self.__backtest_cache = None
            return None
        try:
            from app.services.backtest_redis_cache import get_backtest_cache

            cache = get_backtest_cache()
            self.__backtest_cache = cache if cache.available else None
        except Exception:
            self.__backtest_cache = None
        return self.__backtest_cache

    def _redis_daily_frame(self, redis_key):
        redis_cache = self._backtest_cache()
        if redis_cache is None:
            return None
        payload = redis_cache.get_json(redis_key)
        if not isinstance(payload, dict):
            return None
        try:
            return {
                field: np.asarray(payload.get(field, []), dtype=float)
                for field in ("open", "high", "low", "close", "volume", "amount")
            }
        except Exception:
            return None

    def _set_redis_daily_frame(self, redis_key, rows):
        redis_cache = self._backtest_cache()
        if redis_cache is None or not rows:
            return
        try:
            first = rows[0]
            has_symbol = len(first) >= 8
            date_idx = 1 if has_symbol else 0
            open_idx = date_idx + 1
            payload = {
                "date": [str(r[date_idx])[:10] for r in rows],
                "open": [round(float(r[open_idx]), 6) for r in rows],
                "high": [round(float(r[open_idx + 1]), 6) for r in rows],
                "low": [round(float(r[open_idx + 2]), 6) for r in rows],
                "close": [round(float(r[open_idx + 3]), 6) for r in rows],
                "volume": [round(float(r[open_idx + 4]), 3) for r in rows],
                "amount": [round(float(r[open_idx + 5]), 3) for r in rows],
            }
            redis_cache.set_json(redis_key, payload)
        except Exception:
            return

    @staticmethod
    def _shares_to_10k(value):
        if value > 10_000_000:
            return value / 10_000.0
        return value

    def _latest_closes(self, symbols, as_of):
        if not symbols:
            return {}
        cache_key = ("__closes__", as_of, tuple(sorted(symbols)))
        cached = self._daily_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            result = self._market_store().load_latest_daily_values(
                [self._store_data_symbol(s) for s in symbols],
                "close",
                as_of - timedelta(days=500),
                as_of,
            )
            if result:
                reverse_symbols = {self._store_data_symbol(s): s for s in symbols}
                normalized = {
                    reverse_symbols.get(str(symbol), str(symbol)): float(value)
                    for symbol, value in result.items()
                    if float(value or 0.0) > 0
                }
                self._daily_cache[cache_key] = normalized
                return normalized
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            self._daily_cache[cache_key] = {}
            return {}
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                """
                SELECT symbol, argMax(close, trade_date) AS close
                FROM klines_daily
                WHERE symbol IN %(symbols)s
                  AND trade_date <= %(as_of)s
                GROUP BY symbol
                """,
                {"symbols": list(symbols), "as_of": as_of},
            )
            result = {str(symbol): float(close) for symbol, close in rows if close and close > 0}
            self._daily_cache[cache_key] = result
            return result
        except Exception:
            return {}

    def _load_previous_trade_date(self, trade_date):
        cached = self._previous_trade_date_cache.get(trade_date)
        if cached is not None:
            return cached
        try:
            days = self._market_store().load_trading_dates(
                [self._store_data_symbol(self.index_symbol)],
                trade_date - timedelta(days=30),
                trade_date - timedelta(days=1),
            )
            if days:
                value = days[-1]
                self._previous_trade_date_cache[trade_date] = value
                return value
        except Exception:
            pass
        if not self._allow_clickhouse_fallback():
            fallback = trade_date - timedelta(days=1)
            self._previous_trade_date_cache[trade_date] = fallback
            return fallback
        try:
            from app.db.clickhouse import get_ch_client

            ch = get_ch_client()
            rows = ch.execute(
                """
                SELECT max(trade_date)
                FROM klines_daily
                WHERE trade_date < %(trade_date)s
                  AND symbol = %(symbol)s
                """,
                {"trade_date": trade_date, "symbol": self.jq_index_symbol},
            )
            if rows and rows[0][0]:
                self._previous_trade_date_cache[trade_date] = rows[0][0]
                return rows[0][0]
        except Exception:
            pass
        fallback = trade_date - timedelta(days=1)
        self._previous_trade_date_cache[trade_date] = fallback
        return fallback

    def _market_store(self):
        if hasattr(self, "_market_store_instance"):
            return self._market_store_instance
        from app.data_stores import get_market_data_store

        self._market_store_instance = get_market_data_store()
        return self._market_store_instance

    def _allow_clickhouse_fallback(self):
        backend = str(os.getenv("MARKET_DATA_BACKEND", "") or "").lower()
        enabled = str(os.getenv("CLICKHOUSE_ENABLED", "") or "").lower()
        if backend == "parquet":
            return False
        if enabled in {"0", "false", "no", "off"}:
            return False
        return True

    def _load_stock_name_changes(self):
        db_path = self._db_path()
        if not db_path:
            return {}
        try:
            with sqlite3.connect(db_path) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_name_changes'"
                ).fetchone()
                if not exists:
                    return {}
                rows = conn.execute(
                    """
                    SELECT symbol, name, start_date, end_date
                    FROM stock_name_changes
                    WHERE start_date IS NOT NULL
                    ORDER BY symbol, start_date, name
                    """
                ).fetchall()
        except Exception:
            return {}
        changes = {}
        for symbol, name, start_date, end_date in rows:
            changes.setdefault(str(symbol), []).append((str(name or ""), start_date, end_date))
        return changes

    def _is_st_or_delisted(self, symbol, as_of):
        meta = self._stock_meta.get(symbol, {})
        if not self.filter_st:
            return bool(meta.get("is_delist"))
        if as_of is not None:
            intervals = self._name_change_intervals(symbol)
            if intervals:
                return any(
                    start_date <= as_of <= end_date and self._is_bad_stock_name(name)
                    for name, start_date, end_date in intervals
                )
        name = meta.get("name") or ""
        return bool(meta.get("is_st") or meta.get("is_delist") or self._is_bad_stock_name(name))

    def _name_change_intervals(self, symbol):
        cached = self._stock_name_change_intervals.get(symbol)
        if cached is not None:
            return cached
        raw = self._stock_name_changes.get(symbol, [])
        grouped = {}
        for name, start_text, end_text in raw:
            start_date = self._parse_date(start_text)
            if start_date is None:
                continue
            key = (start_date, name)
            end_date = self._parse_date(end_text)
            if key not in grouped or end_date is not None:
                grouped[key] = end_date
        entries = [
            {"start": start, "name": name, "end": end}
            for (start, name), end in grouped.items()
        ]
        entries.sort(key=lambda item: (item["start"], item["name"]))
        intervals = []
        for idx, item in enumerate(entries):
            end_date = item["end"]
            if end_date is None:
                next_start = next(
                    (
                        entry["start"]
                        for entry in entries[idx + 1 :]
                        if entry["start"] > item["start"]
                    ),
                    None,
                )
                end_date = next_start - timedelta(days=1) if next_start else date(2099, 12, 31)
            intervals.append((item["name"], item["start"], end_date))
        self._stock_name_change_intervals[symbol] = intervals
        return intervals

    def _list_date(self, symbol):
        list_date = self._stock_meta.get(symbol, {}).get("list_date")
        intervals = self._name_change_intervals(symbol)
        if intervals:
            earliest = min(start for _, start, _ in intervals)
            if list_date is None or list_date > earliest:
                return earliest
        return list_date

    @staticmethod
    def _is_bad_stock_name(name):
        text = str(name or "").upper()
        return "ST" in text or "*" in text or "退" in text

    @staticmethod
    def _parse_date(value):
        if value in {None, "", "None"}:
            return None
        if isinstance(value, date):
            return value
        try:
            text = str(value)[:10]
            if "-" in text:
                return datetime.strptime(text, "%Y-%m-%d").date()
            return datetime.strptime(text[:8], "%Y%m%d").date()
        except Exception:
            return None

    def _sync_position_state(self, symbol, price):
        positions = self.get_positions()
        for held in list(self._position_qty.keys()):
            if held not in positions:
                self._position_qty.pop(held, None)
                self._position_cost.pop(held, None)
        qty = float(positions.get(symbol, 0))
        old_qty = float(self._position_qty.get(symbol, 0))
        if qty <= 0:
            return
        if qty > old_qty and price > 0:
            self._record_buy_cost(symbol, qty - old_qty, price)
        elif old_qty == 0 and price > 0:
            self._position_cost[symbol] = price
        self._position_qty[symbol] = qty

    def _record_buy_cost(self, symbol, buy_qty, price):
        old_qty = float(self._position_qty.get(symbol, 0))
        old_cost = float(self._position_cost.get(symbol, price))
        new_qty = old_qty + float(buy_qty)
        if new_qty <= 0:
            return
        self._position_cost[symbol] = (old_cost * old_qty + float(price) * float(buy_qty)) / new_qty
        self._position_qty[symbol] = new_qty

    def _avg_cost(self, symbol):
        cost = self._position_cost.get(symbol)
        if cost and cost > 0:
            return float(cost)
        price = self._last_price(symbol)
        if price > 0 and symbol in self.get_positions():
            self._position_cost[symbol] = price
            self._position_qty[symbol] = float(self.get_position(symbol))
        return float(self._position_cost.get(symbol, 0.0))

    def _portfolio_total_value(self):
        total = float(self.get_cash())
        for symbol, quantity in self.get_positions().items():
            total += float(quantity or 0) * max(0.0, self._last_price(symbol))
        return total

    def _event_log(self, message, **fields):
        if not self.debug_logging and str(message).startswith("DEBUG_"):
            return
        if fields:
            parts = []
            for key, value in fields.items():
                if isinstance(value, (list, tuple)):
                    rendered = "[" + ",".join(str(item) for item in value) + "]"
                elif isinstance(value, dict):
                    rendered = "{" + ",".join(f"{k}:{v}" for k, v in value.items()) + "}"
                else:
                    rendered = str(value)
                parts.append(f"{key}={rendered}")
            message = f"{message} | " + " | ".join(parts)
        self.log(message)

    def _bar_datetime(self, bar):
        for attr in ("datetime", "date", "trade_date"):
            value = getattr(bar, attr, None)
            if isinstance(value, datetime):
                return value
            if isinstance(value, date):
                return datetime.combine(value, time(15, 0))
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value[:19])
                except ValueError:
                    pass
        ts = getattr(bar, "timestamp", None)
        if ts is None:
            return None
        try:
            return self._timestamp_to_local_datetime(ts)
        except Exception:
            return None

    def _timestamp_to_local_datetime(self, value):
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        try:
            ts = int(value)
        except Exception:
            local = self.to_local_time(value)
            if isinstance(local, datetime):
                return local.replace(tzinfo=None)
            raise
        if ts > 10**15:
            seconds = ts / 1_000_000_000
        elif ts > 10**12:
            seconds = ts / 1000
        else:
            seconds = ts
        return datetime.utcfromtimestamp(seconds) + timedelta(hours=8)

    def _is_daily_bar(self, dt_value):
        return dt_value is not None and dt_value.time() == time(0, 0)

    def _using_intraday_bars(self):
        trade_date = self._current_trade_date()
        if trade_date is None:
            return False
        for bar in self._current_bars.values():
            dt_value = self._bar_datetime(bar)
            if dt_value is not None and dt_value.date() == trade_date and not self._is_daily_bar(dt_value):
                return True
        return False

    def _normalize_timer_time_set(self, values):
        result = set()
        if values is None:
            return result
        if isinstance(values, str):
            values = [item.strip() for item in values.split(",")]
        for value in values:
            text = str(value).strip()
            if not text:
                continue
            parts = text.split(":")
            if len(parts) >= 2:
                try:
                    result.add(f"{int(parts[0]):02d}:{int(parts[1]):02d}")
                except ValueError:
                    continue
        return result

    def _load_stock_meta(self):
        db_path = self._db_path()
        if db_path is None or not db_path.exists():
            return {}
        meta = {}
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT symbol, name, industry, industry2, industry3, sector, list_date,
                       is_st, is_delist, is_suspend, total_shares, total_mv, circ_mv,
                       security_type, product_class
                FROM stocks
                """
            )
            for row in rows:
                symbol = row["symbol"]
                if not symbol:
                    continue
                meta[symbol] = {
                    "name": row["name"] or "",
                    "industry": row["industry"] or row["industry2"] or row["industry3"] or "unknown",
                    "sector": row["sector"] or "",
                    "list_date": self._parse_date(row["list_date"]),
                    "is_st": bool(row["is_st"]),
                    "is_delist": bool(row["is_delist"]),
                    "is_suspend": bool(row["is_suspend"]),
                    "market_cap": float(row["circ_mv"] or row["total_mv"] or 0),
                    "total_shares": float(row["total_shares"] or 0),
                    "security_type": row["security_type"] or "stock",
                    "product_class": row["product_class"] or "",
                }
        return meta

    def _db_path(self):
        try:
            from app.core.config import settings

            return Path(settings.data_dir) / "gaoshou.db"
        except Exception:
            for path in (Path.cwd() / "data" / "gaoshou.db", Path.cwd() / "backend" / "data" / "gaoshou.db"):
                if path.exists():
                    return path
        return None

    @staticmethod
    def _parse_date(value):
        if value is None:
            return None
        if isinstance(value, date):
            return value
        text = str(value)
        for fmt, size in (("%Y-%m-%d", 10), ("%Y%m%d", 8)):
            try:
                return datetime.strptime(text[:size], fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _fill_nan(values):
        arr = np.asarray(values, dtype=float).copy()
        if len(arr) == 0:
            return arr
        finite = np.isfinite(arr)
        if not finite.any():
            return arr
        first = int(np.argmax(finite))
        arr[:first] = arr[first]
        for i in range(first + 1, len(arr)):
            if not np.isfinite(arr[i]):
                arr[i] = arr[i - 1]
        return arr

    @staticmethod
    def _sma(values, period):
        arr = np.asarray(values, dtype=float)
        out = np.full(len(arr), np.nan)
        for i in range(period - 1, len(arr)):
            window = arr[i - period + 1 : i + 1]
            finite = window[np.isfinite(window)]
            if len(finite):
                out[i] = float(np.mean(finite))
        return out

    @staticmethod
    def _talib_sma(values, period):
        return np.asarray(aq_talib.SMA(values, timeperiod=period, backend="python"), dtype=float)

    @staticmethod
    def _talib_ema(values, period):
        return np.asarray(aq_talib.EMA(values, timeperiod=period, backend="python"), dtype=float)

    @staticmethod
    def _talib_min(values, period):
        return np.asarray(aq_talib.MIN(values, timeperiod=period, backend="python"), dtype=float)

    @staticmethod
    def _talib_max(values, period):
        return np.asarray(aq_talib.MAX(values, timeperiod=period, backend="python"), dtype=float)

    @staticmethod
    def _ema(values, period):
        arr = np.asarray(values, dtype=float)
        out = np.full(len(arr), np.nan)
        alpha = 2.0 / (period + 1.0)
        prev = np.nan
        for i, value in enumerate(arr):
            if not np.isfinite(value):
                out[i] = prev
                continue
            prev = value if not np.isfinite(prev) else alpha * value + (1 - alpha) * prev
            out[i] = prev
        return out

    @staticmethod
    def _rolling_min(values, period):
        arr = np.asarray(values, dtype=float)
        out = np.full(len(arr), np.nan)
        for i in range(period - 1, len(arr)):
            out[i] = np.nanmin(arr[i - period + 1 : i + 1])
        return out

    @staticmethod
    def _rolling_max(values, period):
        arr = np.asarray(values, dtype=float)
        out = np.full(len(arr), np.nan)
        for i in range(period - 1, len(arr)):
            out[i] = np.nanmax(arr[i - period + 1 : i + 1])
        return out

    @staticmethod
    def _atr(high, low, close, period):
        true_ranges = []
        for i in range(1, len(close)):
            true_ranges.append(max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1])))
        if len(true_ranges) < period:
            return np.nan
        return float(np.nanmean(true_ranges[-period:]))

    @staticmethod
    def _safe_remove(items, value):
        try:
            items.remove(value)
        except ValueError:
            pass

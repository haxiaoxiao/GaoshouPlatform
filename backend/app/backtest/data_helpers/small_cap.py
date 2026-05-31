"""小市值策略数据助手 — 通过 MarketDataStore 访问行情数据"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.data_stores.factory import get_indicator_store


class SmallCapDataHelper:
    """小市值策略数据访问封装

    优先通过 MarketDataStore 读取，ClickHouse 作为高级功能（anyLast/argMax）的 fallback。
    """

    def __init__(self):
        self._store = get_market_data_store()
        self._indicator_store = get_indicator_store()
        self._ch = None

    @property
    def ch(self):
        if self._ch is None and (settings.clickhouse_enabled or settings.market_data_backend == "clickhouse"):
            from app.db.clickhouse import get_ch_client
            self._ch = get_ch_client()
        return self._ch

    # ── 交易日历 ──

    def is_trading_date(self, index_symbol: str, check_date: date) -> bool:
        dates = self._store.load_trading_dates([index_symbol],
                                                check_date - timedelta(days=10),
                                                check_date + timedelta(days=10))
        return check_date in dates

    def get_last_trading_date(self, index_symbol: str, before: date) -> date | None:
        dates = self._store.load_trading_dates([index_symbol],
                                                before - timedelta(days=60), before)
        return dates[-1] if dates else None

    # ── 日线数据 ──

    def load_daily_data(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        """加载日线数据，返回 index=trade_date 的 DataFrame"""
        return self._store.load_daily(symbols, start_date, end_date)

    def get_latest_field(self, symbol: str, field: str, as_of: date) -> float | None:
        """获取截至 as_of 日期的最新非空字段值"""
        df = self._store.load_daily([symbol], as_of - timedelta(days=120), as_of,
                                    columns=["symbol", "trade_date", field])
        if df.empty:
            return None
        vals = df[field].dropna()
        return float(vals.iloc[-1]) if len(vals) > 0 else None

    def get_field_on_date(self, symbol: str, field: str, on_date: date) -> float | None:
        """获取指定日期的字段值"""
        df = self._store.load_daily([symbol], on_date, on_date,
                                    columns=["symbol", "trade_date", field])
        if df.empty:
            return None
        val = df[field].iloc[0]
        return float(val) if not pd.isna(val) else None

    def get_bulk_latest_fields(
        self, symbols: list[str], field: str, as_of: date
    ) -> dict[str, float]:
        """批量获取多只股票截至 as_of 的最新字段值"""
        df = self._store.load_daily(symbols, as_of - timedelta(days=120), as_of,
                                    columns=["symbol", "trade_date", field])
        if df.empty:
            return {}
        result = {}
        for sym, grp in df.groupby("symbol"):
            vals = grp[field].dropna()
            if len(vals) > 0:
                result[sym] = float(vals.iloc[-1])
        return result

    def get_bulk_fields_on_date(
        self, symbols: list[str], field: str, on_date: date
    ) -> dict[str, float]:
        """批量获取多只股票在指定日期的字段值"""
        df = self._store.load_daily(symbols, on_date, on_date,
                                    columns=["symbol", "trade_date", field])
        if df.empty:
            return {}
        result = {}
        for sym, grp in df.groupby("symbol"):
            val = grp[field].iloc[0]
            if not pd.isna(val):
                result[sym] = float(val)
        return result

    def preload_daily_blocks(
        self, symbols: list[str], start_date: date
    ) -> dict[str, pd.DataFrame]:
        """批量预加载所有股票自 start_date 以来的日线数据"""
        df = self._store.load_daily(symbols, start_date, date.today())
        if df.empty:
            return {}
        return {sym: grp for sym, grp in df.groupby("symbol")}

    # ── 分钟成交量（ClickHouse 专有，Parquet 无完整分钟线时返回 0）─

    def get_intraday_volume(
        self, symbol: str, trade_date: date, up_to_hour: int = 10, up_to_min: int = 30
    ) -> float:
        """获取交易日开盘到指定时间之间的累积成交量"""
        if self.ch is None:
            return 0.0
        try:
            r = self.ch.execute(
                """
                SELECT sum(volume) FROM klines_minute
                WHERE symbol = %(s)s
                  AND toDate(datetime) = %(d)s
                  AND (toHour(datetime) < %(h)s
                       OR (toHour(datetime) = %(h)s AND toMinute(datetime) <= %(m)s))
                """,
                {"s": symbol, "d": trade_date, "h": up_to_hour, "m": up_to_min},
            )
            return float(r[0][0] or 0) if r and r[0] else 0.0
        except Exception:
            return 0.0

    # ── 指标 ──

    def get_latest_indicator(self, symbol: str, name: str, as_of: date) -> float | None:
        """获取截至 as_of 的最新指标值"""
        df = self._indicator_store.load_cross_section([name], as_of, [symbol])
        if df.empty:
            # Try recent dates
            for days_back in [1, 2, 3, 5, 10, 20]:
                td = as_of - timedelta(days=days_back)
                df = self._indicator_store.load_cross_section([name], td, [symbol])
                if not df.empty:
                    break
        if df.empty:
            return None
        val = df["value"].iloc[0]
        return float(val) if val is not None else None

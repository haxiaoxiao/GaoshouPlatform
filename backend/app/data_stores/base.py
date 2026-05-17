"""行情数据存储抽象基类"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Sequence

import pandas as pd


class MarketDataStore(ABC):
    """行情数据存储抽象 — 屏蔽底层 Parquet/ClickHouse 细节

    所有方法均为同步方法，由上层 DataProvider 通过 asyncio.to_thread 调用。
    """

    @abstractmethod
    def load_daily(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """加载日线数据

        Returns:
            DataFrame with index=trade_date(DatetimeIndex),
            columns: symbol, open, high, low, close, volume, amount [, turnover_rate]
        """
        ...

    @abstractmethod
    def load_minute(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        columns: Sequence[str] | None = None,
        timer_times: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        """加载分钟线数据

        Args:
            timer_times: 可选的时间点过滤列表，如 ["10:00", "10:30", "14:50"]
                None 表示不过滤，返回完整分钟线。

        Returns:
            DataFrame with index=datetime(DatetimeIndex),
            columns: symbol, datetime, open, high, low, close, volume, amount
        """
        ...

    def load_minute_volume_sum(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, float]:
        """返回指定区间内每只股票的分钟成交量合计。

        默认实现复用 load_minute；具体存储可以覆盖成数据库侧聚合，避免把分钟明细
        拉回 Python。
        """
        df = self.load_minute(symbols, start, end, columns=["symbol", "datetime", "volume"])
        if df.empty or "symbol" not in df.columns or "volume" not in df.columns:
            return {}
        grouped = df.groupby("symbol")["volume"].sum()
        return {str(symbol): float(volume or 0.0) for symbol, volume in grouped.items()}

    def load_minute_cum_volume(
        self,
        symbols: Sequence[str],
        as_of: datetime,
    ) -> dict[str, float]:
        """Return per-symbol cumulative minute volume from market open through as_of."""
        start = datetime.combine(as_of.date(), datetime.min.time())
        return self.load_minute_volume_sum(symbols, start, as_of + timedelta(microseconds=1))

    def load_latest_daily_values(
        self,
        symbols: Sequence[str],
        field: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, float]:
        """Return latest non-null daily field per symbol in [start_date, end_date]."""
        df = self.load_daily(symbols, start_date, end_date, columns=["symbol", "trade_date", field])
        if df.empty or field not in df.columns:
            return {}
        result: dict[str, float] = {}
        for symbol, group in df.groupby("symbol"):
            values = group.sort_index()[field].dropna()
            if len(values) and values.iloc[-1] > 0:
                result[str(symbol)] = float(values.iloc[-1])
        return result

    @abstractmethod
    def write_daily(self, df: pd.DataFrame) -> int:
        """写入日线数据，返回写入行数"""
        ...

    @abstractmethod
    def write_minute(self, df: pd.DataFrame, *, dataset: str = "klines_minute") -> int:
        """写入分钟线数据，返回写入行数"""
        ...

    @abstractmethod
    def coverage(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        *,
        dataset: str,
        timer_times: Sequence[str] | None = None,
    ) -> dict:
        """返回数据覆盖摘要

        Returns:
            dict with keys like: total_rows, symbols_covered, date_range, ...
        """
        ...

    @abstractmethod
    def load_trading_dates(
        self, symbols: Sequence[str], start_date: date, end_date: date
    ) -> list[date]:
        """获取有数据的交易日列表"""
        ...

    @abstractmethod
    def load_benchmark(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.Series:
        """获取基准指数的日收益率序列"""
        ...

    def top_by_avg_amount(
        self, start_date: date, end_date: date, limit: int
    ) -> list[str]:
        """按平均日成交额排序返回 Top-N 股票代码"""
        return []

    def has_data(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        bar_type: str = "daily",
    ) -> bool:
        """快速检查是否有数据。默认实现走 load_daily/load_minute，子类可重写优化。"""
        if not symbols:
            return False
        if bar_type in {"minute", "minute_timer"}:
            dt_start = datetime.combine(start_date, datetime.min.time())
            dt_end = datetime.combine(end_date, datetime.min.time())
            df = self.load_minute(symbols, dt_start, dt_end)
        else:
            df = self.load_daily(symbols, start_date, end_date)
        return not df.empty

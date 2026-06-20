"""指标数据存储抽象基类"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class IndicatorStore(ABC):
    """指标存储抽象 — 当前使用 Parquet/DuckDB"""

    @abstractmethod
    def load_cross_section(
        self,
        names: list[str],
        trade_date: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        """加载截面指标

        Returns:
            DataFrame with columns: symbol, indicator_name, trade_date, value
        """
        ...

    @abstractmethod
    def load_timeseries(
        self,
        names: list[str],
        start: date,
        end: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        """加载时序指标

        Returns:
            DataFrame with columns: symbol, indicator_name, value, updated_at
        """
        ...

    def latest_trade_date(
        self,
        names: list[str] | None = None,
        symbols: list[str] | None = None,
    ) -> date | None:
        """Return the latest cross-section trade date available in the store."""
        return None

    @abstractmethod
    def write_cross_section(self, df: pd.DataFrame) -> int:
        """写入截面指标，返回写入行数"""
        ...

    @abstractmethod
    def write_timeseries(self, df: pd.DataFrame) -> int:
        """写入时序指标，返回写入行数"""
        ...

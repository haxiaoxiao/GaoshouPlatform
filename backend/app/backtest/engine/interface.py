"""回测引擎抽象接口"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Callable, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from app.backtest.config import BacktestConfig, BacktestResult


class IDataProvider(ABC):
    """数据提供者抽象 — 屏蔽底层 ClickHouse/SQLite 细节"""

    @abstractmethod
    async def load_daily(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        """加载日线数据，返回 DataFrame

        Columns: trade_date(DatetimeIndex), symbol, open, high, low, close, volume, amount, turnover_rate
        """
        ...

    @abstractmethod
    async def load_minute(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        """加载分钟线数据，返回 DataFrame

        Columns: datetime(DatetimeIndex), symbol, open, high, low, close, volume, amount
        """
        ...

    @abstractmethod
    async def load_trading_dates(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> list[date]:
        """获取交易日列表"""
        ...

    @abstractmethod
    async def load_benchmark(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.Series:
        """获取基准指数的日收益率序列"""
        ...


class IBacktestEngine(ABC):
    """回测引擎抽象接口"""

    name: str = ""
    label: str = ""
    supported_modes: list[str] = []

    @abstractmethod
    async def run(
        self,
        config: "BacktestConfig",
        data_provider: IDataProvider,
        progress_callback: Callable[[float, dict | None], None] | None = None,
    ) -> "BacktestResult":
        """执行回测，返回统一的 BacktestResult"""
        ...

    def validate_config(self, config: "BacktestConfig") -> list[str]:
        """校验配置，返回错误列表。默认无错误。"""
        return []

    def to_desc(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "modes": self.supported_modes,
        }

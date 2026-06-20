"""MarketDataStore-backed data provider for backtests."""
from __future__ import annotations

import asyncio
from datetime import date, datetime

import pandas as pd

from app.backtest.engine.interface import IDataProvider
from app.data_stores import get_market_data_store
from app.data_stores.base import MarketDataStore


class StoreDataProvider(IDataProvider):
    """Load backtest data through the local MarketDataStore abstraction."""

    def __init__(self, store: MarketDataStore | None = None):
        self._store = store or get_market_data_store()

    async def load_daily(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        return await asyncio.to_thread(
            self._store.load_daily, symbols, start_date, end_date
        )

    async def load_minute(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        timer_times: tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        dt_start = datetime.combine(start_date, datetime.min.time())
        dt_end = datetime.combine(end_date, datetime.min.time()) + pd.Timedelta(days=1)
        return await asyncio.to_thread(
            self._store.load_minute,
            symbols, dt_start, dt_end, None, timer_times,
        )

    async def has_data(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        bar_type: str = "daily",
    ) -> bool:
        return await asyncio.to_thread(
            self._store.has_data, symbols, start_date, end_date, bar_type
        )

    async def load_trading_dates(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> list[date]:
        return await asyncio.to_thread(
            self._store.load_trading_dates, symbols, start_date, end_date
        )

    async def load_benchmark(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.Series:
        return await asyncio.to_thread(
            self._store.load_benchmark, symbol, start_date, end_date
        )

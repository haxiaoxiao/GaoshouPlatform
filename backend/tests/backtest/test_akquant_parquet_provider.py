"""StoreDataProvider + Parquet 后端 AKQuant 冒烟测试"""
from __future__ import annotations

import shutil
import tempfile
from datetime import date, datetime

import pandas as pd
import pytest

from app.backtest.engine.akquant.adapter import MarketDataStoreFeedAdapter
from app.backtest.engine.data_provider import StoreDataProvider
from app.data_stores.parquet_store import ParquetMarketDataStore


@pytest.fixture
def provider():
    """使用临时目录的 Parquet store 创建 StoreDataProvider"""
    tmp = tempfile.mkdtemp(prefix="test_akquant_parquet_")
    store = ParquetMarketDataStore(data_dir=tmp)
    provider = StoreDataProvider(store)

    # 写入测试数据: 2 只股票 × 2天 × 2个 timer 点
    symbols = ["000001.SZ", "600000.SH"]
    dates = [date(2025, 6, 2), date(2025, 6, 3)]  # Mon, Tue
    timer_times = [(10, 0), (14, 30)]  # 10:00, 14:30

    minute_rows = []
    daily_rows = []
    for sym in symbols:
        for dt in dates:
            daily_rows.append({
                "symbol": sym, "trade_date": dt,
                "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5,
                "volume": 1000000, "amount": 10500000.0,
            })
            for h, m in timer_times:
                minute_rows.append({
                    "symbol": sym,
                    "datetime": datetime(dt.year, dt.month, dt.day, h, m),
                    "open": 10.0, "high": 10.2, "low": 9.8, "close": 10.1,
                    "volume": 50000, "amount": 505000.0,
                })
    store.write_daily(pd.DataFrame(daily_rows))
    store.write_minute(pd.DataFrame(minute_rows), dataset="klines_minute_timer")

    yield provider
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.asyncio
class TestStoreDataProviderSmoke:
    async def test_load_daily_returns_dataframe(self, provider):
        df = await provider.load_daily(["000001.SZ"], date(2025, 6, 2), date(2025, 6, 3))
        assert not df.empty
        assert "close" in df.columns
        assert df.index.name == "trade_date"

    async def test_load_minute_returns_dataframe(self, provider):
        df = await provider.load_minute(
            ["000001.SZ"], date(2025, 6, 2), date(2025, 6, 3)
        )
        assert not df.empty
        assert "close" in df.columns
        assert df.index.name == "datetime"

    async def test_minute_timer_filters_requested_times(self, provider):
        """验证 timer_times 只加载指定时间点"""
        df = await provider.load_minute(
            ["000001.SZ"],
            date(2025, 6, 2),
            date(2025, 6, 3),
            timer_times=("10:00",),
        )
        assert not df.empty
        for dt in df.index:
            assert dt.hour == 10
            assert dt.minute == 0

    async def test_minute_timer_all_times_returns_all(self, provider):
        """验证 timer_times 含多个时间点"""
        df = await provider.load_minute(
            ["000001.SZ", "600000.SH"],
            date(2025, 6, 2),
            date(2025, 6, 3),
            timer_times=("10:00", "14:30"),
        )
        # 2 symbols × 2 days × 2 timer points = 8 rows
        assert len(df) == 8

    async def test_has_data_positive(self, provider):
        result = await provider.has_data(["000001.SZ"], date(2025, 6, 2), date(2025, 6, 3))
        assert result is True

    async def test_has_data_negative(self, provider):
        result = await provider.has_data(["UNKNOWN.SZ"], date(2025, 6, 2), date(2025, 6, 3))
        assert result is False

    async def test_load_trading_dates(self, provider):
        dates = await provider.load_trading_dates(
            ["000001.SZ"], date(2025, 6, 1), date(2025, 6, 5)
        )
        assert len(dates) == 2
        assert date(2025, 6, 2) in dates
        assert date(2025, 6, 3) in dates

    async def test_load_benchmark_returns_series(self, provider):
        s = await provider.load_benchmark("000001.SZ", date(2025, 6, 2), date(2025, 6, 3))
        assert isinstance(s, pd.Series)
        assert not s.empty
        assert s.name == "000001.SZ"

    async def test_akquant_adapter_bulk_uses_provider(self, provider):
        adapter = MarketDataStoreFeedAdapter(
            provider,
            ["000001.SZ", "600000.SH"],
            date(2025, 6, 2),
            date(2025, 6, 3),
            bar_type="minute_timer",
            timer_times=("10:00", "14:30"),
        )

        await adapter.preload()

        assert adapter.has_any_data is True

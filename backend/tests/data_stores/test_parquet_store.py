"""ParquetMarketDataStore 单元测试 — 无需 ClickHouse"""
from __future__ import annotations

import shutil
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from app.data_stores.parquet_store import ParquetMarketDataStore


@pytest.fixture
def store():
    """创建使用临时目录的 Parquet store"""
    tmp = tempfile.mkdtemp(prefix="test_parquet_")
    yield ParquetMarketDataStore(data_dir=tmp)
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def store_with_data(store: ParquetMarketDataStore):
    """写入日线和分钟线测试数据"""
    symbols = ["000001.SZ", "600000.SH"]
    dates = [date(2025, 6, 1) + timedelta(days=i) for i in range(5)]
    # 跳过周末 (6/1 是周日, 6/7 是周六)
    real_dates = [d for d in dates if d.weekday() < 5]
    if len(real_dates) < 2:
        real_dates = [date(2025, 6, 2), date(2025, 6, 3), date(2025, 6, 4)]

    # 日线
    daily_rows = []
    for sym in symbols:
        for dt in real_dates:
            daily_rows.append({
                "symbol": sym,
                "trade_date": dt,
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "volume": 1000000,
                "amount": 10500000.0,
            })
    df_daily = pd.DataFrame(daily_rows)
    store.write_daily(df_daily)

    # 分钟线 (简化: 每天只写 09:30 和 10:30)
    minute_rows = []
    for sym in symbols:
        for dt in real_dates[:2]:  # 只写前两天
            for h, m in [(9, 30), (10, 30)]:
                minute_rows.append({
                    "symbol": sym,
                    "datetime": datetime(dt.year, dt.month, dt.day, h, m),
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.8,
                    "close": 10.1,
                    "volume": 50000,
                    "amount": 505000.0,
                })
    df_minute = pd.DataFrame(minute_rows)
    store.write_minute(df_minute, dataset="klines_minute_timer")

    return {
        "store": store,
        "symbols": symbols,
        "dates": real_dates,
        "daily_df": df_daily,
        "minute_df": df_minute,
    }


# ------------------------------------------------------------------
# load_daily
# ------------------------------------------------------------------

class TestLoadDaily:
    def test_empty_store_returns_empty_df(self, store):
        df = store.load_daily(["000001.SZ"], date(2025, 1, 1), date(2025, 12, 31))
        assert df.empty

    def test_returns_data_for_requested_symbols(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        df = store.load_daily(symbols[:1], dates[0], dates[-1])
        assert not df.empty
        assert df.index.name == "trade_date"
        assert "open" in df.columns
        assert "close" in df.columns
        assert "amount" in df.columns

    def test_respects_date_range(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        # 只查一天
        df = store.load_daily(symbols, dates[0], dates[0])
        assert not df.empty
        unique_dates = df.index.unique()
        assert len(unique_dates) == 1

    def test_unknown_symbol_returns_empty(self, store_with_data):
        store = store_with_data["store"]
        dates = store_with_data["dates"]
        df = store.load_daily(["UNKNOWN.SZ"], dates[0], dates[-1])
        assert df.empty

    def test_columns_parameter(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        df = store.load_daily(symbols, dates[0], dates[-1], columns=["symbol", "trade_date", "close"])
        assert not df.empty
        assert "close" in df.columns
        assert "open" not in df.columns  # 只请求了 close

    def test_rewrite_same_key_keeps_latest_value(self, store):
        first = pd.DataFrame([{
            "symbol": "000001.SZ",
            "trade_date": date(2025, 6, 2),
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": 100,
            "amount": 1000.0,
        }])
        second = first.copy()
        second.loc[0, "close"] = 12.5
        second.loc[0, "amount"] = 2000.0

        store.write_daily(first)
        store.write_daily(second)

        df = store.load_daily(["000001.SZ"], date(2025, 6, 2), date(2025, 6, 2))
        assert len(df) == 1
        assert float(df.iloc[0]["close"]) == 12.5
        assert float(df.iloc[0]["amount"]) == 2000.0


# ------------------------------------------------------------------
# load_minute
# ------------------------------------------------------------------

class TestLoadMinute:
    def test_empty_store_returns_empty_df(self, store):
        df = store.load_minute(
            ["000001.SZ"],
            datetime(2025, 6, 1, 9, 30),
            datetime(2025, 6, 1, 15, 0),
        )
        assert df.empty

    def test_returns_minute_data(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        df = store.load_minute(
            symbols[:1],
            datetime(dates[0].year, dates[0].month, dates[0].day, 9, 0),
            datetime(dates[1].year, dates[1].month, dates[1].day, 15, 0),
        )
        assert not df.empty
        assert df.index.name == "datetime"
        assert "open" in df.columns

    def test_timer_times_filter(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        # 只请求 10:30 的数据
        df = store.load_minute(
            symbols,
            datetime(dates[0].year, dates[0].month, dates[0].day, 9, 0),
            datetime(dates[1].year, dates[1].month, dates[1].day, 15, 0),
            timer_times=["10:30"],
        )
        if not df.empty:
            # 所有返回的数据应该是 10:30
            for dt in df.index:
                assert dt.hour == 10 and dt.minute == 30


# ------------------------------------------------------------------
# has_data
# ------------------------------------------------------------------

class TestHasData:
    def test_empty_store_returns_false(self, store):
        assert store.has_data(["000001.SZ"], date(2025, 1, 1), date(2025, 12, 31)) is False

    def test_existing_data_returns_true(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]
        assert store.has_data(symbols, dates[0], dates[-1]) is True

    def test_unknown_symbol_returns_false(self, store_with_data):
        store = store_with_data["store"]
        dates = store_with_data["dates"]
        assert store.has_data(["UNKNOWN.SZ"], dates[0], dates[-1]) is False


# ------------------------------------------------------------------
# load_trading_dates
# ------------------------------------------------------------------

class TestLoadTradingDates:
    def test_empty_store_returns_empty_list(self, store):
        dates = store.load_trading_dates(["000001.SZ"], date(2025, 1, 1), date(2025, 12, 31))
        assert dates == []

    def test_returns_sorted_dates(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        result = store.load_trading_dates(symbols, dates[0], dates[-1])
        assert len(result) > 0
        assert result == sorted(result)


# ------------------------------------------------------------------
# load_benchmark
# ------------------------------------------------------------------

class TestLoadBenchmark:
    def test_empty_store_returns_empty_series(self, store):
        s = store.load_benchmark("000001.SZ", date(2025, 1, 1), date(2025, 12, 31))
        assert isinstance(s, pd.Series)
        assert s.empty

    def test_returns_returns_series(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        s = store.load_benchmark(symbols[0], dates[0], dates[-1])
        assert isinstance(s, pd.Series)
        assert not s.empty
        assert s.name == symbols[0]


# ------------------------------------------------------------------
# coverage
# ------------------------------------------------------------------

class TestCoverage:
    def test_empty_returns_zero(self, store):
        info = store.coverage(["000001.SZ"], date(2025, 1, 1), date(2025, 12, 31), dataset="klines_daily")
        assert info["total_rows"] == 0

    def test_returns_summary(self, store_with_data):
        store = store_with_data["store"]
        symbols = store_with_data["symbols"]
        dates = store_with_data["dates"]

        info = store.coverage(symbols, dates[0], dates[-1], dataset="klines_daily")
        assert info["total_rows"] > 0
        assert len(info["symbols_covered"]) > 0
        assert info["date_range"] is not None


# ------------------------------------------------------------------
# factory
# ------------------------------------------------------------------

class TestFactory:
    def test_parquet_default(self, monkeypatch):
        monkeypatch.setattr("app.core.config.settings.market_data_backend", "parquet")
        from app.data_stores.factory import get_market_data_store
        from app.data_stores.parquet_store import ParquetMarketDataStore

        s = get_market_data_store()
        assert isinstance(s, ParquetMarketDataStore)

    def test_clickhouse_backend(self, monkeypatch):
        monkeypatch.setattr("app.core.config.settings.market_data_backend", "clickhouse")
        from app.data_stores.factory import get_market_data_store
        from app.data_stores.clickhouse_store import ClickHouseMarketDataStore

        s = get_market_data_store()
        assert isinstance(s, ClickHouseMarketDataStore)

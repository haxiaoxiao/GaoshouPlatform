"""Minute Bar / BarEventSource 分钟数据单元测试"""
import pytest
from datetime import date, datetime

import pandas as pd

from app.backtest.event.event_source import Bar, BarEventSource
from app.backtest.event.calendar import TradingCalendar


class TestMinuteBar:
    def test_minute_bar_has_datetime(self):
        bar = Bar("TEST.SH", datetime(2024, 1, 2, 9, 35),
                  {"open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 5000, "amount": 51000})
        assert bar.trade_date == date(2024, 1, 2)
        assert bar.minute_time == datetime(2024, 1, 2, 9, 35)
        assert bar.datetime == datetime(2024, 1, 2, 9, 35)
        assert bar.close == 10.2

    def test_daily_bar_has_no_minute_time(self):
        bar = Bar("TEST.SH", date(2024, 1, 2),
                  {"open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 5000, "amount": 51000})
        assert bar.trade_date == date(2024, 1, 2)
        assert bar.minute_time is None
        assert isinstance(bar.datetime, datetime)


@pytest.fixture
def minute_data():
    """生成 2 天 x 2 只股票的模拟分钟数据"""
    symbols = ["000300.SH", "600000.SH"]
    data: dict[str, pd.DataFrame] = {}
    base = datetime(2024, 1, 2, 9, 30)
    for sym in symbols:
        rows = []
        for day_offset in [0, 1]:
            for minute in range(0, 240, 30):  # 每30分钟一根bar
                ts = base + pd.Timedelta(days=day_offset, minutes=minute)
                rows.append({
                    "open": 10 + day_offset * 0.5 + minute * 0.01,
                    "high": 10.2 + day_offset * 0.5 + minute * 0.01,
                    "low": 9.8 + day_offset * 0.5 + minute * 0.01,
                    "close": 10.1 + day_offset * 0.5 + minute * 0.01,
                    "volume": 100000 + minute * 1000,
                    "amount": 1_000_000 + minute * 10000,
                })
        df = pd.DataFrame(rows, index=pd.date_range(base, periods=len(rows), freq="30min"))
        data[sym] = df
    return data


@pytest.fixture
def daily_data():
    symbols = ["000300.SH", "600000.SH"]
    dates = pd.date_range("2024-01-02", "2024-01-05", freq="B")
    data: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        rows = []
        for i, dt in enumerate(dates):
            base_price = 10 + i * 0.5
            rows.append({
                "open": base_price,
                "high": base_price + 0.3,
                "low": base_price - 0.3,
                "close": base_price + 0.1,
                "volume": 1000000 + i * 100000,
                "amount": (base_price + 0.1) * (1000000 + i * 100000),
            })
        df = pd.DataFrame(rows, index=dates)
        data[sym] = df
    return data


class TestBarEventSourceMinute:
    def test_get_intraday_returns_minute_bars(self, daily_data, minute_data):
        calendar = TradingCalendar.from_list([
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5),
        ])
        source = BarEventSource.from_dataframe(daily_data, calendar, minute_data=minute_data)

        bars = source.get_intraday("000300.SH", date(2024, 1, 2))
        assert len(bars) == 16  # 2 days * 8 bars/day, all on same date_range index
        assert all(isinstance(b, Bar) for b in bars)
        assert all(b.minute_time is not None for b in bars)

    def test_get_intraday_empty_for_no_data(self, daily_data):
        calendar = TradingCalendar.from_list([date(2024, 1, 2)])
        source = BarEventSource.from_dataframe(daily_data, calendar)

        bars = source.get_intraday("000300.SH", date(2024, 1, 2))
        assert bars == []

    def test_get_intraday_history_returns_dataframe(self, daily_data, minute_data):
        calendar = TradingCalendar.from_list([
            date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4),
        ])
        source = BarEventSource.from_dataframe(daily_data, calendar, minute_data=minute_data)

        df = source.get_intraday_history("000300.SH", date(2024, 1, 3), n_days=5)
        assert not df.empty
        assert "close" in df.columns
        assert "volume" in df.columns

    def test_get_intraday_history_empty_for_no_minute_data(self, daily_data):
        calendar = TradingCalendar.from_list([date(2024, 1, 2)])
        source = BarEventSource.from_dataframe(daily_data, calendar)

        df = source.get_intraday_history("000300.SH", date(2024, 1, 2))
        assert df.empty

    def test_daily_get_bars_still_works(self, daily_data, minute_data):
        calendar = TradingCalendar.from_list([
            date(2024, 1, 2), date(2024, 1, 3),
        ])
        source = BarEventSource.from_dataframe(daily_data, calendar, minute_data=minute_data)

        bars = source.get_bars(date(2024, 1, 2))
        assert len(bars) == 2
        b = bars["000300.SH"]
        assert b.trade_date == date(2024, 1, 2)
        assert b.minute_time is None  # daily bars have no minute time

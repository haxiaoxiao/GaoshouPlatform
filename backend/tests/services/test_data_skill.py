from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from app.api import data_skill as data_skill_api
from app.services.data_skill import DataSkill


@pytest.mark.asyncio
async def test_data_skill_get_kline_minute_passes_timer_times(monkeypatch):
    class FakeMarketDataStore:
        def __init__(self):
            self.last_timer_times = None

        def load_minute(self, symbols, start, end, columns=None, timer_times=None):
            self.last_timer_times = list(timer_times) if timer_times else None
            df = pd.DataFrame(
                [
                    {
                        "symbol": symbols[0],
                        "datetime": datetime(2026, 5, 30, 10, 0),
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 100,
                        "amount": 1000.0,
                    }
                ]
            )
            df["datetime"] = pd.to_datetime(df["datetime"])
            return df.set_index("datetime").sort_index()

    store = FakeMarketDataStore()
    monkeypatch.setattr("app.services.data_skill.get_market_data_store", lambda: store)

    skill = DataSkill(session=None)  # session not used by get_kline_minute
    bars = await skill.get_kline_minute(
        "000001.SZ",
        start_date=date(2026, 5, 30),
        end_date=date(2026, 5, 30),
        limit=10,
        timer_times=["10:00"],
    )

    assert store.last_timer_times == ["10:00"]
    assert len(bars) == 1
    assert isinstance(bars[0].datetime, datetime)


@pytest.mark.asyncio
async def test_data_skill_get_kline_minute_default_window_is_bounded(monkeypatch):
    captured = {}

    class FakeMarketDataStore:
        def load_minute(self, symbols, start, end, columns=None, timer_times=None):
            captured["start"] = start
            captured["end"] = end
            df = pd.DataFrame(
                [
                    {
                        "symbol": symbols[0],
                        "datetime": datetime(2026, 5, 30, 10, 0),
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 100,
                        "amount": 1000.0,
                    }
                ]
            )
            df["datetime"] = pd.to_datetime(df["datetime"])
            return df.set_index("datetime").sort_index()

    monkeypatch.setattr("app.services.data_skill.get_market_data_store", lambda: FakeMarketDataStore())

    end = date(2026, 5, 30)
    skill = DataSkill(session=None)
    await skill.get_kline_minute("000001.SZ", start_date=None, end_date=end, limit=500)

    assert captured["end"].date() == end + timedelta(days=1)
    assert captured["start"].date() == end - timedelta(days=3)


@pytest.mark.asyncio
async def test_data_skill_get_kline_daily_default_window_is_bounded(monkeypatch):
    captured = {}

    class FakeMarketDataStore:
        def load_daily(self, symbols, start_date, end_date, columns=None):
            captured["start_date"] = start_date
            captured["end_date"] = end_date
            df = pd.DataFrame(
                [
                    {
                        "symbol": symbols[0],
                        "trade_date": date(2026, 5, 30),
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                        "volume": 100,
                        "amount": 1000.0,
                    }
                ]
            )
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            return df.set_index("trade_date").sort_index()

    monkeypatch.setattr("app.services.data_skill.get_market_data_store", lambda: FakeMarketDataStore())

    end = date(2026, 5, 30)
    skill = DataSkill(session=None)
    await skill.get_kline_daily("000001.SZ", start_date=None, end_date=end, limit=10)

    assert captured["end_date"] == end
    assert captured["start_date"] == end - timedelta(days=30)


@pytest.mark.asyncio
async def test_data_skill_indicator_route_awaits(monkeypatch):
    async def fake_get_indicator(self, symbol: str, indicator_name: str, trade_date=None):
        return 12.34

    monkeypatch.setattr(DataSkill, "get_indicator", fake_get_indicator)

    resp = await data_skill_api.get_indicator(
        symbol="000001.SZ",
        name="pe_ttm",
        trade_date=None,
        session=None,
    )

    assert resp["code"] == 0
    assert resp["data"]["value"] == 12.34


@pytest.mark.asyncio
async def test_data_skill_indicator_batch_route_awaits(monkeypatch):
    async def fake_get_indicators_batch(self, symbols, trade_date=None, names=None):
        return [
            {
                "symbol": "000001.SZ",
                "indicator_name": "pe_ttm",
                "value": 1.0,
                "trade_date": date(2026, 5, 30),
            }
        ]

    monkeypatch.setattr(DataSkill, "get_indicators_batch", fake_get_indicators_batch)

    resp = await data_skill_api.get_indicators_batch(
        symbols=["000001.SZ"],
        names=["pe_ttm"],
        trade_date=date(2026, 5, 30),
        session=None,
    )

    assert resp["code"] == 0
    assert resp["data"][0]["indicator_name"] == "pe_ttm"


@pytest.mark.asyncio
async def test_data_skill_indicator_timeseries_route_awaits(monkeypatch):
    async def fake_get_indicator_timeseries(self, symbol, names, start_date, end_date, limit=5000):
        return [{"symbol": symbol, "indicator_name": names[0], "datetime": datetime(2026, 5, 30, 0, 0), "value": 1.0}]

    monkeypatch.setattr(DataSkill, "get_indicator_timeseries", fake_get_indicator_timeseries)

    resp = await data_skill_api.get_indicator_timeseries(
        symbol="000001.SZ",
        names=["pe_ttm"],
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 30),
        limit=10,
        session=None,
    )

    assert resp["code"] == 0
    assert resp["data"][0]["symbol"] == "000001.SZ"


@pytest.mark.asyncio
async def test_data_skill_indicator_timeseries_batch_route_awaits(monkeypatch):
    async def fake_get_indicators_timeseries_batch(self, symbols, names, start_date, end_date, limit=200000):
        return [{"symbol": symbols[0], "indicator_name": names[0], "datetime": datetime(2026, 5, 30, 0, 0), "value": 1.0}]

    monkeypatch.setattr(DataSkill, "get_indicators_timeseries_batch", fake_get_indicators_timeseries_batch)

    resp = await data_skill_api.get_indicators_timeseries_batch(
        symbols=["000001.SZ"],
        names=["pe_ttm"],
        start_date=date(2026, 5, 1),
        end_date=date(2026, 5, 30),
        limit=10,
        session=None,
    )

    assert resp["code"] == 0
    assert resp["data"][0]["indicator_name"] == "pe_ttm"

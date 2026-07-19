from __future__ import annotations

import asyncio
import math
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from fastapi.encoders import jsonable_encoder
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.services.market_radar_data as radar_data
from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.models.sentiment import SentimentAnalysis, SentimentPost
from app.db.models.stock import Stock
from app.services.market_radar_data import MarketRadarDataService

NOW = datetime(2026, 7, 20, 15, 30)


class StaticCalendar:
    def __init__(self, dates: tuple[date, ...]) -> None:
        self.dates = dates

    def get_trading_days(self, start_date: date, end_date: date) -> tuple[date, ...]:
        return tuple(item for item in self.dates if start_date <= item <= end_date)


def _write_dataset(root: Path, dataset: str, records: list[dict]) -> None:
    frame = pd.DataFrame.from_records(records)
    assert not frame.empty
    date_value = pd.Timestamp(frame.iloc[0].get("trade_date_dt", frame.iloc[0].get("trade_date")))
    target = root / dataset / f"year={date_value.year}" / f"month={date_value.month:02d}"
    target.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(target / "part-0.parquet", index=False)


async def _database(tmp_path: Path, stocks: list[Stock]):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'radar-data.db').as_posix()}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Stock.__table__.create)
        await connection.run_sync(SentimentPost.__table__.create)
        await connection.run_sync(SentimentAnalysis.__table__.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        session.add_all(stocks)
        await session.commit()
    return engine, sessions


def _stock(
    symbol: str,
    *,
    list_date: date = date(2020, 1, 1),
    name: str | None = None,
    industry: str | None = "电子",
    is_st: int = 0,
    security_type: str | None = "stock",
    product_class: str | None = "stock",
    delist_date: date | None = None,
    is_delist: int = 0,
) -> Stock:
    return Stock(
        symbol=symbol,
        name=name or symbol,
        exchange=symbol.rsplit(".", 1)[-1],
        list_date=list_date,
        is_st=is_st,
        delist_date=delist_date,
        is_delist=is_delist,
        is_suspend=0,
        industry=industry,
        security_type=security_type,
        product_class=product_class,
    )


def _write_corrupt_dataset(root: Path, dataset: str, records: list[dict]) -> None:
    target = root / dataset / "year=2026" / "month=07"
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame.from_records(records).to_parquet(target / "part-0.parquet", index=False)


def _bar(
    symbol: str,
    trade_date: date,
    close: float,
    *,
    amount: float = 100.0,
    volume: float = 10.0,
) -> dict:
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "amount": amount,
    }


def _weekdays(start: date, count: int) -> tuple[date, ...]:
    result: list[date] = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return tuple(result)


@pytest.mark.asyncio
async def test_daily_market_uses_exact_previous_market_date_and_calculator_bins(tmp_path):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    returns = (-9.0, -7.0, -5.0, -3.0, -1.0, 0.0, 1.0, 3.0, 5.0, 7.0, 9.0)
    symbols = [f"6000{i:02d}.SH" for i in range(9)] + ["000001.SZ", "920001.BJ"]
    stocks = [_stock(symbol, is_st=1 if index == 1 else 0) for index, symbol in enumerate(symbols)]
    stocks.extend(
        [
            _stock("000001.SH", security_type="index", product_class="index"),
            _stock("000985.SH", security_type="index", product_class="index"),
        ]
    )
    stocks.extend(
        [
            _stock("510300.SH", security_type=None, product_class=None),
            _stock("399001.SZ", security_type=None, product_class=None),
        ]
    )
    records: list[dict] = []
    for symbol, return_pct in zip(symbols, returns, strict=True):
        records.extend(
            [
                _bar(symbol, previous, 100.0),
                _bar(symbol, target, 100.0 + return_pct, amount=100.0 + return_pct),
            ]
        )
    records.append(_bar(symbols[0], target, 91.0, amount=91.0))
    for symbol, previous_close, current_close in (
        ("000001.SH", 3_000.0, 3_030.0),
        ("399001.SZ", 10_000.0, 9_900.0),
        ("000985.SH", 5_000.0, 5_100.0),
    ):
        records.extend(
            [
                _bar(symbol, previous, previous_close),
                _bar(symbol, target, current_close),
            ]
        )
    _write_dataset(tmp_path, "klines_daily", records)
    engine, sessions = await _database(tmp_path, stocks)
    try:
        async with sessions() as session:
            service = MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((previous, target)),
                now=lambda: NOW,
            )
            result = await service.load_daily_market(target_date=target)

        current = result.slices[-1]
        assert result.status == "fresh"
        assert len(result.universe) == len(symbols)
        assert current.breadth.flat_count == 1
        assert [bucket.count for bucket in current.breadth.buckets.values()] == [1, 1, 1, 1, 1, 2, 1, 1, 1, 1]
        assert current.breadth.coverage.valid == len(symbols)
        assert current.breakdowns[0].key == "all"
        assert current.breakdowns[0].valid == len(symbols)
        assert {item.key for item in current.breakdowns} >= {"all", "SH", "BJ", "ST"}
        assert all(item.exclusion_reason is None for item in current.facts)
        assert result.source_freshness.source_date == target
        assert result.source_freshness.expected_date == target
        index_returns = {item.symbol: item for item in current.indices}
        assert set(index_returns) == {"000001.SH", "399001.SZ", "000985.SH"}
        assert index_returns["000001.SH"].return_pct == pytest.approx(1.0)
        assert index_returns["399001.SZ"].return_pct == pytest.approx(-1.0)
        assert index_returns["000985.SH"].return_pct == pytest.approx(2.0)
        assert all(item.status == "fresh" for item in index_returns.values())
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_daily_market_cpu_pipeline_keeps_event_loop_responsive(tmp_path, monkeypatch):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    _write_dataset(
        tmp_path,
        "klines_daily",
        [
            _bar("600001.SH", previous, 10.0),
            _bar("600001.SH", target, 10.5),
        ],
    )
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    original = radar_data.calculate_breadth

    def slow_breadth(*args, **kwargs):
        time.sleep(0.2)
        return original(*args, **kwargs)

    monkeypatch.setattr(radar_data, "calculate_breadth", slow_breadth)
    beats: list[float] = []
    stopped = False

    async def heartbeat() -> None:
        while not stopped:
            beats.append(time.perf_counter())
            await asyncio.sleep(0.01)

    monitor = asyncio.create_task(heartbeat())
    try:
        await asyncio.sleep(0.02)
        async with sessions() as session:
            await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((previous, target)),
                now=lambda: NOW,
            ).load_daily_market(target_date=target)
        await asyncio.sleep(0.02)
    finally:
        stopped = True
        await monitor
        await engine.dispose()

    gaps = [right - left for left, right in zip(beats, beats[1:], strict=False)]
    assert gaps and max(gaps) < 0.12


@pytest.mark.asyncio
async def test_daily_market_freshness_ignores_newer_core_index_rows(tmp_path):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    records = [
        _bar("600001.SH", previous, 10.0),
        _bar("000001.SH", previous, 3_000.0),
        _bar("000001.SH", target, 3_030.0),
    ]
    _write_dataset(tmp_path, "klines_daily", records)
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((previous, target)),
                now=lambda: NOW,
            ).load_daily_market(target_date=target)

        assert result.status == "stale"
        assert result.source_freshness.status == "stale"
        assert result.source_freshness.source_date == previous
        assert result.source_freshness.row_count == 1
        index = {item.symbol: item for item in result.slices[-1].indices}["000001.SH"]
        assert index.status == "fresh"
        assert index.return_pct == pytest.approx(1.0)
    finally:
        await engine.dispose()


def test_daily_index_returns_keep_missing_invalid_and_conflict_gaps():
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    rows = {
        ("000001.SH", previous): _bar("000001.SH", previous, 3_000.0),
        ("399001.SZ", target): _bar("399001.SZ", target, 10_000.0),
        ("000985.SH", previous): _bar("000985.SH", previous, 0.0),
        ("000985.SH", target): _bar("000985.SH", target, 5_000.0),
    }

    values = {
        item.symbol: item
        for item in MarketRadarDataService._daily_index_returns(
            target,
            previous,
            rows,
            set(),
        )
    }

    assert values["000001.SH"].reason == "missing_current"
    assert values["399001.SZ"].reason == "missing_previous"
    assert values["000985.SH"].reason == "invalid_price"
    assert all(item.return_pct is None for item in values.values())
    conflicted = MarketRadarDataService._daily_index_returns(
        target,
        previous,
        rows,
        {("000001.SH", target)},
    )
    assert next(item for item in conflicted if item.symbol == "000001.SH").reason == (
        "duplicate_conflict"
    )


@pytest.mark.asyncio
async def test_daily_market_reports_exclusions_conflicts_and_incomplete_universe(tmp_path):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    stocks = [
        _stock("600001.SH"),
        _stock("600002.SH", list_date=target),
        _stock("000001.SZ"),
        _stock("000002.SZ"),
        _stock("600003.SH"),
        _stock("600004.SH"),
    ]
    records = [
        _bar("600001.SH", previous, 10),
        _bar("600001.SH", target, 11),
        _bar("600002.SH", target, 12),
        _bar("000001.SZ", previous, 10),
        _bar("000001.SZ", target, 10, volume=0),
        _bar("000002.SZ", previous, 10),
        _bar("600003.SH", previous, 10),
        _bar("600003.SH", target, 11),
        _bar("600003.SH", target, 12),
        _bar("600004.SH", target, 11),
    ]
    _write_dataset(tmp_path, "klines_daily", records)
    engine, sessions = await _database(tmp_path, stocks)
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((previous, target)),
                now=lambda: NOW,
            ).load_daily_market(target_date=target)

        facts = {item.symbol: item for item in result.slices[-1].facts}
        assert facts["600002.SH"].exclusion_reason == "first_listing"
        assert facts["000001.SZ"].exclusion_reason == "zero_volume"
        assert facts["000002.SZ"].exclusion_reason == "missing_current"
        assert facts["600003.SH"].exclusion_reason == "duplicate_conflict"
        assert facts["600004.SH"].exclusion_reason == "missing_previous"
        assert result.slices[-1].breadth.coverage.valid == 1
        assert result.status == "partial"
        assert "BJ" in (result.universe_freshness.reason or "")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_observed_calendar_is_explicitly_partial_and_stale_daily_keeps_real_date(tmp_path):
    source_date = date(2026, 7, 17)
    target = date(2026, 7, 20)
    _write_dataset(tmp_path, "klines_daily", [_bar("600001.SH", source_date, 10)])
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                now=lambda: NOW,
            ).load_daily_market(target_date=target)

        assert result.calendar.freshness.status == "partial"
        assert "authoritative" in (result.calendar.freshness.reason or "")
        assert result.source_freshness.status == "stale"
        assert result.source_freshness.source_date == source_date
        assert result.source_freshness.expected_date == target
        assert result.status == "stale"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_implicit_calendar_uses_authoritative_last_trading_day_on_weekend(tmp_path):
    friday = date(2026, 7, 17)
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            calendar = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((date(2026, 7, 16), friday)),
                now=lambda: datetime(2026, 7, 19, 12),
            ).resolve_calendar()

        assert calendar.expected_date == friday
        assert calendar.freshness.status == "fresh"
        assert calendar.freshness.expected_date == friday
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_no_provider_keeps_today_expected_and_exposes_observed_lag(tmp_path):
    friday = date(2026, 7, 17)
    monday = date(2026, 7, 20)
    _write_dataset(tmp_path, "klines_daily", [_bar("600001.SH", friday, 10)])
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            service = MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                now=lambda: datetime(2026, 7, 20, 12),
            )
            calendar = await service.resolve_calendar()
            daily = await service.load_daily_market()

        assert calendar.expected_date == monday
        assert calendar.freshness.status == "partial"
        assert calendar.freshness.source_date == friday
        assert daily.expected_date == monday
        assert daily.source_freshness.status == "stale"
        assert daily.slices[-1].trade_date == friday
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_explicit_weekend_preserves_expected_but_slices_only_trading_dates(tmp_path):
    dates = (date(2026, 7, 15), date(2026, 7, 16), date(2026, 7, 17))
    weekend = date(2026, 7, 19)
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    engine, sessions = await _database(tmp_path, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_daily_market(target_date=weekend, days=2)

        assert result.expected_date == weekend
        assert [item.trade_date for item in result.slices] == list(dates[-2:])
        assert weekend not in {item.trade_date for item in result.slices}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_fifteen_day_history_has_only_real_trading_slices(tmp_path):
    dates = _weekdays(date(2026, 6, 22), 16)
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    engine, sessions = await _database(tmp_path, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_daily_market(target_date=dates[-1], days=15)

        assert [item.trade_date for item in result.slices] == list(dates[-15:])
        assert all(item.trade_date.weekday() < 5 for item in result.slices)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_daily_universe_is_point_in_time_for_listings_and_delistings(tmp_path):
    dates = tuple(date(2026, 7, 14) + timedelta(days=offset) for offset in range(4))
    old = _stock("600001.SH", delist_date=dates[-1], is_delist=1)
    new = _stock("000001.SZ", list_date=dates[2])
    always = _stock("920001.BJ")
    dirty_delisted = _stock("600009.SH", is_delist=1, delist_date=None)
    records = _history_records([old.symbol, always.symbol], dates[:3])
    records.extend(_history_records([new.symbol], dates[2:]))
    records.extend(_history_records([always.symbol], dates[3:]))
    records.extend(_history_records([dirty_delisted.symbol], dates))
    _write_dataset(tmp_path, "klines_daily", records)
    engine, sessions = await _database(tmp_path, [old, new, always, dirty_delisted])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_daily_market(target_date=dates[-1], days=3)

        facts = {item.trade_date: {fact.symbol for fact in item.facts} for item in result.slices}
        assert old.symbol in facts[dates[1]]
        assert new.symbol not in facts[dates[1]]
        assert old.symbol not in facts[dates[-1]]
        assert new.symbol in facts[dates[-1]]
        assert dirty_delisted.symbol not in set().union(*facts.values())
        assert result.universe_freshness.row_count == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_limit_ladder_prefers_exact_official_step_and_keeps_step_only_st(tmp_path):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    _write_dataset(
        tmp_path,
        "tushare_limit_step",
        [
            {"symbol": "600001.SH", "name": "甲", "trade_date_dt": previous, "nums": "1"},
            {"symbol": "600004.SH", "name": "戊", "trade_date_dt": previous, "nums": "2"},
            {"symbol": "600001.SH", "name": "甲", "trade_date_dt": target, "nums": "2"},
            {"symbol": "000001.SZ", "name": "ST乙", "trade_date_dt": target, "nums": "3"},
        ],
    )
    _write_dataset(
        tmp_path,
        "tushare_limit_list_d",
        [
            {"symbol": "600001.SH", "name": "甲", "industry": "电子", "trade_date_dt": target, "limit": "U", "limit_times": 2, "pct_chg": 10.01, "turnover_ratio": 8.5, "amount": 100.0, "fd_amount": 10.0, "first_time": "093100", "last_time": "145500", "open_times": 1},
            {"symbol": "600002.SH", "name": "丙", "industry": "电子", "trade_date_dt": target, "limit": "D", "limit_times": None, "amount": 80.0, "fd_amount": None, "first_time": None, "last_time": None, "open_times": 0},
            {"symbol": "600003.SH", "name": "丁", "industry": "机械", "trade_date_dt": target, "limit": "Z", "limit_times": None, "amount": 70.0, "fd_amount": None, "first_time": "100000", "last_time": None, "open_times": 2},
        ],
    )
    stocks = [_stock("600001.SH"), _stock("000001.SZ", is_st=1), _stock("600002.SH"), _stock("600003.SH"), _stock("600004.SH")]
    engine, sessions = await _database(tmp_path, stocks)
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((previous, target)),
                now=lambda: NOW,
            ).load_limit_ladder(target_date=target)

        rows = {row.symbol: row for row in result.rows}
        assert result.source_mode == "official"
        assert rows["000001.SZ"].board_count == 3
        assert rows["000001.SZ"].is_st is True
        assert rows["600001.SH"].pct_change == pytest.approx(10.01)
        assert rows["600001.SH"].turnover_ratio == pytest.approx(8.5)
        assert rows["600001.SH"].limit_times == 2
        assert result.up_count == 1
        assert result.down_count == 1
        assert result.broken_count == 1
        assert result.broken_rate == pytest.approx(0.5)
        assert result.highest_board == 3
        assert result.promotion_rate == pytest.approx(0.5)
        assert result.status == "fresh"
        encoded = radar_data.serialize_market_radar_data(result)
        assert jsonable_encoder(encoded) == encoded
        encoded_row = next(item for item in encoded["rows"] if item["symbol"] == "600001.SH")
        assert encoded_row["pct_change"] == pytest.approx(10.01)
        assert encoded_row["turnover_ratio"] == pytest.approx(8.5)
        assert encoded_row["limit_times"] == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_limit_ladder_ignores_stale_step_and_derives_over_weekend(tmp_path):
    thursday = date(2026, 7, 16)
    friday = date(2026, 7, 17)
    monday = date(2026, 7, 20)
    _write_dataset(
        tmp_path,
        "tushare_limit_step",
        [{"symbol": "600001.SH", "name": "甲", "trade_date_dt": thursday, "nums": "8"}],
    )
    _write_dataset(
        tmp_path,
        "tushare_limit_list_d",
        [
            {"symbol": "600001.SH", "name": "甲", "industry": "电子", "trade_date_dt": friday, "limit": "U", "limit_times": None, "amount": 90.0},
            {"symbol": "600001.SH", "name": "甲", "industry": "电子", "trade_date_dt": monday, "limit": "U", "limit_times": None, "amount": 100.0},
        ],
    )
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((thursday, friday, monday)),
                now=lambda: NOW,
            ).load_limit_ladder(target_date=monday)

        assert result.source_mode == "derived"
        assert result.rows[0].board_count == 2
        assert result.rows[0].board_count != 8
        assert result.step_freshness.status == "stale"
        assert result.detail_freshness.status == "fresh"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_limit_ladder_keeps_exact_step_when_detail_is_unavailable(tmp_path):
    target = date(2026, 7, 20)
    _write_dataset(
        tmp_path,
        "tushare_limit_step",
        [{"symbol": "000001.SZ", "name": "ST乙", "trade_date_dt": target, "nums": "3"}],
    )
    engine, sessions = await _database(tmp_path, [_stock("000001.SZ", is_st=1)])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((target,)),
                now=lambda: NOW,
            ).load_limit_ladder(target_date=target)

        assert result.status == "partial"
        assert result.source_mode == "official"
        assert result.rows[0].board_count == 3
        assert result.up_count is None
        assert result.down_count is None
        assert result.broken_count is None
        assert result.detail_freshness.status == "unavailable"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_limit_ladder_stale_sources_return_no_scoring_rows_and_real_dates(tmp_path):
    source_date = date(2026, 4, 17)
    target = date(2026, 7, 20)
    _write_dataset(tmp_path, "tushare_limit_step", [{"symbol": "600001.SH", "name": "甲", "trade_date_dt": source_date, "nums": "2"}])
    _write_dataset(tmp_path, "tushare_limit_list_d", [{"symbol": "600001.SH", "name": "甲", "trade_date_dt": source_date, "limit": "U", "limit_times": 2}])
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar((source_date, target)),
                now=lambda: NOW,
            ).load_limit_ladder(target_date=target)

        assert result.status == "stale"
        assert result.rows == ()
        assert result.detail_freshness.source_date == source_date
        assert result.step_freshness.source_date == source_date
    finally:
        await engine.dispose()


def _history_records(symbols: list[str], dates: tuple[date, ...]) -> list[dict]:
    records: list[dict] = []
    for day_index, trade_day in enumerate(dates):
        for symbol_index, symbol in enumerate(symbols):
            amount = float((symbol_index + 1) * 10 * (1 + day_index / 100))
            close = float(10 + symbol_index + day_index * (0.1 + symbol_index * 0.01))
            records.append(_bar(symbol, trade_day, close, amount=amount))
    return records


@pytest.mark.asyncio
async def test_crowding_inputs_are_fixed_safe_and_exclude_future_history(tmp_path):
    dates = tuple(date(2026, 6, 22) + timedelta(days=offset) for offset in range(29))
    target = dates[-2]
    future = dates[-1]
    symbols = ["600001.SH", "600002.SH", "000001.SZ", "000002.SZ"]
    stocks = [
        _stock(symbols[0], industry="电子"),
        _stock(symbols[1], industry="电子"),
        _stock(symbols[2], industry="机械"),
        _stock(symbols[3], industry="医药"),
    ]
    records = _history_records(symbols, dates)
    for record in records:
        if record["trade_date"] == future:
            record["amount"] = 10_000_000.0
    _write_dataset(tmp_path, "klines_daily", records)
    margin_records: list[dict] = []
    for index, trade_day in enumerate(dates[-8:-1]):
        for exchange in ("SSE", "SZSE", "BSE"):
            margin_records.append({"trade_date_dt": trade_day, "exchange_id": exchange, "rzye": 1000.0 + index * 10})
    _write_dataset(tmp_path, "tushare_margin", margin_records)
    engine, sessions = await _database(tmp_path, stocks)
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_crowding_inputs(target_date=target)

        components = {item.key: item for item in result.components}
        assert set(components) == {
            "top_1_amount_share",
            "top_5_amount_share",
            "top_3_sector_share",
            "market_amount_vs_20d",
            "high_liquidity_correlation",
            "margin_balance_5d_change",
        }
        assert components["top_1_amount_share"].current_value == pytest.approx(0.4)
        assert components["top_5_amount_share"].current_value == pytest.approx(0.4)
        assert components["top_3_sector_share"].current_value == pytest.approx(1.0)
        assert components["market_amount_vs_20d"].current_value is not None
        assert components["market_amount_vs_20d"].current_value < 2
        assert components["high_liquidity_correlation"].current_value is not None
        assert components["margin_balance_5d_change"].current_value == pytest.approx(1060 / 1010 - 1)
        assert all(len(item.history) <= 120 for item in result.components)
        assert result.as_of == target
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_correlation_requires_full_twenty_day_window(tmp_path):
    dates = _weekdays(date(2026, 6, 15), 21)
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    engine, sessions = await _database(tmp_path, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            service = MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            )
            fifteen = await service.load_crowding_inputs(target_date=dates[15])
            twenty = await service.load_crowding_inputs(target_date=dates[-1])

        fifteen_corr = next(
            item for item in fifteen.components if item.key == "high_liquidity_correlation"
        )
        twenty_corr = next(
            item for item in twenty.components if item.key == "high_liquidity_correlation"
        )
        assert fifteen_corr.current_value is None
        assert twenty_corr.current_value is not None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_correlation_requires_eighty_percent_finite_pair_coverage(tmp_path):
    dates = _weekdays(date(2026, 6, 15), 21)

    def records(symbols: list[str], *, constant_last: bool) -> list[dict]:
        result: list[dict] = []
        for index, trade_day in enumerate(dates):
            closes = [10 + index + index**2 * 0.01, 20 + index * 1.5 + index**2 * 0.03]
            if len(symbols) == 3:
                closes.append(30.0 if constant_last else 30 + index)
            result.extend(
                _bar(symbol, trade_day, close, amount=100.0 + symbol_index * 10)
                for symbol_index, (symbol, close) in enumerate(zip(symbols, closes, strict=True))
            )
        return result

    three_root = tmp_path / "three"
    three_symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(three_root, "klines_daily", records(three_symbols, constant_last=True))
    engine, sessions = await _database(three_root, [_stock(symbol) for symbol in three_symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(three_root)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_crowding_inputs(target_date=dates[-1])
        correlation = next(
            item for item in result.components if item.key == "high_liquidity_correlation"
        )
        assert correlation.current_value is None
        assert correlation.excluded_reason == "insufficient_pair_coverage"
    finally:
        await engine.dispose()

    two_root = tmp_path / "two"
    two_symbols = ["600001.SH", "000001.SZ"]
    _write_dataset(two_root, "klines_daily", records(two_symbols, constant_last=False))
    engine, sessions = await _database(two_root, [_stock(symbol) for symbol in two_symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(two_root)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_crowding_inputs(target_date=dates[-1])
        correlation = next(
            item for item in result.components if item.key == "high_liquidity_correlation"
        )
        assert correlation.current_value is not None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_margin_component_requires_all_exchanges_and_lag_at_most_two_days(tmp_path):
    dates = tuple(date(2026, 6, 22) + timedelta(days=offset) for offset in range(25))
    target = dates[-1]
    symbols = ["600001.SH", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    _write_dataset(
        tmp_path,
        "tushare_margin",
        [
            {"trade_date_dt": dates[-3], "exchange_id": "SSE", "rzye": 100.0},
            {"trade_date_dt": dates[-3], "exchange_id": "SZSE", "rzye": 100.0},
        ],
    )
    engine, sessions = await _database(tmp_path, [_stock(symbols[0]), _stock(symbols[1])])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_crowding_inputs(target_date=target)
        margin = next(item for item in result.components if item.key == "margin_balance_5d_change")
        assert margin.current_value is None
        assert margin.excluded_reason == "incomplete_exchange_coverage"
        assert margin.freshness.status == "partial"
        assert "BSE" in (margin.freshness.reason or "")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_margin_change_requires_the_exact_fifth_prior_trading_day(tmp_path):
    dates = tuple(date(2026, 6, 22) + timedelta(days=offset) for offset in range(25))
    target = dates[-1]
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    margin_records = [
        {"trade_date_dt": trade_day, "exchange_id": exchange, "rzye": 100.0 + index}
        for index, trade_day in enumerate(dates[-8:])
        if trade_day != dates[-6]
        for exchange in ("SSE", "SZSE", "BSE")
    ]
    _write_dataset(tmp_path, "tushare_margin", margin_records)
    engine, sessions = await _database(tmp_path, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_crowding_inputs(target_date=target)

        margin = next(item for item in result.components if item.key == "margin_balance_5d_change")
        assert margin.current_value is None
        assert margin.excluded_reason == "missing_fifth_prior_trading_day"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_margin_uses_latest_complete_exchange_date_not_later_partial_date(tmp_path):
    dates = tuple(date(2026, 6, 22) + timedelta(days=offset) for offset in range(25))
    target = dates[-1]
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    records = [
        {"trade_date_dt": trade_day, "exchange_id": exchange, "rzye": 100.0 + index}
        for index, trade_day in enumerate(dates[-8:-1])
        for exchange in ("SSE", "SZSE", "BSE")
    ]
    records.append({"trade_date_dt": target, "exchange_id": "SSE", "rzye": 999.0})
    _write_dataset(tmp_path, "tushare_margin", records)
    engine, sessions = await _database(tmp_path, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_crowding_inputs(target_date=target)

        margin = next(item for item in result.components if item.key == "margin_balance_5d_change")
        assert margin.freshness.source_date == dates[-2]
        assert margin.freshness.lag_trading_days == 1
        assert margin.freshness.status == "fresh"
        assert margin.current_value is not None
        assert margin.excluded_reason is None
        assert margin.freshness.reason and "ignored later incomplete" in margin.freshness.reason
        assert len(margin.history) == 1
        assert margin.current_value not in margin.history
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sector_inputs_use_all_market_denominator_but_do_not_rank_null_industry(tmp_path):
    dates = tuple(date(2026, 6, 22) + timedelta(days=offset) for offset in range(22))
    target = dates[-1]
    stocks = [_stock("600001.SH", industry="电子"), _stock("000001.SZ", industry="机械"), _stock("920001.BJ", industry=None)]
    records = _history_records([item.symbol for item in stocks], dates)
    _write_dataset(tmp_path, "klines_daily", records)
    engine, sessions = await _database(tmp_path, stocks)
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_sector_inputs(target_date=target)

        assert isinstance(result, radar_data.SectorInputSet)
        assert result.status == "fresh"
        assert result.source_freshness.status == "fresh"
        assert {item.industry for item in result.sectors} == {"电子", "机械"}
        assert sum(item.amount_share for item in result.sectors) < 1.0
        assert all(item.classification == "current_non_pit" for item in result.sectors)
        assert all(item.amount_vs_20d is not None for item in result.sectors)
        assert all(item.share_z20 is not None for item in result.sectors)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sector_input_set_distinguishes_stale_source_and_empty_classification(tmp_path):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]

    stale_root = tmp_path / "stale"
    _write_dataset(stale_root, "klines_daily", _history_records(symbols, (date(2026, 7, 16), previous)))
    engine, sessions = await _database(stale_root, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            stale = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(stale_root)),
                calendar_provider=StaticCalendar((date(2026, 7, 16), previous, target)),
                now=lambda: NOW,
            ).load_sector_inputs(target_date=target)
        assert stale.status == "stale"
        assert stale.sectors == ()
        assert stale.source_freshness.source_date == previous
        assert stale.source_freshness.reason
    finally:
        await engine.dispose()

    empty_root = tmp_path / "empty"
    _write_dataset(empty_root, "klines_daily", _history_records(symbols, (previous, target)))
    empty_stocks = [_stock(symbol, industry=None) for symbol in symbols]
    engine, sessions = await _database(empty_root, empty_stocks)
    try:
        async with sessions() as session:
            empty = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(empty_root)),
                calendar_provider=StaticCalendar((previous, target)),
                now=lambda: NOW,
            ).load_sector_inputs(target_date=target)
        assert empty.status == "partial"
        assert empty.sectors == ()
        assert empty.source_freshness.status == "partial"
        assert "classification" in (empty.source_freshness.reason or "")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sentiment_batch_uses_scores_and_analysis_without_future_leakage(tmp_path):
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    as_of = NOW
    try:
        async with sessions() as session:
            session.add_all(
                [
                    SentimentPost(source="guba", source_post_id="fresh", symbol="600001.SH", title="market crash and earnings warning", content="negative demand collapse across factories", published_at=as_of - timedelta(hours=6), sentiment_score=-0.8, sentiment_label="negative", reply_count=10, like_count=5, comment_count=0),
                    SentimentPost(source="xueqiu", source_post_id="analyzed", symbol="600001.SH", title="new product launch growth outlook", content="positive orders and expanding customer demand", published_at=as_of - timedelta(hours=2), sentiment_score=None, reply_count=0, like_count=0, comment_count=0),
                    SentimentPost(source="guba", source_post_id="future", symbol="600001.SH", published_at=as_of + timedelta(minutes=1), sentiment_score=1.0, reply_count=100, like_count=100, comment_count=100),
                    SentimentPost(source="guba", source_post_id="empty", symbol="600001.SH", published_at=as_of - timedelta(hours=1), sentiment_score=None, reply_count=0, like_count=0, comment_count=0),
                    SentimentAnalysis(source="xueqiu", source_item_id="analyzed", symbol="600001.SH", model_version="model-v2", score=0.4, label="positive", confidence=0.8, analyzed_at=as_of - timedelta(hours=1)),
                ]
            )
            await session.commit()
            result = await MarketRadarDataService(session, now=lambda: NOW).load_sentiment_inputs(as_of=as_of, mode="intraday")

        assert result.status == "fresh"
        assert result.sample_size == 2
        assert result.source_count == 2
        assert result.latest_at == as_of - timedelta(hours=2)
        assert result.latest_model == "model-v2"
        assert -0.8 < result.weighted_score < 0.4
        assert result.negative_ratio == pytest.approx(
            (1 + math.log1p(15)) / ((1 + math.log1p(15)) + 0.8)
        )
        assert result.disagreement > 0
        assert result.cluster_intensity == pytest.approx(0.5)
        assert result.freshness.row_count == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sentiment_freshness_uses_six_and_twenty_four_hour_boundaries_and_empty_is_unavailable(tmp_path):
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            session.add(SentimentPost(source="guba", source_post_id="edge", symbol="600001.SH", published_at=NOW - timedelta(hours=7), sentiment_score=-0.2, reply_count=0, like_count=0, comment_count=0))
            await session.commit()
            service = MarketRadarDataService(session, now=lambda: NOW)
            intraday = await service.load_sentiment_inputs(as_of=NOW, mode="intraday")
            eod = await service.load_sentiment_inputs(as_of=NOW, mode="eod")
            empty = await service.load_sentiment_inputs(as_of=NOW - timedelta(days=200), mode="eod")

        assert intraday.status == "stale"
        assert eod.status == "fresh"
        assert empty.status == "unavailable"
        assert empty.weighted_score is None
        assert empty.freshness.reason
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sentiment_current_window_is_not_diluted_by_121_day_history(tmp_path):
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            session.add_all(
                [
                    SentimentPost(source="guba", source_post_id="current", symbol="600001.SH", published_at=NOW - timedelta(hours=2), sentiment_score=-0.5, reply_count=0, like_count=0, comment_count=0),
                    SentimentPost(source="guba", source_post_id="history", symbol="600001.SH", published_at=NOW - timedelta(days=30), sentiment_score=1.0, reply_count=10000, like_count=10000, comment_count=10000),
                ]
            )
            await session.commit()
            result = await MarketRadarDataService(session, now=lambda: NOW).load_sentiment_inputs(
                as_of=NOW,
                mode="intraday",
            )

        assert result.status == "fresh"
        assert result.sample_size == 1
        assert result.weighted_score == pytest.approx(-0.5)
        assert len(result.daily_history) == 2
        assert result.freshness.row_count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sentiment_uses_weighted_dispersion_and_text_event_clusters(tmp_path):
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            session.add_all(
                [
                    SentimentPost(
                        source="guba",
                        source_post_id="negative",
                        symbol="600001.SH",
                        title="semiconductor earnings collapse warning",
                        content="factory orders and margins deteriorated sharply",
                        published_at=NOW - timedelta(hours=2),
                        sentiment_score=-1.0,
                        reply_count=9,
                        like_count=0,
                        comment_count=0,
                    ),
                    SentimentPost(
                        source="guba",
                        source_post_id="positive",
                        symbol="600001.SH",
                        title="renewable energy policy expansion",
                        content="new solar capacity targets improve demand outlook",
                        published_at=NOW - timedelta(hours=1),
                        sentiment_score=1.0,
                        reply_count=0,
                        like_count=0,
                        comment_count=0,
                    ),
                ]
            )
            await session.commit()
            result = await MarketRadarDataService(session, now=lambda: NOW).load_sentiment_inputs(
                as_of=NOW,
                mode="intraday",
            )

        negative_weight = 1 + math.log1p(9)
        positive_weight = 1.0
        total_weight = negative_weight + positive_weight
        expected_mean = (-negative_weight + positive_weight) / total_weight
        expected_variance = (
            negative_weight * (-1.0 - expected_mean) ** 2
            + positive_weight * (1.0 - expected_mean) ** 2
        ) / total_weight
        assert result.negative_ratio == pytest.approx(negative_weight / total_weight)
        assert result.disagreement == pytest.approx(expected_variance**0.5)
        assert result.cluster_intensity == pytest.approx(0.5)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sentiment_cluster_intensity_uses_only_the_hundred_cluster_candidates(tmp_path):
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    try:
        async with sessions() as session:
            session.add_all(
                [
                    SentimentPost(
                        source="guba",
                        source_post_id=f"cluster-{index}",
                        symbol="600001.SH",
                        title="semiconductor production recovery signal",
                        content="factory utilization and orders improve across the supply chain",
                        published_at=NOW - timedelta(minutes=index),
                        sentiment_score=0.2,
                        reply_count=0,
                        like_count=0,
                        comment_count=0,
                    )
                    for index in range(120)
                ]
            )
            await session.commit()
            result = await MarketRadarDataService(session, now=lambda: NOW).load_sentiment_inputs(
                as_of=NOW,
                mode="intraday",
            )

        assert result.sample_size == 120
        assert result.cluster_intensity == pytest.approx(1.0)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_sentiment_historical_cutoff_rejects_future_created_or_updated_raw_values(tmp_path):
    engine, sessions = await _database(tmp_path, [_stock("600001.SH")])
    before = NOW - timedelta(days=1)
    after = NOW + timedelta(days=1)
    try:
        async with sessions() as session:
            session.add_all(
                [
                    SentimentPost(source="guba", source_post_id="trusted", symbol="600001.SH", title="trusted current market note", content="ordinary evidence available before cutoff", published_at=NOW - timedelta(hours=2), sentiment_score=-0.2, reply_count=0, like_count=0, comment_count=0, created_at=before, updated_at=before),
                    SentimentPost(source="xueqiu", source_post_id="analyzed-backfill", symbol="600001.SH", title="analysis existed before cutoff", content="raw values were updated only after cutoff", published_at=NOW - timedelta(hours=1), sentiment_score=-1.0, reply_count=1000, like_count=1000, comment_count=1000, created_at=before, updated_at=after),
                    SentimentPost(source="guba", source_post_id="raw-backfill", symbol="600001.SH", title="future raw sentiment update", content="this score was backfilled after historical cutoff", published_at=NOW - timedelta(minutes=30), sentiment_score=1.0, reply_count=1000, like_count=1000, comment_count=1000, created_at=before, updated_at=after),
                    SentimentPost(source="guba", source_post_id="future-created", symbol="600001.SH", title="future created row", content="record did not exist at historical cutoff", published_at=NOW - timedelta(minutes=10), sentiment_score=1.0, reply_count=0, like_count=0, comment_count=0, created_at=after, updated_at=after),
                    SentimentAnalysis(source="xueqiu", source_item_id="analyzed-backfill", symbol="600001.SH", model_version="historical-v1", score=0.4, label="positive", confidence=0.8, analyzed_at=NOW - timedelta(minutes=45)),
                ]
            )
            await session.commit()
            result = await MarketRadarDataService(session, now=lambda: NOW).load_sentiment_inputs(
                as_of=NOW,
                mode="intraday",
            )

        assert result.sample_size == 2
        assert result.heat == pytest.approx(1.8)
        assert result.weighted_score == pytest.approx((-0.2 + 0.4 * 0.8) / 1.8)
        assert result.latest_at == NOW - timedelta(hours=1)
        assert result.latest_model == "historical-v1"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_corrupt_optional_limit_sources_degrade_independently(tmp_path):
    target = date(2026, 7, 20)
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]

    bad_step_root = tmp_path / "bad-step"
    _write_dataset(
        bad_step_root,
        "tushare_limit_list_d",
        [{"symbol": "600001.SH", "name": "甲", "trade_date_dt": target, "limit": "U", "limit_times": 2}],
    )
    _write_corrupt_dataset(bad_step_root, "tushare_limit_step", [{"wrong_date": "20260720"}])
    engine, sessions = await _database(bad_step_root, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(bad_step_root)),
                calendar_provider=StaticCalendar((target,)),
                now=lambda: NOW,
            ).load_limit_ladder(target_date=target)
        assert result.source_mode == "derived"
        assert result.rows[0].board_count == 2
        assert result.step_freshness.status == "unavailable"
        assert result.step_freshness.reason
        assert str(bad_step_root) not in result.step_freshness.reason
        assert len(result.step_freshness.reason) < 160
    finally:
        await engine.dispose()

    both_root = tmp_path / "both-invalid"
    _write_dataset(
        both_root,
        "tushare_limit_list_d",
        [{"trade_date_dt": target, "wrong_detail": "x"}],
    )
    _write_dataset(
        both_root,
        "tushare_limit_step",
        [{"trade_date_dt": target, "wrong_step": "x"}],
    )
    engine, sessions = await _database(both_root, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(both_root)),
                calendar_provider=StaticCalendar((target,)),
                now=lambda: NOW,
            ).load_limit_ladder(target_date=target)
        assert result.detail_freshness.status == "unavailable"
        assert result.step_freshness.status == "unavailable"
        assert result.status == "unavailable"
    finally:
        await engine.dispose()

    bad_detail_root = tmp_path / "bad-detail"
    _write_corrupt_dataset(bad_detail_root, "tushare_limit_list_d", [{"wrong_date": "20260720"}])
    _write_dataset(
        bad_detail_root,
        "tushare_limit_step",
        [{"symbol": "600001.SH", "name": "甲", "trade_date_dt": target, "nums": "2"}],
    )
    engine, sessions = await _database(bad_detail_root, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(bad_detail_root)),
                calendar_provider=StaticCalendar((target,)),
                now=lambda: NOW,
            ).load_limit_ladder(target_date=target)
        assert result.status == "partial"
        assert result.source_mode == "official"
        assert result.rows[0].board_count == 2
        assert result.detail_freshness.status == "unavailable"
        assert result.detail_freshness.reason
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_corrupt_optional_margin_keeps_daily_crowding_components(tmp_path):
    dates = _weekdays(date(2026, 6, 15), 21)
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    _write_corrupt_dataset(tmp_path, "tushare_margin", [{"wrong_date": "20260720"}])
    engine, sessions = await _database(tmp_path, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_crowding_inputs(target_date=dates[-1])

        components = {item.key: item for item in result.components}
        assert components["top_1_amount_share"].current_value is not None
        assert components["margin_balance_5d_change"].current_value is None
        assert components["margin_balance_5d_change"].freshness.status == "unavailable"
        assert components["margin_balance_5d_change"].freshness.reason
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_market_radar_data_serializer_is_explicit_and_json_ready(tmp_path):
    dates = (date(2026, 7, 17), date(2026, 7, 20))
    symbols = ["600001.SH", "000001.SZ", "920001.BJ"]
    _write_dataset(tmp_path, "klines_daily", _history_records(symbols, dates))
    engine, sessions = await _database(tmp_path, [_stock(symbol) for symbol in symbols])
    try:
        async with sessions() as session:
            result = await MarketRadarDataService(
                session,
                store=ParquetMarketDataStore(str(tmp_path)),
                calendar_provider=StaticCalendar(dates),
                now=lambda: NOW,
            ).load_daily_market(target_date=dates[-1])

        payload = radar_data.serialize_market_radar_data(result)
        assert jsonable_encoder(payload) == payload
        assert payload["expected_date"] == dates[-1].isoformat()
        assert payload["source_freshness"]["source_date"] == dates[-1].isoformat()
        assert isinstance(payload["slices"][0]["breadth"]["buckets"], dict)
    finally:
        await engine.dispose()

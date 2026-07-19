from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

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
) -> Stock:
    return Stock(
        symbol=symbol,
        name=name or symbol,
        exchange=symbol.rsplit(".", 1)[-1],
        list_date=list_date,
        is_st=is_st,
        is_delist=0,
        is_suspend=0,
        industry=industry,
        security_type=security_type,
        product_class=product_class,
    )


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


@pytest.mark.asyncio
async def test_daily_market_uses_exact_previous_market_date_and_calculator_bins(tmp_path):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    returns = (-9.0, -7.0, -5.0, -3.0, -1.0, 0.0, 1.0, 3.0, 5.0, 7.0, 9.0)
    symbols = [f"6000{i:02d}.SH" for i in range(9)] + ["000001.SZ", "920001.BJ"]
    stocks = [_stock(symbol, is_st=1 if index == 1 else 0) for index, symbol in enumerate(symbols)]
    stocks.append(_stock("000985.SH", security_type="index", product_class="index"))
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
    records.extend([_bar(symbols[0], target, 91.0, amount=91.0), _bar("000985.SH", target, 100.0)])
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
    finally:
        await engine.dispose()


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
async def test_limit_ladder_prefers_exact_official_step_and_keeps_step_only_st(tmp_path):
    previous = date(2026, 7, 17)
    target = date(2026, 7, 20)
    _write_dataset(
        tmp_path,
        "tushare_limit_step",
        [
            {"symbol": "600001.SH", "name": "甲", "trade_date_dt": previous, "nums": "1"},
            {"symbol": "600001.SH", "name": "甲", "trade_date_dt": target, "nums": "2"},
            {"symbol": "000001.SZ", "name": "ST乙", "trade_date_dt": target, "nums": "3"},
        ],
    )
    _write_dataset(
        tmp_path,
        "tushare_limit_list_d",
        [
            {"symbol": "600001.SH", "name": "甲", "industry": "电子", "trade_date_dt": target, "limit": "U", "limit_times": 2, "amount": 100.0, "fd_amount": 10.0, "first_time": "093100", "last_time": "145500", "open_times": 1},
            {"symbol": "600002.SH", "name": "丙", "industry": "电子", "trade_date_dt": target, "limit": "D", "limit_times": None, "amount": 80.0, "fd_amount": None, "first_time": None, "last_time": None, "open_times": 0},
            {"symbol": "600003.SH", "name": "丁", "industry": "机械", "trade_date_dt": target, "limit": "Z", "limit_times": None, "amount": 70.0, "fd_amount": None, "first_time": "100000", "last_time": None, "open_times": 2},
        ],
    )
    stocks = [_stock("600001.SH"), _stock("000001.SZ", is_st=1), _stock("600002.SH"), _stock("600003.SH")]
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
        assert result.up_count == 1
        assert result.down_count == 1
        assert result.broken_count == 1
        assert result.broken_rate == pytest.approx(0.5)
        assert result.highest_board == 3
        assert result.promotion_rate == pytest.approx(1.0)
        assert result.status == "fresh"
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

        assert {item.industry for item in result} == {"电子", "机械"}
        assert sum(item.amount_share for item in result) < 1.0
        assert all(item.classification == "current_non_pit" for item in result)
        assert all(item.amount_vs_20d is not None for item in result)
        assert all(item.share_z20 is not None for item in result)
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
                    SentimentPost(source="guba", source_post_id="fresh", symbol="600001.SH", published_at=as_of - timedelta(hours=6), sentiment_score=-0.8, sentiment_label="negative", reply_count=10, like_count=5, comment_count=0),
                    SentimentPost(source="xueqiu", source_post_id="analyzed", symbol="600001.SH", published_at=as_of - timedelta(hours=2), sentiment_score=None, reply_count=0, like_count=0, comment_count=0),
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
        assert result.negative_ratio == pytest.approx(0.5)
        assert result.disagreement > 0
        assert result.cluster_intensity > 0
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

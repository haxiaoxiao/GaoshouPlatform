"""Safe, freshness-aware data aggregation for the market radar."""

from __future__ import annotations

import inspect
import math
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, timedelta
from statistics import median
from typing import Literal

import numpy as np
import pandas as pd
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.blocking import run_blocking
from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.duckdb import get_duckdb
from app.db.models.sentiment import SentimentAnalysis, SentimentPost
from app.db.models.stock import Stock
from app.services.market_radar_calculator import (
    BreadthResult,
    QuoteTick,
    calculate_breadth,
    serialize_breadth_result,
)

FreshnessStatus = Literal["fresh", "partial", "stale", "unavailable"]
CORE_INDEX_LABELS = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "000985.SH": "中证全指",
}
_ALLOWED_DATASETS = frozenset(
    {"klines_daily", "tushare_limit_list_d", "tushare_limit_step", "tushare_margin"}
)


@dataclass(frozen=True, slots=True)
class SourceFreshness:
    source: str
    status: FreshnessStatus
    expected_date: date | datetime | None
    source_date: date | datetime | None
    lag_trading_days: int | None
    row_count: int
    coverage: float | None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class TradingCalendarSnapshot:
    expected_date: date
    trading_dates: tuple[date, ...]
    authoritative: bool
    freshness: SourceFreshness


@dataclass(frozen=True, slots=True)
class UniverseMember:
    symbol: str
    name: str | None
    exchange: str
    industry: str | None
    list_date: date
    delist_date: date | None
    is_st: bool


@dataclass(frozen=True, slots=True)
class DailyStockFact:
    symbol: str
    name: str | None
    exchange: str
    industry: str | None
    is_st: bool
    trade_date: date
    previous_trade_date: date | None
    close: float | None
    previous_close: float | None
    return_pct: float | None
    volume: float | None
    amount: float | None
    exclusion_reason: str | None


@dataclass(frozen=True, slots=True)
class MarketBreakdown:
    key: str
    label: str
    eligible: int
    valid: int
    excluded: int
    advance: int
    decline: int
    flat: int
    median_return: float | None
    amount: float


@dataclass(frozen=True, slots=True)
class DailyIndexReturn:
    symbol: str
    label: str
    trade_date: date
    previous_trade_date: date | None
    close: float | None
    previous_close: float | None
    return_pct: float | None
    status: FreshnessStatus
    reason: str | None


@dataclass(frozen=True, slots=True)
class DailyMarketSlice:
    trade_date: date
    previous_trade_date: date | None
    facts: tuple[DailyStockFact, ...]
    breadth: BreadthResult
    breakdowns: tuple[MarketBreakdown, ...]
    exclusion_counts: tuple[tuple[str, int], ...]
    indices: tuple[DailyIndexReturn, ...]


@dataclass(frozen=True, slots=True)
class DailyMarketData:
    expected_date: date
    status: FreshnessStatus
    calendar: TradingCalendarSnapshot
    universe: tuple[UniverseMember, ...]
    slices: tuple[DailyMarketSlice, ...]
    source_freshness: SourceFreshness
    universe_freshness: SourceFreshness


@dataclass(frozen=True, slots=True)
class LimitLadderRow:
    symbol: str
    name: str | None
    industry: str | None
    is_st: bool
    board_count: int
    pct_change: float | None = None
    turnover_ratio: float | None = None
    limit_times: int | None = None
    amount: float | None = None
    seal_amount: float | None = None
    first_time: str | None = None
    last_time: str | None = None
    open_times: int | None = None


@dataclass(frozen=True, slots=True)
class LimitLadderData:
    trade_date: date
    status: FreshnessStatus
    source_mode: Literal["official", "derived", "unavailable"]
    rows: tuple[LimitLadderRow, ...]
    distribution: tuple[tuple[int, int], ...]
    highest_board: int | None
    up_count: int | None
    down_count: int | None
    broken_count: int | None
    broken_rate: float | None
    promotion_rate: float | None
    detail_freshness: SourceFreshness
    step_freshness: SourceFreshness


@dataclass(frozen=True, slots=True)
class RawComponent:
    key: str
    current_value: float | None
    history: tuple[float, ...]
    freshness: SourceFreshness
    excluded_reason: str | None = None


@dataclass(frozen=True, slots=True)
class CrowdingInputSet:
    as_of: date
    status: FreshnessStatus
    components: tuple[RawComponent, ...]


@dataclass(frozen=True, slots=True)
class SectorInput:
    industry: str
    median_return: float
    advance_ratio: float
    amount_share: float
    share_z20: float | None
    amount_vs_20d: float | None
    stock_count: int
    classification: str = "current_non_pit"


@dataclass(frozen=True, slots=True)
class SectorInputSet:
    as_of: date
    status: FreshnessStatus
    sectors: tuple[SectorInput, ...]
    source_freshness: SourceFreshness
    classification: str
    coverage: float


@dataclass(frozen=True, slots=True)
class SentimentInputSet:
    as_of: datetime
    status: FreshnessStatus
    weighted_score: float | None
    heat: float | None
    sample_size: int
    negative_ratio: float | None
    disagreement: float | None
    cluster_intensity: float | None
    source_count: int
    latest_at: datetime | None
    latest_model: str | None
    daily_history: tuple[tuple[date, float], ...]
    freshness: SourceFreshness


def serialize_market_radar_data(value: object) -> object:
    """Serialize radar contracts without deep-copying immutable data structures."""
    if isinstance(value, BreadthResult):
        return serialize_breadth_result(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: serialize_market_radar_data(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): serialize_market_radar_data(item)
            for key, item in value.items()
        }
    if isinstance(value, (tuple, list)):
        return [serialize_market_radar_data(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported market radar value: {type(value).__name__}")


class MarketRadarDataService:
    """Load normalized facts while keeping AsyncSession on the event-loop thread."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        store: ParquetMarketDataStore | None = None,
        calendar_provider: object | None = None,
        now: Callable[[], datetime] = datetime.now,
    ) -> None:
        self._session = session
        self._store = store or ParquetMarketDataStore()
        self._calendar_provider = calendar_provider
        self._now = now

    async def resolve_calendar(
        self,
        *,
        target_date: date | None = None,
        lookback_days: int = 160,
    ) -> TradingCalendarSnapshot:
        if lookback_days < 2 or lookback_days > 400:
            raise ValueError("lookback_days must be between 2 and 400")
        explicit_target = target_date is not None
        requested = target_date or self._now().date()
        expected = requested
        start = requested - timedelta(days=max(lookback_days * 2, 240))
        if self._calendar_provider is not None:
            raw_dates = await self._provider_dates(start, requested)
            dates = _normalize_dates(raw_dates, end=requested)[-lookback_days:]
            if not explicit_target and dates:
                expected = dates[-1]
            exact = expected in dates
            freshness = SourceFreshness(
                source="trading_calendar",
                status="fresh" if exact else "partial",
                expected_date=expected,
                source_date=dates[-1] if dates else None,
                lag_trading_days=0 if exact else None,
                row_count=len(dates),
                coverage=1.0 if exact else None,
                reason=None if exact else "target date is not present in the authoritative calendar",
            )
            return TradingCalendarSnapshot(expected, dates, True, freshness)

        frame = await self._read_dataset(
            "klines_daily",
            "SELECT DISTINCT CAST(trade_date AS DATE) AS trade_date "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true) "
            "WHERE CAST(trade_date AS DATE) <= ? ORDER BY trade_date DESC LIMIT ?",
            [expected, lookback_days],
        )
        dates = tuple(sorted(_frame_dates(frame, "trade_date")))
        observed = dates[-1] if dates else None
        freshness = SourceFreshness(
            source="observed_klines_calendar",
            status="partial" if dates else "unavailable",
            expected_date=expected,
            source_date=observed,
            lag_trading_days=None,
            row_count=len(dates),
            coverage=None,
            reason=(
                "authoritative trading calendar unavailable; dates are derived from observed klines"
                if dates
                else "authoritative trading calendar and observed klines are unavailable"
            ),
        )
        return TradingCalendarSnapshot(expected, dates, False, freshness)

    async def load_daily_market(
        self,
        *,
        target_date: date | None = None,
        days: int = 1,
    ) -> DailyMarketData:
        if not 1 <= days <= 120:
            raise ValueError("days must be between 1 and 120")
        calendar = await self.resolve_calendar(target_date=target_date, lookback_days=days + 121)
        expected = target_date or calendar.expected_date
        calendar_dates = tuple(item for item in calendar.trading_dates if item <= expected)
        requested_dates = list(calendar_dates[-days:])
        universe, universe_freshness = await self._load_universe(
            expected,
            start_date=requested_dates[0] if requested_dates else expected,
        )
        query_dates = sorted(set(calendar_dates[-(days + 1) :]) | set(requested_dates))

        frame = await self._load_daily_frame(query_dates)
        universe_by_symbol = {item.symbol: item for item in universe}
        universe_symbols = set(universe_by_symbol)
        stock_frame = (
            frame.loc[frame["symbol"].astype(str).isin(universe_symbols)]
            if not frame.empty and "symbol" in frame
            else pd.DataFrame()
        )
        source_date = _latest_date(stock_frame, "trade_date")
        rows_by_key, conflicts = _normalize_daily_rows(
            frame,
            universe_symbols | set(CORE_INDEX_LABELS),
        )
        slices: list[DailyMarketSlice] = []
        for trade_day in requested_dates:
            prior_dates = [item for item in calendar_dates if item < trade_day]
            previous_day = prior_dates[-1] if prior_dates else None
            active_universe = tuple(
                member for member in universe if _member_active_on(member, trade_day)
            )
            facts = tuple(
                self._daily_fact(
                    member,
                    trade_day,
                    previous_day,
                    rows_by_key,
                    conflicts,
                )
                for member in active_universe
            )
            ticks = {
                fact.symbol: QuoteTick(
                    symbol=fact.symbol,
                    quote_time=self._now(),
                    last_price=fact.close or 0.0,
                    previous_close=fact.previous_close or 0.0,
                )
                for fact in facts
                if fact.exclusion_reason is None
            }
            breadth = calculate_breadth(
                ticks,
                (member.symbol for member in active_universe),
                now=self._now(),
                max_age_seconds=1,
            )
            slices.append(
                DailyMarketSlice(
                    trade_date=trade_day,
                    previous_trade_date=previous_day,
                    facts=facts,
                    breadth=breadth,
                    breakdowns=_market_breakdowns(facts),
                    exclusion_counts=tuple(sorted(Counter(
                        fact.exclusion_reason for fact in facts if fact.exclusion_reason
                    ).items())),
                    indices=self._daily_index_returns(
                        trade_day,
                        previous_day,
                        rows_by_key,
                        conflicts,
                    ),
                )
            )

        latest_slice = slices[-1] if slices else None
        coverage = latest_slice.breadth.coverage.coverage if latest_slice else 0.0
        source_status, source_reason = _exact_source_status(
            expected=expected,
            source_date=source_date,
            coverage=coverage,
            minimum_coverage=0.8,
            rows=len(stock_frame.index),
        )
        source_freshness = SourceFreshness(
            source="klines_daily",
            status=source_status,
            expected_date=expected,
            source_date=source_date,
            lag_trading_days=_trading_lag(expected, source_date, calendar.trading_dates),
            row_count=len(stock_frame.index),
            coverage=coverage,
            reason=source_reason,
        )
        status = source_status
        if status == "fresh" and (
            universe_freshness.status != "fresh" or calendar.freshness.status != "fresh"
        ):
            status = "partial"
        return DailyMarketData(
            expected_date=expected,
            status=status,
            calendar=calendar,
            universe=universe,
            slices=tuple(slices),
            source_freshness=source_freshness,
            universe_freshness=universe_freshness,
        )

    async def load_limit_ladder(self, *, target_date: date | None = None) -> LimitLadderData:
        calendar = await self.resolve_calendar(target_date=target_date, lookback_days=40)
        expected = target_date or calendar.expected_date
        universe, _ = await self._load_universe(expected)
        universe_by_symbol = {item.symbol: item for item in universe}
        history_start = (
            calendar.trading_dates[-20]
            if len(calendar.trading_dates) >= 20
            else (calendar.trading_dates[0] if calendar.trading_dates else expected - timedelta(days=60))
        )
        detail, detail_date, detail_count, detail_error = await self._load_optional_dated_dataset(
            "tushare_limit_list_d",
            date_column="trade_date_dt",
            expected=expected,
            start=history_start,
        )
        step, step_date, step_count, step_error = await self._load_optional_dated_dataset(
            "tushare_limit_step",
            date_column="trade_date_dt",
            expected=expected,
            start=history_start,
        )
        if detail_error is None and detail_date == expected:
            detail_error = _optional_schema_error(
                detail,
                source="tushare_limit_list_d",
                required=("limit",),
                any_of=("symbol", "ts_code"),
            )
        if step_error is None and step_date == expected:
            step_error = _optional_schema_error(
                step,
                source="tushare_limit_step",
                required=("nums",),
                any_of=("symbol", "ts_code"),
            )
        detail_exact = detail_date == expected and detail_error is None
        step_exact = step_date == expected and step_error is None
        detail_freshness = (
            _unavailable_freshness("tushare_limit_list_d", expected, detail_error)
            if detail_error
            else _dated_freshness(
                "tushare_limit_list_d",
                expected,
                detail_date,
                detail_count,
                calendar.trading_dates,
            )
        )
        step_freshness = (
            _unavailable_freshness("tushare_limit_step", expected, step_error)
            if step_error
            else _dated_freshness(
                "tushare_limit_step",
                expected,
                step_date,
                step_count,
                calendar.trading_dates,
            )
        )

        if not detail_exact and not step_exact:
            both_unavailable = (
                detail_freshness.status == "unavailable"
                and step_freshness.status == "unavailable"
            )
            status: FreshnessStatus = (
                "unavailable"
                if both_unavailable or (detail_date is None and step_date is None)
                else "stale"
            )
            return LimitLadderData(
                trade_date=expected,
                status=status,
                source_mode="unavailable",
                rows=(),
                distribution=(),
                highest_board=None,
                up_count=None,
                down_count=None,
                broken_count=None,
                broken_rate=None,
                promotion_rate=None,
                detail_freshness=detail_freshness,
                step_freshness=step_freshness,
            )

        detail_current = _rows_on_date(detail, "trade_date_dt", expected)
        detail_by_symbol = {
            str(row.get("symbol") or row.get("ts_code") or ""): row
            for row in detail_current
        }
        rows: list[LimitLadderRow] = []
        source_mode: Literal["official", "derived", "unavailable"]
        promotion_rate: float | None = None
        if step_exact:
            source_mode = "official"
            step_current = _rows_on_date(step, "trade_date_dt", expected)
            for source in step_current:
                symbol = str(source.get("symbol") or source.get("ts_code") or "")
                board_count = _positive_int(source.get("nums"))
                if not symbol or board_count is None:
                    continue
                rows.append(
                    _ladder_row(
                        symbol,
                        board_count,
                        source,
                        detail_by_symbol.get(symbol),
                        universe_by_symbol.get(symbol),
                    )
                )
            prior_dates = [item for item in calendar.trading_dates if item < expected]
            previous = prior_dates[-1] if prior_dates else None
            previous_steps = _rows_on_date(step, "trade_date_dt", previous)
            previous_boards = {
                str(row.get("symbol") or row.get("ts_code") or ""): _positive_int(row.get("nums"))
                for row in previous_steps
            }
            valid_previous = {
                symbol: board
                for symbol, board in previous_boards.items()
                if board is not None
            }
            current_boards = {row.symbol: row.board_count for row in rows}
            if valid_previous:
                promotion_rate = sum(
                    current_boards.get(symbol, 0) > board
                    for symbol, board in valid_previous.items()
                ) / len(valid_previous)
        else:
            source_mode = "derived"
            up_history = _up_limit_dates(detail)
            for source in detail_current:
                if str(source.get("limit") or "").upper() != "U":
                    continue
                symbol = str(source.get("symbol") or source.get("ts_code") or "")
                if not symbol:
                    continue
                board_count = _positive_int(source.get("limit_times"))
                if board_count is None:
                    board_count = _consecutive_limit_ups(
                        symbol, expected, calendar.trading_dates, up_history
                    )
                rows.append(
                    _ladder_row(
                        symbol,
                        max(1, board_count),
                        source,
                        source,
                        universe_by_symbol.get(symbol),
                    )
                )

        rows.sort(key=lambda item: (-item.board_count, item.symbol))
        distribution_counter = Counter(item.board_count for item in rows)
        limit_types = Counter(
            str(row.get("limit") or "").upper() for row in detail_current
        ) if detail_exact else Counter()
        up_count = limit_types.get("U", 0) if detail_exact else None
        down_count = limit_types.get("D", 0) if detail_exact else None
        broken_count = limit_types.get("Z", 0) if detail_exact else None
        broken_denominator = (up_count or 0) + (broken_count or 0)
        broken_rate = (
            (broken_count or 0) / broken_denominator
            if detail_exact and broken_denominator > 0
            else None
        )
        status = "fresh" if detail_exact and step_exact else "partial"
        return LimitLadderData(
            trade_date=expected,
            status=status,
            source_mode=source_mode,
            rows=tuple(rows),
            distribution=tuple(sorted(distribution_counter.items())),
            highest_board=max((item.board_count for item in rows), default=None),
            up_count=up_count,
            down_count=down_count,
            broken_count=broken_count,
            broken_rate=broken_rate,
            promotion_rate=promotion_rate,
            detail_freshness=detail_freshness,
            step_freshness=step_freshness,
        )

    async def load_crowding_inputs(self, *, target_date: date | None = None) -> CrowdingInputSet:
        calendar = await self.resolve_calendar(target_date=target_date, lookback_days=180)
        expected = target_date or calendar.expected_date
        dates = tuple(item for item in calendar.trading_dates if item <= expected)[-141:]
        universe, universe_freshness = await self._load_universe(
            expected,
            start_date=dates[0] if dates else expected,
        )
        daily = await self._load_daily_range(dates)
        series, _ = await run_blocking(_aggregate_daily_inputs, daily, universe, dates)
        daily_source_date = _latest_date(daily, "trade_date")
        current_panel = series.get("coverage", {}).get(expected)
        coverage = float(current_panel) if current_panel is not None else 0.0
        daily_status, daily_reason = _exact_source_status(
            expected=expected,
            source_date=daily_source_date,
            coverage=coverage,
            minimum_coverage=0.8,
            rows=len(daily.index),
        )
        if daily_status == "fresh" and universe_freshness.status != "fresh":
            daily_status = "partial"
            daily_reason = universe_freshness.reason
        daily_freshness = SourceFreshness(
            source="klines_daily",
            status=daily_status,
            expected_date=expected,
            source_date=daily_source_date,
            lag_trading_days=_trading_lag(expected, daily_source_date, calendar.trading_dates),
            row_count=len(daily.index),
            coverage=coverage,
            reason=daily_reason,
        )

        components: list[RawComponent] = []
        for key in (
            "top_1_amount_share",
            "top_5_amount_share",
            "top_3_sector_share",
            "market_amount_vs_20d",
            "high_liquidity_correlation",
        ):
            values = series.get(key, {})
            current = _finite(values.get(expected))
            history = tuple(
                value
                for trade_day, raw in sorted(values.items())
                if trade_day < expected and (value := _finite(raw)) is not None
            )[-120:]
            excluded_reason = None
            if daily_source_date != expected:
                current = None
                excluded_reason = "daily_source_not_exact"
            elif current is None:
                pair_coverage = _finite(
                    series.get("high_liquidity_correlation_pair_coverage", {}).get(expected)
                )
                excluded_reason = (
                    "insufficient_pair_coverage"
                    if key == "high_liquidity_correlation"
                    and pair_coverage is not None
                    and pair_coverage < 0.8
                    else "insufficient_daily_history"
                )
            components.append(
                RawComponent(
                    key=key,
                    current_value=current,
                    history=history,
                    freshness=daily_freshness,
                    excluded_reason=excluded_reason,
                )
            )

        margin = await self._load_margin_component(expected, calendar)
        components.append(margin)
        statuses = {item.freshness.status for item in components}
        if all(item.current_value is None for item in components):
            status: FreshnessStatus = "unavailable" if "unavailable" in statuses else "stale"
        elif statuses == {"fresh"}:
            status = "fresh"
        else:
            status = "partial"
        return CrowdingInputSet(as_of=expected, status=status, components=tuple(components))

    async def load_sector_inputs(self, *, target_date: date | None = None) -> SectorInputSet:
        calendar = await self.resolve_calendar(target_date=target_date, lookback_days=180)
        expected = target_date or calendar.expected_date
        dates = tuple(item for item in calendar.trading_dates if item <= expected)[-141:]
        universe, universe_freshness = await self._load_universe(
            expected,
            start_date=dates[0] if dates else expected,
        )
        daily = await self._load_daily_range(dates)
        series, sectors_by_date = await run_blocking(
            _aggregate_daily_inputs,
            daily,
            universe,
            dates,
            False,
        )
        current = sectors_by_date.get(expected, {})
        source_date = _latest_date(daily, "trade_date")
        coverage = float(series.get("coverage", {}).get(expected, 0.0))
        source_status, source_reason = _exact_source_status(
            expected=expected,
            source_date=source_date,
            coverage=coverage,
            minimum_coverage=0.8,
            rows=len(daily.index),
        )
        if source_status == "fresh" and not current:
            source_status = "partial"
            source_reason = "exact daily source has no non-null industry classification"
        elif source_status == "fresh" and universe_freshness.status != "fresh":
            source_status = "partial"
            source_reason = universe_freshness.reason
        elif source_status == "fresh" and calendar.freshness.status != "fresh":
            source_status = "partial"
            source_reason = calendar.freshness.reason
        source_freshness = SourceFreshness(
            source="klines_daily_sector_inputs",
            status=source_status,
            expected_date=expected,
            source_date=source_date,
            lag_trading_days=_trading_lag(expected, source_date, calendar.trading_dates),
            row_count=len(_rows_on_date(daily, "trade_date", source_date)),
            coverage=coverage,
            reason=source_reason,
        )
        prior_dates = [item for item in dates if item < expected][-20:]
        result: list[SectorInput] = []
        for industry, metrics in current.items():
            shares = [
                value
                for trade_day in prior_dates
                if (value := _finite(
                    sectors_by_date.get(trade_day, {}).get(industry, {}).get("amount_share")
                )) is not None
            ]
            amounts = [
                value
                for trade_day in prior_dates
                if (value := _finite(
                    sectors_by_date.get(trade_day, {}).get(industry, {}).get("amount")
                )) is not None
            ]
            current_share = float(metrics["amount_share"])
            current_amount = float(metrics["amount"])
            share_z20 = _z_score(current_share, shares) if len(shares) >= 20 else None
            amount_vs20 = (
                current_amount / (sum(amounts) / len(amounts))
                if len(amounts) >= 20 and sum(amounts) > 0
                else None
            )
            result.append(
                SectorInput(
                    industry=industry,
                    median_return=float(metrics["median_return"]),
                    advance_ratio=float(metrics["advance_ratio"]),
                    amount_share=current_share,
                    share_z20=share_z20,
                    amount_vs_20d=amount_vs20,
                    stock_count=int(metrics["stock_count"]),
                )
            )
        return SectorInputSet(
            as_of=expected,
            status=source_status,
            sectors=tuple(sorted(result, key=lambda item: (-item.amount_share, item.industry))),
            source_freshness=source_freshness,
            classification="current_non_pit",
            coverage=coverage,
        )

    async def load_sentiment_inputs(
        self,
        *,
        as_of: datetime | None = None,
        mode: Literal["intraday", "eod"] = "intraday",
    ) -> SentimentInputSet:
        if mode not in ("intraday", "eod"):
            raise ValueError("mode must be 'intraday' or 'eod'")
        cutoff = as_of or self._now()
        start = cutoff - timedelta(days=121)
        post_statement = (
            select(
                SentimentPost.source,
                SentimentPost.source_post_id,
                SentimentPost.symbol,
                SentimentPost.title,
                SentimentPost.content,
                SentimentPost.author,
                SentimentPost.published_at,
                SentimentPost.sentiment_score,
                SentimentPost.reply_count,
                SentimentPost.like_count,
                SentimentPost.comment_count,
                SentimentPost.created_at,
                SentimentPost.updated_at,
            )
            .where(
                SentimentPost.published_at.is_not(None),
                SentimentPost.published_at >= start,
                SentimentPost.published_at <= cutoff,
                SentimentPost.created_at <= cutoff,
            )
            .order_by(SentimentPost.published_at)
        )
        analysis_statement = (
            select(
                SentimentAnalysis.source,
                SentimentAnalysis.source_item_id,
                SentimentAnalysis.symbol,
                SentimentAnalysis.model_version,
                SentimentAnalysis.score,
                SentimentAnalysis.confidence,
                SentimentAnalysis.analyzed_at,
            )
            .where(
                SentimentAnalysis.score.is_not(None),
                SentimentAnalysis.analyzed_at >= start,
                SentimentAnalysis.analyzed_at <= cutoff,
            )
            .order_by(SentimentAnalysis.analyzed_at)
        )
        post_rows = (await self._session.execute(post_statement)).all()
        analysis_rows = (await self._session.execute(analysis_statement)).all()
        latest_analysis: dict[tuple[str, str, str], dict[str, object]] = {}
        for row in analysis_rows:
            score = _finite(row.score)
            if score is None:
                continue
            latest_analysis[(str(row.source), str(row.source_item_id), str(row.symbol))] = {
                "score": score,
                "confidence": max(0.0, min(1.0, _finite(row.confidence) or 0.0)),
                "model": str(row.model_version),
                "analyzed_at": row.analyzed_at,
            }

        observations: list[dict[str, object]] = []
        for row in post_rows:
            published_at = row.published_at
            if published_at is None or published_at > cutoff:
                continue
            analysis = latest_analysis.get(
                (str(row.source), str(row.source_post_id), str(row.symbol))
            )
            raw_trusted = row.updated_at is None or row.updated_at <= cutoff
            raw_score = _finite(row.sentiment_score) if raw_trusted else None
            if analysis is not None:
                score = _finite(analysis["score"])
                confidence = _finite(analysis["confidence"]) or 0.0
                model = str(analysis["model"])
                analyzed_at = analysis["analyzed_at"]
            else:
                score = raw_score
                confidence = 1.0
                model = None
                analyzed_at = None
            if score is None:
                continue
            engagement = (
                sum(
                    max(0, int(value or 0))
                    for value in (row.reply_count, row.like_count, row.comment_count)
                )
                if raw_trusted
                else 0
            )
            weight = (1.0 + math.log1p(engagement)) * max(0.1, confidence)
            observations.append(
                {
                    "source": str(row.source),
                    "published_at": published_at,
                    "score": score,
                    "weight": weight,
                    "model": model,
                    "analyzed_at": analyzed_at,
                    "cluster_post": SentimentPost(
                        source=str(row.source),
                        source_post_id=str(row.source_post_id),
                        symbol=str(row.symbol),
                        title=row.title,
                        content=row.content,
                        author=row.author,
                        published_at=published_at,
                        sentiment_score=score,
                        reply_count=int(row.reply_count or 0),
                        like_count=int(row.like_count or 0),
                        comment_count=int(row.comment_count or 0),
                    ),
                }
            )

        if not observations:
            freshness = SourceFreshness(
                source="sentiment_posts",
                status="unavailable",
                expected_date=cutoff,
                source_date=None,
                lag_trading_days=None,
                row_count=0,
                coverage=0.0,
                reason="no scored or analyzed sentiment posts in the 121-day window",
            )
            return SentimentInputSet(
                as_of=cutoff,
                status="unavailable",
                weighted_score=None,
                heat=None,
                sample_size=0,
                negative_ratio=None,
                disagreement=None,
                cluster_intensity=None,
                source_count=0,
                latest_at=None,
                latest_model=None,
                daily_history=(),
                freshness=freshness,
            )

        latest_at = max(item["published_at"] for item in observations)
        threshold = timedelta(hours=6 if mode == "intraday" else 24)
        current_observations = [
            item
            for item in observations
            if cutoff - threshold <= item["published_at"] <= cutoff
        ]
        status = "fresh" if current_observations else "stale"
        reason = None if status == "fresh" else (
            f"latest scored sentiment is older than {int(threshold.total_seconds() // 3600)} hours"
        )
        sources = {str(item["source"]) for item in current_observations}
        from app.services.sentiment import _event_clusters

        cluster_candidates = sorted(
            current_observations,
            key=lambda item: item["published_at"],
            reverse=True,
        )[:100]
        event_clusters = _event_clusters(
            [item["cluster_post"] for item in cluster_candidates],
            limit=max(1, len(cluster_candidates)),
        )
        clustered_sample_count = sum(
            int(cluster["post_count"]) for cluster in event_clusters
        )
        analyzed = [
            item for item in current_observations
            if item["model"] is not None and isinstance(item["analyzed_at"], datetime)
        ]
        latest_model = (
            str(max(analyzed, key=lambda item: item["analyzed_at"])["model"])
            if analyzed
            else None
        )
        daily_groups: dict[date, list[dict[str, object]]] = defaultdict(list)
        for item in observations:
            daily_groups[item["published_at"].date()].append(item)
        daily_history = tuple(
            (
                trade_day,
                sum(float(item["score"]) * float(item["weight"]) for item in items)
                / sum(float(item["weight"]) for item in items),
            )
            for trade_day, items in sorted(daily_groups.items())
        )
        total_weight = sum(float(item["weight"]) for item in current_observations)
        weighted_score = (
            sum(
                float(item["score"]) * float(item["weight"])
                for item in current_observations
            )
            / total_weight
            if total_weight > 0
            else None
        )
        freshness = SourceFreshness(
            source="sentiment_posts",
            status=status,
            expected_date=cutoff,
            source_date=latest_at,
            lag_trading_days=None,
            row_count=len(current_observations),
            coverage=min(1.0, len(sources) / 4),
            reason=reason,
        )
        return SentimentInputSet(
            as_of=cutoff,
            status=status,
            weighted_score=weighted_score,
            heat=total_weight if current_observations else None,
            sample_size=len(current_observations),
            negative_ratio=(
                sum(
                    float(item["weight"])
                    for item in current_observations
                    if float(item["score"]) < 0
                )
                / total_weight
                if total_weight > 0
                else None
            ),
            disagreement=(
                math.sqrt(
                    sum(
                        float(item["weight"])
                        * (float(item["score"]) - weighted_score) ** 2
                        for item in current_observations
                    )
                    / total_weight
                )
                if total_weight > 0 and weighted_score is not None
                else None
            ),
            cluster_intensity=(
                max(int(cluster["post_count"]) for cluster in event_clusters)
                / clustered_sample_count
                if clustered_sample_count > 0
                else None
            ),
            source_count=len(sources),
            latest_at=latest_at,
            latest_model=latest_model,
            daily_history=daily_history,
            freshness=freshness,
        )

    async def _provider_dates(self, start: date, end: date) -> Iterable[date]:
        provider = self._calendar_provider
        method = getattr(provider, "get_trading_days", None) or getattr(provider, "trading_days", None)
        if method is None and callable(provider):
            method = provider
        if method is None:
            raise TypeError("calendar_provider must expose get_trading_days(start, end)")
        value = method(start, end)
        return await value if inspect.isawaitable(value) else value

    async def _load_universe(
        self,
        target: date,
        *,
        start_date: date | None = None,
    ) -> tuple[tuple[UniverseMember, ...], SourceFreshness]:
        active_since = start_date or target
        statement = (
            select(
                Stock.symbol,
                Stock.name,
                Stock.exchange,
                Stock.industry,
                Stock.list_date,
                Stock.delist_date,
                Stock.is_st,
                Stock.security_type,
                Stock.product_class,
            )
            .where(
                Stock.list_date.is_not(None),
                Stock.list_date <= target,
                or_(
                    Stock.delist_date > active_since,
                    and_(
                        Stock.delist_date.is_(None),
                        func.coalesce(Stock.is_delist, 0) == 0,
                    ),
                ),
                Stock.exchange.in_(("SH", "SZ", "BJ")),
                or_(Stock.security_type.is_(None), func.lower(Stock.security_type) == "stock"),
                or_(Stock.product_class.is_(None), func.lower(Stock.product_class) == "stock"),
            )
            .order_by(Stock.symbol)
        )
        records = (await self._session.execute(statement)).all()
        members = tuple(
            UniverseMember(
                symbol=str(row.symbol),
                name=row.name,
                exchange=str(row.exchange),
                industry=row.industry,
                list_date=row.list_date,
                delist_date=row.delist_date,
                is_st=bool(row.is_st),
            )
            for row in records
            if _is_equity_symbol(str(row.symbol), str(row.exchange))
        )
        target_members = tuple(item for item in members if _member_active_on(item, target))
        exchanges = {item.exchange for item in target_members}
        missing = sorted({"SH", "SZ", "BJ"} - exchanges)
        status: FreshnessStatus = "fresh" if target_members and not missing else ("partial" if target_members else "unavailable")
        reason = None
        if missing:
            reason = f"universe missing exchange coverage: {', '.join(missing)}"
        elif not target_members:
            reason = "no eligible A-share stocks in SQLite universe"
        return members, SourceFreshness(
            source="sqlite_stocks",
            status=status,
            expected_date=target,
            source_date=target if target_members else None,
            lag_trading_days=0 if target_members else None,
            row_count=len(target_members),
            coverage=len(exchanges) / 3,
            reason=reason,
        )

    async def _load_daily_frame(self, dates: Sequence[date]) -> pd.DataFrame:
        if not dates:
            return pd.DataFrame()
        placeholders = ", ".join("?" for _ in dates)
        sql = (
            "SELECT symbol, CAST(trade_date AS DATE) AS trade_date, close, volume, amount "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true) "
            f"WHERE CAST(trade_date AS DATE) IN ({placeholders})"
        )
        return await self._read_dataset("klines_daily", sql, list(dates))

    async def _load_daily_range(self, dates: Sequence[date]) -> pd.DataFrame:
        if not dates:
            return pd.DataFrame()
        return await self._read_dataset(
            "klines_daily",
            "SELECT symbol, CAST(trade_date AS DATE) AS trade_date, close, volume, amount "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true) "
            "WHERE CAST(trade_date AS DATE) BETWEEN ? AND ?",
            [dates[0], dates[-1]],
        )

    async def _load_margin_component(
        self,
        expected: date,
        calendar: TradingCalendarSnapshot,
    ) -> RawComponent:
        dates = tuple(item for item in calendar.trading_dates if item <= expected)[-130:]
        start = dates[0] if dates else expected - timedelta(days=200)
        frame, error = await self._read_optional_dataset(
            "tushare_margin",
            "SELECT CAST(trade_date_dt AS DATE) AS trade_date_dt, exchange_id, rzye "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true) "
            "WHERE CAST(trade_date_dt AS DATE) BETWEEN ? AND ?",
            [start, expected],
        )
        if error:
            return RawComponent(
                key="margin_balance_5d_change",
                current_value=None,
                history=(),
                freshness=_unavailable_freshness("tushare_margin", expected, error),
                excluded_reason="margin_unavailable",
            )
        latest_raw = _latest_date(frame, "trade_date_dt")
        required = {"SSE", "SZSE", "BSE"}
        by_date: dict[date, dict[str, float]] = defaultdict(dict)
        if not frame.empty:
            for row in frame.to_dict("records"):
                trade_day = _as_date(row.get("trade_date_dt"))
                exchange = str(row.get("exchange_id") or "").upper()
                value = _nonnegative(row.get("rzye"))
                if trade_day is not None and exchange and value is not None:
                    by_date[trade_day][exchange] = value
        complete_dates = {
            trade_day: sum(exchanges[item] for item in required)
            for trade_day, exchanges in by_date.items()
            if required <= exchanges.keys()
        }
        latest = max(complete_dates, default=latest_raw)
        lag = _trading_lag(expected, latest, calendar.trading_dates)
        latest_exchanges = set(by_date.get(latest, {})) if latest is not None else set()
        missing = sorted(required - latest_exchanges)
        status: FreshnessStatus
        reason: str | None
        excluded: str | None = None
        current: float | None = None
        if latest is None:
            status = "unavailable"
            reason = "margin dataset is unavailable or empty"
            excluded = "margin_unavailable"
        elif lag is None or lag > 2:
            status = "stale"
            reason = "margin source lags expected date by more than two trading days"
            excluded = "margin_stale"
        elif missing:
            status = "partial"
            reason = f"margin exchange coverage missing: {', '.join(missing)}"
            excluded = "incomplete_exchange_coverage"
        else:
            status = "fresh"
            reason = (
                f"ignored later incomplete margin rows dated {latest_raw.isoformat()}"
                if latest_raw is not None and latest is not None and latest_raw > latest
                else None
            )
            calendar_order = sorted(item for item in calendar.trading_dates if item <= expected)
            if latest in calendar_order and calendar_order.index(latest) >= 5:
                baseline_day = calendar_order[calendar_order.index(latest) - 5]
                base = complete_dates.get(baseline_day)
                current_total = complete_dates.get(latest)
                if base is None:
                    excluded = "missing_fifth_prior_trading_day"
                elif current_total is not None and base > 0:
                    current = current_total / base - 1.0
            else:
                excluded = "insufficient_margin_history"
        history_values: list[float] = []
        calendar_order = sorted(item for item in calendar.trading_dates if item <= expected)
        for index in range(5, len(calendar_order)):
            if latest is not None and calendar_order[index] >= latest:
                continue
            base = complete_dates.get(calendar_order[index - 5])
            current_total = complete_dates.get(calendar_order[index])
            if base is not None and current_total is not None and base > 0:
                history_values.append(current_total / base - 1.0)
        freshness = SourceFreshness(
            source="tushare_margin",
            status=status,
            expected_date=expected,
            source_date=latest,
            lag_trading_days=lag,
            row_count=len(_rows_on_date(frame, "trade_date_dt", latest)),
            coverage=len(latest_exchanges & required) / len(required),
            reason=reason,
        )
        return RawComponent(
            key="margin_balance_5d_change",
            current_value=current,
            history=tuple(history_values[-120:]),
            freshness=freshness,
            excluded_reason=excluded,
        )

    async def _read_dataset(
        self,
        dataset: str,
        sql: str,
        parameters: Sequence[object],
    ) -> pd.DataFrame:
        if dataset not in _ALLOWED_DATASETS:
            raise ValueError(f"unsupported market radar dataset: {dataset}")
        if not self._store._exists(dataset):
            return pd.DataFrame()
        pattern = self._store._glob_pattern(dataset)

        def query() -> pd.DataFrame:
            return get_duckdb().execute(sql, [pattern, *parameters]).df()

        return await run_blocking(query)

    async def _read_optional_dataset(
        self,
        dataset: str,
        sql: str,
        parameters: Sequence[object],
    ) -> tuple[pd.DataFrame, str | None]:
        try:
            return await self._read_dataset(dataset, sql, parameters), None
        except Exception as exc:  # Optional sources must not take down mandatory radar data.
            reason = f"{dataset} query failed ({type(exc).__name__})"
            return pd.DataFrame(), reason[:159]

    async def _load_optional_dated_dataset(
        self,
        dataset: str,
        *,
        date_column: str,
        expected: date,
        start: date,
    ) -> tuple[pd.DataFrame, date | None, int, str | None]:
        if date_column != "trade_date_dt":
            raise ValueError("unsupported optional source date column")
        latest_frame, error = await self._read_optional_dataset(
            dataset,
            "SELECT CAST(trade_date_dt AS DATE) AS source_date, COUNT(*) AS row_count "
            "FROM read_parquet(?, hive_partitioning=true, union_by_name=true) "
            "WHERE CAST(trade_date_dt AS DATE) <= ? GROUP BY source_date "
            "ORDER BY source_date DESC LIMIT 1",
            [expected],
        )
        if error:
            return pd.DataFrame(), None, 0, error
        if latest_frame.empty:
            return pd.DataFrame(), None, 0, None
        source_date = _as_date(latest_frame.iloc[0].get("source_date"))
        row_count = int(latest_frame.iloc[0].get("row_count") or 0)
        if source_date != expected:
            return pd.DataFrame(), source_date, row_count, None
        history, error = await self._read_optional_dataset(
            dataset,
            "SELECT * FROM read_parquet(?, hive_partitioning=true, union_by_name=true) "
            "WHERE CAST(trade_date_dt AS DATE) BETWEEN ? AND ?",
            [start, expected],
        )
        return history, source_date, row_count, error

    @staticmethod
    def _daily_index_returns(
        trade_day: date,
        previous_day: date | None,
        rows: dict[tuple[str, date], dict[str, object]],
        conflicts: set[tuple[str, date]],
    ) -> tuple[DailyIndexReturn, ...]:
        values: list[DailyIndexReturn] = []
        for symbol, label in CORE_INDEX_LABELS.items():
            current_key = (symbol, trade_day)
            previous_key = (symbol, previous_day) if previous_day is not None else None
            current = rows.get(current_key)
            previous = rows.get(previous_key) if previous_key is not None else None
            reason: str | None = None
            if current_key in conflicts or (
                previous_key is not None and previous_key in conflicts
            ):
                reason = "duplicate_conflict"
            elif current is None:
                reason = "missing_current"
            elif previous is None:
                reason = "missing_previous"
            current_close = _positive(current.get("close")) if current else None
            previous_close = _positive(previous.get("close")) if previous else None
            if reason is None and (current_close is None or previous_close is None):
                reason = "invalid_price"
            values.append(
                DailyIndexReturn(
                    symbol=symbol,
                    label=label,
                    trade_date=trade_day,
                    previous_trade_date=previous_day,
                    close=current_close,
                    previous_close=previous_close,
                    return_pct=(
                        (current_close / previous_close - 1.0) * 100.0
                        if reason is None
                        and current_close is not None
                        and previous_close is not None
                        else None
                    ),
                    status="fresh" if reason is None else "unavailable",
                    reason=reason,
                )
            )
        return tuple(values)

    @staticmethod
    def _daily_fact(
        member: UniverseMember,
        trade_day: date,
        previous_day: date | None,
        rows: dict[tuple[str, date], dict[str, object]],
        conflicts: set[tuple[str, date]],
    ) -> DailyStockFact:
        current_key = (member.symbol, trade_day)
        previous_key = (member.symbol, previous_day) if previous_day else None
        current = rows.get(current_key)
        previous = rows.get(previous_key) if previous_key else None
        reason: str | None = None
        if current_key in conflicts or (previous_key is not None and previous_key in conflicts):
            reason = "duplicate_conflict"
        elif current is None:
            reason = "missing_current"
        elif previous is None:
            reason = "first_listing" if member.list_date == trade_day else "missing_previous"
        elif (volume := _finite(current.get("volume"))) is None or volume <= 0:
            reason = "zero_volume" if volume == 0 else "invalid_volume"
        current_close = _positive(current.get("close")) if current else None
        previous_close = _positive(previous.get("close")) if previous else None
        if reason is None and (current_close is None or previous_close is None):
            reason = "invalid_price"
        return_pct = (
            (current_close / previous_close - 1.0) * 100.0
            if reason is None and current_close is not None and previous_close is not None
            else None
        )
        return DailyStockFact(
            symbol=member.symbol,
            name=member.name,
            exchange=member.exchange,
            industry=member.industry,
            is_st=member.is_st,
            trade_date=trade_day,
            previous_trade_date=previous_day,
            close=current_close,
            previous_close=previous_close,
            return_pct=return_pct,
            volume=_finite(current.get("volume")) if current else None,
            amount=_nonnegative(current.get("amount")) if current else None,
            exclusion_reason=reason,
        )


def _normalize_dates(values: Iterable[object], *, end: date) -> tuple[date, ...]:
    result = {_as_date(value) for value in values}
    return tuple(sorted(item for item in result if item is not None and item <= end))


def _frame_dates(frame: pd.DataFrame, column: str) -> tuple[date, ...]:
    if frame.empty or column not in frame:
        return ()
    return tuple(item for value in frame[column] if (item := _as_date(value)) is not None)


def _latest_date(frame: pd.DataFrame, column: str) -> date | None:
    dates = _frame_dates(frame, column)
    return max(dates) if dates else None


def _as_date(value: object) -> date | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return pd.Timestamp(value).date()
    except (TypeError, ValueError):
        return None


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _positive(value: object) -> float | None:
    number = _finite(value)
    return number if number is not None and number > 0 else None


def _nonnegative(value: object) -> float | None:
    number = _finite(value)
    return number if number is not None and number >= 0 else None


def _is_equity_symbol(symbol: str, exchange: str) -> bool:
    prefix, separator, suffix = symbol.partition(".")
    if separator != "." or suffix != exchange or len(prefix) != 6 or not prefix.isdigit():
        return False
    if exchange == "SH":
        return prefix.startswith("6")
    if exchange == "SZ":
        return prefix.startswith(("000", "001", "002", "003", "300", "301"))
    if exchange == "BJ":
        return prefix.startswith(("4", "8", "9"))
    return False


def _member_active_on(member: UniverseMember, trade_day: date) -> bool:
    return (
        member.list_date <= trade_day
        and (member.delist_date is None or member.delist_date > trade_day)
    )


def _normalize_daily_rows(
    frame: pd.DataFrame,
    universe: set[str],
) -> tuple[dict[tuple[str, date], dict[str, object]], set[tuple[str, date]]]:
    if frame.empty:
        return {}, set()
    values: dict[tuple[str, date], list[dict[str, object]]] = defaultdict(list)
    for row in frame.to_dict("records"):
        symbol = str(row.get("symbol") or "")
        trade_day = _as_date(row.get("trade_date"))
        if symbol in universe and trade_day is not None:
            values[(symbol, trade_day)].append(row)
    normalized: dict[tuple[str, date], dict[str, object]] = {}
    conflicts: set[tuple[str, date]] = set()
    for key, rows in values.items():
        signatures = {
            tuple(_finite(row.get(column)) for column in ("close", "volume", "amount"))
            for row in rows
        }
        if len(signatures) > 1:
            conflicts.add(key)
        else:
            normalized[key] = rows[0]
    return normalized, conflicts


def _market_breakdowns(facts: tuple[DailyStockFact, ...]) -> tuple[MarketBreakdown, ...]:
    groups: list[tuple[str, str, tuple[DailyStockFact, ...]]] = [("all", "全市场", facts)]
    for exchange in ("SH", "SZ", "BJ"):
        selected = tuple(item for item in facts if item.exchange == exchange)
        if selected:
            groups.append((exchange, exchange, selected))
    st = tuple(item for item in facts if item.is_st)
    if st:
        groups.append(("ST", "ST", st))
    result: list[MarketBreakdown] = []
    for key, label, selected in groups:
        valid = tuple(item for item in selected if item.exclusion_reason is None and item.return_pct is not None)
        returns = [item.return_pct for item in valid if item.return_pct is not None]
        result.append(
            MarketBreakdown(
                key=key,
                label=label,
                eligible=len(selected),
                valid=len(valid),
                excluded=len(selected) - len(valid),
                advance=sum(value > 0 for value in returns),
                decline=sum(value < 0 for value in returns),
                flat=sum(value == 0 for value in returns),
                median_return=float(median(returns)) if returns else None,
                amount=sum(item.amount or 0.0 for item in valid),
            )
        )
    return tuple(result)


def _exact_source_status(
    *,
    expected: date,
    source_date: date | None,
    coverage: float,
    minimum_coverage: float,
    rows: int,
) -> tuple[FreshnessStatus, str | None]:
    if source_date is None or rows == 0:
        return "unavailable", "source dataset has no rows on or before the expected date"
    if source_date != expected:
        return "stale", f"latest source date {source_date.isoformat()} is older than expected {expected.isoformat()}"
    if coverage < minimum_coverage:
        return "partial", f"valid coverage {coverage:.1%} is below {minimum_coverage:.0%}"
    return "fresh", None


def _trading_lag(expected: date, source: date | None, calendar: Sequence[date]) -> int | None:
    if source is None:
        return None
    if source >= expected:
        return 0
    dates = sorted(set(calendar))
    return sum(source < item <= expected for item in dates)


def _rows_on_date(
    frame: pd.DataFrame,
    column: str,
    target: date | None,
) -> list[dict[str, object]]:
    if target is None or frame.empty or column not in frame:
        return []
    return [
        row
        for row in frame.to_dict("records")
        if _as_date(row.get(column)) == target
    ]


def _dated_freshness(
    source: str,
    expected: date,
    source_date: date | None,
    row_count: int,
    calendar: Sequence[date],
) -> SourceFreshness:
    if source_date is None:
        status: FreshnessStatus = "unavailable"
        reason = "dataset is unavailable or empty"
    elif source_date == expected:
        status = "fresh"
        reason = None
    else:
        status = "stale"
        reason = (
            f"latest source date {source_date.isoformat()} is older than expected "
            f"{expected.isoformat()}"
        )
    return SourceFreshness(
        source=source,
        status=status,
        expected_date=expected,
        source_date=source_date,
        lag_trading_days=_trading_lag(expected, source_date, calendar),
        row_count=row_count,
        coverage=None,
        reason=reason,
    )


def _unavailable_freshness(
    source: str,
    expected: date | datetime,
    reason: str | None,
) -> SourceFreshness:
    return SourceFreshness(
        source=source,
        status="unavailable",
        expected_date=expected,
        source_date=None,
        lag_trading_days=None,
        row_count=0,
        coverage=0.0,
        reason=reason or "optional source is unavailable",
    )


def _optional_schema_error(
    frame: pd.DataFrame,
    *,
    source: str,
    required: Sequence[str],
    any_of: Sequence[str],
) -> str | None:
    columns = set(frame.columns)
    missing = [column for column in required if column not in columns]
    if missing or not columns.intersection(any_of):
        return f"{source} schema is missing required market radar fields"
    return None


def _positive_int(value: object) -> int | None:
    number = _finite(value)
    if number is None or number < 1 or not number.is_integer():
        return None
    return int(number)


def _text_or_none(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _ladder_row(
    symbol: str,
    board_count: int,
    primary: dict[str, object],
    detail: dict[str, object] | None,
    member: UniverseMember | None,
) -> LimitLadderRow:
    detail = detail or {}
    return LimitLadderRow(
        symbol=symbol,
        name=(
            _text_or_none(primary.get("name"))
            or _text_or_none(detail.get("name"))
            or (member.name if member else None)
        ),
        industry=_text_or_none(detail.get("industry")) or (member.industry if member else None),
        is_st=bool(member.is_st) if member else False,
        board_count=board_count,
        pct_change=_finite(detail.get("pct_chg")),
        turnover_ratio=_nonnegative(detail.get("turnover_ratio")),
        limit_times=_positive_int(detail.get("limit_times")),
        amount=_nonnegative(detail.get("amount")),
        seal_amount=_nonnegative(detail.get("fd_amount")),
        first_time=_text_or_none(detail.get("first_time")),
        last_time=_text_or_none(detail.get("last_time")),
        open_times=(
            int(value)
            if (value := _nonnegative(detail.get("open_times"))) is not None
            and value.is_integer()
            else None
        ),
    )


def _up_limit_dates(frame: pd.DataFrame) -> set[tuple[str, date]]:
    if frame.empty:
        return set()
    return {
        (symbol, trade_day)
        for row in frame.to_dict("records")
        if str(row.get("limit") or "").upper() == "U"
        and (symbol := str(row.get("symbol") or row.get("ts_code") or ""))
        and (trade_day := _as_date(row.get("trade_date_dt"))) is not None
    }


def _consecutive_limit_ups(
    symbol: str,
    target: date,
    calendar: Sequence[date],
    up_history: set[tuple[str, date]],
) -> int:
    count = 0
    for trade_day in reversed([item for item in calendar if item <= target]):
        if (symbol, trade_day) not in up_history:
            break
        count += 1
    return count


def _aggregate_daily_inputs(
    frame: pd.DataFrame,
    universe: tuple[UniverseMember, ...],
    trading_dates: Sequence[date],
    include_correlation: bool = True,
) -> tuple[
    dict[str, dict[date, float]],
    dict[date, dict[str, dict[str, float]]],
]:
    """Compute all daily crowding/sector facts in one blocking Pandas pass."""
    rows, conflicts = _normalize_daily_rows(frame, {item.symbol for item in universe})
    panels: dict[date, tuple[DailyStockFact, ...]] = {}
    for index, trade_day in enumerate(trading_dates):
        previous = trading_dates[index - 1] if index else None
        panels[trade_day] = tuple(
            MarketRadarDataService._daily_fact(
                member,
                trade_day,
                previous,
                rows,
                conflicts,
            )
            for member in universe
            if _member_active_on(member, trade_day)
        )

    series: dict[str, dict[date, float]] = {
        key: {}
        for key in (
            "coverage",
            "top_1_amount_share",
            "top_5_amount_share",
            "top_3_sector_share",
            "market_amount_vs_20d",
            "high_liquidity_correlation",
            "high_liquidity_correlation_pair_coverage",
        )
    }
    sectors_by_date: dict[date, dict[str, dict[str, float]]] = {}
    totals: dict[date, float] = {}
    returns_by_date: dict[date, dict[str, float]] = {}
    amounts_by_date: dict[date, dict[str, float]] = {}

    for trade_day in trading_dates:
        valid = tuple(
            item
            for item in panels[trade_day]
            if item.exclusion_reason is None
            and item.return_pct is not None
            and item.amount is not None
        )
        eligible_count = len(panels[trade_day])
        series["coverage"][trade_day] = len(valid) / eligible_count if eligible_count else 0.0
        if not valid:
            continue
        amounts = sorted((item.amount or 0.0 for item in valid), reverse=True)
        total = sum(amounts)
        if total <= 0:
            continue
        totals[trade_day] = total
        returns_by_date[trade_day] = {
            item.symbol: float(item.return_pct) for item in valid if item.return_pct is not None
        }
        amounts_by_date[trade_day] = {
            item.symbol: float(item.amount) for item in valid if item.amount is not None
        }
        top_one_count = max(1, math.ceil(len(amounts) * 0.01))
        top_five_count = max(1, math.ceil(len(amounts) * 0.05))
        series["top_1_amount_share"][trade_day] = sum(amounts[:top_one_count]) / total
        series["top_5_amount_share"][trade_day] = sum(amounts[:top_five_count]) / total

        sector_members: dict[str, list[DailyStockFact]] = defaultdict(list)
        for item in valid:
            if item.industry:
                sector_members[item.industry].append(item)
        sector_amounts = {
            industry: sum(item.amount or 0.0 for item in members)
            for industry, members in sector_members.items()
        }
        series["top_3_sector_share"][trade_day] = (
            sum(sorted(sector_amounts.values(), reverse=True)[:3]) / total
        )
        sectors_by_date[trade_day] = {
            industry: {
                "median_return": float(median(
                    item.return_pct for item in members if item.return_pct is not None
                )),
                "advance_ratio": sum((item.return_pct or 0.0) > 0 for item in members)
                / len(members),
                "amount": sector_amounts[industry],
                "amount_share": sector_amounts[industry] / total,
                "stock_count": float(len(members)),
            }
            for industry, members in sector_members.items()
        }

    ordered_dates = list(trading_dates)
    symbol_columns = sorted({member.symbol for member in universe})
    returns_frame = pd.DataFrame.from_dict(returns_by_date, orient="index").reindex(
        index=ordered_dates,
        columns=symbol_columns,
    )
    amounts_frame = pd.DataFrame.from_dict(amounts_by_date, orient="index").reindex(
        index=ordered_dates,
        columns=symbol_columns,
    )
    for index, trade_day in enumerate(ordered_dates):
        total = totals.get(trade_day)
        previous_totals = [
            totals[item]
            for item in ordered_dates[max(0, index - 20) : index]
            if item in totals
        ]
        if total is not None and len(previous_totals) >= 20:
            mean_total = sum(previous_totals) / len(previous_totals)
            if mean_total > 0:
                series["market_amount_vs_20d"][trade_day] = total / mean_total

        if not include_correlation:
            continue
        window_start = index - 19
        if window_start < 0:
            continue
        window_amounts = amounts_frame.iloc[window_start : index + 1]
        amount_counts = window_amounts.count()
        amount_means = window_amounts.mean()
        active_symbols = {
            member.symbol for member in universe if _member_active_on(member, trade_day)
        }
        top_symbols = [
            symbol
            for symbol, _ in sorted(
                (
                    (symbol, float(amount_means[symbol]))
                    for symbol in symbol_columns
                    if symbol in active_symbols
                    and int(amount_counts[symbol]) >= 15
                    and _finite(amount_means[symbol]) is not None
                ),
                key=lambda item: (-item[1], item[0]),
            )[:300]
        ]
        if len(top_symbols) < 2:
            continue
        correlations = returns_frame.iloc[window_start : index + 1][top_symbols].corr(
            min_periods=15
        )
        correlation, pair_coverage = _correlation_upper_triangle(correlations)
        series["high_liquidity_correlation_pair_coverage"][trade_day] = pair_coverage
        if correlation is not None and pair_coverage >= 0.8:
            series["high_liquidity_correlation"][trade_day] = correlation
    return series, sectors_by_date


def _correlation_upper_triangle(correlations: pd.DataFrame) -> tuple[float | None, float]:
    matrix = correlations.to_numpy(copy=False)
    expected_pairs = len(matrix) * (len(matrix) - 1) // 2
    if expected_pairs == 0:
        return None, 0.0
    row_indices, column_indices = np.triu_indices_from(matrix, k=1)
    upper = matrix[row_indices, column_indices]
    finite = upper[np.isfinite(upper)]
    pair_coverage = len(finite) / expected_pairs
    if len(finite) == 0:
        return None, pair_coverage
    return sum(finite.tolist()) / len(finite), pair_coverage


def _z_score(value: float, history: Sequence[float]) -> float | None:
    finite_history = [item for raw in history if (item := _finite(raw)) is not None]
    if not finite_history:
        return None
    mean = sum(finite_history) / len(finite_history)
    variance = sum((item - mean) ** 2 for item in finite_history) / len(finite_history)
    if variance <= 1e-24:
        return 0.0
    return (value - mean) / math.sqrt(variance)

"""Cached SQLite/Parquet enrichment for market-radar intraday observations."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta
from statistics import mean, pstdev

import pandas as pd
from sqlalchemy import bindparam, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.blocking import run_blocking
from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.models.sentiment import SentimentPost
from app.db.models.stock import Stock
from app.services.market_radar_calculator import QuoteTick
from app.services.market_radar_contracts import (
    EligibleUniverse,
    IntradaySymbolContext,
    MetricValue,
)

_DAILY_VOLUME_TO_SHARES = 100.0
_CONTEXT_KEYS = (
    "volume_ratio_20d",
    "down_limit_price",
    "up_limit_price",
    "negative_heat_z20",
    "weighted_sentiment",
)


class MarketRadarIntradayContextLoader:
    """Load static baselines in batches; raw ticks remain in memory only."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        market_store: ParquetMarketDataStore | None = None,
        clock: Callable[[], datetime] = datetime.now,
    ) -> None:
        self._session = session
        self._market_store = market_store or ParquetMarketDataStore()
        self._clock = clock
        self._baseline_date: date | None = None
        self._baseline_symbols: frozenset[str] = frozenset()
        self._volume_baselines: dict[str, float] = {}
        self._limit_prices: dict[str, tuple[float, float]] = {}
        self._sentiment_cached_at: datetime | None = None
        self._sentiment_symbols: frozenset[str] = frozenset()
        self._sentiment_cache: dict[str, dict[str, MetricValue]] = {}

    async def load_eligible_universe(self) -> EligibleUniverse:
        now = self._clock()
        today = now.date()
        result = await self._session.execute(
            select(Stock.symbol, Stock.exchange).where(
                Stock.is_delist != 1,
                Stock.is_suspend != 1,
                or_(Stock.list_date.is_(None), Stock.list_date <= today),
                or_(Stock.delist_date.is_(None), Stock.delist_date > today),
            )
        )
        symbols = tuple(
            sorted(
                symbol
                for raw_symbol, raw_exchange in result.all()
                if (
                    symbol := _equity_symbol(
                        str(raw_symbol or ""),
                        str(raw_exchange or ""),
                    )
                )
                is not None
            )
        )
        return EligibleUniverse(
            symbols=symbols,
            status="fresh" if symbols else "unavailable",
            as_of=now,
            source="sqlite_stocks",
            reason=None if symbols else "SQLite has no active non-suspended A-share universe",
        )

    async def load_symbol_context(
        self,
        symbols: tuple[str, ...],
        ticks: Mapping[str, QuoteTick],
        as_of: datetime,
    ) -> Mapping[str, IntradaySymbolContext]:
        ordered = tuple(dict.fromkeys(symbols))
        if not ordered:
            return {}
        await self._refresh_static_baselines(ordered, as_of.date())
        requested = frozenset(ordered)
        if (
            self._sentiment_cached_at is not None
            and requested.issubset(self._sentiment_symbols)
            and 0 <= (as_of - self._sentiment_cached_at).total_seconds() < 30
        ):
            sentiment = self._sentiment_cache
        else:
            sentiment = await self._load_symbol_sentiment(ordered, as_of)
            self._sentiment_cache = sentiment
            self._sentiment_symbols = requested
            self._sentiment_cached_at = as_of
        result: dict[str, IntradaySymbolContext] = {}
        for symbol in ordered:
            tick = ticks.get(symbol)
            baseline = self._volume_baselines.get(symbol)
            ratio = (
                float(tick.volume) / baseline
                if tick is not None
                and _positive(tick.volume) is not None
                and baseline is not None
                and baseline > 0
                else None
            )
            prices = self._limit_prices.get(symbol)
            sentiment_metrics = sentiment.get(symbol, {})
            result[symbol] = IntradaySymbolContext(
                metrics={
                    "volume_ratio_20d": MetricValue(
                        ratio,
                        "fresh" if ratio is not None else "unavailable",
                        as_of,
                        "klines_daily_20d",
                        baseline=baseline,
                        reason=(
                            None
                            if ratio is not None
                            else "20 complete daily volume observations or current volume are unavailable"
                        ),
                    ),
                    "down_limit_price": _limit_metric(prices, 1, as_of),
                    "up_limit_price": _limit_metric(prices, 0, as_of),
                    "negative_heat_z20": sentiment_metrics.get(
                        "negative_heat_z20",
                        _unavailable_metric(
                            "sentiment_posts",
                            as_of,
                            "20-day symbol negative-heat history is unavailable",
                        ),
                    ),
                    "weighted_sentiment": sentiment_metrics.get(
                        "weighted_sentiment",
                        _unavailable_metric(
                            "sentiment_posts",
                            as_of,
                            "fresh symbol sentiment is unavailable",
                        ),
                    ),
                }
            )
        return result

    async def _refresh_static_baselines(
        self,
        symbols: Sequence[str],
        target_date: date,
    ) -> None:
        requested = frozenset(symbols)
        if self._baseline_date == target_date and requested.issubset(self._baseline_symbols):
            return
        daily = await run_blocking(
            self._market_store.load_daily,
            list(symbols),
            target_date - timedelta(days=60),
            target_date - timedelta(days=1),
            ["symbol", "trade_date", "volume", "close", "amount"],
        )
        self._volume_baselines = _volume_baselines(daily)
        self._limit_prices = await self._load_limit_prices(symbols, target_date)
        self._baseline_date = target_date
        self._baseline_symbols = requested

    async def _load_limit_prices(
        self,
        symbols: Sequence[str],
        target_date: date,
    ) -> dict[str, tuple[float, float]]:
        table = (
            await self._session.execute(
                text(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='stock_limit_prices' LIMIT 1"
                )
            )
        ).scalar_one_or_none()
        if table is None:
            return {}
        statement = text(
            "SELECT symbol, up_limit, down_limit FROM stock_limit_prices "
            "WHERE symbol IN :symbols AND trade_date = :trade_date"
        ).bindparams(bindparam("symbols", expanding=True))
        rows = (
            await self._session.execute(
                statement,
                {"symbols": list(symbols), "trade_date": target_date.isoformat()},
            )
        ).all()
        result: dict[str, tuple[float, float]] = {}
        for symbol, raw_up, raw_down in rows:
            up = _positive(raw_up)
            down = _positive(raw_down)
            if up is not None and down is not None:
                result[str(symbol)] = (up, down)
        return result

    async def _load_symbol_sentiment(
        self,
        symbols: Sequence[str],
        as_of: datetime,
    ) -> dict[str, dict[str, MetricValue]]:
        start = as_of - timedelta(days=21)
        result = await self._session.execute(
            select(
                SentimentPost.symbol,
                SentimentPost.sentiment_score,
                SentimentPost.published_at,
                SentimentPost.reply_count,
                SentimentPost.like_count,
                SentimentPost.comment_count,
            ).where(
                SentimentPost.symbol.in_(symbols),
                SentimentPost.sentiment_score.is_not(None),
                SentimentPost.published_at.is_not(None),
                SentimentPost.published_at >= start,
                SentimentPost.published_at <= as_of,
            )
        )
        grouped: dict[str, list[tuple[float, datetime, int]]] = defaultdict(list)
        for symbol, raw_score, published_at, replies, likes, comments in result.all():
            score = _finite(raw_score)
            if score is None or published_at is None:
                continue
            engagement = sum(max(0, int(value or 0)) for value in (replies, likes, comments))
            grouped[str(symbol)].append((score, published_at, engagement))

        cutoff = as_of - timedelta(hours=6)
        output: dict[str, dict[str, MetricValue]] = {}
        for symbol, rows in grouped.items():
            current = [row for row in rows if row[1] >= cutoff]
            weight = sum(1.0 + math.log1p(row[2]) for row in current)
            weighted = (
                sum(row[0] * (1.0 + math.log1p(row[2])) for row in current) / weight
                if weight > 0
                else None
            )
            history_days = tuple(
                as_of.date() - timedelta(days=offset) for offset in range(20, 0, -1)
            )
            history_by_day = {day: 0.0 for day in history_days}
            current_negative = 0.0
            for score, published_at, engagement in rows:
                if score >= 0:
                    continue
                heat = math.log1p(1 + engagement)
                if published_at.date() == as_of.date():
                    current_negative += heat
                elif published_at.date() in history_by_day:
                    history_by_day[published_at.date()] += heat
            history = tuple(history_by_day[day] for day in history_days)
            heat_z = _z_score(current_negative, history)
            latest = max((row[1] for row in current), default=as_of)
            output[symbol] = {
                "weighted_sentiment": MetricValue(
                    weighted,
                    "fresh" if weighted is not None else "unavailable",
                    latest,
                    "sentiment_posts",
                    reason=None if weighted is not None else "no sentiment posts in six hours",
                ),
                "negative_heat_z20": MetricValue(
                    heat_z,
                    "fresh" if heat_z is not None else "unavailable",
                    latest,
                    "sentiment_posts",
                    baseline=mean(history) if history else None,
                    reason=None
                    if heat_z is not None
                    else "20-day negative-heat history is incomplete",
                ),
            }
        return output


def _volume_baselines(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty or not {"symbol", "volume", "close", "amount"}.issubset(frame.columns):
        return {}
    result: dict[str, float] = {}
    for symbol, group in frame.groupby("symbol", sort=False):
        values: list[float] = []
        for row in group.sort_index().itertuples():
            volume = _positive(getattr(row, "volume", None))
            close = _positive(getattr(row, "close", None))
            amount = _positive(getattr(row, "amount", None))
            if volume is None or close is None or amount is None:
                continue
            implied_unit = amount / (volume * close)
            multiplier = _DAILY_VOLUME_TO_SHARES if implied_unit > 20 else 1.0
            values.append(volume * multiplier)
        values = values[-20:]
        if len(values) == 20:
            result[str(symbol)] = mean(values)
    return result


def _limit_metric(
    prices: tuple[float, float] | None,
    index: int,
    as_of: datetime,
) -> MetricValue:
    value = prices[index] if prices is not None else None
    label = "down" if index == 1 else "up"
    return MetricValue(
        value,
        "fresh" if value is not None else "unavailable",
        as_of,
        "stock_limit_prices",
        reason=None if value is not None else f"exact {label}-limit price is unavailable",
    )


def _unavailable_metric(source: str, as_of: datetime, reason: str) -> MetricValue:
    return MetricValue(None, "unavailable", as_of, source, reason=reason)


def _equity_symbol(symbol: str, exchange: str) -> str | None:
    code, separator, suffix = symbol.upper().partition(".")
    market = exchange.upper() or suffix
    if separator != "." or suffix != market or len(code) != 6 or not code.isdigit():
        return None
    valid = (
        (market == "SH" and code.startswith("6"))
        or (market == "SZ" and code.startswith(("000", "001", "002", "003", "300", "301")))
        or (market == "BJ" and code.startswith(("4", "8", "9")))
    )
    return f"{code}.{market}" if valid else None


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _positive(value: object) -> float | None:
    number = _finite(value)
    return number if number is not None and number > 0 else None


def _z_score(value: float, history: Sequence[float]) -> float | None:
    deviation = pstdev(history) if len(history) >= 2 else 0.0
    if deviation <= 0:
        return 0.0 if value == mean(history) else None
    return (value - mean(history)) / deviation

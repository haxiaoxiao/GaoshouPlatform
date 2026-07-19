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
from app.db.models.sentiment import SentimentAnalysis
from app.db.models.stock import Stock
from app.services.market_radar import (
    EligibleUniverse,
    IntradaySymbolContext,
    MetricValue,
)
from app.services.market_radar_calculator import QuoteTick

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
                            "sentiment_analysis",
                            as_of,
                            "20-day symbol negative-heat history is unavailable",
                        ),
                    ),
                    "weighted_sentiment": sentiment_metrics.get(
                        "weighted_sentiment",
                        _unavailable_metric(
                            "sentiment_analysis",
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
            ["symbol", "trade_date", "volume"],
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
                SentimentAnalysis.symbol,
                SentimentAnalysis.score,
                SentimentAnalysis.confidence,
                SentimentAnalysis.analyzed_at,
            ).where(
                SentimentAnalysis.symbol.in_(symbols),
                SentimentAnalysis.score.is_not(None),
                SentimentAnalysis.analyzed_at >= start,
                SentimentAnalysis.analyzed_at <= as_of,
            )
        )
        grouped: dict[str, list[tuple[float, float, datetime]]] = defaultdict(list)
        for symbol, raw_score, raw_confidence, analyzed_at in result.all():
            score = _finite(raw_score)
            confidence = _finite(raw_confidence)
            if score is None or confidence is None or confidence <= 0:
                continue
            grouped[str(symbol)].append((score, confidence, analyzed_at))

        cutoff = as_of - timedelta(hours=6)
        output: dict[str, dict[str, MetricValue]] = {}
        for symbol, rows in grouped.items():
            current = [row for row in rows if row[2] >= cutoff]
            weight = sum(row[1] for row in current)
            weighted = sum(row[0] * row[1] for row in current) / weight if weight > 0 else None
            current_negative = sum(row[1] for row in current if row[0] < 0)
            history_by_day: dict[date, float] = defaultdict(float)
            for score, confidence, analyzed_at in rows:
                if analyzed_at < cutoff and score < 0:
                    history_by_day[analyzed_at.date()] += confidence
            history = tuple(history_by_day[day] for day in sorted(history_by_day))[-20:]
            heat_z = _z_score(current_negative, history) if len(history) == 20 else None
            latest = max((row[2] for row in current), default=as_of)
            output[symbol] = {
                "weighted_sentiment": MetricValue(
                    weighted,
                    "fresh" if weighted is not None else "unavailable",
                    latest,
                    "sentiment_analysis",
                    reason=None if weighted is not None else "no sentiment analysis in six hours",
                ),
                "negative_heat_z20": MetricValue(
                    heat_z,
                    "fresh" if heat_z is not None else "unavailable",
                    latest,
                    "sentiment_analysis",
                    baseline=mean(history) if history else None,
                    reason=None
                    if heat_z is not None
                    else "20-day negative-heat history is incomplete",
                ),
            }
        return output


def _volume_baselines(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty or "symbol" not in frame.columns or "volume" not in frame.columns:
        return {}
    result: dict[str, float] = {}
    for symbol, group in frame.groupby("symbol", sort=False):
        values = [
            value * _DAILY_VOLUME_TO_SHARES
            for raw in group.sort_index()["volume"].tolist()
            if (value := _positive(raw)) is not None
        ][-20:]
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

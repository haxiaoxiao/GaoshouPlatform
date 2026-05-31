"""Synchronize sparse timer-minute bars into ClickHouse before backtests."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Iterable

import pandas as pd
from loguru import logger

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.db.clickhouse import get_ch_client
from app.engines.qmt_gateway import qmt_gateway
from app.services.backtest_redis_cache import get_backtest_cache
from app.services.index_components import normalize_index_symbol
from app.services.security_symbols import normalize_security_symbol

DEFAULT_TIMER_TIMES = ("10:00", "10:30", "14:30", "14:50")
MARKET_BAR_START = time(9, 30)
MARKET_BAR_END = time(15, 0)

ProgressCallback = Callable[[dict[str, Any]], None]


def parse_timer_times(values: str | Iterable[str] | None) -> tuple[time, ...]:
    if values is None:
        items = DEFAULT_TIMER_TIMES
    elif isinstance(values, str):
        items = [item.strip() for item in values.split(",")]
    else:
        items = [str(item).strip() for item in values]

    parsed: list[time] = []
    seen: set[time] = set()
    for item in items:
        if not item:
            continue
        text = item
        if len(text) == 5:
            text = f"{text}:00"
        value = datetime.strptime(text, "%H:%M:%S").time().replace(second=0, microsecond=0)
        if value not in seen:
            seen.add(value)
            parsed.append(value)
    return tuple(parsed)


def timer_times_from_params(params: dict[str, Any] | None) -> tuple[time, ...]:
    params = params or {}
    raw = (
        params.get("timer_times")
        or params.get("timer_minute_times")
        or params.get("minute_timer_times")
    )
    return parse_timer_times(raw)


def timer_times_to_strings(values: Iterable[time]) -> tuple[str, ...]:
    return tuple(value.strftime("%H:%M:%S") for value in values)


def _bar_timer_times(timer_times: tuple[time, ...]) -> tuple[time, ...]:
    return tuple(t for t in timer_times if MARKET_BAR_START <= t <= MARKET_BAR_END)


def _normalize_symbol(symbol: str) -> str:
    return normalize_security_symbol(symbol) or ""


def _should_use_clickhouse() -> bool:
    return settings.clickhouse_enabled or settings.market_data_backend == "clickhouse"


def _minute_strings(timer_times: tuple[time, ...]) -> tuple[str, ...]:
    return tuple(t.strftime("%H:%M:%S") for t in _bar_timer_times(timer_times))


def _index_symbols(index_symbol: str, start: date, end: date) -> list[str]:
    db_path = settings.sqlite_db_path
    normalized = normalize_index_symbol(index_symbol) or _normalize_symbol(index_symbol)
    jq_symbol = normalized.replace(".SH", ".XSHG").replace(".SZ", ".XSHE")

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT symbol
            FROM index_components
            WHERE (index_symbol IN (?, ?) OR jq_index_symbol IN (?, ?))
              AND trade_date >= COALESCE((
                  SELECT MAX(trade_date)
                  FROM index_components
                  WHERE (index_symbol IN (?, ?) OR jq_index_symbol IN (?, ?))
                    AND trade_date <= ?
              ), ?)
              AND trade_date <= ?
            ORDER BY symbol
            """,
            (
                normalized,
                jq_symbol,
                normalized,
                jq_symbol,
                normalized,
                jq_symbol,
                normalized,
                jq_symbol,
                start.isoformat(),
                start.isoformat(),
                end.isoformat(),
            ),
        ).fetchall()
    return [str(r[0]) for r in rows]


def _existing_timer_keys(
    symbol: str,
    start: date,
    end: date,
    timer_times: tuple[time, ...],
) -> set[tuple[date, int]]:
    timer_times = _bar_timer_times(timer_times)
    timer_minutes = tuple(t.hour * 60 + t.minute for t in timer_times)
    if not timer_minutes:
        return set()
    if not _should_use_clickhouse():
        store = get_market_data_store()
        start_dt = datetime.combine(start, datetime.min.time())
        end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())
        df = store.load_minute([symbol], start_dt, end_dt, timer_times=_minute_strings(timer_times))
        if df.empty:
            return set()
        values = set()
        for dt in df.index:
            ts = pd.Timestamp(dt)
            values.add((ts.date(), ts.hour * 60 + ts.minute))
        return values
    ch = get_ch_client()
    try:
        rows = ch.execute(
            """
            SELECT toDate(datetime) AS d, toHour(datetime) * 60 + toMinute(datetime) AS m
            FROM klines_minute
            WHERE symbol = %(symbol)s
              AND toDate(datetime) >= %(start)s
              AND toDate(datetime) <= %(end)s
              AND (toHour(datetime) * 60 + toMinute(datetime)) IN %(timer_minutes)s
            GROUP BY d, m
            """,
            {
                "symbol": symbol,
                "start": start,
                "end": end,
                "timer_minutes": timer_minutes,
            },
        )
    finally:
        try:
            ch.disconnect()
        except Exception:
            pass
    return {(r[0], int(r[1])) for r in rows}


def _expected_timer_point_count(
    symbol: str,
    start: date,
    end: date,
    timer_times: tuple[time, ...],
) -> int:
    if not timer_times:
        return 0
    timer_times = _bar_timer_times(timer_times)
    if not timer_times:
        return 0
    if not _should_use_clickhouse():
        store = get_market_data_store()
        df = store.load_daily([symbol], start, end)
        if not df.empty and {"open", "close"}.issubset(df.columns):
            df = df[(df["open"].astype(float) > 0) & (df["close"].astype(float) > 0)]
            trading_days = len(df.index.normalize().unique())
        else:
            trading_days = 0
        if trading_days <= 0:
            current = start
            while current <= end:
                if current.weekday() < 5:
                    trading_days += 1
                current += timedelta(days=1)
        return trading_days * len(timer_times)
    ch = get_ch_client()
    try:
        rows = ch.execute(
            """
            SELECT count()
            FROM klines_daily
            WHERE symbol = %(symbol)s
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
              AND open > 0
              AND close > 0
            """,
            {"symbol": symbol, "start": start, "end": end},
        )
    finally:
        try:
            ch.disconnect()
        except Exception:
            pass
    trading_days = int(rows[0][0]) if rows else 0
    if trading_days <= 0:
        current = start
        while current <= end:
            if current.weekday() < 5:
                trading_days += 1
            current += timedelta(days=1)
    return trading_days * len(timer_times)


def _missing_timer_month_ranges(
    symbol: str,
    start: date,
    end: date,
    timer_times: tuple[time, ...],
    existing_keys: set[tuple[date, int]],
) -> list[tuple[date, date]]:
    timer_times = _bar_timer_times(timer_times)
    if not timer_times:
        return []

    if not _should_use_clickhouse():
        store = get_market_data_store()
        df = store.load_daily([symbol], start, end)
        if not df.empty and {"open", "close"}.issubset(df.columns):
            df = df[(df["open"].astype(float) > 0) & (df["close"].astype(float) > 0)]
            trading_days = [pd.Timestamp(v).date() for v in df.index.unique()]
        else:
            trading_days = []
    else:
        ch = get_ch_client()
        try:
            rows = ch.execute(
                """
                SELECT trade_date
                FROM klines_daily
                WHERE symbol = %(symbol)s
                  AND trade_date >= %(start)s
                  AND trade_date <= %(end)s
                  AND open > 0
                  AND close > 0
                ORDER BY trade_date
                """,
                {"symbol": symbol, "start": start, "end": end},
            )
        finally:
            try:
                ch.disconnect()
            except Exception:
                pass
        trading_days = [r[0] for r in rows]
    if not trading_days:
        current = start
        while current <= end:
            if current.weekday() < 5:
                trading_days.append(current)
            current += timedelta(days=1)

    wanted_minutes = tuple(t.hour * 60 + t.minute for t in timer_times)
    missing_days = []
    for trading_day in trading_days:
        if any((trading_day, minute) not in existing_keys for minute in wanted_minutes):
            missing_days.append(trading_day)

    if not missing_days:
        return []

    ranges: list[tuple[date, date]] = []
    range_start = range_end = missing_days[0]
    for trading_day in missing_days[1:]:
        same_month = trading_day.year == range_end.year and trading_day.month == range_end.month
        if same_month:
            range_end = trading_day
        else:
            ranges.append((range_start, range_end))
            range_start = range_end = trading_day
    ranges.append((range_start, range_end))
    return ranges


def _filter_timer_rows(
    klines: list[Any],
    timer_times: tuple[time, ...],
    existing_keys: set[tuple[date, int]],
) -> list[dict[str, Any]]:
    wanted = {t.hour * 60 + t.minute for t in timer_times}
    seen: set[tuple[str, datetime]] = set()
    rows = []
    for kline in klines:
        dt = kline.datetime if isinstance(kline.datetime, datetime) else datetime.fromisoformat(str(kline.datetime))
        dt = dt.replace(second=0, microsecond=0)
        minute = dt.hour * 60 + dt.minute
        if minute not in wanted:
            continue
        if (dt.date(), minute) in existing_keys:
            continue
        symbol = _normalize_symbol(str(kline.symbol))
        key = (symbol, dt)
        if key in seen:
            continue
        seen.add(key)
        if not all(float(v or 0) > 0 for v in (kline.open, kline.high, kline.low, kline.close)):
            continue
        rows.append(
            {
                "symbol": symbol,
                "datetime": dt,
                "open": float(kline.open),
                "high": float(kline.high),
                "low": float(kline.low),
                "close": float(kline.close),
                "volume": int(kline.volume or 0),
                "amount": float(kline.amount or 0),
            }
        )
    return rows


def _insert_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    if _should_use_clickhouse():
        ch = get_ch_client()
        try:
            ch.execute(
                """
                INSERT INTO klines_minute
                (symbol, datetime, open, high, low, close, volume, amount)
                VALUES
                """,
                rows,
            )
        finally:
            try:
                ch.disconnect()
            except Exception:
                pass
    if settings.market_data_backend == "parquet":
        try:
            import pandas as pd
            df = pd.DataFrame(rows)
            store = get_market_data_store()
            store.write_minute(df, dataset="klines_minute_timer")
        except Exception as exc:
            logger.warning("Parquet timer minute write failed: {}", exc)


async def sync_symbol_timer_minutes(
    symbol: str,
    start: date,
    end: date,
    timer_times: tuple[time, ...],
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    existing = await asyncio.to_thread(_existing_timer_keys, symbol, start, end, timer_times)
    expected = await asyncio.to_thread(_expected_timer_point_count, symbol, start, end, timer_times)
    if expected > 0 and len(existing) >= expected:
        return {
            "symbol": symbol,
            "fetched": 0,
            "inserted": 0,
            "existing_points": len(existing),
            "expected_points": expected,
            "skipped": True,
        }
    ranges = await asyncio.to_thread(
        _missing_timer_month_ranges,
        symbol,
        start,
        end,
        timer_times,
        existing,
    )
    klines = []
    for chunk_start, chunk_end in ranges:
        fetch_task = qmt_gateway.get_kline_minute(symbol, chunk_start, chunk_end)
        if timeout_seconds and timeout_seconds > 0:
            klines.extend(await asyncio.wait_for(fetch_task, timeout=timeout_seconds))
        else:
            klines.extend(await fetch_task)
    rows = _filter_timer_rows(klines, timer_times, existing)
    await asyncio.to_thread(_insert_rows, rows)
    return {
        "symbol": symbol,
        "fetched": len(klines),
        "inserted": len(rows),
        "existing_points": len(existing),
        "expected_points": expected,
        "skipped": False,
    }


def find_earliest_timer_coverage_date(
    *,
    symbols: Iterable[str] | None = None,
    index_symbol: str | None = None,
    start: date,
    end: date,
    timer_times: tuple[time, ...] | None = None,
    min_symbol_coverage: float = 0.9,
    min_point_coverage: float = 0.75,
) -> dict[str, Any]:
    timer_times = _bar_timer_times(timer_times or parse_timer_times(None))
    all_symbols = [_normalize_symbol(s) for s in (symbols or []) if str(s).strip()]
    if index_symbol:
        all_symbols.extend(_index_symbols(index_symbol, start, end))
    all_symbols = sorted(set(all_symbols))
    redis_key = _timer_coverage_cache_key(
        all_symbols,
        index_symbol,
        start,
        end,
        timer_times,
        min_symbol_coverage,
        min_point_coverage,
    )
    redis_cache = get_backtest_cache()
    cached = redis_cache.get_json(redis_key) if redis_key else None
    if isinstance(cached, dict):
        cached["cache_hit"] = True
        return cached
    if not all_symbols or not timer_times:
        return {
            "earliest_date": None,
            "symbols": len(all_symbols),
            "timer_times": [t.strftime("%H:%M") for t in timer_times],
            "coverage": [],
            "cache_hit": False,
        }

    timer_minutes = tuple(t.hour * 60 + t.minute for t in timer_times)
    required_symbols = max(1, int(len(all_symbols) * min_symbol_coverage))
    if not _should_use_clickhouse():
        store = get_market_data_store()
        df = store.load_minute(
            all_symbols,
            datetime.combine(start, datetime.min.time()),
            datetime.combine(end + timedelta(days=1), datetime.min.time()),
            timer_times=_minute_strings(timer_times),
        )
        rows = []
        if not df.empty:
            temp = df.reset_index()
            temp["d"] = pd.to_datetime(temp["datetime"]).dt.date
            grouped = temp.groupby("d").agg(
                symbol_count=("symbol", "nunique"),
                point_count=("symbol", "count"),
            )
            grouped = grouped[grouped["symbol_count"] >= required_symbols].sort_index()
            rows = [
                (idx, int(row.symbol_count), int(row.point_count))
                for idx, row in grouped.iterrows()
            ]
    else:
        ch = get_ch_client()
        try:
            rows = ch.execute(
                """
                SELECT
                    toDate(datetime) AS d,
                    uniqExact(symbol) AS symbol_count,
                    count() AS point_count
                FROM klines_minute
                WHERE symbol IN %(symbols)s
                  AND datetime >= %(start)s
                  AND datetime < %(end_plus)s
                  AND (toHour(datetime) * 60 + toMinute(datetime)) IN %(timer_minutes)s
                GROUP BY d
                HAVING symbol_count >= %(required_symbols)s
                ORDER BY d
                """,
                {
                    "symbols": tuple(all_symbols),
                    "start": datetime.combine(start, datetime.min.time()),
                    "end_plus": datetime.combine(end + timedelta(days=1), datetime.min.time()),
                    "timer_minutes": timer_minutes,
                    "required_symbols": required_symbols,
                },
            )
        finally:
            try:
                ch.disconnect()
            except Exception:
                pass

    coverage = []
    earliest = None
    for d, symbol_count, point_count in rows:
        expected_points = int(symbol_count) * len(timer_times)
        point_ratio = float(point_count) / expected_points if expected_points else 0.0
        item = {
            "date": d.isoformat() if hasattr(d, "isoformat") else str(d),
            "symbol_count": int(symbol_count),
            "point_count": int(point_count),
            "expected_points": expected_points,
            "symbol_coverage": int(symbol_count) / len(all_symbols),
            "point_coverage": point_ratio,
        }
        coverage.append(item)
        if earliest is None and point_ratio >= min_point_coverage:
            earliest = d

    result = {
        "earliest_date": earliest.isoformat() if hasattr(earliest, "isoformat") else None,
        "symbols": len(all_symbols),
        "required_symbols": required_symbols,
        "timer_times": [t.strftime("%H:%M") for t in timer_times],
        "min_symbol_coverage": min_symbol_coverage,
        "min_point_coverage": min_point_coverage,
        "coverage": coverage[:60],
        "cache_hit": False,
    }
    if redis_key:
        redis_cache.set_json(redis_key, result, ttl=86400)
    return result


def _timer_coverage_cache_key(
    symbols: list[str],
    index_symbol: str | None,
    start: date,
    end: date,
    timer_times: tuple[time, ...],
    min_symbol_coverage: float,
    min_point_coverage: float,
) -> str | None:
    try:
        import hashlib

        payload = json.dumps(
            {
                "symbols": symbols,
                "index_symbol": normalize_index_symbol(index_symbol) if index_symbol else None,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "timer_times": [t.strftime("%H:%M") for t in timer_times],
                "min_symbol_coverage": min_symbol_coverage,
                "min_point_coverage": min_point_coverage,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
        return get_backtest_cache().key("timer_coverage", digest)
    except Exception:
        return None


async def sync_timer_minute_points(
    *,
    symbols: Iterable[str] | None = None,
    index_symbol: str | None = None,
    start: date,
    end: date,
    timer_times: tuple[time, ...] | None = None,
    limit: int = 0,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    timer_times = timer_times or parse_timer_times(None)
    timeout_seconds = float(os.getenv("QMT_TIMER_SYNC_SYMBOL_TIMEOUT", "120"))
    all_symbols = [_normalize_symbol(s) for s in (symbols or []) if str(s).strip()]
    if index_symbol:
        all_symbols.extend(_index_symbols(index_symbol, start, end))
    all_symbols = sorted(set(all_symbols))
    if limit > 0:
        all_symbols = all_symbols[:limit]

    total_inserted = 0
    total_fetched = 0
    failures: list[dict[str, str]] = []
    results: list[dict[str, Any]] = []

    total = len(all_symbols)
    if progress_callback:
        progress_callback(
            {
                "stage": "sync_timer_minute",
                "done": 0,
                "total": total,
                "inserted": 0,
                "times": [t.strftime("%H:%M") for t in timer_times],
            }
        )

    for index, symbol in enumerate(all_symbols, start=1):
        try:
            result = await sync_symbol_timer_minutes(
                symbol,
                start,
                end,
                timer_times,
                timeout_seconds=timeout_seconds,
            )
            total_inserted += int(result["inserted"])
            total_fetched += int(result["fetched"])
            results.append(result)
        except Exception as exc:
            result = {"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"}
            failures.append(result)

        if progress_callback:
            progress_callback(
                {
                    "stage": "sync_timer_minute",
                    "done": index,
                    "total": total,
                    "symbol": symbol,
                    "inserted": total_inserted,
                    "fetched": total_fetched,
                    "last": result,
                    "times": [t.strftime("%H:%M") for t in timer_times],
                }
            )

    return {
        "symbols": total,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "times": [t.strftime("%H:%M") for t in timer_times],
        "fetched": total_fetched,
        "inserted": total_inserted,
        "failures": failures,
        "results": results,
    }

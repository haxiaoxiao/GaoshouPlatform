"""Precompute factor values for factor research and strategy reuse."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any, Sequence

import numpy as np
import pandas as pd

from app.data_stores import get_market_data_store
from app.db.duckdb import get_duckdb
from app.services.factor_value_store import (
    FactorValueStore,
    factor_params_hash,
    get_factor_value_store,
    normalize_factor_time,
)
from app.services.index_components import load_index_symbols


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _timer_datetime(trade_date: date, as_of_time: str) -> datetime:
    hour, minute = [int(part) for part in normalize_factor_time(as_of_time).split(":")[:2]]
    return datetime.combine(trade_date, datetime.min.time()).replace(hour=hour, minute=minute)


def _lookback_start(start_date: date, window: int) -> date:
    return start_date - timedelta(days=max(int(window * 2.5), window + 20))


def _date_range(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)
    return days


def _load_cum_timer_from_parquet(
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    as_of_time: str,
) -> pd.DataFrame:
    store = get_market_data_store()
    exists = getattr(store, "_exists", None)
    glob_pattern = getattr(store, "_glob_pattern", None)
    year_month_filter = getattr(store, "_year_month_filter", None)
    if not callable(exists) or not callable(glob_pattern) or not exists("klines_minute_cum_timer"):
        return pd.DataFrame()

    sym_list = ", ".join("'" + str(s).replace("'", "''") + "'" for s in symbols)
    start_dt = _timer_datetime(start_date, as_of_time)
    end_dt = _timer_datetime(end_date, as_of_time)
    partition_filter = year_month_filter("klines_minute_cum_timer", start_dt, end_dt) if callable(year_month_filter) else ""
    sql = f"""
        SELECT symbol, CAST(datetime AS DATE) AS trade_date, volume
        FROM read_parquet('{glob_pattern("klines_minute_cum_timer")}', hive_partitioning=true)
        WHERE symbol IN ({sym_list})
          AND datetime >= '{start_dt}'
          AND datetime <= '{end_dt}'
          AND hour(datetime) = {start_dt.hour}
          AND minute(datetime) = {start_dt.minute}
          {partition_filter}
        ORDER BY symbol, trade_date
    """
    df = get_duckdb().execute(sql).df()
    if df.empty:
        return df
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    return df


def _resolve_symbols(
    *,
    symbols: Sequence[str] | None,
    index_symbol: str | None,
    start_date: date,
    end_date: date,
) -> list[str]:
    if symbols:
        return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    if index_symbol:
        return asyncio.run(load_index_symbols(index_symbol, start_date, end_date))
    raise ValueError("Either symbols or index_symbol is required")


def _sqlite_db_path():
    from pathlib import Path
    from app.core.config import settings

    return Path(settings.data_dir) / "gaoshou.db"


def _load_daily_basic(symbols: Sequence[str], start_date: date, end_date: date) -> pd.DataFrame:
    import sqlite3

    if not symbols:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT symbol, trade_date, circ_mv, total_mv
        FROM stock_daily_basic
        WHERE symbol IN ({placeholders})
          AND trade_date >= ?
          AND trade_date <= ?
    """
    with sqlite3.connect(_sqlite_db_path()) as conn:
        return pd.read_sql_query(sql, conn, params=[*symbols, start_date.isoformat(), end_date.isoformat()])


def _load_st_symbols(symbols: Sequence[str], as_of: date) -> set[str]:
    import sqlite3

    if not symbols:
        return set()
    placeholders = ",".join("?" for _ in symbols)
    with sqlite3.connect(_sqlite_db_path()) as conn:
        stock_rows = conn.execute(
            f"""
            SELECT symbol, name, is_st, is_delist, delist_date
            FROM stocks
            WHERE symbol IN ({placeholders})
            """,
            list(symbols),
        ).fetchall()
        change_rows = conn.execute(
            f"""
            SELECT symbol
            FROM stock_name_changes
            WHERE symbol IN ({placeholders})
              AND start_date <= ?
              AND (end_date IS NULL OR end_date >= ?)
              AND (name LIKE '%ST%' OR name LIKE '%*%' OR name LIKE '%退%')
            """,
            [*symbols, as_of.isoformat(), as_of.isoformat()],
        ).fetchall()
    flagged: set[str] = {str(row[0]) for row in change_rows}
    for symbol, name, is_st, is_delist, delist_date in stock_rows:
        text = str(name or "")
        if is_st or is_delist or "ST" in text or "*" in text or "退" in text:
            flagged.add(str(symbol))
            continue
        if delist_date and str(delist_date)[:10] <= as_of.isoformat():
            flagged.add(str(symbol))
    return flagged


def _load_st_reference(symbols: Sequence[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    import sqlite3

    if not symbols:
        return pd.DataFrame(), pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    with sqlite3.connect(_sqlite_db_path()) as conn:
        stocks = pd.read_sql_query(
            f"""
            SELECT symbol, name, is_st, is_delist, delist_date
            FROM stocks
            WHERE symbol IN ({placeholders})
            """,
            conn,
            params=list(symbols),
        )
        changes = pd.read_sql_query(
            f"""
            SELECT symbol, name, start_date, end_date
            FROM stock_name_changes
            WHERE symbol IN ({placeholders})
              AND (name LIKE '%ST%' OR name LIKE '%*%' OR name LIKE '%閫€%')
            """,
            conn,
            params=list(symbols),
        )
    if not stocks.empty:
        stocks["symbol"] = stocks["symbol"].astype(str)
        stocks["name"] = stocks["name"].fillna("").astype(str)
        stocks["delist_date"] = pd.to_datetime(stocks["delist_date"], errors="coerce").dt.date
    if not changes.empty:
        changes["symbol"] = changes["symbol"].astype(str)
        changes["start_date"] = pd.to_datetime(changes["start_date"], errors="coerce").dt.date
        changes["end_date"] = pd.to_datetime(changes["end_date"], errors="coerce").dt.date
    return stocks, changes


def _st_symbols_from_reference(stocks: pd.DataFrame, changes: pd.DataFrame, as_of: date) -> set[str]:
    flagged: set[str] = set()
    if not changes.empty:
        active = changes[
            (changes["start_date"].isna() | (changes["start_date"] <= as_of))
            & (changes["end_date"].isna() | (changes["end_date"] >= as_of))
        ]
        flagged.update(active["symbol"].astype(str).tolist())
    if stocks.empty:
        return flagged
    for row in stocks.itertuples(index=False):
        name = str(getattr(row, "name", "") or "")
        delist_date = getattr(row, "delist_date", None)
        if getattr(row, "is_st", 0) or getattr(row, "is_delist", 0) or "ST" in name or "*" in name or "閫€" in name:
            flagged.add(str(row.symbol))
            continue
        if pd.notna(delist_date) and delist_date <= as_of:
            flagged.add(str(row.symbol))
    return flagged


def _load_limit_prices(symbols: Sequence[str], start_date: date, end_date: date) -> pd.DataFrame:
    import sqlite3

    if not symbols:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT symbol, trade_date, up_limit, down_limit
        FROM stock_limit_prices
        WHERE symbol IN ({placeholders})
          AND trade_date >= ?
          AND trade_date <= ?
    """
    with sqlite3.connect(_sqlite_db_path()) as conn:
        return pd.read_sql_query(sql, conn, params=[*symbols, start_date.isoformat(), end_date.isoformat()])


def precompute_small_cap_core_features(
    *,
    start_date: str | date,
    end_date: str | date,
    symbols: Sequence[str] | None = None,
    index_symbol: str | None = None,
    timer_time: str = "10:30",
    include_high_volume: bool = True,
    store: FactorValueStore | None = None,
) -> dict[str, Any]:
    """Precompute core small-cap cross-section/status factors."""

    start = _parse_date(start_date)
    end = _parse_date(end_date)
    timer_time = normalize_factor_time(timer_time)
    symbol_list = _resolve_symbols(
        symbols=symbols,
        index_symbol=index_symbol,
        start_date=start,
        end_date=end,
    )
    if not symbol_list:
        return {"symbols": 0, "rows": {}, "message": "no symbols"}

    factor_store = store or get_factor_value_store()
    created_at = datetime.now()
    rows: list[dict[str, Any]] = []
    by_feature: dict[str, int] = {}
    written = 0
    direct_dates: set[date] = set()

    def add_feature_row(row: dict[str, Any]) -> None:
        nonlocal rows
        rows.append(row)
        factor_name = str(row["factor_name"])
        by_feature[factor_name] = by_feature.get(factor_name, 0) + 1
        if len(rows) >= 250_000:
            flush_rows()

    def flush_rows() -> None:
        nonlocal rows, written
        if not rows:
            return
        written += factor_store.write(pd.DataFrame(rows))
        rows = []

    # Daily market cap and universe rank.
    basic = _load_daily_basic(symbol_list, start - timedelta(days=370), end)
    if not basic.empty:
        basic["trade_date"] = pd.to_datetime(basic["trade_date"]).dt.date
        basic["market_cap"] = pd.to_numeric(basic["circ_mv"], errors="coerce")
        basic["market_cap"] = basic["market_cap"].fillna(pd.to_numeric(basic["total_mv"], errors="coerce"))
        basic = basic.dropna(subset=["market_cap"])
        direct = basic[(basic["trade_date"] >= start) & (basic["trade_date"] <= end)].copy()
        direct_dates = set(direct["trade_date"].tolist())
        for row in direct.itertuples(index=False):
            add_feature_row(
                {
                    "symbol": row.symbol,
                    "trade_date": row.trade_date,
                    "as_of_time": "",
                    "factor_name": "market_cap",
                    "params_hash": factor_params_hash({}),
                    "value": float(row.market_cap),
                    "source": "precompute.small_cap_core",
                    "created_at": created_at,
                }
            )

        for trade_date, frame in direct.groupby("trade_date", sort=True):
            ranked = frame.sort_values(["market_cap", "symbol"], ascending=[True, True]).reset_index(drop=True)
            for rank, row in enumerate(ranked.itertuples(index=False), start=1):
                add_feature_row(
                    {
                        "symbol": row.symbol,
                        "trade_date": trade_date,
                        "as_of_time": "",
                        "factor_name": "market_cap_rank",
                        "params_hash": factor_params_hash({}),
                        "value": float(rank),
                        "source": "precompute.small_cap_core",
                        "created_at": created_at,
                    }
                )

    market_store = get_market_data_store()
    timer_start = datetime.combine(start, datetime.min.time())
    timer_end = datetime.combine(end, datetime.min.time()) + timedelta(days=1)
    minute = market_store.load_minute(
        symbol_list,
        timer_start,
        timer_end,
        columns=["symbol", "datetime", "close"],
        timer_times=[timer_time],
    )
    if not minute.empty:
        minute = minute.reset_index() if "datetime" not in minute.columns else minute.copy()
        minute["trade_date"] = pd.to_datetime(minute["datetime"]).dt.date
        minute["close"] = pd.to_numeric(minute["close"], errors="coerce")

    limit_prices = _load_limit_prices(symbol_list, start, end)
    if not limit_prices.empty:
        limit_prices["trade_date"] = pd.to_datetime(limit_prices["trade_date"]).dt.date
        limit_prices["up_limit"] = pd.to_numeric(limit_prices["up_limit"], errors="coerce")
        limit_prices["down_limit"] = pd.to_numeric(limit_prices["down_limit"], errors="coerce")

    close_daily = market_store.load_daily(
        symbol_list,
        start - timedelta(days=10),
        end,
        columns=["symbol", "trade_date", "close"],
    )
    if not close_daily.empty:
        close_daily = close_daily.reset_index() if "trade_date" not in close_daily.columns else close_daily.copy()
        close_daily["trade_date"] = pd.to_datetime(close_daily["trade_date"]).dt.date
        close_daily["close"] = pd.to_numeric(close_daily["close"], errors="coerce")

    minute_keys = set()
    if not minute.empty:
        minute_keys = {(str(row.symbol), row.trade_date) for row in minute.itertuples(index=False)}
    limit_map = {}
    if not limit_prices.empty:
        limit_map = {
            (str(row.symbol), row.trade_date): (float(row.up_limit or 0.0), float(row.down_limit or 0.0))
            for row in limit_prices.itertuples(index=False)
        }
    minute_close = {}
    if not minute.empty:
        minute_close = {
            (str(row.symbol), row.trade_date): float(row.close or 0.0)
            for row in minute.itertuples(index=False)
        }
    daily_close = {}
    if not close_daily.empty:
        daily_close = {
            (str(row.symbol), row.trade_date): float(row.close or 0.0)
            for row in close_daily.itertuples(index=False)
        }

    trading_day_set = set(direct_dates)
    if not minute.empty:
        trading_day_set.update(minute["trade_date"].tolist())
    if not limit_prices.empty:
        trading_day_set.update(limit_prices["trade_date"].tolist())
    if not close_daily.empty:
        trading_day_set.update(day for day in close_daily["trade_date"].tolist() if start <= day <= end)
    days = sorted(day for day in trading_day_set if start <= day <= end)
    if not days:
        days = [day for day in _date_range(start, end) if day.weekday() < 5]
    st_stocks, st_changes = _load_st_reference(symbol_list)
    for day in days:
        st_symbols = _st_symbols_from_reference(st_stocks, st_changes, day)
        for symbol in symbol_list:
            common = {
                "symbol": symbol,
                "trade_date": day,
                "source": "precompute.small_cap_core",
                "created_at": created_at,
            }
            add_feature_row({**common, "as_of_time": "", "factor_name": "is_st", "params_hash": factor_params_hash({}), "value": 1.0 if symbol in st_symbols else 0.0})
            paused = (symbol, day) not in minute_keys
            add_feature_row({**common, "as_of_time": timer_time, "factor_name": "is_paused", "params_hash": factor_params_hash({"time": timer_time}), "value": 1.0 if paused else 0.0})
            price = minute_close.get((symbol, day), 0.0)
            up_limit, down_limit = limit_map.get((symbol, day), (0.0, 0.0))
            add_feature_row({**common, "as_of_time": timer_time, "factor_name": "is_limit_up", "params_hash": factor_params_hash({"time": timer_time}), "value": 1.0 if price > 0 and up_limit > 0 and price >= up_limit - 1e-4 else 0.0})
            add_feature_row({**common, "as_of_time": timer_time, "factor_name": "is_limit_down", "params_hash": factor_params_hash({"time": timer_time}), "value": 1.0 if price > 0 and down_limit > 0 and price <= down_limit + 1e-4 else 0.0})

    if limit_map and daily_close:
        for day in days:
            previous = max((d for (_, d) in daily_close if d < day), default=None)
            if previous is None:
                continue
            for symbol in symbol_list:
                close = daily_close.get((symbol, previous), 0.0)
                up_limit = limit_map.get((symbol, previous), (0.0, 0.0))[0]
                add_feature_row(
                    {
                        "symbol": symbol,
                        "trade_date": day,
                        "as_of_time": "",
                        "factor_name": "yesterday_limit_up",
                        "params_hash": factor_params_hash({}),
                        "value": 1.0 if close > 0 and up_limit > 0 and close >= up_limit - 1e-4 else 0.0,
                        "source": "precompute.small_cap_core",
                        "created_at": created_at,
                    }
                )

    flush_rows()

    high_volume_result = None
    if include_high_volume:
        high_volume_result = precompute_high_volume_features(
            start_date=start,
            end_date=end,
            symbols=symbol_list,
            as_of_time="14:30",
            window=120,
            threshold=0.9,
            daily_volume_to_share_multiplier=100.0,
            store=factor_store,
        )

    return {
        "symbols": len(symbol_list),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "as_of_time": timer_time,
        "rows": by_feature,
        "rows_written": written,
        "high_volume": high_volume_result,
    }


def precompute_high_volume_features(
    *,
    start_date: str | date,
    end_date: str | date,
    symbols: Sequence[str] | None = None,
    index_symbol: str | None = None,
    as_of_time: str = "14:30",
    window: int = 120,
    threshold: float = 0.9,
    daily_volume_to_share_multiplier: float = 100.0,
    store: FactorValueStore | None = None,
) -> dict[str, Any]:
    """Build 14:30 volume features used by ID=43, as generic factor values."""

    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end_date must be greater than or equal to start_date")
    as_of_time = normalize_factor_time(as_of_time)
    window = max(int(window), 2)
    threshold = float(threshold)
    multiplier = float(daily_volume_to_share_multiplier)
    if not np.isfinite(multiplier) or multiplier <= 0:
        multiplier = 1.0

    symbol_list = _resolve_symbols(
        symbols=symbols,
        index_symbol=index_symbol,
        start_date=start,
        end_date=end,
    )
    if not symbol_list:
        return {"symbols": 0, "rows": {}, "message": "no symbols"}

    market_store = get_market_data_store()
    daily = market_store.load_daily(
        symbol_list,
        _lookback_start(start, window),
        end,
        columns=["symbol", "trade_date", "volume"],
    )
    if daily.empty:
        raise RuntimeError("No daily volume data found for feature precompute")
    daily = daily.reset_index() if "trade_date" not in daily.columns else daily.copy()
    daily["trade_date"] = pd.to_datetime(daily["trade_date"]).dt.date
    daily["volume"] = pd.to_numeric(daily["volume"], errors="coerce").fillna(0.0) * multiplier
    daily = daily.sort_values(["symbol", "trade_date"])

    cum_timer = _load_cum_timer_from_parquet(symbol_list, start, end, as_of_time)
    if cum_timer.empty:
        raise RuntimeError(
            "No klines_minute_cum_timer data found. Run build_minute_cum_timer_parquet.py first."
        )

    rows: list[dict[str, Any]] = []
    params_base = {
        "time": as_of_time,
        "window": window,
        "daily_volume_to_share_multiplier": multiplier,
    }
    signal_params = {**params_base, "threshold": threshold}
    hash_base = factor_params_hash(params_base)
    hash_signal = factor_params_hash(signal_params)
    created_at = datetime.now()

    daily_by_symbol = {
        symbol: frame[["trade_date", "volume"]].sort_values("trade_date").reset_index(drop=True)
        for symbol, frame in daily.groupby("symbol", sort=False)
    }

    for timer_row in cum_timer.itertuples(index=False):
        symbol = str(timer_row.symbol)
        trade_date = timer_row.trade_date
        current_volume = float(timer_row.volume or 0.0)
        history = daily_by_symbol.get(symbol)
        if history is None or history.empty:
            continue
        previous = history[history["trade_date"] < trade_date]["volume"].tail(window - 1).to_numpy(dtype=float)
        volumes = np.append(previous, current_volume)
        if len(volumes) < window:
            continue
        max_volume = float(np.nanmax(volumes))
        if max_volume <= 0:
            continue
        ratio = current_volume / max_volume
        signal = 1.0 if ratio > threshold else 0.0
        common = {
            "symbol": symbol,
            "trade_date": trade_date,
            "as_of_time": as_of_time,
            "source": "precompute.high_volume",
            "created_at": created_at,
        }
        rows.extend(
            [
                {
                    **common,
                    "factor_name": "cum_volume_at_time",
                    "params_hash": factor_params_hash({"time": as_of_time}),
                    "value": current_volume,
                },
                {
                    **common,
                    "factor_name": "rolling_max_volume",
                    "params_hash": hash_base,
                    "value": max_volume,
                },
                {
                    **common,
                    "factor_name": "high_volume_ratio",
                    "params_hash": hash_base,
                    "value": ratio,
                },
                {
                    **common,
                    "factor_name": "high_volume_signal",
                    "params_hash": hash_signal,
                    "value": signal,
                },
            ]
        )

    factor_store = store or get_factor_value_store()
    written = factor_store.write(pd.DataFrame(rows)) if rows else 0
    by_feature: dict[str, int] = {}
    for row in rows:
        by_feature[row["factor_name"]] = by_feature.get(row["factor_name"], 0) + 1

    return {
        "symbols": len(symbol_list),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "as_of_time": as_of_time,
        "window": window,
        "threshold": threshold,
        "rows": by_feature,
        "rows_written": written,
    }

"""Point-in-time index constituent cache."""

from __future__ import annotations

import asyncio
import calendar
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from app.core.config import settings
from app.services.index_catalog import (
    get_index_item,
    jq_index_symbol as catalog_jq_index_symbol,
    list_index_items,
    normalize_index_symbol as catalog_normalize_index_symbol,
)


KNOWN_INDEX_POOLS = [
    {
        "symbol": item.symbol,
        "jq_symbol": item.jq_symbol or item.symbol,
        "name": item.display_name,
        "source": "derived.union" if item.component_mode == "derived_union" else "tushare.index_weight",
        "component_mode": item.component_mode,
        "pool_enabled": item.pool_enabled,
    }
    for item in list_index_items(pool_only=True)
]

DERIVED_INDEX_COMPONENT_SOURCES: dict[str, tuple[str, ...]] = {
    "000906.SH": ("000300.SH", "000905.SH"),
}


def normalize_index_symbol(symbol: str | None) -> str | None:
    return catalog_normalize_index_symbol(symbol)


def jq_index_symbol(symbol: str) -> str:
    return catalog_jq_index_symbol(symbol)


def _db_path() -> Path:
    return settings.sqlite_db_path


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def init_index_component_table() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS index_components (
                index_symbol TEXT NOT NULL,
                jq_index_symbol TEXT,
                symbol TEXT NOT NULL,
                trade_date DATE NOT NULL,
                weight REAL,
                source TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (index_symbol, trade_date, symbol)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_index_components_lookup
            ON index_components(index_symbol, trade_date)
            """
        )
        conn.commit()


def _month_ranges(start: date, end: date) -> list[tuple[date, date]]:
    ranges: list[tuple[date, date]] = []
    current = date(start.year, start.month, 1)
    while current <= end:
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = date(current.year, current.month, last_day)
        ranges.append((max(start, current), min(end, month_end)))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return ranges


def _has_snapshot_covering(index_symbol: str, start: date, end: date) -> bool:
    init_index_component_table()
    idx = normalize_index_symbol(index_symbol) or index_symbol
    with _connect() as conn:
        before = conn.execute(
            """
            SELECT trade_date
            FROM index_components
            WHERE index_symbol = ? AND trade_date <= ?
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (idx, start.isoformat()),
        ).fetchone()
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT trade_date), MAX(trade_date)
            FROM index_components
            WHERE index_symbol = ? AND trade_date >= ? AND trade_date <= ?
            """,
            (idx, start.isoformat(), end.isoformat()),
        ).fetchone()
    snapshot_count = int(row[0] or 0)
    latest_snapshot = date.fromisoformat(row[1]) if row[1] else None
    return bool(before and snapshot_count and latest_snapshot and latest_snapshot >= end - timedelta(days=45))


def _tushare_token() -> str | None:
    import tushare as ts

    return os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or ts.get_token()


def _fetch_tushare_index_weight(index_symbol: str, start: date, end: date) -> pd.DataFrame:
    import tushare as ts

    item = get_index_item(index_symbol)
    if item is None:
        raise RuntimeError(f"Unsupported index symbol: {index_symbol}")
    if not item.pool_enabled:
        raise RuntimeError(f"Index {item.symbol} is market-only and cannot be used as a historical stock pool")

    token = _tushare_token()
    if not token:
        raise RuntimeError("Tushare token is not configured")
    ts.set_token(token)
    pro = ts.pro_api()

    frames = []
    for chunk_start, chunk_end in _month_ranges(start, end):
        df = pro.index_weight(
            index_code=item.provider_symbol,
            start_date=chunk_start.strftime("%Y%m%d"),
            end_date=chunk_end.strftime("%Y%m%d"),
        )
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["index_code", "con_code", "trade_date", "weight"])
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset=["index_code", "con_code", "trade_date"])


def _upsert_components(index_symbol: str, df: pd.DataFrame, *, source_name: str = "tushare.index_weight") -> int:
    if df.empty:
        return 0
    idx = normalize_index_symbol(index_symbol) or index_symbol
    jq_idx = jq_index_symbol(idx)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows: list[tuple[Any, ...]] = []
    for _, row in df.iterrows():
        trade_date = str(row["trade_date"])
        if len(trade_date) == 8:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
        rows.append(
            (
                idx,
                jq_idx,
                str(row["con_code"]).upper(),
                trade_date,
                float(row["weight"]) if pd.notna(row.get("weight")) else None,
                source_name,
                now,
            )
        )

    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO index_components (
                index_symbol, jq_index_symbol, symbol, trade_date, weight, source, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(index_symbol, trade_date, symbol) DO UPDATE SET
                jq_index_symbol = excluded.jq_index_symbol,
                weight = excluded.weight,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def _derive_union_components(index_symbol: str, start: date, end: date) -> int:
    idx = normalize_index_symbol(index_symbol) or index_symbol
    source_symbols = DERIVED_INDEX_COMPONENT_SOURCES.get(idx)
    if not source_symbols:
        return 0

    init_index_component_table()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    jq_idx = jq_index_symbol(idx)
    placeholders = ",".join("?" for _ in source_symbols)
    with _connect() as conn:
        source_rows = conn.execute(
            f"""
            SELECT DISTINCT trade_date, symbol
            FROM index_components
            WHERE index_symbol IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY trade_date, symbol
            """,
            [*source_symbols, start.isoformat(), end.isoformat()],
        ).fetchall()
        if not source_rows:
            return 0

        rows = [
            (
                idx,
                jq_idx,
                str(row["symbol"]).upper(),
                str(row["trade_date"]),
                None,
                "derived.union",
                now,
            )
            for row in source_rows
        ]
        conn.executemany(
            """
            INSERT INTO index_components (
                index_symbol, jq_index_symbol, symbol, trade_date, weight, source, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(index_symbol, trade_date, symbol) DO UPDATE SET
                jq_index_symbol = excluded.jq_index_symbol,
                weight = excluded.weight,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


async def ensure_index_components(index_symbol: str, start: date, end: date) -> dict[str, Any]:
    idx = normalize_index_symbol(index_symbol) or index_symbol
    item = get_index_item(idx)
    if item is None:
        raise ValueError(f"Unsupported index symbol: {index_symbol}")
    if not item.pool_enabled:
        raise ValueError(f"Index {item.symbol} is not enabled as a historical stock pool: market_only_index")

    fetch_start = start - timedelta(days=370)
    if _has_snapshot_covering(idx, start, end):
        return {"index_symbol": idx, "inserted": 0, "source": "cache"}

    if item.component_mode == "derived_union":
        def _sync_derived() -> dict[str, Any]:
            inserted = _derive_union_components(idx, fetch_start, end)
            logger.info("Index components derived: {} rows for {}", inserted, idx)
            return {"index_symbol": idx, "inserted": inserted, "source": "derived.union"}

        result = await asyncio.to_thread(_sync_derived)
        if _has_snapshot_covering(idx, start, end):
            return result
        raise RuntimeError(
            f"Strict historical constituent snapshots are missing for {idx}; cannot derive union coverage"
        )

    def _sync() -> dict[str, Any]:
        df = _fetch_tushare_index_weight(idx, fetch_start, end)
        inserted = _upsert_components(idx, df)
        logger.info("Index components synced: {} rows for {}", inserted, idx)
        return {"index_symbol": idx, "inserted": inserted, "source": "tushare.index_weight"}

    return await asyncio.to_thread(_sync)


def _has_any_snapshot(index_symbol: str, start: date, end: date) -> bool:
    init_index_component_table()
    idx = normalize_index_symbol(index_symbol) or index_symbol
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM index_components
            WHERE index_symbol = ?
              AND trade_date >= ?
              AND trade_date <= ?
            """,
            (idx, (start - timedelta(days=370)).isoformat(), end.isoformat()),
        ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


async def load_index_symbols(index_symbol: str, start: date, end: date) -> list[str]:
    await ensure_index_components(index_symbol, start, end)
    idx = normalize_index_symbol(index_symbol) or index_symbol
    lookback = start - timedelta(days=370)

    def _query() -> list[str]:
        with _connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT symbol
                FROM index_components
                WHERE index_symbol = ?
                  AND trade_date >= ?
                  AND trade_date <= ?
                ORDER BY symbol
                """,
                (idx, lookback.isoformat(), end.isoformat()),
            ).fetchall()
        return [str(row["symbol"]).upper() for row in rows]

    return await asyncio.to_thread(_query)


async def index_pool_summary(index_symbol: str, start: date, end: date) -> dict[str, Any]:
    item = get_index_item(index_symbol)
    symbols = await load_index_symbols(index_symbol, start, end)
    idx = normalize_index_symbol(index_symbol) or index_symbol

    def _query_dates() -> tuple[str | None, str | None, int]:
        with _connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date)
                FROM index_components
                WHERE index_symbol = ?
                  AND trade_date >= ?
                  AND trade_date <= ?
                """,
                (idx, (start - timedelta(days=370)).isoformat(), end.isoformat()),
            ).fetchone()
        return row[0], row[1], int(row[2] or 0)

    min_date, max_date, snapshot_count = await asyncio.to_thread(_query_dates)
    return {
        "index_symbol": idx,
        "display_name": item.display_name if item else idx,
        "jq_symbol": jq_index_symbol(idx),
        "symbol_count": len(symbols),
        "snapshot_count": snapshot_count,
        "min_snapshot_date": min_date,
        "max_snapshot_date": max_date,
        "symbols": symbols,
        "component_mode": item.component_mode if item else "snapshot",
        "pool_enabled": bool(item.pool_enabled) if item else False,
        "component_status": "available",
        "reason": None,
    }

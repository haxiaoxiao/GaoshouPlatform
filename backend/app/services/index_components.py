"""Point-in-time index constituent cache.

The backtest UI can select an index as the stock pool. The engine still needs
all symbols preloaded for the whole run, but strategies must choose the actual
pool at each rebalance date from a point-in-time constituent snapshot.
"""

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


KNOWN_INDEX_POOLS = [
    {
        "symbol": "399101.SZ",
        "jq_symbol": "399101.XSHE",
        "name": "中小综指",
        "source": "tushare.index_weight",
    },
]


def normalize_index_symbol(symbol: str | None) -> str | None:
    if not symbol:
        return None
    text = symbol.strip().upper()
    if text == "399101.XSHE":
        return "399101.SZ"
    return text


def jq_index_symbol(symbol: str) -> str:
    normalized = normalize_index_symbol(symbol) or symbol
    if normalized == "399101.SZ":
        return "399101.XSHE"
    return normalized


def _db_path() -> Path:
    return Path(settings.data_dir) / "gaoshou.db"


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
    # Index weights are periodic snapshots, not daily rows. A cache is fresh
    # enough when the latest snapshot is near the backtest end.
    return bool(before and snapshot_count and latest_snapshot and latest_snapshot >= end - timedelta(days=45))


def _tushare_token() -> str | None:
    import tushare as ts

    return os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or ts.get_token()


def _fetch_tushare_index_weight(index_symbol: str, start: date, end: date) -> pd.DataFrame:
    import tushare as ts

    token = _tushare_token()
    if not token:
        raise RuntimeError("Tushare token is not configured")
    ts.set_token(token)
    pro = ts.pro_api()

    frames = []
    for chunk_start, chunk_end in _month_ranges(start, end):
        df = pro.index_weight(
            index_code=index_symbol,
            start_date=chunk_start.strftime("%Y%m%d"),
            end_date=chunk_end.strftime("%Y%m%d"),
        )
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["index_code", "con_code", "trade_date", "weight"])
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset=["index_code", "con_code", "trade_date"])


def _upsert_components(index_symbol: str, df: pd.DataFrame) -> int:
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
                "tushare.index_weight",
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


async def ensure_index_components(index_symbol: str, start: date, end: date) -> dict[str, Any]:
    """Ensure local snapshots exist around the requested backtest window."""
    idx = normalize_index_symbol(index_symbol) or index_symbol
    fetch_start = start - timedelta(days=370)
    if _has_snapshot_covering(idx, start, end):
        return {"index_symbol": idx, "inserted": 0, "source": "cache"}

    def _sync() -> dict[str, Any]:
        df = _fetch_tushare_index_weight(idx, fetch_start, end)
        inserted = _upsert_components(idx, df)
        logger.info("Index components synced: {} rows for {}", inserted, idx)
        return {"index_symbol": idx, "inserted": inserted, "source": "tushare.index_weight"}

    return await asyncio.to_thread(_sync)


async def load_index_symbols(index_symbol: str, start: date, end: date) -> list[str]:
    """Return the union of symbols required to preload data for a date window."""
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
        return [r["symbol"] for r in rows]

    return await asyncio.to_thread(_query)


async def index_pool_summary(index_symbol: str, start: date, end: date) -> dict[str, Any]:
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
        "jq_symbol": jq_index_symbol(idx),
        "symbol_count": len(symbols),
        "snapshot_count": snapshot_count,
        "min_snapshot_date": min_date,
        "max_snapshot_date": max_date,
        "symbols": symbols,
    }

"""Reusable A-share universe resolvers."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Sequence

from app.core.config import settings


def _db_path() -> Path:
    return Path(getattr(settings, "sqlite_db_path", None) or Path(settings.data_dir) / "gaoshou.db")


def normalize_universe_mode(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"all_a", "alla", "a_share", "ashare", "a_shares", "full_a", "full_market"}:
        return "all_a"
    if text in {"full_market_small_cap_rank", "full_market_small_cap", "all_a_small_cap"}:
        return "all_a"
    if text == "index":
        return "index"
    return "symbols"


def is_all_a_universe(value: Any) -> bool:
    return normalize_universe_mode(value) == "all_a"


def load_all_a_symbols(
    *,
    as_of: date | None = None,
    start_date: date | None = None,
    exchanges: Sequence[str] | None = None,
    include_delisted_during_range: bool = True,
) -> list[str]:
    """Load the broad SH/SZ A-share universe from the local stocks table."""

    db_path = _db_path()
    if not db_path.exists():
        return []

    as_of_date = as_of or date.today()
    raw_exchanges = [str(item).strip().upper() for item in exchanges or ("SH", "SZ") if str(item).strip()]
    if not raw_exchanges:
        raw_exchanges = ["SH", "SZ"]

    conditions = [
        f"exchange IN ({','.join(['?'] * len(raw_exchanges))})",
        "(security_type IS NULL OR lower(security_type) = 'stock')",
        "(product_class IS NULL OR lower(product_class) = 'stock')",
        "list_date IS NOT NULL",
        "date(list_date) <= date(?)",
    ]
    params: list[Any] = [*raw_exchanges, as_of_date.isoformat()]

    if include_delisted_during_range and start_date is not None:
        conditions.append("(COALESCE(is_delist, 0) = 0 OR delist_date IS NULL OR date(delist_date) >= date(?))")
        params.append(start_date.isoformat())
    else:
        conditions.append("COALESCE(is_delist, 0) = 0")
        conditions.append("(delist_date IS NULL OR date(delist_date) > date(?))")
        params.append(as_of_date.isoformat())

    sql = f"""
        SELECT symbol
        FROM stocks
        WHERE {' AND '.join(conditions)}
        ORDER BY symbol
    """
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [str(row[0]) for row in rows if str(row[0]).strip()]

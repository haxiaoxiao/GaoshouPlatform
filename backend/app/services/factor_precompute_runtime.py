"""Shared memory policy for factor precompute jobs.

This module is intentionally small and dependency-light. Factor calculators use
it to keep data reads, intermediate DataFrames, and Parquet writes bounded by a
single policy instead of each implementation inventing its own batch sizes.
"""

from __future__ import annotations

import gc
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Sequence


@dataclass(frozen=True)
class PrecomputeChunk:
    symbols: list[str]
    start_date: date
    end_date: date
    index: int
    total: int


@dataclass(frozen=True)
class PrecomputeMemoryPolicy:
    symbol_chunk_size: int = 300
    date_chunk_days: int = 10
    batch_rows: int = 50_000
    wide_panel_max_rows: int = 2_000_000


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def precompute_memory_policy() -> PrecomputeMemoryPolicy:
    """Return the process-wide precompute memory policy.

    Env overrides are useful when a machine has unusually small or large RAM,
    but the defaults should keep ordinary full-market factor jobs bounded.
    """
    return PrecomputeMemoryPolicy(
        symbol_chunk_size=_positive_int_env("FACTOR_PRECOMPUTE_SYMBOL_CHUNK_SIZE", 300),
        date_chunk_days=_positive_int_env("FACTOR_PRECOMPUTE_DATE_CHUNK_DAYS", 10),
        batch_rows=_positive_int_env("FACTOR_PRECOMPUTE_BATCH_ROWS", 50_000),
        wide_panel_max_rows=_positive_int_env("FACTOR_PRECOMPUTE_WIDE_PANEL_MAX_ROWS", 2_000_000),
    )


def symbol_chunks(symbols: Sequence[str], chunk_size: int | None = None) -> list[list[str]]:
    policy = precompute_memory_policy()
    size = max(1, int(chunk_size or policy.symbol_chunk_size))
    items = [str(symbol) for symbol in symbols if str(symbol)]
    return [items[index:index + size] for index in range(0, len(items), size)]


def date_chunks(start_date: date, end_date: date, chunk_days: int | None = None) -> list[tuple[date, date]]:
    policy = precompute_memory_policy()
    size = max(1, int(chunk_days or policy.date_chunk_days))
    chunks: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        chunk_end = min(end_date, current + timedelta(days=size - 1))
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def symbol_date_chunks(
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    *,
    policy: PrecomputeMemoryPolicy | None = None,
) -> list[PrecomputeChunk]:
    policy = policy or precompute_memory_policy()
    symbol_parts = symbol_chunks(symbols, policy.symbol_chunk_size)
    date_parts = date_chunks(start_date, end_date, policy.date_chunk_days)
    total = max(1, len(symbol_parts) * len(date_parts))
    chunks: list[PrecomputeChunk] = []
    index = 0
    for chunk_start, chunk_end in date_parts:
        for symbol_part in symbol_parts:
            index += 1
            chunks.append(PrecomputeChunk(
                symbols=symbol_part,
                start_date=chunk_start,
                end_date=chunk_end,
                index=index,
                total=total,
            ))
    return chunks


def release_precompute_memory() -> None:
    """Collect cyclic garbage after a large chunk has been written."""
    gc.collect()

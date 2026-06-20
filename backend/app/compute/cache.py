"""Compute result cache: process LRU + Redis + Parquet factor_cache."""
from __future__ import annotations

import hashlib
import json
import math
import os
import threading
from collections import OrderedDict
from datetime import date
from typing import Any

import pandas as pd
from loguru import logger

from app.cache.redis_cache import get_redis_client as _get_redis_client


class LRUCache:
    def __init__(self, max_size: int = 256):
        self._max_size = max_size
        self._cache: OrderedDict[str, Any] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            return None

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = value
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


class ComputeCache:
    def __init__(self):
        self.l1 = LRUCache(max_size=256)

    @staticmethod
    def make_key(full_expression: str) -> str:
        normalized = full_expression.strip().lower().replace(" ", "")
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    def get(self, expression: str) -> dict[str, pd.Series] | None:
        key = self.make_key(expression)
        cached = self.l1.get(key)
        if cached is not None:
            return cached

        try:
            redis_val = _get_redis_client().get(key)
            if redis_val is not None:
                deserialized = self._deserialize_result(redis_val)
                if deserialized is not None:
                    self.l1.set(key, deserialized)
                    return deserialized
        except Exception:
            logger.debug("Redis get failed for key=%s", key[:8], exc_info=True)
        return None

    def set(self, expression: str, result: dict[str, pd.Series]) -> None:
        key = self.make_key(expression)
        max_points = int(os.getenv("COMPUTE_CACHE_MAX_POINTS", "200000"))
        total_points = sum(len(series) for series in result.values())
        if total_points > max_points:
            logger.info(
                "Compute cache skipped for key=%s: result too large (%s points)",
                key[:8],
                total_points,
            )
            return

        try:
            serialized = self._serialize_result(result)
        except Exception:
            logger.debug("Compute cache serialization failed for key=%s", key[:8], exc_info=True)
            return

        self.l1.set(key, result)
        try:
            _get_redis_client().set(key, serialized, ttl=3600)
        except Exception:
            logger.debug("Redis set failed for key=%s", key[:8], exc_info=True)

    @staticmethod
    def _serialize_result(result: dict[str, pd.Series]) -> str:
        serialized: dict[str, dict] = {}
        for symbol, series in result.items():
            values = series.to_dict()
            serialized[symbol] = {
                key: (None if isinstance(value, float) and math.isnan(value) else value)
                for key, value in values.items()
            }
        return json.dumps(serialized, default=str)

    @staticmethod
    def _deserialize_result(raw: str) -> dict[str, pd.Series] | None:
        try:
            data = json.loads(raw)
            result = {}
            for symbol, values in data.items():
                series = pd.Series(values)
                if series.dtype == object:
                    series = series.replace({None: float("nan")})
                result[symbol] = series
            return result
        except Exception:
            return None

    def save_to_parquet(
        self,
        expr_hash: str,
        trade_date: date,
        series: pd.Series,
        expression: str = "",
        engine: str = "builtin",
    ) -> None:
        from app.data_stores import get_market_data_store

        try:
            rows = [
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "expr_hash": expr_hash,
                    "value": float(value),
                    "engine": engine,
                    "expression": expression,
                    "updated_at": pd.Timestamp.now(),
                }
                for symbol, value in series.dropna().items()
            ]
            if rows:
                get_market_data_store().write_dataset(
                    pd.DataFrame(rows),
                    dataset="factor_cache",
                    date_col="trade_date",
                )
        except Exception:
            logger.debug("Parquet factor cache save failed", exc_info=True)

    def get_from_parquet(
        self,
        expr_hash: str,
        symbols: list[str],
        trade_date: date,
    ) -> pd.Series | None:
        try:
            from app.data_stores import get_market_data_store
            from app.data_stores.parquet_store import _list_param
            from app.db.duckdb import get_duckdb

            store = get_market_data_store()
            if not store._exists("factor_cache"):
                return None
            pattern = store._glob_pattern("factor_cache")
            rows = get_duckdb().execute(
                f"""
                SELECT symbol, value
                FROM read_parquet('{pattern}', hive_partitioning=true)
                WHERE expr_hash = '{expr_hash}'
                  AND trade_date = '{trade_date}'
                  AND symbol IN {_list_param(symbols)}
                """
            ).fetchall()
            if rows:
                return pd.Series({row[0]: row[1] for row in rows})
        except Exception:
            logger.debug("Parquet factor cache read failed", exc_info=True)
        return None

    def clear_l1(self) -> None:
        self.l1.clear()


_compute_cache: ComputeCache | None = None


def get_compute_cache() -> ComputeCache:
    global _compute_cache
    if _compute_cache is None:
        _compute_cache = ComputeCache()
    return _compute_cache


def reset_compute_cache() -> None:
    global _compute_cache
    _compute_cache = ComputeCache()

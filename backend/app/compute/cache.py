"""Compute result cache: process LRU + Redis + Parquet factor_cache."""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
import uuid
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from loguru import logger

from app.cache.redis_cache import get_redis_client as _get_redis_client
from app.core.config import settings

_REDIS_PREFIX = "compute:v2:"
_DEFAULT_DATA_VERSION = "klines_daily:v1"
_CACHE_GENERATION_PATH = Path(settings.base_dir) / ".runtime" / "compute-cache-generation"


def _read_cache_generation() -> str:
    try:
        return _CACHE_GENERATION_PATH.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return ""


def _bump_cache_generation() -> str:
    token = f"{time.time_ns()}-{uuid.uuid4().hex}"
    _CACHE_GENERATION_PATH.parent.mkdir(parents=True, exist_ok=True)
    temporary = _CACHE_GENERATION_PATH.with_name(
        f"{_CACHE_GENERATION_PATH.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        temporary.write_text(token, encoding="ascii")
        os.replace(temporary, _CACHE_GENERATION_PATH)
    finally:
        temporary.unlink(missing_ok=True)
    return token


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
        self._generation = _read_cache_generation()

    def _refresh_generation(self) -> None:
        generation = _read_cache_generation()
        if generation != self._generation:
            self.l1.clear()
            self._generation = generation

    def current_data_version(self) -> str:
        self._refresh_generation()
        return f"{_DEFAULT_DATA_VERSION}:{self._generation or 'initial'}"

    @staticmethod
    def make_key(
        full_expression: str,
        *,
        symbols: Sequence[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        engine: str = "builtin",
        data_version: str = _DEFAULT_DATA_VERSION,
    ) -> str:
        normalized = full_expression.strip().lower().replace(" ", "")
        has_context = symbols is not None or start_date is not None or end_date is not None
        if not has_context and engine == "builtin" and data_version == _DEFAULT_DATA_VERSION:
            payload = normalized
        else:
            payload = json.dumps(
                {
                    "expression": normalized,
                    "symbols": sorted(set(symbols or [])),
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None,
                    "engine": engine,
                    "data_version": data_version,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    @staticmethod
    def make_persistent_key(full_expression: str, *, engine: str) -> str:
        if engine not in {"builtin", "akquant"}:
            raise ValueError("engine must be builtin or akquant")
        normalized = full_expression.strip().lower().replace(" ", "")
        payload = json.dumps(
            {"expression": normalized, "engine": engine, "schema": "factor-cache-v2"},
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]

    def get(
        self,
        expression: str,
        *,
        symbols: Sequence[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        engine: str = "builtin",
        data_version: str = _DEFAULT_DATA_VERSION,
    ) -> dict[str, pd.Series] | None:
        self._refresh_generation()
        key = self.make_key(
            expression,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            engine=engine,
            data_version=data_version,
        )
        cached = self.l1.get(key)
        if cached is not None:
            return cached

        try:
            redis_val = _get_redis_client().get(f"{_REDIS_PREFIX}{key}")
            if redis_val is not None:
                deserialized = self._deserialize_result(redis_val)
                if deserialized is not None:
                    self.l1.set(key, deserialized)
                    return deserialized
        except Exception:
            logger.debug("Redis get failed for key={}", key[:8], exc_info=True)
        return None

    def set(
        self,
        expression: str,
        result: dict[str, pd.Series],
        *,
        symbols: Sequence[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        engine: str = "builtin",
        data_version: str = _DEFAULT_DATA_VERSION,
    ) -> None:
        self._refresh_generation()
        key = self.make_key(
            expression,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            engine=engine,
            data_version=data_version,
        )
        max_points = int(os.getenv("COMPUTE_CACHE_MAX_POINTS", "200000"))
        total_points = sum(len(series) for series in result.values())
        if total_points > max_points:
            logger.info(
                "Compute cache skipped for key={}: result too large ({} points)",
                key[:8],
                total_points,
            )
            return

        try:
            serialized = self._serialize_result(result)
        except Exception:
            logger.debug("Compute cache serialization failed for key={}", key[:8], exc_info=True)
            return

        self.l1.set(key, result)
        try:
            _get_redis_client().set(f"{_REDIS_PREFIX}{key}", serialized, ttl=3600)
        except Exception:
            logger.debug("Redis set failed for key={}", key[:8], exc_info=True)

    @staticmethod
    def _serialize_result(result: dict[str, pd.Series]) -> str:
        serialized: dict[str, dict[str, Any]] = {}
        for symbol, series in result.items():
            datetime_index = isinstance(series.index, pd.DatetimeIndex) or all(
                isinstance(item, (date, datetime, pd.Timestamp)) for item in series.index
            )
            index = [
                pd.Timestamp(item).isoformat() if datetime_index else item
                for item in series.index
            ]
            values: list[Any] = []
            for value in series.tolist():
                if value is None or value is pd.NA:
                    values.append(None)
                    continue
                if isinstance(value, float) and math.isnan(value):
                    values.append(None)
                    continue
                values.append(value.item() if hasattr(value, "item") else value)
            serialized[symbol] = {
                "index_type": "datetime" if datetime_index else "plain",
                "index": index,
                "values": values,
            }
        return json.dumps({"version": 2, "series": serialized}, default=str)

    @staticmethod
    def _deserialize_result(raw: str) -> dict[str, pd.Series] | None:
        try:
            data = json.loads(raw)
            result: dict[str, pd.Series] = {}
            if data.get("version") == 2 and isinstance(data.get("series"), dict):
                for symbol, payload in data["series"].items():
                    index = payload.get("index", [])
                    if payload.get("index_type") == "datetime":
                        index = pd.to_datetime(index)
                    result[symbol] = pd.Series(payload.get("values", []), index=index)
                return result

            # Backward-compatible reader for values written before cache schema v2.
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
        *,
        engine: str = "builtin",
    ) -> pd.Series | None:
        if engine not in {"builtin", "akquant"}:
            raise ValueError("engine must be builtin or akquant")
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
                  AND engine = '{engine}'
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

    def clear(self) -> int:
        self._generation = _bump_cache_generation()
        self.l1.clear()
        try:
            return _get_redis_client().delete_prefix(_REDIS_PREFIX)
        except Exception:
            logger.debug("Redis compute-cache invalidation failed", exc_info=True)
            return 0


_compute_cache: ComputeCache | None = None


def get_compute_cache() -> ComputeCache:
    global _compute_cache
    if _compute_cache is None:
        _compute_cache = ComputeCache()
    return _compute_cache


def reset_compute_cache() -> None:
    global _compute_cache
    _compute_cache = ComputeCache()

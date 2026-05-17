"""Redis helpers for coarse-grained backtest caches."""

from __future__ import annotations

import json
import zlib
from typing import Any

from loguru import logger
import redis

from app.cache.redis_cache import get_redis_client
from app.core.config import settings


class BacktestRedisCache:
    """Small wrapper around Redis for compressed JSON payloads."""

    def __init__(self, namespace: str = "bt:v4:v1", ttl: int = 86400 * 30):
        self.namespace = namespace.strip(":")
        self.ttl = ttl
        self._redis = None
        self._binary_redis = None

    @property
    def available(self) -> bool:
        client = self._client()
        return bool(client and client.available)

    def key(self, *parts: Any) -> str:
        clean_parts = [self.namespace]
        for part in parts:
            text = str(part).replace(" ", "").replace(":", "_")
            clean_parts.append(text)
        return ":".join(clean_parts)

    def get_json(self, key: str) -> Any | None:
        client = self._client()
        if client is None or not client.available:
            return None
        raw = client.get(key)
        if raw is None:
            return None
        try:
            if isinstance(raw, str):
                raw_bytes = raw.encode("latin1")
            else:
                raw_bytes = raw
            return json.loads(zlib.decompress(raw_bytes).decode("utf-8"))
        except Exception:
            try:
                return json.loads(raw)
            except Exception as exc:
                logger.debug("Backtest Redis decode failed for {}: {}", key, exc)
                return None

    def set_json(self, key: str, value: Any, ttl: int | None = None) -> None:
        client = self._client()
        if client is None or not client.available:
            return
        try:
            payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
            compressed = zlib.compress(payload.encode("utf-8"), level=5)
            client.set(key, compressed.decode("latin1"), ttl=ttl or self.ttl)
        except Exception as exc:
            logger.debug("Backtest Redis set failed for {}: {}", key, exc)

    def get_bytes(self, key: str) -> bytes | None:
        client = self._binary_client()
        if client is None:
            return None
        try:
            return client.get(key)
        except Exception as exc:
            logger.debug("Backtest Redis binary get failed for {}: {}", key, exc)
            return None

    def _client(self):
        if self._redis is None:
            try:
                self._redis = get_redis_client()
            except Exception:
                self._redis = None
        return self._redis

    def _binary_client(self):
        if self._binary_redis is not None:
            return self._binary_redis
        try:
            client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password or None,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
                decode_responses=False,
            )
            client.ping()
            self._binary_redis = client
            return client
        except Exception:
            self._binary_redis = None
            return None


_backtest_cache: BacktestRedisCache | None = None


def get_backtest_cache() -> BacktestRedisCache:
    global _backtest_cache
    if _backtest_cache is None:
        _backtest_cache = BacktestRedisCache()
    return _backtest_cache

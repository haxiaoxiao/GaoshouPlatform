"""三级缓存管理器 — L1 内存 LRU → L1.5 Redis → L2 ClickHouse → L3 原始数据"""
from __future__ import annotations

import hashlib
import json
import threading
from collections import OrderedDict
from datetime import date
from typing import Any

import pandas as pd
from clickhouse_driver import Client
from loguru import logger

from app.cache.redis_cache import get_redis_client as _get_redis_client


class LRUCache:
    """线程安全的 LRU 内存缓存"""

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
    """三级因子计算缓存

    L1: 进程内 LRU dict（请求级复用）
    L2: ClickHouse factor_cache 表（跨请求持久化）
    L3: ClickHouse klines_daily（原始数据，不算入缓存逻辑）
    """

    def __init__(self, ch_client: Client | None = None):
        self.l1 = LRUCache(max_size=256)
        self._ch = ch_client

    @property
    def ch_client(self) -> Client:
        if self._ch is None:
            from app.db.clickhouse import get_ch_client
            self._ch = get_ch_client()
        return self._ch

    @staticmethod
    def make_key(full_expression: str) -> str:
        """生成规范化表达式 hash key（16 字符 hex）"""
        normalized = full_expression.strip().lower().replace(" ", "")
        return hashlib.sha256(normalized.encode()).hexdigest()[:16]

    def get(self, expression: str) -> dict[str, pd.Series] | None:
        """从缓存获取计算结果

        L1: 进程内 LRU → L1.5: Redis → 回源（调用方自行走 L2 ClickHouse）
        """
        key = self.make_key(expression)

        # L1: 内存 LRU（最快）
        cached = self.l1.get(key)
        if cached is not None:
            return cached

        # L1.5: Redis（跨进程共享）
        try:
            redis_val = _get_redis_client().get(key)
            if redis_val is not None:
                deserialized = self._deserialize_result(redis_val)
                if deserialized is not None:
                    # 回写 L1（缓存预热）
                    self.l1.set(key, deserialized)
                    return deserialized
        except Exception:
            logger.debug("Redis get failed for key=%s", key[:8], exc_info=True)

        return None

    def set(self, expression: str, result: dict[str, pd.Series]) -> None:
        """写入缓存

        L1 内存 LRU + L1.5 Redis（TTL=3600s）
        """
        key = self.make_key(expression)

        # L1: 内存 LRU
        self.l1.set(key, result)

        # L1.5: Redis（后台写入，失败静默降级）
        try:
            _get_redis_client().set(key, self._serialize_result(result), ttl=3600)
        except Exception:
            logger.debug("Redis set failed for key=%s", key[:8], exc_info=True)

    # ------------------------------------------------------------------
    # 序列化 / 反序列化 helper（dict[str, pd.Series] <-> JSON str）
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize_result(result: dict[str, pd.Series]) -> str:
        """将 dict[str, pd.Series] 序列化为 JSON 字符串（NaN → None 保证跨平台兼容）"""
        import math

        serialized: dict[str, dict] = {}
        for sym, ser in result.items():
            d = ser.to_dict()
            d = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in d.items()}
            serialized[sym] = d
        return json.dumps(serialized, default=str)

    @staticmethod
    def _deserialize_result(raw: str) -> dict[str, pd.Series] | None:
        """将 JSON 字符串反序列化为 dict[str, pd.Series]（None → NaN）"""
        try:
            data = json.loads(raw)
            result = {}
            for sym, vals in data.items():
                ser = pd.Series(vals)
                if ser.dtype == object:
                    ser = ser.replace({None: float("nan")})
                result[sym] = ser
            return result
        except Exception:
            return None

    def get_from_ch(
        self,
        expr_hash: str,
        symbols: list[str],
        trade_date: date,
    ) -> pd.Series | None:
        """从 L2 ClickHouse 读取预计算结果"""
        try:
            rows = self.ch_client.execute(
                """
                SELECT symbol, value FROM factor_cache
                WHERE expr_hash = %(h)s AND trade_date = %(d)s
                AND symbol IN %(syms)s
                """,
                {"h": expr_hash, "d": trade_date, "syms": symbols},
            )
            if rows:
                return pd.Series({r[0]: r[1] for r in rows})
        except Exception:
            pass
        return None

    def save_to_ch(
        self,
        expr_hash: str,
        trade_date: date,
        series: pd.Series,
    ) -> None:
        """写入 L2 ClickHouse 预计算结果"""
        try:
            rows = [
                {"symbol": sym, "trade_date": trade_date, "expr_hash": expr_hash, "value": float(val)}
                for sym, val in series.dropna().items()
            ]
            if rows:
                self.ch_client.execute(
                    """
                    INSERT INTO factor_cache (symbol, trade_date, expr_hash, value)
                    VALUES
                    """,
                    rows,
                )
        except Exception:
            pass

    def clear_l1(self) -> None:
        """清空 L1 缓存"""
        self.l1.clear()


# 全局单例
_compute_cache: ComputeCache | None = None


def get_compute_cache() -> ComputeCache:
    global _compute_cache
    if _compute_cache is None:
        _compute_cache = ComputeCache()
    return _compute_cache


def reset_compute_cache() -> None:
    global _compute_cache
    _compute_cache = ComputeCache()

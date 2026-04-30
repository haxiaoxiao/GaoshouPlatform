# backend/app/cache/redis_cache.py
"""Redis cache client wrapper"""

import json
import logging
from typing import Any, Optional

import redis

from app.core.config import settings

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis 客户端封装，使用连接池"""

    def __init__(
        self,
        host: str = settings.redis_host,
        port: int = settings.redis_port,
        db: int = settings.redis_db,
        password: str = settings.redis_password,
        socket_timeout: float = 2.0,
        socket_connect_timeout: float = 2.0,
        decode_responses: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._available: bool = False

        try:
            self._pool = redis.ConnectionPool(
                host=host,
                port=port,
                db=db,
                password=password or None,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout,
                decode_responses=decode_responses,
            )
            # 尝试验证连接
            r = redis.Redis(connection_pool=self._pool)
            r.ping()
            self._available = True
            logger.info(
                "Redis connected at %s:%s/%s",
                host,
                port,
                db,
            )
        except Exception as e:
            self._available = False
            self._pool = None  # type: ignore[assignment]
            logger.warning("Redis unavailable at %s:%s: %s", host, port, e)

    @property
    def available(self) -> bool:
        return self._available

    def get(self, key: str) -> Optional[str]:
        """获取缓存值"""
        if not self._available or self._pool is None:
            return None
        try:
            r = redis.Redis(connection_pool=self._pool)
            return r.get(key)
        except Exception as e:
            logger.warning("Redis get(%s) failed: %s", key, e)
            return None

    def set(self, key: str, value: str, ttl: int = 3600) -> None:
        """设置缓存值，默认过期时间 3600 秒"""
        if not self._available or self._pool is None:
            return
        try:
            r = redis.Redis(connection_pool=self._pool)
            r.set(key, value, ex=ttl)
        except Exception as e:
            logger.warning("Redis set(%s) failed: %s", key, e)

    def delete(self, key: str) -> None:
        """删除缓存键"""
        if not self._available or self._pool is None:
            return
        try:
            r = redis.Redis(connection_pool=self._pool)
            r.delete(key)
        except Exception as e:
            logger.warning("Redis delete(%s) failed: %s", key, e)

    def exists(self, key: str) -> bool:
        """检查缓存键是否存在"""
        if not self._available or self._pool is None:
            return False
        try:
            r = redis.Redis(connection_pool=self._pool)
            result = r.exists(key)
            return bool(result)
        except Exception as e:
            logger.warning("Redis exists(%s) failed: %s", key, e)
            return False

    def serialize(self, value: Any) -> str:
        """将 Python 对象序列化为 JSON 字符串"""
        return json.dumps(value, default=str)

    def deserialize(self, value: str) -> Any:
        """将 JSON 字符串反序列化为 Python 对象"""
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("Redis deserialize failed: %s", e)
            return None

    def close(self) -> None:
        """关闭连接池"""
        if self._pool is not None:
            try:
                self._pool.disconnect()
            except Exception as e:
                logger.warning("Redis disconnect error: %s", e)
        self._available = False


# 全局单例
_redis_client: Optional[RedisClient] = None


def get_redis_client() -> RedisClient:
    """获取全局 Redis 客户端（单例）"""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
    return _redis_client


def reset_redis_client() -> None:
    """重置全局 Redis 客户端（测试用）"""
    global _redis_client
    if _redis_client is not None:
        _redis_client.close()
    _redis_client = None

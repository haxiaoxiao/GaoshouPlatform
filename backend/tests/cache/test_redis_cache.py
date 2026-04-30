# backend/tests/cache/test_redis_cache.py
"""Redis 缓存客户端测试"""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from app.cache.redis_cache import RedisClient, get_redis_client, reset_redis_client


# ---------- fixtures ----------

@pytest.fixture(autouse=True)
def _reset_singleton():
    """每个测试前重置全局单例"""
    reset_redis_client()
    yield
    reset_redis_client()


@pytest.fixture
def mock_redis():
    """模拟 redis.Redis 实例"""
    with patch("app.cache.redis_cache.redis.ConnectionPool") as mock_pool, \
         patch("app.cache.redis_cache.redis.Redis") as mock_redis_cls:
        mock_instance = MagicMock()
        mock_instance.ping.return_value = True
        mock_redis_cls.return_value = mock_instance
        yield mock_instance


# ---------- 连接池与初始化 ----------

class TestRedisClientInit:
    def test_successful_connection(self, mock_redis):
        """连接成功时应标记 available=True"""
        client = RedisClient(host="localhost", port=6379, db=0)
        assert client.available is True

    def test_connection_failure(self):
        """连接失败时应标记 available=False 且不崩溃"""
        with patch(
            "app.cache.redis_cache.redis.ConnectionPool",
            side_effect=ConnectionError("connection refused"),
        ):
            client = RedisClient(host="bad_host", port=6379, db=0)
            assert client.available is False

    def test_redis_error_on_ping(self, mock_redis):
        """ping 失败时应标记 available=False"""
        mock_redis.ping.side_effect = ConnectionError("timeout")
        # 需要让 ConnectionPool 创建成功，但 ping 失败
        with patch("app.cache.redis_cache.redis.ConnectionPool") as mock_pool:
            with patch("app.cache.redis_cache.redis.Redis") as mock_cls:
                mock_instance = MagicMock()
                mock_instance.ping.side_effect = ConnectionError("timeout")
                mock_cls.return_value = mock_instance
                client = RedisClient(host="slow_host", port=6379, db=0)
                assert client.available is False

    def test_custom_settings(self, mock_redis):
        """应使用自定义连接参数"""
        client = RedisClient(host="myhost", port=9999, db=5, password="secret")
        assert client._host == "myhost"
        assert client._port == 9999
        assert client._db == 5
        assert client._password == "secret"


# ---------- 操作方法 ----------

class TestRedisClientGetSet:
    def test_get_existing_key(self, mock_redis):
        """get 应返回已存在的值"""
        mock_redis.get.return_value = "hello"
        client = RedisClient()
        result = client.get("mykey")
        assert result == "hello"
        mock_redis.get.assert_called_once_with("mykey")

    def test_get_missing_key(self, mock_redis):
        """get 对不存在的键应返回 None"""
        mock_redis.get.return_value = None
        client = RedisClient()
        result = client.get("missing")
        assert result is None

    def test_set_value(self, mock_redis):
        """set 应调用 redis 的 set 方法（含过期时间）"""
        client = RedisClient()
        client.set("mykey", "myvalue", ttl=600)
        mock_redis.set.assert_called_once_with("mykey", "myvalue", ex=600)

    def test_set_default_ttl(self, mock_redis):
        """set 不指定过期时间时应使用默认值"""
        client = RedisClient()
        client.set("mykey", "myvalue")
        mock_redis.set.assert_called_once_with("mykey", "myvalue", ex=3600)

    def test_delete_key(self, mock_redis):
        """delete 应调用 redis 的 delete 方法"""
        client = RedisClient()
        client.delete("mykey")
        mock_redis.delete.assert_called_once_with("mykey")

    def test_exists_true(self, mock_redis):
        """exists 对存在的键应返回 True"""
        mock_redis.exists.return_value = 1
        client = RedisClient()
        assert client.exists("mykey") is True

    def test_exists_false(self, mock_redis):
        """exists 对不存在的键应返回 False"""
        mock_redis.exists.return_value = 0
        client = RedisClient()
        assert client.exists("mykey") is False


# ---------- 优雅降级 ----------

class TestGracefulDegradation:
    def test_get_when_unavailable(self):
        """Redis 不可用时 get 应返回 None"""
        with patch(
            "app.cache.redis_cache.redis.ConnectionPool",
            side_effect=ConnectionError("connection refused"),
        ):
            client = RedisClient()
            assert client.get("key") is None

    def test_set_when_unavailable(self):
        """Redis 不可用时 set 不应该崩溃"""
        with patch(
            "app.cache.redis_cache.redis.ConnectionPool",
            side_effect=ConnectionError("connection refused"),
        ):
            client = RedisClient()
            client.set("key", "value")  # should not raise

    def test_delete_when_unavailable(self):
        """Redis 不可用时 delete 不应该崩溃"""
        with patch(
            "app.cache.redis_cache.redis.ConnectionPool",
            side_effect=ConnectionError("connection refused"),
        ):
            client = RedisClient()
            client.delete("key")  # should not raise

    def test_exists_when_unavailable(self):
        """Redis 不可用时 exists 应返回 False"""
        with patch(
            "app.cache.redis_cache.redis.ConnectionPool",
            side_effect=ConnectionError("connection refused"),
        ):
            client = RedisClient()
            assert client.exists("key") is False

    def test_redis_error_at_runtime(self, mock_redis):
        """运行中 Redis 报错应优雅降级"""
        mock_redis.get.side_effect = ConnectionError("lost connection")
        client = RedisClient()
        assert client.get("key") is None

    def test_redis_set_error_at_runtime(self, mock_redis):
        """运行中 set 报错不应崩溃"""
        mock_redis.set.side_effect = ConnectionError("lost connection")
        client = RedisClient()
        client.set("key", "value")  # should not raise


# ---------- 序列化 ----------

class TestSerialization:
    def test_serialize_dict(self, mock_redis):
        """serialize 应正确序列化字典"""
        client = RedisClient()
        result = client.serialize({"a": 1, "b": 2})
        assert result == '{"a": 1, "b": 2}'

    def test_deserialize_dict(self, mock_redis):
        """deserialize 应正确反序列化 JSON"""
        client = RedisClient()
        result = client.deserialize('{"a": 1, "b": 2}')
        assert result == {"a": 1, "b": 2}

    def test_deserialize_invalid(self, mock_redis):
        """deserialize 对无效 JSON 应返回 None"""
        client = RedisClient()
        result = client.deserialize("not valid json")
        assert result is None


# ---------- 连接池 ----------

class TestConnectionPool:
    def test_pool_created(self, mock_redis):
        """初始化时应创建连接池"""
        with patch("app.cache.redis_cache.redis.ConnectionPool") as mock_pool:
            client = RedisClient(host="localhost", port=6379, db=0)
            mock_pool.assert_called_once_with(
                host="localhost",
                port=6379,
                db=0,
                password=None,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
                decode_responses=True,
            )

    def test_close_disconnects_pool(self):
        """close 应断开连接池"""
        with patch("app.cache.redis_cache.redis.ConnectionPool") as mock_pool:
            mock_pool_instance = MagicMock()
            mock_pool.return_value = mock_pool_instance
            with patch("app.cache.redis_cache.redis.Redis") as mock_cls:
                mock_instance = MagicMock()
                mock_instance.ping.return_value = True
                mock_cls.return_value = mock_instance
                client = RedisClient()
                client.close()
                mock_pool_instance.disconnect.assert_called_once()
                assert client.available is False


# ---------- 全局单例 ----------

class TestSingleton:
    def test_get_redis_client_returns_same_instance(self, mock_redis):
        """get_redis_client 应返回单例"""
        c1 = get_redis_client()
        c2 = get_redis_client()
        assert c1 is c2

    def test_reset_redis_client_creates_new_instance(self, mock_redis):
        """reset_redis_client 后应创建新实例"""
        c1 = get_redis_client()
        reset_redis_client()
        with patch("app.cache.redis_cache.RedisClient") as mock_cls:
            c2 = get_redis_client()
            # 应创建新实例
            mock_cls.assert_called_once()

    def test_get_redis_client_when_not_available(self):
        """Redis 不可用时 get_redis_client 仍应返回客户端"""
        with patch(
            "app.cache.redis_cache.redis.ConnectionPool",
            side_effect=ConnectionError("connection refused"),
        ):
            client = get_redis_client()
            assert client.available is False
            assert client.get("key") is None

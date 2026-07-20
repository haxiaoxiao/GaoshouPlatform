"""缓存管理器测试"""
import json
from datetime import date
from unittest.mock import patch

import pandas as pd

from app.compute import cache as cache_module
from app.compute.cache import ComputeCache, LRUCache, get_compute_cache, reset_compute_cache


class TestLRUCache:
    def test_get_set(self):
        c = LRUCache(max_size=10)
        c.set("a", 1)
        assert c.get("a") == 1

    def test_miss(self):
        c = LRUCache(max_size=10)
        assert c.get("nonexistent") is None

    def test_eviction(self):
        c = LRUCache(max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.set("c", 3)
        assert c.get("a") is None
        assert c.get("b") == 2
        assert c.get("c") == 3

    def test_touch_on_get(self):
        c = LRUCache(max_size=2)
        c.set("a", 1)
        c.set("b", 2)
        c.get("a")
        c.set("c", 3)
        assert c.get("a") == 1
        assert c.get("b") is None


class TestComputeCache:
    def test_make_key_deterministic(self):
        k1 = ComputeCache.make_key("Mean($close, 20)")
        k2 = ComputeCache.make_key("Mean($close, 20)")
        assert k1 == k2

    def test_make_key_whitespace_insensitive(self):
        k1 = ComputeCache.make_key("Mean($close, 20)")
        k2 = ComputeCache.make_key("  mean($close,20)  ")
        assert k1 == k2

    def test_make_key_includes_evaluation_context(self):
        base = {
            "symbols": ["600519.SH", "000001.SZ"],
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 12, 31),
            "engine": "builtin",
        }

        key = ComputeCache.make_key("Mean($close, 20)", **base)

        assert key == ComputeCache.make_key(
            " mean($close,20) ",
            **{**base, "symbols": list(reversed(base["symbols"]))},
        )
        assert key != ComputeCache.make_key(
            "Mean($close, 20)",
            **{**base, "symbols": ["600519.SH"]},
        )
        assert key != ComputeCache.make_key(
            "Mean($close, 20)",
            **{**base, "end_date": date(2025, 1, 2)},
        )

    def test_persistent_key_separates_compute_engines(self):
        builtin = ComputeCache.make_persistent_key("Mean($close, 20)", engine="builtin")
        akquant = ComputeCache.make_persistent_key("Mean($close, 20)", engine="akquant")

        assert builtin != akquant

    def test_get_does_not_reuse_another_evaluation_context(self):
        cache = ComputeCache()
        context = {
            "symbols": ["600519.SH"],
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 12, 31),
            "engine": "builtin",
        }
        result = {"600519.SH": pd.Series([1.0])}

        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_get.return_value.get.return_value = None
            cache.set("Mean($close, 20)", result, **context)

            assert cache.get("Mean($close, 20)", **context) is result
            assert cache.get(
                "Mean($close, 20)",
                **{**context, "symbols": ["000001.SZ"]},
            ) is None

    def test_generation_change_invalidates_l1_across_cache_instances(self, monkeypatch, tmp_path):
        generation_path = tmp_path / "compute-cache-generation"
        monkeypatch.setattr(cache_module, "_CACHE_GENERATION_PATH", generation_path)
        first_process = ComputeCache()
        second_process = ComputeCache()
        context = {
            "symbols": ["600519.SH"],
            "start_date": date(2024, 1, 1),
            "end_date": date(2024, 12, 31),
            "engine": "builtin",
            "data_version": second_process.current_data_version(),
        }
        result = {"600519.SH": pd.Series([1.0])}

        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_get.return_value.get.return_value = None
            second_process.set("Mean($close, 20)", result, **context)
            assert second_process.get("Mean($close, 20)", **context) is result

            first_process.clear()

            assert second_process.get("Mean($close, 20)", **context) is None

    def test_l1_cache(self):
        reset_compute_cache()
        cache = get_compute_cache()
        cache.set("Mean($close, 20)", {"test": pd.Series([1, 2, 3])})
        result = cache.get("Mean($close, 20)")
        assert result is not None
        assert len(result["test"]) == 3


class TestComputeCacheRedis:
    """缓存整合 Redis 场景测试（mock Redis 客户端）"""

    KEY = ComputeCache.make_key("Mean($close, 20)")
    SERIES = pd.Series([1.0, 2.0, 3.0], index=pd.RangeIndex(3))
    RESULT = {"000001.SZ": SERIES}

    # ------------------------------------------------------------------
    # Redis set & get —— L1 miss, Redis hit
    # ------------------------------------------------------------------

    def test_redis_hit_warms_l1(self):
        """Redis 命中 → 反序列化返回且回写 L1"""
        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_redis = mock_get.return_value
            mock_redis.get.return_value = json.dumps(
                {"000001.SZ": {0: 1.0, 1: 2.0, 2: 3.0}}
            )

            cache = ComputeCache()
            result = cache.get("Mean($close, 20)")

            assert result is not None
            assert list(result["000001.SZ"].values) == [1.0, 2.0, 3.0]

            # L1 也被回写
            l1_val = cache.l1.get(TestComputeCacheRedis.KEY)
            assert l1_val is not None
            assert list(l1_val["000001.SZ"].values) == [1.0, 2.0, 3.0]

    def test_redis_miss_falls_through(self):
        """Redis 未命中 → 返回 None"""
        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_redis = mock_get.return_value
            mock_redis.get.return_value = None

            cache = ComputeCache()
            result = cache.get("Mean($close, 20)")
            assert result is None

    # ------------------------------------------------------------------
    # Redis set writes on set()
    # ------------------------------------------------------------------

    def test_set_writes_redis(self):
        """set() 写入 Redis 且 TTL=3600"""
        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_redis = mock_get.return_value

            cache = ComputeCache()
            cache.set("Mean($close, 20)", TestComputeCacheRedis.RESULT)

            # 验证 Redis.set 被调用，且携带序列化 JSON 与 TTL=3600
            mock_redis.set.assert_called_once()
            _args, kwargs = mock_redis.set.call_args
            assert kwargs.get("ttl") == 3600

            # 反序列化验证内容正确
            written = kwargs.get("value") if "value" in kwargs else _args[1]
            if isinstance(written, str):
                data = json.loads(written)
                assert "000001.SZ" in data["series"]

    # ------------------------------------------------------------------
    # 序列化 / 反序列化单元测试
    # ------------------------------------------------------------------

    def test_serialize_deserialize_roundtrip(self):
        """序列化 → 反序列化 往返一致性"""
        original = {"A": pd.Series({"x": 1.0, "y": 2.0})}
        raw = ComputeCache._serialize_result(original)
        restored = ComputeCache._deserialize_result(raw)

        assert restored is not None
        assert "A" in restored
        assert restored["A"]["x"] == 1.0
        assert restored["A"]["y"] == 2.0

    def test_serialize_deserialize_preserves_datetime_index(self):
        original = {
            "A": pd.Series(
                [1.0, 2.0],
                index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
            )
        }

        restored = ComputeCache._deserialize_result(ComputeCache._serialize_result(original))

        assert restored is not None
        assert isinstance(restored["A"].index, pd.DatetimeIndex)
        assert restored["A"].index[0].date() == date(2024, 1, 2)

    def test_deserialize_invalid_json_returns_none(self):
        """非法 JSON → _deserialize_result 返回 None"""
        assert ComputeCache._deserialize_result("{{broken}") is None
        assert ComputeCache._deserialize_result("") is None

    def test_redis_key_uses_make_key(self):
        """Redis 使用的 key 与 make_key() 一致"""
        expr = "Mean($close, 20)"
        expected_key = ComputeCache.make_key(expr)

        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_redis = mock_get.return_value

            cache = ComputeCache()
            cache.set(expr, TestComputeCacheRedis.RESULT)

            # Redis keys are namespaced so invalidation cannot touch unrelated data.
            args, _kwargs = mock_redis.set.call_args
            assert args[0] == f"compute:v2:{expected_key}"

    # ------------------------------------------------------------------
    # 降级场景 —— Redis 不可用
    # ------------------------------------------------------------------

    def test_redis_unavailable_get_falls_to_none(self):
        """Redis 连接失败 → get 仍然正常返回 None（无异常）"""
        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_get.side_effect = RuntimeError("Redis down")

            cache = ComputeCache()
            # L1 为空，Redis 抛异常 → 返回 None
            result = cache.get("Mean($close, 20)")
            assert result is None

    def test_redis_unavailable_set_does_not_crash(self):
        """Redis 连接失败 → set 不抛异常"""
        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_get.side_effect = RuntimeError("Redis down")

            cache = ComputeCache()
            # 不应抛出任何异常
            cache.set("Mean($close, 20)", TestComputeCacheRedis.RESULT)

            # L1 仍正常写入
            l1_val = cache.l1.get(TestComputeCacheRedis.KEY)
            assert l1_val is not None

    def test_redis_get_returns_none_no_crash(self):
        """Redis.get() 返回 None → 不崩溃，降级为 None"""
        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_redis = mock_get.return_value
            mock_redis.get.return_value = None

            cache = ComputeCache()
            result = cache.get("Mean($close, 20)")
            assert result is None

    def test_l1_hit_skips_redis(self):
        """L1 命中时不查询 Redis"""
        with patch("app.compute.cache._get_redis_client") as mock_get:
            mock_redis = mock_get.return_value

            cache = ComputeCache()
            # 预置 L1 数据
            cache.l1.set(TestComputeCacheRedis.KEY, TestComputeCacheRedis.RESULT)

            result = cache.get("Mean($close, 20)")
            assert result is not None

            # Redis.get 未被调用（L1 直接返回）
            mock_redis.get.assert_not_called()

    def test_clear_invalidates_l1_and_compute_redis_namespace(self):
        with patch("app.compute.cache._get_redis_client") as mock_get:
            cache = ComputeCache()
            cache.l1.set(TestComputeCacheRedis.KEY, TestComputeCacheRedis.RESULT)
            mock_get.return_value.delete_prefix.return_value = 3

            deleted = cache.clear()

            assert len(cache.l1) == 0
            assert deleted == 3
            mock_get.return_value.delete_prefix.assert_called_once_with("compute:v2:")

"""缓存管理器测试"""
import pandas as pd
from app.compute.cache import LRUCache, ComputeCache, get_compute_cache, reset_compute_cache


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

    def test_l1_cache(self):
        reset_compute_cache()
        cache = get_compute_cache()
        cache.set("Mean($close, 20)", {"test": pd.Series([1, 2, 3])})
        result = cache.get("Mean($close, 20)")
        assert result is not None
        assert len(result["test"]) == 3

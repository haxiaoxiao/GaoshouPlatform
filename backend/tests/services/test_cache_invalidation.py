"""Cache invalidation service tests."""


def test_invalidate_after_sync_clears_compute_and_matching_redis(monkeypatch):
    from app.services.cache_invalidation import invalidate_after_sync

    cleared = {"l1": False}
    deleted = []

    class FakeComputeCache:
        def clear_l1(self):
            cleared["l1"] = True

    class FakeBinaryClient:
        def scan_iter(self, match, count):
            assert match == "bt:test:*"
            yield b"bt:test:timer_coverage:abc"
            yield b"bt:test:index_components:399101.SZ:2025-01-02"
            yield b"bt:test:other:keep"

        def delete(self, key):
            deleted.append(key)
            return 1

    class FakeBacktestCache:
        available = True
        namespace = "bt:test"

        def _binary_client(self):
            return FakeBinaryClient()

    monkeypatch.setattr("app.services.cache_invalidation.get_compute_cache", lambda: FakeComputeCache())
    monkeypatch.setattr("app.services.cache_invalidation.get_backtest_cache", lambda: FakeBacktestCache())

    result = invalidate_after_sync("kline_minute")

    assert cleared["l1"] is True
    assert result["redis_deleted"] == 1
    assert deleted == [b"bt:test:timer_coverage:abc"]

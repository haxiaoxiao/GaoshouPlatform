"""Timer coverage Redis cache tests."""

from datetime import date, time


def test_find_earliest_timer_coverage_uses_redis_cache(monkeypatch):
    from app.services import timer_minute_sync as svc

    class FakeCache:
        def key(self, *parts):
            return "bt:test:" + ":".join(map(str, parts))

        def get_json(self, key):
            assert key.startswith("bt:test:timer_coverage")
            return {"earliest_date": "2025-01-02", "coverage": []}

        def set_json(self, key, value, ttl=None):
            raise AssertionError("cache hit should not write")

    monkeypatch.setattr(svc, "get_backtest_cache", lambda: FakeCache())
    monkeypatch.setattr(svc, "_should_use_clickhouse", lambda: False)

    result = svc.find_earliest_timer_coverage_date(
        symbols=["000001.SZ"],
        start=date(2025, 1, 1),
        end=date(2025, 1, 31),
        timer_times=(time(10, 30),),
    )

    assert result["earliest_date"] == "2025-01-02"
    assert result["cache_hit"] is True

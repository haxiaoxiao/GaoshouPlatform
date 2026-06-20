"""System cache status API tests."""

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_system_cache_status_route(monkeypatch):
    class FakeRedis:
        available = True

    class FakeBacktestCache:
        available = True
        namespace = "bt:test"
        ttl = 123

    monkeypatch.setattr("app.api.system.get_redis_client", lambda: FakeRedis())
    monkeypatch.setattr("app.api.system.get_backtest_cache", lambda: FakeBacktestCache())

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/system/cache")

    body = resp.json()
    assert resp.status_code == 200
    assert body["redis"]["available"] is True
    assert body["backtest_cache"]["namespace"] == "bt:test"
    assert "factor_cache_parquet" == body["compute_cache"]["l2"]

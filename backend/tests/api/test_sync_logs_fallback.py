"""Sync log API fallback tests."""

from datetime import datetime

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_sync_logs_falls_back_to_local_store_when_sync_service_unavailable(monkeypatch):
    async def fake_proxy_sync_request(*args, **kwargs):
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail="sync service unavailable")

    class FakeLog:
        id = 1
        task_id = None
        sync_type = "stock_info"
        status = "completed"
        total_count = 10
        success_count = 10
        failed_count = 0
        start_time = datetime(2026, 5, 24, 9, 30, 0)
        end_time = datetime(2026, 5, 24, 9, 35, 0)
        error_message = None
        details = {"source": "local"}
        created_at = datetime(2026, 5, 24, 9, 35, 1)

    async def fake_get_sync_logs(self, sync_type=None, task_id=None, limit=50):
        assert sync_type == "stock_info"
        assert task_id is None
        assert limit == 10
        return [FakeLog()]

    monkeypatch.setattr("app.api.data.proxy_sync_request", fake_proxy_sync_request)
    monkeypatch.setattr("app.api.data.SyncService.get_sync_logs", fake_get_sync_logs)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/data/sync/logs", params={"sync_type": "stock_info", "limit": 10})

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert len(body["data"]) == 1
    assert body["data"][0]["sync_type"] == "stock_info"
    assert body["data"][0]["details"]["source"] == "local"

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


@pytest.mark.asyncio
async def test_sync_status_reports_trigger_availability_when_service_unavailable(monkeypatch):
    async def fake_sync_service_health():
        return {"healthy": False, "error": "dev sync service unavailable"}

    async def fake_proxy_sync_request(*args, **kwargs):
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail="dev sync service unavailable")

    async def fake_get_persisted_sync_status(self):
        return None

    monkeypatch.setattr("app.api.data.sync_service_health", fake_sync_service_health)
    monkeypatch.setattr("app.api.data.proxy_sync_request", fake_proxy_sync_request)
    monkeypatch.setattr("app.api.data.SyncService.get_persisted_sync_status", fake_get_persisted_sync_status)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/data/sync/status")

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["status"] == "idle"
    assert body["data"]["sync_service_available"] is False
    assert body["data"]["can_trigger"] is False
    assert "unavailable" in body["data"]["reason"]


@pytest.mark.asyncio
async def test_sync_status_prefers_live_sync_service_queue(monkeypatch):
    async def fake_sync_service_health():
        return {"healthy": True}

    async def fake_get_persisted_sync_status(self):
        return {
            "sync_type": "stock_info",
            "status": "running",
            "total": 10,
            "current": 1,
            "success_count": 1,
            "failed_count": 0,
            "progress_percent": 10.0,
            "start_time": None,
            "end_time": None,
            "error_message": None,
            "details": {},
        }

    async def fake_proxy_sync_request(method, path, **kwargs):
        assert method == "GET"
        assert path == "/api/data/sync/status"
        return {
            "code": 0,
            "message": "success",
            "data": {
                "sync_type": "index_daily",
                "status": "running",
                "total": 100,
                "current": 37,
                "success_count": 37,
                "failed_count": 0,
                "progress_percent": 37.0,
                "start_time": None,
                "end_time": None,
                "error_message": None,
                "details": {
                    "queue_mode": True,
                    "queue_pending_count": 2,
                    "queue_active_task_id": "run-live",
                },
            },
        }

    monkeypatch.setattr("app.api.data.sync_service_health", fake_sync_service_health)
    monkeypatch.setattr("app.api.data.proxy_sync_request", fake_proxy_sync_request)
    monkeypatch.setattr("app.api.data.SyncService.get_persisted_sync_status", fake_get_persisted_sync_status)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/data/sync/status")

    body = resp.json()["data"]
    assert resp.status_code == 200
    assert body["sync_type"] == "index_daily"
    assert body["progress_percent"] == 37.0
    assert body["sync_service_available"] is True
    assert body["can_trigger"] is True
    assert body["details"]["queue_pending_count"] == 2
    assert body["details"]["queue_active_task_id"] == "run-live"


@pytest.mark.asyncio
async def test_sync_status_marks_unavailable_when_live_status_proxy_fails(monkeypatch):
    async def fake_sync_service_health():
        return {"healthy": True}

    async def fake_proxy_sync_request(*args, **kwargs):
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail="sync status proxy down")

    async def fake_get_persisted_sync_status(self):
        return {
            "sync_type": "tushare_relay",
            "status": "running",
            "total": 100,
            "current": 37,
            "success_count": 37,
            "failed_count": 0,
            "progress_percent": 37.0,
            "start_time": None,
            "end_time": None,
            "error_message": None,
            "details": {},
        }

    monkeypatch.setattr("app.api.data.sync_service_health", fake_sync_service_health)
    monkeypatch.setattr("app.api.data.proxy_sync_request", fake_proxy_sync_request)
    monkeypatch.setattr("app.api.data.SyncService.get_persisted_sync_status", fake_get_persisted_sync_status)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/data/sync/status")

    body = resp.json()["data"]
    assert resp.status_code == 200
    assert body["status"] == "running"
    assert body["sync_service_available"] is False
    assert body["can_trigger"] is False
    assert body["reason"] == "sync status proxy down"

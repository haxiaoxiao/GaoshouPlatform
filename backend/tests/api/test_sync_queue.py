from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from app.db.sqlite import get_async_session
from app.services.task_queue import get_task_queue, reset_task_queues
from app.sync_main import app as sync_app


@pytest.mark.asyncio
async def test_sync_endpoint_accepts_multiple_requests_and_runs_them_serially(monkeypatch):
    run_order: list[str] = []
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def fake_session():
        yield object()

    async def fake_upsert_sync_run(*args, **kwargs):
        return None

    async def fake_get_persisted_sync_status(*args, **kwargs):
        raise AssertionError("sync trigger should enqueue instead of rejecting active runs")

    async def fake_run_sync_task(run_id, request):
        run_order.append(request.sync_type)
        if request.sync_type == "stock_info":
            first_started.set()
            await release_first.wait()

    reset_task_queues()
    sync_app.dependency_overrides[get_async_session] = fake_session
    monkeypatch.setattr("app.api.sync.upsert_sync_run", fake_upsert_sync_run)
    monkeypatch.setattr("app.api.sync.SyncService.get_persisted_sync_status", fake_get_persisted_sync_status)
    monkeypatch.setattr("app.api.sync._run_sync_task", fake_run_sync_task)

    try:
        async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
            first = await client.post("/api/data/sync", json={"sync_type": "stock_info"})
            assert first.status_code == 200

            await asyncio.wait_for(first_started.wait(), timeout=2)

            second = await client.post("/api/data/sync", json={"sync_type": "financial_data"})
            assert second.status_code == 200
            assert second.json()["data"]["status"] == "queued"

            release_first.set()
            await asyncio.wait_for(get_task_queue("data_sync").join(), timeout=2)
    finally:
        sync_app.dependency_overrides.clear()
        reset_task_queues()

    assert run_order == ["stock_info", "financial_data"]


@pytest.mark.asyncio
async def test_sync_status_stays_triggerable_while_queue_is_busy(monkeypatch):
    async def fake_session():
        yield object()

    async def fake_get_persisted_sync_status(*args, **kwargs):
        return {
            "run_id": "sync-busy",
            "task_id": "sync-busy",
            "sync_type": "stock_info",
            "status": "running",
            "total": 10,
            "current": 3,
            "success_count": 3,
            "failed_count": 0,
            "progress_percent": 30.0,
            "start_time": None,
            "end_time": None,
            "error_message": None,
            "details": {},
        }

    reset_task_queues()
    sync_app.dependency_overrides[get_async_session] = fake_session
    monkeypatch.setattr("app.api.sync.SyncService.get_persisted_sync_status", fake_get_persisted_sync_status)

    try:
        async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
            response = await client.get("/api/data/sync/status")
            assert response.status_code == 200
            body = response.json()["data"]
    finally:
        sync_app.dependency_overrides.clear()
        reset_task_queues()

    assert body["status"] == "running"
    assert body["can_trigger"] is True
    assert body["sync_service_available"] is True
    assert body["details"]["queue_mode"] is True

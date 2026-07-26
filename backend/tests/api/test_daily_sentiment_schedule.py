from __future__ import annotations

from datetime import datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.api import sync as sync_api
from app.db.models.sync import SyncTask
from app.db.sqlite import async_session_factory
from app.sync_main import app as sync_app

NEXT_RUN = datetime(2026, 7, 21, 22, 30)


class FakeScheduler:
    def __init__(self) -> None:
        self.jobs: dict[str, object] = {}

    def get_job(self, job_id: str) -> object | None:
        return self.jobs.get(job_id)


async def seed_daily_sentiment_task(
    *,
    enabled: bool,
    next_run_at: datetime | None = NEXT_RUN,
) -> int:
    async with async_session_factory() as session:
        task = SyncTask(
            name="每日舆情增量",
            cron_expression="30 22 * * *",
            sync_type="sentiment",
            failure_strategy="skip",
            retry_count=3,
            enabled=enabled,
            next_run_at=next_run_at,
        )
        session.add(task)
        await session.commit()
        await session.refresh(task)
        return task.id


def install_scheduler_fakes(monkeypatch, scheduler: FakeScheduler):
    reload_calls = 0

    async def fake_reload_scheduler_tasks() -> int:
        nonlocal reload_calls
        reload_calls += 1
        async with async_session_factory() as session:
            result = await session.execute(
                select(SyncTask).where(SyncTask.name == "每日舆情增量")
            )
            task = result.scalar_one()
            job_id = f"sync_task_{task.id}"
            if task.enabled:
                scheduler.jobs[job_id] = object()
                task.next_run_at = NEXT_RUN
            else:
                scheduler.jobs.pop(job_id, None)
                task.next_run_at = None
            await session.commit()
        return len(scheduler.jobs)

    monkeypatch.setattr(sync_api, "get_scheduler", lambda: scheduler, raising=False)
    monkeypatch.setattr(
        sync_api,
        "reload_scheduler_tasks",
        fake_reload_scheduler_tasks,
        raising=False,
    )
    return lambda: reload_calls


@pytest.mark.asyncio
async def test_get_daily_sentiment_schedule_returns_persisted_and_runtime_state(monkeypatch):
    task_id = await seed_daily_sentiment_task(enabled=True)
    scheduler = FakeScheduler()
    scheduler.jobs[f"sync_task_{task_id}"] = object()
    install_scheduler_fakes(monkeypatch, scheduler)

    async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
        response = await client.get("/api/data/sync/scheduler/daily-sentiment")

    assert response.status_code == 200
    assert response.json()["data"] == {
        "task_id": task_id,
        "name": "每日舆情增量",
        "enabled": True,
        "cron_expression": "30 22 * * *",
        "last_run_at": None,
        "next_run_at": NEXT_RUN.isoformat(),
        "scheduler_job_present": True,
    }


@pytest.mark.asyncio
async def test_put_daily_sentiment_schedule_disables_future_runs(monkeypatch):
    task_id = await seed_daily_sentiment_task(enabled=True)
    scheduler = FakeScheduler()
    scheduler.jobs[f"sync_task_{task_id}"] = object()
    reload_calls = install_scheduler_fakes(monkeypatch, scheduler)

    async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
        response = await client.put(
            "/api/data/sync/scheduler/daily-sentiment",
            json={"enabled": False},
        )

    assert response.status_code == 200
    assert response.json()["data"]["enabled"] is False
    assert response.json()["data"]["next_run_at"] is None
    assert response.json()["data"]["scheduler_job_present"] is False
    assert reload_calls() == 1


@pytest.mark.asyncio
async def test_put_daily_sentiment_schedule_enables_future_runs(monkeypatch):
    task_id = await seed_daily_sentiment_task(enabled=False, next_run_at=None)
    scheduler = FakeScheduler()
    reload_calls = install_scheduler_fakes(monkeypatch, scheduler)

    async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
        response = await client.put(
            "/api/data/sync/scheduler/daily-sentiment",
            json={"enabled": True},
        )

    assert response.status_code == 200
    assert response.json()["data"]["enabled"] is True
    assert response.json()["data"]["next_run_at"] == NEXT_RUN.isoformat()
    assert response.json()["data"]["scheduler_job_present"] is True
    assert f"sync_task_{task_id}" in scheduler.jobs
    assert reload_calls() == 1


@pytest.mark.asyncio
async def test_put_daily_sentiment_schedule_is_idempotent(monkeypatch):
    task_id = await seed_daily_sentiment_task(enabled=False, next_run_at=None)
    scheduler = FakeScheduler()
    reload_calls = install_scheduler_fakes(monkeypatch, scheduler)

    async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
        response = await client.put(
            "/api/data/sync/scheduler/daily-sentiment",
            json={"enabled": False},
        )

    assert response.status_code == 200
    assert response.json()["data"]["enabled"] is False
    assert response.json()["data"]["scheduler_job_present"] is False
    assert f"sync_task_{task_id}" not in scheduler.jobs
    assert reload_calls() == 1


@pytest.mark.asyncio
async def test_daily_sentiment_schedule_returns_404_when_task_is_missing(monkeypatch):
    scheduler = FakeScheduler()
    install_scheduler_fakes(monkeypatch, scheduler)

    async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
        response = await client.get("/api/data/sync/scheduler/daily-sentiment")

    assert response.status_code == 404
    assert response.json()["detail"] == "daily sentiment schedule not found"


@pytest.mark.asyncio
async def test_reload_failure_keeps_persisted_daily_sentiment_state(monkeypatch):
    task_id = await seed_daily_sentiment_task(enabled=True)
    scheduler = FakeScheduler()
    scheduler.jobs[f"sync_task_{task_id}"] = object()
    monkeypatch.setattr(sync_api, "get_scheduler", lambda: scheduler, raising=False)

    async def fail_reload_scheduler_tasks() -> int:
        raise RuntimeError("scheduler unavailable")

    monkeypatch.setattr(
        sync_api,
        "reload_scheduler_tasks",
        fail_reload_scheduler_tasks,
        raising=False,
    )

    async with AsyncClient(transport=ASGITransport(app=sync_app), base_url="http://test") as client:
        response = await client.put(
            "/api/data/sync/scheduler/daily-sentiment",
            json={"enabled": False},
        )

    async with async_session_factory() as session:
        persisted = await session.get(SyncTask, task_id)

    assert response.status_code == 500
    assert response.json()["detail"] == "daily sentiment scheduler reload failed"
    assert persisted is not None
    assert persisted.enabled is False
    assert persisted.next_run_at is None

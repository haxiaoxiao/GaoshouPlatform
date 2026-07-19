from __future__ import annotations

import asyncio
from datetime import date, datetime
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from app.services.task_queue import (
    MARKET_RADAR_QUEUE_NAME,
    QueuedTask,
    get_task_queue,
    reset_task_queues,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("sync_queue_name", ["data_sync", "sentiment_sync", "sync"])
async def test_radar_and_sync_queues_share_one_exclusive_writer_group(sync_queue_name):
    sync_started = asyncio.Event()
    release_sync = asyncio.Event()
    radar_started = asyncio.Event()

    async def sync_handler() -> None:
        sync_started.set()
        await release_sync.wait()

    async def radar_handler() -> None:
        radar_started.set()

    reset_task_queues()
    sync_queue = get_task_queue(sync_queue_name)
    radar_queue = get_task_queue(MARKET_RADAR_QUEUE_NAME)
    try:
        await sync_queue.submit(QueuedTask("sync-1", "sync", sync_handler))
        await asyncio.wait_for(sync_started.wait(), timeout=1)
        await radar_queue.submit(QueuedTask("radar-1", "radar", radar_handler))
        await asyncio.sleep(0.02)
        assert radar_started.is_set() is False

        release_sync.set()
        await asyncio.wait_for(radar_started.wait(), timeout=1)
        await asyncio.gather(sync_queue.join(), radar_queue.join())
    finally:
        release_sync.set()
        await asyncio.gather(sync_queue.join(), radar_queue.join())
        reset_task_queues()


@pytest.mark.asyncio
async def test_reset_task_queues_stops_idle_workers():
    queue = get_task_queue("reset-worker")

    async def handler() -> None:
        return None

    await queue.submit(QueuedTask("reset-1", "reset", handler))
    await queue.join()
    worker = queue._worker
    assert worker is not None and worker.done() is False

    try:
        reset_task_queues()
        await asyncio.sleep(0)
        assert worker.done() is True
    finally:
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_reset_task_queues_cancels_active_and_balances_pending_work():
    queue = get_task_queue("reset-active-worker")
    started = asyncio.Event()
    cancelled = asyncio.Event()
    pending_ran = False

    async def active_handler() -> None:
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def pending_handler() -> None:
        nonlocal pending_ran
        pending_ran = True

    await queue.submit(QueuedTask("active", "active", active_handler))
    await queue.submit(QueuedTask("pending", "pending", pending_handler))
    await asyncio.wait_for(started.wait(), timeout=1)
    worker = queue._worker
    assert worker is not None

    reset_task_queues()
    await asyncio.wait_for(cancelled.wait(), timeout=1)
    await asyncio.gather(worker, return_exceptions=True)
    await asyncio.wait_for(queue.join(), timeout=1)

    assert worker.done() is True
    assert pending_ran is False
    assert queue.pending_count == 0


@pytest.mark.asyncio
async def test_api_triggered_sync_cannot_overlap_radar(monkeypatch):
    from app.sync_main import app as sync_app

    sync_started = asyncio.Event()
    release_sync = asyncio.Event()
    radar_started = asyncio.Event()

    async def fake_session():
        yield object()

    async def fake_upsert(*_args, **_kwargs):
        return None

    async def fake_sync(*_args, **_kwargs):
        sync_started.set()
        await release_sync.wait()

    async def radar_handler() -> None:
        radar_started.set()

    reset_task_queues()
    sync_app.dependency_overrides.clear()
    from app.db.sqlite import get_async_session

    sync_app.dependency_overrides[get_async_session] = fake_session
    monkeypatch.setattr("app.api.sync.upsert_sync_run", fake_upsert)
    monkeypatch.setattr("app.api.sync._run_sync_task", fake_sync)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=sync_app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/api/data/sync",
                json={"sync_type": "kline_daily"},
            )
            assert response.status_code == 200
            await asyncio.wait_for(sync_started.wait(), timeout=1)
            radar_queue = get_task_queue(MARKET_RADAR_QUEUE_NAME)
            await radar_queue.submit(QueuedTask("radar-api", "radar", radar_handler))
            await asyncio.sleep(0.02)
            assert radar_started.is_set() is False
            release_sync.set()
            await asyncio.wait_for(radar_started.wait(), timeout=1)
            await asyncio.gather(
                get_task_queue("sync").join(),
                get_task_queue("data_sync").join(),
                radar_queue.join(),
            )
    finally:
        release_sync.set()
        await asyncio.gather(
            get_task_queue("sync").join(),
            get_task_queue("data_sync").join(),
            get_task_queue(MARKET_RADAR_QUEUE_NAME).join(),
        )
        sync_app.dependency_overrides.clear()
        reset_task_queues()


@pytest.mark.asyncio
async def test_scheduler_triggered_sync_cannot_overlap_radar(monkeypatch):
    from app.core import scheduler

    sync_started = asyncio.Event()
    release_sync = asyncio.Event()
    radar_started = asyncio.Event()

    async def scheduled_sync(*_args, **_kwargs):
        sync_started.set()
        await release_sync.wait()

    async def radar_handler() -> None:
        radar_started.set()

    reset_task_queues()
    monkeypatch.setattr(scheduler, "_run_scheduled_sync_job", scheduled_sync)
    try:
        await scheduler._execute_sync_job(7, "kline_daily")
        await asyncio.wait_for(sync_started.wait(), timeout=1)
        radar_queue = get_task_queue(MARKET_RADAR_QUEUE_NAME)
        await radar_queue.submit(QueuedTask("radar-cron", "radar", radar_handler))
        await asyncio.sleep(0.02)
        assert radar_started.is_set() is False
        release_sync.set()
        await asyncio.wait_for(radar_started.wait(), timeout=1)
        await asyncio.gather(
            get_task_queue("sync").join(),
            radar_queue.join(),
        )
    finally:
        release_sync.set()
        await asyncio.gather(
            get_task_queue("sync").join(),
            get_task_queue(MARKET_RADAR_QUEUE_NAME).join(),
        )
        reset_task_queues()


@pytest.mark.asyncio
async def test_scheduled_sync_success_notifies_radar_with_queued_run_id(monkeypatch):
    from app.core import scheduler
    from app.services import sync_service

    notifications: list[tuple[str, str]] = []

    class Session:
        async def execute(self, _statement):
            return None

        async def commit(self):
            return None

    class SessionContext:
        async def __aenter__(self):
            return Session()

        async def __aexit__(self, *_args):
            return None

    class Service:
        def __init__(self, _session):
            return None

        async def sync_kline_daily(self, **_kwargs):
            return SimpleNamespace(status="completed")

    async def notify(run_id: str, sync_type: str):
        notifications.append((run_id, sync_type))

    monkeypatch.setattr(scheduler, "async_session_factory", lambda: SessionContext())
    monkeypatch.setattr(sync_service, "SyncService", Service)
    monkeypatch.setattr(scheduler, "notify_market_radar_sync_completed", notify)
    monkeypatch.setattr("app.services.cache_invalidation.invalidate_after_sync", lambda _kind: {})

    await scheduler._run_scheduled_sync_job(
        7,
        "kline_daily",
        run_id="scheduled-sync-7-abc",
    )

    assert notifications == [("scheduled-sync-7-abc", "kline_daily")]


@pytest.mark.asyncio
@pytest.mark.parametrize("progress_status", ["failed", "cancelled"])
async def test_scheduled_sync_without_completed_progress_has_no_success_side_effects(
    monkeypatch,
    progress_status,
):
    from app.core import scheduler
    from app.services import sync_service

    calls: list[str] = []

    class Session:
        async def execute(self, _statement):
            calls.append("last_run")

        async def commit(self):
            calls.append("commit")

    class SessionContext:
        async def __aenter__(self):
            return Session()

        async def __aexit__(self, *_args):
            return None

    class Service:
        def __init__(self, _session):
            return None

        async def sync_kline_daily(self, **_kwargs):
            return SimpleNamespace(status=progress_status)

    async def notify(*_args):
        calls.append("notify")

    def invalidate(_kind):
        calls.append("invalidate")
        return {}

    monkeypatch.setattr(scheduler, "async_session_factory", lambda: SessionContext())
    monkeypatch.setattr(sync_service, "SyncService", Service)
    monkeypatch.setattr(scheduler, "notify_market_radar_sync_completed", notify)
    monkeypatch.setattr("app.services.cache_invalidation.invalidate_after_sync", invalidate)

    await scheduler._run_scheduled_sync_job(
        7,
        "kline_daily",
        run_id="scheduled-sync-7-failed",
    )

    assert calls == []


class FakeScheduler:
    def __init__(self) -> None:
        self.jobs: dict[str, SimpleNamespace] = {}

    def add_job(self, func, *, trigger, id, **kwargs):
        self.jobs[id] = SimpleNamespace(
            func=func,
            trigger=trigger,
            id=id,
            kwargs=kwargs,
        )
        return self.jobs[id]

    def remove_job(self, job_id: str) -> None:
        self.jobs.pop(job_id)


@pytest.mark.asyncio
async def test_sync_owner_registers_one_hong_kong_weekday_eod_job():
    from app.services.market_radar_runtime import MarketRadarRuntime

    scheduler = FakeScheduler()
    runtime = MarketRadarRuntime(run_eod=lambda *_args, **_kwargs: None)

    await runtime.start(scheduler)
    await runtime.start(scheduler)

    assert list(scheduler.jobs) == ["market-radar-eod"]
    trigger = scheduler.jobs["market-radar-eod"].trigger
    assert str(trigger.fields[4]) == "mon-fri"
    assert str(trigger.fields[5]) == "15"
    assert str(trigger.fields[6]) == "20"
    assert str(trigger.timezone) == "Asia/Hong_Kong"

    await runtime.stop()
    assert scheduler.jobs == {}


@pytest.mark.asyncio
async def test_runtime_stop_does_not_hide_unexpected_scheduler_errors():
    from app.services.market_radar_runtime import MarketRadarRuntime

    class BrokenScheduler(FakeScheduler):
        def remove_job(self, job_id: str) -> None:
            raise RuntimeError("scheduler storage failed")

    runtime = MarketRadarRuntime(run_eod=lambda *_args, **_kwargs: None)
    await runtime.start(BrokenScheduler())

    with pytest.raises(RuntimeError, match="scheduler storage failed"):
        await runtime.stop()

    reset_task_queues()


@pytest.mark.asyncio
async def test_runtime_stop_awaits_its_idle_queue_worker():
    from app.services.market_radar_runtime import MarketRadarRuntime, RadarEodResult

    async def run_eod(*_args):
        return RadarEodResult(status="fresh", deleted_intraday=0)

    runtime = MarketRadarRuntime(run_eod=run_eod)
    await runtime.start(FakeScheduler())
    await runtime.submit_eod(date(2026, 7, 18), reason="manual")
    queue = get_task_queue(MARKET_RADAR_QUEUE_NAME)
    await asyncio.wait_for(queue.join(), timeout=1)
    worker = queue._worker
    assert worker is not None and worker.done() is False

    await runtime.stop()

    assert worker.done() is True
    reset_task_queues()


@pytest.mark.asyncio
async def test_queue_shutdown_is_bounded_and_records_persistent_cancellation_failure(monkeypatch):
    from app.services import task_queue as task_queue_module

    cancellation_count = 0
    release = asyncio.Event()
    recorded: list[str] = []

    async def stubborn_worker():
        nonlocal cancellation_count
        while cancellation_count < 2:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancellation_count += 1
        await release.wait()

    queue = get_task_queue("stubborn-worker")
    worker = asyncio.create_task(stubborn_worker())
    queue._worker = worker
    await asyncio.sleep(0)
    monkeypatch.setattr(
        task_queue_module.logger,
        "error",
        lambda message, *args: recorded.append(message.format(*args)),
    )

    try:
        stopped = await asyncio.wait_for(queue.shutdown(timeout_seconds=0.01), timeout=0.1)
        assert stopped is False
        assert recorded == ["Task queue stubborn-worker worker did not stop after bounded retry"]
    finally:
        release.set()
        for _ in range(3):
            if worker.done():
                break
            worker.cancel()
            await asyncio.sleep(0)
        await asyncio.gather(worker, return_exceptions=True)
        reset_task_queues()


@pytest.mark.asyncio
async def test_eod_run_cleans_old_intraday_and_marks_partial_for_recompute():
    from app.services.market_radar_runtime import MarketRadarRuntime, RadarEodResult

    calls: list[tuple[date, datetime]] = []

    async def run_eod(target_date: date, cutoff: datetime) -> RadarEodResult:
        calls.append((target_date, cutoff))
        return RadarEodResult(status="partial", deleted_intraday=7)

    runtime = MarketRadarRuntime(
        run_eod=run_eod,
        clock=lambda: datetime(2026, 7, 20, 15, 20),
    )
    await runtime.start(FakeScheduler())
    try:
        task = await runtime.submit_eod(date(2026, 7, 18), reason="manual")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
        state = runtime.task_status(task.task_id)
    finally:
        await runtime.stop()
        reset_task_queues()

    assert calls == [(date(2026, 7, 18), datetime(2026, 4, 21, 15, 20))]
    assert state is not None
    assert state.status == "completed"
    assert state.snapshot_status == "partial"
    assert state.recompute_needed is True
    assert state.deleted_intraday == 7


@pytest.mark.asyncio
async def test_runtime_restores_partial_recompute_intent_after_restart():
    from app.services.market_radar_runtime import MarketRadarRuntime, RadarEodResult

    calls: list[date] = []

    async def load_dates():
        return (date(2026, 7, 18),)

    async def run_eod(target_date: date, _cutoff: datetime) -> RadarEodResult:
        calls.append(target_date)
        return RadarEodResult(status="fresh", deleted_intraday=0)

    runtime = MarketRadarRuntime(
        run_eod=run_eod,
        load_recompute_dates=load_dates,
    )
    await runtime.start(FakeScheduler())
    try:
        task = await runtime.notify_sync_completed("sync-after-restart", "kline_daily")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
    finally:
        await runtime.stop()
        reset_task_queues()

    assert task is not None
    assert task.target_date == date(2026, 7, 18)
    assert calls == [date(2026, 7, 18)]
    assert task.recompute_needed is False


@pytest.mark.asyncio
async def test_related_sync_run_recomputes_partial_date_at_most_once_even_if_still_partial():
    from app.services.market_radar_runtime import MarketRadarRuntime, RadarEodResult

    calls: list[date] = []

    async def run_eod(target_date: date, _cutoff: datetime) -> RadarEodResult:
        calls.append(target_date)
        return RadarEodResult(status="partial", deleted_intraday=0)

    runtime = MarketRadarRuntime(run_eod=run_eod)
    await runtime.start(FakeScheduler())
    try:
        first = await runtime.submit_eod(date(2026, 7, 18), reason="scheduled")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
        follow_up = await runtime.notify_sync_completed("sync-run-42", "kline_daily")
        duplicate = await runtime.notify_sync_completed("sync-run-42", "kline_daily")
        unrelated = await runtime.notify_sync_completed("sync-run-43", "financial_data")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
    finally:
        await runtime.stop()
        reset_task_queues()

    assert first.task_id != follow_up.task_id
    assert duplicate is None
    assert unrelated is None
    assert calls == [date(2026, 7, 18), date(2026, 7, 18)]
    assert runtime.task_status(follow_up.task_id).source_run_id == "sync-run-42"
    assert runtime.task_status(follow_up.task_id).recompute_needed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("sync_type", ["datasync", "stock_info", "stock_full"])
async def test_stock_universe_syncs_recompute_partial_eod(sync_type):
    from app.services.market_radar_runtime import MarketRadarRuntime, RadarEodResult

    async def run_eod(_target_date: date, _cutoff: datetime) -> RadarEodResult:
        return RadarEodResult(status="partial", deleted_intraday=0)

    runtime = MarketRadarRuntime(run_eod=run_eod)
    await runtime.start(FakeScheduler())
    try:
        await runtime.submit_eod(date(2026, 7, 18), reason="scheduled")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
        follow_up = await runtime.notify_sync_completed(f"run-{sync_type}", sync_type)
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
    finally:
        await runtime.stop()
        reset_task_queues()

    assert follow_up is not None
    assert follow_up.source_run_id == f"run-{sync_type}"


@pytest.mark.asyncio
async def test_fresh_recompute_of_latest_date_does_not_fall_back_to_older_partial():
    from app.services.market_radar_runtime import MarketRadarRuntime, RadarEodResult

    statuses = iter(("partial", "partial", "fresh"))

    async def run_eod(_target_date: date, _cutoff: datetime) -> RadarEodResult:
        return RadarEodResult(status=next(statuses), deleted_intraday=0)

    runtime = MarketRadarRuntime(run_eod=run_eod)
    await runtime.start(FakeScheduler())
    try:
        await runtime.submit_eod(date(2026, 7, 17), reason="scheduled")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
        await runtime.submit_eod(date(2026, 7, 18), reason="scheduled")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
        refreshed = await runtime.notify_sync_completed("run-latest", "kline_daily")
        await asyncio.wait_for(get_task_queue(MARKET_RADAR_QUEUE_NAME).join(), timeout=1)
        stale_follow_up = await runtime.notify_sync_completed("run-next", "kline_daily")
    finally:
        await runtime.stop()
        reset_task_queues()

    assert refreshed is not None
    assert refreshed.target_date == date(2026, 7, 18)
    assert refreshed.recompute_needed is False
    assert stale_follow_up is None


@pytest.mark.asyncio
async def test_sync_completion_hook_only_notifies_after_success(monkeypatch):
    from app.api import sync as sync_api

    calls: list[tuple[str, str]] = []

    async def notify(run_id: str, sync_type: str):
        calls.append((run_id, sync_type))

    monkeypatch.setattr(sync_api, "notify_market_radar_sync_completed", notify)

    await sync_api._notify_market_radar_after_sync(
        "sync-success",
        "kline_daily",
        SimpleNamespace(status="completed"),
    )
    await sync_api._notify_market_radar_after_sync(
        "sync-failed",
        "kline_daily",
        SimpleNamespace(status="failed"),
    )
    await sync_api._notify_market_radar_after_sync("sync-empty", "kline_daily", None)

    assert calls == [("sync-success", "kline_daily")]


@pytest.mark.asyncio
async def test_sync_internal_endpoint_only_accepts_eod_and_exposes_task_status(monkeypatch):
    from app import sync_main
    from app.services.market_radar_runtime import RadarRuntimeTask

    class Runtime:
        async def submit_eod(self, target_date, *, reason):
            assert target_date == date(2026, 7, 18)
            assert reason == "manual"
            return RadarRuntimeTask(
                task_id="radar-eod-1",
                status="queued",
                target_date=target_date,
                reason="manual",
                source_run_id=None,
                recompute_needed=False,
                snapshot_status=None,
                deleted_intraday=0,
                error=None,
            )

        def task_status(self, task_id):
            assert task_id == "radar-eod-1"
            return RadarRuntimeTask(
                task_id=task_id,
                status="completed",
                target_date=date(2026, 7, 18),
                reason="manual",
                source_run_id=None,
                recompute_needed=True,
                snapshot_status="partial",
                deleted_intraday=4,
                error=None,
            )

    monkeypatch.setattr(sync_main, "get_market_radar_runtime", lambda: Runtime())
    transport = httpx.ASGITransport(app=sync_main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        submitted = await client.post(
            "/internal/market-radar/eod",
            json={"trade_date": "2026-07-18"},
        )
        status = await client.get("/internal/market-radar/tasks/radar-eod-1")

    assert submitted.status_code == 202
    assert submitted.json()["data"]["refresh_kind"] == "eod"
    assert status.status_code == 200
    assert status.json()["data"]["snapshot_status"] == "partial"
    paths = {route.path for route in sync_main.app.routes if hasattr(route, "path")}
    assert "/internal/market-radar/intraday" not in paths


@pytest.mark.asyncio
async def test_sync_lifespan_owns_radar_runtime_and_stops_it(monkeypatch):
    from app import sync_main

    calls: list[str] = []

    class Runtime:
        started = False

        async def start(self, scheduler):
            self.started = True
            calls.append("radar_start")

        async def stop(self):
            self.started = False
            calls.append("radar_stop")

        async def notify_sync_completed(self, run_id, sync_type):
            assert self.started is True
            assert (run_id, sync_type) == ("recovered-run", "kline_daily")
            calls.append("recovered_sync_notified")

    runtime = Runtime()

    async def noop_async(*_args, **_kwargs):
        return None

    async def load_tasks():
        calls.append("load_tasks")

    async def recover_runs():
        calls.append("recover_runs")
        await runtime.notify_sync_completed("recovered-run", "kline_daily")

    async def shutdown_sync_queues(names):
        assert names == ("sync", "data_sync", "sentiment_sync")
        assert runtime.started is True
        calls.append("sync_queues_stop")
        calls.append("sync_completion_notified")
        return {name: True for name in names}

    monkeypatch.setattr(sync_main, "init_db", noop_async)
    monkeypatch.setattr(sync_main, "_mark_stale_sync_runs_after_startup", noop_async)
    monkeypatch.setattr(sync_main, "_recover_queued_sync_runs", recover_runs)
    monkeypatch.setattr(sync_main, "start_market_radar_runtime", lambda: runtime)
    monkeypatch.setattr(sync_main, "get_scheduler", lambda: object())
    monkeypatch.setattr(sync_main, "install_default_executor", lambda: calls.append("executor_start"))
    monkeypatch.setattr(sync_main, "shutdown_default_executor", lambda: calls.append("executor_stop"))
    monkeypatch.setattr(sync_main, "start_scheduler", lambda: calls.append("scheduler_start"))
    monkeypatch.setattr(sync_main, "stop_scheduler", lambda: calls.append("scheduler_stop"))
    monkeypatch.setattr(sync_main.settings, "enable_sync_scheduler", True)
    monkeypatch.setattr(sync_main, "load_enabled_tasks", load_tasks)
    monkeypatch.setattr(sync_main, "shutdown_task_queues", shutdown_sync_queues)

    async with sync_main.lifespan(FastAPI()):
        calls.append("serving")

    assert calls == [
        "executor_start",
        "scheduler_start",
        "radar_start",
        "load_tasks",
        "recover_runs",
        "recovered_sync_notified",
        "serving",
        "scheduler_stop",
        "sync_queues_stop",
        "sync_completion_notified",
        "radar_stop",
        "executor_stop",
    ]


@pytest.mark.asyncio
async def test_sync_lifespan_awaits_sync_queue_worker_shutdown(monkeypatch):
    from app import sync_main

    class Runtime:
        async def start(self, scheduler=None):
            return None

        async def stop(self):
            return None

    async def noop_async(*_args, **_kwargs):
        return None

    monkeypatch.setattr(sync_main, "init_db", noop_async)
    monkeypatch.setattr(sync_main, "_mark_stale_sync_runs_after_startup", noop_async)
    monkeypatch.setattr(sync_main, "_recover_queued_sync_runs", noop_async)
    monkeypatch.setattr(sync_main, "start_market_radar_runtime", Runtime)
    monkeypatch.setattr(sync_main, "install_default_executor", lambda: None)
    monkeypatch.setattr(sync_main, "shutdown_default_executor", lambda: None)
    monkeypatch.setattr(sync_main, "stop_scheduler", lambda: None)
    monkeypatch.setattr(sync_main.settings, "enable_sync_scheduler", False)
    reset_task_queues()

    async with sync_main.lifespan(FastAPI()):
        queue = get_task_queue("sync")
        await queue.submit(QueuedTask("sync-idle", "sync idle", noop_async))
        await asyncio.wait_for(queue.join(), timeout=1)
        worker = queue._worker
        assert worker is not None and worker.done() is False

    assert worker.done() is True
    reset_task_queues()

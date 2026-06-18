from __future__ import annotations

import asyncio

import pytest

from app.services.task_queue import QueuedTask, get_task_queue, reset_task_queues


@pytest.mark.asyncio
async def test_task_queue_cancels_active_task_without_stopping_worker():
    queue = get_task_queue("test-cancel")
    started = asyncio.Event()
    release = asyncio.Event()
    events: list[str] = []

    async def blocking_handler() -> None:
        events.append("first-started")
        started.set()
        await release.wait()

    async def next_handler() -> None:
        events.append("second-ran")

    try:
        await queue.submit(QueuedTask(task_id="first", title="first", handler=blocking_handler))
        await queue.submit(QueuedTask(task_id="second", title="second", handler=next_handler))

        await asyncio.wait_for(started.wait(), timeout=2)
        assert queue.cancel("first") is True
        await asyncio.wait_for(queue.join(), timeout=2)
    finally:
        release.set()
        reset_task_queues()

    assert events == ["first-started", "second-ran"]


@pytest.mark.asyncio
async def test_task_queue_cancel_all_drains_pending_tasks():
    queue = get_task_queue("test-cancel-all")
    started = asyncio.Event()
    release = asyncio.Event()
    events: list[str] = []

    async def blocking_handler() -> None:
        events.append("first-started")
        started.set()
        await release.wait()

    async def pending_handler() -> None:
        events.append("pending-ran")

    try:
        await queue.submit(QueuedTask(task_id="first", title="first", handler=blocking_handler))
        await queue.submit(QueuedTask(task_id="second", title="second", handler=pending_handler))
        await queue.submit(QueuedTask(task_id="third", title="third", handler=pending_handler))

        await asyncio.wait_for(started.wait(), timeout=2)
        result = queue.cancel_all()
        await asyncio.wait_for(queue.join(), timeout=2)
    finally:
        release.set()
        reset_task_queues()

    assert result["active_cancelled"] is True
    assert result["pending_cancelled_count"] == 2
    assert result["cancelled_task_ids"] == ["first", "second", "third"]
    assert events == ["first-started"]

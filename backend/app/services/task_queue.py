"""Small process-local async queues for serialized background work."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loguru import logger

TaskHandler = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class QueuedTask:
    task_id: str
    title: str
    handler: TaskHandler
    metadata: dict[str, Any] = field(default_factory=dict)
    enqueued_at: datetime = field(default_factory=datetime.now)


class AsyncTaskQueue:
    """A named FIFO queue backed by one async worker in the current process."""

    def __init__(self, name: str):
        self.name = name
        self._queue: asyncio.Queue[QueuedTask] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None
        self._lock: asyncio.Lock | None = None
        self._active_task: QueuedTask | None = None
        self._active_handle: asyncio.Task[None] | None = None
        self._known_ids: set[str] = set()
        self._cancelled_ids: set[str] = set()

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    @property
    def active_task(self) -> QueuedTask | None:
        return self._active_task

    @property
    def pending_tasks(self) -> list[QueuedTask]:
        """Return a read-only snapshot of queued work for status UIs."""
        return list(self._queue._queue)  # type: ignore[attr-defined]

    def snapshot(self) -> dict[str, Any]:
        """Expose queue state without leaking task handlers."""
        return {
            "active": self._task_summary(self._active_task),
            "pending": [self._task_summary(task) for task in self.pending_tasks],
            "pending_count": self.pending_count,
            "active_task_id": self._active_task.task_id if self._active_task else None,
        }

    @staticmethod
    def _task_summary(task: QueuedTask | None) -> dict[str, Any] | None:
        if task is None:
            return None
        return {
            "task_id": task.task_id,
            "title": task.title,
            "metadata": task.metadata,
            "enqueued_at": task.enqueued_at.isoformat(),
        }

    async def submit(self, task: QueuedTask) -> None:
        await self._ensure_worker()
        self._known_ids.add(task.task_id)
        await self._queue.put(task)

    def cancel(self, task_id: str) -> bool:
        if task_id not in self._known_ids and self._active_task is None:
            return False
        self._cancelled_ids.add(task_id)
        if (
            self._active_task is not None
            and self._active_task.task_id == task_id
            and self._active_handle is not None
            and not self._active_handle.done()
        ):
            self._active_handle.cancel()
        return task_id in self._known_ids

    async def join(self) -> None:
        await self._queue.join()

    async def _ensure_worker(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._worker is None or self._worker.done():
                self._worker = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            task = await self._queue.get()
            self._active_task = task
            try:
                if task.task_id in self._cancelled_ids:
                    continue
                self._active_handle = asyncio.create_task(task.handler())
                await self._active_handle
            except asyncio.CancelledError:
                logger.info("Queued task {} in {} was cancelled", task.task_id, self.name)
            except Exception as exc:
                logger.opt(exception=True).error(
                    "Queued task {} in {} failed: {}",
                    task.task_id,
                    self.name,
                    exc,
                )
            finally:
                self._known_ids.discard(task.task_id)
                self._cancelled_ids.discard(task.task_id)
                self._active_task = None
                self._active_handle = None
                self._queue.task_done()


_queues: dict[str, AsyncTaskQueue] = {}


def get_task_queue(name: str) -> AsyncTaskQueue:
    queue = _queues.get(name)
    if queue is None:
        queue = AsyncTaskQueue(name)
        _queues[name] = queue
    return queue


def reset_task_queues() -> None:
    """Test helper: clear idle queues between API tests."""
    _queues.clear()

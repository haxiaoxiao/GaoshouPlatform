"""Small process-local async queues for serialized background work."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loguru import logger

TaskHandler = Callable[[], Awaitable[None]]
SYNC_QUEUE_NAME = "sync"
MARKET_RADAR_QUEUE_NAME = "market_radar"
MARKET_DATA_WRITE_GROUP = "market_data_writes"

_QUEUE_EXCLUSIVE_GROUPS = {
    "data_sync": MARKET_DATA_WRITE_GROUP,
    "sentiment_sync": MARKET_DATA_WRITE_GROUP,
    "sync": MARKET_DATA_WRITE_GROUP,
    MARKET_RADAR_QUEUE_NAME: MARKET_DATA_WRITE_GROUP,
}


@dataclass(frozen=True)
class QueuedTask:
    task_id: str
    title: str
    handler: TaskHandler
    metadata: dict[str, Any] = field(default_factory=dict)
    enqueued_at: datetime = field(default_factory=datetime.now)


class AsyncTaskQueue:
    """A named FIFO queue backed by one async worker in the current process."""

    def __init__(self, name: str, *, exclusive_group: str | None = None):
        self.name = name
        self.exclusive_group = exclusive_group
        self._queue: asyncio.Queue[QueuedTask] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None
        self._lock: asyncio.Lock | None = None
        self._active_task: QueuedTask | None = None
        self._active_handle: asyncio.Task[None] | None = None
        self._known_ids: set[str] = set()
        self._cancelled_ids: set[str] = set()
        self._shutting_down = False

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

    def cancel_all(self) -> dict[str, Any]:
        """Cancel the active task and drain all pending tasks."""
        active = self._task_summary(self._active_task)
        pending: list[QueuedTask] = []
        while True:
            try:
                task = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            pending.append(task)
            self._queue.task_done()

        pending_ids = [task.task_id for task in pending]
        for task_id in pending_ids:
            self._known_ids.discard(task_id)
            self._cancelled_ids.discard(task_id)

        active_cancelled = False
        if self._active_task is not None:
            active_cancelled = self.cancel(self._active_task.task_id)

        return {
            "active": active,
            "active_cancelled": active_cancelled,
            "pending": [self._task_summary(task) for task in pending],
            "pending_cancelled_count": len(pending),
            "cancelled_task_ids": ([self._active_task.task_id] if self._active_task else []) + pending_ids,
        }

    def shutdown_nowait(self) -> None:
        """Cancel this queue's worker and discard pending work during teardown."""
        self._shutting_down = True
        while True:
            try:
                task = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._known_ids.discard(task.task_id)
            self._cancelled_ids.discard(task.task_id)
            self._queue.task_done()
        if self._active_handle is not None and not self._active_handle.done():
            self._active_handle.cancel()
        if self._worker is not None and not self._worker.done():
            self._worker.cancel()

    async def shutdown(self, *, timeout_seconds: float = 1.0) -> bool:
        """Stop the worker with one bounded cancellation retry."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.shutdown_nowait()
        worker = self._worker
        if worker is None or worker.done():
            return True
        if await self._wait_for_worker(worker, timeout_seconds):
            return True

        if self._active_handle is not None and not self._active_handle.done():
            self._active_handle.cancel()
        worker.cancel()
        if await self._wait_for_worker(worker, timeout_seconds):
            return True

        logger.error(
            "Task queue {} worker did not stop after bounded retry",
            self.name,
        )
        return False

    @staticmethod
    async def _wait_for_worker(
        worker: asyncio.Task[None],
        timeout_seconds: float,
    ) -> bool:
        try:
            await asyncio.wait_for(asyncio.shield(worker), timeout=timeout_seconds)
        except TimeoutError:
            return False
        except asyncio.CancelledError:
            if worker.done():
                return True
            raise
        return True

    async def join(self) -> None:
        await self._queue.join()

    async def _ensure_worker(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._worker is None or self._worker.done():
                self._shutting_down = False
                self._worker = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            task = await self._queue.get()
            self._active_task = task
            try:
                if task.task_id in self._cancelled_ids:
                    continue
                self._active_handle = asyncio.create_task(self._run_handler(task.handler))
                await self._active_handle
            except asyncio.CancelledError:
                logger.info("Queued task {} in {} was cancelled", task.task_id, self.name)
                if self._shutting_down:
                    raise
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

    async def _run_handler(self, handler: TaskHandler) -> None:
        if self.exclusive_group is None:
            await handler()
            return
        lock = _get_exclusive_group_lock(self.exclusive_group)
        async with lock:
            await handler()


_queues: dict[str, AsyncTaskQueue] = {}
_exclusive_group_locks: dict[tuple[int, str], asyncio.Lock] = {}


def _get_exclusive_group_lock(group: str) -> asyncio.Lock:
    loop_key = id(asyncio.get_running_loop())
    key = (loop_key, group)
    lock = _exclusive_group_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _exclusive_group_locks[key] = lock
    return lock


def get_task_queue(name: str) -> AsyncTaskQueue:
    queue = _queues.get(name)
    if queue is None:
        queue = AsyncTaskQueue(
            name,
            exclusive_group=_QUEUE_EXCLUSIVE_GROUPS.get(name),
        )
        _queues[name] = queue
    return queue


async def shutdown_task_queues(
    names: Iterable[str] | None = None,
    *,
    timeout_seconds: float = 1.0,
) -> dict[str, bool]:
    """Bound and await production queue teardown without creating absent queues."""
    selected = tuple(dict.fromkeys(names if names is not None else tuple(_queues)))

    async def stop(name: str) -> tuple[str, bool]:
        queue = _queues.get(name)
        if queue is None:
            return name, True
        return name, await queue.shutdown(timeout_seconds=timeout_seconds)

    results = await asyncio.gather(*(stop(name) for name in selected))
    return dict(results)


def reset_task_queues() -> None:
    """Test helper: clear idle queues between API tests."""
    for queue in _queues.values():
        queue.shutdown_nowait()
    _queues.clear()
    _exclusive_group_locks.clear()

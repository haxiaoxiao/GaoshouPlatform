"""In-memory runtime task registry for UI notifications.

This is intentionally process-local. It gives the frontend a single lightweight
place to poll for long-running task completion without introducing WebSocket or
database-backed notifications yet.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

TERMINAL_STATUSES = {"done", "failed", "completed", "cancelled"}
_TASK_TTL_SECONDS = 6 * 3600
_MAX_TASKS = 300


@dataclass
class RuntimeTask:
    task_id: str
    kind: str
    title: str
    status: str = "running"
    progress: float = 0.0
    result_ref: str | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "kind": self.kind,
            "title": self.title,
            "status": self.status,
            "progress": self.progress,
            "result_ref": self.result_ref,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "finished_at": self.finished_at,
            "meta": self.meta,
        }


_tasks: dict[str, RuntimeTask] = {}


def _cleanup() -> None:
    now = time.time()
    expired = [
        task_id
        for task_id, task in _tasks.items()
        if task.finished_at is not None and now - task.finished_at > _TASK_TTL_SECONDS
    ]
    for task_id in expired:
        _tasks.pop(task_id, None)

    if len(_tasks) <= _MAX_TASKS:
        return
    items = sorted(_tasks.items(), key=lambda item: item[1].created_at)
    for task_id, task in items[: max(0, len(_tasks) - _MAX_TASKS)]:
        if task.status in TERMINAL_STATUSES:
            _tasks.pop(task_id, None)


def register_task(
    *,
    task_id: str,
    kind: str,
    title: str,
    status: str = "running",
    progress: float = 0.0,
    result_ref: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    _cleanup()
    _tasks[task_id] = RuntimeTask(
        task_id=task_id,
        kind=kind,
        title=title,
        status=status,
        progress=float(progress),
        result_ref=result_ref,
        meta=meta or {},
    )


def update_task(
    task_id: str,
    *,
    status: str | None = None,
    progress: float | None = None,
    result_ref: str | None = None,
    error: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    task = _tasks.get(task_id)
    if task is None:
        return
    if status is not None:
        task.status = status
    if progress is not None:
        task.progress = float(progress)
    if result_ref is not None:
        task.result_ref = result_ref
    if error is not None:
        task.error = error
    if meta:
        task.meta.update(meta)
    task.updated_at = time.time()
    if task.status in TERMINAL_STATUSES and task.finished_at is None:
        task.finished_at = task.updated_at


def list_tasks(include_finished: bool = True) -> list[dict[str, Any]]:
    _cleanup()
    tasks = _tasks.values()
    if not include_finished:
        tasks = [task for task in tasks if task.status not in TERMINAL_STATUSES]
    return [task.to_dict() for task in sorted(tasks, key=lambda task: task.created_at, reverse=True)]


def get_task(task_id: str) -> dict[str, Any] | None:
    _cleanup()
    task = _tasks.get(task_id)
    return task.to_dict() if task else None

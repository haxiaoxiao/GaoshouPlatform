"""Persistent runtime task registry with an in-process hot cache."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from loguru import logger

from app.core.config import settings
from app.core.contracts import JobStatus

TERMINAL_STATUSES = {
    JobStatus.SUCCEEDED.value,
    JobStatus.FAILED.value,
    JobStatus.CANCELLED.value,
}
_STATUS_ALIASES = {
    "done": JobStatus.SUCCEEDED.value,
    "completed": JobStatus.SUCCEEDED.value,
}
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
_task_lock = threading.RLock()


def _normalize_status(status: str) -> str:
    return _STATUS_ALIASES.get(status, status)


def _connect() -> sqlite3.Connection | None:
    path = settings.sqlite_db_path
    if not path.exists():
        return None
    connection = sqlite3.connect(path, timeout=5)
    connection.row_factory = sqlite3.Row
    return connection


def _persist_task(task: RuntimeTask, *, event_type: str) -> None:
    connection = _connect()
    if connection is None:
        return
    now = datetime.fromtimestamp(task.updated_at)
    try:
        connection.execute(
            """
            INSERT INTO jobs (
                id, kind, title, status, progress, payload, result_ref, error,
                heartbeat_at, finished_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                kind=excluded.kind,
                title=excluded.title,
                status=excluded.status,
                progress=excluded.progress,
                payload=excluded.payload,
                result_ref=excluded.result_ref,
                error=excluded.error,
                heartbeat_at=excluded.heartbeat_at,
                finished_at=excluded.finished_at,
                updated_at=excluded.updated_at
            """,
            (
                task.task_id,
                task.kind,
                task.title,
                task.status,
                task.progress,
                json.dumps(task.meta, ensure_ascii=False),
                task.result_ref,
                task.error,
                now.isoformat(sep=" "),
                datetime.fromtimestamp(task.finished_at).isoformat(sep=" ") if task.finished_at else None,
                datetime.fromtimestamp(task.created_at).isoformat(sep=" "),
                now.isoformat(sep=" "),
            ),
        )
        connection.execute(
            "INSERT INTO job_events (job_id, event_type, data, created_at) VALUES (?, ?, ?, ?)",
            (
                task.task_id,
                event_type,
                json.dumps(task.to_dict(), ensure_ascii=False),
                now.isoformat(sep=" "),
            ),
        )
        connection.commit()
    except sqlite3.OperationalError as exc:
        logger.debug("Runtime task persistence unavailable: {}", exc)
    finally:
        connection.close()


def _row_to_task(row: sqlite3.Row) -> RuntimeTask:
    created_at = datetime.fromisoformat(str(row["created_at"])).timestamp()
    updated_at = datetime.fromisoformat(str(row["updated_at"])).timestamp()
    finished_at = (
        datetime.fromisoformat(str(row["finished_at"])).timestamp()
        if row["finished_at"]
        else None
    )
    return RuntimeTask(
        task_id=str(row["id"]),
        kind=str(row["kind"]),
        title=str(row["title"]),
        status=_normalize_status(str(row["status"])),
        progress=float(row["progress"] or 0.0),
        result_ref=row["result_ref"],
        error=row["error"],
        created_at=created_at,
        updated_at=updated_at,
        finished_at=finished_at,
        meta=json.loads(row["payload"] or "{}"),
    )


def _load_persistent_task(task_id: str) -> RuntimeTask | None:
    connection = _connect()
    if connection is None:
        return None
    try:
        row = connection.execute("SELECT * FROM jobs WHERE id = ?", (task_id,)).fetchone()
        return _row_to_task(row) if row else None
    except sqlite3.OperationalError as exc:
        logger.debug("Runtime task persistence unavailable: {}", exc)
        return None
    finally:
        connection.close()


def _load_persistent_tasks(*, kinds: set[str] | None = None, limit: int | None = _MAX_TASKS) -> list[RuntimeTask]:
    connection = _connect()
    if connection is None:
        return []
    try:
        clauses: list[str] = []
        params: list[Any] = []
        if kinds:
            placeholders = ",".join("?" for _ in kinds)
            clauses.append(f"kind IN ({placeholders})")
            params.extend(sorted(kinds))
        query = "SELECT * FROM jobs"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(max(int(limit), 1))
        rows = connection.execute(query, tuple(params)).fetchall()
        return [_row_to_task(row) for row in rows]
    except sqlite3.OperationalError as exc:
        logger.debug("Runtime task persistence unavailable: {}", exc)
        return []
    finally:
        connection.close()


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
    task = RuntimeTask(
        task_id=task_id,
        kind=kind,
        title=title,
        status=_normalize_status(status),
        progress=float(progress),
        result_ref=result_ref,
        meta=meta or {},
    )
    _tasks[task_id] = task
    _persist_task(task, event_type="registered")


def update_task(
    task_id: str,
    *,
    status: str | None = None,
    progress: float | None = None,
    result_ref: str | None = None,
    error: str | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    task = _tasks.get(task_id) or _load_persistent_task(task_id)
    if task is None:
        return
    _tasks[task_id] = task
    if status is not None:
        task.status = _normalize_status(status)
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
    _persist_task(task, event_type="updated")


def claim_task(
    task_id: str,
    *,
    expected_kind: str,
    expected_status: str,
    status: str,
    meta: dict[str, Any] | None = None,
    expected_updated_at: float | None = None,
) -> dict[str, Any] | None:
    """Atomically transition one persistent task before a side effect."""
    with _task_lock:
        connection = _connect()
        if connection is None:
            task = _tasks.get(task_id)
            if task is None or task.kind != expected_kind or task.status != expected_status:
                return None
            if expected_updated_at is not None and abs(task.updated_at - expected_updated_at) > 1e-6:
                return None
            task.status = _normalize_status(status)
            if meta:
                task.meta.update(meta)
            task.updated_at = time.time()
            if task.status in TERMINAL_STATUSES:
                task.finished_at = task.updated_at
            return task.to_dict()

        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM jobs WHERE id = ?", (task_id,)).fetchone()
            if row is None or str(row["kind"]) != expected_kind or _normalize_status(str(row["status"])) != expected_status:
                connection.rollback()
                return None
            task = _row_to_task(row)
            if expected_updated_at is not None and abs(task.updated_at - expected_updated_at) > 1e-6:
                connection.rollback()
                return None
            task.status = _normalize_status(status)
            if meta:
                task.meta.update(meta)
            task.updated_at = time.time()
            if task.status in TERMINAL_STATUSES:
                task.finished_at = task.updated_at
            now = datetime.fromtimestamp(task.updated_at).isoformat(sep=" ")
            connection.execute(
                """
                UPDATE jobs
                SET status = ?, payload = ?, heartbeat_at = ?, finished_at = ?, updated_at = ?
                WHERE id = ? AND kind = ? AND status = ?
                """,
                (
                    task.status,
                    json.dumps(task.meta, ensure_ascii=False),
                    now,
                    datetime.fromtimestamp(task.finished_at).isoformat(sep=" ") if task.finished_at else None,
                    now,
                    task_id,
                    expected_kind,
                    expected_status,
                ),
            )
            connection.execute(
                "INSERT INTO job_events (job_id, event_type, data, created_at) VALUES (?, ?, ?, ?)",
                (task_id, "claimed", json.dumps(task.to_dict(), ensure_ascii=False), now),
            )
            connection.commit()
            _tasks[task_id] = task
            return task.to_dict()
        except sqlite3.OperationalError as exc:
            connection.rollback()
            logger.warning("Runtime task claim unavailable for {}: {}", task_id, exc)
            return None
        finally:
            connection.close()


def list_tasks(
    include_finished: bool = True,
    *,
    kinds: set[str] | None = None,
    limit: int | None = _MAX_TASKS,
) -> list[dict[str, Any]]:
    _cleanup()
    persistent_limit = None if not include_finished else limit
    merged = {
        task.task_id: task
        for task in _load_persistent_tasks(kinds=kinds, limit=persistent_limit)
    }
    merged.update({
        task_id: task
        for task_id, task in _tasks.items()
        if not kinds or task.kind in kinds
    })
    tasks = merged.values()
    if not include_finished:
        tasks = [task for task in tasks if task.status not in TERMINAL_STATUSES]
    rows = [task.to_dict() for task in sorted(tasks, key=lambda task: task.created_at, reverse=True)]
    return rows if limit is None else rows[:limit]


def get_task(task_id: str) -> dict[str, Any] | None:
    _cleanup()
    task = _tasks.get(task_id) or _load_persistent_task(task_id)
    if task is not None:
        _tasks[task_id] = task
    return task.to_dict() if task else None


def reset_runtime_tasks(*, clear_persistent: bool = False) -> None:
    """Clear process memory; persistent deletion is reserved for isolated tests."""
    _tasks.clear()
    if not clear_persistent:
        return
    connection = _connect()
    if connection is None:
        return
    try:
        connection.execute("DELETE FROM job_events")
        connection.execute("DELETE FROM jobs")
        connection.commit()
    finally:
        connection.close()


def mark_stale_runtime_tasks_failed(
    *,
    kinds: set[str],
    older_than_seconds: int = 30 * 60,
    message: str = "Application restarted before the task completed",
) -> int:
    connection = _connect()
    if connection is None or not kinds:
        return 0
    cutoff = datetime.fromtimestamp(time.time() - max(older_than_seconds, 0)).isoformat(sep=" ")
    placeholders = ",".join("?" for _ in kinds)
    now = datetime.now().isoformat(sep=" ")
    try:
        rows = connection.execute(
            f"""
            SELECT id FROM jobs
            WHERE kind IN ({placeholders})
              AND status IN ('queued', 'running')
              AND updated_at < ?
            """,
            (*sorted(kinds), cutoff),
        ).fetchall()
        cursor = connection.execute(
            f"""
            UPDATE jobs
            SET status = ?, error = ?, finished_at = ?, updated_at = ?
            WHERE kind IN ({placeholders})
              AND status IN ('queued', 'running')
              AND updated_at < ?
            """,
            (JobStatus.FAILED.value, message, now, now, *sorted(kinds), cutoff),
        )
        for row in rows:
            task_id = str(row["id"])
            connection.execute(
                "INSERT INTO job_events (job_id, event_type, data, created_at) VALUES (?, ?, ?, ?)",
                (task_id, "interrupted", json.dumps({"status": JobStatus.FAILED.value, "error": message}), now),
            )
            _tasks.pop(task_id, None)
        connection.commit()
        return int(cursor.rowcount or 0)
    except sqlite3.OperationalError as exc:
        logger.debug("Runtime task stale cleanup unavailable: {}", exc)
        return 0
    finally:
        connection.close()

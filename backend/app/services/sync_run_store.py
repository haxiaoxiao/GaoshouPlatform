from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import case, desc, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.sync import SyncRun

TERMINAL_SYNC_STATUSES = {"completed", "failed", "cancelled"}


def idle_sync_status() -> dict[str, Any]:
    return {
        "task_id": None,
        "run_id": None,
        "sync_type": None,
        "status": "idle",
        "total": 0,
        "current": 0,
        "success_count": 0,
        "failed_count": 0,
        "progress_percent": 0.0,
        "start_time": None,
        "end_time": None,
        "error_message": None,
        "details": {},
    }


def run_to_status(run: SyncRun) -> dict[str, Any]:
    return {
        "task_id": run.run_id,
        "run_id": run.run_id,
        "sync_type": run.sync_type,
        "status": run.status,
        "total": run.total,
        "current": run.current,
        "success_count": run.success_count,
        "failed_count": run.failed_count,
        "progress_percent": run.progress_percent,
        "start_time": run.start_time.isoformat() if run.start_time else None,
        "end_time": run.end_time.isoformat() if run.end_time else None,
        "error_message": run.error_message,
        "details": run.details or {},
    }


async def upsert_sync_run(
    session: AsyncSession,
    *,
    run_id: str,
    sync_type: str | None,
    status: str,
    total: int = 0,
    current: int = 0,
    success_count: int = 0,
    failed_count: int = 0,
    progress_percent: float = 0.0,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    error_message: str | None = None,
    request: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    sync_task_id: int | None = None,
    commit: bool = True,
) -> None:
    now = datetime.now()
    values = {
        "run_id": run_id,
        "sync_task_id": sync_task_id,
        "sync_type": sync_type,
        "status": status,
        "total": int(total or 0),
        "current": int(current or 0),
        "success_count": int(success_count or 0),
        "failed_count": int(failed_count or 0),
        "progress_percent": float(progress_percent or 0.0),
        "start_time": start_time,
        "end_time": end_time,
        "error_message": error_message,
        "details": details or {},
        "created_at": now,
        "updated_at": now,
    }
    if request is not None:
        values["request"] = request
    stmt = insert(SyncRun).values(**values)
    update_values = {
        k: v
        for k, v in values.items()
        if k not in {"run_id", "created_at"}
    }
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=[SyncRun.run_id],
            set_=update_values,
        )
    )
    if commit:
        await session.commit()


async def get_current_sync_run(session: AsyncSession) -> SyncRun | None:
    active = await session.execute(
        select(SyncRun)
        .where(SyncRun.status.in_(("queued", "running")))
        .order_by(
            desc(case((SyncRun.status == "running", 1), else_=0)),
            desc(SyncRun.updated_at),
        )
        .limit(1)
    )
    return active.scalar_one_or_none()


async def get_latest_sync_run(session: AsyncSession) -> SyncRun | None:
    latest = await session.execute(
        select(SyncRun).order_by(desc(SyncRun.updated_at)).limit(1)
    )
    return latest.scalar_one_or_none()


async def mark_stale_running_syncs_failed(
    session: AsyncSession,
    *,
    message: str = "Sync service restarted while task was running",
    commit: bool = True,
) -> int:
    now = datetime.now()
    result = await session.execute(
        update(SyncRun)
        .where(SyncRun.status.in_(("queued", "running")))
        .values(
            status="failed",
            end_time=now,
            error_message=message,
            updated_at=now,
        )
    )
    if commit:
        await session.commit()
    return int(result.rowcount or 0)

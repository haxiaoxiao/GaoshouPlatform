import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from loguru import logger

from app.api.sync import SyncRequest, _run_sync_task, router as sync_router
from app.core.blocking import install_default_executor, shutdown_default_executor
from app.core.config import settings
from app.core.dev_data_mode import apply_dev_data_mode_to_settings
from app.core.logging import setup_logging
from app.core.scheduler import load_enabled_tasks, start_scheduler, stop_scheduler
from app.db import init_db
from app.db.sqlite import async_session_factory
from app.services.sync_run_store import get_queued_sync_runs, mark_stale_running_syncs_failed, upsert_sync_run
from app.services.task_queue import QueuedTask, get_task_queue

setup_logging(debug=True)


async def _mark_stale_sync_runs_after_startup() -> None:
    try:
        async with async_session_factory() as session:
            stale_count = await mark_stale_running_syncs_failed(session)
            if stale_count:
                logger.warning("Marked {} stale sync run(s) as failed after sync service restart", stale_count)
    except Exception as exc:
        logger.warning("Failed to mark stale sync runs after startup: {}", exc)


async def _recover_queued_sync_runs() -> None:
    async with async_session_factory() as session:
        queued_runs = await get_queued_sync_runs(session)
        for run in queued_runs:
            try:
                request = SyncRequest.model_validate(run.request or {})
            except Exception as exc:
                await upsert_sync_run(
                    session,
                    run_id=run.run_id,
                    sync_type=run.sync_type,
                    status="failed",
                    end_time=datetime.now(),
                    error_message=f"Cannot recover queued sync request: {exc}",
                )
                continue
            queue_name = "sentiment_sync" if request.sync_type.startswith("sentiment") else "data_sync"
            await get_task_queue(queue_name).submit(
                QueuedTask(
                    task_id=run.run_id,
                    title=f"recovered data sync {request.sync_type}",
                    handler=lambda run_id=run.run_id, recovered=request: _run_sync_task(run_id, recovered),
                    metadata={"sync_type": request.sync_type, "recovered": True},
                )
            )
        if queued_runs:
            logger.warning("Recovered {} queued sync run(s)", len(queued_runs))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting sync service...")
    apply_dev_data_mode_to_settings()
    install_default_executor()
    await init_db()
    logger.info("Sync service database initialized")
    await _mark_stale_sync_runs_after_startup()
    await _recover_queued_sync_runs()

    if settings.enable_sync_scheduler:
        start_scheduler()
        await load_enabled_tasks()
        logger.info("Sync scheduler loaded")
    else:
        logger.info("Sync scheduler disabled")

    yield

    logger.info("Stopping sync service...")
    stop_scheduler()
    shutdown_default_executor()
    logger.info("Sync service stopped")


app = FastAPI(
    title="GaoshouPlatform Sync Service",
    description="Isolated data synchronization service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(sync_router, prefix="/api/data", tags=["sync"])


@app.middleware("http")
async def apply_dev_data_mode_middleware(request, call_next):
    apply_dev_data_mode_to_settings()
    return await call_next(request)


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "sync", "version": "0.1.0"}

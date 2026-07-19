from contextlib import asynccontextmanager
from datetime import date, datetime

from fastapi import FastAPI, HTTPException, status
from loguru import logger
from pydantic import BaseModel

from app.api.sync import SyncRequest, _run_sync_task
from app.api.sync import router as sync_router
from app.core.blocking import install_default_executor, shutdown_default_executor
from app.core.config import settings
from app.core.dev_data_mode import apply_dev_data_mode_to_settings
from app.core.logging import setup_logging
from app.core.scheduler import (
    get_scheduler,
    load_enabled_tasks,
    start_scheduler,
    stop_scheduler,
)
from app.db import init_db
from app.db.sqlite import async_session_factory
from app.services.market_radar_runtime import (
    get_market_radar_runtime,
    start_market_radar_runtime,
)
from app.services.sync_run_store import (
    get_queued_sync_runs,
    mark_stale_running_syncs_failed,
    upsert_sync_run,
)
from app.services.task_queue import (
    QueuedTask,
    get_task_queue,
    shutdown_task_queues,
)

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

    radar_runtime = start_market_radar_runtime()
    try:
        if settings.enable_sync_scheduler:
            start_scheduler()
            await radar_runtime.start(get_scheduler())
            await load_enabled_tasks()
            logger.info("Sync scheduler loaded")
        else:
            await radar_runtime.start()
            logger.info("Sync scheduler disabled")
        app.state.market_radar_runtime = radar_runtime
        await _recover_queued_sync_runs()

        yield
    finally:
        logger.info("Stopping sync service...")
        stop_scheduler()
        try:
            await shutdown_task_queues(
                ("sync", "data_sync", "sentiment_sync")
            )
        finally:
            try:
                await radar_runtime.stop()
            finally:
                app.state.market_radar_runtime = None
                shutdown_default_executor()
        logger.info("Sync service stopped")


app = FastAPI(
    title="GaoshouPlatform Sync Service",
    description="Isolated data synchronization service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(sync_router, prefix="/api/data", tags=["sync"])


class MarketRadarEodRequest(BaseModel):
    trade_date: date


def _market_radar_runtime_or_503():
    runtime = get_market_radar_runtime()
    if runtime is None:
        raise HTTPException(status_code=503, detail="market radar runtime is not initialized")
    return runtime


@app.post("/internal/market-radar/eod", status_code=status.HTTP_202_ACCEPTED)
async def submit_market_radar_eod(payload: MarketRadarEodRequest):
    task = await _market_radar_runtime_or_503().submit_eod(
        payload.trade_date,
        reason="manual",
    )
    return {"code": 0, "message": "success", "data": task.as_dict()}


@app.get("/internal/market-radar/tasks/{task_id}")
async def get_market_radar_task(task_id: str):
    task = _market_radar_runtime_or_503().task_status(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="market radar task not found")
    return {"code": 0, "message": "success", "data": task.as_dict()}


@app.middleware("http")
async def apply_dev_data_mode_middleware(request, call_next):
    apply_dev_data_mode_to_settings()
    return await call_next(request)


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "sync", "version": "0.1.0"}

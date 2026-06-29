from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI
from loguru import logger

from app.api.sync import router as sync_router
from app.core.blocking import install_default_executor, shutdown_default_executor
from app.core.config import settings
from app.core.dev_data_mode import apply_dev_data_mode_to_settings
from app.core.logging import setup_logging
from app.core.scheduler import load_enabled_tasks, start_scheduler, stop_scheduler
from app.db import init_db
from app.db.sqlite import async_session_factory
from app.services.sync_run_store import mark_stale_running_syncs_failed

setup_logging(debug=True)


async def _mark_stale_sync_runs_after_startup() -> None:
    try:
        async with async_session_factory() as session:
            stale_count = await mark_stale_running_syncs_failed(session)
            if stale_count:
                logger.warning("Marked {} stale sync run(s) as failed after sync service restart", stale_count)
    except Exception as exc:
        logger.warning("Failed to mark stale sync runs after startup: {}", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting sync service...")
    apply_dev_data_mode_to_settings()
    install_default_executor()
    await init_db()
    logger.info("Sync service database initialized")
    asyncio.create_task(_mark_stale_sync_runs_after_startup())

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

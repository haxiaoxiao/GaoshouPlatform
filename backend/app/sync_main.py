from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from app.api.sync import router as sync_router
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.scheduler import load_enabled_tasks, start_scheduler, stop_scheduler
from app.db import init_db
from app.db.clickhouse import init_clickhouse_tables
from app.db.sqlite import async_session_factory
from app.services.sync_run_store import mark_stale_running_syncs_failed

setup_logging(debug=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting sync service...")
    await init_db()
    logger.info("Sync service database initialized")
    async with async_session_factory() as session:
        stale_count = await mark_stale_running_syncs_failed(session)
        if stale_count:
            logger.warning("Marked {} stale sync run(s) as failed after sync service restart", stale_count)

    if settings.clickhouse_enabled:
        try:
            init_clickhouse_tables()
            logger.info("ClickHouse tables initialized")
        except Exception as exc:
            logger.warning("ClickHouse not available, skipping: {}", exc)
    else:
        logger.info("ClickHouse disabled, using Parquet/DuckDB backend")

    if settings.enable_sync_scheduler:
        start_scheduler()
        await load_enabled_tasks()
        logger.info("Sync scheduler loaded")
    else:
        logger.info("Sync scheduler disabled")

    yield

    logger.info("Stopping sync service...")
    stop_scheduler()
    logger.info("Sync service stopped")


app = FastAPI(
    title="GaoshouPlatform Sync Service",
    description="Isolated data synchronization service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(sync_router, prefix="/api/data", tags=["sync"])


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "sync", "version": "0.1.0"}

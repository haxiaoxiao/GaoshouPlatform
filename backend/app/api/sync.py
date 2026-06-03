from __future__ import annotations

import asyncio
import uuid
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

import app.services.sync_service as sync_service_module
from app.db.sqlite import get_async_session
from app.engines.qmt_gateway import qmt_gateway
from app.services.cache_invalidation import invalidate_after_sync
from app.services.sync_run_store import (
    get_latest_sync_run,
    idle_sync_status,
    run_to_status,
    upsert_sync_run,
)
from app.services.sync_service import SyncProgress, SyncService

router = APIRouter()


class SyncRequest(BaseModel):
    sync_type: str = Field(description="sync type")
    symbols: list[str] | None = None
    index_symbols: list[str] | None = None
    start_date: date | None = None
    end_date: date | None = None
    sync_mode: str = "range"
    failure_strategy: str = "skip"
    full_sync: bool = False
    factor_sync_plan: dict[str, Any] | None = None
    relay_datasets: list[str] | None = None
    relay_options: dict[str, Any] | None = None


VALID_SYNC_TYPES = (
    "datasync",
    "stock_info",
    "stock_full",
    "financial_data",
    "kline_daily",
    "index_daily",
    "kline_minute",
    "kline_weekly",
    "realtime_mv",
    "dividends",
    "factor_dependency",
    "tushare_relay",
    "ths_concept",
    "sentiment_xueqiu",
    "sentiment_nga",
)


def _attach_sync_availability(data: dict[str, Any]) -> dict[str, Any]:
    status = str(data.get("status") or "idle")
    is_busy = status in {"queued", "running"}
    return {
        **data,
        "sync_service_available": True,
        "can_trigger": not is_busy,
        "reason": "已有同步任务正在运行" if is_busy else None,
    }


async def _run_sync_task(
    run_id: str,
    request: SyncRequest,
) -> None:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.core.config import settings

    engine = create_async_engine(settings.database_url)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    request_payload = request.model_dump(mode="json")
    effective_full_sync = request.full_sync or request.sync_mode == "full"

    # Periodically persist in-memory sync progress into sync_runs.
    async def _persister() -> None:
        while True:
            await asyncio.sleep(2)
            progress = sync_service_module._current_sync
            if progress is None:
                continue
            async with async_session() as s:
                svc = SyncService(s)
                await svc.persist_sync_progress(progress, run_id=run_id, sync_task_id=None)

    persister_task = asyncio.create_task(_persister())

    try:
        # 标记为 running
        async with async_session() as session:
            await upsert_sync_run(
                session,
                run_id=run_id,
                sync_type=request.sync_type,
                status="running",
                start_time=datetime.now(),
                request=request_payload,
            )

        qmt_required_types = {"datasync", "stock_info", "stock_full", "financial_data", "realtime_mv", "kline_daily", "kline_minute", "kline_weekly", "dividends"}
        qmt_required = request.sync_type in qmt_required_types
        if request.sync_type == "kline_daily" and request.sync_mode == "incremental" and not effective_full_sync:
            try:
                target_end = request.end_date or date.today()
                latest = (
                    sync_service_module._latest_market_date_for_symbols("klines_daily", request.symbols)
                    if request.symbols
                    else sync_service_module._latest_market_date("klines_daily")
                )
                if latest is not None and latest >= target_end:
                    qmt_required = False
            except Exception as exc:
                logger.debug("Unable to pre-check incremental daily coverage: {}", exc)
        if request.sync_type == "factor_dependency":
            steps = (request.factor_sync_plan or {}).get("steps") or []
            qmt_required = any(
                isinstance(step, dict) and step.get("type") in {"kline_daily", "kline_minute"}
                for step in steps
            )
        if qmt_required and not await qmt_gateway.check_connection():
            async with async_session() as session:
                await upsert_sync_run(
                    session,
                    run_id=run_id,
                    sync_type=request.sync_type,
                    status="failed",
                    progress_percent=100.0,
                    end_time=datetime.now(),
                    error_message="QMT (miniQMT) is not connected",
                    request=request_payload,
                )
            return

        async with async_session() as session:
            service = SyncService(session)
            progress: SyncProgress | None = None
            if request.sync_type == "datasync":
                progress = await service.sync_datasync(
                    symbols=request.symbols,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                    full_sync=effective_full_sync,
                    run_id=run_id,
                )
            elif request.sync_type == "stock_info":
                progress = await service.sync_stock_info(
                    failure_strategy=request.failure_strategy,
                    full_sync=effective_full_sync,
                )
            elif request.sync_type == "stock_full":
                progress = await service.sync_stock_full(
                    failure_strategy=request.failure_strategy,
                    run_id=run_id,
                )
            elif request.sync_type == "financial_data":
                progress = await service.sync_financial_data(
                    failure_strategy=request.failure_strategy,
                )
            elif request.sync_type == "realtime_mv":
                progress = await service.sync_realtime_mv(
                    symbols=request.symbols,
                    failure_strategy=request.failure_strategy,
                )
            elif request.sync_type == "kline_daily":
                progress = await service.sync_kline_daily(
                    symbols=request.symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                    full_sync=effective_full_sync,
                    auto_incremental=request.sync_mode == "incremental" and not effective_full_sync,
                    run_id=run_id,
                )
            elif request.sync_type == "index_daily":
                progress = await service.sync_index_daily(
                    index_symbols=request.index_symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                    full_sync=effective_full_sync,
                    run_id=run_id,
                )
            elif request.sync_type == "kline_weekly":
                progress = await service.sync_kline_weekly(
                    symbols=request.symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                    full_sync=effective_full_sync,
                )
            elif request.sync_type == "dividends":
                progress = await service.sync_dividends(
                    symbols=request.symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                )
            elif request.sync_type == "kline_minute":
                progress = await service.sync_kline_minute(
                    symbols=request.symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                    full_sync=effective_full_sync,
                    run_id=run_id,
                )
            elif request.sync_type == "factor_dependency":
                progress = await service.sync_factor_dependency(
                    plan=request.factor_sync_plan,
                    task_id=None,
                    run_id=run_id,
                    failure_strategy=request.failure_strategy,
                )
            elif request.sync_type == "tushare_relay":
                progress = await service.sync_tushare_relay(
                    relay_datasets=request.relay_datasets,
                    symbols=request.symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    relay_options=request.relay_options,
                    failure_strategy=request.failure_strategy,
                    run_id=run_id,
                )
            elif request.sync_type == "ths_concept":
                progress = await service.sync_ths_concept(
                    start_date=request.start_date,
                    end_date=request.end_date,
                    relay_options=request.relay_options,
                    failure_strategy=request.failure_strategy,
                    run_id=run_id,
                )
            elif request.sync_type == "sentiment_xueqiu":
                progress = await service.sync_sentiment_xueqiu(
                    symbols=request.symbols,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                    run_id=run_id,
                )
            elif request.sync_type == "sentiment_nga":
                progress = await service.sync_sentiment_nga(
                    start_date=request.start_date,
                    end_date=request.end_date,
                    failure_strategy=request.failure_strategy,
                    run_id=run_id,
                )

            if progress is not None:
                await service.persist_sync_progress(progress, run_id=run_id)
            try:
                invalidated = invalidate_after_sync(request.sync_type)
                logger.info("Cache invalidated after {} sync: {}", request.sync_type, invalidated)
            except Exception as exc:
                logger.warning("Cache invalidation after {} sync failed: {}", request.sync_type, exc)
    except Exception as exc:
        logger.opt(exception=True).error("Sync task {} failed: {}", request.sync_type, exc)
        async with async_session() as session:
            await upsert_sync_run(
                session,
                run_id=run_id,
                sync_type=request.sync_type,
                status="failed",
                progress_percent=100.0,
                end_time=datetime.now(),
                error_message=str(exc),
                request=request_payload,
            )
    finally:
        persister_task.cancel()
        try:
            await persister_task
        except asyncio.CancelledError:
            pass
        await engine.dispose()


@router.post("/sync")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    request: SyncRequest = Body(...),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    if request.sync_type not in VALID_SYNC_TYPES:
        raise HTTPException(status_code=400, detail=f"sync_type must be one of: {', '.join(VALID_SYNC_TYPES)}")
    if request.failure_strategy not in ("skip", "retry", "stop"):
        raise HTTPException(status_code=400, detail="failure_strategy must be skip, retry or stop")
    if request.sync_mode not in ("incremental", "range", "full"):
        raise HTTPException(status_code=400, detail="sync_mode must be incremental, range or full")

    service = SyncService(session)
    persisted = await service.get_persisted_sync_status()
    if persisted and persisted.get("status") in {"queued", "running"}:
        raise HTTPException(status_code=409, detail="已有同步任务正在运行中")

    run_id = f"sync-{str(uuid.uuid4())[:8]}"
    await upsert_sync_run(
        session,
        run_id=run_id,
        sync_type=request.sync_type,
        status="queued",
        request=request.model_dump(mode="json"),
    )
    background_tasks.add_task(_run_sync_task, run_id, request)

    initial_progress = SyncProgress(
        sync_type=request.sync_type,
        status="queued",
        start_time=datetime.now(),
        details={"run_id": run_id},
    )
    return {"code": 0, "message": "success", "data": {**initial_progress.to_dict(), "task_id": run_id, "run_id": run_id}}


@router.get("/sync/catalog")
async def get_sync_catalog(refresh: bool = Query(default=False)) -> dict[str, Any]:
    from app.services.tushare_relay_sync import build_sync_catalog

    return {"code": 0, "message": "success", "data": build_sync_catalog(refresh=refresh)}


@router.get("/sync/status")
async def get_sync_status(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    service = SyncService(session)
    persisted = await service.get_persisted_sync_status()
    if persisted:
        return {"code": 0, "message": "success", "data": _attach_sync_availability(persisted)}
    latest = await get_latest_sync_run(session)
    idle = idle_sync_status()
    if latest is not None:
        idle["last_run"] = run_to_status(latest)
    return {"code": 0, "message": "success", "data": _attach_sync_availability(idle)}


@router.get("/sync/logs")
async def get_sync_logs(
    sync_type: str | None = Query(default=None),
    task_id: int | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    service = SyncService(session)
    logs = await service.get_sync_logs(sync_type=sync_type, task_id=task_id, limit=limit)
    return {
        "code": 0,
        "message": "success",
        "data": [
            {
                "id": log.id,
                "task_id": log.task_id,
                "sync_type": log.sync_type,
                "status": log.status,
                "total_count": log.total_count,
                "success_count": log.success_count,
                "failed_count": log.failed_count,
                "start_time": log.start_time.isoformat(),
                "end_time": log.end_time.isoformat() if log.end_time else None,
                "error_message": log.error_message,
                "details": log.details,
                "created_at": log.created_at.isoformat(),
            }
            for log in logs
        ],
    }


@router.post("/sync/cancel")
async def cancel_sync(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    service = SyncService(session)
    cancelled = await service.cancel_sync()
    persisted = await service.get_persisted_sync_status()
    run_id = persisted.get("run_id") if persisted else None
    if run_id and persisted.get("status") in {"queued", "running"}:
        await upsert_sync_run(
            session,
            run_id=run_id,
            sync_type=persisted.get("sync_type"),
            status="cancelled",
            total=persisted.get("total", 0),
            current=persisted.get("current", 0),
            success_count=persisted.get("success_count", 0),
            failed_count=persisted.get("failed_count", 0),
            progress_percent=persisted.get("progress_percent", 0.0),
            end_time=datetime.now(),
            error_message="User cancelled",
            details=persisted.get("details") or {},
        )
        cancelled = True
    return {"code": 0, "message": "success", "data": {"cancelled": cancelled}}

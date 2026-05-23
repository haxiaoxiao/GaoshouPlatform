# backend/app/api/system.py
from datetime import date

from fastapi import APIRouter

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.cache.redis_cache import get_redis_client
from app.services.backtest_redis_cache import get_backtest_cache
from app.services.runtime_tasks import get_task, list_tasks
from app.services.sync_proxy import proxy_sync_request, sync_service_health

router = APIRouter()


@router.get("/status")
async def get_system_status():
    """获取系统状态（含数据后端配置和覆盖摘要）"""
    store = get_market_data_store()
    parquet_info = {}

    if settings.market_data_backend == "parquet":
        try:
            daily = store.coverage([], date(2000, 1, 1), date.today(), dataset="klines_daily")
            minute_dataset = (
                "klines_minute_timer"
                if store._exists("klines_minute_timer")
                else "klines_minute"
            )
            minute = store.coverage([], date(2000, 1, 1), date.today(), dataset=minute_dataset)
            cum_timer = store.coverage([], date(2000, 1, 1), date.today(), dataset="klines_minute_cum_timer")
            parquet_info = {
                "klines_daily_rows": daily.get("total_rows", 0),
                "klines_daily_symbols": len(daily.get("symbols_covered", [])),
                "klines_minute_dataset": minute_dataset,
                "klines_minute_rows": minute.get("total_rows", 0),
                "klines_minute_symbols": len(minute.get("symbols_covered", [])),
                "klines_minute_cum_timer_rows": cum_timer.get("total_rows", 0),
                "klines_minute_cum_timer_symbols": len(cum_timer.get("symbols_covered", [])),
            }
        except Exception:
            parquet_info = {"error": "无法读取 Parquet 状态"}

    sync_health = await sync_service_health()
    sync_current_status = None
    if sync_health.get("healthy"):
        try:
            sync_response = await proxy_sync_request("GET", "/api/data/sync/status")
            sync_current_status = sync_response.get("data")
        except Exception:
            sync_current_status = None

    return {
        "status": "running",
        "database": "connected",
        "market_data_backend": settings.market_data_backend,
        "parquet_data_dir": settings.parquet_data_dir,
        "duckdb_path": settings.duckdb_path,
        "clickhouse_enabled": settings.clickhouse_enabled,
        "sync_service": {
            "enabled": True,
            "url": settings.sync_service_url,
            "healthy": bool(sync_health.get("healthy")),
            "current_status": sync_current_status,
        },
        "parquet_coverage": parquet_info if settings.market_data_backend == "parquet" else None,
    }


@router.get("/cache")
async def get_cache_status():
    """Return Redis and cache namespace status for runtime tuning."""
    redis_client = get_redis_client()
    backtest_cache = get_backtest_cache()
    return {
        "redis": {
            "available": redis_client.available,
            "host": settings.redis_host,
            "port": settings.redis_port,
            "db": settings.redis_db,
        },
        "backtest_cache": {
            "available": backtest_cache.available,
            "namespace": backtest_cache.namespace,
            "ttl_seconds": backtest_cache.ttl,
            "uses": [
                "index_components",
                "daily_window",
                "daily_basic_mv",
                "small_cap_indicator_arrays",
            ],
        },
        "compute_cache": {
            "l1": "process_lru",
            "l1_5": "redis",
            "l2": "factor_cache_parquet_or_clickhouse",
        },
    }


@router.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy"}


@router.get("/tasks")
async def get_runtime_tasks(include_finished: bool = True):
    """Return runtime long-task states for frontend polling notifications."""
    return {
        "code": 0,
        "message": "success",
        "data": list_tasks(include_finished=include_finished),
    }


@router.get("/tasks/{task_id}")
async def get_runtime_task(task_id: str):
    """Return one runtime long-task state."""
    task = get_task(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    return {"code": 0, "message": "success", "data": task}

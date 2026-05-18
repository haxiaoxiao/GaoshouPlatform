# backend/app/api/system.py
from datetime import date

from fastapi import APIRouter

from app.core.config import settings
from app.data_stores import get_market_data_store

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

    return {
        "status": "running",
        "database": "connected",
        "market_data_backend": settings.market_data_backend,
        "parquet_data_dir": settings.parquet_data_dir,
        "duckdb_path": settings.duckdb_path,
        "clickhouse_enabled": settings.clickhouse_enabled,
        "parquet_coverage": parquet_info if settings.market_data_backend == "parquet" else None,
    }


@router.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy"}

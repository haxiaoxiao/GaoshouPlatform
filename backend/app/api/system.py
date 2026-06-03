# backend/app/api/system.py
import asyncio
from datetime import date, datetime
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.redis_cache import get_redis_client
from app.core.config import settings
from app.db.models.financial import FinancialData
from app.db.models.stock import Stock
from app.db.sqlite import get_async_session
from app.services.backtest_redis_cache import get_backtest_cache
from app.services.runtime_tasks import get_task, list_tasks
from app.services.sentiment import SentimentService
from app.services.sync_proxy import proxy_sync_request, sync_service_health
from app.services.tushare_relay_sync import dataset_coverage

router = APIRouter()


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None


def _freshness_status(latest_date: str | None, *, max_good_days: int) -> str:
    parsed = _parse_date(latest_date)
    if parsed is None:
        return "missing"
    age_days = (datetime.now().date() - parsed).days
    if age_days <= max_good_days:
        return "good"
    return "stale"


def _summary_item(
    *,
    key: str,
    label: str,
    source: str,
    storage: str,
    date_column: str,
    coverage: dict[str, Any],
    max_good_days: int,
    detail: str,
) -> dict[str, Any]:
    latest_date = _iso_or_none(coverage.get("max_date") or coverage.get("latest_date"))
    status = "error" if coverage.get("error") else _freshness_status(latest_date, max_good_days=max_good_days)
    status_text = {
        "good": "新鲜",
        "stale": "需关注",
        "missing": "待补齐",
        "error": "异常",
    }.get(status, status)
    estimated = bool(coverage.get("estimated", False))
    return {
        "key": key,
        "label": label,
        "source": source,
        "storage": storage,
        "dataset": storage,
        "date_column": date_column,
        "latest_date": latest_date,
        "latest_datetime": latest_date if date_column == "datetime" else None,
        "min_date": _iso_or_none(coverage.get("min_date")),
        "row_count": coverage.get("row_count"),
        "row_count_estimated": estimated,
        "estimated": estimated,
        "partition_count": coverage.get("partition_count"),
        "status": status,
        "status_text": status_text,
        "detail": detail,
        "notes": detail,
        "error": coverage.get("error"),
    }


async def _sqlite_summary(
    session: AsyncSession,
    *,
    model: Any,
    count_column: Any,
    latest_column: Any,
    updated_column: Any | None = None,
) -> dict[str, Any]:
    columns = [func.count(count_column), func.max(latest_column)]
    if updated_column is not None:
        columns.append(func.max(updated_column))
    result = await session.execute(select(*columns))
    row = result.one()
    data: dict[str, Any] = {
        "row_count": int(row[0] or 0),
        "max_date": _iso_or_none(row[1]),
        "estimated": False,
    }
    if updated_column is not None:
        data["updated_at"] = _iso_or_none(row[2])
    return data


async def _dataset_coverages(specs: dict[str, tuple[str, str]]) -> dict[str, dict[str, Any]]:
    async def read_one(dataset: str, date_column: str) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(dataset_coverage, dataset, date_column)
        except Exception as exc:
            return {"row_count": None, "min_date": None, "max_date": None, "error": str(exc)}

    keys = list(specs)
    results = await asyncio.gather(*(read_one(*specs[key]) for key in keys))
    return dict(zip(keys, results))


@router.get("/status")
async def get_system_status():
    """Return system status, backend config, and lightweight data coverage."""
    parquet_info = {}

    if settings.market_data_backend == "parquet":
        try:
            daily, minute_timer, minute, cum_timer = await asyncio.gather(
                asyncio.to_thread(dataset_coverage, "klines_daily", "trade_date"),
                asyncio.to_thread(dataset_coverage, "klines_minute_timer", "datetime"),
                asyncio.to_thread(dataset_coverage, "klines_minute", "datetime"),
                asyncio.to_thread(dataset_coverage, "klines_minute_cum_timer", "datetime"),
            )
            minute_info = minute_timer if minute_timer.get("max_date") else minute
            minute_dataset = "klines_minute_timer" if minute_timer.get("max_date") else "klines_minute"
            parquet_info = {
                "klines_daily_rows": int(daily.get("row_count") or 0),
                "klines_daily_latest": daily.get("max_date"),
                "klines_minute_dataset": minute_dataset,
                "klines_minute_rows": int(minute_info.get("row_count") or 0),
                "klines_minute_latest": minute_info.get("max_date"),
                "klines_minute_cum_timer_rows": int(cum_timer.get("row_count") or 0),
                "klines_minute_cum_timer_latest": cum_timer.get("max_date"),
            }
        except Exception:
            parquet_info = {"error": "Unable to read Parquet status"}

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


@router.get("/data-summary")
async def get_data_summary(session: AsyncSession = Depends(get_async_session)):
    """Return one lightweight freshness contract for frontend cockpit pages."""
    coverage_specs = {
        "market_daily": ("klines_daily", "trade_date"),
        "market_minute": ("klines_minute", "datetime"),
        "minute_timer": ("klines_minute_timer", "datetime"),
        "factor_values": ("factor_values", "trade_date"),
        "stock_indicators": ("stock_indicators", "trade_date"),
        "concept_membership": ("ths_member", "snapshot_date"),
        "moneyflow": ("moneyflow", "trade_date"),
    }
    coverage_map = await _dataset_coverages(coverage_specs)

    stocks_summary, financial_summary = await asyncio.gather(
        _sqlite_summary(
            session,
            model=Stock,
            count_column=Stock.symbol,
            latest_column=Stock.updated_at,
            updated_column=Stock.updated_at,
        ),
        _sqlite_summary(
            session,
            model=FinancialData,
            count_column=FinancialData.id,
            latest_column=FinancialData.report_date,
            updated_column=FinancialData.updated_at,
        ),
    )

    try:
        sentiment_overview = await SentimentService(session).overview(None)
    except Exception as exc:
        sentiment_overview = {
            "total_posts": 0,
            "latest_published_at": None,
            "error": str(exc),
        }

    backend_label = "Parquet" if settings.market_data_backend == "parquet" else "ClickHouse"
    items = [
        _summary_item(
            key="market_daily",
            label="日线行情",
            source=f"{backend_label} klines_daily",
            storage="klines_daily",
            date_column="trade_date",
            coverage=coverage_map["market_daily"],
            max_good_days=5,
            detail="因子、回测、基准行情的主口径，关注最新 trade_date。",
        ),
        _summary_item(
            key="market_minute",
            label="分钟行情",
            source=f"{backend_label} klines_minute",
            storage="klines_minute",
            date_column="datetime",
            coverage=coverage_map["market_minute"],
            max_good_days=10,
            detail="日内策略和分钟研究的完整分钟口径。",
        ),
        _summary_item(
            key="minute_timer",
            label="定时分钟",
            source=f"{backend_label} klines_minute_timer",
            storage="klines_minute_timer",
            date_column="datetime",
            coverage=coverage_map["minute_timer"],
            max_good_days=10,
            detail="minute_timer 回测优先读取的稀疏分钟数据。",
        ),
        _summary_item(
            key="stocks",
            label="股票基础数据",
            source="SQLite stocks",
            storage="stocks",
            date_column="updated_at",
            coverage=stocks_summary,
            max_good_days=14,
            detail="代码、名称、行业、状态、市值和最新基础快照。",
        ),
        _summary_item(
            key="financial",
            label="财务报表",
            source="SQLite financial_data",
            storage="financial_data",
            date_column="report_date",
            coverage=financial_summary,
            max_good_days=140,
            detail="季度财务和质量/成长类因子的 point-in-time 依赖。",
        ),
        _summary_item(
            key="factor_values",
            label="因子缓存",
            source=f"{backend_label} factor_values",
            storage="factor_values",
            date_column="trade_date",
            coverage=coverage_map["factor_values"],
            max_good_days=14,
            detail="因子评估页消费的预计算结果。",
        ),
        _summary_item(
            key="stock_indicators",
            label="指标缓存",
            source=f"{backend_label} stock_indicators",
            storage="stock_indicators",
            date_column="trade_date",
            coverage=coverage_map["stock_indicators"],
            max_good_days=14,
            detail="Indicator 体系落盘后的截面指标。",
        ),
        _summary_item(
            key="concept_membership",
            label="概念与行业扩展",
            source=f"{backend_label} ths_member",
            storage="ths_member",
            date_column="snapshot_date",
            coverage=coverage_map["concept_membership"],
            max_good_days=30,
            detail="同花顺概念成员和主题股票池扩展。",
        ),
        _summary_item(
            key="moneyflow",
            label="资金流 / 复权",
            source=f"{backend_label} moneyflow",
            storage="moneyflow",
            date_column="trade_date",
            coverage=coverage_map["moneyflow"],
            max_good_days=30,
            detail="资金流、复权校验和技术/资金类特征依赖。",
        ),
        _summary_item(
            key="sentiment",
            label="新闻舆情数据",
            source="SQLite sentiment_posts",
            storage="sentiment_posts",
            date_column="published_at",
            coverage={
                "row_count": sentiment_overview.get("total_posts", 0),
                "max_date": sentiment_overview.get("latest_published_at"),
                "estimated": False,
                "error": sentiment_overview.get("error"),
            },
            max_good_days=14,
            detail="雪球/NGA 等文本样本，默认用于研究验证而非直接回测。",
        ),
    ]
    by_key = {item["key"]: item for item in items}
    critical_keys = {"market_daily", "stocks"}
    if any(by_key[key]["status"] in {"missing", "error"} for key in critical_keys):
        overall_status = "error"
    elif any(item["status"] != "good" for item in items):
        overall_status = "degraded"
    else:
        overall_status = "good"

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overall_status": overall_status,
        "market_data_backend": settings.market_data_backend,
        "parquet_data_dir": settings.parquet_data_dir,
        "items": items,
        "by_key": by_key,
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
    """Health check."""
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

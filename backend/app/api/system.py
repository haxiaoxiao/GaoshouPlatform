# backend/app/api/system.py
import asyncio
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.redis_cache import get_redis_client
from app.core.config import settings
from app.db.models.financial import FinancialData
from app.db.models.stock import Stock
from app.db.sqlite import get_async_session
from app.services.backtest_redis_cache import get_backtest_cache
from app.core.dev_data_mode import (
    apply_dev_data_mode_to_settings,
    dev_data_mode_payload,
    set_dev_data_mode,
)
from app.services.runtime_tasks import get_task, list_tasks
from app.services.sentiment import SentimentService
from app.services.sync_proxy import proxy_sync_request, sync_service_health
from app.services.parquet_dataset_catalog import get_parquet_date_column
from app.services.tushare_relay_sync import dataset_coverage

router = APIRouter()


class DevDataModeUpdate(BaseModel):
    use_prod_data: bool
    acknowledge_warning: bool = False


class LiveTradingGuardrailsUpdate(BaseModel):
    enable_order_submit: bool
    auto_execute_enabled: bool
    acknowledge_risk: bool = False
    confirm_text: str | None = None


LIVE_TRADING_CONFIRM_TEXT = "ENABLE LIVE TRADING"
LIVE_TRADING_GUARDRAIL_KEYS = {
    "enable_order_submit": ("LIVE_TRADING_ENABLE_ORDER_SUBMIT", "live_trading_enable_order_submit"),
    "auto_execute_enabled": ("LIVE_TRADING_AUTO_EXECUTE_ENABLED", "live_trading_auto_execute_enabled"),
}


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


def _env_bool_text(value: bool) -> str:
    return "true" if value else "false"


def _live_trading_guardrail_env_path() -> Path:
    return settings.base_dir / ".env.local"


def _read_env_value(path: Path, key: str) -> str | None:
    if not path.exists():
        return None
    value: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        current_key, raw_value = text.split("=", 1)
        if current_key.strip() == key:
            value = raw_value.strip()
    return value


def _write_env_values(path: Path, updates: dict[str, bool]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    remaining = set(updates)
    output: list[str] = []

    for line in existing_lines:
        text = line.strip()
        if text and not text.startswith("#") and "=" in text:
            key = text.split("=", 1)[0].strip()
            if key in updates:
                output.append(f"{key}={_env_bool_text(updates[key])}")
                remaining.discard(key)
                continue
        output.append(line)

    if remaining:
        if output and output[-1].strip():
            output.append("")
        output.append("# Live trading guardrails. Keep false unless production trading is intentionally armed.")
        for key in LIVE_TRADING_GUARDRAIL_KEYS.values():
            env_key = key[0]
            if env_key in remaining:
                output.append(f"{env_key}={_env_bool_text(updates[env_key])}")

    path.write_text("\n".join(output).rstrip() + "\n", encoding="utf-8")


def _live_trading_guardrails_payload() -> dict[str, Any]:
    env_path = _live_trading_guardrail_env_path()
    env_values = {
        env_key: _read_env_value(env_path, env_key)
        for env_key, _settings_attr in LIVE_TRADING_GUARDRAIL_KEYS.values()
    }
    return {
        "enable_order_submit": bool(settings.live_trading_enable_order_submit),
        "auto_execute_enabled": bool(settings.live_trading_auto_execute_enabled),
        "env_file": str(env_path),
        "env_values": env_values,
        "requires_restart": False,
        "confirm_text": LIVE_TRADING_CONFIRM_TEXT,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


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


async def _dataset_coverages(specs: dict[str, tuple[str, str] | tuple[str, str, bool]]) -> dict[str, dict[str, Any]]:
    async def read_one(dataset: str, date_column: str, exact: bool = False) -> dict[str, Any]:
        try:
            return await asyncio.to_thread(dataset_coverage, dataset, date_column, exact=exact)
        except Exception as exc:
            return {"row_count": None, "min_date": None, "max_date": None, "error": str(exc)}

    keys = list(specs)
    results = await asyncio.gather(*(read_one(*specs[key]) for key in keys))
    return dict(zip(keys, results, strict=False))


@router.get("/status")
async def get_system_status():
    """Return system status, backend config, and lightweight data coverage."""
    apply_dev_data_mode_to_settings()
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
        "data_dir": settings.gaoshou_data_dir,
        "dev_data_mode": dev_data_mode_payload(),
        "duckdb_path": settings.duckdb_path,
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
    apply_dev_data_mode_to_settings()
    coverage_specs = {
        "market_daily": ("klines_daily", "trade_date"),
        "market_minute": ("klines_minute", "datetime"),
        "minute_timer": ("klines_minute_timer", "datetime"),
        "factor_values": ("factor_values", "trade_date"),
        "stock_indicators": ("stock_indicators", "trade_date"),
        "concept_membership": ("ths_member", "snapshot_date"),
        "moneyflow": ("moneyflow", "trade_date"),
        "jq_money_flow_daily": ("jq_money_flow_daily", get_parquet_date_column("jq_money_flow_daily") or "trade_date_1", True),
        "jq_financial_income": ("jq_financial_income", get_parquet_date_column("jq_financial_income") or "available_date", True),
        "jq_financial_balance": ("jq_financial_balance", get_parquet_date_column("jq_financial_balance") or "available_date", True),
        "jq_financial_cash_flow": ("jq_financial_cash_flow", get_parquet_date_column("jq_financial_cash_flow") or "available_date", True),
        "jq_index_daily_bars": ("jq_index_daily_bars", get_parquet_date_column("jq_index_daily_bars") or "trade_date"),
        "jq_etf_daily_bars": ("jq_etf_daily_bars", get_parquet_date_column("jq_etf_daily_bars") or "trade_date"),
        "tushare_margin_detail": ("tushare_margin_detail", get_parquet_date_column("tushare_margin_detail") or "trade_date"),
        "tushare_limit_list_d": ("tushare_limit_list_d", get_parquet_date_column("tushare_limit_list_d") or "trade_date"),
    }
    coverage_map = await _dataset_coverages(coverage_specs)

    stocks_summary = await _sqlite_summary(
        session,
        model=Stock,
        count_column=Stock.symbol,
        latest_column=Stock.updated_at,
        updated_column=Stock.updated_at,
    )
    financial_summary = await _sqlite_summary(
        session,
        model=FinancialData,
        count_column=FinancialData.id,
        latest_column=FinancialData.report_date,
        updated_column=FinancialData.updated_at,
    )

    try:
        sentiment_overview = await SentimentService(session).overview(None)
    except Exception as exc:
        sentiment_overview = {
            "total_posts": 0,
            "latest_published_at": None,
            "error": str(exc),
        }

    backend_label = "Parquet"
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
            key="jq_money_flow_daily",
            label="JQ 个股资金流",
            source=f"{backend_label} jq_money_flow_daily",
            storage="jq_money_flow_daily",
            date_column="trade_date_1",
            coverage=coverage_map["jq_money_flow_daily"],
            max_good_days=90,
            detail="JoinQuant 个股资金流，trade_date 字段为空，统一用 trade_date_1。",
        ),
        _summary_item(
            key="jq_financial_income",
            label="JQ 利润表",
            source=f"{backend_label} jq_financial_income",
            storage="jq_financial_income",
            date_column="available_date",
            coverage=coverage_map["jq_financial_income"],
            max_good_days=140,
            detail="JoinQuant 利润表，按可用日期做 point-in-time 查询。",
        ),
        _summary_item(
            key="jq_financial_balance",
            label="JQ 资产负债表",
            source=f"{backend_label} jq_financial_balance",
            storage="jq_financial_balance",
            date_column="available_date",
            coverage=coverage_map["jq_financial_balance"],
            max_good_days=140,
            detail="JoinQuant 资产负债表，按可用日期做 point-in-time 查询。",
        ),
        _summary_item(
            key="jq_financial_cash_flow",
            label="JQ 现金流量表",
            source=f"{backend_label} jq_financial_cash_flow",
            storage="jq_financial_cash_flow",
            date_column="available_date",
            coverage=coverage_map["jq_financial_cash_flow"],
            max_good_days=140,
            detail="JoinQuant 现金流量表，按可用日期做 point-in-time 查询。",
        ),
        _summary_item(
            key="jq_index_daily_bars",
            label="JQ 指数日线",
            source=f"{backend_label} jq_index_daily_bars",
            storage="jq_index_daily_bars",
            date_column="trade_date",
            coverage=coverage_map["jq_index_daily_bars"],
            max_good_days=90,
            detail="JoinQuant 指数日线，可用于基准和指数择时特征。",
        ),
        _summary_item(
            key="jq_etf_daily_bars",
            label="JQ ETF 日线",
            source=f"{backend_label} jq_etf_daily_bars",
            storage="jq_etf_daily_bars",
            date_column="trade_date",
            coverage=coverage_map["jq_etf_daily_bars"],
            max_good_days=90,
            detail="JoinQuant ETF 日线，可用于 ETF 轮动和基准扩展。",
        ),
        _summary_item(
            key="tushare_margin_detail",
            label="融资融券明细",
            source=f"{backend_label} tushare_margin_detail",
            storage="tushare_margin_detail",
            date_column="trade_date",
            coverage=coverage_map["tushare_margin_detail"],
            max_good_days=30,
            detail="个股融资融券明细，可用于杠杆资金和风险偏好因子。",
        ),
        _summary_item(
            key="tushare_limit_list_d",
            label="涨跌停明细",
            source=f"{backend_label} tushare_limit_list_d",
            storage="tushare_limit_list_d",
            date_column="trade_date",
            coverage=coverage_map["tushare_limit_list_d"],
            max_good_days=30,
            detail="涨跌停明细，可用于情绪、连板和交易限制特征。",
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
        "data_dir": settings.gaoshou_data_dir,
        "dev_data_mode": dev_data_mode_payload(),
        "items": items,
        "by_key": by_key,
    }


@router.get("/dev-data-mode")
async def get_dev_data_mode():
    """Return the dev-only real-data switch state."""
    apply_dev_data_mode_to_settings()
    return dev_data_mode_payload()


@router.put("/dev-data-mode")
async def update_dev_data_mode(payload: DevDataModeUpdate):
    """Toggle whether dev reads/writes the production real-data directory."""
    current = dev_data_mode_payload()
    if not current.get("enabled"):
        return current
    if payload.use_prod_data and not payload.acknowledge_warning:
        raise HTTPException(
            status_code=400,
            detail="Enabling production real data in dev requires acknowledge_warning=true",
        )
    try:
        set_dev_data_mode(payload.use_prod_data)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return dev_data_mode_payload()


@router.get("/live-trading-guardrails")
async def get_live_trading_guardrails():
    """Return live trading kill switches used by the production runner."""
    return _live_trading_guardrails_payload()


@router.put("/live-trading-guardrails")
async def update_live_trading_guardrails(payload: LiveTradingGuardrailsUpdate):
    """Persist live trading guardrails and update this process immediately."""
    current = _live_trading_guardrails_payload()
    turning_on = (
        (payload.enable_order_submit and not current["enable_order_submit"])
        or (payload.auto_execute_enabled and not current["auto_execute_enabled"])
    )
    if payload.auto_execute_enabled and not payload.enable_order_submit:
        raise HTTPException(
            status_code=400,
            detail="LIVE_TRADING_AUTO_EXECUTE_ENABLED=true requires LIVE_TRADING_ENABLE_ORDER_SUBMIT=true",
        )
    if turning_on and (
        not payload.acknowledge_risk
        or (payload.confirm_text or "").strip() != LIVE_TRADING_CONFIRM_TEXT
    ):
        raise HTTPException(
            status_code=400,
            detail=f"Enabling live trading guardrails requires confirm_text={LIVE_TRADING_CONFIRM_TEXT!r}",
        )

    desired = {
        "LIVE_TRADING_ENABLE_ORDER_SUBMIT": payload.enable_order_submit,
        "LIVE_TRADING_AUTO_EXECUTE_ENABLED": payload.auto_execute_enabled,
    }
    _write_env_values(_live_trading_guardrail_env_path(), desired)
    os.environ.update({key: _env_bool_text(value) for key, value in desired.items()})
    settings.live_trading_enable_order_submit = payload.enable_order_submit
    settings.live_trading_auto_execute_enabled = payload.auto_execute_enabled
    return _live_trading_guardrails_payload()


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
            "l2": "factor_cache_parquet",
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

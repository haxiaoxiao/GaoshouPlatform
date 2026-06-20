from __future__ import annotations

import asyncio
import os
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
from loguru import logger

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.services.factor_catalog import (
    CN_PAPER_FACTOR_SPECS,
    RELAY_FACTOR_SPECS,
    RESEARCH_FACTOR_SPECS,
    TA_FACTOR_SPECS,
)
from app.services.factor_value_store import (
    get_factor_definition,
    get_factor_group,
    normalize_factor_time,
)
from app.services.index_components import (
    ensure_index_components,
    load_index_symbols,
    normalize_index_symbol,
)

CORE_FACTORS = {
    "market_cap",
    "market_cap_rank",
    "is_st",
    "is_paused",
    "is_limit_up",
    "is_limit_down",
    "yesterday_limit_up",
    "v4gv",
    "v4gv_signal",
    "macd_positive",
    "indicator_buy_signal",
    "tsmf_overheat_penalty",
    "v4gv_dead_cross",
}
HIGH_VOLUME_FACTORS = {
    "cum_volume_at_time",
    "rolling_max_volume",
    "high_volume_ratio",
    "avoid_high_volume_ratio",
    "high_volume_signal",
}
ALPHA101_FACTORS = {f"alpha101_{index:03d}" for index in range(1, 102)}
CATALOG_FACTORS = (
    set(TA_FACTOR_SPECS)
    | set(RESEARCH_FACTOR_SPECS)
    | set(RELAY_FACTOR_SPECS)
    | set(CN_PAPER_FACTOR_SPECS)
    | ALPHA101_FACTORS
)
SUPPORTED_PRECOMPUTE_FACTORS = CORE_FACTORS | HIGH_VOLUME_FACTORS | CATALOG_FACTORS


def build_precompute_prepare(
    *,
    mode: str,
    factor_names: list[str] | None,
    group_name: str | None,
    start_date: date,
    end_date: date,
    symbols: list[str] | None,
    index_symbol: str | None,
    params: dict[str, Any] | None,
) -> dict[str, Any]:
    names = _resolve_factor_names(mode=mode, factor_names=factor_names, group_name=group_name)
    supported_names = [name for name in names if name in SUPPORTED_PRECOMPUTE_FACTORS or get_factor_definition(name) is not None]
    coverage_gaps = _build_coverage_gaps(
        factor_names=supported_names,
        start_date=start_date,
        end_date=end_date,
        index_symbol=index_symbol,
        params=params or {},
    )
    sync_plan = _build_sync_plan(
        coverage_gaps=coverage_gaps,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        index_symbol=index_symbol,
    )
    return {
        "can_precompute": len(coverage_gaps) == 0,
        "coverage_gaps": coverage_gaps,
        "sync_plan": sync_plan,
        "precompute_payload": {
            "mode": mode,
            "factor_names": factor_names or supported_names,
            "group_name": group_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "symbols": symbols,
            "index_symbol": index_symbol,
            "params": params or {},
        },
    }


def _resolve_factor_names(*, mode: str, factor_names: list[str] | None, group_name: str | None) -> list[str]:
    if mode == "single":
        return [name for name in factor_names or [] if name]
    if mode == "group":
        group = get_factor_group(group_name or "")
        if group is None:
            raise ValueError(f"Unknown factor group: {group_name}")
        return [str(name) for name in group.get("factor_names") or []]
    raise ValueError("mode must be single or group")


def _build_coverage_gaps(
    *,
    factor_names: list[str],
    start_date: date,
    end_date: date,
    index_symbol: str | None,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    checks: dict[str, dict[str, Any]] = {}
    if index_symbol:
        latest_index_date = _latest_index_component_date(index_symbol)
        if not _index_components_cover_request(index_symbol, start_date, end_date):
            checks["index_components"] = {
                "dependency": "index_components",
                "label": "指数成分",
                "latest_date": latest_index_date,
                "required_start": start_date.isoformat(),
                "required_end": end_date.isoformat(),
                "sync_step": "index_components",
                "reason": f"{normalize_index_symbol(index_symbol) or index_symbol} 指数成分不足",
            }

    timer_time = normalize_factor_time(str(params.get("time") or params.get("as_of_time") or "10:30"))
    high_volume_time = normalize_factor_time(str(params.get("time") or params.get("as_of_time") or "14:30"))

    for factor_name in factor_names:
        if factor_name in {"market_cap", "market_cap_rank"}:
            checks["stock_daily_basic"] = {
                "dependency": "stock_daily_basic",
                "label": "每日基础指标/市值",
                "latest_date": _latest_sqlite_date("stock_daily_basic", "trade_date"),
                "required_start": start_date.isoformat(),
                "required_end": end_date.isoformat(),
                "sync_step": "tushare_daily",
                "reason": "市值排序需要 stock_daily_basic.circ_mv/total_mv",
            }
        if factor_name in {"is_paused", "is_limit_up", "is_limit_down"}:
            checks[f"klines_minute:{timer_time}"] = {
                "dependency": "klines_minute",
                "label": f"分钟线 {timer_time}",
                "latest_date": _latest_market_date("klines_minute", timer_time=timer_time),
                "required_start": start_date.isoformat(),
                "required_end": end_date.isoformat(),
                "sync_step": "kline_minute",
                "timer_time": timer_time,
                "reason": f"{factor_name} 需要 {timer_time} 分钟行情",
            }
        if factor_name in {"is_limit_up", "is_limit_down", "yesterday_limit_up"}:
            checks["stock_limit_prices"] = {
                "dependency": "stock_limit_prices",
                "label": "涨跌停价格",
                "latest_date": _latest_sqlite_date("stock_limit_prices", "trade_date"),
                "required_start": start_date.isoformat(),
                "required_end": end_date.isoformat(),
                "sync_step": "tushare_daily",
                "reason": "涨跌停过滤需要 stock_limit_prices",
            }
        if factor_name == "yesterday_limit_up":
            checks["klines_daily"] = {
                "dependency": "klines_daily",
                "label": "日线行情",
                "latest_date": _latest_market_date("klines_daily"),
                "required_start": start_date.isoformat(),
                "required_end": end_date.isoformat(),
                "sync_step": "kline_daily",
                "reason": "昨日涨停判断需要日线收盘价",
            }
        if factor_name in HIGH_VOLUME_FACTORS:
            checks["klines_daily"] = {
                "dependency": "klines_daily",
                "label": "日线行情",
                "latest_date": _latest_market_date("klines_daily"),
                "required_start": start_date.isoformat(),
                "required_end": end_date.isoformat(),
                "sync_step": "kline_daily",
                "reason": "放量因子需要历史日成交量",
            }
            checks[f"klines_minute_cum_timer:{high_volume_time}"] = {
                "dependency": "klines_minute_cum_timer",
                "label": f"累计分钟成交量 {high_volume_time}",
                "latest_date": _latest_market_date("klines_minute_cum_timer", timer_time=high_volume_time),
                "required_start": start_date.isoformat(),
                "required_end": end_date.isoformat(),
                "sync_step": "cum_timer",
                "timer_time": high_volume_time,
                "reason": "放量因子需要指定时点累计成交量",
            }

    for factor_name in factor_names:
        definition = get_factor_definition(factor_name) or {}
        for dependency in definition.get("dependencies") or []:
            _append_catalog_dependency_check(
                checks,
                dependency=str(dependency),
                start_date=start_date,
                end_date=end_date,
                factor_name=factor_name,
            )

    gaps: list[dict[str, Any]] = []
    for item in checks.values():
        latest = _parse_date(item.get("latest_date"))
        if item.get("dependency") == "financial_data" and latest is not None and latest >= end_date - timedelta(days=400):
            continue
        if item.get("dependency") == "klines_minute_cum_timer":
            source_latest = _parse_date(_latest_market_date("klines_minute", timer_time=str(item.get("timer_time") or "14:30")))
            if source_latest is not None and source_latest < end_date:
                item["source_latest_date"] = source_latest.isoformat()
                item["missing_start"] = (latest + timedelta(days=1)).isoformat() if latest else start_date.isoformat()
                item["missing_end"] = end_date.isoformat()
                item["severity"] = "informational"
                item["sync_step"] = "none"
                item["reason"] = f"{item['reason']}；底层分钟线仅覆盖到 {source_latest.isoformat()}，暂无法同步到目标日期"
                continue
        if latest is None or latest < end_date:
            item["missing_start"] = (latest + timedelta(days=1)).isoformat() if latest else start_date.isoformat()
            item["missing_end"] = end_date.isoformat()
            item["severity"] = "blocking"
            gaps.append(item)
    return gaps


def _append_catalog_dependency_check(
    checks: dict[str, dict[str, Any]],
    *,
    dependency: str,
    start_date: date,
    end_date: date,
    factor_name: str,
) -> None:
    dep = dependency.strip()
    if not dep:
        return
    if dep in {"v4gv", "v4gv_signal", "macd_positive", "indicator_buy_signal", "v4gv_dead_cross", "tsmf_overheat_penalty"}:
        dep = "klines_daily"
    if dep.startswith("klines_daily") or dep == "klines_daily":
        checks.setdefault("klines_daily", {
            "dependency": "klines_daily",
            "label": "日线行情",
            "latest_date": _latest_market_date("klines_daily"),
            "required_start": start_date.isoformat(),
            "required_end": end_date.isoformat(),
            "sync_step": "kline_daily",
            "reason": f"{factor_name} 需要日线行情数据",
        })
        return
    if dep.startswith("klines_minute") or dep == "klines_minute":
        checks.setdefault("klines_minute", {
            "dependency": "klines_minute",
            "label": "分钟线行情",
            "latest_date": _latest_market_date("klines_minute"),
            "required_start": start_date.isoformat(),
            "required_end": end_date.isoformat(),
            "sync_step": "kline_minute",
            "reason": f"{factor_name} 需要分钟线行情数据",
        })
        return
    if dep.startswith("financial_data") or dep == "financial_data":
        checks.setdefault("financial_data", {
            "dependency": "financial_data",
            "label": "财务数据",
            "latest_date": _latest_sqlite_date("financial_data", "report_date"),
            "required_start": start_date.isoformat(),
            "required_end": end_date.isoformat(),
            "sync_step": "financial_data",
            "reason": f"{factor_name} 需要财务报表数据",
        })
        return
    if dep.startswith("stock_daily_basic") or dep == "stock_daily_basic":
        checks.setdefault("stock_daily_basic", {
            "dependency": "stock_daily_basic",
            "label": "每日基础指标/市值",
            "latest_date": _latest_sqlite_date("stock_daily_basic", "trade_date"),
            "required_start": start_date.isoformat(),
            "required_end": end_date.isoformat(),
            "sync_step": "tushare_daily",
            "reason": f"{factor_name} 需要 stock_daily_basic 数据",
        })
        return
    if dep.startswith("stock_limit_prices") or dep == "stock_limit_prices":
        checks.setdefault("stock_limit_prices", {
            "dependency": "stock_limit_prices",
            "label": "stock_limit_prices",
            "latest_date": _latest_sqlite_date("stock_limit_prices", "trade_date"),
            "required_start": start_date.isoformat(),
            "required_end": end_date.isoformat(),
            "sync_step": "tushare_daily",
            "reason": f"{factor_name} requires stock_limit_prices data",
        })


def _build_sync_plan(
    *,
    coverage_gaps: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    symbols: list[str] | None,
    index_symbol: str | None,
) -> dict[str, Any] | None:
    if not coverage_gaps:
        return None
    steps: list[dict[str, Any]] = []
    sync_steps = {str(gap["sync_step"]) for gap in coverage_gaps}
    if "index_components" in sync_steps and index_symbol:
        steps.append(_step("index_components", start_date, end_date, index_symbol=index_symbol))
    if "tushare_daily" in sync_steps:
        datasets = sorted({str(gap["dependency"]) for gap in coverage_gaps if gap["sync_step"] == "tushare_daily"})
        steps.append(_step("tushare_daily", start_date, end_date, datasets=datasets))
    if "kline_daily" in sync_steps:
        steps.append(_step("kline_daily", start_date, end_date))
    if "kline_minute" in sync_steps:
        times = sorted({str(gap["timer_time"]) for gap in coverage_gaps if gap["sync_step"] == "kline_minute" and gap.get("timer_time")})
        extra = {"timer_times": times} if times else {}
        steps.append(_step("kline_minute", start_date, end_date, **extra))
    if "cum_timer" in sync_steps:
        times = sorted({str(gap.get("timer_time") or "14:30") for gap in coverage_gaps if gap["sync_step"] == "cum_timer"})
        steps.append(_step("cum_timer", start_date, end_date, timer_times=times))
    if "financial_data" in sync_steps:
        steps.append(_step("financial_data", start_date, end_date))
    return {
        "sync_type": "factor_dependency",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "symbols": symbols,
        "index_symbol": index_symbol,
        "steps": steps,
        "coverage_gaps": coverage_gaps,
    }


def _step(step_type: str, start_date: date, end_date: date, **extra: Any) -> dict[str, Any]:
    return {
        "type": step_type,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        **extra,
    }


async def execute_factor_dependency_sync(
    service: Any,
    plan: dict[str, Any],
    *,
    run_id: str | None = None,
    task_id: int | None = None,
    failure_strategy: str = "stop",
    progress: Any,
) -> list[dict[str, Any]]:
    step_results: list[dict[str, Any]] = []
    for step in plan.get("steps") or []:
        step_type = str(step.get("type") or "")
        progress.details["current_step"] = step_type
        await service.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
        try:
            if step_type == "index_components":
                result = await ensure_index_components(
                    str(step.get("index_symbol") or plan.get("index_symbol")),
                    date.fromisoformat(step["start_date"]),
                    date.fromisoformat(step["end_date"]),
                )
            elif step_type == "tushare_daily":
                result = await asyncio.to_thread(_sync_tushare_daily_step, step)
            elif step_type == "kline_daily":
                result = await _sync_kline_daily_step(service, plan, step, run_id, task_id, failure_strategy, progress)
            elif step_type == "kline_minute":
                result = await _sync_kline_minute_step(service, plan, step, run_id, task_id, failure_strategy, progress)
            elif step_type == "cum_timer":
                result = await asyncio.to_thread(_sync_cum_timer_step, step)
            elif step_type == "financial_data":
                result = await _sync_financial_data_step(service, plan, step, run_id, task_id, failure_strategy, progress)
            else:
                result = {"skipped": True, "reason": f"unknown step: {step_type}"}
            step_results.append({"type": step_type, "status": "completed", "result": result})
        except Exception as exc:
            logger.opt(exception=True).error("Factor dependency sync step failed: {}", step_type)
            item = {"type": step_type, "status": "failed", "error": str(exc)}
            step_results.append(item)
            if failure_strategy == "stop":
                raise
        progress.current += 1
        progress.success_count += 1 if step_results[-1]["status"] == "completed" else 0
        progress.failed_count += 1 if step_results[-1]["status"] == "failed" else 0
        progress.details["step_results"] = step_results
        await service.persist_sync_progress(progress, run_id=run_id, sync_task_id=task_id)
    return step_results


def _sync_tushare_daily_step(step: dict[str, Any]) -> dict[str, Any]:
    from app.scripts.sync_tushare_daily_to_parquet import (
        fetch_daily_basic,
        fetch_limit_prices,
        iter_days,
        tushare_client,
        write_daily_basic_sqlite,
        write_limit_prices_sqlite,
    )

    token = (os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or "").strip()
    if not token:
        try:
            import tushare as ts

            token = (ts.get_token() or "").strip()
        except Exception:
            token = ""
    if not token:
        raise RuntimeError("Tushare token is not configured")
    pro = tushare_client(token)
    start = date.fromisoformat(step["start_date"])
    end = date.fromisoformat(step["end_date"])
    datasets = set(step.get("datasets") or [])
    db_path = Path(settings.data_dir) / "gaoshou.db"
    daily_basic_rows = 0
    limit_rows = 0
    for trade_date in iter_days(start, end):
        if "stock_daily_basic" in datasets:
            daily_basic_rows += write_daily_basic_sqlite(db_path, fetch_daily_basic(pro, trade_date))
        if "stock_limit_prices" in datasets:
            limit_rows += write_limit_prices_sqlite(db_path, fetch_limit_prices(pro, trade_date))
    return {"daily_basic_rows": daily_basic_rows, "limit_rows": limit_rows}


async def _sync_kline_daily_step(
    service: Any,
    plan: dict[str, Any],
    step: dict[str, Any],
    run_id: str | None,
    task_id: int | None,
    failure_strategy: str,
    outer_progress: Any,
) -> dict[str, Any]:
    import app.services.sync_service as sync_service_module

    progress = await service.sync_kline_daily(
        symbols=await _resolve_plan_symbols(plan),
        start_date=date.fromisoformat(step["start_date"]),
        end_date=date.fromisoformat(step["end_date"]),
        run_id=run_id,
        task_id=task_id,
        failure_strategy=failure_strategy,
    )
    sync_service_module._current_sync = outer_progress
    return progress.to_dict()


async def _sync_kline_minute_step(
    service: Any,
    plan: dict[str, Any],
    step: dict[str, Any],
    run_id: str | None,
    task_id: int | None,
    failure_strategy: str,
    outer_progress: Any,
) -> dict[str, Any]:
    import app.services.sync_service as sync_service_module

    progress = await service.sync_kline_minute(
        symbols=await _resolve_plan_symbols(plan),
        start_date=date.fromisoformat(step["start_date"]),
        end_date=date.fromisoformat(step["end_date"]),
        run_id=run_id,
        task_id=task_id,
        failure_strategy=failure_strategy,
    )
    sync_service_module._current_sync = outer_progress
    return progress.to_dict()


async def _sync_financial_data_step(
    service: Any,
    plan: dict[str, Any],
    step: dict[str, Any],
    run_id: str | None,
    task_id: int | None,
    failure_strategy: str,
    outer_progress: Any,
) -> dict[str, Any]:
    import app.services.sync_service as sync_service_module

    progress = await service.sync_financial_data(
        symbols=await _resolve_plan_symbols(plan),
        run_id=run_id,
        task_id=task_id,
        failure_strategy=failure_strategy,
    )
    sync_service_module._current_sync = outer_progress
    return progress.to_dict()


def _sync_cum_timer_step(step: dict[str, Any]) -> dict[str, Any]:
    from app.scripts.build_minute_cum_timer_parquet import build

    start = date.fromisoformat(step["start_date"])
    end_exclusive = date.fromisoformat(step["end_date"]) + timedelta(days=1)
    times = [str(item) for item in step.get("timer_times") or ["14:30"]]
    written = build(start, end_exclusive, times, batch_months=True)
    return {"rows_written": written, "times": times}


async def _resolve_plan_symbols(plan: dict[str, Any]) -> list[str] | None:
    symbols = plan.get("symbols")
    if symbols:
        return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    index_symbol = plan.get("index_symbol")
    if not index_symbol:
        return None
    start = date.fromisoformat(str(plan["start_date"]))
    end = date.fromisoformat(str(plan["end_date"]))
    return await load_index_symbols(str(index_symbol), start, end)


def _latest_sqlite_date(table: str, column: str) -> str | None:
    if table not in {"stock_daily_basic", "stock_limit_prices", "financial_data"}:
        return None
    with sqlite3.connect(settings.sqlite_db_path) as conn:
        row = conn.execute(f"SELECT MAX({column}) FROM {table}").fetchone()
    return str(row[0]) if row and row[0] else None


def _latest_index_component_date(index_symbol: str) -> str | None:
    idx = normalize_index_symbol(index_symbol) or index_symbol
    with sqlite3.connect(settings.sqlite_db_path) as conn:
        row = conn.execute(
            "SELECT MAX(trade_date) FROM index_components WHERE index_symbol = ?",
            (idx,),
        ).fetchone()
    return str(row[0]) if row and row[0] else None


def _index_components_cover_request(index_symbol: str, start_date: date, end_date: date) -> bool:
    idx = normalize_index_symbol(index_symbol) or index_symbol
    # Current-snapshot fallback is accepted for factor research when strict
    # point-in-time constituents have not been accumulated yet.
    with sqlite3.connect(settings.sqlite_db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM index_components
            WHERE index_symbol = ?
            """,
            (idx,),
        ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _latest_market_date(dataset: str, *, timer_time: str | None = None) -> str | None:
    store = get_market_data_store()
    exists = getattr(store, "_exists", None)
    glob_pattern = getattr(store, "_glob_pattern", None)
    if not callable(exists) or not callable(glob_pattern) or not exists(dataset):
        return None
    pattern = _latest_partition_pattern(dataset) or glob_pattern(dataset)
    if dataset == "klines_daily":
        sql = f"SELECT MAX(trade_date) FROM read_parquet('{pattern}', hive_partitioning=true)"
    else:
        where = ""
        if timer_time:
            safe_time = timer_time.replace("'", "''")
            where = f" WHERE strftime(datetime, '%H:%M') = '{safe_time}'"
        sql = f"SELECT MAX(CAST(datetime AS DATE)) FROM read_parquet('{pattern}', hive_partitioning=true){where}"
    try:
        row = duckdb.connect(":memory:").execute(sql).fetchone()
    except Exception:
        return None
    return str(row[0]) if row and row[0] else None


def _latest_partition_pattern(dataset: str) -> str | None:
    root = Path(settings.parquet_data_dir) / dataset
    if not root.exists():
        return None
    candidates: list[tuple[int, int, Path]] = []
    for year_dir in root.glob("year=*"):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except (IndexError, ValueError):
            continue
        for month_dir in year_dir.glob("month=*"):
            try:
                month = int(month_dir.name.split("=", 1)[1])
            except (IndexError, ValueError):
                continue
            if any(month_dir.glob("*.parquet")):
                candidates.append((year, month, month_dir))
    if not candidates:
        return None
    latest = max(candidates, key=lambda item: (item[0], item[1]))[2]
    return str(latest / "*.parquet").replace("\\", "/")


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None

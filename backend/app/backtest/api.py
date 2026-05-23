"""Backtest engine API - /api/backtest.

Flow: Frontend -> API -> EngineRegistry.get(engine) -> IBacktestEngine.run().
"""
import asyncio
import math
import time
import uuid
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
from fastapi import APIRouter, Query
from loguru import logger
from pydantic import BaseModel, Field

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.engine import EngineRegistry
from app.backtest.engine.data_provider import StoreDataProvider
from app.db.models.strategy import Strategy
from app.db.sqlite import async_session_factory
from app.models.factor import FactorConfig, BtConfig
from app.services.factor_backtest import factor_backtest_service
from app.services.index_catalog import get_index_item, list_index_items
from app.services.index_components import (
    KNOWN_INDEX_POOLS,
    index_pool_summary,
    load_index_symbols,
    normalize_index_symbol,
)
from app.services.timer_minute_sync import (
    find_earliest_timer_coverage_date,
    sync_timer_minute_points,
    timer_times_from_params,
    parse_timer_times,
)
from app.services.optimization_result_store import save_optimization_result
from app.services.runtime_tasks import register_task, update_task
from sqlalchemy import select
from app.backtest.strategies.builtin_templates import (
    DEFAULT_DUAL_STOCK_GRID_PARAM_GRID,
    DEFAULT_DUAL_STOCK_GRID_PARAMS,
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    DUAL_STOCK_GRID_STRATEGY_CODE,
    DUAL_STOCK_GRID_SYMBOLS,
    MULTI_FACTOR_STRATEGY_CODE,
    get_builtin_strategy_template,
    list_builtin_strategy_templates,
)

router = APIRouter()

_tasks: dict[str, dict] = {}
_task_handles: dict[str, asyncio.Task] = {}
_TASK_TTL_SECONDS = 3600
_MAX_TASKS = 100


def _cleanup_tasks() -> None:
    now = time.time()
    expired = [
        task_id
        for task_id, task in _tasks.items()
        if task.get("status") in ("done", "failed")
        and now - float(task.get("finished_at", now)) > _TASK_TTL_SECONDS
    ]
    for task_id in expired:
        _tasks.pop(task_id, None)
        _task_handles.pop(task_id, None)

    if len(_tasks) <= _MAX_TASKS:
        return
    sorted_items = sorted(_tasks.items(), key=lambda item: item[1].get("created_at", 0))
    for task_id, task in sorted_items[: max(0, len(_tasks) - _MAX_TASKS)]:
        if task.get("status") != "running":
            _tasks.pop(task_id, None)
            _task_handles.pop(task_id, None)


def _set_task_progress(
    task_id: str,
    task_store: dict,
    *,
    progress: float,
    phase: str,
    message: str,
    event_type: str = "progress",
) -> None:
    progress = max(0.0, min(float(progress), 0.99))
    progress = max(progress, float(task_store.get("progress") or 0.0))
    task_store["progress"] = progress
    live = task_store.setdefault(
        "live",
        {"events": [], "positions": {}, "metrics_snapshot": {}, "metadata": {}},
    )
    live.setdefault("events", []).append({"type": event_type, "message": message})
    live.setdefault("metadata", {})["phase"] = phase
    live["metadata"]["progress_message"] = message
    update_task(task_id, status="running", progress=progress, meta={"phase": phase})


async def _run_progress_heartbeat(
    task_id: str,
    task_store: dict,
    *,
    label: str,
    phase: str,
    start_progress: float = 0.15,
    max_progress: float = 0.88,
    interval_seconds: float = 5.0,
    expected_seconds: float = 180.0,
) -> None:
    start_ts = time.time()
    tick = 0
    while task_store.get("status") == "running":
        await asyncio.sleep(interval_seconds)
        if task_store.get("status") != "running":
            break
        elapsed = max(0.0, time.time() - start_ts)
        ratio = min(elapsed / max(expected_seconds, interval_seconds), 1.0)
        progress = start_progress + (max_progress - start_progress) * ratio
        tick += 1
        message = f"{label} AKQuant native WFO running internally... elapsed {int(elapsed)}s"
        live = task_store.setdefault(
            "live",
            {"events": [], "positions": {}, "metrics_snapshot": {}, "metadata": {}},
        )
        live.setdefault("metadata", {})["phase"] = phase
        live["metadata"]["progress_message"] = message
        live.setdefault("metrics_snapshot", {})["elapsed_seconds"] = int(elapsed)
        if tick % 6 == 0:
            live.setdefault("events", []).append({"type": "heartbeat", "message": message})
        task_store["progress"] = max(float(task_store.get("progress") or 0.0), min(progress, max_progress))
        update_task(task_id, status="running", progress=task_store["progress"], meta={"phase": phase, "elapsed_seconds": int(elapsed)})


def _sanitize_json(obj: object) -> object:
    """Recursively convert NaN/Inf/Timestamp values into JSON-safe values."""
    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        ts = pd.Timestamp(obj)
        return ts.strftime("%Y-%m-%d") if ts.hour == 0 and ts.minute == 0 else ts.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return 0.0 if math.isnan(v) or math.isinf(v) else v
    if isinstance(obj, float):
        return 0.0 if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {str(k): _sanitize_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_json(v) for v in obj]
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "strftime"):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    return obj


def _validate_backtest_request(req: "RunBacktestRequest", start: date, end: date) -> str | None:
    if end < start:
        return "end_date must be greater than or equal to start_date"
    if not req.symbols and not req.index_symbol:
        return "symbols or index_symbol is required"
    return None


def _timer_times_for_config(config: BacktestConfig):
    if config.timer_times:
        return parse_timer_times(config.timer_times)
    return timer_times_from_params(config.strategy_params)


def _market_dataset_for_bar_type(bar_type: str) -> str:
    text = str(bar_type or "daily").strip().lower()
    if text == "minute_timer":
        return "klines_minute_timer"
    if text == "minute":
        return "klines_minute"
    return "klines_daily"


def _coverage_ratio(covered: int, requested: int) -> float:
    if requested <= 0:
        return 0.0
    return round(covered / requested, 4)


def _is_multi_factor_strategy(config: BacktestConfig) -> bool:
    code = str(config.strategy_code or "")
    return "class MultiFactorStrategy" in code or (
        "FactorPipeline" in code and "FACTOR_CONFIGS" in code
    )


def _multi_factor_coverage(
    *,
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> dict:
    from app.services.factor_value_store import get_factor_value_store

    store = get_factor_value_store()
    factors = [
        {"name": "market_cap", "required": True},
        {"name": "market_cap_rank", "required": True},
        {"name": "v4gv", "required": True},
        {"name": "is_st", "required": False},
        {
            "name": "is_paused",
            "required": False,
            "as_of_time": "10:30",
            "params": {"time": "10:30"},
        },
        {
            "name": "is_limit_up",
            "required": False,
            "as_of_time": "10:30",
            "params": {"time": "10:30"},
        },
        {
            "name": "is_limit_down",
            "required": False,
            "as_of_time": "10:30",
            "params": {"time": "10:30"},
        },
    ]
    items = []
    requested = len(symbols)
    for item in factors:
        summary = store.coverage(
            item["name"],
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            as_of_time=item.get("as_of_time"),
            params=item.get("params"),
        )
        covered = int(summary.get("symbol_count") or 0)
        items.append({
            **summary,
            "required": bool(item["required"]),
            "coverage_ratio": _coverage_ratio(covered, requested),
        })
    required_items = [item for item in items if item["required"]]
    min_required_ratio = min((float(item["coverage_ratio"]) for item in required_items), default=0.0)
    return {
        "strategy": "multi_factor",
        "min_required_symbol_coverage": min_required_ratio,
        "items": items,
        "warning": min_required_ratio < 0.6,
    }


def _config_from_request(req: "RunBacktestRequest", start_date: date, end_date: date) -> BacktestConfig:
    return BacktestConfig(
        mode=req.mode,
        factor_expression=req.factor_expression,
        buy_condition=req.buy_condition,
        sell_condition=req.sell_condition,
        symbols=req.symbols,
        start_date=start_date,
        end_date=end_date,
        initial_capital=req.initial_capital,
        rebalance_freq=req.rebalance_freq,
        n_groups=req.n_groups,
        bar_type=req.bar_type,
        timer_times=req.timer_times,
        commission_rate=req.commission_rate,
        slippage=req.slippage,
        stamp_tax_rate=req.stamp_tax_rate,
        transfer_fee_rate=req.transfer_fee_rate,
        min_commission=req.min_commission,
        volume_limit_pct=req.volume_limit_pct,
        lot_size=req.lot_size,
        t_plus_one=req.t_plus_one,
        exit_on_last_bar=req.exit_on_last_bar,
        max_positions=req.max_positions,
        risk_config=req.risk_config,
        instruments_config=req.instruments_config,
        indicator_mode=req.indicator_mode,
        bootstrap_samples=req.bootstrap_samples,
        analysis_config=req.analysis_config,
        strategy_params=req.strategy_params,
        engine=req.engine,
        benchmark_symbol=req.benchmark_symbol,
        strategy_id=req.strategy_id,
        strategy_code=req.strategy_code,
        index_symbol=normalize_index_symbol(req.index_symbol),
        universe_mode=req.universe_mode,
    )


async def _prepare_backtest_config(
    req: "RunBacktestRequest",
    start_date: date,
    end_date: date,
    *,
    task_id: str | None = None,
) -> tuple[BacktestConfig | None, dict | None]:
    """Build a BacktestConfig and resolve index universes / stored strategy code."""
    config = _config_from_request(req, start_date, end_date)
    if config.index_symbol:
        config.universe_mode = "index"
        config.symbols = await load_index_symbols(config.index_symbol, start_date, end_date)
        if not config.symbols:
            return None, {
                "code": 1,
                "message": f"No index constituents found for {config.index_symbol}",
                "data": None,
            }

    if config.strategy_id is not None:
        async with async_session_factory() as session:
            result = await session.execute(
                select(Strategy).where(Strategy.id == config.strategy_id).limit(1)
            )
            strategy = result.scalars().first()
            if strategy is None:
                return None, {
                    "code": 1,
                    "message": f"Strategy not found: {config.strategy_id}",
                    "data": None,
                }
            if not config.strategy_code:
                config.strategy_code = strategy.code

    config._task_id = task_id
    return config, None




class RunBacktestRequest(BaseModel):
    mode: str = "vectorized"
    engine: str = "builtin"  # "builtin" | "akquant"
    factor_expression: str | None = None
    buy_condition: str | None = None
    sell_condition: str | None = None
    symbols: list[str] = Field(default_factory=list)
    universe_mode: str = "symbols"  # symbols | index
    index_symbol: str | None = None
    start_date: str
    end_date: str
    initial_capital: float = 1_000_000
    rebalance_freq: str = "monthly"
    n_groups: int = 5
    bar_type: str = "daily"
    timer_times: list[str] | None = None
    commission_rate: float = 0.0003
    slippage: float = 0.001
    stamp_tax_rate: float = 0.001
    transfer_fee_rate: float = 0.00001
    min_commission: float = 5.0
    volume_limit_pct: float | None = 0.25
    lot_size: int | dict[str, int] | None = 100
    t_plus_one: bool = True
    exit_on_last_bar: bool = True
    max_positions: int | None = None
    risk_config: dict | None = None
    instruments_config: list[dict] | dict[str, dict] | None = None
    indicator_mode: str = "precompute"
    bootstrap_samples: int = 1000
    analysis_config: dict | None = None
    strategy_params: dict | None = None
    strategy_id: int | None = None
    strategy_name: str | None = None
    strategy_code: str | None = None  # akquant strategy code
    benchmark_symbol: str | None = None  # benchmark index


class OptimizeRequest(RunBacktestRequest):
    param_grid: dict[str, list] = Field(default_factory=dict)
    sort_by: str | list[str] = "sharpe_ratio"
    ascending: bool | list[bool] = False
    max_workers: int = 1
    timeout: float | None = None


class WalkForwardRequest(OptimizeRequest):
    train_period: int = Field(default=252, gt=0)
    test_period: int = Field(default=63, gt=0)
    metric: str | list[str] = "sharpe_ratio"


class StrategyParamsSchemaRequest(BaseModel):
    strategy_code: str | None = None
    strategy_id: int | None = None


class StrategyParamsValidateRequest(StrategyParamsSchemaRequest):
    payload: dict = Field(default_factory=dict)


class BuiltinStrategyCreateRequest(BaseModel):
    name: str = Field(default="dual_stock_grid")


async def _save_backtest_result(
    task_id: str,
    config: BacktestConfig,
    result_dict: dict,
    success: bool,
):
    """Persist backtest result to database."""
    try:
        from app.db.models.strategy import Backtest

        async with async_session_factory() as session:
            code = config.strategy_code or config.factor_expression or config.buy_condition or ""

            strategy = None
            if config.strategy_id is not None:
                result = await session.execute(
                    select(Strategy).where(Strategy.id == config.strategy_id).limit(1)
                )
                strategy = result.scalars().first()

            if strategy is None:
                stmt = select(Strategy).where(Strategy.code == code).limit(1)
                result = await session.execute(stmt)
                strategy = result.scalars().first()

            if strategy is None:
                strategy = Strategy(
                    name=f"backtest-{task_id}",
                    code=code,
                description="完美世界 + 昆仑万维分钟级底仓网格策略",
                )
                session.add(strategy)
                await session.flush()

            backtest = Backtest(
                strategy_id=strategy.id,
                status="completed" if success else "failed",
                start_date=config.start_date or date.today(),
                end_date=config.end_date or date.today(),
                initial_capital=Decimal(str(config.initial_capital)),
                parameters={
                    "engine": config.engine,
                    "mode": config.mode,
                    "symbols": config.symbols,
                    "symbol_count": len(config.symbols),
                    "bar_type": config.bar_type,
                    "rebalance_freq": config.rebalance_freq,
                    "commission_rate": config.commission_rate,
                    "slippage": config.slippage,
                    "stamp_tax_rate": config.stamp_tax_rate,
                    "transfer_fee_rate": config.transfer_fee_rate,
                    "min_commission": config.min_commission,
                    "volume_limit_pct": config.volume_limit_pct,
                    "t_plus_one": config.t_plus_one,
                    "max_positions": config.max_positions,
                    "risk_config": config.risk_config,
                    "strategy_id": config.strategy_id,
                },
                result=result_dict if success else None,
            )
            session.add(backtest)
            await session.commit()
            logger.info("Backtest {} saved as DB id={}", task_id, backtest.id)
    except Exception as e:
        logger.error("Failed to persist backtest {}: {}", task_id, e)


@router.post("/run")
async def run_backtest(req: RunBacktestRequest):
    """Submit an async backtest task and return task_id."""
    from datetime import date as date_cls

    _cleanup_tasks()
    task_id = str(uuid.uuid4())[:8]
    start_date = date_cls.fromisoformat(req.start_date)
    end_date = date_cls.fromisoformat(req.end_date)
    validation_error = _validate_backtest_request(req, start_date, end_date)
    if validation_error:
        return {"code": 1, "message": validation_error, "data": None}

    # Validate engine availability.
    try:
        EngineRegistry.get(req.engine)
    except ValueError:
        return {"code": 1, "message": f"Unknown engine: {req.engine}", "data": None}

    config, config_error = await _prepare_backtest_config(
        req, start_date, end_date, task_id=task_id
    )
    if config_error:
        return config_error
    assert config is not None

    task_store: dict = {
        "status": "queued",
        "progress": 0,
        "result": None,
        "live": None,
        "created_at": time.time(),
    }
    _tasks[task_id] = task_store
    register_task(
        task_id=task_id,
        kind="backtest",
        title=f"回测任务 {req.strategy_name or req.strategy_id or req.engine}",
        status="queued",
        progress=0,
        result_ref=f"/backtest?task_id={task_id}",
        meta={"engine": req.engine, "start_date": req.start_date, "end_date": req.end_date},
    )

    async def _run():
        try:
            task_store["status"] = "running"
            task_store["live"] = {
                "current_date": str(start_date),
                "events": [{
                    "type": "queued",
                    "timestamp": time.time(),
                    "message": "回测任务已启动，正在准备配置",
                }],
                "positions": {},
                "metrics_snapshot": {},
                "metadata": {
                    "phase": "preparing",
                    "progress_message": "正在准备回测配置",
                    "bar_type": config.bar_type,
                    "engine": config.engine,
                    "symbol_count": len(config.symbols or []),
                },
            }
            update_task(
                task_id,
                status="running",
                progress=0.01,
                meta=task_store["live"]["metadata"],
            )
            if config.engine == "akquant" and config.bar_type == "minute_timer":
                timer_times = _timer_times_for_config(config)
                coverage = await asyncio.to_thread(
                    find_earliest_timer_coverage_date,
                    symbols=config.symbols,
                    index_symbol=config.index_symbol,
                    start=config.start_date or start_date,
                    end=config.end_date or end_date,
                    timer_times=timer_times,
                )
                earliest = coverage.get("earliest_date")
                if earliest:
                    earliest_date = date.fromisoformat(str(earliest))
                    if config.start_date and config.start_date < earliest_date:
                        config.start_date = earliest_date
                        task_store["timer_coverage"] = _sanitize_json(coverage)
                        task_store["live"] = {
                            "current_date": None,
                            "events": [{
                                "type": "timer_coverage_adjusted",
                                "timestamp": time.time(),
                                "requested_start": start_date.isoformat(),
                                "actual_start": earliest,
                            }],
                            "positions": {},
                            "metrics_snapshot": {},
                        }

                def on_sync_progress(event: dict):
                    total = max(1, int(event.get("total") or 1))
                    done = int(event.get("done") or 0)
                    task_store["progress"] = min(0.25, done / total * 0.25)
                    task_store["live"] = {
                        "current_date": None,
                        "events": [{
                            "type": "sync_timer_minute",
                            "timestamp": time.time(),
                            **event,
                        }],
                        "positions": {},
                        "metrics_snapshot": {
                            "sync_inserted": event.get("inserted", 0),
                            "sync_fetched": event.get("fetched", 0),
                        },
                    }

                sync_summary = await sync_timer_minute_points(
                    symbols=config.symbols,
                    index_symbol=config.index_symbol,
                    start=config.start_date or start_date,
                    end=config.end_date or end_date,
                    timer_times=timer_times,
                    progress_callback=on_sync_progress,
                )
                task_store["sync_summary"] = sync_summary

            data_provider = StoreDataProvider()
            engine_class = EngineRegistry.get(config.engine)
            engine = engine_class()

            def on_progress(pct: float, live: dict | None):
                task_store["progress"] = 0.25 + pct * 0.75 if config.bar_type == "minute_timer" else pct
                if live:
                    task_store["live"] = live
                metadata = (task_store.get("live") or {}).get("metadata") or {}
                update_task(
                    task_id,
                    progress=task_store["progress"],
                    meta=metadata if isinstance(metadata, dict) else None,
                )

            result = await engine.run(config, data_provider, progress_callback=on_progress)
            result_dict = _sanitize_json(result.to_dict())
            if task_store.get("sync_summary"):
                result_dict["sync_summary"] = _sanitize_json(task_store["sync_summary"])
            task_store["status"] = "done"
            task_store["progress"] = 1.0
            task_store["result"] = result_dict
            task_store["live"] = task_store.get("live")
            task_store["finished_at"] = time.time()
            update_task(task_id, status="done", progress=1.0, result_ref=f"/backtest?task_id={task_id}")
            await _save_backtest_result(task_id, config, result_dict, success=True)
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            logger.error("Backtest task {} failed: {} ({})", task_id, e, type(e).__name__)
            task_store["status"] = "failed"
            task_store["progress"] = 1.0
            task_store["result"] = _sanitize_json({"error": f"{type(e).__name__}: {e}"})
            task_store["finished_at"] = time.time()
            update_task(
                task_id,
                status="failed",
                progress=1.0,
                result_ref=f"/backtest?task_id={task_id}",
                error=f"{type(e).__name__}: {e}",
            )
            await _save_backtest_result(task_id, config, task_store["result"], success=False)

    _task_handles[task_id] = asyncio.create_task(_run())
    return {"code": 0, "message": "success", "data": {"task_id": task_id}}


@router.get("/status/{task_id}")
async def get_status(task_id: str):
    """Return async backtest task status."""
    _cleanup_tasks()
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    return {
        "code": 0,
        "message": "success",
        "data": {
            "status": task["status"],
            "progress": task.get("progress", 0),
            "live": task.get("live"),
        },
    }


@router.get("/result/{task_id}")
async def get_result(task_id: str):
    """Return async backtest task result."""
    _cleanup_tasks()
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    if task["status"] not in ("done", "failed", "cancelled"):
        return {"code": 1, "message": f"Task status: {task['status']}", "data": None}
    return {"code": 0, "message": "success", "data": task["result"]}


@router.post("/cancel/{task_id}")
async def cancel_task(task_id: str):
    """Cancel a running backtest or optimization task."""
    _cleanup_tasks()
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    if task.get("status") in ("done", "failed", "cancelled"):
        return {"code": 0, "message": "Task already finished", "data": {"task_id": task_id, "status": task.get("status")}}

    handle = _task_handles.get(task_id)
    if handle is not None and not handle.done():
        handle.cancel()
    _task_handles.pop(task_id, None)

    task["status"] = "cancelled"
    task["progress"] = 1.0
    task["result"] = _sanitize_json({"error": "Task cancelled by user"})
    task["finished_at"] = time.time()
    live = task.setdefault("live", {"events": [], "positions": {}, "metrics_snapshot": {}, "metadata": {}})
    live.setdefault("events", []).append({"type": "cancelled", "message": "Task cancelled by user"})
    live.setdefault("metadata", {})["phase"] = "cancelled"
    live["metadata"]["progress_message"] = "任务已停止"
    update_task(
        task_id,
        status="cancelled",
        progress=1.0,
        result_ref=f"/backtest?task_id={task_id}",
        error="Task cancelled by user",
        meta={"phase": "cancelled"},
    )
    return {"code": 0, "message": "success", "data": {"task_id": task_id, "status": "cancelled"}}


@router.get("/engines")
async def list_engines():
    """List available backtest engines."""
    engines = EngineRegistry.list_all()
    return {"code": 0, "message": "success", "data": engines}


@router.get("/capabilities")
async def get_capabilities():
    """Return optional AKQuant feature availability for UI gating."""
    from app.backtest.engine.akquant.capabilities import get_akquant_capabilities

    return {
        "code": 0,
        "message": "success",
        "data": {
            "engines": EngineRegistry.list_all(),
            "akquant": get_akquant_capabilities(),
        },
    }


@router.get("/presets/dual-stock-grid")
async def get_dual_stock_grid_preset():
    """Return the built-in dual-stock grid strategy preset."""
    return {
        "code": 0,
        "message": "success",
        "data": {
            "name": "双标的底仓网格",
            "strategy_key": "dual_stock_grid",
            "symbols": DUAL_STOCK_GRID_SYMBOLS,
            "engine": "akquant",
            "mode": "event_driven",
            "bar_type": "minute",
            "strategy_code": DUAL_STOCK_GRID_STRATEGY_CODE,
            "strategy_params": DEFAULT_DUAL_STOCK_GRID_PARAMS,
            "param_grid": DEFAULT_DUAL_STOCK_GRID_PARAM_GRID,
            "walk_forward": {
                "train_period": 14400,
                "test_period": 2400,
                "metric": "calmar_ratio",
                "ascending": False,
            },
        },
    }


@router.post("/presets/dual-stock-grid/strategy")
async def create_dual_stock_grid_strategy(req: BuiltinStrategyCreateRequest):
    """Create or update the built-in dual-stock grid strategy in the strategy list."""
    strategy_name = "双标的底仓网格"
    async with async_session_factory() as session:
        result = await session.execute(
            select(Strategy).where(Strategy.name == strategy_name).limit(1)
        )
        strategy = result.scalars().first()
        if strategy is None:
            strategy = Strategy(
                name=strategy_name,
                code=DUAL_STOCK_GRID_STRATEGY_CODE,
                parameters=DEFAULT_DUAL_STOCK_GRID_PARAMS,
                description="完美世界 + 昆仑万维分钟级底仓网格策略",
            )
            session.add(strategy)
            await session.flush()
        else:
            strategy.code = DUAL_STOCK_GRID_STRATEGY_CODE
            strategy.parameters = DEFAULT_DUAL_STOCK_GRID_PARAMS
            strategy.description = "完美世界 + 昆仑万维分钟级底仓网格策略"
        await session.commit()
        return {
            "code": 0,
            "message": "success",
            "data": {
                "id": strategy.id,
                "name": strategy.name,
                "code": strategy.code,
                "parameters": strategy.parameters,
                "description": strategy.description,
            },
        }


@router.post("/presets/multi-factor/strategy")
async def create_multi_factor_strategy(req: BuiltinStrategyCreateRequest):
    """Create or update the built-in generic multi-factor strategy in the strategy list."""
    strategy_name = "通用多因子模型"
    parameters = {
        **DEFAULT_MULTI_FACTOR_PARAMS,
        "risk_config": DEFAULT_MULTI_FACTOR_RISK_CONFIG,
        "backtest_settings": {
            "engine": "akquant",
            "barType": "daily",
            "showOptimizationPanel": False,
        },
    }
    async with async_session_factory() as session:
        result = await session.execute(
            select(Strategy).where(Strategy.name == strategy_name).limit(1)
        )
        strategy = result.scalars().first()
        if strategy is None:
            strategy = Strategy(
                name=strategy_name,
                code=MULTI_FACTOR_STRATEGY_CODE,
                parameters=parameters,
                description="AKQuant 通用多因子选股模板；因子、权重、标准化和 ML 接口在策略代码顶部配置。",
            )
            session.add(strategy)
            await session.flush()
        else:
            strategy.code = MULTI_FACTOR_STRATEGY_CODE
            strategy.parameters = parameters
            strategy.description = "AKQuant 通用多因子选股模板；因子、权重、标准化和 ML 接口在策略代码顶部配置。"
        await session.commit()
        return {
            "code": 0,
            "message": "success",
            "data": {
                "id": strategy.id,
                "name": strategy.name,
                "code": strategy.code,
                "parameters": strategy.parameters,
                "description": strategy.description,
            },
        }


@router.post("/optimize/grid")
async def optimize_grid(req: OptimizeRequest):
    """Run AKQuant Grid Search asynchronously and return a task id."""
    from datetime import date as date_cls
    from app.services.akquant_optimize import run_grid_search

    _cleanup_tasks()
    task_id = str(uuid.uuid4())[:8]
    start_date = date_cls.fromisoformat(req.start_date)
    end_date = date_cls.fromisoformat(req.end_date)
    validation_error = _validate_backtest_request(req, start_date, end_date)
    if validation_error:
        return {"code": 1, "message": validation_error, "data": None}
    if req.engine != "akquant":
        return {"code": 1, "message": "Grid Search currently requires engine='akquant'", "data": None}
    if not req.param_grid:
        return {"code": 1, "message": "param_grid cannot be empty", "data": None}

    config, config_error = await _prepare_backtest_config(
        req, start_date, end_date, task_id=task_id
    )
    if config_error:
        return config_error
    assert config is not None
    task_store = {
        "status": "queued",
        "progress": 0,
        "result": None,
        "live": None,
        "created_at": time.time(),
    }
    _tasks[task_id] = task_store
    register_task(
        task_id=task_id,
        kind="optimization",
        title="Grid Search 参数优化",
        status="queued",
        progress=0,
        result_ref=f"/backtest?task_id={task_id}",
        meta={"optimization_type": "grid_search"},
    )

    async def _run():
        try:
            task_store["status"] = "running"
            task_store["live"] = {
                "current_date": str(start_date),
                "events": [{"type": "optimize_start", "message": "AKQuant Grid Search started"}],
                "positions": {},
                "metrics_snapshot": {"param_count": len(req.param_grid)},
                "metadata": {"phase": "optimization"},
            }
            _set_task_progress(
                task_id,
                task_store,
                progress=0.05,
                phase="prepare",
                message="准备 Grid Search 参数组合",
                event_type="optimize_prepare",
            )
            await asyncio.sleep(0)
            _set_task_progress(
                task_id,
                task_store,
                progress=0.15,
                phase="running",
                message="AKQuant Grid Search 正在运行",
                event_type="optimize_running",
            )
            rows = await run_grid_search(
                config,
                req.param_grid,
                sort_by=req.sort_by,
                ascending=req.ascending,
                max_workers=req.max_workers,
                timeout=req.timeout,
            )
            _set_task_progress(
                task_id,
                task_store,
                progress=0.9,
                phase="saving",
                message="Grid Search 完成，正在保存结果",
                event_type="optimize_saving",
            )
            task_store["status"] = "done"
            task_store["progress"] = 1.0
            task_store["result"] = _sanitize_json({
                "rows": rows,
                "count": len(rows),
                "sort_by": req.sort_by,
                "ascending": req.ascending,
            })
            update_task(task_id, status="done", progress=1.0, result_ref=f"/backtest?task_id={task_id}")
            db_id = await save_optimization_result(
                task_id=task_id,
                optimization_type="grid_search",
                config=config,
                request_params={
                    "param_grid": req.param_grid,
                    "sort_by": req.sort_by,
                    "ascending": req.ascending,
                    "max_workers": req.max_workers,
                    "timeout": req.timeout,
                },
                result=task_store["result"],
                success=True,
            )
            if db_id is not None:
                task_store["result"]["backtest_id"] = db_id
            task_store["finished_at"] = time.time()
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            logger.error("Optimization task {} failed: {} ({})", task_id, e, type(e).__name__)
            task_store["status"] = "failed"
            task_store["progress"] = 1.0
            task_store["result"] = _sanitize_json({"error": f"{type(e).__name__}: {e}"})
            update_task(
                task_id,
                status="failed",
                progress=1.0,
                result_ref=f"/backtest?task_id={task_id}",
                error=f"{type(e).__name__}: {e}",
            )
            await save_optimization_result(
                task_id=task_id,
                optimization_type="grid_search",
                config=config,
                request_params={
                    "param_grid": req.param_grid,
                    "sort_by": req.sort_by,
                    "ascending": req.ascending,
                    "max_workers": req.max_workers,
                    "timeout": req.timeout,
                },
                result=task_store["result"],
                success=False,
            )
            task_store["finished_at"] = time.time()

    _task_handles[task_id] = asyncio.create_task(_run())
    return {"code": 0, "message": "success", "data": {"task_id": task_id}}


@router.post("/optimize/walk-forward")
async def optimize_walk_forward(req: WalkForwardRequest):
    """Run AKQuant Walk-forward Validation asynchronously and return a task id."""
    from datetime import date as date_cls
    from app.services.akquant_optimize import run_walk_forward

    _cleanup_tasks()
    task_id = str(uuid.uuid4())[:8]
    start_date = date_cls.fromisoformat(req.start_date)
    end_date = date_cls.fromisoformat(req.end_date)
    validation_error = _validate_backtest_request(req, start_date, end_date)
    if validation_error:
        return {"code": 1, "message": validation_error, "data": None}
    if req.engine != "akquant":
        return {"code": 1, "message": "Walk-forward currently requires engine='akquant'", "data": None}
    if not req.param_grid:
        return {"code": 1, "message": "param_grid cannot be empty", "data": None}

    config, config_error = await _prepare_backtest_config(
        req, start_date, end_date, task_id=task_id
    )
    if config_error:
        return config_error
    assert config is not None
    task_store = {
        "status": "queued",
        "progress": 0,
        "result": None,
        "live": None,
        "created_at": time.time(),
    }
    _tasks[task_id] = task_store
    register_task(
        task_id=task_id,
        kind="optimization",
        title="Walk-forward 滚动验证",
        status="queued",
        progress=0,
        result_ref=f"/backtest?task_id={task_id}",
        meta={"optimization_type": "walk_forward"},
    )

    async def _run():
        try:
            task_store["status"] = "running"
            task_store["live"] = {
                "current_date": str(start_date),
                "events": [{"type": "walk_forward_start", "message": "AKQuant Walk-forward started"}],
                "positions": {},
                "metrics_snapshot": {
                    "param_count": len(req.param_grid),
                    "train_period": req.train_period,
                    "test_period": req.test_period,
                },
                "metadata": {"phase": "walk_forward"},
            }
            _set_task_progress(
                task_id,
                task_store,
                progress=0.05,
                phase="prepare",
                message="准备 Walk-forward 滚动窗口",
                event_type="walk_forward_prepare",
            )
            await asyncio.sleep(0)
            _set_task_progress(
                task_id,
                task_store,
                progress=0.15,
                phase="running",
                message="AKQuant Walk-forward 正在运行",
                event_type="walk_forward_running",
            )
            def _on_wfo_progress(
                phase: str,
                message: str,
                progress: float | None = None,
                meta: dict | None = None,
            ) -> None:
                current = float(task_store.get("progress") or 0.0)
                _set_task_progress(
                    task_id,
                    task_store,
                    progress=max(current, float(progress if progress is not None else current)),
                    phase=phase,
                    message=message,
                    event_type=f"walk_forward_{phase}",
                )
                if meta:
                    task_store.setdefault("live", {}).setdefault("metrics_snapshot", {}).update(meta)

            heartbeat = asyncio.create_task(
                _run_progress_heartbeat(
                    task_id,
                    task_store,
                    label="Walk-forward",
                    phase="akquant_running",
                    start_progress=0.25,
                    max_progress=0.84,
                    interval_seconds=5.0,
                    expected_seconds=900.0,
                )
            )
            try:
                rows = await run_walk_forward(
                    config,
                    req.param_grid,
                    train_period=req.train_period,
                    test_period=req.test_period,
                    metric=req.metric,
                    ascending=req.ascending,
                    max_workers=req.max_workers,
                    timeout=req.timeout,
                    progress_callback=_on_wfo_progress,
                )
            finally:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            _set_task_progress(
                task_id,
                task_store,
                progress=0.9,
                phase="saving",
                message="Walk-forward 完成，正在保存结果",
                event_type="walk_forward_saving",
            )
            task_store["status"] = "done"
            task_store["progress"] = 1.0
            task_store["result"] = _sanitize_json({
                "rows": rows,
                "count": len(rows),
                "metric": req.metric,
                "ascending": req.ascending,
                "train_period": req.train_period,
                "test_period": req.test_period,
            })
            update_task(task_id, status="done", progress=1.0, result_ref=f"/backtest?task_id={task_id}")
            db_id = await save_optimization_result(
                task_id=task_id,
                optimization_type="walk_forward",
                config=config,
                request_params={
                    "param_grid": req.param_grid,
                    "metric": req.metric,
                    "ascending": req.ascending,
                    "train_period": req.train_period,
                    "test_period": req.test_period,
                    "max_workers": req.max_workers,
                    "timeout": req.timeout,
                },
                result=task_store["result"],
                success=True,
            )
            if db_id is not None:
                task_store["result"]["backtest_id"] = db_id
            task_store["finished_at"] = time.time()
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            logger.error("Walk-forward task {} failed: {} ({})", task_id, e, type(e).__name__)
            task_store["status"] = "failed"
            task_store["progress"] = 1.0
            task_store["result"] = _sanitize_json({"error": f"{type(e).__name__}: {e}"})
            update_task(
                task_id,
                status="failed",
                progress=1.0,
                result_ref=f"/backtest?task_id={task_id}",
                error=f"{type(e).__name__}: {e}",
            )
            await save_optimization_result(
                task_id=task_id,
                optimization_type="walk_forward",
                config=config,
                request_params={
                    "param_grid": req.param_grid,
                    "metric": req.metric,
                    "ascending": req.ascending,
                    "train_period": req.train_period,
                    "test_period": req.test_period,
                    "max_workers": req.max_workers,
                    "timeout": req.timeout,
                },
                result=task_store["result"],
                success=False,
            )
            task_store["finished_at"] = time.time()

    _task_handles[task_id] = asyncio.create_task(_run())
    return {"code": 0, "message": "success", "data": {"task_id": task_id}}


async def _resolve_strategy_code(strategy_code: str | None, strategy_id: int | None) -> tuple[str | None, dict | None]:
    if strategy_code:
        return strategy_code, None
    if strategy_id is None:
        return None, {"code": 1, "message": "strategy_code or strategy_id is required", "data": None}

    async with async_session_factory() as session:
        result = await session.execute(
            select(Strategy).where(Strategy.id == strategy_id).limit(1)
        )
        strategy = result.scalars().first()
    if strategy is None:
        return None, {"code": 1, "message": f"Strategy not found: {strategy_id}", "data": None}
    return strategy.code, None


@router.post("/strategy-params/schema")
async def get_strategy_params_schema(req: StrategyParamsSchemaRequest):
    """Return AKQuant strategy parameter schema for UI form generation."""
    from app.services.akquant_params import get_strategy_param_schema

    strategy_code, error = await _resolve_strategy_code(req.strategy_code, req.strategy_id)
    if error:
        return error
    try:
        schema = get_strategy_param_schema(strategy_code or "")
        return {"code": 0, "message": "success", "data": schema}
    except Exception as e:
        logger.error("Strategy parameter schema failed: {} ({})", e, type(e).__name__)
        return {"code": 1, "message": f"{type(e).__name__}: {e}", "data": None}


@router.post("/strategy-params/validate")
async def validate_strategy_params_payload(req: StrategyParamsValidateRequest):
    """Validate AKQuant strategy parameters before running a backtest."""
    from app.services.akquant_params import validate_strategy_params

    strategy_code, error = await _resolve_strategy_code(req.strategy_code, req.strategy_id)
    if error:
        return error
    try:
        params = validate_strategy_params(strategy_code or "", req.payload)
        return {"code": 0, "message": "success", "data": params}
    except Exception as e:
        logger.error("Strategy parameter validation failed: {} ({})", e, type(e).__name__)
        return {"code": 1, "message": f"{type(e).__name__}: {e}", "data": None}


@router.get("/report/{task_id}")
async def get_report(task_id: str):
    """Return quantstats HTML report."""
    from app.backtest.engine.akquant.reporter import get_report_path, serve_report

    _cleanup_tasks()
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}

    html = serve_report(task_id)
    if html:
        from fastapi.responses import HTMLResponse
        return HTMLResponse(content=html)

    # Fallback to report_path stored in task result.
    result = task.get("result")
    if result and result.get("report_path"):
        path = result["report_path"]
        try:
            with open(path, "r", encoding="utf-8") as f:
                from fastapi.responses import HTMLResponse
                return HTMLResponse(content=f.read())
        except FileNotFoundError:
            pass

    return {"code": 1, "message": "Report not available", "data": None}


@router.post("/factor")
async def run_factor_backtest(config: FactorConfig, bt_config: BtConfig | None = None):
    """Run factor quantile-based layered backtest."""
    report = await factor_backtest_service.run(config, bt_config)
    return {"code": 0, "data": report.model_dump()}


# Stock pools
_POOL_CACHE: dict[str, list[str]] = {}


@router.get("/pools/{pool_name}")
async def get_pool_symbols(pool_name: str):
    """Return predefined stock pools: top100/top300/top500 by one-year average amount."""
    if pool_name in _POOL_CACHE:
        return {"code": 0, "data": {"symbols": _POOL_CACHE[pool_name]}}

    if pool_name == "all":
        from app.db.models.stock import Stock
        from app.core.config import settings
        from sqlalchemy import create_engine, select
        url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(
                select(Stock.symbol).where(Stock.is_st == 0, Stock.is_delist == 0)
            ).all()
        engine.dispose()
        symbols = [r[0] for r in rows]
        _POOL_CACHE["all"] = symbols
        logger.info("Pool all: {} symbols loaded", len(symbols))
        return {"code": 0, "data": {"symbols": symbols}}

    size_map = {"top100": 100, "top300": 300, "top500": 500}
    limit = size_map.get(pool_name)
    if limit is None:
        return {"code": 1, "message": f"Unknown pool: {pool_name}. Use top100/top300/top500/all", "data": None}

    try:
        from app.data_stores import get_market_data_store

        store = get_market_data_store()
        symbols = store.top_by_avg_amount(
            start_date=date.today() - timedelta(days=365),
            end_date=date.today(),
            limit=limit,
        )

        _POOL_CACHE[pool_name] = symbols
        logger.info("Pool {}: {} symbols loaded", pool_name, len(symbols))
        return {"code": 0, "data": {"symbols": symbols}}
    except Exception as e:
        logger.error("Pool {} query failed: {}", pool_name, e)
        return {"code": 1, "message": f"Query failed: {e}", "data": None}


# Stock names and index pools

@router.get("/index-pools")
async def list_index_pools():
    items = [
        {
            "symbol": item.symbol,
            "jq_symbol": item.jq_symbol or item.symbol,
            "name": item.display_name,
            "display_name": item.display_name,
            "component_mode": item.component_mode,
            "pool_enabled": item.pool_enabled,
        }
        for item in list_index_items(pool_only=True)
    ]
    return {"code": 0, "data": items}


@router.get("/index-pools/{index_symbol}")
async def get_index_pool(
    index_symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
):
    start = date.fromisoformat(start_date) if start_date else date.today() - timedelta(days=365)
    end = date.fromisoformat(end_date) if end_date else date.today()
    item = get_index_item(index_symbol)
    if item is None:
        return {"code": 1, "message": f"Unknown index symbol: {index_symbol}", "data": None}
    if not item.pool_enabled:
        return {
            "code": 0,
            "data": {
                "index_symbol": item.symbol,
                "display_name": item.display_name,
                "pool_enabled": False,
                "component_status": "unavailable",
                "reason": "market_only_index" if item.benchmark_enabled else "strict_snapshot_missing",
                "symbol_count": 0,
                "snapshot_count": 0,
                "symbols": [],
            },
        }
    try:
        summary = await index_pool_summary(item.symbol, start, end)
        return {"code": 0, "data": summary}
    except Exception as e:
        logger.error("Index pool {} query failed: {}", index_symbol, e)
        return {"code": 1, "message": f"Index pool query failed: {e}", "data": None}


@router.get("/timer-coverage")
async def get_timer_coverage(
    index_symbol: str | None = None,
    symbols: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    times: str = "10:00,10:30,14:30,14:50",
):
    start = date.fromisoformat(start_date) if start_date else date(2021, 5, 15)
    end = date.fromisoformat(end_date) if end_date else date.today()
    try:
        summary = find_earliest_timer_coverage_date(
            symbols=[s.strip() for s in symbols.split(",") if s.strip()],
            index_symbol=index_symbol,
            start=start,
            end=end,
            timer_times=parse_timer_times(times),
        )
        return {"code": 0, "data": summary}
    except Exception as e:
        logger.error("Timer coverage query failed: {}", e)
        return {"code": 1, "message": f"Timer coverage query failed: {e}", "data": None}


@router.post("/data-coverage")
async def get_backtest_data_coverage(req: RunBacktestRequest):
    """Check market and factor data coverage for a backtest request."""
    from datetime import date as date_cls
    from app.data_stores import get_market_data_store

    start_date = date_cls.fromisoformat(req.start_date)
    end_date = date_cls.fromisoformat(req.end_date)
    validation_error = _validate_backtest_request(req, start_date, end_date)
    if validation_error:
        return {"code": 1, "message": validation_error, "data": None}

    config, config_error = await _prepare_backtest_config(req, start_date, end_date)
    if config_error:
        return config_error
    assert config is not None

    symbols = [str(item) for item in config.symbols or [] if str(item).strip()]
    dataset = _market_dataset_for_bar_type(config.bar_type)
    try:
        store = get_market_data_store()
        market = store.coverage(
            symbols,
            start_date,
            end_date,
            dataset=dataset,
            timer_times=_timer_times_for_config(config),
        )
        covered_symbols = [str(item) for item in market.get("symbols_covered") or []]
        requested_set = set(symbols)
        covered_set = set(covered_symbols)
        missing_symbols = sorted(requested_set - covered_set)
        market_summary = {
            **market,
            "dataset": dataset,
            "requested_symbol_count": len(symbols),
            "covered_symbol_count": len(covered_symbols),
            "missing_symbol_count": len(missing_symbols),
            "coverage_ratio": _coverage_ratio(len(covered_symbols), len(symbols)),
            "missing_symbols_sample": missing_symbols[:30],
            "symbols_covered_sample": covered_symbols[:30],
        }

        factor_summary = None
        if _is_multi_factor_strategy(config):
            factor_summary = _multi_factor_coverage(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )

        warnings = []
        if market_summary["coverage_ratio"] < 0.8:
            warnings.append(
                f"{dataset} symbol coverage is {market_summary['coverage_ratio']:.0%}"
            )
        if factor_summary and factor_summary.get("warning"):
            warnings.append(
                "multi-factor required factor coverage is below 60%"
            )

        return {
            "code": 0,
            "message": "success",
            "data": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "bar_type": config.bar_type,
                "index_symbol": config.index_symbol,
                "symbol_count": len(symbols),
                "market": market_summary,
                "factor": factor_summary,
                "warnings": warnings,
                "ok": not warnings,
            },
        }
    except Exception as e:
        logger.error("Backtest data coverage query failed: {}", e)
        return {"code": 1, "message": f"Data coverage query failed: {e}", "data": None}


@router.get("/stock-names")
async def get_stock_names(symbols: str = ""):
    """Return stock name mapping: ?symbols=000001.SZ,600000.SH."""
    if not symbols.strip():
        return {"code": 0, "data": {}}
    try:
        sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
        if not sym_list:
            return {"code": 0, "data": {}}
        from app.core.config import settings
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(
                select(Stock.symbol, Stock.name).where(Stock.symbol.in_(sym_list))
            ).all()
        engine.dispose()
        name_map = {r[0]: r[1] for r in rows}
        return {"code": 0, "data": name_map}
    except Exception as e:
        logger.error("stock-names query failed: {}", e)
        return {"code": 1, "message": str(e), "data": None}

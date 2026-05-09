"""回测 API — /api/v2/backtest

引擎架构: Frontend → API → EngineRegistry.get(engine) → IBacktestEngine.run()
"""
import asyncio
import math
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
from app.backtest.engine.data_provider import ClickHouseDataProvider
from app.db.sqlite import async_session_factory
from app.models.factor import FactorConfig, BtConfig
from app.services.factor_backtest import factor_backtest_service

router = APIRouter(prefix="/v2/backtest")

_tasks: dict[str, dict] = {}


def _sanitize_json(obj: object) -> object:
    """递归清理所有 NaN/Inf/Timestamp 为 JSON 安全值"""
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


class RunBacktestRequest(BaseModel):
    mode: str = "vectorized"
    engine: str = "builtin"  # "builtin" | "akquant"
    factor_expression: str | None = None
    buy_condition: str | None = None
    sell_condition: str | None = None
    symbols: list[str]
    start_date: str
    end_date: str
    initial_capital: float = 1_000_000
    rebalance_freq: str = "monthly"
    n_groups: int = 5
    bar_type: str = "daily"
    commission_rate: float = 0.0003
    slippage: float = 0.001
    strategy_params: dict | None = None
    strategy_id: int | None = None
    strategy_name: str | None = None
    strategy_code: str | None = None  # akquant 策略代码
    benchmark_symbol: str | None = None  # 基准指数


async def _save_backtest_result(
    task_id: str,
    config: BacktestConfig,
    result_dict: dict,
    success: bool,
):
    """Persist backtest result to database."""
    try:
        from app.db.models.strategy import Strategy, Backtest
        from sqlalchemy import select

        async with async_session_factory() as session:
            code = config.strategy_code or config.factor_expression or config.buy_condition or ""

            stmt = select(Strategy).where(Strategy.code == code).limit(1)
            result = await session.execute(stmt)
            strategy = result.scalars().first()

            if strategy is None:
                strategy = Strategy(
                    name=f"回测-{task_id}",
                    code=code,
                    description=f"自动创建（{config.engine}回测）",
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
    """提交回测任务（异步执行，立即返回 task_id）"""
    from datetime import date as date_cls

    task_id = str(uuid.uuid4())[:8]

    # 验证引擎是否存在
    try:
        EngineRegistry.get(req.engine)
    except ValueError:
        return {"code": 1, "message": f"Unknown engine: {req.engine}", "data": None}

    config = BacktestConfig(
        mode=req.mode,
        factor_expression=req.factor_expression,
        buy_condition=req.buy_condition,
        sell_condition=req.sell_condition,
        symbols=req.symbols,
        start_date=date_cls.fromisoformat(req.start_date),
        end_date=date_cls.fromisoformat(req.end_date),
        initial_capital=req.initial_capital,
        rebalance_freq=req.rebalance_freq,
        n_groups=req.n_groups,
        bar_type=req.bar_type,
        commission_rate=req.commission_rate,
        slippage=req.slippage,
        strategy_params=req.strategy_params,
        engine=req.engine,
        benchmark_symbol=req.benchmark_symbol,
        strategy_code=req.strategy_code,
    )
    # 缓存 task_id 供报告使用
    config._task_id = task_id

    task_store: dict = {"status": "queued", "progress": 0, "result": None, "live": None}
    _tasks[task_id] = task_store

    async def _run():
        try:
            task_store["status"] = "running"
            data_provider = ClickHouseDataProvider()
            engine_class = EngineRegistry.get(config.engine)
            engine = engine_class()

            def on_progress(pct: float, live: dict | None):
                task_store["progress"] = pct
                if live:
                    task_store["live"] = live

            result = await engine.run(config, data_provider, progress_callback=on_progress)
            result_dict = _sanitize_json(result.to_dict())
            task_store["status"] = "done"
            task_store["progress"] = 1.0
            task_store["result"] = result_dict
            task_store["live"] = task_store.get("live")
            await _save_backtest_result(task_id, config, result_dict, success=True)
        except Exception as e:
            logger.error("Backtest task {} failed: {} ({})", task_id, e, type(e).__name__)
            task_store["status"] = "failed"
            task_store["progress"] = 1.0
            task_store["result"] = _sanitize_json({"error": f"{type(e).__name__}: {e}"})
            await _save_backtest_result(task_id, config, task_store["result"], success=False)

    asyncio.create_task(_run())
    return {"code": 0, "message": "success", "data": {"task_id": task_id}}


@router.get("/status/{task_id}")
async def get_status(task_id: str):
    """查询回测进度"""
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
    """获取回测结果"""
    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}
    if task["status"] not in ("done", "failed"):
        return {"code": 1, "message": f"Task status: {task['status']}", "data": None}
    return {"code": 0, "message": "success", "data": task["result"]}


@router.get("/engines")
async def list_engines():
    """列出所有可用的回测引擎"""
    engines = EngineRegistry.list_all()
    return {"code": 0, "message": "success", "data": engines}


@router.get("/report/{task_id}")
async def get_report(task_id: str):
    """获取 quantstats HTML 报告"""
    from app.backtest.engine.akquant.reporter import get_report_path, serve_report

    task = _tasks.get(task_id)
    if task is None:
        return {"code": 1, "message": "Task not found", "data": None}

    # 先检查已有文件
    html = serve_report(task_id)
    if html:
        from fastapi.responses import HTMLResponse
        return HTMLResponse(content=html)

    # 检查结果中是否有 report_path
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


# ── Stock pools ──
_POOL_CACHE: dict[str, list[str]] = {}


@router.get("/pools/{pool_name}")
async def get_pool_symbols(pool_name: str):
    """获取预定义股票池 — top100/top300/top500 by 近一年日均成交额"""
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
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()

        rows = ch.execute(
            """
            SELECT symbol, avg(amount) as avg_amount
            FROM klines_daily
            WHERE trade_date >= %(start)s AND trade_date <= %(end)s
              AND amount > 0 AND close > 0
            GROUP BY symbol
            ORDER BY avg_amount DESC
            LIMIT %(limit)s
            """,
            {
                "start": (date.today() - timedelta(days=365)).isoformat(),
                "end": date.today().isoformat(),
                "limit": limit,
            },
        )

        symbols = [row[0] for row in rows]
        _POOL_CACHE[pool_name] = symbols
        logger.info("Pool {}: {} symbols loaded", pool_name, len(symbols))
        return {"code": 0, "data": {"symbols": symbols}}
    except Exception as e:
        logger.error("Pool {} query failed: {}", pool_name, e)
        return {"code": 1, "message": f"Query failed: {e}", "data": None}


# ── Stock names ──

@router.get("/stock-names")
async def get_stock_names(symbols: str = ""):
    """获取股票中文名映射 — ?symbols=000001.SZ,600000.SH"""
    if not symbols.strip():
        return {"code": 0, "data": {}}
    try:
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()
        sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
        if not sym_list:
            return {"code": 0, "data": {}}
        rows = ch.execute(
            "SELECT symbol, name FROM stock_info WHERE symbol IN %(syms)s",
            {"syms": sym_list},
        )
        name_map = {r[0]: r[1] for r in rows}
        return {"code": 0, "data": name_map}
    except Exception as e:
        logger.error("stock-names query failed: {}", e)
        return {"code": 1, "message": str(e), "data": None}

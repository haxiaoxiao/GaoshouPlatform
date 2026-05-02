"""回测 API — /api/v2/backtest"""
import uuid

from fastapi import APIRouter
from loguru import logger
from pydantic import BaseModel, Field

from app.backtest.config import BacktestConfig
from app.backtest.runner import get_backtest_runner
from app.models.factor import FactorConfig, BtConfig
from app.services.factor_backtest import factor_backtest_service

router = APIRouter(prefix="/v2/backtest")

_tasks: dict[str, dict] = {}


class RunBacktestRequest(BaseModel):
    mode: str = "vectorized"
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


@router.post("/run")
async def run_backtest(req: RunBacktestRequest):
    """提交回测任务"""
    from datetime import date

    task_id = str(uuid.uuid4())[:8]
    config = BacktestConfig(
        mode=req.mode,
        factor_expression=req.factor_expression,
        buy_condition=req.buy_condition,
        sell_condition=req.sell_condition,
        symbols=req.symbols,
        start_date=date.fromisoformat(req.start_date),
        end_date=date.fromisoformat(req.end_date),
        initial_capital=req.initial_capital,
        rebalance_freq=req.rebalance_freq,
        n_groups=req.n_groups,
        bar_type=req.bar_type,
        commission_rate=req.commission_rate,
        slippage=req.slippage,
        strategy_params=req.strategy_params,
    )

    task_store = {"status": "queued", "progress": 0, "result": None, "live": None}
    _tasks[task_id] = task_store

    try:
        _tasks[task_id]["status"] = "running"
        runner = get_backtest_runner()
        result = await runner.run(config, task_store=task_store)
        _tasks[task_id] = {
            "status": "done",
            "progress": 1.0,
            "result": result.to_dict(),
            "live": task_store.get("live"),
        }
    except Exception as e:
        logger.error("Backtest task {} failed: {} ({})", task_id, e, type(e).__name__)
        _tasks[task_id] = {
            "status": "failed",
            "progress": 1.0,
            "result": {"error": f"{type(e).__name__}: {e}"},
            "live": task_store.get("live"),
        }

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


@router.post("/factor")
async def run_factor_backtest(config: FactorConfig, bt_config: BtConfig | None = None):
    """Run factor quantile-based layered backtest."""
    report = await factor_backtest_service.run(config, bt_config)
    return {"code": 0, "data": report.model_dump()}

"""内置回测引擎 — 包装现有 BacktestRunner"""
from __future__ import annotations

import asyncio
from typing import Any, Callable

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.engine import EngineRegistry
from app.backtest.engine.interface import IBacktestEngine, IDataProvider
from app.services.benchmark_series import attach_benchmark_result


@EngineRegistry.register
class BuiltinEngine(IBacktestEngine):
    """内置回测引擎，包装现有 BacktestRunner（向量化 + 事件驱动）"""

    name = "builtin"
    label = "内置引擎"
    supported_modes = ["vectorized", "event_driven"]

    async def run(
        self,
        config: BacktestConfig,
        data_provider: IDataProvider,
        progress_callback: Callable[[float, dict | None], None] | None = None,
    ) -> BacktestResult:
        from app.backtest.runner import BacktestRunner

        # 构建 task_store 以支持进度回调
        task_store: dict[str, Any] = {"status": "running", "progress": 0, "live": None}

        runner = BacktestRunner()
        runner_task = asyncio.create_task(runner.run(config, task_store=task_store))
        if progress_callback:
            last_progress = -1.0
            last_event_count = -1
            while not runner_task.done():
                await asyncio.sleep(0.8)
                progress = float(task_store.get("progress") or 0.0)
                live = task_store.get("live")
                event_count = len((live or {}).get("events") or [])
                if live and (progress != last_progress or event_count != last_event_count):
                    progress_callback(progress, live)
                    last_progress = progress
                    last_event_count = event_count
        result = await runner_task
        if config.benchmark_symbol and config.start_date and config.end_date:
            benchmark_returns = None
            benchmark_warning = None
            try:
                benchmark_returns = await data_provider.load_benchmark(
                    config.benchmark_symbol, config.start_date, config.end_date
                )
            except Exception as exc:
                benchmark_warning = f"Benchmark data load failed for {config.benchmark_symbol}: {exc}"
            attach_benchmark_result(
                result,
                benchmark_symbol=config.benchmark_symbol,
                benchmark_returns=benchmark_returns,
                warning=benchmark_warning,
            )

        if progress_callback:
            live = task_store.get("live")
            if isinstance(live, dict):
                live = {
                    **live,
                    "trades": result.trades[-120:],
                    "orders": result.orders[-120:],
                    "equity_curve": result.nav_series[-600:],
                    "metrics_snapshot": {
                        **(live.get("metrics_snapshot") or {}),
                        "total_return": result.total_return,
                        "max_drawdown": result.max_drawdown,
                        "sharpe": result.sharpe_ratio,
                        "cash": result.final_capital,
                        "total_value": result.final_capital,
                        "n_trades": result.total_trades,
                    },
                    "metadata": {
                        **(live.get("metadata") or {}),
                        "phase": "completed",
                        "progress_message": "内置回测完成",
                    },
                }
            progress_callback(1.0, live)

        return result

    def validate_config(self, config: BacktestConfig) -> list[str]:
        errors = []
        if config.mode == "vectorized" and not config.factor_expression:
            errors.append("向量化模式需要 factor_expression")
        if config.mode == "event_driven" and not config.strategy_code and not config.factor_expression and not config.buy_condition:
            errors.append("事件驱动模式需要 strategy_code / factor_expression 或 buy_condition")
        if not config.symbols:
            errors.append("symbols 不能为空")
        if config.start_date is None or config.end_date is None:
            errors.append("需要 start_date 和 end_date")
        return errors

"""内置回测引擎 — 包装现有 BacktestRunner"""
from __future__ import annotations

from typing import Any, Callable

from loguru import logger

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.engine import EngineRegistry
from app.backtest.engine.interface import IBacktestEngine, IDataProvider


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
        result = await runner.run(config, task_store=task_store)

        if progress_callback:
            progress_callback(1.0, task_store.get("live"))

        return result

    def validate_config(self, config: BacktestConfig) -> list[str]:
        errors = []
        if config.mode == "vectorized" and not config.factor_expression:
            errors.append("向量化模式需要 factor_expression")
        if config.mode == "event_driven" and not config.factor_expression and not config.buy_condition:
            errors.append("事件驱动模式需要 factor_expression 或 buy_condition")
        if not config.symbols:
            errors.append("symbols 不能为空")
        if config.start_date is None or config.end_date is None:
            errors.append("需要 start_date 和 end_date")
        return errors

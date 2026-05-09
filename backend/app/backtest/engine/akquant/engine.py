"""AkquantEngine — 实现 IBacktestEngine，接入 akquant 回测内核"""
from __future__ import annotations

import asyncio
import textwrap
from datetime import date
from typing import Any, Callable

from loguru import logger

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.engine import EngineRegistry
from app.backtest.engine.akquant import AKQUANT_AVAILABLE
from app.backtest.engine.interface import IBacktestEngine, IDataProvider


if AKQUANT_AVAILABLE:
    import akquant as aq
    from akquant.config import BacktestConfig as AQBacktestConfig
    from akquant.config import StrategyConfig, InstrumentConfig, RiskConfig

from app.backtest.engine.akquant.adapter import ClickHouseFeedAdapter
from app.backtest.engine.akquant.normalizer import normalize_result
from app.backtest.engine.akquant.reporter import generate_report


@EngineRegistry.register
class AkquantEngine(IBacktestEngine):
    """AKQuant 回测引擎"""

    name = "akquant"
    label = "AKQuant"
    supported_modes = ["event_driven"]

    async def run(
        self,
        config: BacktestConfig,
        data_provider: IDataProvider,
        progress_callback: Callable[[float, dict | None], None] | None = None,
    ) -> BacktestResult:
        if not AKQUANT_AVAILABLE:
            raise RuntimeError(
                "akquant is not installed. Run: pip install akquant"
            )

        start_date = config.start_date or date(2020, 1, 1)
        end_date = config.end_date or date(2025, 12, 31)

        # ── 1. 预加载数据 ──
        adapter = ClickHouseFeedAdapter(
            data_provider, config.symbols, start_date, end_date
        )
        await adapter.preload()

        if not adapter._cache:
            logger.warning("AkquantEngine: no data loaded, returning empty result")
            return BacktestResult(initial_capital=config.initial_capital)

        # ── 2. 构建策略 ──
        strategy_code = config.strategy_code or config.factor_expression or ""
        strategy = _build_strategy(strategy_code, config)

        # ── 3. 加载基准数据 ──
        benchmark_returns = None
        if config.benchmark_symbol:
            benchmark_returns = await data_provider.load_benchmark(
                config.benchmark_symbol, start_date, end_date
            )

        # ── 4. 构建 akquant 配置 ──
        aq_config = AQBacktestConfig(
            strategy_config=StrategyConfig(
                initial_cash=config.initial_capital,
                commission_rate=config.commission_rate,
                slippage=config.slippage,
            ),
            start_time=str(start_date),
            end_time=str(end_date),
            instruments=config.symbols,
            benchmark=config.benchmark_symbol,
            show_progress=False,
        )

        # ── 5. 在独立线程中运行 akquant（同步阻塞） ──
        def _run_sync():
            return aq.run_backtest(
                data=adapter,
                strategy=strategy,
                symbols=config.symbols,
                config=aq_config,
            )

        loop = asyncio.get_running_loop()
        raw_result = await loop.run_in_executor(None, _run_sync)

        # ── 6. 归一化结果 ──
        result = normalize_result(
            raw_result,
            start_date=start_date,
            end_date=end_date,
            initial_capital=config.initial_capital,
        )

        # ── 7. 生成报告（异步不阻塞） ──
        # report path stored, frontend fetches separately
        try:
            task_id = getattr(config, "_task_id", None)
            if task_id:
                asyncio.create_task(
                    asyncio.to_thread(
                        generate_report, task_id, raw_result, benchmark_returns
                    )
                )
        except Exception:
            pass

        if progress_callback:
            progress_callback(1.0, None)

        return result

    def validate_config(self, config: BacktestConfig) -> list[str]:
        errors: list[str] = []
        if not config.symbols:
            errors.append("symbols 不能为空")
        if config.start_date is None or config.end_date is None:
            errors.append("需要 start_date 和 end_date")
        if not config.strategy_code and not config.factor_expression:
            errors.append("需要 strategy_code 或 factor_expression")
        return errors


def _build_strategy(code: str, config: BacktestConfig) -> Any:
    """从用户代码构建 akquant 策略

    支持两种模式:
    1. 用户编写完整 Strategy 子类 → 动态加载
    2. 因子表达式 → 生成简单 FunctionalStrategy
    """
    if not code or not code.strip():
        return _default_strategy()

    code = code.strip()

    # 判断是否是完整策略代码（包含 class ... (aq.Strategy)）
    if "Strategy" in code and "def on_bar" in code:
        return _load_strategy_class(code)
    elif "class " in code and "aq.Strategy" in code:
        return _load_strategy_class(code)
    else:
        # 视为表达式，生成信号策略
        return _build_expression_strategy(code, config)


def _load_strategy_class(code: str) -> Any:
    """动态加载用户编写的 akquant Strategy 子类"""
    import numpy as np
    import pandas as pd
    import akquant as aq

    namespace = {"aq": aq, "np": np, "pd": pd}
    exec(code, namespace)

    candidates = []
    for obj in namespace.values():
        if (
            isinstance(obj, type)
            and issubclass(obj, aq.Strategy)
            and obj is not aq.Strategy
        ):
            candidates.append(obj)

    if not candidates:
        raise ValueError(
            "No akquant.Strategy subclass found in code. "
            "Define a class like: class MyStrategy(aq.Strategy): def on_bar(self, bar): ..."
        )

    strategy_class = candidates[0]
    logger.info("Loaded strategy class: {}", strategy_class.__name__)
    return strategy_class


def _build_expression_strategy(expression: str, config: BacktestConfig) -> Any:
    """将因子表达式转为 akquant FunctionalStrategy"""
    import akquant as aq

    def on_bar_fn(strategy, bar):
        pos = strategy.get_position(bar.symbol)

        # 用 akquant 的 get_history 和 numpy 近似计算信号
        closes = strategy.get_history(20, bar.symbol, "close")
        if len(closes) < 20:
            return

        try:
            close = float(bar.close)
            ma5 = float(closes[-5:].mean()) if len(closes) >= 5 else close
            # 简化信号: 当前价格 vs 5日均线
            signal = (close - ma5) / ma5
        except (ValueError, ZeroDivisionError):
            signal = 0.0

        if signal > 0.01 and pos == 0:
            strategy.buy(bar.symbol, 100)
        elif signal < -0.01 and pos > 0:
            strategy.close_position(bar.symbol)

    return aq.FunctionalStrategy(on_bar=on_bar_fn)


def _default_strategy() -> Any:
    """默认策略模板"""
    import akquant as aq

    class DefaultStrategy(aq.Strategy):
        def on_bar(self, bar):
            pass

    return DefaultStrategy

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

    三种模式:
    1. akquant Strategy 子类 → exec 加载
    2. 因子表达式 → akquant FunctionalStrategy
    3. 空/RQAlpha 代码 → 报错提示
    """
    if not code or not code.strip():
        raise ValueError(
            "策略代码为空。请编写 akquant Strategy 子类，例如:\n"
            "class MyStrategy(aq.Strategy):\n"
            "    def on_bar(self, bar):\n"
            "        if bar.close > bar.open:\n"
            "            self.buy(bar.symbol, 100)"
        )

    code = code.strip()

    # 检测 RQAlpha 旧语法 → 直接报错
    if "def handle_bar" in code or "def init(context)" in code:
        raise ValueError(
            "检测到 RQAlpha 语法 (def handle_bar / def init)。"
            "AKQuant 引擎使用 Strategy 类语法:\n"
            "class MyStrategy(aq.Strategy):\n"
            "    def on_bar(self, bar): ..."
        )

    # 判断是否是 akquant Strategy 子类
    if ("class " in code and "aq.Strategy" in code) or \
       ("Strategy" in code and "def on_bar" in code):
        return _load_strategy_class(code)

    # 因子表达式 → akquant FunctionalStrategy
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
    """将因子表达式转为 akquant FunctionalStrategy

    表达式在每根 bar 上求值：signal > 0 买入，signal < 0 卖出。
    可用变量: close, open, high, low, volume（当前 bar 值）
    """
    from akquant.backtest.engine import FunctionalStrategy

    def on_bar_fn(strategy, bar):
        pos = strategy.get_position(bar.symbol)

        ctx = {
            "close": float(bar.close),
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "volume": float(bar.volume),
        }

        try:
            signal = float(eval(expression, {"__builtins__": {}}, ctx))
        except Exception:
            return

        if signal > 0 and pos == 0:
            strategy.buy(bar.symbol, 100)
        elif signal < 0 and pos > 0:
            strategy.close_position(bar.symbol)

    return FunctionalStrategy(lambda s: None, on_bar=on_bar_fn)

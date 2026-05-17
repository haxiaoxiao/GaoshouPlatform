"""AKQuant optimization service."""
from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from app.backtest.config import BacktestConfig
from app.backtest.engine.akquant import AKQUANT_AVAILABLE
from app.backtest.engine.akquant.engine import _load_strategy_class
from app.backtest.engine.data_provider import StoreDataProvider
from app.services.timer_minute_sync import timer_times_from_params, timer_times_to_strings


class AkquantOptimizeUnavailable(RuntimeError):
    """Raised when AKQuant optimization cannot run."""


async def run_grid_search(
    config: BacktestConfig,
    param_grid: dict[str, list[Any]],
    *,
    sort_by: str | list[str] = "sharpe_ratio",
    ascending: bool | list[bool] = False,
    max_workers: int = 1,
    timeout: float | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    """Run AKQuant Grid Search and return JSON-safe rows."""
    if not AKQUANT_AVAILABLE:
        raise AkquantOptimizeUnavailable("akquant is not installed")
    if not config.strategy_code:
        raise ValueError("strategy_code is required for AKQuant optimization")
    if not param_grid:
        raise ValueError("param_grid cannot be empty")

    return await asyncio.to_thread(
        _run_grid_search_sync,
        config,
        param_grid,
        sort_by,
        ascending,
        max_workers,
        timeout,
        db_path,
    )


def _run_grid_search_sync(
    config: BacktestConfig,
    param_grid: dict[str, list[Any]],
    sort_by: str | list[str],
    ascending: bool | list[bool],
    max_workers: int,
    timeout: float | None,
    db_path: str | None,
) -> list[dict[str, Any]]:
    import akquant as aq

    start = config.start_date or date(2020, 1, 1)
    end = config.end_date or date.today()
    strategy_cls = _load_strategy_class(config.strategy_code or "")
    data = _load_optimization_data(config, start, end)
    if not data:
        return []

    if db_path is None:
        db_path = str(
            Path(__file__).resolve().parent.parent
            / "reports"
            / "akquant_optimization.sqlite"
        )

    result = aq.run_grid_search(
        strategy_cls,
        param_grid,
        data=data,
        max_workers=max(1, int(max_workers or 1)),
        sort_by=sort_by,
        ascending=ascending,
        return_df=True,
        timeout=timeout,
        db_path=db_path,
        symbols=config.symbols,
        initial_cash=config.initial_capital,
        commission_rate=config.commission_rate,
        stamp_tax_rate=config.stamp_tax_rate,
        transfer_fee_rate=config.transfer_fee_rate,
        min_commission=config.min_commission,
        slippage=_build_slippage_policy(config.slippage),
        volume_limit_pct=config.volume_limit_pct,
        t_plus_one=config.t_plus_one,
        lot_size=config.lot_size,
        start_time=str(start),
        end_time=str(end),
        benchmark=config.benchmark_symbol,
    )
    if not isinstance(result, pd.DataFrame):
        return []
    return [_json_safe(row) for row in result.to_dict("records")]



async def run_walk_forward(
    config: BacktestConfig,
    param_grid: dict[str, list[Any]],
    *,
    train_period: int,
    test_period: int,
    metric: str | list[str] = "sharpe_ratio",
    ascending: bool | list[bool] = False,
    timeout: float | None = None,
) -> list[dict[str, Any]]:
    """Run AKQuant Walk-forward Validation and return JSON-safe rows."""
    if not AKQUANT_AVAILABLE:
        raise AkquantOptimizeUnavailable("akquant is not installed")
    if not config.strategy_code:
        raise ValueError("strategy_code is required for AKQuant walk-forward")
    if not param_grid:
        raise ValueError("param_grid cannot be empty")
    if train_period <= 0 or test_period <= 0:
        raise ValueError("train_period and test_period must be positive")

    return await asyncio.to_thread(
        _run_walk_forward_sync,
        config,
        param_grid,
        train_period,
        test_period,
        metric,
        ascending,
        timeout,
    )


def _run_walk_forward_sync(
    config: BacktestConfig,
    param_grid: dict[str, list[Any]],
    train_period: int,
    test_period: int,
    metric: str | list[str],
    ascending: bool | list[bool],
    timeout: float | None,
) -> list[dict[str, Any]]:
    import akquant as aq

    start = config.start_date or date(2020, 1, 1)
    end = config.end_date or date.today()
    strategy_cls = _load_strategy_class(config.strategy_code or "")
    data = _load_optimization_data(config, start, end)
    if not data:
        return []

    result = aq.run_walk_forward(
        strategy_cls,
        param_grid,
        data=data,
        train_period=int(train_period),
        test_period=int(test_period),
        metric=metric,
        ascending=ascending,
        initial_cash=config.initial_capital,
        warmup_period=0,
        timeout=timeout,
        symbols=config.symbols,
        commission_rate=config.commission_rate,
        stamp_tax_rate=config.stamp_tax_rate,
        transfer_fee_rate=config.transfer_fee_rate,
        min_commission=config.min_commission,
        slippage=_build_slippage_policy(config.slippage),
        volume_limit_pct=config.volume_limit_pct,
        t_plus_one=config.t_plus_one,
        lot_size=config.lot_size,
        start_time=str(start),
        end_time=str(end),
        benchmark=config.benchmark_symbol,
        risk_config=config.risk_config,
    )
    if not isinstance(result, pd.DataFrame):
        return []
    return [_json_safe(row) for row in result.to_dict("records")]

def _load_optimization_data(
    config: BacktestConfig,
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    provider = StoreDataProvider()

    async def _load() -> pd.DataFrame:
        if config.bar_type in {"minute", "minute_timer"}:
            timer_times = None
            if config.bar_type == "minute_timer":
                timer_times = timer_times_to_strings(
                    timer_times_from_params(config.strategy_params)
                )
            return await provider.load_minute(
                config.symbols,
                start_date,
                end_date,
                timer_times=timer_times,
            )
        return await provider.load_daily(config.symbols, start_date, end_date)

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        df = loop.run_until_complete(_load())
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    if df.empty:
        return {}

    data: dict[str, pd.DataFrame] = {}
    for symbol, group in df.reset_index().groupby("symbol"):
        time_col = "datetime" if "datetime" in group.columns else "trade_date"
        frame = group.drop(columns=["symbol"], errors="ignore").copy()
        if time_col in frame.columns:
            frame[time_col] = pd.to_datetime(frame[time_col])
            frame = frame.set_index(time_col)
        frame = frame.sort_index()
        data[str(symbol)] = frame
    return data


def _json_safe(row: dict[str, Any]) -> dict[str, Any]:
    import math
    import numpy as np

    clean: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (np.integer,)):
            clean[key] = int(value)
        elif isinstance(value, (np.floating, float)):
            v = float(value)
            clean[key] = None if math.isnan(v) or math.isinf(v) else v
        elif hasattr(value, "isoformat"):
            clean[key] = value.isoformat()
        else:
            clean[key] = value
    return clean


def _build_slippage_policy(slippage: Any) -> Any:
    if isinstance(slippage, (int, float)):
        return {"type": "percent", "value": float(slippage)}
    return slippage

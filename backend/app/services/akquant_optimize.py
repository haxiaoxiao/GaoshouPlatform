"""AKQuant optimization service."""
from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import sys
import time
from datetime import date
from itertools import product
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from loguru import logger

from app.backtest.config import BacktestConfig
from app.backtest.engine.akquant import AKQUANT_AVAILABLE
from app.backtest.engine.akquant.engine import _install_dynamic_strategy_pickle_hooks
from app.backtest.engine.data_provider import StoreDataProvider
from app.services.timer_minute_sync import timer_times_from_params, timer_times_to_strings

ProgressCallback = Callable[[str, str, float | None, dict[str, Any] | None], None]


class AkquantOptimizeUnavailableError(RuntimeError):
    """Raised when AKQuant optimization cannot run."""


def _load_importable_strategy_class(code: str) -> type:
    """Persist strategy code as an importable module for AKQuant multiprocessing."""
    import akquant as aq

    digest = hashlib.sha1(code.encode("utf-8")).hexdigest()[:12]
    module_name = f"app.reports.akquant_strategy_modules.strategy_{digest}"
    root = Path(__file__).resolve().parent.parent
    module_dir = root / "reports" / "akquant_strategy_modules"
    module_dir.mkdir(parents=True, exist_ok=True)
    (root / "reports" / "__init__.py").touch()
    (module_dir / "__init__.py").touch()
    module_path = module_dir / f"strategy_{digest}.py"
    module_path.write_text(code, encoding="utf-8")

    if module_name in sys.modules:
        module = sys.modules[module_name]
    else:
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Cannot import strategy module: {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

    candidates = [
        obj
        for obj in module.__dict__.values()
        if isinstance(obj, type) and issubclass(obj, aq.Strategy) and obj is not aq.Strategy
    ]
    if not candidates:
        raise ValueError("No akquant.Strategy subclass found in strategy code")
    strategy_cls = candidates[0]
    strategy_cls.__module__ = module_name
    _install_dynamic_strategy_pickle_hooks(strategy_cls)
    logger.info("Loaded importable AKQuant strategy class: {}.{}", module_name, strategy_cls.__name__)
    return strategy_cls


def _emit_progress(
    callback: ProgressCallback | None,
    phase: str,
    message: str,
    progress: float | None = None,
    meta: dict[str, Any] | None = None,
) -> None:
    logger.info("[AKQuant WFO] phase={} progress={} {}", phase, progress, message)
    if callback is not None:
        callback(phase, message, progress, meta)


def _param_combinations(param_grid: dict[str, list[Any]]) -> list[dict[str, Any]]:
    keys = list(param_grid.keys())
    values = [list(param_grid[key]) for key in keys]
    return [dict(zip(keys, combo, strict=False)) for combo in product(*values)]


def _param_preview(combinations: list[dict[str, Any]], limit: int = 20) -> str:
    shown = combinations[:limit]
    parts = [f"#{idx + 1}={params}" for idx, params in enumerate(shown)]
    if len(combinations) > limit:
        parts.append(f"... +{len(combinations) - limit} more")
    return "; ".join(parts)


def _estimate_window_count(total_len: int, train_period: int, test_period: int) -> int:
    if total_len < train_period + test_period:
        return 0
    return ((total_len - train_period - test_period) // test_period) + 1


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
        raise AkquantOptimizeUnavailableError("akquant is not installed")
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
    strategy_cls = _load_importable_strategy_class(config.strategy_code or "")
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
    result = _ensure_calmar_ratio(result)
    return [_json_safe(row) for row in result.to_dict("records")]



async def run_walk_forward(
    config: BacktestConfig,
    param_grid: dict[str, list[Any]],
    *,
    train_period: int,
    test_period: int,
    metric: str | list[str] = "sharpe_ratio",
    ascending: bool | list[bool] = False,
    max_workers: int = 1,
    timeout: float | None = None,
    progress_callback: ProgressCallback | None = None,
) -> list[dict[str, Any]]:
    """Run AKQuant Walk-forward Validation and return JSON-safe rows."""
    if not AKQUANT_AVAILABLE:
        raise AkquantOptimizeUnavailableError("akquant is not installed")
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
        max_workers,
        timeout,
        progress_callback,
    )


def _run_walk_forward_sync(
    config: BacktestConfig,
    param_grid: dict[str, list[Any]],
    train_period: int,
    test_period: int,
    metric: str | list[str],
    ascending: bool | list[bool],
    max_workers: int,
    timeout: float | None,
    progress_callback: ProgressCallback | None,
) -> list[dict[str, Any]]:
    import akquant as aq

    t0 = time.perf_counter()
    start = config.start_date or date(2020, 1, 1)
    end = config.end_date or date.today()
    combinations = _param_combinations(param_grid)
    param_count = len(combinations)
    worker_count = max(1, int(max_workers or 1))
    _emit_progress(
        progress_callback,
        "load_strategy",
        (
            f"Loading strategy class, symbols={len(config.symbols)}, "
            f"param_keys={list(param_grid.keys())}, param_combinations={param_count}, "
            f"train_period={train_period}, test_period={test_period}, "
            f"max_workers={worker_count}"
        ),
        0.17,
        {
            "param_count": param_count,
            "symbols": len(config.symbols),
            "max_workers": worker_count,
            "param_combinations_preview": combinations[:20],
        },
    )
    _emit_progress(
        progress_callback,
        "param_preview",
        f"Parameter combinations preview ({min(param_count, 20)}/{param_count}): {_param_preview(combinations)}",
        0.18,
        {"param_combinations_preview": combinations[:20]},
    )
    strategy_cls = _load_importable_strategy_class(config.strategy_code or "")
    _emit_progress(
        progress_callback,
        "load_data",
        f"Loading optimization data {start}~{end}, bar_type={config.bar_type}",
        0.2,
        None,
    )
    data = _load_optimization_data(config, start, end, progress_callback=progress_callback)
    if not data:
        _emit_progress(progress_callback, "empty_data", "Optimization data is empty", 1.0, None)
        return []

    row_count = sum(len(frame) for frame in data.values())
    timeline_len = len(pd.Index(sorted(set().union(*[frame.index for frame in data.values()]))))
    window_count = _estimate_window_count(timeline_len, int(train_period), int(test_period))
    estimated_backtests = window_count * param_count + window_count
    _emit_progress(
        progress_callback,
        "wfo_plan",
        (
            f"WFO plan: timeline_bars={timeline_len}, windows={window_count}, "
            f"param_combinations={param_count}, estimated_backtests={estimated_backtests}, "
            f"max_workers={worker_count}"
        ),
        0.245,
        {
            "timeline_bars": timeline_len,
            "window_count": window_count,
            "estimated_backtests": estimated_backtests,
            "max_workers": worker_count,
        },
    )
    _emit_progress(
        progress_callback,
        "akquant_running",
        (
            f"Calling aq.run_walk_forward with symbols={len(data)}, rows={row_count}, "
            f"param_combinations={param_count}, windows={window_count}, "
            f"estimated_backtests={estimated_backtests}, max_workers={worker_count}"
        ),
        0.25,
        {
            "rows": row_count,
            "param_count": param_count,
            "window_count": window_count,
            "estimated_backtests": estimated_backtests,
            "max_workers": worker_count,
        },
    )
    result = aq.run_walk_forward(
        strategy_cls,
        param_grid,
        data=data,
        train_period=int(train_period),
        test_period=int(test_period),
        metric=metric,
        ascending=ascending,
        max_workers=worker_count,
        initial_cash=config.initial_capital,
        warmup_period=0,
        timeout=timeout,
        forward_worker_logs=True,
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
    elapsed = time.perf_counter() - t0
    _emit_progress(
        progress_callback,
        "akquant_returned",
        f"aq.run_walk_forward returned type={type(result).__name__}, elapsed={elapsed:.1f}s",
        0.86,
        {"elapsed_seconds": int(elapsed)},
    )
    if not isinstance(result, pd.DataFrame):
        _emit_progress(progress_callback, "unexpected_result", "AKQuant returned non-DataFrame result", 0.88, None)
        return []
    result = _ensure_calmar_ratio(result)
    _emit_progress(
        progress_callback,
        "normalize_result",
        f"Walk-forward result rows={len(result)}, columns={list(result.columns)}",
        0.88,
        {"result_rows": len(result)},
    )
    return [_json_safe(row) for row in result.to_dict("records")]


def _ensure_calmar_ratio(df: pd.DataFrame) -> pd.DataFrame:
    """Fill calmar_ratio when AKQuant returns annual_return and max_drawdown only."""
    if "calmar_ratio" in df.columns:
        return df
    if "calmar" in df.columns:
        df = df.copy()
        df["calmar_ratio"] = df["calmar"]
        return df
    annual_col = "annual_return" if "annual_return" in df.columns else "annual_return_pct"
    drawdown_col = "max_drawdown" if "max_drawdown" in df.columns else "max_drawdown_pct"
    if annual_col not in df.columns or drawdown_col not in df.columns:
        return df
    df = df.copy()
    annual = pd.to_numeric(df[annual_col], errors="coerce")
    drawdown = pd.to_numeric(df[drawdown_col], errors="coerce").abs()
    drawdown = drawdown.where(drawdown > 0)
    df["calmar_ratio"] = (annual / drawdown).fillna(0.0)
    return df

def _load_optimization_data(
    config: BacktestConfig,
    start_date: date,
    end_date: date,
    *,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, pd.DataFrame]:
    provider = StoreDataProvider()

    def _timer_times_for_config() -> tuple[str, ...]:
        if config.timer_times:
            return timer_times_to_strings(
                timer_times_from_params({"timer_times": config.timer_times})
            )
        return timer_times_to_strings(timer_times_from_params(config.strategy_params))

    async def _load() -> pd.DataFrame:
        if config.bar_type in {"minute", "minute_timer"}:
            timer_times = None
            if config.bar_type == "minute_timer":
                timer_times = _timer_times_for_config()
            _emit_progress(
                progress_callback,
                "data_provider",
                f"provider.load_minute start symbols={len(config.symbols)}, timer_times={timer_times}",
                0.22,
                {"timer_times": list(timer_times or [])},
            )
            return await provider.load_minute(
                config.symbols,
                start_date,
                end_date,
                timer_times=timer_times,
            )
        _emit_progress(
            progress_callback,
            "data_provider",
            f"provider.load_daily start symbols={len(config.symbols)}",
            0.22,
            None,
        )
        return await provider.load_daily(config.symbols, start_date, end_date)

    t0 = time.perf_counter()
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        df = loop.run_until_complete(_load())
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    if df.empty:
        _emit_progress(
            progress_callback,
            "data_loaded",
            f"provider returned empty dataframe in {time.perf_counter() - t0:.1f}s",
            0.24,
            None,
        )
        return {}

    _emit_progress(
        progress_callback,
        "data_loaded",
        (
            f"provider returned rows={len(df)}, columns={list(df.columns)}, "
            f"elapsed={time.perf_counter() - t0:.1f}s"
        ),
        0.24,
        {"rows": len(df), "columns": list(df.columns)},
    )
    data: dict[str, pd.DataFrame] = {}
    for symbol, group in df.reset_index().groupby("symbol"):
        time_col = "datetime" if "datetime" in group.columns else "trade_date"
        frame = group.drop(columns=["symbol"], errors="ignore").copy()
        if time_col in frame.columns:
            frame[time_col] = pd.to_datetime(frame[time_col])
            frame = frame.set_index(time_col)
        frame = frame.sort_index()
        data[str(symbol)] = frame
    _emit_progress(
        progress_callback,
        "data_prepared",
        f"prepared AKQuant data dict symbols={len(data)}, rows={sum(len(v) for v in data.values())}",
        0.25,
        {"symbols": len(data), "rows": sum(len(v) for v in data.values())},
    )
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

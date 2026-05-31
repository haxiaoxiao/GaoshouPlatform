"""Helpers for benchmark NAV and excess-return series."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from app.backtest.config import BacktestResult
from app.services.index_catalog import get_index_item


DEFAULT_BENCHMARK_SYMBOL = "000300.SH"


def benchmark_display_name(symbol: str | None) -> str | None:
    if not symbol:
        return None
    item = get_index_item(symbol)
    return item.display_name if item else str(symbol)


def resolve_benchmark_symbol(symbol_or_alias: str | None, *, fallback: str = DEFAULT_BENCHMARK_SYMBOL) -> str:
    item = get_index_item(symbol_or_alias)
    if item and item.benchmark_enabled:
        return item.symbol
    text = str(symbol_or_alias or "").strip().upper()
    return text or fallback


def returns_to_nav_series(returns: pd.Series | None) -> pd.Series:
    if returns is None or returns.empty:
        return pd.Series(dtype=float)
    series = pd.Series(returns, dtype=float).replace([float("inf"), float("-inf")], pd.NA).fillna(0.0)
    series.index = pd.to_datetime(series.index).normalize()
    series = series.sort_index()
    return (1.0 + series).cumprod()


def nav_points(nav: pd.Series, *, value_key: str = "nav") -> list[dict[str, Any]]:
    if nav.empty:
        return []
    return [
        {"date": _date_text(idx), value_key: round(float(value), 6)}
        for idx, value in nav.items()
        if pd.notna(value)
    ]


def return_points(returns: pd.Series | None) -> list[dict[str, Any]]:
    if returns is None or returns.empty:
        return []
    series = pd.Series(returns, dtype=float)
    series.index = pd.to_datetime(series.index).normalize()
    return [
        {"date": _date_text(idx), "return": round(float(value), 6)}
        for idx, value in series.sort_index().items()
        if pd.notna(value)
    ]


def excess_nav_points(
    strategy_points: list[dict[str, Any]],
    benchmark_nav: pd.Series,
    *,
    strategy_key: str = "nav",
    value_key: str = "nav",
) -> list[dict[str, Any]]:
    if not strategy_points or benchmark_nav.empty:
        return []
    bench_by_date = {_date_text(idx): float(value) for idx, value in benchmark_nav.items() if pd.notna(value)}
    common: list[tuple[str, float, float]] = []
    for item in strategy_points:
        date_text = str(item.get("date") or "")
        if date_text not in bench_by_date:
            continue
        try:
            strategy_value = float(item.get(strategy_key) or 0.0)
            bench_value = float(bench_by_date[date_text])
        except (TypeError, ValueError):
            continue
        if strategy_value > 0 and bench_value > 0:
            common.append((date_text, strategy_value, bench_value))
    if not common:
        return []
    strategy_base = common[0][1]
    bench_base = common[0][2]
    if strategy_base <= 0 or bench_base <= 0:
        return []
    return [
        {
            "date": date_text,
            value_key: round((strategy_value / strategy_base) / (bench_value / bench_base), 6),
        }
        for date_text, strategy_value, bench_value in common
    ]


def attach_benchmark_result(
    result: BacktestResult,
    *,
    benchmark_symbol: str | None,
    benchmark_returns: pd.Series | None,
    warning: str | None = None,
) -> BacktestResult:
    symbol = resolve_benchmark_symbol(benchmark_symbol)
    result.benchmark_symbol = symbol
    result.benchmark_name = benchmark_display_name(symbol)
    if warning:
        result.warnings.append(warning)
        return result
    if benchmark_returns is None or benchmark_returns.empty:
        result.warnings.append(f"Benchmark data is unavailable for {symbol}; comparison series is omitted.")
        return result

    nav = returns_to_nav_series(benchmark_returns)
    result.benchmark_nav_series = nav_points(nav)
    result.benchmark_daily_returns = return_points(benchmark_returns)
    result.excess_nav_series = excess_nav_points(result.nav_series, nav)
    return result


def benchmark_summary(symbol: str, returns: pd.Series | None) -> dict[str, Any]:
    resolved = resolve_benchmark_symbol(symbol)
    if returns is None or returns.empty:
        return {
            "symbol": resolved,
            "name": benchmark_display_name(resolved),
            "covered_days": 0,
            "min_date": None,
            "max_date": None,
            "ok": False,
            "warning": f"Benchmark data is unavailable for {resolved}.",
        }
    index = pd.to_datetime(returns.index).normalize()
    return {
        "symbol": resolved,
        "name": benchmark_display_name(resolved),
        "covered_days": int(len(returns)),
        "min_date": _date_text(index.min()),
        "max_date": _date_text(index.max()),
        "ok": True,
        "warning": None,
    }


def _date_text(value: Any) -> str:
    try:
        ts = pd.Timestamp(value)
        if not pd.isna(ts):
            return ts.strftime("%Y-%m-%d")
    except Exception:
        pass
    if isinstance(value, date):
        return value.isoformat()
    return str(value)

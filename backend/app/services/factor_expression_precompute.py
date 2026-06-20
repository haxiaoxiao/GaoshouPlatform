"""Batch precompute DSL/AKQuant factor expressions into factor_cache."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from app.compute.cache import get_compute_cache
from app.compute.expression import evaluate_expression
from app.compute.operators import auto_discover
from app.data_stores import get_market_data_store
from app.services.akquant_factor import evaluate_akquant_factor


async def precompute_factor_expressions(
    *,
    expressions: list[str],
    symbols: list[str],
    start_date: date,
    end_date: date,
    engine: str = "builtin",
) -> dict[str, Any]:
    """Compute expressions for a date range and persist each value to factor_cache."""
    if not expressions:
        raise ValueError("expressions cannot be empty")
    if not symbols:
        raise ValueError("symbols cannot be empty")
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")
    if engine not in {"builtin", "akquant"}:
        raise ValueError("engine must be builtin or akquant")

    auto_discover()
    cache = get_compute_cache()
    result: dict[str, Any] = {
        "engine": engine,
        "symbols": len(symbols),
        "expressions": [],
        "rows_written": 0,
    }
    if engine == "akquant":
        for expression in expressions:
            out = await evaluate_akquant_factor(expression, symbols, start_date, end_date)
            written = _persist_api_result(cache, expression, engine, out)
            result["rows_written"] += written
            result["expressions"].append({
                "expression": expression,
                "expr_hash": cache.make_key(expression),
                "rows_written": written,
            })
        return result

    store = get_market_data_store()
    df = store.load_daily(symbols, start_date, end_date)
    if df.empty:
        return result

    data = {}
    for sym, group in df.groupby("symbol"):
        frame = group.copy()
        if "trade_date" in frame.columns:
            frame["trade_date"] = pd.to_datetime(frame["trade_date"])
            frame = frame.set_index("trade_date")
        data[str(sym)] = frame.sort_index()

    for expression in expressions:
        values = evaluate_expression(expression, data)
        fallback_symbol = symbols[0] if len(symbols) == 1 else "result"
        written = _persist_expression_result(
            cache,
            expression,
            engine,
            values,
            start_date,
            end_date,
            fallback_symbol=fallback_symbol,
        )
        result["rows_written"] += written
        result["expressions"].append({
            "expression": expression,
            "expr_hash": cache.make_key(expression),
            "rows_written": written,
        })
    return result


def _persist_api_result(cache, expression: str, engine: str, out: dict[str, list[dict[str, Any]]]) -> int:
    by_date: dict[date, dict[str, float]] = {}
    for symbol, rows in out.items():
        for row in rows:
            value = row.get("value")
            if value is None:
                continue
            trade_date = date.fromisoformat(str(row["trade_date"])[:10])
            by_date.setdefault(trade_date, {})[str(symbol)] = float(value)

    written = 0
    expr_hash = cache.make_key(expression)
    for trade_date, values in by_date.items():
        series = pd.Series(values, dtype=float)
        cache.save_to_parquet(expr_hash, trade_date, series, expression=expression, engine=engine)
        written += int(series.notna().sum())
    return written


def _persist_expression_result(
    cache,
    expression: str,
    engine: str,
    values: Any,
    start_date: date,
    end_date: date,
    *,
    fallback_symbol: str = "result",
) -> int:
    expr_hash = cache.make_key(expression)
    by_date: dict[date, dict[str, float]] = {}

    if isinstance(values, dict):
        for symbol, series in values.items():
            if not isinstance(series, pd.Series):
                continue
            ser = series.copy()
            if not isinstance(ser.index, pd.DatetimeIndex):
                ser.index = pd.to_datetime(ser.index)
            for idx, value in ser.dropna().items():
                trade_date = idx.date()
                if start_date <= trade_date <= end_date:
                    by_date.setdefault(trade_date, {})[str(symbol)] = float(value)
    elif isinstance(values, pd.Series):
        ser = values.copy()
        if not isinstance(ser.index, pd.DatetimeIndex):
            ser.index = pd.to_datetime(ser.index)
        for idx, value in ser.dropna().items():
            trade_date = idx.date()
            if start_date <= trade_date <= end_date:
                by_date.setdefault(trade_date, {})[fallback_symbol] = float(value)

    written = 0
    for trade_date, row in by_date.items():
        series = pd.Series(row, dtype=float)
        cache.save_to_parquet(expr_hash, trade_date, series, expression=expression, engine=engine)
        written += int(series.notna().sum())
    return written

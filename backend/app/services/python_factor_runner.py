"""Python 因子本地可信执行器。

按照 2026-05-19 因子研究通用化文档，提供固定 compute(data, context) 接口的执行环境。
第一版在后端进程内执行，不做强沙箱（使用场景为个人本地研究）。
"""

from __future__ import annotations

import traceback
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import date, datetime, timedelta
from typing import Any, Sequence

import numpy as np
import pandas as pd

from app.data_stores import get_market_data_store

# 默认超时 120 秒（文档要求）
DEFAULT_TIMEOUT_SECONDS = 120.0


def run_python_factor(
    *,
    code: str,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    params: dict[str, Any] | None = None,
    stock_pool: str = "zz500",
    benchmark: str = "000905.SH",
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """在子进程中执行 Python 因子代码，返回标准化结果。

    Args:
        code: Python 代码，必须定义 compute(data, context) 函数。
        symbols: 股票代码列表。
        start_date: 开始日期。
        end_date: 结束日期。
        params: 用户参数。
        stock_pool: 股票池名称。
        benchmark: 基准代码。
        timeout_seconds: 超时秒数，默认 120。

    Returns:
        {"rows": [...], "errors": [...], "elapsed_seconds": float}
    """
    started = datetime.now()
    try:
        daily_bars = _load_daily_bars(symbols, start_date, end_date)
        trading_days = _resolve_trading_days(symbols, start_date, end_date)
        context = {
            "symbols": list(symbols),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "params": dict(params or {}),
            "stock_pool": stock_pool,
            "benchmark": benchmark,
            "trading_calendar": [d.isoformat() for d in trading_days],
        }
        data = {
            "daily": daily_bars,
            "stock_info": _load_stock_info(symbols),
            "financial": _load_financial(symbols),
            "factor_values": {},
        }
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_execute_python_code, code, data, context)
            result = future.result(timeout=timeout_seconds)
        rows, errors = _validate_and_normalize(result, context)
        return {
            "rows": rows,
            "errors": errors,
            "elapsed_seconds": round((datetime.now() - started).total_seconds(), 3),
        }
    except FutureTimeoutError:
        return {
            "rows": [],
            "errors": [f"执行超时（{timeout_seconds} 秒）"],
            "elapsed_seconds": round((datetime.now() - started).total_seconds(), 3),
        }
    except Exception:
        return {
            "rows": [],
            "errors": [traceback.format_exc()],
            "elapsed_seconds": round((datetime.now() - started).total_seconds(), 3),
        }


def _execute_python_code(
    code: str,
    data: dict[str, Any],
    context: dict[str, Any],
) -> Any:
    """在同步上下文中执行用户 Python 代码。

    此函数在 executor 线程中运行。
    """
    local_ns: dict[str, Any] = {}
    exec(code, {"pd": pd, "np": np, "__builtins__": __builtins__}, local_ns)

    compute_fn = local_ns.get("compute")
    if compute_fn is None:
        raise ValueError("代码中未定义 compute(data, context) 函数")

    if not callable(compute_fn):
        raise TypeError("compute 必须是可调用函数")

    return compute_fn(data, context)


def _validate_and_normalize(
    raw_result: Any,
    context: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    """校验返回值是否为 DataFrame 或 dict 列表，并标准化。"""
    errors: list[str] = []
    rows: list[dict[str, Any]] = []

    if raw_result is None:
        errors.append("compute() 返回了 None，期望 pd.DataFrame 或 list[dict]")
        return rows, errors

    # 尝试转为 DataFrame
    if isinstance(raw_result, pd.DataFrame):
        df = raw_result.copy()
    elif isinstance(raw_result, (list, tuple)):
        try:
            df = pd.DataFrame(raw_result)
        except Exception as e:
            errors.append(f"无法将返回值转换为 DataFrame: {e}")
            return rows, errors
    else:
        errors.append(f"compute() 返回了不支持的类型: {type(raw_result).__name__}，期望 pd.DataFrame 或 list[dict]")
        return rows, errors

    if df.empty:
        return rows, errors

    # 检查必需的列
    required_cols = {"symbol", "trade_date", "value"}
    missing = required_cols - set(df.columns)
    if missing:
        errors.append(f"返回值缺少必需列: {sorted(missing)}（需要 symbol, trade_date, value）")
        # 尝试修复：如果 trade_date 在 index 中
        if "trade_date" in missing and df.index.name == "trade_date":
            df = df.reset_index()
            missing = required_cols - set(df.columns)
        if missing:
            return rows, errors

    set(context.get("symbols", []))
    start = date.fromisoformat(context["start_date"])
    end = date.fromisoformat(context["end_date"])

    for _, row in df.iterrows():
        try:
            symbol = str(row["symbol"]).strip().upper()
            if not symbol or "." not in symbol:
                continue

            # 标准化 trade_date
            td_raw = row["trade_date"]
            if isinstance(td_raw, date):
                td = td_raw
            elif isinstance(td_raw, datetime):
                td = td_raw.date()
            elif isinstance(td_raw, pd.Timestamp):
                td = td_raw.date()
            else:
                td = date.fromisoformat(str(td_raw)[:10])

            # 过滤范围外的日期
            if td < start or td > end:
                continue

            # 标准化 value
            val_raw = row["value"]
            try:
                val = float(val_raw)
            except (ValueError, TypeError):
                continue
            if not np.isfinite(val):
                continue

            rows.append({
                "symbol": symbol,
                "trade_date": td.isoformat(),
                "value": val,
            })
        except Exception:
            continue

    return rows, errors


# ── 数据加载辅助函数 ──

def _load_daily_bars(
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """加载日线 OHLCV 数据。"""
    store = get_market_data_store()
    lookback = start_date - timedelta(days=370)
    return store.load_daily(
        list(symbols),
        lookback,
        end_date,
        columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
    )


def _resolve_trading_days(
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
) -> list[date]:
    """获取交易日历。"""
    store = get_market_data_store()
    return store.load_trading_dates(list(symbols), start_date, end_date)


def _load_stock_info(symbols: Sequence[str]) -> pd.DataFrame:
    """加载股票基础信息。"""
    import sqlite3
    from pathlib import Path

    from app.core.config import settings

    db_path = Path(settings.data_dir) / "gaoshou.db"
    if not db_path.exists():
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(
            f"SELECT symbol, name, industry, total_mv, circ_mv, pe_ttm, pb, roe FROM stocks WHERE symbol IN ({placeholders})",
            conn,
            params=list(symbols),
        )


def _load_financial(symbols: Sequence[str]) -> pd.DataFrame:
    """加载最近财务数据。"""
    import sqlite3
    from pathlib import Path

    from app.core.config import settings

    db_path = Path(settings.data_dir) / "gaoshou.db"
    if not db_path.exists():
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query(
            f"SELECT symbol, report_date, eps, bvps, roe, revenue, net_profit, revenue_yoy, profit_yoy, gross_margin FROM financial_data WHERE symbol IN ({placeholders}) ORDER BY symbol, report_date DESC",
            conn,
            params=list(symbols),
        )

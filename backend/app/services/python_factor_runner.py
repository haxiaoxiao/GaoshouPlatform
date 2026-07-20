"""Python 因子本地可信执行器。

按照 2026-05-19 因子研究通用化文档，提供固定 compute(data, context) 接口的执行环境。
用户计算在可终止的子进程中执行并限制常见文件/进程入口，但这不是操作系统级沙箱；
仅应运行本机受信任的研究代码。
"""

from __future__ import annotations

import ast
import asyncio
import multiprocessing
import traceback
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Any, Sequence

import numpy as np
import pandas as pd

from app.data_stores import get_market_data_store

# 默认超时 120 秒（文档要求）
DEFAULT_TIMEOUT_SECONDS = 120.0

_SAFE_BUILTINS: Mapping[str, Any] = {
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "filter": filter,
    "float": float,
    "int": int,
    "isinstance": isinstance,
    "len": len,
    "list": list,
    "map": map,
    "max": max,
    "min": min,
    "next": next,
    "range": range,
    "reversed": reversed,
    "round": round,
    "set": set,
    "slice": slice,
    "sorted": sorted,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
    "Exception": Exception,
    "KeyError": KeyError,
    "RuntimeError": RuntimeError,
    "TypeError": TypeError,
    "ValueError": ValueError,
}
_DISALLOWED_CALLS = {"__import__", "breakpoint", "compile", "eval", "exec", "input", "open"}
_DISALLOWED_ATTRIBUTE_PARTS = {
    "builtins",
    "ctypes",
    "ctypeslib",
    "importlib",
    "os",
    "pathlib",
    "pickle",
    "popen",
    "shutil",
    "socket",
    "spawn",
    "subprocess",
    "sys",
    "system",
    "tempfile",
}
_DISALLOWED_ATTRIBUTE_NAMES = {
    "dump",
    "dumps",
    "eval",
    "exec",
    "fromfile",
    "load",
    "loads",
    "memmap",
    "open_memmap",
    "read_pickle",
    "save",
    "savetxt",
    "savez",
    "savez_compressed",
    "to_clipboard",
    "to_csv",
    "to_excel",
    "to_feather",
    "to_hdf",
    "to_json",
    "to_parquet",
    "to_pickle",
    "to_sql",
    "to_stata",
    "tofile",
    "to_xml",
}


def _attribute_path(node: ast.Attribute) -> list[str]:
    parts = [node.attr]
    value: ast.expr = node.value
    while isinstance(value, ast.Attribute):
        parts.append(value.attr)
        value = value.value
    if isinstance(value, ast.Name):
        parts.append(value.id)
    return list(reversed(parts))


def validate_python_factor_source(code: str) -> str | None:
    """Return a policy error for source that should not enter the worker."""
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        return f"Python 语法错误: {exc}"

    has_compute = False
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return "Python 因子不允许 import；可直接使用预置的 pd 和 np"
        if isinstance(node, ast.FunctionDef) and node.name == "compute":
            has_compute = True
        if isinstance(node, ast.Attribute):
            attribute_path = _attribute_path(node)
            lowered = [part.casefold() for part in attribute_path]
            blocked_part = next(
                (
                    part
                    for part in lowered
                    if part.startswith("_")
                    or part in _DISALLOWED_ATTRIBUTE_PARTS
                    or part in _DISALLOWED_ATTRIBUTE_NAMES
                    or part.startswith("read_")
                ),
                None,
            )
            if blocked_part:
                return f"Python 因子不允许访问属性 {'.'.join(attribute_path)}"
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in _DISALLOWED_CALLS
        ):
            return f"Python 因子不允许调用 {node.func.id}()"
    if not has_compute:
        return "代码中未定义 compute(data, context) 函数"
    return None


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
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        source_error = validate_python_factor_source(code)
        if source_error:
            return {
                "rows": [],
                "errors": [source_error],
                "elapsed_seconds": round((datetime.now() - started).total_seconds(), 3),
            }
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
        rows, errors = _execute_in_subprocess(
            code=code,
            data=data,
            context=context,
            timeout_seconds=timeout_seconds,
        )
        return {
            "rows": rows,
            "errors": errors,
            "elapsed_seconds": round((datetime.now() - started).total_seconds(), 3),
        }
    except Exception:
        return {
            "rows": [],
            "errors": [traceback.format_exc()],
            "elapsed_seconds": round((datetime.now() - started).total_seconds(), 3),
        }


async def run_python_factor_async(**kwargs: Any) -> dict[str, Any]:
    """Run data loading and process orchestration without blocking the event loop."""
    return await asyncio.to_thread(run_python_factor, **kwargs)


def _execute_in_subprocess(
    *,
    code: str,
    data: dict[str, Any],
    context: dict[str, Any],
    timeout_seconds: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    process_context = multiprocessing.get_context("spawn")
    receiver, sender = process_context.Pipe(duplex=False)
    process = process_context.Process(
        target=_python_factor_worker,
        args=(sender, code, data, context),
        daemon=True,
        name="gaoshou-python-factor",
    )
    process.start()
    sender.close()
    try:
        if not receiver.poll(timeout_seconds):
            _stop_process(process)
            return [], [f"执行超时（{timeout_seconds} 秒）"]
        try:
            payload = receiver.recv()
        except EOFError:
            process.join(timeout=1)
            return [], [f"Python 因子子进程异常退出（exitcode={process.exitcode}）"]
        process.join(timeout=1)
        if process.is_alive():
            _stop_process(process)
        if not isinstance(payload, dict):
            return [], ["Python 因子子进程返回了无效结果"]
        return list(payload.get("rows") or []), list(payload.get("errors") or [])
    finally:
        receiver.close()
        if process.is_alive():
            _stop_process(process)


def _stop_process(process: multiprocessing.Process) -> None:
    process.terminate()
    process.join(timeout=1)
    if process.is_alive():
        process.kill()
        process.join(timeout=1)


def _python_factor_worker(
    sender: Any,
    code: str,
    data: dict[str, Any],
    context: dict[str, Any],
) -> None:
    try:
        raw_result = _execute_python_code(code, data, context)
        rows, errors = _validate_and_normalize(raw_result, context)
        sender.send({"rows": rows, "errors": errors})
    except BaseException:
        sender.send({"rows": [], "errors": [traceback.format_exc()]})
    finally:
        sender.close()


def _execute_python_code(
    code: str,
    data: dict[str, Any],
    context: dict[str, Any],
) -> Any:
    """Execute trusted local factor source inside the isolated worker."""
    namespace: dict[str, Any] = {
        "pd": pd,
        "np": np,
        "__builtins__": dict(_SAFE_BUILTINS),
    }
    exec(code, namespace, namespace)

    compute_fn = namespace.get("compute")
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

    allowed_symbols = {
        str(symbol).strip().upper()
        for symbol in context.get("symbols", [])
        if str(symbol).strip()
    }
    start = date.fromisoformat(context["start_date"])
    end = date.fromisoformat(context["end_date"])

    for _, row in df.iterrows():
        try:
            symbol = str(row["symbol"]).strip().upper()
            if not symbol or "." not in symbol or symbol not in allowed_symbols:
                continue

            # 标准化 trade_date
            td_raw = row["trade_date"]
            if isinstance(td_raw, pd.Timestamp):
                td = td_raw.date()
            elif isinstance(td_raw, datetime):
                td = td_raw.date()
            elif isinstance(td_raw, date):
                td = td_raw
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
    if not symbols or not db_path.exists():
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
    if not symbols or not db_path.exists():
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    with sqlite3.connect(db_path) as conn:
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(financial_data)").fetchall()
        }
        ann_date_expr = "ann_date" if "ann_date" in columns else "NULL AS ann_date"
        return pd.read_sql_query(
            f"SELECT symbol, report_date, {ann_date_expr}, eps, bvps, roe, revenue, net_profit, revenue_yoy, profit_yoy, gross_margin FROM financial_data WHERE symbol IN ({placeholders}) ORDER BY symbol, report_date DESC",
            conn,
            params=list(symbols),
        )

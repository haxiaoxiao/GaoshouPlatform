"""akquant BacktestResult → 平台 BacktestResult 归一化"""
from __future__ import annotations

import math
from datetime import date
from typing import Any

import pandas as pd
from loguru import logger

from app.backtest.config import BacktestResult
from app.backtest.engine.akquant import AKQUANT_AVAILABLE

if AKQUANT_AVAILABLE:
    import akquant as aq


def _safe_float(val: Any, default: float = 0.0) -> float:
    """安全转 float，处理 NaN/None"""
    try:
        v = float(val)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except (TypeError, ValueError):
        return default


def _to_json_safe(obj: Any) -> Any:
    """递归将值转为 JSON 可序列化格式（处理 Timestamp/numpy 类型）"""
    import numpy as np

    if isinstance(obj, (pd.Timestamp, np.datetime64)):
        ts = pd.Timestamp(obj)
        return ts.strftime("%Y-%m-%d") if ts.hour == 0 and ts.minute == 0 else ts.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return 0.0 if math.isnan(v) or math.isinf(v) else v
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {str(k): _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_safe(v) for v in obj]
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "strftime"):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    return obj
    """安全转 float，处理 NaN/None"""
    try:
        v = float(val)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except (TypeError, ValueError):
        return default


def normalize_result(
    raw_result: Any,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    initial_capital: float = 1_000_000,
) -> BacktestResult:
    """将 akquant BacktestResult 转为平台统一格式"""

    metrics = raw_result.metrics

    # 净值曲线
    eq = raw_result.equity_curve
    if isinstance(eq, pd.Series) and not eq.empty:
        nav_series = [
            {"date": idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx),
             "nav": round(val / initial_capital, 6)}
            for idx, val in eq.items()
        ]
    else:
        nav_series = []

    # 日收益率
    dr = raw_result.daily_returns
    if isinstance(dr, pd.Series) and not dr.empty:
        daily_returns = [
            {"date": idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx),
             "return": round(val, 6)}
            for idx, val in dr.items()
        ]
    else:
        daily_returns = []

    # 交易记录
    trades: list[dict[str, Any]] = []
    try:
        trades_df = raw_result.trades_df
        if not trades_df.empty:
            raw_trades = trades_df.to_dict("records")
            for t in raw_trades:
                for old_key, new_key in [
                    ("entry_time", "entry_date"), ("exit_time", "date"),
                    ("entry_price", "price"), ("net_pnl", "pnl"),
                ]:
                    if old_key in t and new_key not in t:
                        t[new_key] = t[old_key]
            trades = [_to_json_safe(t) for t in raw_trades]
    except Exception as e:
        logger.debug("Failed to parse trades: {}", e)

    # 订单记录
    orders: list[dict[str, Any]] = []
    try:
        orders_df = raw_result.orders_df
        if not orders_df.empty:
            orders = [_to_json_safe(o) for o in orders_df.to_dict("records")]
    except Exception:
        pass

    # 统计交易指标
    total_trades = len(trades)
    win_count = sum(1 for t in trades if t.get("pnl", 0) > 0)
    loss_count = total_trades - win_count

    # 指标 — akquant 内部为百分数，/100 转小数
    total_return = _safe_float(getattr(metrics, "total_return_pct", 0)) / 100.0
    annual_return = _safe_float(getattr(metrics, "annualized_return", 0)) / 100.0
    max_drawdown = _safe_float(getattr(metrics, "max_drawdown_pct", 0)) / 100.0
    win_rate_pct = _safe_float(getattr(metrics, "win_rate", 0))  # akquant 已是百分数

    result = BacktestResult(
        total_return=round(total_return, 6),
        annual_return=round(annual_return, 6),
        annual_volatility=round(_safe_float(getattr(metrics, "volatility", 0)), 6),
        sharpe_ratio=round(_safe_float(getattr(metrics, "sharpe_ratio", 0)), 4),
        sortino_ratio=round(_safe_float(getattr(metrics, "sortino_ratio", 0)), 4),
        max_drawdown=round(max_drawdown, 6),
        calmar_ratio=round(_safe_float(getattr(metrics, "calmar_ratio", 0)), 4),
        win_rate=round(win_rate_pct / 100.0, 4),  # 百分数→小数
        total_trades=total_trades,
        win_trades=win_count,
        loss_trades=loss_count,
        nav_series=nav_series,
        daily_returns=daily_returns,
        group_navs=None,
        trades=trades,
        orders=orders,
        start_date=start_date,
        end_date=end_date,
        initial_capital=initial_capital,
        final_capital=initial_capital * (1 + total_return),
        n_trading_days=len(daily_returns),
    )

    return result

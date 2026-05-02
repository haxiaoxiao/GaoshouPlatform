"""绩效度量计算 — Sharpe, Sortino, Alpha, Beta, MaxDD, IR, Calmar"""
import numpy as np
import pandas as pd
from dataclasses import dataclass, field


@dataclass
class PerformanceMetrics:
    """回测绩效指标"""

    total_return: float = 0.0
    annual_return: float = 0.0
    annual_volatility: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    max_drawdown: float = 0.0
    calmar_ratio: float = 0.0
    alpha: float = 0.0
    beta: float = 0.0
    information_ratio: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0
    avg_return: float = 0.0

    def to_dict(self) -> dict:
        return {
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "annual_volatility": self.annual_volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "max_drawdown": self.max_drawdown,
            "calmar_ratio": self.calmar_ratio,
            "alpha": self.alpha,
            "beta": self.beta,
            "information_ratio": self.information_ratio,
            "win_rate": self.win_rate,
            "total_trades": self.total_trades,
            "win_trades": self.win_trades,
            "loss_trades": self.loss_trades,
            "avg_return": self.avg_return,
        }


def _annual_factor(freq: str) -> float:
    """年化因子"""
    if freq == "daily":
        return 252
    elif freq == "weekly":
        return 52
    elif freq == "monthly":
        return 12
    return 252


def compute_metrics(
    nav_series: list[dict],
    daily_returns: list[dict],
    trades: list[dict] | None = None,
    benchmark_returns: list[float] | None = None,
    risk_free: float = 0.02,
    freq: str = "daily",
) -> PerformanceMetrics:
    """从净值/收益/交易序列计算全套绩效指标

    Args:
        nav_series: [{"date": str, "nav": float}]
        daily_returns: [{"date": str, "return": float}]
        trades: [{"pnl": float, "action": str, ...}]
        benchmark_returns: 基准日收益序列
        risk_free: 无风险利率
        freq: 频率 "daily" | "weekly" | "monthly"
    """
    result = PerformanceMetrics()
    ann = _annual_factor(freq)

    ret_vals = np.array([d["return"] for d in daily_returns], dtype=float)

    if len(ret_vals) == 0:
        return result

    # Basic
    result.total_return = float(nav_series[-1]["nav"] - 1) if nav_series else 0.0
    n = max(len(ret_vals), 1)
    result.annual_return = float((1 + result.total_return) ** (ann / n) - 1)
    result.annual_volatility = float(np.std(ret_vals) * np.sqrt(ann))

    if result.annual_volatility > 0:
        result.sharpe_ratio = float((result.annual_return - risk_free) / result.annual_volatility)

    # Sortino
    downside = ret_vals[ret_vals < 0]
    downside_std = float(np.std(downside) * np.sqrt(ann)) if len(downside) > 0 else 0.0
    if downside_std > 0:
        result.sortino_ratio = float((result.annual_return - risk_free) / downside_std)

    # Max drawdown
    if nav_series:
        nav_vals = np.array([d["nav"] for d in nav_series], dtype=float)
        cummax = np.maximum.accumulate(nav_vals)
        dd = np.min((nav_vals - cummax) / np.where(cummax > 0, cummax, 1))
        result.max_drawdown = float(dd)
        if abs(result.max_drawdown) > 1e-8:
            result.calmar_ratio = float(result.annual_return / abs(result.max_drawdown))

    # Alpha / Beta / IR (vs benchmark)
    if benchmark_returns is not None and len(benchmark_returns) == len(ret_vals):
        bench = np.array(benchmark_returns, dtype=float)
        excess = ret_vals - bench
        cov = np.cov(ret_vals, bench, ddof=1) if len(ret_vals) > 1 else np.array([[0, 0], [0, 0]])
        bench_var = float(np.var(bench, ddof=1)) if len(bench) > 1 else 0.0

        if bench_var > 0:
            result.beta = float(cov[0, 1] / bench_var)
        else:
            result.beta = 0.0

        rf_daily = risk_free / ann
        result.alpha = float((np.mean(excess) - rf_daily) * ann)

        tracking_error = float(np.std(excess) * np.sqrt(ann)) if len(excess) > 0 else 0.0
        if tracking_error > 0:
            result.information_ratio = float(
                (np.mean(ret_vals) - np.mean(bench)) * ann / tracking_error
            )

    # Trade stats
    if trades:
        result.total_trades = len(trades)
        pnls = [t.get("pnl", 0) or 0 for t in trades]
        result.win_trades = sum(1 for p in pnls if p > 0)
        result.loss_trades = result.total_trades - result.win_trades
        result.win_rate = float(result.win_trades / max(result.total_trades, 1))
        result.avg_return = float(np.mean(pnls)) if pnls else 0.0

    return result

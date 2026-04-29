"""回测分析器 — 收益/回撤/夏普/IC/换手率"""
import numpy as np
import pandas as pd


def compute_annual_return(nav_series: list[dict], n_trading_days: int) -> float:
    """从净值序列计算年化收益"""
    if not nav_series or n_trading_days == 0:
        return 0.0
    total_return = nav_series[-1]["nav"] - 1
    return (1 + total_return) ** (252 / max(n_trading_days, 1)) - 1


def compute_max_drawdown(nav_series: list[dict]) -> float:
    """计算最大回撤"""
    if not nav_series:
        return 0.0
    navs = pd.Series([d["nav"] for d in nav_series])
    cummax = navs.cummax()
    drawdown = (navs - cummax) / cummax
    return float(drawdown.min())


def compute_sharpe_ratio(daily_returns: list[dict], risk_free: float = 0.02) -> float:
    """计算夏普比率"""
    if not daily_returns:
        return 0.0
    rets = pd.Series([d["return"] for d in daily_returns])
    excess = rets.mean() * 252 - risk_free
    vol = rets.std() * np.sqrt(252)
    if vol == 0:
        return 0.0
    return float(excess / vol)


def compute_win_rate(trades: list[dict]) -> float:
    """计算胜率"""
    if not trades:
        return 0.0
    wins = sum(1 for t in trades if t.get("pnl", 0) > 0)
    return wins / len(trades)


def compute_ic_series(
    factor_matrix: pd.DataFrame,
    return_matrix: pd.DataFrame,
) -> pd.Series:
    """计算 IC 序列（Spearman 秩相关）"""
    ic_list = []
    common_dates = factor_matrix.index.intersection(return_matrix.index)
    common_symbols = factor_matrix.columns.intersection(return_matrix.columns)

    for d in common_dates:
        f = factor_matrix.loc[d, common_symbols].dropna()
        r = return_matrix.loc[d, common_symbols].dropna()
        common = f.index.intersection(r.index)
        if len(common) < 10:
            continue
        ic = f[common].corr(r[common], method="spearman")
        if not np.isnan(ic):
            ic_list.append({"date": str(d.date()), "ic": float(ic)})

    if not ic_list:
        return pd.Series()

    return pd.Series({d["date"]: d["ic"] for d in ic_list})


def compute_turnover(
    factor_matrix: pd.DataFrame,
    rebalance_dates: list,
    n_positions: int = 50,
) -> list[dict]:
    """计算每期换手率"""
    turnover = []
    for i in range(1, len(rebalance_dates)):
        prev_date = rebalance_dates[i - 1]
        curr_date = rebalance_dates[i]

        prev_factors = factor_matrix.loc[prev_date].dropna()
        curr_factors = factor_matrix.loc[curr_date].dropna()

        prev_top = set(prev_factors.nlargest(n_positions).index)
        curr_top = set(curr_factors.nlargest(n_positions).index)
        common = prev_top & curr_top

        if len(curr_top) > 0:
            to_rate = 1 - len(common) / n_positions
        else:
            to_rate = 1.0

        turnover.append({"date": str(curr_date.date()), "rate": to_rate})

    return turnover

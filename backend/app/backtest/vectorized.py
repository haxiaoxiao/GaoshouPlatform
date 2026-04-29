"""向量化回测引擎 — 因子分组回测"""
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from app.backtest.config import BacktestConfig, BacktestResult


class VectorizedBacktestEngine:
    """向量化回测引擎

    核心思路：将整个回测区间转化为矩阵运算，避免逐日循环。
    适用于因子分组/排序策略。
    """

    FREQ_MAP = {"daily": 1, "weekly": 5, "monthly": 21}

    def run(
        self,
        factor_matrix: pd.DataFrame,
        return_matrix: pd.DataFrame,
        config: BacktestConfig,
    ) -> BacktestResult:
        """Run vectorized backtest"""
        common_dates = factor_matrix.index.intersection(return_matrix.index)
        common_symbols = factor_matrix.columns.intersection(return_matrix.columns)

        factor_matrix = factor_matrix.loc[common_dates, common_symbols]
        return_matrix = return_matrix.loc[common_dates, common_symbols]

        if len(common_dates) < 2 or len(common_symbols) < 2:
            return BacktestResult(
                initial_capital=config.initial_capital,
                final_capital=config.initial_capital,
            )

        freq_days = self.FREQ_MAP.get(config.rebalance_freq, 21)
        rebalance_dates = common_dates[::freq_days]

        n_groups = config.n_groups

        group_daily_returns: dict[int, list[float]] = {i: [] for i in range(n_groups)}
        nav_dates = []

        for i, rb_date in enumerate(rebalance_dates):
            factors = factor_matrix.loc[rb_date].dropna()
            if len(factors) < n_groups * 2:
                continue

            try:
                groups = pd.qcut(factors, n_groups, labels=False, duplicates="drop")
            except ValueError:
                continue

            if i + 1 < len(rebalance_dates):
                hold_end = rebalance_dates[i + 1]
            else:
                hold_end = common_dates[-1]

            hold_dates = common_dates[
                (common_dates > rb_date) & (common_dates <= hold_end)
            ]
            if len(hold_dates) == 0:
                continue

            hold_returns = return_matrix.loc[hold_dates]

            for g in range(n_groups):
                g_symbols = factors[groups == g].index
                valid_symbols = g_symbols.intersection(common_symbols)
                if len(valid_symbols) == 0:
                    group_daily_returns[g].append(0.0)
                    continue
                g_returns = hold_returns[valid_symbols]
                mean_ret = g_returns.mean(axis=1).mean()
                group_daily_returns[g].append(mean_ret)

            nav_dates.append(rb_date)

        if not nav_dates:
            return BacktestResult(
                initial_capital=config.initial_capital,
                final_capital=config.initial_capital,
            )

        group_navs = {}
        for g in range(n_groups):
            returns = pd.Series(group_daily_returns[g])
            if config.rebalance_freq == "monthly":
                returns = returns - config.commission_rate * 2 / 12
            elif config.rebalance_freq == "weekly":
                returns = returns - config.commission_rate * 2 / 52
            else:
                returns = returns - config.commission_rate * 2

            nav = (1 + returns).cumprod()
            group_navs[f"group_{g + 1}"] = [
                {"date": str(d.date()), "nav": float(v)}
                for d, v in zip(nav_dates, nav)
            ]

        long_returns = pd.Series(group_daily_returns[n_groups - 1])
        short_returns = pd.Series(group_daily_returns[0])
        long_short_returns = long_returns - short_returns - config.commission_rate * 4
        ls_nav = (1 + long_short_returns).cumprod()

        return self._compute_metrics(
            long_short_returns, ls_nav, nav_dates, config, group_navs,
        )

    def _compute_metrics(
        self,
        returns: pd.Series,
        nav: pd.Series,
        nav_dates: list,
        config: BacktestConfig,
        group_navs: dict,
    ) -> BacktestResult:
        """计算回测统计指标"""
        if len(returns) == 0:
            return BacktestResult()

        total_return = float(nav.iloc[-1] - 1)
        n_periods = len(returns)
        annual_return = (1 + total_return) ** (252 / max(n_periods, 1)) - 1
        annual_vol = float(returns.std() * np.sqrt(252 / max(len(returns), 1)))
        sharpe = (annual_return - 0.02) / annual_vol if annual_vol > 0 else 0

        cummax = nav.cummax()
        drawdown = (nav - cummax) / cummax
        max_dd = float(drawdown.min())

        calmar = annual_return / abs(max_dd) if max_dd != 0 else 0

        wins = (returns > 0).sum()
        total = len(returns)
        win_rate = wins / total if total > 0 else 0

        nav_series = [
            {"date": str(d.date()), "nav": float(v)}
            for d, v in zip(nav_dates, nav)
        ]

        return BacktestResult(
            total_return=total_return,
            annual_return=annual_return,
            annual_volatility=annual_vol,
            sharpe_ratio=sharpe,
            max_drawdown=max_dd,
            calmar_ratio=calmar,
            total_trades=total,
            win_trades=wins,
            loss_trades=total - wins,
            win_rate=win_rate,
            avg_return=float(returns.mean()),
            nav_series=nav_series,
            group_navs=group_navs,
            start_date=nav_dates[0] if nav_dates else None,
            end_date=nav_dates[-1] if nav_dates else None,
            initial_capital=config.initial_capital,
            final_capital=config.initial_capital * (1 + total_return),
            n_trading_days=len(returns),
        )


# 全局单例
_vectorized_engine: VectorizedBacktestEngine | None = None


def get_vectorized_engine() -> VectorizedBacktestEngine:
    global _vectorized_engine
    if _vectorized_engine is None:
        _vectorized_engine = VectorizedBacktestEngine()
    return _vectorized_engine

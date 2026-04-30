"""单因子评估服务 — 串联计算层和回测层"""
import logging
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from app.backtest.analyzers import compute_ic_series
from app.backtest.config import BacktestConfig
from app.backtest.runner import get_backtest_runner
from app.compute.expression import evaluate_expression
from app.db.clickhouse import get_ch_client

logger = logging.getLogger(__name__)

_IC_DECAY_LAGS = [1, 3, 5, 10, 20]


class FactorEvaluationService:
    """单因子评估服务 — 串联计算层和回测层"""

    async def run_ic_analysis(
        self,
        expression: str,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> dict:
        """IC 分析：IC 序列 + 统计量 + 衰减分析

        Returns:
            {
                "ic_series": [{"date": str, "ic": float}, ...],
                "ic_stats": {"mean": float, "std": float, "icir": float, "positive_rate": float},
                "ic_decay": [{"lag": int, "ic_mean": float}, ...]
            }
        """
        factor_matrix = self._load_factor_matrix(expression, symbols, start_date, end_date)
        return_matrix = self._load_return_matrix(symbols, start_date, end_date)

        if factor_matrix.empty or return_matrix.empty:
            return {
                "ic_series": [],
                "ic_stats": {"mean": 0.0, "std": 0.0, "icir": 0.0, "positive_rate": 0.0},
                "ic_decay": [{"lag": lag, "ic_mean": 0.0} for lag in _IC_DECAY_LAGS],
            }

        # IC 序列
        ic_series = compute_ic_series(factor_matrix, return_matrix)
        ic_list = [
            {"date": str(d), "ic": float(v)} for d, v in ic_series.items()
        ]

        # IC 统计量
        ic_values = ic_series.values.astype(float) if len(ic_series) > 0 else np.array([])
        if len(ic_values) > 0:
            ic_mean = float(np.mean(ic_values))
            ic_std = float(np.std(ic_values, ddof=1)) if len(ic_values) > 1 else 0.0
            icir = ic_mean / ic_std if ic_std > 0 else 0.0
            positive_rate = float(np.sum(ic_values > 0) / len(ic_values))
        else:
            ic_mean = 0.0
            ic_std = 0.0
            icir = 0.0
            positive_rate = 0.0

        # IC 衰减分析
        ic_decay = []
        for lag in _IC_DECAY_LAGS:
            lagged_return = return_matrix.shift(-lag)
            lagged_ic = compute_ic_series(factor_matrix, lagged_return)
            if len(lagged_ic) > 0:
                lag_ic_mean = float(np.mean(lagged_ic.values.astype(float)))
            else:
                lag_ic_mean = 0.0
            ic_decay.append({"lag": lag, "ic_mean": lag_ic_mean})

        return {
            "ic_series": ic_list,
            "ic_stats": {
                "mean": ic_mean,
                "std": ic_std,
                "icir": icir,
                "positive_rate": positive_rate,
            },
            "ic_decay": ic_decay,
        }

    async def run_quantile_backtest(
        self,
        expression: str,
        symbols: list[str],
        start_date: date,
        end_date: date,
        n_groups: int = 5,
        rebalance_freq: str = "monthly",
    ) -> dict:
        """分层回测：委托给 BacktestRunner

        Returns:
            BacktestResult.to_dict()
        """
        config = BacktestConfig(
            mode="vectorized",
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            factor_expression=expression,
            rebalance_freq=rebalance_freq,
            n_groups=n_groups,
        )
        runner = get_backtest_runner()
        result = await runner.run(config)
        return result.to_dict()

    async def run_full_report(
        self,
        expression: str,
        symbols: list[str],
        start_date: date,
        end_date: date,
        n_groups: int = 5,
        rebalance_freq: str = "monthly",
    ) -> dict:
        """完整单因子报告：IC 分析 + 分层回测"""
        ic_result = await self.run_ic_analysis(expression, symbols, start_date, end_date)
        qt_result = await self.run_quantile_backtest(
            expression, symbols, start_date, end_date, n_groups, rebalance_freq,
        )

        ic_stats = ic_result.get("ic_stats", {})
        summary = {
            "ic_mean": ic_stats.get("mean", 0.0),
            "icir": ic_stats.get("icir", 0.0),
            "long_short_annual_return": qt_result.get("annual_return", 0.0),
            "long_short_sharpe": qt_result.get("sharpe_ratio", 0.0),
            "max_drawdown": qt_result.get("max_drawdown", 0.0),
        }

        return {
            "expression": expression,
            "parameters": {
                "symbols": symbols,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "n_groups": n_groups,
                "rebalance_freq": rebalance_freq,
            },
            "ic_analysis": ic_result,
            "quantile_backtest": qt_result,
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Internal helpers (mirror BacktestRunner data-loading logic)
    # ------------------------------------------------------------------

    def _load_factor_matrix(
        self,
        expression: str,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载并计算因子矩阵"""
        ch = get_ch_client()

        query = """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s
        """
        params: dict[str, Any] = {"syms": symbols}
        if start_date:
            query += " AND trade_date >= %(start)s"
            params["start"] = start_date
        if end_date:
            query += " AND trade_date <= %(end)s"
            params["end"] = end_date
        query += " ORDER BY symbol, trade_date"

        rows = ch.execute(query, params)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low", "close",
                     "volume", "amount", "turnover_rate"],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        result = evaluate_expression(expression, data)

        if isinstance(result, dict):
            factor_dfs = []
            for sym, series in result.items():
                if isinstance(series, pd.Series):
                    s = series.rename(sym)
                    factor_dfs.append(s)
            if factor_dfs:
                return pd.concat(factor_dfs, axis=1)

        if isinstance(result, pd.Series):
            return result.to_frame()

        if isinstance(result, pd.DataFrame):
            return result

        return pd.DataFrame()

    def _load_return_matrix(
        self,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载收益率矩阵 — 下一日收益率"""
        ch = get_ch_client()

        rows = ch.execute(
            """
            SELECT symbol, trade_date, close
            FROM klines_daily
            WHERE symbol IN %(syms)s
            ORDER BY symbol, trade_date
            """,
            {"syms": symbols},
        )

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["symbol", "trade_date", "close"])
        df["close"] = df["close"].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        return_matrix: dict[str, pd.Series] = {}
        for sym, grp in df.groupby("symbol"):
            grp = grp.sort_values("trade_date")
            ret = grp["close"].pct_change().shift(-1)
            ret.index = grp["trade_date"]
            return_matrix[sym] = ret

        if return_matrix:
            return pd.DataFrame(return_matrix)
        return pd.DataFrame()

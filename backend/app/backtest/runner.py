"""回测统一入口"""
from datetime import date
from typing import Any

import pandas as pd
from loguru import logger

from app.backtest.config import BacktestConfig, BacktestResult
from app.backtest.vectorized import get_vectorized_engine


class BacktestRunner:
    """回测运行器 — 统一入口"""

    async def run(self, config: BacktestConfig) -> BacktestResult:
        """运行回测"""
        if config.mode == "vectorized":
            return await self._run_vectorized(config)
        elif config.mode == "event_driven":
            return await self._run_event_driven(config)
        else:
            raise ValueError(f"Unknown backtest mode: {config.mode}")

    async def _run_vectorized(self, config: BacktestConfig) -> BacktestResult:
        if config.factor_expression is None:
            raise ValueError("factor_expression is required for vectorized backtest")

        factor_matrix = await self._load_factor_matrix(
            config.factor_expression, config.symbols,
            config.start_date, config.end_date,
        )

        return_matrix = await self._load_return_matrix(
            config.symbols, config.start_date, config.end_date,
        )

        engine = get_vectorized_engine()
        result = engine.run(factor_matrix, return_matrix, config)
        return result

    async def _run_event_driven(self, config: BacktestConfig) -> BacktestResult:
        logger.warning("Event-driven backtest not yet implemented")
        return BacktestResult(
            initial_capital=config.initial_capital,
            final_capital=config.initial_capital,
        )

    async def _load_factor_matrix(
        self,
        expression: str,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载并计算因子矩阵"""
        from app.db.clickhouse import get_ch_client
        from app.compute.expression import evaluate_expression

        ch = get_ch_client()

        query = """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s
        """
        params: dict = {"syms": symbols}
        if start_date:
            query += " AND trade_date >= %(start)s"
            params["start"] = start_date
        if end_date:
            query += " AND trade_date <= %(end)s"
            params["end"] = end_date
        query += " ORDER BY symbol, trade_date"

        rows = ch.execute(query, params)
        if not rows:
            raise ValueError("No data found")

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

        raise ValueError("Failed to compute factor matrix")

    async def _load_return_matrix(
        self,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载收益率矩阵 — 下一日收益率"""
        from app.db.clickhouse import get_ch_client
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

        df = pd.DataFrame(rows, columns=["symbol", "trade_date", "close"])
        df["close"] = df["close"].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        return_matrix = {}
        for sym, grp in df.groupby("symbol"):
            grp = grp.sort_values("trade_date")
            ret = grp["close"].pct_change().shift(-1)
            ret.index = grp["trade_date"]
            return_matrix[sym] = ret

        if return_matrix:
            return pd.DataFrame(return_matrix)
        return pd.DataFrame()


# 全局单例
_runner: BacktestRunner | None = None


def get_backtest_runner() -> BacktestRunner:
    global _runner
    if _runner is None:
        _runner = BacktestRunner()
    return _runner

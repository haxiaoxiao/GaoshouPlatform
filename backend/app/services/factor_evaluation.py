"""单因子评估服务 — 串联计算层和回测层"""
import logging
from datetime import date
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from app.backtest.analyzers import compute_ic_series
from app.backtest.config import BacktestConfig
from app.backtest.runner import get_backtest_runner
from app.compute.expression import evaluate_expression
from app.db.clickhouse import get_ch_client
from app.models.factor import (
    FactorConfig, EvalConfig, BtConfig, FactorReport,
    ICPoint, IndustryIC, TurnoverPoint, DecayPoint, StockFactorValue,
    BoardQuery, BoardRow, BoardResponse, StockPool,
)
from app.services.compute_service import compute_service

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
    # New report + board (Tasks A5-A6)
    # ------------------------------------------------------------------

    async def report(self, config: FactorConfig, eval_config: EvalConfig | None = None) -> FactorReport:
        """Generate 6-module factor analysis report."""
        eval_config = eval_config or EvalConfig()
        pool_symbols = await self._resolve_pool(config.stock_pool)
        factor_df = self._load_factor_matrix(
            config.expression, pool_symbols,
            config.start_date, config.end_date,
        )
        return_df = self._load_return_matrix(
            pool_symbols, config.start_date, config.end_date,
        )

        # Module 1: IC time series
        ic_points = self._compute_ic_series(factor_df, return_df, eval_config)

        # Module 2: Industry IC
        industry_ic = self._compute_industry_ic(factor_df, return_df)

        # Module 3: Turnover
        turnover = self._compute_turnover(factor_df)

        # Module 4: Signal decay
        signal_decay = self._compute_signal_decay(factor_df, return_df)

        # Module 5 & 6: Top/Bottom 20
        top20: list[StockFactorValue] = []
        bottom20: list[StockFactorValue] = []
        if isinstance(factor_df, pd.DataFrame) and not factor_df.empty:
            if factor_df.index.nlevels > 1:
                latest_date = factor_df.index.get_level_values("date").max()
                latest_factor = factor_df.xs(latest_date, level="date")
            else:
                latest_date = factor_df.index.max()
                latest_factor = factor_df.loc[latest_date]

            if isinstance(latest_factor, pd.Series):
                sorted_factor = latest_factor.sort_values()
                top20 = self._top_n_stocks(sorted_factor, 20, ascending=False)
                bottom20 = self._top_n_stocks(sorted_factor, 20, ascending=True)
            elif isinstance(latest_factor, pd.DataFrame) and "value" in latest_factor.columns:
                sorted_factor = latest_factor.sort_values("value")
                top20 = self._top_n_stocks(sorted_factor["value"], 20, ascending=False)
                bottom20 = self._top_n_stocks(sorted_factor["value"], 20, ascending=True)

        return FactorReport(
            ic_series=[ICPoint(date=d, value=v) for d, v in ic_points],
            industry_ic=[IndustryIC(industry=i, value=v) for i, v in industry_ic],
            turnover=[TurnoverPoint(date=d, min_quantile=mn, max_quantile=mx)
                      for d, mn, mx in turnover],
            signal_decay=[DecayPoint(lag=l, min_quantile=mn, max_quantile=mx)
                          for l, mn, mx in signal_decay],
            top20=top20,
            bottom20=bottom20,
            update_date=date.today(),
        )

    async def board_query(self, query: BoardQuery) -> BoardResponse:
        """Query factor board with filters, sorting, and pagination."""
        from app.services.factor_templates import FactorTemplatesService
        templates_svc = FactorTemplatesService()
        all_templates = templates_svc.list_templates()
        if query.categories:
            all_templates = [t for t in all_templates if t.category in query.categories]

        rows: list[BoardRow] = []
        for tmpl in all_templates:
            cfg = FactorConfig(
                expression=tmpl.preset_expression,
                stock_pool=query.stock_pool,
                start_date=self._period_start_date(query.period),
                end_date=date.today(),
            )
            try:
                report_obj = await self.report(cfg)
                ic_values = [p.value for p in report_obj.ic_series]
                if ic_values:
                    ic_mean = sum(ic_values) / len(ic_values)
                    variance = sum((v - ic_mean) ** 2 for v in ic_values) / len(ic_values)
                    ic_std = variance ** 0.5
                    ir = ic_mean / ic_std if ic_std != 0 else 0.0
                else:
                    ic_mean = 0.0
                    ir = 0.0

                rows.append(BoardRow(
                    factor_name=tmpl.name,
                    category=tmpl.category,
                    min_quantile_excess_return=0.0,
                    max_quantile_excess_return=0.0,
                    min_quantile_turnover=0.0,
                    max_quantile_turnover=0.0,
                    ic_mean=round(ic_mean, 4),
                    ir=round(ir, 4),
                ))
            except Exception:
                logger.exception("board_query failed for template: %s", tmpl.name)
                continue

        # Sort
        reverse = query.sort_order == "desc"
        rows.sort(key=lambda r: getattr(r, query.sort_by, 0), reverse=reverse)

        # Paginate
        total = len(rows)
        start = (query.page - 1) * query.page_size
        end = start + query.page_size
        page_rows = rows[start:end]

        return BoardResponse(
            rows=page_rows, total=total,
            page=query.page, page_size=query.page_size,
        )

    # ------------------------------------------------------------------
    # Report helpers
    # ------------------------------------------------------------------

    def _compute_ic_series(self, factor_df, return_df, eval_config) -> list[tuple]:
        series = compute_ic_series(factor_df, return_df)
        result: list[tuple] = []
        for d_str, v in series.items():
            try:
                d = date.fromisoformat(str(d_str)[:10])
            except (ValueError, TypeError):
                continue
            result.append((d, float(v)))
        return result

    def _compute_industry_ic(self, factor_df, return_df) -> list[tuple]:
        return []  # Requires industry classification data

    def _compute_turnover(self, factor_df) -> list[tuple]:
        return []  # Requires quantile group tracking

    def _compute_signal_decay(self, factor_df, return_df) -> list[tuple]:
        return []  # Requires lagged IC computation

    def _top_n_stocks(self, sorted_series, n, ascending) -> list[StockFactorValue]:
        items = sorted_series.nlargest(n) if not ascending else sorted_series.nsmallest(n)
        result: list[StockFactorValue] = []
        for idx, val in items.items():
            result.append(StockFactorValue(symbol=str(idx), name=str(idx), value=float(val)))
        return result

    def _period_start_date(self, period: str) -> date:
        today = date.today()
        mapping = {"3m": 90, "1y": 365, "3y": 3 * 365, "10y": 10 * 365}
        days = mapping.get(period, 365)
        return today - timedelta(days=days)

    async def _resolve_pool(self, stock_pool) -> list[str]:
        sp = stock_pool if isinstance(stock_pool, StockPool) else StockPool(stock_pool)
        return await compute_service._resolve_stock_pool(sp)

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


# ------------------------------------------------------------------
# Singleton factory
# ------------------------------------------------------------------

_evaluation_service: FactorEvaluationService | None = None


def get_evaluation_service() -> FactorEvaluationService:
    global _evaluation_service
    if _evaluation_service is None:
        _evaluation_service = FactorEvaluationService()
    return _evaluation_service

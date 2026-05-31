"""单因子评估服务 — 串联计算层和回测层"""
import asyncio
import calendar
import inspect
from datetime import date
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger

from app.backtest.analyzers import compute_ic_series
from app.backtest.config import BacktestConfig
from app.backtest.runner import get_backtest_runner
from app.compute.expression import evaluate_expression
from app.compute.operators import auto_discover
from app.data_stores import get_market_data_store

try:
    from app.db.clickhouse import get_ch_client
except Exception:  # pragma: no cover - compatibility for parquet-only deployments.
    def get_ch_client():
        return None

# Ensure operators are registered before any expression evaluation
auto_discover()
from app.models.factor import (
    FactorConfig, EvalConfig, BtConfig, FactorReport,
    ICPoint, IndustryIC, TurnoverPoint, DecayPoint, StockFactorValue,
    BoardQuery, BoardRow, BoardResponse, StockPool,
)
from app.services.compute_service import compute_service

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
        rows = await self._saved_factor_board_rows(query)
        if rows or query.factor_groups or query.factor_keyword:
            total = len(rows)
            await self._attach_latest_research_runs(rows, query)
            if self._sort_uses_coverage(query.sort_by):
                await self._maybe_await(self._attach_board_coverage(rows, query))
            rows = self._sort_and_page_rows(rows, query)
            return BoardResponse(
                rows=rows,
                total=total,
                page=query.page,
                page_size=query.page_size,
            )

        from app.services.factor_templates import FactorTemplatesService
        templates_svc = FactorTemplatesService()
        all_templates = templates_svc.list_templates()
        if query.categories:
            all_templates = [t for t in all_templates if t.category in query.categories]

        rows = []
        for tmpl in all_templates:
            start_date, end_date = self._board_date_range(query)
            cfg = FactorConfig(
                expression=tmpl.preset_expression,
                stock_pool=query.stock_pool,
                start_date=start_date,
                end_date=end_date,
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

        total = len(rows)
        await self._attach_latest_research_runs(rows, query)
        if self._sort_uses_coverage(query.sort_by):
            await self._maybe_await(self._attach_board_coverage(rows, query))
        rows = self._sort_and_page_rows(rows, query)
        return BoardResponse(
            rows=rows, total=total,
            page=query.page, page_size=query.page_size,
        )

    async def _saved_factor_board_rows(self, query: BoardQuery) -> list[BoardRow]:
        """Return saved custom factors + built-in indicator definitions for the board."""
        from app.db.sqlite import async_session_factory
        from app.db.models import Factor, FactorAnalysis
        from sqlalchemy import select

        rows: list[BoardRow] = []
        from app.services.factor_value_store import list_factor_definitions, list_factor_groups

        definitions = list_factor_definitions()
        definition_by_name = {str(item.get("name")): item for item in definitions}
        group_by_factor: dict[str, dict[str, Any]] = {}
        selected_group_names = set(query.factor_groups or [])
        for group in list_factor_groups():
            if selected_group_names and str(group.get("name")) not in selected_group_names:
                continue
            for factor_name in group.get("factor_names") or []:
                group_by_factor[str(factor_name)] = group

        # 1) 用户自建因子 (factors 表)
        async with async_session_factory() as session:
            stmt = select(Factor)
            result = await session.execute(stmt.order_by(Factor.created_at.desc()))
            factors = list(result.scalars().all())
            latest_analysis_by_factor_id: dict[int, FactorAnalysis] = {}
            factor_ids = [int(factor.id) for factor in factors if factor.id is not None]
            if factor_ids:
                from sqlalchemy import func

                ranked = (
                    select(
                        FactorAnalysis.id.label("analysis_id"),
                        func.row_number().over(
                            partition_by=FactorAnalysis.factor_id,
                            order_by=FactorAnalysis.created_at.desc(),
                        ).label("row_num"),
                    )
                    .where(FactorAnalysis.factor_id.in_(factor_ids))
                    .subquery()
                )
                latest_stmt = (
                    select(FactorAnalysis)
                    .join(ranked, FactorAnalysis.id == ranked.c.analysis_id)
                    .where(ranked.c.row_num == 1)
                )
                latest_rows = (await session.execute(latest_stmt)).scalars().all()
                latest_analysis_by_factor_id = {
                    int(analysis.factor_id): analysis
                    for analysis in latest_rows
                    if analysis.factor_id is not None
                }
            for factor in factors:
                analysis = latest_analysis_by_factor_id.get(int(factor.id))
                details = analysis.details if analysis and analysis.details else {}
                definition = definition_by_name.get(str(factor.name), {})
                group = group_by_factor.get(str(factor.name))
                if selected_group_names and group is None:
                    continue
                category = str(definition.get("category") or "custom")
                if query.categories and category not in set(query.categories):
                    continue
                rows.append(BoardRow(
                    factor_name=factor.name,
                    display_name=str(definition.get("display_name") or factor.name),
                    description=str(definition.get("description") or factor.description or ""),
                    source=str(definition.get("source") or factor.source or "custom.factor"),
                    factor_group=str(group.get("name")) if group else None,
                    factor_group_display_name=str(group.get("display_name")) if group else None,
                    category=category,
                    min_quantile_excess_return=float(details.get("min_quantile_excess_return") or 0.0),
                    max_quantile_excess_return=float(details.get("max_quantile_excess_return") or 0.0),
                    min_quantile_turnover=float(analysis.turnover_rate or 0.0) if analysis else 0.0,
                    max_quantile_turnover=float(details.get("max_quantile_turnover") or 0.0),
                    ic_mean=float(analysis.ic_mean or 0.0) if analysis else 0.0,
                    ir=float(analysis.ir or 0.0) if analysis else 0.0,
                ))

        # 2) 内置指标定义（因子值缓存中的通用因子）
        builtins = definitions
        if query.categories:
            builtins = [d for d in builtins if d.get("category") in query.categories]
        seen_names = {row.factor_name for row in rows}
        for d in builtins:
            name = d["name"]
            if name in seen_names:
                continue
            group = group_by_factor.get(str(name))
            if selected_group_names and group is None:
                continue
            rows.append(BoardRow(
                factor_name=name,
                display_name=str(d.get("display_name") or name),
                description=str(d.get("description") or ""),
                source=str(d.get("source") or "builtin"),
                factor_group=str(group.get("name")) if group else None,
                factor_group_display_name=str(group.get("display_name")) if group else None,
                category=d.get("category", "builtin"),
                min_quantile_excess_return=0.0,
                max_quantile_excess_return=0.0,
                min_quantile_turnover=0.0,
                max_quantile_turnover=0.0,
                ic_mean=0.0,
                ir=0.0,
            ))
        return self._filter_board_rows(rows, query)

    def _filter_board_rows(self, rows: list[BoardRow], query: BoardQuery) -> list[BoardRow]:
        keyword = str(query.factor_keyword or "").strip().lower()
        if not keyword:
            return rows
        return [row for row in rows if self._board_row_matches_keyword(row, keyword)]

    @staticmethod
    def _board_row_matches_keyword(row: BoardRow, keyword: str) -> bool:
        haystack = " ".join(
            str(value or "")
            for value in (
                row.factor_name,
                row.display_name,
                row.description,
                row.source,
                row.factor_group,
                row.factor_group_display_name,
                row.category,
            )
        ).lower()
        return keyword in haystack

    async def _attach_missing_board_coverage(self, rows: list[BoardRow], query: BoardQuery) -> None:
        missing = [row for row in rows if row.coverage_status == "unknown"]
        if missing:
            await self._maybe_await(self._attach_board_coverage(missing, query))

    async def _attach_board_coverage(self, rows: list[BoardRow], query: BoardQuery) -> None:
        if not rows:
            return
        from app.services.factor_value_store import get_factor_value_store

        start_date, end_date = self._board_date_range(query)
        store = get_factor_value_store()
        try:
            # Limit board coverage scans to the active research window so
            # sorting by latest metrics can stay responsive on large groups.
            coverage_by_factor = await asyncio.to_thread(
                store.coverage_many,
                [row.factor_name for row in rows],
                start_date=start_date,
                end_date=end_date,
            )
        except Exception:
            logger.exception("Failed to batch attach factor board coverage")
            coverage_by_factor = {}
        for row in rows:
            try:
                coverage = coverage_by_factor.get(row.factor_name) or store._empty_coverage(row.factor_name)
                row.coverage_total_rows = int(coverage.get("total_rows") or 0)
                row.coverage_symbol_count = int(coverage.get("symbol_count") or 0)
                row.coverage_date_count = int(coverage.get("date_count") or 0)
                row.coverage_min_date = coverage.get("min_date")
                row.coverage_max_date = coverage.get("max_date")
                row.coverage_status = self._coverage_status(row.coverage_total_rows, row.coverage_max_date, end_date)
            except Exception:
                logger.exception("Failed to attach factor board coverage: %s", row.factor_name)
                row.coverage_status = "unknown"

    async def _attach_latest_research_runs(self, rows: list[BoardRow], query: BoardQuery) -> None:
        if not rows:
            return
        try:
            from app.services.factor_research_runs import (
                factor_research_run_service,
                research_match_hash,
                research_params_hash,
            )

            payload_by_factor = {
                row.factor_name: self._board_research_payload(row.factor_name, query)
                for row in rows
            }
            normalized_by_factor = {
                factor_name: factor_research_run_service._normalized_params(payload)
                for factor_name, payload in payload_by_factor.items()
            }
            params_hash_by_factor = {
                factor_name: research_params_hash(params)
                for factor_name, params in normalized_by_factor.items()
            }
            match_hash_by_factor = {
                factor_name: research_match_hash(params, ignore_date_range=True)
                for factor_name, params in normalized_by_factor.items()
            }
            requested_range_by_factor = {
                factor_name: (
                    date.fromisoformat(str(payload["start_date"])),
                    date.fromisoformat(str(payload["end_date"])),
                )
                for factor_name, payload in payload_by_factor.items()
            }
            summaries = await factor_research_run_service.latest_summaries(
                [row.factor_name for row in rows],
                stock_pool_value=str(query.stock_pool),
                params_hash_by_factor=params_hash_by_factor,
                match_hash_by_factor=match_hash_by_factor,
                requested_range_by_factor=requested_range_by_factor,
            )
        except Exception:
            logger.exception("Failed to attach latest factor research runs")
            return

        for row in rows:
            summary = summaries.get(row.factor_name)
            if not summary:
                continue
            row.latest_run_id = summary.get("run_id")
            row.latest_run_at = summary.get("completed_at") or summary.get("created_at")
            row.latest_ic_mean = summary.get("ic_mean")
            row.latest_icir = summary.get("icir")
            row.latest_long_short_return = summary.get("long_short_return")
            row.latest_max_drawdown = summary.get("max_drawdown")
            row.latest_turnover = summary.get("turnover")
            row.latest_active_symbol_count = summary.get("active_symbol_count")
            self._attach_research_snapshot_coverage(row, summary)
            row.ic_mean = float(row.latest_ic_mean or 0.0)
            row.ir = float(row.latest_icir or 0.0)

    def _attach_research_snapshot_coverage(self, row: BoardRow, summary: dict[str, Any]) -> None:
        """Use persisted research summary as the board coverage snapshot."""
        min_date = summary.get("effective_start_date") or summary.get("requested_start_date")
        max_date = summary.get("effective_end_date") or summary.get("requested_end_date")
        symbol_count = self._summary_int(summary.get("active_symbol_count") or summary.get("symbol_count"))
        date_count = self._summary_int(summary.get("effective_trading_days"))
        coverage_ratio = self._summary_float(summary.get("coverage_ratio"))

        if min_date is not None:
            row.coverage_min_date = str(min_date)
        if max_date is not None:
            row.coverage_max_date = str(max_date)
        if symbol_count is not None:
            row.coverage_symbol_count = symbol_count
        if date_count is not None:
            row.coverage_date_count = date_count

        if symbol_count is None or date_count is None:
            return
        total_rows = symbol_count * date_count
        if coverage_ratio is not None:
            total_rows = round(total_rows * coverage_ratio)
        row.coverage_total_rows = max(int(total_rows), 0)
        if row.coverage_total_rows <= 0:
            row.coverage_status = "empty"
        elif coverage_ratio is None or coverage_ratio >= 0.99:
            row.coverage_status = "covered"
        else:
            row.coverage_status = "partial"

    @staticmethod
    def _summary_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _summary_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _board_research_payload(self, factor_name: str, query: BoardQuery) -> dict[str, Any]:
        start_date, end_date = self._board_date_range(query)
        costs = self._cost_params_from_query(query)
        factor_value_params_hash = (query.factor_value_params_hashes or {}).get(factor_name)
        return {
            "factor_name": factor_name,
            "stock_pool_value": str(query.stock_pool),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "portfolio_type": getattr(query.portfolio_type, "value", str(query.portfolio_type)),
            "rebalance_period": "monthly",
            **costs,
            "filter_limit_up": bool(query.filter_limit_up),
            "filter_limit_down": bool(query.filter_limit_down),
            "group_count": int(query.group_count or 5),
            "direction": getattr(query.direction, "value", str(query.direction)),
            "pool_membership_mode": query.pool_membership_mode,
            "factor_value_params_hash": factor_value_params_hash,
            "factor_value_params_hashes": {factor_name: factor_value_params_hash} if factor_value_params_hash else {},
            "outlier_handling": query.outlier_handling,
            "industry_neutralization": bool(query.industry_neutralization),
            "standardize": bool(query.standardize),
        }

    @staticmethod
    def _cost_params_from_query(query: BoardQuery) -> dict[str, float]:
        if any(
            getattr(query, field) is not None
            for field in ("fee_rate", "stamp_tax_rate", "transfer_fee_rate", "slippage")
        ):
            return {
                "fee_rate": float(query.fee_rate or 0.0),
                "stamp_tax_rate": float(query.stamp_tax_rate or 0.0),
                "transfer_fee_rate": float(query.transfer_fee_rate or 0.0),
                "slippage": float(query.slippage or 0.0),
            }
        return FactorEvaluationService._cost_params_from_config(query.fee_config)

    @staticmethod
    def _cost_params_from_config(fee_config: str) -> dict[str, float]:
        if fee_config == "none":
            return {"fee_rate": 0.0, "stamp_tax_rate": 0.0, "transfer_fee_rate": 0.0, "slippage": 0.0}
        if fee_config == "commission_stamp_slippage":
            return {"fee_rate": 0.003, "stamp_tax_rate": 0.001, "transfer_fee_rate": 0.0, "slippage": 0.001}
        return {"fee_rate": 0.003, "stamp_tax_rate": 0.001, "transfer_fee_rate": 0.0, "slippage": 0.0}

    @staticmethod
    def _coverage_status(total_rows: int, max_date: str | None, end_date: date) -> str:
        if total_rows <= 0:
            return "empty"
        if max_date == end_date.isoformat():
            return "covered"
        return "partial"

    def _sort_and_page_rows(self, rows: list[BoardRow], query: BoardQuery) -> list[BoardRow]:
        reverse = query.sort_order == "desc"
        sort_attr = query.sort_by
        sortable: list[BoardRow] = []
        missing: list[BoardRow] = []
        for row in rows:
            value = getattr(row, sort_attr, None)
            if value is None:
                missing.append(row)
                continue
            sortable.append(row)
        sortable.sort(key=lambda row: self._board_sort_key(getattr(row, sort_attr, None)), reverse=reverse)
        rows[:] = sortable + missing
        start = (query.page - 1) * query.page_size
        end = start + query.page_size
        return rows[start:end]

    @staticmethod
    def _sort_uses_coverage(sort_by: str) -> bool:
        return sort_by in {
            "coverage_min_date",
            "coverage_max_date",
            "coverage_total_rows",
            "coverage_symbol_count",
            "coverage_date_count",
            "coverage_status",
        }

    @staticmethod
    def _board_sort_key(value: Any) -> Any:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return float(value)
        return str(value)

    @staticmethod
    async def _maybe_await(value: Any) -> Any:
        if inspect.isawaitable(value):
            return await value
        return value

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
        """Compute IC per industry using SQLite stocks.industry classification.

        Returns list of (industry_name, mean_ic).
        """
        if not isinstance(factor_df, pd.DataFrame) or factor_df.empty:
            return []
        if not isinstance(return_df, pd.DataFrame) or return_df.empty:
            return []

        # Load industry mapping
        symbols = list(factor_df.columns) if isinstance(factor_df, pd.DataFrame) else []
        if not symbols:
            return []
        industry_map = self._load_industry_map(symbols)
        if not industry_map:
            return []

        result: list[tuple] = []
        # Group symbols by industry
        industries: dict[str, list[str]] = {}
        for sym in symbols:
            ind = industry_map.get(sym, "未知")
            industries.setdefault(ind, []).append(sym)

        # Compute IC per industry
        for ind_name, ind_symbols in industries.items():
            if len(ind_symbols) < 5:  # skip industries with too few stocks
                continue
            ind_factor = factor_df[ind_symbols].dropna(how="all")
            ind_return = return_df[ind_symbols].dropna(how="all")
            if ind_factor.empty or ind_return.empty:
                continue
            try:
                ic_series = compute_ic_series(ind_factor, ind_return)
                if ic_series.empty:
                    continue
                mean_ic = float(ic_series.mean())
                result.append((ind_name, round(mean_ic, 4)))
            except Exception:
                continue

        result.sort(key=lambda x: -abs(x[1]))  # sort by absolute IC
        return result

    def _load_industry_map(self, symbols: list[str]) -> dict[str, str]:
        """Load industry classification from SQLite stocks table."""
        import sqlite3
        from pathlib import Path
        from app.core.config import settings

        db_path = Path(settings.data_dir) / "gaoshou.db"
        if not db_path.exists():
            return {}
        placeholders = ",".join("?" for _ in symbols)
        try:
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    f"SELECT symbol, industry FROM stocks WHERE symbol IN ({placeholders})",
                    list(symbols),
                ).fetchall()
        except sqlite3.Error:
            return {}
        return {str(row[0]): str(row[1] or "未知") for row in rows if row[1]}

    def _compute_turnover(self, factor_df) -> list[tuple]:
        """Compute quantile turnover between consecutive dates.

        For each pair of consecutive trading dates, assigns stocks to
        quintiles based on factor rank, then measures what fraction of
        stocks in the top quintile (and bottom quintile) changed from
        the previous date.

        Returns list of (date, top_quintile_turnover, bottom_quintile_turnover).
        """
        if not isinstance(factor_df, pd.DataFrame) or factor_df.empty:
            return []
        n_groups = 5
        dates = sorted(factor_df.index.unique())
        if len(dates) < 2:
            return []

        # Assign quintile labels per date
        quantile_map: dict[object, pd.Series] = {}
        for d in dates:
            row = factor_df.loc[d]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[:, 0]
            valid = row.dropna()
            if len(valid) < n_groups * 2:
                quantile_map[d] = pd.Series(dtype=int)
                continue
            try:
                quantile_map[d] = pd.qcut(valid, n_groups, labels=False, duplicates="drop")
            except Exception:
                quantile_map[d] = pd.Series(dtype=int)

        result: list[tuple] = []
        for i in range(1, len(dates)):
            prev_d = dates[i - 1]
            curr_d = dates[i]
            prev_q = quantile_map.get(prev_d, pd.Series(dtype=int))
            curr_q = quantile_map.get(curr_d, pd.Series(dtype=int))
            if prev_q.empty or curr_q.empty:
                continue

            common = prev_q.index.intersection(curr_q.index)
            if len(common) < n_groups * 2:
                continue
            prev_q = prev_q.loc[common]
            curr_q = curr_q.loc[common]

            # Top quintile turnover (label == n_groups-1)
            top_prev = set(prev_q[prev_q == n_groups - 1].index)
            top_curr = set(curr_q[curr_q == n_groups - 1].index)
            top_stayers = len(top_prev & top_curr)
            top_turnover = 1.0 - top_stayers / max(len(top_prev), 1)

            # Bottom quintile turnover (label == 0)
            bot_prev = set(prev_q[prev_q == 0].index)
            bot_curr = set(curr_q[curr_q == 0].index)
            bot_stayers = len(bot_prev & bot_curr)
            bot_turnover = 1.0 - bot_stayers / max(len(bot_prev), 1)

            try:
                d = date.fromisoformat(str(curr_d)[:10])
            except (ValueError, TypeError):
                continue
            result.append((d, round(bot_turnover, 4), round(top_turnover, 4)))
        return result

    def _compute_signal_decay(self, factor_df, return_df) -> list[tuple]:
        """Compute IC decay across multiple forward-return horizons.

        For each lag in [1, 3, 5, 10, 20], shifts the return matrix
        forward by `lag` periods and computes the cross-sectional
        Spearman IC, returning the mean IC at each lag.

        Returns list of (lag, mean_ic, top_quintile_return, bottom_quintile_return).
        """
        lags = [1, 3, 5, 10, 20]
        if not isinstance(factor_df, pd.DataFrame) or factor_df.empty:
            return [(lag, 0.0, 0.0) for lag in lags]
        if not isinstance(return_df, pd.DataFrame) or return_df.empty:
            return [(lag, 0.0, 0.0) for lag in lags]

        factor_df = factor_df.copy()
        return_df = return_df.copy()
        factor_df.index = pd.to_datetime(factor_df.index).normalize()
        return_df.index = pd.to_datetime(return_df.index).normalize()

        result: list[tuple] = []
        for lag in lags:
            lagged_return = return_df.shift(-lag)
            try:
                ic_series = compute_ic_series(factor_df, lagged_return)
            except Exception:
                result.append((lag, 0.0, 0.0))
                continue
            if ic_series.empty:
                result.append((lag, 0.0, 0.0))
                continue
            mean_ic = float(ic_series.mean())

            # Also compute top/bottom quantile returns at this lag
            dates = sorted(factor_df.index.intersection(lagged_return.dropna(how="all").index))
            top_ret = 0.0
            bot_ret = 0.0
            if len(dates) >= 2:
                top_rets = []
                bot_rets = []
                for d in dates:
                    f = factor_df.loc[d]
                    r = lagged_return.loc[d]
                    if isinstance(f, pd.DataFrame):
                        f = f.iloc[:, 0]
                    if isinstance(r, pd.DataFrame):
                        r = r.iloc[:, 0]
                    common = f.dropna().index.intersection(r.dropna().index)
                    if len(common) < 10:
                        continue
                    f_common = f.loc[common]
                    r_common = r.loc[common]
                    try:
                        labels = pd.qcut(f_common, 5, labels=False, duplicates="drop")
                        top_rets.append(r_common[labels == 4].mean())
                        bot_rets.append(r_common[labels == 0].mean())
                    except Exception:
                        continue
                if top_rets:
                    top_ret = float(np.mean(top_rets))
                    bot_ret = float(np.mean(bot_rets))

            result.append((lag, round(bot_ret, 4), round(top_ret, 4)))
        return result

    def _top_n_stocks(self, sorted_series, n, ascending) -> list[StockFactorValue]:
        items = sorted_series.nlargest(n) if not ascending else sorted_series.nsmallest(n)
        result: list[StockFactorValue] = []
        for idx, val in items.items():
            result.append(StockFactorValue(symbol=str(idx), name=str(idx), value=float(val)))
        return result

    def _period_start_date(self, period: str) -> date:
        today = date.today()
        if period == "3m":
            month = today.month - 3
            year = today.year
            while month <= 0:
                month += 12
                year -= 1
            day = min(today.day, calendar.monthrange(year, month)[1])
            return date(year, month, day)
        years = {"1y": 1, "3y": 3, "10y": 10}.get(period, 1)
        year = today.year - years
        day = min(today.day, calendar.monthrange(year, today.month)[1])
        return date(year, today.month, day)

    def _board_date_range(self, query: BoardQuery) -> tuple[date, date]:
        end_date = query.end_date or date.today()
        start_date = query.start_date or self._period_start_date(query.period)
        if end_date < start_date:
            return end_date, end_date
        return start_date, end_date

    async def _resolve_pool(self, stock_pool) -> list[str]:
        return await compute_service._resolve_stock_pool(str(stock_pool))

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
        """加载并计算因子矩阵。

        支持两种来源：
        1) 因子值缓存中的内置指标名（如 market_cap, v4gv）
        2) DSL 表达式（如 ts_mean($close, 20)）
        """
        sd = start_date or date(2000, 1, 1)
        ed = end_date or date.today()

        # 1) 尝试从因子值缓存读取（内置指标）
        factor_df = self._load_from_factor_store(expression, symbols, sd, ed)
        if factor_df is not None and not factor_df.empty:
            return factor_df

        # 内置因子无缓存数据时返回空，不尝试 DSL 解析
        if self._is_builtin_factor(expression):
            return pd.DataFrame()

        # 2) DSL 表达式计算
        store = get_market_data_store()
        df = store.load_daily(symbols, sd, ed)
        if df.empty:
            return pd.DataFrame()
        if "turnover_rate" not in df.columns:
            df["turnover_rate"] = np.nan

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp

        from app.compute.operators import auto_discover
        auto_discover(force_reload=True)
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

    def _is_builtin_factor(self, name: str) -> bool:
        """检查名称是否是内置因子定义（非 DSL 表达式）。"""
        try:
            from app.services.factor_value_store import FACTOR_DEFINITIONS
            return name in FACTOR_DEFINITIONS
        except Exception:
            return False

    def _load_from_factor_store(
        self,
        name: str,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame | None:
        """尝试从 FactorValueStore (Parquet) 加载内置因子的历史矩阵。"""
        try:
            from app.services.factor_value_store import get_factor_value_store
            store = get_factor_value_store()
            if not store.exists():
                return None
            df = store.load(
                factor_names=[name],
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
            )
            if df.empty:
                return None
            # 转换为因子矩阵：index=trade_date, columns=symbol, values=value
            df = df.drop_duplicates(subset=["symbol", "trade_date"], keep="last")
            matrix = df.pivot_table(
                index="trade_date", columns="symbol", values="value", aggfunc="last"
            )
            return matrix if not matrix.empty else None
        except Exception:
            return None

    def _load_return_matrix(
        self,
        symbols: list[str],
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        """加载收益率矩阵 — 下一日收益率

        load_daily returns a DataFrame with trade_date as the index
        (via set_index), so we use sort_index / .index rather than
        referencing a "trade_date" column.
        """
        store = get_market_data_store()
        sd = start_date or date(2000, 1, 1)
        ed = end_date or date.today()
        df = store.load_daily(symbols, sd, ed, columns=["symbol", "trade_date", "close"])
        if df.empty:
            return pd.DataFrame()

        return_matrix: dict[str, pd.Series] = {}
        for sym, grp in df.groupby("symbol"):
            grp = grp.sort_index()
            ret = grp["close"].pct_change().shift(-1)
            ret.index = grp.index
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

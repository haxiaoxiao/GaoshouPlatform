"""Independent factor research run storage and computation."""

from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import select

from app.db.models import Factor, FactorResearchRun, FactorResearchRunItem, WatchlistStock
from app.db.sqlite import async_session_factory
from app.models.factor import EvalConfig
from app.services.factor_evaluation import FactorEvaluationService
from app.services.factor_value_store import get_factor_definition, get_factor_value_store
from app.services.index_catalog import get_index_item
from app.services.index_components import load_index_symbols


DEFAULT_PARAMS: dict[str, Any] = {
    "portfolio_type": "long_only",
    "rebalance_period": "monthly",
    "fee_rate": 0.001,
    "slippage": 0.001,
    "filter_limit_up": True,
    "filter_limit_down": True,
    "group_count": 5,
    "direction": "desc",
    "industry_neutralization": False,
    "standardize": False,
}

_LATEST_SUMMARY_CACHE: dict[tuple[str, ...], tuple[float, dict[str, dict[str, Any]]]] = {}
_LATEST_SUMMARY_CACHE_TTL_SECONDS = 60.0


def research_params_hash(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:20]


class FactorResearchRunService:
    @staticmethod
    def _clear_latest_summary_cache() -> None:
        _LATEST_SUMMARY_CACHE.clear()

    async def prepare(self, payload: dict[str, Any]) -> dict[str, Any]:
        params = self._normalized_params(payload)
        existing = await self.find_latest(
            factor_name=str(payload["factor_name"]),
            stock_pool_value=str(payload["stock_pool_value"]),
            params_hash=research_params_hash(params),
        )
        coverage = get_factor_value_store().coverage(
            str(payload["factor_name"]),
            start_date=date.fromisoformat(str(payload["start_date"])),
            end_date=date.fromisoformat(str(payload["end_date"])),
        )
        is_custom_factor = await self._is_custom_factor(str(payload["factor_name"]))
        has_cached_values = bool(coverage.get("total_rows"))
        return {
            "cache_hit": existing is not None,
            "latest_run": self._summary_from_model(existing) if existing else None,
            "params_hash": research_params_hash(params),
            "coverage": coverage,
            "can_run": has_cached_values or is_custom_factor,
            "message": None if has_cached_values or is_custom_factor else "内置因子缺少缓存，请先在因子缓存页完成预计算。",
        }

    async def run(self, payload: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
        factor_name = str(payload["factor_name"])
        stock_pool_value = str(payload["stock_pool_value"])
        params = self._normalized_params(payload)
        params_hash = research_params_hash(params)
        if not force:
            existing = await self.find_latest(factor_name=factor_name, stock_pool_value=stock_pool_value, params_hash=params_hash)
            if existing is not None:
                return self._detail_from_model(existing)

        run_id = f"fr-{uuid.uuid4().hex[:12]}"
        definition = get_factor_definition(factor_name) or {}
        stock_pool_type = "watchlist" if stock_pool_value.startswith("watchlist_") else "index"
        async with async_session_factory() as session:
            model = FactorResearchRun(
                run_id=run_id,
                factor_name=factor_name,
                factor_display_name=str(definition.get("display_name") or factor_name),
                stock_pool_type=stock_pool_type,
                stock_pool_value=stock_pool_value,
                start_date=date.fromisoformat(str(payload["start_date"])),
                end_date=date.fromisoformat(str(payload["end_date"])),
                params_hash=params_hash,
                params=params,
                status="running",
            )
            session.add(model)
            await session.commit()

        try:
            detail = await self._compute_detail(
                factor_name=factor_name,
                stock_pool_value=stock_pool_value,
                start_date=date.fromisoformat(str(payload["start_date"])),
                end_date=date.fromisoformat(str(payload["end_date"])),
                params=params,
            )
            summary = detail["summary"]
            async with async_session_factory() as session:
                result = await session.execute(select(FactorResearchRun).where(FactorResearchRun.run_id == run_id))
                model = result.scalar_one()
                model.status = "success"
                model.completed_at = datetime.now()
                model.summary = summary
                model.detail = detail
                await session.commit()
            self._clear_latest_summary_cache()
            return {"run_id": run_id, **detail, "status": "success", "created_at": model.created_at.isoformat(), "completed_at": model.completed_at.isoformat()}
        except Exception as exc:
            logger.exception("Factor research run failed: {}", factor_name)
            async with async_session_factory() as session:
                result = await session.execute(select(FactorResearchRun).where(FactorResearchRun.run_id == run_id))
                model = result.scalar_one()
                model.status = "failed"
                model.error_message = str(exc)
                model.completed_at = datetime.now()
                await session.commit()
            self._clear_latest_summary_cache()
            raise

    async def batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_run_id = f"frb-{uuid.uuid4().hex[:12]}"
        factor_names = [str(name) for name in payload.get("factor_names") or [] if str(name)]
        results: list[dict[str, Any]] = []
        async with async_session_factory() as session:
            for index, factor_name in enumerate(factor_names):
                session.add(FactorResearchRunItem(batch_run_id=batch_run_id, factor_name=factor_name, status="queued", sort_order=index))
            await session.commit()
        for factor_name in factor_names:
            item_payload = {**payload, "factor_name": factor_name}
            try:
                result = await self.run(item_payload, force=bool(payload.get("force")))
                status = "success"
                error = None
                run_id = result.get("run_id")
            except Exception as exc:
                status = "failed"
                error = str(exc)
                run_id = None
            async with async_session_factory() as session:
                row = (await session.execute(
                    select(FactorResearchRunItem)
                    .where(FactorResearchRunItem.batch_run_id == batch_run_id)
                    .where(FactorResearchRunItem.factor_name == factor_name)
                )).scalar_one()
                row.status = status
                row.error_message = error
                row.run_id = run_id
                row.completed_at = datetime.now()
                await session.commit()
            results.append({"factor_name": factor_name, "status": status, "run_id": run_id, "error_message": error})
        return {"batch_run_id": batch_run_id, "items": results}

    async def get(self, run_id: str) -> dict[str, Any] | None:
        async with async_session_factory() as session:
            row = (await session.execute(select(FactorResearchRun).where(FactorResearchRun.run_id == run_id))).scalar_one_or_none()
            return self._detail_from_model(row) if row else None

    async def find_latest(self, *, factor_name: str, stock_pool_value: str | None = None, params_hash: str | None = None) -> FactorResearchRun | None:
        async with async_session_factory() as session:
            stmt = (
                select(FactorResearchRun)
                .where(FactorResearchRun.factor_name == factor_name)
                .where(FactorResearchRun.status == "success")
            )
            if stock_pool_value:
                stmt = stmt.where(FactorResearchRun.stock_pool_value == stock_pool_value)
            if params_hash:
                stmt = stmt.where(FactorResearchRun.params_hash == params_hash)
            stmt = stmt.order_by(FactorResearchRun.completed_at.desc(), FactorResearchRun.created_at.desc()).limit(1)
            return (await session.execute(stmt)).scalar_one_or_none()

    async def latest_summaries(self, factor_names: list[str]) -> dict[str, dict[str, Any]]:
        if not factor_names:
            return {}
        cache_key = tuple(sorted({str(name) for name in factor_names}))
        cached = _LATEST_SUMMARY_CACHE.get(cache_key)
        now = time.monotonic()
        if cached and now - cached[0] <= _LATEST_SUMMARY_CACHE_TTL_SECONDS:
            return cached[1]
        summaries: dict[str, dict[str, Any]] = {}
        async with async_session_factory() as session:
            rows = (await session.execute(
                select(FactorResearchRun)
                .where(FactorResearchRun.factor_name.in_(factor_names))
                .where(FactorResearchRun.status == "success")
                .order_by(FactorResearchRun.factor_name, FactorResearchRun.completed_at.desc())
            )).scalars().all()
        for row in rows:
            if row.factor_name not in summaries:
                summaries[row.factor_name] = self._summary_from_model(row)
        _LATEST_SUMMARY_CACHE[cache_key] = (now, summaries)
        return summaries

    def _normalized_params(self, payload: dict[str, Any]) -> dict[str, Any]:
        params = {**DEFAULT_PARAMS, **dict(payload.get("params") or {})}
        for key in DEFAULT_PARAMS:
            if key in payload:
                params[key] = payload[key]
        for key in ["factor_name", "stock_pool_value", "start_date", "end_date"]:
            params[key] = str(payload[key])
        return params

    async def _is_custom_factor(self, factor_name: str) -> bool:
        async with async_session_factory() as session:
            return (await session.execute(select(Factor.id).where(Factor.name == factor_name))).scalar_one_or_none() is not None

    async def _resolve_symbols(self, stock_pool_value: str, start_date: date, end_date: date) -> list[str]:
        if stock_pool_value.startswith("watchlist_"):
            group_id = int(stock_pool_value.split("_", 1)[1])
            async with async_session_factory() as session:
                rows = (await session.execute(
                    select(WatchlistStock.symbol).where(WatchlistStock.group_id == group_id)
                )).scalars().all()
            return sorted({str(symbol) for symbol in rows})
        item = get_index_item(stock_pool_value)
        if item is None or not item.pool_enabled:
            raise ValueError(f"股票池不可用或缺少严格历史成分: {stock_pool_value}")
        return await load_index_symbols(item.symbol, start_date, end_date)

    async def _load_factor_matrix(self, factor_name: str, symbols: list[str], start_date: date, end_date: date, params: dict[str, Any]) -> pd.DataFrame:
        store = get_factor_value_store()
        if store.exists():
            df = store.load(factor_names=[factor_name], start_date=start_date, end_date=end_date, symbols=symbols)
            if not df.empty:
                df = df.drop_duplicates(subset=["symbol", "trade_date"], keep="last")
                matrix = df.pivot_table(index="trade_date", columns="symbol", values="value", aggfunc="last")
                if not matrix.empty:
                    matrix.index = pd.to_datetime(matrix.index).normalize()
                    return matrix

        async with async_session_factory() as session:
            factor = (await session.execute(select(Factor).where(Factor.name == factor_name))).scalar_one_or_none()
        if factor is None:
            raise ValueError("内置因子缺少缓存，请先在因子缓存页完成预计算。")

        from app.api.factors import _compute_factor_rows, _factor_metadata, _rows_to_factor_matrix
        meta = _factor_metadata(factor)
        if meta["source_type"] == "python":
            from app.services.python_factor_runner import run_python_factor
            result = run_python_factor(
                code=meta["expression"],
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                params=params,
                stock_pool=str(params.get("stock_pool_value") or ""),
                benchmark="000905.SH",
            )
            if result["errors"]:
                raise ValueError("; ".join(result["errors"]))
            matrix = _rows_to_factor_matrix(result["rows"])
        else:
            rows = await _compute_factor_rows(expression=meta["expression"], symbols=symbols, start_date=start_date, end_date=end_date)
            matrix = _rows_to_factor_matrix(rows)
        if matrix.empty:
            raise ValueError("自定义因子没有生成有效结果。")
        matrix.index = pd.to_datetime(matrix.index).normalize()
        return matrix

    async def _compute_detail(self, *, factor_name: str, stock_pool_value: str, start_date: date, end_date: date, params: dict[str, Any]) -> dict[str, Any]:
        symbols = await self._resolve_symbols(stock_pool_value, start_date, end_date)
        if not symbols:
            raise ValueError("股票池没有可用股票。")
        evaluator = FactorEvaluationService()
        factor_df = await self._load_factor_matrix(factor_name, symbols, start_date, end_date, params)
        return_df = evaluator._load_return_matrix(symbols, start_date, end_date)
        factor_df, return_df = self._align_frames(factor_df, return_df)
        if factor_df.empty or return_df.empty:
            raise ValueError("因子或收益矩阵为空。")
        eval_config = EvalConfig(group_count=int(params.get("group_count") or 5))
        ic_points = evaluator._compute_ic_series(factor_df, return_df, eval_config)
        industry_ic = evaluator._compute_industry_ic(factor_df, return_df)
        turnover = evaluator._compute_turnover(factor_df)
        signal_decay = evaluator._compute_signal_decay(factor_df, return_df)
        quantile = self._quantile_nav(factor_df, return_df, params)
        latest = factor_df.loc[factor_df.index.max()].dropna()
        top = [{"symbol": str(k), "value": float(v)} for k, v in latest.sort_values(ascending=False).head(20).items()]
        bottom = [{"symbol": str(k), "value": float(v)} for k, v in latest.sort_values(ascending=True).head(20).items()]
        ic_values = [float(value) for _, value in ic_points if value is not None and math.isfinite(float(value))]
        ic_mean = float(np.mean(ic_values)) if ic_values else 0.0
        ic_std = float(np.std(ic_values)) if len(ic_values) > 1 else 0.0
        icir = ic_mean / ic_std if ic_std > 0 else 0.0
        summary = {
            "symbol_count": len(symbols),
            "coverage_ratio": self._coverage_ratio(factor_df, symbols),
            "ic_mean": round(ic_mean, 4),
            "ic_std": round(ic_std, 4),
            "icir": round(icir, 4),
            "abs_ic_gt_002_ratio": round(sum(abs(v) > 0.02 for v in ic_values) / len(ic_values), 4) if ic_values else 0.0,
            "long_short_return": quantile["summary"].get("long_short_return", 0.0),
            "max_drawdown": quantile["summary"].get("max_drawdown", 0.0),
            "sharpe": quantile["summary"].get("sharpe", 0.0),
            "turnover": self._mean_turnover(turnover),
        }
        definition = get_factor_definition(factor_name) or {}
        return {
            "factor_name": factor_name,
            "factor_display_name": str(definition.get("display_name") or factor_name),
            "stock_pool_value": stock_pool_value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "params": params,
            "summary": summary,
            "ic_series": [{"date": str(day), "value": value} for day, value in ic_points],
            "industry_ic": [{"industry": industry, "value": value} for industry, value in industry_ic],
            "turnover": [{"date": str(day), "min_quantile": mn, "max_quantile": mx} for day, mn, mx in turnover],
            "signal_decay": [{"lag": lag, "min_quantile": mn, "max_quantile": mx} for lag, mn, mx in signal_decay],
            "quantile_nav": quantile["series"],
            "quantile_summary": quantile["summary"],
            "top": top,
            "bottom": bottom,
            "logs": [f"因子研究完成: {factor_name}, 股票数 {len(symbols)}"],
        }

    @staticmethod
    def _align_frames(factor_df: pd.DataFrame, return_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        factor_df = factor_df.copy()
        return_df = return_df.copy()
        factor_df.index = pd.to_datetime(factor_df.index).normalize()
        return_df.index = pd.to_datetime(return_df.index).normalize()
        dates = factor_df.index.intersection(return_df.index)
        symbols = factor_df.columns.intersection(return_df.columns)
        return factor_df.loc[dates, symbols], return_df.loc[dates, symbols]

    @staticmethod
    def _coverage_ratio(factor_df: pd.DataFrame, symbols: list[str]) -> float:
        possible = max(1, len(factor_df.index) * max(1, len(symbols)))
        return round(float(factor_df.notna().sum().sum()) / possible, 4)

    @staticmethod
    def _mean_turnover(turnover: list[tuple]) -> float:
        values = [float((mn + mx) / 2) for _, mn, mx in turnover if mn is not None and mx is not None]
        return round(float(np.mean(values)), 4) if values else 0.0

    def _quantile_nav(self, factor_df: pd.DataFrame, return_df: pd.DataFrame, params: dict[str, Any]) -> dict[str, Any]:
        group_count = int(params.get("group_count") or 5)
        direction = str(params.get("direction") or "desc")
        fee = float(params.get("fee_rate") or 0.0) + float(params.get("slippage") or 0.0)
        series_by_group: dict[str, list[dict[str, Any]]] = {f"q{i + 1}": [] for i in range(group_count)}
        nav_values = {key: 1.0 for key in series_by_group}
        ls_nav = 1.0
        long_short_series: list[dict[str, Any]] = []
        ls_returns: list[float] = []
        for day in factor_df.index:
            factors = factor_df.loc[day].dropna()
            returns = return_df.loc[day].dropna()
            common = factors.index.intersection(returns.index)
            if len(common) < group_count * 2:
                continue
            factors = factors.loc[common]
            try:
                groups = pd.qcut(factors, group_count, labels=False, duplicates="drop")
            except ValueError:
                continue
            group_returns: dict[int, float] = {}
            for idx in range(group_count):
                names = factors[groups == idx].index
                group_return = float(returns.loc[names].mean()) if len(names) else 0.0
                group_return -= fee / 252.0
                nav_values[f"q{idx + 1}"] *= 1 + group_return
                group_returns[idx] = group_return
                series_by_group[f"q{idx + 1}"].append({"date": str(day.date()), "value": nav_values[f"q{idx + 1}"]})
            best_idx = group_count - 1 if direction == "desc" else 0
            worst_idx = 0 if direction == "desc" else group_count - 1
            ls_ret = group_returns.get(best_idx, 0.0) - group_returns.get(worst_idx, 0.0) - fee / 126.0
            ls_nav *= 1 + ls_ret
            ls_returns.append(ls_ret)
            long_short_series.append({"date": str(day.date()), "value": ls_nav})
        returns_s = pd.Series(ls_returns, dtype=float)
        max_drawdown = self._max_drawdown([item["value"] for item in long_short_series])
        return {
            "series": {"groups": series_by_group, "long_short": long_short_series},
            "summary": {
                "long_short_return": round(ls_nav - 1, 4) if long_short_series else 0.0,
                "max_drawdown": round(max_drawdown, 4),
                "sharpe": round(float(returns_s.mean() / returns_s.std() * np.sqrt(252)), 4) if len(returns_s) > 1 and returns_s.std() > 0 else 0.0,
            },
        }

    @staticmethod
    def _max_drawdown(nav_values: list[float]) -> float:
        if not nav_values:
            return 0.0
        series = pd.Series(nav_values, dtype=float)
        drawdown = series / series.cummax() - 1
        return abs(float(drawdown.min()))

    def _summary_from_model(self, row: FactorResearchRun | None) -> dict[str, Any] | None:
        if row is None:
            return None
        summary = dict(row.summary or {})
        return {
            "run_id": row.run_id,
            "factor_name": row.factor_name,
            "factor_display_name": row.factor_display_name,
            "stock_pool_value": row.stock_pool_value,
            "start_date": row.start_date.isoformat(),
            "end_date": row.end_date.isoformat(),
            "status": row.status,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            **summary,
        }

    def _detail_from_model(self, row: FactorResearchRun) -> dict[str, Any]:
        detail = dict(row.detail or {})
        return {
            "run_id": row.run_id,
            "status": row.status,
            "error_message": row.error_message,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            **detail,
        }


factor_research_run_service = FactorResearchRunService()

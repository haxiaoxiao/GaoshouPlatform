"""Independent factor research run storage and computation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import sqlite3
import time
import uuid
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger
from sqlalchemy import select

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.db.models import Factor, FactorResearchRun, FactorResearchRunItem, WatchlistStock
from app.db.sqlite import async_session_factory
from app.models.factor import EvalConfig
from app.services.benchmark_series import (
    DEFAULT_BENCHMARK_SYMBOL,
    benchmark_display_name,
    excess_nav_points,
    nav_points,
    resolve_benchmark_symbol,
    returns_to_nav_series,
)
from app.services.factor_evaluation import FactorEvaluationService
from app.services.factor_value_store import get_factor_definition, get_factor_value_store
from app.services.index_catalog import get_index_item
from app.services.index_components import load_index_symbols

DEFAULT_PARAMS: dict[str, Any] = {
    "portfolio_type": "long_only",
    "rebalance_period": "monthly",
    "fee_rate": 0.001,
    "stamp_tax_rate": 0.0,
    "transfer_fee_rate": 0.0,
    "slippage": 0.001,
    "filter_limit_up": True,
    "filter_limit_down": True,
    "group_count": 5,
    "direction": "desc",
    "pool_membership_mode": "static_latest",
    "factor_value_params_hash": None,
    "factor_value_params_hashes": {},
    "outlier_handling": "none",
    "industry_neutralization": False,
    "standardize": False,
    "benchmark_symbol": DEFAULT_BENCHMARK_SYMBOL,
}

_LATEST_SUMMARY_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, dict[str, Any]]]] = {}
_LATEST_SUMMARY_CACHE_TTL_SECONDS = 60.0
_RESEARCH_MATCH_CONTEXT_KEYS = frozenset({"factor_name", "stock_pool_value"})
_RESEARCH_MATCH_DATE_KEYS = frozenset({"start_date", "end_date"})
_COMBINATION_FACET_FIELDS = (
    "factor_value_params_hash",
    "stock_pool_value",
    "date_range",
    "portfolio_type",
    "benchmark_symbol",
    "fee_profile",
    "pool_membership_mode",
    "group_count",
    "direction",
    "filter_limit_up",
    "filter_limit_down",
    "rebalance_period",
    "outlier_handling",
    "industry_neutralization",
    "standardize",
)


def research_params_hash(payload: dict[str, Any], *, exclude_keys: set[str] | frozenset[str] | None = None) -> str:
    excluded = set(exclude_keys or ())
    normalized = {
        str(key): payload[key]
        for key in sorted(payload)
        if key not in excluded
    }
    text = json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:20]


def research_match_hash(payload: dict[str, Any], *, ignore_date_range: bool = False) -> str:
    exclude = set(_RESEARCH_MATCH_CONTEXT_KEYS)
    if ignore_date_range:
        exclude.update(_RESEARCH_MATCH_DATE_KEYS)
    return research_params_hash(payload, exclude_keys=exclude)


class FactorResearchRunService:
    @staticmethod
    def _clear_latest_summary_cache() -> None:
        _LATEST_SUMMARY_CACHE.clear()

    async def prepare(self, payload: dict[str, Any]) -> dict[str, Any]:
        params = self._normalized_params(payload)
        start_date = date.fromisoformat(str(payload["start_date"]))
        end_date = date.fromisoformat(str(payload["end_date"]))
        stock_pool_value = str(payload["stock_pool_value"])
        symbols: list[str] = []
        pool_error: str | None = None
        try:
            symbols = await self._resolve_symbols(stock_pool_value, start_date, end_date)
        except Exception as exc:
            pool_error = str(exc)
        existing = await self.find_latest(
            factor_name=str(payload["factor_name"]),
            stock_pool_value=stock_pool_value,
            params_hash=research_params_hash(params),
        )
        factor_value_params_hash = self._factor_value_params_hash_for(str(payload["factor_name"]), params)
        coverage = await asyncio.to_thread(
            get_factor_value_store().coverage,
            str(payload["factor_name"]),
            start_date=start_date,
            end_date=end_date,
            symbols=symbols or None,
            params_hash=factor_value_params_hash,
            include_symbols_sample=False,
        )
        is_custom_factor = await self._is_custom_factor(str(payload["factor_name"]))
        effective_range = self._coverage_effective_range(coverage, start_date, end_date)
        max_cached_date = str(coverage.get("max_date") or "")
        has_cached_values = bool(coverage.get("total_rows")) and bool(symbols) and effective_range is not None
        if pool_error:
            message = pool_error
        elif is_custom_factor:
            message = None
        elif has_cached_values and effective_range and effective_range[1] < end_date:
            message = (
                f"Factor cache is available through {effective_range[1].isoformat()}; "
                f"research will use {effective_range[0].isoformat()} to {effective_range[1].isoformat()} "
                f"and skip uncached tail dates through {end_date.isoformat()}."
            )
        elif has_cached_values:
            message = None
        else:
            message = (
                "内置因子缺少当前股票池/区间的缓存。"
                f"股票池 {stock_pool_value} 解析到 {len(symbols)} 只，"
                f"当前覆盖 {int(coverage.get('symbol_count') or 0)} 只，"
                f"缓存日期至 {max_cached_date or '无'}，目标至 {end_date.isoformat()}；"
                "请先在因子缓存页完成预计算。"
            )
        return {
            "cache_hit": existing is not None,
            "latest_run": self._summary_from_model(existing) if existing else None,
            "params_hash": research_params_hash(params),
            "coverage": coverage,
            "effective_start_date": effective_range[0].isoformat() if effective_range else None,
            "effective_end_date": effective_range[1].isoformat() if effective_range else None,
            "is_clipped_to_cache": bool(effective_range and (effective_range[0] > start_date or effective_range[1] < end_date)),
            "can_run": has_cached_values or is_custom_factor,
            "message": message,
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
                summary = dict(result.get("summary") or {})
            except Exception as exc:
                status = "failed"
                error = str(exc)
                run_id = None
                summary = {}
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
            results.append({
                "factor_name": factor_name,
                "status": status,
                "run_id": run_id,
                "error_message": error,
                "summary": summary or None,
                "ic_mean": summary.get("ic_mean"),
                "icir": summary.get("icir"),
            })
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

    async def combinations(self, payload: dict[str, Any]) -> dict[str, Any]:
        factor_names = sorted({str(name) for name in payload.get("factor_names") or [] if str(name)})
        limit = max(1, min(int(payload.get("limit") or 200), 500))
        selection = {
            str(key): value
            for key, value in dict(payload.get("selection") or {}).items()
            if value is not None and value != ""
        }
        if not factor_names:
            return {
                "factor_count": 0,
                "total_candidates": 0,
                "candidates": [],
                "combo_groups": [],
                "facets": {},
                "selection": selection,
            }

        rows = await self._list_success_rows(factor_names)
        candidates = [self._combination_candidate(row) for row in rows]
        candidates = [candidate for candidate in candidates if self._combination_matches_selection(candidate, selection)]
        combo_groups = self._combination_groups(candidates, factor_names)
        facets = self._combination_facets(candidates, factor_names)
        return {
            "factor_count": len(factor_names),
            "total_candidates": len(candidates),
            "candidates": candidates[:limit],
            "combo_groups": combo_groups[:limit],
            "facets": facets,
            "selection": selection,
        }

    async def latest_summaries(
        self,
        factor_names: list[str],
        *,
        stock_pool_value: str | None = None,
        params_hash_by_factor: dict[str, str] | None = None,
        match_hash_by_factor: dict[str, str] | None = None,
        requested_range_by_factor: dict[str, tuple[date, date]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        if not factor_names:
            return {}
        clean_names = sorted({str(name) for name in factor_names})
        hash_key = tuple(sorted((params_hash_by_factor or {}).items()))
        match_key = tuple(sorted((match_hash_by_factor or {}).items()))
        range_key = tuple(
            sorted(
                (name, bounds[0].isoformat(), bounds[1].isoformat())
                for name, bounds in (requested_range_by_factor or {}).items()
            )
        )
        cache_key = (tuple(clean_names), stock_pool_value or "", hash_key, match_key, range_key)
        cached = _LATEST_SUMMARY_CACHE.get(cache_key)
        now = time.monotonic()
        if cached and now - cached[0] <= _LATEST_SUMMARY_CACHE_TTL_SECONDS:
            return cached[1]
        summaries: dict[str, dict[str, Any]] = {}
        async with async_session_factory() as session:
            stmt = (
                select(FactorResearchRun)
                .where(FactorResearchRun.factor_name.in_(clean_names))
                .where(FactorResearchRun.status == "success")
            )
            if stock_pool_value:
                stmt = stmt.where(FactorResearchRun.stock_pool_value == stock_pool_value)
            if params_hash_by_factor:
                stmt = stmt.where(FactorResearchRun.params_hash.in_(set(params_hash_by_factor.values())))
            rows = (await session.execute(
                stmt.order_by(FactorResearchRun.factor_name, FactorResearchRun.completed_at.desc())
            )).scalars().all()
        for row in rows:
            if params_hash_by_factor and row.params_hash != params_hash_by_factor.get(row.factor_name):
                continue
            if row.factor_name not in summaries:
                summaries[row.factor_name] = self._summary_from_model(row)
        if match_hash_by_factor:
            fallback = await self._latest_summaries_by_match_hash(
                clean_names,
                stock_pool_value=stock_pool_value,
                match_hash_by_factor=match_hash_by_factor,
                requested_range_by_factor=requested_range_by_factor or {},
                skip_factor_names=set(summaries),
            )
            for factor_name, summary in fallback.items():
                summaries.setdefault(factor_name, summary)
        _LATEST_SUMMARY_CACHE[cache_key] = (now, summaries)
        return summaries

    async def _latest_summaries_by_match_hash(
        self,
        factor_names: list[str],
        *,
        stock_pool_value: str | None,
        match_hash_by_factor: dict[str, str],
        requested_range_by_factor: dict[str, tuple[date, date]],
        skip_factor_names: set[str],
    ) -> dict[str, dict[str, Any]]:
        candidate_rows = await self._list_success_rows(factor_names, stock_pool_value=stock_pool_value)
        matched: dict[str, dict[str, Any]] = {}
        for factor_name in factor_names:
            if factor_name in skip_factor_names:
                continue
            requested_match_hash = match_hash_by_factor.get(factor_name)
            if not requested_match_hash:
                continue
            factor_candidates = [
                row for row in candidate_rows
                if row.factor_name == factor_name
                and self._row_match_hash(row) == requested_match_hash
            ]
            if not factor_candidates:
                continue
            best_row = self._select_best_compatible_run(
                factor_candidates,
                requested_range=requested_range_by_factor.get(factor_name),
            )
            if best_row is not None:
                matched[factor_name] = self._summary_from_model(best_row)
        return matched

    async def _list_success_rows(
        self,
        factor_names: list[str],
        *,
        stock_pool_value: str | None = None,
    ) -> list[FactorResearchRun]:
        async with async_session_factory() as session:
            stmt = (
                select(FactorResearchRun)
                .where(FactorResearchRun.factor_name.in_(factor_names))
                .where(FactorResearchRun.status == "success")
            )
            if stock_pool_value:
                stmt = stmt.where(FactorResearchRun.stock_pool_value == stock_pool_value)
            rows = (await session.execute(
                stmt.order_by(FactorResearchRun.factor_name, FactorResearchRun.completed_at.desc(), FactorResearchRun.created_at.desc())
            )).scalars().all()
        return rows

    def _row_match_hash(self, row: FactorResearchRun) -> str:
        params = dict(row.params or {})
        if "factor_name" not in params:
            params["factor_name"] = row.factor_name
        if "stock_pool_value" not in params:
            params["stock_pool_value"] = row.stock_pool_value
        if "start_date" not in params and row.start_date is not None:
            params["start_date"] = row.start_date.isoformat()
        if "end_date" not in params and row.end_date is not None:
            params["end_date"] = row.end_date.isoformat()
        return research_match_hash(params, ignore_date_range=True)

    @staticmethod
    def _select_best_compatible_run(
        rows: list[FactorResearchRun],
        *,
        requested_range: tuple[date, date] | None = None,
    ) -> FactorResearchRun | None:
        if not rows:
            return None
        if requested_range is None:
            return rows[0]

        requested_start, requested_end = requested_range

        def score(row: FactorResearchRun) -> tuple[int, int, float]:
            row_start = row.start_date
            row_end = row.end_date
            overlap_days = max(0, (min(row_end, requested_end) - max(row_start, requested_start)).days + 1)
            requested_span_days = max(1, (requested_end - requested_start).days + 1)
            clipped_days = (
                max(0, (requested_start - row_start).days) +
                max(0, (row_end - requested_end).days) +
                max(0, (requested_end - row_end).days) +
                max(0, (row_start - requested_start).days)
            )
            completed_ts = row.completed_at.timestamp() if row.completed_at else row.created_at.timestamp()
            full_cover = 1 if row_start <= requested_start and row_end >= requested_end else 0
            overlap_ratio = overlap_days / requested_span_days
            return (full_cover, overlap_days, overlap_ratio - clipped_days / max(requested_span_days, 1) + completed_ts / 1_000_000_000)

        return max(rows, key=score)

    def _combination_candidate(self, row: FactorResearchRun) -> dict[str, Any]:
        params = dict(row.params or {})
        settings = self._combination_settings(row, params)
        summary = dict(row.summary or {})
        return {
            "combo_id": self._combination_id(settings),
            "run_id": row.run_id,
            "factor_name": row.factor_name,
            "factor_display_name": row.factor_display_name,
            "research_params_hash": row.params_hash,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "settings": settings,
            "summary": summary or None,
            "ic_mean": summary.get("ic_mean"),
            "icir": summary.get("icir"),
            "factor_value_params_hash": settings.get("factor_value_params_hash"),
        }

    def _combination_settings(self, row: FactorResearchRun, params: dict[str, Any]) -> dict[str, Any]:
        start_date = str(params.get("start_date") or row.start_date.isoformat())
        end_date = str(params.get("end_date") or row.end_date.isoformat())
        fee_rate = self._combo_float(params.get("fee_rate"), 0.0)
        stamp_tax_rate = self._combo_float(params.get("stamp_tax_rate"), 0.0)
        transfer_fee_rate = self._combo_float(params.get("transfer_fee_rate"), 0.0)
        slippage = self._combo_float(params.get("slippage"), 0.0)
        return {
            "stock_pool_value": str(params.get("stock_pool_value") or row.stock_pool_value),
            "start_date": start_date,
            "end_date": end_date,
            "date_range": f"{start_date}..{end_date}",
            "portfolio_type": str(params.get("portfolio_type") or "long_only"),
            "rebalance_period": str(params.get("rebalance_period") or "monthly"),
            "fee_rate": fee_rate,
            "stamp_tax_rate": stamp_tax_rate,
            "transfer_fee_rate": transfer_fee_rate,
            "slippage": slippage,
            "fee_profile": self._fee_profile(fee_rate, stamp_tax_rate, transfer_fee_rate, slippage),
            "benchmark_symbol": str(params.get("benchmark_symbol") or DEFAULT_BENCHMARK_SYMBOL),
            "filter_limit_up": bool(params.get("filter_limit_up", True)),
            "filter_limit_down": bool(params.get("filter_limit_down", True)),
            "group_count": int(params.get("group_count") or 5),
            "direction": str(params.get("direction") or "desc"),
            "pool_membership_mode": str(params.get("pool_membership_mode") or "static_latest"),
            "factor_value_params_hash": str(params.get("factor_value_params_hash") or ""),
            "outlier_handling": str(params.get("outlier_handling") or "none"),
            "industry_neutralization": bool(params.get("industry_neutralization", False)),
            "standardize": bool(params.get("standardize", False)),
        }

    @staticmethod
    def _combo_float(value: Any, default: float) -> float:
        try:
            return round(float(value), 8)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _fee_profile(fee_rate: float, stamp_tax_rate: float, transfer_fee_rate: float, slippage: float) -> str:
        return "|".join(
            f"{value:.8f}".rstrip("0").rstrip(".") or "0"
            for value in (fee_rate, stamp_tax_rate, transfer_fee_rate, slippage)
        )

    @staticmethod
    def _combination_id(settings: dict[str, Any]) -> str:
        return research_params_hash(settings)

    @staticmethod
    def _combination_matches_selection(candidate: dict[str, Any], selection: dict[str, Any]) -> bool:
        settings = candidate.get("settings") or {}
        for key, selected in selection.items():
            if key == "factor_name":
                if str(candidate.get("factor_name") or "") != str(selected):
                    return False
                continue
            if key == "params_hash":
                if str(candidate.get("research_params_hash") or "") != str(selected):
                    return False
                continue
            value = settings.get(key)
            if isinstance(value, bool):
                selected_value = selected if isinstance(selected, bool) else str(selected).lower() == "true"
                if value != selected_value:
                    return False
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    if float(value) != float(selected):
                        return False
                except (TypeError, ValueError):
                    return False
            elif str(value) != str(selected):
                return False
        return True

    def _combination_groups(self, candidates: list[dict[str, Any]], factor_names: list[str]) -> list[dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        total_factor_count = len(set(factor_names))
        for candidate in candidates:
            settings = dict(candidate.get("settings") or {})
            combo_id = str(candidate.get("combo_id") or self._combination_id(settings))
            group = groups.setdefault(
                combo_id,
                {
                    "combo_id": combo_id,
                    "settings": settings,
                    "factor_names": [],
                    "run_ids": {},
                    "factor_value_params_hashes": {},
                    "metrics_by_factor": {},
                    "latest_completed_at": None,
                    "covered_factor_count": 0,
                    "total_factor_count": total_factor_count,
                },
            )
            factor_name = str(candidate.get("factor_name") or "")
            if factor_name and factor_name not in group["factor_names"]:
                group["factor_names"].append(factor_name)
            if factor_name:
                group["run_ids"][factor_name] = candidate.get("run_id")
                group["factor_value_params_hashes"][factor_name] = candidate.get("factor_value_params_hash") or ""
                group["metrics_by_factor"][factor_name] = {
                    "ic_mean": candidate.get("ic_mean"),
                    "icir": candidate.get("icir"),
                }
            completed_at = candidate.get("completed_at")
            if completed_at and (group["latest_completed_at"] is None or completed_at > group["latest_completed_at"]):
                group["latest_completed_at"] = completed_at
        for group in groups.values():
            group["factor_names"] = sorted(group["factor_names"])
            group["covered_factor_count"] = len(group["factor_names"])
        return sorted(
            groups.values(),
            key=lambda item: (int(item["covered_factor_count"]), str(item.get("latest_completed_at") or "")),
            reverse=True,
        )

    def _combination_facets(self, candidates: list[dict[str, Any]], factor_names: list[str]) -> dict[str, list[dict[str, Any]]]:
        facets: dict[str, list[dict[str, Any]]] = {}
        total_factor_count = len(set(factor_names))
        for field in _COMBINATION_FACET_FIELDS:
            buckets: dict[str, dict[str, Any]] = {}
            for candidate in candidates:
                settings = candidate.get("settings") or {}
                value = settings.get(field)
                if value is None or value == "":
                    continue
                key = str(value)
                bucket = buckets.setdefault(
                    key,
                    {
                        "field": field,
                        "value": value,
                        "label": self._combination_facet_label(field, value, settings),
                        "count": 0,
                        "factor_names": set(),
                        "covered_factor_count": 0,
                        "total_factor_count": total_factor_count,
                    },
                )
                bucket["count"] += 1
                factor_name = str(candidate.get("factor_name") or "")
                if factor_name:
                    bucket["factor_names"].add(factor_name)
            options: list[dict[str, Any]] = []
            for bucket in buckets.values():
                factors = sorted(bucket.pop("factor_names"))
                bucket["factor_names"] = factors
                bucket["covered_factor_count"] = len(factors)
                options.append(bucket)
            facets[field] = sorted(
                options,
                key=lambda item: (int(item["covered_factor_count"]), int(item["count"]), str(item["label"])),
                reverse=True,
            )
        return facets

    @staticmethod
    def _combination_facet_label(field: str, value: Any, settings: dict[str, Any]) -> str:
        if field == "date_range":
            return f"{settings.get('start_date')} ~ {settings.get('end_date')}"
        if field == "benchmark_symbol":
            return benchmark_display_name(str(value)) or str(value)
        if field == "fee_profile":
            return (
                f"佣金 {settings.get('fee_rate', 0):g} / "
                f"印花税 {settings.get('stamp_tax_rate', 0):g} / "
                f"过户费 {settings.get('transfer_fee_rate', 0):g} / "
                f"滑点 {settings.get('slippage', 0):g}"
            )
        if isinstance(value, bool):
            return "是" if value else "否"
        return str(value)

    def _normalized_params(self, payload: dict[str, Any]) -> dict[str, Any]:
        params = {**DEFAULT_PARAMS, **dict(payload.get("params") or {})}
        for key in DEFAULT_PARAMS:
            if key in payload:
                params[key] = payload[key]
        for key in ["factor_name", "stock_pool_value", "start_date", "end_date"]:
            params[key] = str(payload[key])
        factor_name = str(params.get("factor_name") or "")
        raw_hashes = payload.get("factor_value_params_hashes") or params.get("factor_value_params_hashes") or {}
        selected_hash = None
        if isinstance(raw_hashes, dict) and factor_name:
            selected_hash = raw_hashes.get(factor_name)
        selected_hash = selected_hash or payload.get("factor_value_params_hash") or params.get("factor_value_params_hash")
        if selected_hash:
            selected_hash = str(selected_hash)
            params["factor_value_params_hash"] = selected_hash
            params["factor_value_params_hashes"] = {factor_name: selected_hash} if factor_name else {}
        else:
            params["factor_value_params_hash"] = None
            params["factor_value_params_hashes"] = {}
        return params

    @staticmethod
    def _factor_value_params_hash_for(factor_name: str, params: dict[str, Any]) -> str | None:
        hashes = params.get("factor_value_params_hashes") or {}
        if isinstance(hashes, dict):
            value = hashes.get(factor_name)
            if value:
                return str(value)
        value = params.get("factor_value_params_hash")
        return str(value) if value else None

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
            df = store.load(
                factor_names=[factor_name],
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
                params_hash=self._factor_value_params_hash_for(factor_name, params),
            )
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
                benchmark=resolve_benchmark_symbol(params.get("benchmark_symbol")),
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
        requested_eligible_trading_days = int(return_df.notna().any(axis=1).sum()) if not return_df.empty else 0
        factor_df, return_df = self._align_frames(factor_df, return_df, symbols)
        membership_mask = await self._load_pool_membership_mask(
            stock_pool_value,
            factor_df.index,
            factor_df.columns,
            mode=str(params.get("pool_membership_mode") or "static_latest"),
        )
        if membership_mask is not None:
            factor_df = factor_df.where(membership_mask)
            return_df = return_df.where(membership_mask)
        factor_df, return_df, membership_mask = self._trim_to_effective_factor_dates(
            factor_df,
            return_df,
            membership_mask,
        )
        if factor_df.empty or return_df.empty:
            raise ValueError("No overlapping cached factor values and return data are available in the requested range.")
        factor_df, preprocess_logs = await self._preprocess_factor_matrix(factor_df, params)
        factor_df, return_df, membership_mask = self._drop_empty_factor_dates(
            factor_df,
            return_df,
            membership_mask,
        )
        if factor_df.empty or return_df.empty or not factor_df.notna().any().any():
            raise ValueError("Factor preprocessing removed all valid values for the requested range.")
        effective_start_date = factor_df.index.min().date()
        effective_end_date = factor_df.index.max().date()
        eval_config = EvalConfig(group_count=int(params.get("group_count") or 5))
        ic_points = evaluator._compute_ic_series(factor_df, return_df, eval_config)
        industry_ic = evaluator._compute_industry_ic(factor_df, return_df)
        turnover = evaluator._compute_turnover(factor_df)
        signal_decay = evaluator._compute_signal_decay(factor_df, return_df)
        quantile = self._quantile_nav(factor_df, return_df, params)
        benchmark_logs = await self._attach_benchmark_to_quantile(
            quantile,
            benchmark_symbol=resolve_benchmark_symbol(params.get("benchmark_symbol")),
            start_date=effective_start_date,
            end_date=effective_end_date,
        )
        latest = self._latest_non_empty_cross_section(factor_df)
        top = [{"symbol": str(k), "value": float(v)} for k, v in latest.sort_values(ascending=False).head(20).items()]
        bottom = [{"symbol": str(k), "value": float(v)} for k, v in latest.sort_values(ascending=True).head(20).items()]
        ic_values = [float(value) for _, value in ic_points if value is not None and math.isfinite(float(value))]
        ic_mean = float(np.mean(ic_values)) if ic_values else 0.0
        ic_std = float(np.std(ic_values)) if len(ic_values) > 1 else 0.0
        icir = ic_mean / ic_std if ic_std > 0 else 0.0
        summary = {
            "symbol_count": len(symbols),
            "active_symbol_count": self._active_symbol_count(membership_mask, len(symbols)),
            "coverage_ratio": self._coverage_ratio(factor_df, symbols, membership_mask),
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "effective_start_date": effective_start_date.isoformat(),
            "effective_end_date": effective_end_date.isoformat(),
            "effective_trading_days": int(len(factor_df.index)),
            "dropped_trading_days": int(max(0, requested_eligible_trading_days - len(factor_df.index))),
            "ic_mean": round(ic_mean, 4),
            "ic_std": round(ic_std, 4),
            "icir": round(icir, 4),
            "abs_ic_gt_002_ratio": round(sum(abs(v) > 0.02 for v in ic_values) / len(ic_values), 4) if ic_values else 0.0,
            "long_short_return": quantile["summary"].get("long_short_return", 0.0),
            "max_drawdown": quantile["summary"].get("max_drawdown", 0.0),
            "sharpe": quantile["summary"].get("sharpe", 0.0),
            "benchmark_symbol": quantile["summary"].get("benchmark_symbol"),
            "benchmark_name": quantile["summary"].get("benchmark_name"),
            "benchmark_return": quantile["summary"].get("benchmark_return", 0.0),
            "excess_long_short_return": quantile["summary"].get("excess_long_short_return", 0.0),
            "turnover": self._mean_turnover(turnover),
        }
        definition = get_factor_definition(factor_name) or {}
        return {
            "factor_name": factor_name,
            "factor_display_name": str(definition.get("display_name") or factor_name),
            "stock_pool_value": stock_pool_value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "effective_start_date": effective_start_date.isoformat(),
            "effective_end_date": effective_end_date.isoformat(),
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
            "logs": [*preprocess_logs, *benchmark_logs, f"因子研究完成: {factor_name}, 股票数 {len(symbols)}"],
        }

    @staticmethod
    def _coverage_effective_range(
        coverage: dict[str, Any],
        requested_start: date,
        requested_end: date,
    ) -> tuple[date, date] | None:
        try:
            min_date = date.fromisoformat(str(coverage.get("min_date")))
            max_date = date.fromisoformat(str(coverage.get("max_date")))
        except (TypeError, ValueError):
            return None
        effective_start = max(requested_start, min_date)
        effective_end = min(requested_end, max_date)
        if effective_end < effective_start:
            return None
        return effective_start, effective_end

    async def _preprocess_factor_matrix(
        self,
        factor_df: pd.DataFrame,
        params: dict[str, Any],
    ) -> tuple[pd.DataFrame, list[str]]:
        outlier_handling = str(params.get("outlier_handling") or "none").strip().lower()
        if outlier_handling not in {"none", "winsorize"}:
            raise ValueError(f"Unsupported outlier_handling: {outlier_handling}")

        industry_neutralization = bool(params.get("industry_neutralization", False))
        standardize = bool(params.get("standardize", False))
        if outlier_handling == "none" and not industry_neutralization and not standardize:
            return factor_df, []

        metadata = await self._load_factor_metadata(list(factor_df.columns)) if industry_neutralization else None
        processed = factor_df.apply(
            lambda row: self._preprocess_cross_section(
                row,
                metadata=metadata,
                outlier_handling=outlier_handling,
                industry_neutralization=industry_neutralization,
                standardize=standardize,
            ),
            axis=1,
        )
        processed.index = factor_df.index
        processed.columns = factor_df.columns

        steps = []
        if outlier_handling == "winsorize":
            steps.append("去极值")
        if industry_neutralization:
            steps.append("行业Z-Score" if standardize else "行业去均值")
        elif standardize:
            steps.append("横截面Z-Score")
        return processed, [f"因子预处理: {' + '.join(steps)}"] if steps else []

    @staticmethod
    def _preprocess_cross_section(
        row: pd.Series,
        *,
        metadata: pd.DataFrame | None,
        outlier_handling: str,
        industry_neutralization: bool,
        standardize: bool,
    ) -> pd.Series:
        values = pd.to_numeric(row, errors="coerce").astype("float64")
        if outlier_handling == "winsorize":
            values = FactorResearchRunService._winsorize_cross_section(values)
        if industry_neutralization:
            values = FactorResearchRunService._industry_adjust_cross_section(values, metadata, standardize)
        elif standardize:
            values = FactorResearchRunService._zscore_cross_section(values)
        return values.reindex(row.index)

    @staticmethod
    def _drop_empty_factor_dates(
        factor_df: pd.DataFrame,
        return_df: pd.DataFrame,
        membership_mask: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
        valid_dates = factor_df.notna().any(axis=1)
        factor_df = factor_df.loc[valid_dates]
        return_df = return_df.loc[valid_dates]
        if membership_mask is not None:
            membership_mask = membership_mask.loc[valid_dates]
        return factor_df, return_df, membership_mask

    @staticmethod
    def _winsorize_cross_section(values: pd.Series, limit: float = 0.025) -> pd.Series:
        valid = values.dropna()
        if len(valid) < 3:
            return values
        lower = valid.quantile(limit)
        upper = valid.quantile(1.0 - limit)
        if not np.isfinite(lower) or not np.isfinite(upper) or upper < lower:
            return values
        return values.clip(lower=lower, upper=upper)

    @staticmethod
    def _zscore_cross_section(values: pd.Series) -> pd.Series:
        valid = values.notna()
        std = values.std(ddof=0)
        if not np.isfinite(std) or std == 0:
            result = pd.Series(np.nan, index=values.index, dtype="float64")
            result.loc[valid] = 0.0
            return result
        return ((values - values.mean()) / std).astype("float64")

    @staticmethod
    def _industry_adjust_cross_section(
        values: pd.Series,
        metadata: pd.DataFrame | None,
        standardize: bool,
    ) -> pd.Series:
        if metadata is None or "industry" not in metadata.columns:
            raise ValueError("行业中性化需要 stocks.industry 元数据。")
        aligned = metadata.reindex(values.index)
        industry = aligned["industry"].replace("", np.nan)
        valid_industry = industry.notna()
        if not valid_industry.any():
            raise ValueError("行业中性化需要有效的 stocks.industry 元数据。")
        adjusted = pd.Series(np.nan, index=values.index, dtype="float64")
        grouped = values[valid_industry].groupby(industry[valid_industry])
        mean = grouped.transform("mean")
        adjusted.loc[valid_industry] = values[valid_industry] - mean
        if not standardize:
            return adjusted.astype("float64")
        std = grouped.transform(lambda item: item.std(ddof=0))
        zero_std_mask = std.fillna(0).eq(0)
        standardized = adjusted.loc[valid_industry] / std.replace(0, np.nan)
        if zero_std_mask.any():
            standardized.loc[zero_std_mask & values[valid_industry].notna()] = 0.0
        adjusted.loc[valid_industry] = standardized
        return adjusted.astype("float64")

    async def _load_factor_metadata(self, symbols: list[str]) -> pd.DataFrame:
        ordered_symbols = list(dict.fromkeys(str(symbol) for symbol in symbols))
        if not ordered_symbols:
            return pd.DataFrame(index=pd.Index([], name="symbol"), columns=["industry"])

        def _query() -> pd.DataFrame:
            rows: list[tuple[str, str | None]] = []
            with sqlite3.connect(settings.sqlite_db_path) as conn:
                for start in range(0, len(ordered_symbols), 800):
                    chunk = ordered_symbols[start:start + 800]
                    placeholders = ",".join("?" for _ in chunk)
                    rows.extend(
                        conn.execute(
                            f"SELECT symbol, industry FROM stocks WHERE symbol IN ({placeholders})",
                            chunk,
                        ).fetchall()
                    )
            if not rows:
                return pd.DataFrame(index=ordered_symbols, columns=["industry"])
            frame = pd.DataFrame(rows, columns=["symbol", "industry"]).drop_duplicates("symbol", keep="last")
            return frame.set_index("symbol").reindex(ordered_symbols)

        import asyncio

        try:
            return await asyncio.to_thread(_query)
        except sqlite3.Error as exc:
            raise ValueError(f"加载行业元数据失败: {exc}") from exc

    @staticmethod
    def _align_frames(
        factor_df: pd.DataFrame,
        return_df: pd.DataFrame,
        symbols: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        factor_df = factor_df.copy()
        return_df = return_df.copy()
        factor_df.index = pd.to_datetime(factor_df.index).normalize()
        return_df.index = pd.to_datetime(return_df.index).normalize()
        factor_df.columns = [str(column) for column in factor_df.columns]
        return_df.columns = [str(column) for column in return_df.columns]
        dates = factor_df.index.intersection(return_df.index)
        if symbols:
            target_symbols = list(dict.fromkeys(str(symbol) for symbol in symbols))
        else:
            target_symbols = list(factor_df.columns.intersection(return_df.columns))
        return (
            factor_df.reindex(index=dates, columns=target_symbols),
            return_df.reindex(index=dates, columns=target_symbols),
        )

    @staticmethod
    def _trim_to_effective_factor_dates(
        factor_df: pd.DataFrame,
        return_df: pd.DataFrame,
        membership_mask: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
        if factor_df.empty or return_df.empty:
            return factor_df, return_df, membership_mask
        valid_dates = factor_df.index[factor_df.notna().any(axis=1) & return_df.notna().any(axis=1)]
        factor_df = factor_df.loc[valid_dates]
        return_df = return_df.loc[valid_dates]
        if membership_mask is not None and not membership_mask.empty:
            membership_mask = membership_mask.loc[valid_dates]
        return factor_df, return_df, membership_mask

    async def _load_pool_membership_mask(
        self,
        stock_pool_value: str,
        dates: pd.Index,
        symbols: pd.Index,
        *,
        mode: str = "static_latest",
    ) -> pd.DataFrame | None:
        if mode == "union" or stock_pool_value.startswith("watchlist_") or len(dates) == 0 or len(symbols) == 0:
            return None
        item = get_index_item(stock_pool_value)
        if item is None or not item.pool_enabled:
            return None

        normalized_dates = pd.to_datetime(dates).normalize()
        target_symbols = [str(symbol) for symbol in symbols]
        if mode == "static_latest":
            return await self._load_static_index_membership_mask(item.symbol, normalized_dates, symbols, target_symbols)
        if mode != "point_in_time":
            raise ValueError(f"Unsupported pool_membership_mode: {mode}")
        membership = await self._load_index_membership_by_snapshot(
            item.symbol,
            normalized_dates.max().date(),
            target_symbols,
        )
        if not membership:
            raise ValueError(f"No point-in-time index components available for {item.symbol}")

        snapshot_dates = sorted(membership)
        rows: list[pd.Series] = []
        for current_date in normalized_dates:
            active_snapshot = None
            for snapshot_date in snapshot_dates:
                if snapshot_date <= current_date.date():
                    active_snapshot = snapshot_date
                else:
                    break
            active_symbols = membership.get(active_snapshot, set()) if active_snapshot else set()
            rows.append(pd.Series([str(symbol) in active_symbols for symbol in symbols], index=symbols))
        return pd.DataFrame(rows, index=normalized_dates, columns=symbols).astype(bool)

    async def _load_static_index_membership_mask(
        self,
        index_symbol: str,
        dates: pd.DatetimeIndex,
        columns: pd.Index,
        symbols: list[str],
    ) -> pd.DataFrame:
        latest_symbols = await self._load_latest_index_snapshot_symbols(
            index_symbol,
            dates.max().date(),
            symbols,
        )
        if not latest_symbols:
            raise ValueError(f"No static index component snapshot available for {index_symbol}")
        active = pd.Series([str(symbol) in latest_symbols for symbol in columns], index=columns)
        return pd.DataFrame([active.to_numpy()] * len(dates), index=dates, columns=columns).astype(bool)

    async def _load_latest_index_snapshot_symbols(
        self,
        index_symbol: str,
        end_date: date,
        symbols: list[str],
    ) -> set[str]:
        if not symbols:
            return set()
        placeholders = ",".join("?" for _ in symbols)

        def _query() -> set[str]:
            with sqlite3.connect(settings.sqlite_db_path) as conn:
                snapshot = conn.execute(
                    """
                    SELECT MAX(trade_date)
                    FROM index_components
                    WHERE index_symbol = ?
                      AND trade_date <= ?
                    """,
                    (index_symbol, end_date.isoformat()),
                ).fetchone()
                snapshot_date = snapshot[0] if snapshot else None
                if not snapshot_date:
                    return set()
                rows = conn.execute(
                    f"""
                    SELECT symbol
                    FROM index_components
                    WHERE index_symbol = ?
                      AND trade_date = ?
                      AND symbol IN ({placeholders})
                    """,
                    [index_symbol, snapshot_date, *symbols],
                ).fetchall()
            return {str(row[0]) for row in rows}

        import asyncio

        return await asyncio.to_thread(_query)

    async def _load_index_membership_by_snapshot(
        self,
        index_symbol: str,
        end_date: date,
        symbols: list[str],
    ) -> dict[date, set[str]]:
        if not symbols:
            return {}
        placeholders = ",".join("?" for _ in symbols)

        def _query() -> dict[date, set[str]]:
            with sqlite3.connect(settings.sqlite_db_path) as conn:
                rows = conn.execute(
                    f"""
                    SELECT trade_date, symbol
                    FROM index_components
                    WHERE index_symbol = ?
                      AND trade_date <= ?
                      AND symbol IN ({placeholders})
                    ORDER BY trade_date
                    """,
                    [index_symbol, end_date.isoformat(), *symbols],
                ).fetchall()
            result: dict[date, set[str]] = {}
            for trade_date, symbol in rows:
                result.setdefault(date.fromisoformat(str(trade_date)), set()).add(str(symbol))
            return result

        import asyncio

        return await asyncio.to_thread(_query)

    @staticmethod
    def _latest_non_empty_cross_section(factor_df: pd.DataFrame) -> pd.Series:
        for day in sorted(factor_df.index, reverse=True):
            latest = factor_df.loc[day].dropna()
            if not latest.empty:
                return latest
        return pd.Series(dtype=float)

    @staticmethod
    def _active_symbol_count(membership_mask: pd.DataFrame | None, fallback: int) -> int:
        if membership_mask is None or membership_mask.empty:
            return fallback
        active_counts = membership_mask.sum(axis=1)
        if active_counts.empty:
            return fallback
        return int(round(float(active_counts.mean())))

    @staticmethod
    def _coverage_ratio(factor_df: pd.DataFrame, symbols: list[str], membership_mask: pd.DataFrame | None = None) -> float:
        if membership_mask is not None and not membership_mask.empty:
            possible = int(membership_mask.sum().sum())
        else:
            possible = len(factor_df.index) * max(1, len(symbols))
        possible = max(1, possible)
        return round(float(factor_df.notna().sum().sum()) / possible, 4)

    @staticmethod
    def _mean_turnover(turnover: list[tuple]) -> float:
        values = [float((mn + mx) / 2) for _, mn, mx in turnover if mn is not None and mx is not None]
        return round(float(np.mean(values)), 4) if values else 0.0

    def _quantile_nav(self, factor_df: pd.DataFrame, return_df: pd.DataFrame, params: dict[str, Any]) -> dict[str, Any]:
        group_count = int(params.get("group_count") or 5)
        direction = str(params.get("direction") or "desc")
        fee = self._transaction_cost_rate(params)
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

    async def _attach_benchmark_to_quantile(
        self,
        quantile: dict[str, Any],
        *,
        benchmark_symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[str]:
        summary = quantile.setdefault("summary", {})
        series = quantile.setdefault("series", {})
        series.setdefault("benchmark", [])
        series.setdefault("excess_long_short", [])
        summary["benchmark_symbol"] = benchmark_symbol
        summary["benchmark_name"] = benchmark_display_name(benchmark_symbol)

        try:
            store = get_market_data_store()
            returns = await asyncio.to_thread(
                store.load_benchmark,
                benchmark_symbol,
                start_date,
                end_date,
            )
        except Exception as exc:
            message = f"Benchmark data load failed for {benchmark_symbol}: {exc}"
            summary["benchmark_warning"] = message
            return [message]

        if returns is None or returns.empty:
            message = f"Benchmark data is unavailable for {benchmark_symbol}; comparison series is omitted."
            summary["benchmark_warning"] = message
            return [message]

        benchmark_nav = returns_to_nav_series(returns)
        benchmark_series = nav_points(benchmark_nav, value_key="value")
        excess_series = excess_nav_points(
            series.get("long_short") or [],
            benchmark_nav,
            strategy_key="value",
            value_key="value",
        )
        series["benchmark"] = benchmark_series
        series["excess_long_short"] = excess_series
        summary["benchmark_return"] = round(float(benchmark_nav.iloc[-1] - 1.0), 4) if not benchmark_nav.empty else 0.0
        summary["excess_long_short_return"] = (
            round(float(excess_series[-1]["value"] - 1.0), 4) if excess_series else 0.0
        )
        return []

    @staticmethod
    def _max_drawdown(nav_values: list[float]) -> float:
        if not nav_values:
            return 0.0
        series = pd.Series(nav_values, dtype=float)
        drawdown = series / series.cummax() - 1
        return abs(float(drawdown.min()))

    @staticmethod
    def _transaction_cost_rate(params: dict[str, Any]) -> float:
        return (
            float(params.get("fee_rate") or 0.0)
            + float(params.get("stamp_tax_rate") or 0.0)
            + float(params.get("transfer_fee_rate") or 0.0)
            + float(params.get("slippage") or 0.0)
        )

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

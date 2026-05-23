"""Factor value cache API."""

from __future__ import annotations

import asyncio
import uuid
from datetime import date
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.factor_value_store import (
    get_factor_definition,
    get_factor_group,
    get_factor_value_store,
    list_factor_definitions,
    list_factor_groups,
)
from app.services.factor_catalog import RESEARCH_FACTOR_SPECS, TA_FACTOR_SPECS, is_catalog_factor
from app.services.alpha101_calculator import precompute_alpha101_factors
from app.services.factor_precompute import (
    precompute_high_volume_features,
    precompute_small_cap_core_features,
)
from app.services.factor_dependency_sync import build_precompute_prepare
from app.services.index_components import load_index_symbols
from app.services.research_factor_calculator import precompute_research_factors
from app.services.runtime_tasks import register_task, update_task
from app.services.ta_factor_calculator import precompute_ta_factors

router = APIRouter(tags=["factor-values"])

_PRECOMPUTE_STAGE_WEIGHTS = [
    ("解析参数", 0.03),
    ("加载股票池", 0.05),
    ("加载市值数据", 0.12),
    ("加载分钟行情", 0.10),
    ("加载涨跌停数据", 0.08),
    ("加载日线收盘价", 0.08),
    ("计算状态类因子", 0.18),
    ("计算技术因子", 0.14),
    ("写入核心因子", 0.06),
    ("计算放量因子", 0.12),
    ("完成", 0.04),
]
_STAGE_WEIGHT_MAP = dict(_PRECOMPUTE_STAGE_WEIGHTS)
_STAGE_ORDER = {name: index + 1 for index, (name, _) in enumerate(_PRECOMPUTE_STAGE_WEIGHTS)}
_STAGE_LIST_META = [
    {"name": name, "weight": weight, "index": index + 1}
    for index, (name, weight) in enumerate(_PRECOMPUTE_STAGE_WEIGHTS)
]


class FactorPrecomputeRequest(BaseModel):
    factor_names: list[str] = Field(default_factory=lambda: ["high_volume_signal"])
    start_date: date
    end_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    async_task: bool = False


class FactorGroupPrecomputeRequest(BaseModel):
    group_name: str = "small_cap_v4_core"
    start_date: date
    end_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    async_task: bool = False


class FactorPrecomputePrepareRequest(BaseModel):
    mode: str = Field(default="single", pattern="^(single|group)$")
    factor_names: list[str] = Field(default_factory=list)
    group_name: str | None = None
    start_date: date
    end_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class FactorQueryRequest(BaseModel):
    factor_name: str
    trade_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    as_of_time: str | None = None
    params: dict[str, Any] | None = None


_CORE_FACTORS = {
    "market_cap",
    "market_cap_rank",
    "is_st",
    "is_paused",
    "is_limit_up",
    "is_limit_down",
    "yesterday_limit_up",
}
_HIGH_VOLUME_FACTORS = {
    "cum_volume_at_time",
    "rolling_max_volume",
    "high_volume_ratio",
    "high_volume_signal",
}
_ALPHA101_FACTORS = {f"alpha101_{index:03d}" for index in range(1, 102)}
_TA_FACTORS = set(TA_FACTOR_SPECS)
_RESEARCH_FACTORS = set(RESEARCH_FACTOR_SPECS)
_CATALOG_FACTORS = _ALPHA101_FACTORS | _TA_FACTORS | _RESEARCH_FACTORS
_SUPPORTED_PRECOMPUTE_FACTORS = _CORE_FACTORS | _HIGH_VOLUME_FACTORS | _CATALOG_FACTORS


@router.get("/definitions")
async def definitions() -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": list_factor_definitions()}


@router.get("/groups")
async def groups() -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": list_factor_groups()}


@router.post("/precompute/prepare")
async def prepare_precompute(request: FactorPrecomputePrepareRequest = Body(...)) -> dict[str, Any]:
    if request.end_date < request.start_date:
        raise HTTPException(status_code=400, detail="end_date must be greater than or equal to start_date")
    try:
        data = await run_in_thread(
            build_precompute_prepare,
            mode=request.mode,
            factor_names=request.factor_names,
            group_name=request.group_name,
            start_date=request.start_date,
            end_date=request.end_date,
            symbols=request.symbols,
            index_symbol=request.index_symbol,
            params=request.params,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"code": 0, "message": "success", "data": data}


@router.get("/coverage")
async def coverage(
    factor_name: str = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    index_symbol: str | None = Query(default=None),
    symbols: str | None = Query(default=None),
    as_of_time: str | None = Query(default=None),
    window: int | None = Query(default=None),
    threshold: float | None = Query(default=None),
    daily_volume_to_share_multiplier: float | None = Query(default=None),
    full_range: bool = Query(default=False),
) -> dict[str, Any]:
    if get_factor_definition(factor_name) is None:
        raise HTTPException(status_code=404, detail=f"Unknown factor: {factor_name}")
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else None
    if symbol_list is None and index_symbol:
        symbol_list = await load_index_symbols(index_symbol, start_date, end_date)
    params = _build_params(factor_name, as_of_time, window, threshold, daily_volume_to_share_multiplier)
    store = get_factor_value_store()
    effective_as_of_time = _effective_as_of_time(factor_name, as_of_time, params)
    if full_range:
        data = store.coverage_many(
            [factor_name],
            symbols=symbol_list,
            as_of_time=effective_as_of_time,
            params=params,
        ).get(factor_name, store._empty_coverage(factor_name))
    else:
        data = store.coverage(
            factor_name=factor_name,
            start_date=start_date,
            end_date=end_date,
            symbols=symbol_list,
            as_of_time=effective_as_of_time,
            params=params,
        )
    data["requested_symbol_count"] = len(symbol_list or [])
    return {"code": 0, "message": "success", "data": data}


@router.post("/precompute")
async def precompute(request: FactorPrecomputeRequest = Body(...)) -> dict[str, Any]:
    unknown = [name for name in request.factor_names if name not in _SUPPORTED_PRECOMPUTE_FACTORS]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unsupported precompute factors: {unknown}")
    if not request.symbols and not request.index_symbol:
        raise HTTPException(status_code=400, detail="symbols or index_symbol is required")

    task_id = f"factor-{str(uuid.uuid4())[:8]}"
    register_task(
        task_id=task_id,
        kind="factor_precompute",
        title=f"因子预计算 {', '.join(request.factor_names)}",
        status="running",
        progress=0,
        result_ref="/factor",
        meta={
            "factor_names": request.factor_names,
            "index_symbol": request.index_symbol,
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
        },
    )
    params = request.params or {}
    names = set(request.factor_names)
    if request.async_task:
        asyncio.create_task(_run_single_precompute_task(task_id, request))
        return {"code": 0, "message": "success", "data": _task_started_payload(task_id, request)}

    try:
        result = await _execute_single_precompute(task_id, request)
    except Exception as exc:
        update_task(task_id, status="failed", progress=1.0, result_ref="/factor", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    update_task(
        task_id,
        status="done",
        progress=1.0,
        result_ref="/factor",
        meta={"rows_written": result.get("rows_written") if isinstance(result, dict) else None},
    )
    if isinstance(result, dict):
        _attach_result_coverage(
            result,
            factor_names=request.factor_names,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        result["task_id"] = task_id
    return {"code": 0, "message": "success", "data": result}


@router.post("/groups/precompute")
async def precompute_group(request: FactorGroupPrecomputeRequest = Body(...)) -> dict[str, Any]:
    group = get_factor_group(request.group_name)
    if group is None:
        raise HTTPException(status_code=404, detail=f"Unknown factor group: {request.group_name}")
    if not request.symbols and not request.index_symbol:
        raise HTTPException(status_code=400, detail="symbols or index_symbol is required")
    params = request.params or {}
    task_id = f"factor-group-{str(uuid.uuid4())[:8]}"
    register_task(
        task_id=task_id,
        kind="factor_precompute",
        title=f"因子集合预计算 {request.group_name}",
        status="running",
        progress=0,
        result_ref="/factor",
        meta={
            "group_name": request.group_name,
            "index_symbol": request.index_symbol,
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
        },
    )
    if request.async_task:
        asyncio.create_task(_run_group_precompute_task(task_id, request, group))
        return {"code": 0, "message": "success", "data": _task_started_payload(task_id, request)}

    try:
        result = await _execute_group_precompute(task_id, request, group)
    except Exception as exc:
        update_task(task_id, status="failed", progress=1.0, result_ref="/factor", error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    update_task(
        task_id,
        status="done",
        progress=1.0,
        result_ref="/factor",
        meta={"rows_written": result.get("rows_written") if isinstance(result, dict) else None},
    )
    if isinstance(result, dict):
        _attach_result_coverage(
            result,
            factor_names=group.get("factor_names") or [],
            start_date=request.start_date,
            end_date=request.end_date,
        )
        result["task_id"] = task_id
    return {"code": 0, "message": "success", "data": result}


def _task_started_payload(
    task_id: str,
    request: FactorPrecomputeRequest | FactorGroupPrecomputeRequest,
) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "symbols": len(request.symbols or []),
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
        "as_of_time": str((request.params or {}).get("time") or (request.params or {}).get("as_of_time") or ""),
        "window": int((request.params or {}).get("window") or 0),
        "threshold": float((request.params or {}).get("threshold") or 0.0),
        "rows": {},
        "rows_written": 0,
        "status": "running",
    }


def _progress_updater(task_id: str):
    def update(progress: float, stage: str, meta: dict[str, Any]) -> None:
        stage_key = _normalize_stage(stage)
        stage_weight = _STAGE_WEIGHT_MAP.get(stage_key, 0.0)
        stage_percent = _stage_percent(stage_key, meta)
        stage_index = _STAGE_ORDER.get(stage_key, len(_STAGE_ORDER))
        update_task(
            task_id,
            status="running",
            progress=progress,
            meta={
                "stage": stage,
                "stage_key": stage_key,
                "stage_index": stage_index,
                "stage_total": len(_PRECOMPUTE_STAGE_WEIGHTS),
                "stage_weight": stage_weight,
                "stage_percent": stage_percent,
                "stage_weights": _STAGE_LIST_META,
                **(meta or {}),
            },
        )

    return update


def _normalize_stage(stage: str) -> str:
    text = str(stage or "")
    if text.startswith("放量因子："):
        return "计算放量因子"
    return text


def _stage_percent(stage_key: str, meta: dict[str, Any] | None) -> float:
    meta = meta or {}
    if stage_key == "计算状态类因子":
        current = float(meta.get("current_day") or 0)
        total = float(meta.get("total_days") or 0)
        return max(0.0, min(100.0, current / total * 100)) if total > 0 else 0.0
    if stage_key in {"计算技术因子", "计算放量因子"}:
        current = float(meta.get("current") or 0)
        total = float(meta.get("total") or 0)
        return max(0.0, min(100.0, current / total * 100)) if total > 0 else 0.0
    if stage_key == "完成":
        return 100.0
    return 0.0


def _completion_task_meta(result: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "stage": "完成",
        "stage_key": "完成",
        "stage_index": _STAGE_ORDER["完成"],
        "stage_total": len(_PRECOMPUTE_STAGE_WEIGHTS),
        "stage_weight": _STAGE_WEIGHT_MAP["完成"],
        "stage_percent": 100.0,
        "stage_weights": _STAGE_LIST_META,
        "rows_written": result.get("rows_written") if isinstance(result, dict) else None,
        "result": result if isinstance(result, dict) else None,
    }


async def _execute_single_precompute(task_id: str, request: FactorPrecomputeRequest) -> dict[str, Any]:
    return await _execute_factor_bundle(
        task_id=task_id,
        factor_names=request.factor_names,
        start_date=request.start_date,
        end_date=request.end_date,
        symbols=request.symbols,
        index_symbol=request.index_symbol,
        params=request.params or {},
    )


async def _execute_group_precompute(
    task_id: str,
    request: FactorGroupPrecomputeRequest,
    group: dict[str, Any],
) -> dict[str, Any]:
    result = await _execute_factor_bundle(
        task_id=task_id,
        factor_names=[str(name) for name in group.get("factor_names") or []],
        start_date=request.start_date,
        end_date=request.end_date,
        symbols=request.symbols,
        index_symbol=request.index_symbol,
        params=request.params or {},
    )
    if isinstance(result, dict):
        _attach_result_coverage(
            result,
            factor_names=group.get("factor_names") or [],
            start_date=request.start_date,
            end_date=request.end_date,
        )
    return result


async def _execute_factor_bundle(
    *,
    task_id: str,
    factor_names: list[str],
    start_date: date,
    end_date: date,
    symbols: list[str] | None,
    index_symbol: str | None,
    params: dict[str, Any],
) -> dict[str, Any]:
    names = [str(name) for name in factor_names if str(name)]
    symbol_list = symbols
    if symbol_list is None and index_symbol:
        symbol_list = await load_index_symbols(index_symbol, start_date, end_date)
    if not symbol_list:
        raise ValueError("No symbols resolved for factor precompute")

    small_cap_names = [name for name in names if name in (_CORE_FACTORS | _HIGH_VOLUME_FACTORS)]
    ta_names = [name for name in names if name in _TA_FACTORS]
    alpha_names = [name for name in names if name in _ALPHA101_FACTORS]
    research_names = [name for name in names if name in _RESEARCH_FACTORS]
    unknown = [name for name in names if name not in _SUPPORTED_PRECOMPUTE_FACTORS]
    if unknown:
        raise ValueError(f"Unsupported precompute factors: {unknown}")

    results: list[dict[str, Any]] = []
    updater = _progress_updater(task_id)
    total_steps = sum(bool(items) for items in [small_cap_names, ta_names, alpha_names, research_names]) or 1
    current_step = 0

    def wrap_progress(offset: int):
        def _wrapped(progress: float, stage: str, meta: dict[str, Any]) -> None:
            overall = (offset + max(0.0, min(1.0, progress))) / total_steps
            updater(overall, stage, meta)
        return _wrapped

    if small_cap_names:
        current_step += 1
        if set(small_cap_names) <= _HIGH_VOLUME_FACTORS:
            results.append(await run_in_thread(
                precompute_high_volume_features,
                start_date=start_date,
                end_date=end_date,
                symbols=symbol_list,
                index_symbol=None,
                as_of_time=str(params.get("time") or params.get("as_of_time") or "14:30"),
                window=int(params.get("window") or 120),
                threshold=float(params.get("threshold") or 0.9),
                daily_volume_to_share_multiplier=float(params.get("daily_volume_to_share_multiplier") or 100.0),
                progress_callback=wrap_progress(current_step - 1),
            ))
        else:
            include_high_volume = any(name in _HIGH_VOLUME_FACTORS for name in small_cap_names)
            results.append(await run_in_thread(
                precompute_small_cap_core_features,
                start_date=start_date,
                end_date=end_date,
                symbols=symbol_list,
                index_symbol=None,
                timer_time=str(params.get("time") or params.get("as_of_time") or "10:30"),
                include_high_volume=include_high_volume,
                progress_callback=wrap_progress(current_step - 1),
            ))
    if ta_names:
        current_step += 1
        results.append(await run_in_thread(
            precompute_ta_factors,
            factor_names=ta_names,
            start_date=start_date,
            end_date=end_date,
            symbols=symbol_list,
            progress_callback=wrap_progress(current_step - 1),
        ))
    if alpha_names:
        current_step += 1
        results.append(await run_in_thread(
            precompute_alpha101_factors,
            factor_names=alpha_names,
            start_date=start_date,
            end_date=end_date,
            symbols=symbol_list,
            progress_callback=wrap_progress(current_step - 1),
        ))
    if research_names:
        current_step += 1
        results.append(await run_in_thread(
            precompute_research_factors,
            factor_names=research_names,
            start_date=start_date,
            end_date=end_date,
            symbols=symbol_list,
            progress_callback=wrap_progress(current_step - 1),
        ))
    return _merge_precompute_results(symbol_list, start_date, end_date, names, results)


def _merge_precompute_results(
    symbols: list[str],
    start_date: date,
    end_date: date,
    factor_names: list[str],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    requested_factor_names = list(dict.fromkeys(str(name) for name in factor_names if str(name)))
    merged_rows: dict[str, int] = {name: 0 for name in requested_factor_names}
    total_written = 0
    for result in results:
        total_written += int(result.get("rows_written") or 0)
        for factor_name, count in (result.get("rows") or {}).items():
            merged_rows[str(factor_name)] = merged_rows.get(str(factor_name), 0) + int(count or 0)
    written_factor_names = [name for name, count in merged_rows.items() if count > 0]
    zero_row_factor_names = [name for name, count in merged_rows.items() if count <= 0]
    return {
        "symbols": len(symbols),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "factor_names": requested_factor_names,
        "rows": merged_rows,
        "rows_written": total_written,
        "requested_factor_count": len(requested_factor_names),
        "written_factor_count": len(written_factor_names),
        "zero_row_factor_count": len(zero_row_factor_names),
        "zero_row_factor_names": zero_row_factor_names,
    }


async def _run_single_precompute_task(task_id: str, request: FactorPrecomputeRequest) -> None:
    try:
        result = await _execute_single_precompute(task_id, request)
        if isinstance(result, dict):
            _attach_result_coverage(
                result,
                factor_names=request.factor_names,
                start_date=request.start_date,
                end_date=request.end_date,
            )
            result["task_id"] = task_id
        update_task(
            task_id,
            status="done",
            progress=1.0,
            result_ref="/factor",
            meta=_completion_task_meta(result if isinstance(result, dict) else None),
        )
    except Exception as exc:
        update_task(task_id, status="failed", progress=1.0, result_ref="/factor", error=str(exc), meta={"stage": "失败"})


async def _run_group_precompute_task(
    task_id: str,
    request: FactorGroupPrecomputeRequest,
    group: dict[str, Any],
) -> None:
    try:
        result = await _execute_group_precompute(task_id, request, group)
        if isinstance(result, dict):
            result["task_id"] = task_id
        update_task(
            task_id,
            status="done",
            progress=1.0,
            result_ref="/factor",
            meta=_completion_task_meta(result if isinstance(result, dict) else None),
        )
    except Exception as exc:
        update_task(task_id, status="failed", progress=1.0, result_ref="/factor", error=str(exc), meta={"stage": "失败"})


def _attach_result_coverage(
    result: dict[str, Any],
    *,
    factor_names: list[str],
    start_date: date,
    end_date: date,
) -> None:
    store = get_factor_value_store()
    ranges: list[dict[str, Any]] = []
    for factor_name in factor_names:
        params = _build_params(
            factor_name=factor_name,
            as_of_time=None,
            window=None,
            threshold=None,
            daily_volume_to_share_multiplier=None,
        )
        data = store.coverage(
            factor_name=factor_name,
            start_date=start_date,
            end_date=end_date,
            as_of_time=_effective_as_of_time(factor_name, None, params),
            params=params,
        )
        ranges.append({
            "factor_name": factor_name,
            "total_rows": data.get("total_rows", 0),
            "symbol_count": data.get("symbol_count", 0),
            "date_count": data.get("date_count", 0),
            "min_date": data.get("min_date"),
            "max_date": data.get("max_date"),
            "is_complete_to_end": data.get("max_date") == end_date.isoformat(),
        })
    result["coverage_ranges"] = ranges


@router.post("/query")
async def query(request: FactorQueryRequest = Body(...)) -> dict[str, Any]:
    if get_factor_definition(request.factor_name) is None:
        raise HTTPException(status_code=404, detail=f"Unknown factor: {request.factor_name}")
    symbol_list = request.symbols
    if symbol_list is None and request.index_symbol:
        symbol_list = await load_index_symbols(request.index_symbol, request.trade_date, request.trade_date)
    values = get_factor_value_store().load_cross_section(
        factor_name=request.factor_name,
        trade_date=request.trade_date,
        symbols=symbol_list,
        as_of_time=request.as_of_time,
        params=request.params,
    )
    return {
        "code": 0,
        "message": "success",
        "data": {
            "factor_name": request.factor_name,
            "trade_date": request.trade_date.isoformat(),
            "items": [{"symbol": symbol, "value": value} for symbol, value in values.items()],
        },
    }


@router.get("/preview")
async def preview(
    factor_name: str = Query(...),
    trade_date: date = Query(...),
    index_symbol: str | None = Query(default=None),
    symbols: str | None = Query(default=None),
    as_of_time: str | None = Query(default=None),
    window: int | None = Query(default=None),
    threshold: float | None = Query(default=None),
    daily_volume_to_share_multiplier: float | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    if get_factor_definition(factor_name) is None:
        raise HTTPException(status_code=404, detail=f"Unknown factor: {factor_name}")
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else None
    if symbol_list is None and index_symbol:
        symbol_list = await load_index_symbols(index_symbol, trade_date, trade_date)
    params = _build_params(factor_name, as_of_time, window, threshold, daily_volume_to_share_multiplier)
    effective_as_of_time = _effective_as_of_time(factor_name, as_of_time, params)
    store = get_factor_value_store()
    items = store.preview(
        factor_name=factor_name,
        trade_date=trade_date,
        symbols=symbol_list,
        as_of_time=effective_as_of_time,
        params=params,
        limit=limit,
    )
    values = store.load_cross_section(
        factor_name=factor_name,
        trade_date=trade_date,
        symbols=symbol_list,
        as_of_time=effective_as_of_time,
        params=params,
    )
    return {
        "code": 0,
        "message": "success",
        "data": {
            "factor_name": factor_name,
            "trade_date": trade_date.isoformat(),
            "items": items,
            "total": len(values),
        },
    }


def _build_params(
    factor_name: str,
    as_of_time: str | None,
    window: int | None,
    threshold: float | None,
    daily_volume_to_share_multiplier: float | None,
) -> dict[str, Any] | None:
    if factor_name == "cum_volume_at_time":
        return {"time": as_of_time or "14:30"}
    if factor_name in {"rolling_max_volume", "high_volume_ratio"}:
        return {
            "time": as_of_time or "14:30",
            "window": int(window or 120),
            "daily_volume_to_share_multiplier": float(daily_volume_to_share_multiplier or 100.0),
        }
    if factor_name == "high_volume_signal":
        return {
            "time": as_of_time or "14:30",
            "window": int(window or 120),
            "daily_volume_to_share_multiplier": float(daily_volume_to_share_multiplier or 100.0),
            "threshold": float(threshold or 0.9),
        }
    if factor_name in {"is_paused", "is_limit_up", "is_limit_down"}:
        return {"time": as_of_time or "10:30"}
    return None


def _effective_as_of_time(factor_name: str, as_of_time: str | None, params: dict[str, Any] | None) -> str | None:
    if factor_name in {"cum_volume_at_time", "rolling_max_volume", "high_volume_ratio", "high_volume_signal"}:
        return as_of_time or "14:30"
    if factor_name in {"is_paused", "is_limit_up", "is_limit_down"}:
        return as_of_time or "10:30"
    if params and "time" in params:
        return str(params["time"])
    return as_of_time


async def run_in_thread(func, /, *args, **kwargs):
    import asyncio
    import functools

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

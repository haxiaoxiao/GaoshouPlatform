"""Factor value cache API."""

from __future__ import annotations

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
from app.services.factor_precompute import (
    precompute_high_volume_features,
    precompute_small_cap_core_features,
)
from app.services.index_components import load_index_symbols

router = APIRouter(tags=["factor-values"])


class FactorPrecomputeRequest(BaseModel):
    factor_names: list[str] = Field(default_factory=lambda: ["high_volume_signal"])
    start_date: date
    end_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class FactorGroupPrecomputeRequest(BaseModel):
    group_name: str = "small_cap_v4_core"
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
_SUPPORTED_PRECOMPUTE_FACTORS = _CORE_FACTORS | _HIGH_VOLUME_FACTORS


@router.get("/definitions")
async def definitions() -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": list_factor_definitions()}


@router.get("/groups")
async def groups() -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": list_factor_groups()}


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
) -> dict[str, Any]:
    if get_factor_definition(factor_name) is None:
        raise HTTPException(status_code=404, detail=f"Unknown factor: {factor_name}")
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else None
    if symbol_list is None and index_symbol:
        symbol_list = await load_index_symbols(index_symbol, start_date, end_date)
    params = _build_params(factor_name, as_of_time, window, threshold, daily_volume_to_share_multiplier)
    data = get_factor_value_store().coverage(
        factor_name=factor_name,
        start_date=start_date,
        end_date=end_date,
        symbols=symbol_list,
        as_of_time=_effective_as_of_time(factor_name, as_of_time, params),
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

    params = request.params or {}
    names = set(request.factor_names)
    try:
        if names <= _HIGH_VOLUME_FACTORS:
            result = await run_in_thread(
                precompute_high_volume_features,
                start_date=request.start_date,
                end_date=request.end_date,
                symbols=request.symbols,
                index_symbol=request.index_symbol,
                as_of_time=str(params.get("time") or params.get("as_of_time") or "14:30"),
                window=int(params.get("window") or 120),
                threshold=float(params.get("threshold") or 0.9),
                daily_volume_to_share_multiplier=float(params.get("daily_volume_to_share_multiplier") or 100.0),
            )
        else:
            result = await run_in_thread(
                precompute_small_cap_core_features,
                start_date=request.start_date,
                end_date=request.end_date,
                symbols=request.symbols,
                index_symbol=request.index_symbol,
                timer_time=str(params.get("time") or params.get("as_of_time") or "10:30"),
                include_high_volume=bool(params.get("include_high_volume", True)),
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"code": 0, "message": "success", "data": result}


@router.post("/groups/precompute")
async def precompute_group(request: FactorGroupPrecomputeRequest = Body(...)) -> dict[str, Any]:
    group = get_factor_group(request.group_name)
    if group is None:
        raise HTTPException(status_code=404, detail=f"Unknown factor group: {request.group_name}")
    if request.group_name != "small_cap_v4_core":
        raise HTTPException(status_code=400, detail=f"Unsupported precompute group: {request.group_name}")
    if not request.symbols and not request.index_symbol:
        raise HTTPException(status_code=400, detail="symbols or index_symbol is required")
    params = request.params or {}
    try:
        result = await run_in_thread(
            precompute_small_cap_core_features,
            start_date=request.start_date,
            end_date=request.end_date,
            symbols=request.symbols,
            index_symbol=request.index_symbol,
            timer_time=str(params.get("time") or params.get("as_of_time") or "10:30"),
            include_high_volume=bool(params.get("include_high_volume", True)),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"code": 0, "message": "success", "data": result}


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

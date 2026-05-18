"""Feature store API for factor research."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.feature_precompute import (
    precompute_high_volume_features,
    precompute_small_cap_core_features,
)
from app.services.feature_store import (
    get_feature_definition,
    get_feature_group,
    get_feature_store,
    list_feature_groups,
    list_feature_definitions,
)
from app.services.index_components import load_index_symbols

router = APIRouter(tags=["features"])


HIGH_VOLUME_FEATURES = {
    "cum_volume_at_time",
    "max_volume_nd",
    "high_volume_ratio",
    "high_volume_signal",
}
TIMER_1030_FEATURES = {
    "smallcap_is_paused",
    "smallcap_is_limit_up",
    "smallcap_is_limit_down",
}


class FeaturePrecomputeRequest(BaseModel):
    feature_names: list[str] = Field(default_factory=lambda: ["high_volume_signal"])
    start_date: date
    end_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class FeatureGroupPrecomputeRequest(BaseModel):
    group_name: str = "small_cap_v4_core"
    start_date: date
    end_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class FeatureQueryRequest(BaseModel):
    feature_name: str
    trade_date: date
    symbols: list[str] | None = None
    index_symbol: str | None = None
    as_of_time: str | None = None
    params: dict[str, Any] | None = None


def _feature_query_options(
    feature_name: str,
    *,
    as_of_time: str | None = None,
    window: int | None = None,
    threshold: float | None = None,
    daily_volume_to_share_multiplier: float | None = None,
) -> tuple[str | None, dict[str, Any] | None]:
    """Return the exact as-of time and params used by stored feature partitions."""

    if feature_name == "cum_volume_at_time":
        time_value = as_of_time or "14:30"
        return time_value, {"time": time_value}
    if feature_name in {"max_volume_nd", "high_volume_ratio"}:
        time_value = as_of_time or "14:30"
        return time_value if feature_name == "high_volume_ratio" else None, {
            "time": time_value,
            "window": int(window or 120),
            "daily_volume_to_share_multiplier": float(daily_volume_to_share_multiplier or 100.0),
        }
    if feature_name == "high_volume_signal":
        time_value = as_of_time or "14:30"
        return time_value, {
            "time": time_value,
            "window": int(window or 120),
            "daily_volume_to_share_multiplier": float(daily_volume_to_share_multiplier or 100.0),
            "threshold": float(threshold or 0.9),
        }
    if feature_name in TIMER_1030_FEATURES:
        time_value = as_of_time or "10:30"
        return time_value, {"time": time_value}
    return None, None


@router.get("/definitions")
async def definitions() -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": list_feature_definitions()}


@router.get("/groups")
async def groups() -> dict[str, Any]:
    return {"code": 0, "message": "success", "data": list_feature_groups()}


@router.get("/coverage")
async def coverage(
    feature_name: str = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    index_symbol: str | None = Query(default=None),
    symbols: str | None = Query(default=None),
    as_of_time: str | None = Query(default=None),
    window: int | None = Query(default=None),
    threshold: float | None = Query(default=None),
    daily_volume_to_share_multiplier: float | None = Query(default=None),
) -> dict[str, Any]:
    if get_feature_definition(feature_name) is None:
        raise HTTPException(status_code=404, detail=f"Unknown feature: {feature_name}")
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else None
    if symbol_list is None and index_symbol:
        symbol_list = await load_index_symbols(index_symbol, start_date, end_date)
    effective_as_of_time, params = _feature_query_options(
        feature_name,
        as_of_time=as_of_time,
        window=window,
        threshold=threshold,
        daily_volume_to_share_multiplier=daily_volume_to_share_multiplier,
    )
    data = get_feature_store().coverage(
        feature_name,
        start_date=start_date,
        end_date=end_date,
        symbols=symbol_list,
        as_of_time=effective_as_of_time,
        params=params,
    )
    data["requested_symbol_count"] = len(symbol_list or [])
    return {"code": 0, "message": "success", "data": data}


@router.post("/precompute")
async def precompute(request: FeaturePrecomputeRequest = Body(...)) -> dict[str, Any]:
    supported = {
        "cum_volume_at_time",
        "max_volume_nd",
        "high_volume_ratio",
        "high_volume_signal",
        "smallcap_market_cap",
        "smallcap_market_cap_rank",
        "smallcap_is_st",
        "smallcap_is_paused",
        "smallcap_is_limit_up",
        "smallcap_is_limit_down",
        "smallcap_yesterday_limit_up",
    }
    unknown = [name for name in request.feature_names if name not in supported]
    if unknown:
        raise HTTPException(status_code=400, detail=f"Unsupported precompute features: {unknown}")
    if not request.symbols and not request.index_symbol:
        raise HTTPException(status_code=400, detail="symbols or index_symbol is required")

    params = request.params or {}
    try:
        names = set(request.feature_names)
        if names <= {"cum_volume_at_time", "max_volume_nd", "high_volume_ratio", "high_volume_signal"}:
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
async def precompute_group(request: FeatureGroupPrecomputeRequest = Body(...)) -> dict[str, Any]:
    group = get_feature_group(request.group_name)
    if group is None:
        raise HTTPException(status_code=404, detail=f"Unknown feature group: {request.group_name}")
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
async def query(request: FeatureQueryRequest = Body(...)) -> dict[str, Any]:
    if get_feature_definition(request.feature_name) is None:
        raise HTTPException(status_code=404, detail=f"Unknown feature: {request.feature_name}")
    symbol_list = request.symbols
    if symbol_list is None and request.index_symbol:
        symbol_list = await load_index_symbols(request.index_symbol, request.trade_date, request.trade_date)
    values = get_feature_store().load_cross_section(
        request.feature_name,
        request.trade_date,
        symbols=symbol_list,
        as_of_time=request.as_of_time,
        params=request.params,
    )
    return {
        "code": 0,
        "message": "success",
        "data": {
            "feature_name": request.feature_name,
            "trade_date": request.trade_date.isoformat(),
            "items": [{"symbol": symbol, "value": value} for symbol, value in values.items()],
        },
    }


@router.get("/preview")
async def preview(
    feature_name: str = Query(...),
    trade_date: date = Query(...),
    index_symbol: str | None = Query(default=None),
    symbols: str | None = Query(default=None),
    as_of_time: str | None = Query(default=None),
    window: int | None = Query(default=None),
    threshold: float | None = Query(default=None),
    daily_volume_to_share_multiplier: float | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    if get_feature_definition(feature_name) is None:
        raise HTTPException(status_code=404, detail=f"Unknown feature: {feature_name}")
    symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()] if symbols else None
    if symbol_list is None and index_symbol:
        symbol_list = await load_index_symbols(index_symbol, trade_date, trade_date)
    effective_as_of_time, params = _feature_query_options(
        feature_name,
        as_of_time=as_of_time,
        window=window,
        threshold=threshold,
        daily_volume_to_share_multiplier=daily_volume_to_share_multiplier,
    )
    values = get_feature_store().load_cross_section(
        feature_name,
        trade_date,
        symbols=symbol_list,
        as_of_time=effective_as_of_time,
        params=params,
    )
    items = [
        {"symbol": symbol, "value": value}
        for symbol, value in sorted(values.items(), key=lambda item: (item[1], item[0]))[:limit]
    ]
    return {
        "code": 0,
        "message": "success",
        "data": {
            "feature_name": feature_name,
            "trade_date": trade_date.isoformat(),
            "items": items,
            "total": len(values),
        },
    }


async def run_in_thread(func, /, *args, **kwargs):
    import asyncio
    import functools

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

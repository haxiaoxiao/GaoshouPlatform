"""计算层 API — /api/compute, with /api/v2/compute compatibility."""
import time
from datetime import date

import pandas as pd
from fastapi import APIRouter, Body
from pydantic import BaseModel, Field

from app.compute.cache import ComputeCache, get_compute_cache
from app.compute.expression import evaluate_expression, validate_expression
from app.compute.operators import auto_discover
from app.compute.operators.registry import OperatorRegistry
from app.models.factor import FactorConfig
from app.services.compute_service import compute_service

router = APIRouter()

# 启动时自动发现所有算子
auto_discover()


class EvaluateRequest(BaseModel):
    expression: str = Field(..., description="因子表达式，如 ts_mean($close, 5)")
    symbols: list[str] = Field(..., description="股票代码列表")
    start_date: date = Field(..., description="开始日期")
    end_date: date = Field(..., description="结束日期")
    use_cache: bool = Field(default=True, description="是否使用缓存")
    engine: str = Field(default="builtin", description="builtin or akquant")


class PrecomputeExpressionsRequest(BaseModel):
    expressions: list[str] = Field(..., description="Factor expressions to precompute")
    symbols: list[str] = Field(..., description="Stock symbols")
    start_date: date
    end_date: date
    engine: str = Field(default="builtin", description="builtin or akquant")


class ScreenRequest(BaseModel):
    condition: str = Field(..., description="筛选条件表达式，如 RSI($close, 14) < 30")
    universe: str = Field(default="all", description="股票池: all 或 自选股分组名")
    trade_date: date = Field(..., description="筛选日期")
    limit: int = Field(default=50, description="返回数量上限")


@router.get("/operators")
async def list_operators():
    """返回所有可用算子"""
    return {
        "code": 0,
        "message": "success",
        "data": OperatorRegistry.to_api_list(),
    }


@router.get("/capabilities")
async def capabilities():
    """Return compute/TA runtime capabilities for the UI."""
    from app.compute.operators.ta_ops import get_ta_capabilities

    return {
        "code": 0,
        "message": "success",
        "data": {
            "engines": ["builtin", "akquant"],
            "ta": get_ta_capabilities(),
        },
    }


@router.post("/evaluate")
async def evaluate(req: EvaluateRequest):
    """计算表达式"""
    t0 = time.time()
    cache_hit = False

    cache: ComputeCache = get_compute_cache()
    if req.engine == "akquant":
        from app.services.akquant_factor import evaluate_akquant_factor

        out = await evaluate_akquant_factor(
            req.expression,
            req.symbols,
            req.start_date,
            req.end_date,
        )
        elapsed = (time.time() - t0) * 1000
        return {
            "code": 0,
            "message": "success",
            "data": out,
            "meta": {
                "expression": req.expression,
                "engine": "akquant",
                "cache_hit": False,
                "compute_time_ms": round(elapsed, 1),
            },
        }
    if req.engine != "builtin":
        return {"code": 1, "message": f"Unknown compute engine: {req.engine}", "data": None}

    result = None
    if req.use_cache:
        result = cache.get(req.expression)

    if result is None:
        from app.data_stores import get_market_data_store

        store = get_market_data_store()
        df = store.load_daily(req.symbols, req.start_date, req.end_date)
        if df.empty:
            return {"code": 0, "message": "success", "data": {}, "meta": {"rows": 0}}

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp

        result = evaluate_expression(req.expression, data)

        if req.use_cache:
            cache.set(req.expression, result)
    else:
        cache_hit = True

    out: dict[str, list[dict]] = {}
    if isinstance(result, dict):
        for sym, series in result.items():
            if not isinstance(series, pd.Series):
                continue
            out[sym] = [
                {"trade_date": str(idx.date()), "value": float(v) if pd.notna(v) else None}
                for idx, v in series.items()
                if req.start_date <= idx.date() <= req.end_date
            ]
    elif isinstance(result, pd.Series):
        out["result"] = [
            {"trade_date": str(idx.date()), "value": float(v) if pd.notna(v) else None}
            for idx, v in result.items()
            if req.start_date <= idx.date() <= req.end_date
        ]

    elapsed = (time.time() - t0) * 1000

    return {
        "code": 0,
        "message": "success",
        "data": out,
        "meta": {
            "expression": req.expression,
            "cache_hit": cache_hit,
            "compute_time_ms": round(elapsed, 1),
        },
    }


@router.post("/precompute")
async def precompute_expressions(req: PrecomputeExpressionsRequest):
    """Precompute factor expressions into persistent factor_cache."""
    try:
        from app.services.factor_expression_precompute import precompute_factor_expressions

        result = await precompute_factor_expressions(
            expressions=req.expressions,
            symbols=req.symbols,
            start_date=req.start_date,
            end_date=req.end_date,
            engine=req.engine,
        )
        return {"code": 0, "message": "success", "data": result}
    except Exception as exc:
        return {"code": 1, "message": f"{type(exc).__name__}: {exc}", "data": None}


@router.post("/screen")
async def screen(req: ScreenRequest):
    """基于条件筛选股票"""
    valid, err = validate_expression(req.condition)
    if not valid:
        return {"code": 1, "message": f"Invalid condition: {err}", "data": None}

    from app.data_stores import get_market_data_store

    store = get_market_data_store()
    symbols = store.load_trading_dates([], date(2000, 1, 1), req.trade_date)
    # Get symbols from the coverage API for the target date
    if not symbols:
        # fallback: query daily data for all symbols on the exact date
        info = store.coverage([], date(2000, 1, 1), req.trade_date, dataset="klines_daily")
        symbols = info.get("symbols_covered", [])

    if not symbols:
        return {"code": 0, "message": "success", "data": {"symbols": [], "count": 0}}

    df = store.load_daily(symbols, date(2020, 1, 1), req.trade_date)
    if df.empty:
        return {"code": 0, "message": "success", "data": {"symbols": [], "count": 0}}

    data = {}
    for sym, grp in df.groupby("symbol"):
        data[sym] = grp

    result = evaluate_expression(req.condition, data)

    matching = []
    if isinstance(result, dict):
        for sym, series in result.items():
            if isinstance(series, pd.Series) and req.trade_date in series.index:
                val = series.loc[req.trade_date]
                if isinstance(val, (bool, pd.Series)):
                    cond = bool(val.iloc[0] if isinstance(val, pd.Series) else val)
                elif pd.isna(val):
                    continue
                else:
                    cond = bool(val)
                if cond:
                    matching.append(sym)
        matching = matching[:req.limit]

    return {
        "code": 0,
        "message": "success",
        "data": {"symbols": matching, "count": len(matching)},
    }


@router.post("/validate")
async def validate_expr(expression: str = Body(..., description="要校验的表达式", embed=True)):
    """校验表达式语法"""
    valid, err = validate_expression(expression)
    return {
        "code": 0,
        "message": "success",
        "data": {"valid": valid, "error": err},
    }


@router.post("/batch")
async def batch_evaluate(configs: list[FactorConfig]):
    """Batch compute multiple factors (for factor board use).

    Accepts a list of FactorConfig objects and returns computed results
    for each, with per-config error isolation.
    """
    results = await compute_service.batch_compute(configs)
    return {"code": 0, "message": "success", "data": results}

"""Factor CRUD, templates, validation, preview, and precompute endpoints."""

import asyncio
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.compute.operators import auto_discover
from app.db.models import Factor
from app.db.models.factor import FactorAnalysis
from app.db.sqlite import get_async_session
from app.models.factor import (
    EvalConfig,
    FactorConfig,
    FactorCreate,
    FactorDirection,
    FactorResponse,
    FactorTemplate,
    FactorUpdate,
    ICMethod,
    StockPool,
    ValidateRequest,
    ValidateResponse,
)
from app.services.compute_service import compute_service
from app.services.factor_evaluation import FactorEvaluationService
from app.services.factor_service import FactorCreate as DbFactorCreate
from app.services.factor_service import FactorService
from app.services.factor_service import FactorUpdate as DbFactorUpdate
from app.services.factor_templates import FactorTemplatesService
from app.services.factor_validator import FactorValidator
from app.services.factor_value_store import factor_params_hash, get_factor_value_store

# Ensure operators are registered before any expression evaluation
auto_discover()

router = APIRouter(tags=["因子管理"])

templates_service = FactorTemplatesService()
validator = FactorValidator()


class FactorPreviewRequest(BaseModel):
    symbols: list[str] | None = None
    stock_pool: str | None = None
    start_date: date
    end_date: date
    limit: int = Field(default=200, ge=1, le=5000)


class FactorPrecomputeRequest(BaseModel):
    symbols: list[str] | None = None
    stock_pool: str | None = None
    start_date: date
    end_date: date
    as_of_time: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class PythonFactorValidateRequest(BaseModel):
    code: str = Field(..., description="Python 因子代码")


class FactorAnalyzeRequest(BaseModel):
    """因子分析请求"""
    symbols: list[str] | None = None
    stock_pool: str | None = None
    benchmark: str | None = None
    start_date: date
    end_date: date
    n_groups: int = Field(default=5, ge=2, le=20, description="分组数量")
    group_count: int | None = Field(default=None, ge=2, le=20, description="分组数量别名")
    direction: str | None = None
    rebalance_period: str | None = None
    ic_method: str | None = None
    outlier_handling: str | None = None
    standardize: bool | None = None
    industry_neutralization: bool | None = None
    include_st: bool | None = None
    include_new: bool | None = None
    filter_limit_up: bool | None = None
    filter_limit_down: bool | None = None
    fee_rate: float | None = None
    slippage: float | None = None
    use_cache: bool | None = None
    write_cache: bool | None = None


def _factor_metadata(factor: Factor) -> dict[str, Any]:
    params = dict(factor.parameters or {})
    return {
        "expression": factor.code or params.get("expression") or "",
        "source_type": params.get("source_type") or factor.source or "dsl",
        "engine": params.get("engine") or "builtin",
        "stock_pool": params.get("stock_pool") or StockPool.HS300.value,
        "direction": params.get("direction") or "desc",
        "default_stock_pool": params.get("default_stock_pool") or "zz500",
        "default_benchmark": params.get("default_benchmark") or "000905.SH",
        "cache_enabled": params.get("cache_enabled", True),
        "default_eval_config": params.get("default_eval_config") or {},
        "params": params,
        "kind": params.get("kind") or "factor",
    }


def _factor_response(factor: Factor) -> FactorResponse:
    meta = _factor_metadata(factor)
    return FactorResponse(
        id=factor.id,
        name=factor.name,
        expression=meta["expression"],
        source_type=meta["source_type"],
        engine=meta["engine"],
        stock_pool=meta["stock_pool"],
        direction=meta["direction"],
        default_stock_pool=meta["default_stock_pool"],
        default_benchmark=meta["default_benchmark"],
        cache_enabled=meta["cache_enabled"],
        default_eval_config=meta["default_eval_config"],
        category=factor.category,
        description=factor.description,
        params=meta["params"],
        created_at=factor.created_at.isoformat() if factor.created_at else "",
        updated_at=factor.updated_at.isoformat() if factor.updated_at else "",
    )


async def _resolve_symbols(
    symbols: list[str] | None,
    stock_pool: StockPool | str | None,
) -> list[str]:
    if symbols:
        return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
    pool = stock_pool or StockPool.HS300
    try:
        resolved_pool = pool if isinstance(pool, StockPool) else StockPool(pool)
    except ValueError:
        resolved_pool = StockPool.HS300
    return await compute_service._resolve_stock_pool(resolved_pool)


async def _compute_factor_rows(
    *,
    expression: str,
    symbols: list[str],
    start_date: date,
    end_date: date,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    data = await compute_service._load_market_data(symbols, start_date, end_date)
    if not data:
        return []
    from app.compute.expression import evaluate_expression

    result = evaluate_expression(expression, data)
    rows: list[dict[str, Any]] = []
    if isinstance(result, dict):
        for symbol, series in result.items():
            if not isinstance(series, pd.Series):
                continue
            for idx, value in series.items():
                trade_date = idx.date() if hasattr(idx, "date") else date.fromisoformat(str(idx)[:10])
                if start_date <= trade_date <= end_date:
                    rows.append({
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "value": float(value) if pd.notna(value) else None,
                    })
                    if limit is not None and len(rows) >= limit:
                        return rows
    elif isinstance(result, pd.Series):
        for idx, value in result.items():
            trade_date = idx.date() if hasattr(idx, "date") else date.fromisoformat(str(idx)[:10])
            if start_date <= trade_date <= end_date:
                rows.append({
                    "symbol": "result",
                    "trade_date": trade_date.isoformat(),
                    "value": float(value) if pd.notna(value) else None,
                })
                if limit is not None and len(rows) >= limit:
                    return rows
    return rows


def _rows_to_factor_matrix(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert long factor rows into date x symbol matrix."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if df.empty or not {"symbol", "trade_date", "value"} <= set(df.columns):
        return pd.DataFrame()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["trade_date", "symbol", "value"])
    if df.empty:
        return pd.DataFrame()
    matrix = df.pivot_table(
        index="trade_date",
        columns="symbol",
        values="value",
        aggfunc="last",
    )
    return matrix.sort_index()


@router.get("/templates", response_model=list[FactorTemplate])
async def list_templates():
    """List all factor creation templates."""
    return templates_service.list_templates()


@router.post("/validate-python")
async def validate_python_code(req: PythonFactorValidateRequest = Body(...)):
    """Validate Python factor code by checking syntax and compute() signature."""
    import ast
    try:
        tree = ast.parse(req.code)
    except SyntaxError as e:
        return {"valid": False, "error": f"Python 语法错误: {e}"}

    # 检查是否定义了 compute 函数
    has_compute = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "compute":
            has_compute = True
            break
    if not has_compute:
        return {"valid": False, "error": "代码中未定义 compute(data, context) 函数"}

    return {"valid": True, "error": None}


@router.post("/validate", response_model=ValidateResponse)
async def validate_expression(req: ValidateRequest):
    """Validate a factor expression."""
    result = validator.validate(req.expression)
    return ValidateResponse(**result)


@router.post("/create", response_model=FactorResponse)
async def create_factor(
    data: FactorCreate,
    session: AsyncSession = Depends(get_async_session),
):
    """Create a new expression-backed factor or indicator."""
    if data.source_type == "python":
        py_result = await validate_python_code(PythonFactorValidateRequest(code=data.expression))
        if not py_result["valid"]:
            raise HTTPException(status_code=400, detail=py_result["error"] or "Invalid Python factor")
    else:
        result = validator.validate(data.expression)
        if not result["valid"]:
            raise HTTPException(status_code=400, detail=result["error"] or "Invalid expression")

    service = FactorService(session)
    existing = await service.get_factor_by_name(data.name)
    if existing is not None:
        raise HTTPException(status_code=400, detail=f"Factor '{data.name}' already exists")

    params = dict(data.params or {})
    params.setdefault("expression", data.expression)
    params.setdefault("source_type", data.source_type)
    params.setdefault("stock_pool", str(data.stock_pool))
    params.setdefault("direction", data.direction)
    params.setdefault("default_stock_pool", data.default_stock_pool)
    params.setdefault("default_benchmark", data.default_benchmark)
    params.setdefault("cache_enabled", data.cache_enabled)
    params.setdefault("default_eval_config", data.default_eval_config)
    params.setdefault("kind", params.get("factor_type") or "factor")
    params.setdefault("engine", data.engine)
    factor = await service.create_factor(DbFactorCreate(
        name=data.name,
        category=data.category,
        source=data.source_type if data.source_type != "dsl" else "custom",
        code=data.expression,
        parameters=params,
        description=data.description,
    ))
    await session.commit()
    return _factor_response(factor)


@router.get("/{factor_id}", response_model=FactorResponse)
async def get_factor(
    factor_id: int,
    session: AsyncSession = Depends(get_async_session),
):
    """Get factor by ID."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    return _factor_response(factor)


@router.put("/{factor_id}", response_model=FactorResponse)
async def update_factor(
    factor_id: int,
    data: FactorUpdate,
    session: AsyncSession = Depends(get_async_session),
):
    """Update factor expression or parameters."""
    service = FactorService(session)
    factor = await service.get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")

    expression = data.expression if data.expression is not None else factor.code
    next_source_type = data.source_type or (factor.parameters or {}).get("source_type") or factor.source or "dsl"
    if expression and next_source_type == "python":
        py_result = await validate_python_code(PythonFactorValidateRequest(code=expression))
        if not py_result["valid"]:
            raise HTTPException(status_code=400, detail=py_result["error"] or "Invalid Python factor")
    elif expression:
        result = validator.validate(expression)
        if not result["valid"]:
            raise HTTPException(status_code=400, detail=result["error"] or "Invalid expression")

    params = dict(factor.parameters or {})
    if data.params is not None:
        params.update(data.params)
    if data.expression is not None:
        params["expression"] = data.expression
    if data.source_type is not None:
        params["source_type"] = data.source_type
    if data.engine is not None:
        params["engine"] = data.engine
    if data.stock_pool is not None:
        params["stock_pool"] = str(data.stock_pool)
    if data.direction is not None:
        params["direction"] = data.direction
    if data.default_stock_pool is not None:
        params["default_stock_pool"] = data.default_stock_pool
    if data.default_benchmark is not None:
        params["default_benchmark"] = data.default_benchmark
    if data.cache_enabled is not None:
        params["cache_enabled"] = data.cache_enabled
    if data.default_eval_config is not None:
        params["default_eval_config"] = data.default_eval_config

    updated = await service.update_factor(
        factor_id,
        data=DbFactorUpdate(
            name=data.name,
            category=data.category,
            source=None,
            code=data.expression,
            parameters=params,
            description=data.description,
        ),
    )
    await session.commit()
    return _factor_response(updated)


@router.delete("/{factor_id}")
async def delete_factor(
    factor_id: int,
    session: AsyncSession = Depends(get_async_session),
):
    """Delete a factor."""
    success = await FactorService(session).delete_factor(factor_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    await session.commit()
    return {"deleted": True}


@router.post("/{factor_id}/run-python")
async def run_python_factor(
    factor_id: int,
    request: FactorPreviewRequest = Body(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Run a Python factor and return computed values."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    params = dict(factor.parameters or {})
    source_type = params.get("source_type") or factor.source
    if source_type != "python":
        raise HTTPException(status_code=400, detail="This endpoint only supports Python factors")
    code = factor.code
    if not code:
        raise HTTPException(status_code=400, detail="Factor has no Python code")
    symbols = await _resolve_symbols(request.symbols, request.stock_pool)
    from app.services.python_factor_runner import run_python_factor
    result = run_python_factor(
        code=code,
        symbols=symbols,
        start_date=request.start_date,
        end_date=request.end_date,
        params=dict(request.params or {}),
        stock_pool=request.stock_pool or params.get("stock_pool", "zz500"),
    )
    return {"factor_id": factor_id, **result}


@router.post("/{factor_id}/preview")
async def preview_factor(
    factor_id: int,
    request: FactorPreviewRequest = Body(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Preview expression-backed factor values without writing factor cache data."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    meta = _factor_metadata(factor)
    symbols = await _resolve_symbols(request.symbols, request.stock_pool or meta["stock_pool"])
    if meta["source_type"] == "python":
        from app.services.python_factor_runner import run_python_factor
        result = run_python_factor(
            code=meta["expression"],
            symbols=symbols,
            start_date=request.start_date,
            end_date=request.end_date,
            params=dict(request.params or {}),
            stock_pool=request.stock_pool or meta["stock_pool"],
            benchmark=meta["default_benchmark"],
        )
        rows = result["rows"][:request.limit]
        return {"factor_id": factor_id, "items": rows, "total": len(result["rows"]), "errors": result["errors"]}
    rows = await _compute_factor_rows(
        expression=meta["expression"],
        symbols=symbols,
        start_date=request.start_date,
        end_date=request.end_date,
        limit=request.limit,
    )
    return {"factor_id": factor_id, "items": rows, "total": len(rows)}


@router.post("/{factor_id}/precompute")
async def precompute_factor(
    factor_id: int,
    request: FactorPrecomputeRequest = Body(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Compute and persist factor values into the shared factor value cache."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    meta = _factor_metadata(factor)
    params = dict(request.params or {})
    params.setdefault("factor_id", factor.id)
    params.setdefault("expression", meta["expression"])
    params.setdefault("engine", meta["engine"])
    symbols = await _resolve_symbols(request.symbols, request.stock_pool or meta["stock_pool"])
    if meta["source_type"] == "python":
        from app.services.python_factor_runner import run_python_factor
        result = run_python_factor(
            code=meta["expression"],
            symbols=symbols,
            start_date=request.start_date,
            end_date=request.end_date,
            params=params,
            stock_pool=request.stock_pool or meta["stock_pool"],
            benchmark=meta["default_benchmark"],
        )
        if result["errors"]:
            raise HTTPException(status_code=400, detail=result["errors"])
        rows = result["rows"]
    else:
        rows = await _compute_factor_rows(
            expression=meta["expression"],
            symbols=symbols,
            start_date=request.start_date,
            end_date=request.end_date,
        )
    created_at = datetime.now()
    feature_rows = [
        {
            "symbol": row["symbol"],
            "trade_date": row["trade_date"],
            "as_of_time": request.as_of_time or "",
            "factor_name": factor.name,
            "params_hash": factor_params_hash(params),
            "value": row["value"],
            "source": "custom.factor",
            "created_at": created_at,
        }
        for row in rows
        if row["value"] is not None
    ]
    writer = get_factor_value_store().batch_writer()
    writer.extend(feature_rows)
    writer.flush()
    return {
        "factor_id": factor.id,
        "factor_name": factor.name,
        "symbols": len(symbols),
        "rows_written": writer.written,
        "start_date": request.start_date.isoformat(),
        "end_date": request.end_date.isoformat(),
    }


@router.get("/{factor_id}/coverage")
async def factor_coverage(
    factor_id: int,
    start_date: date,
    end_date: date,
    session: AsyncSession = Depends(get_async_session),
):
    """Inspect factor value cache coverage for a saved factor."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    return await asyncio.to_thread(
        get_factor_value_store().coverage,
        factor.name,
        start_date=start_date,
        end_date=end_date,
    )


@router.post("/{factor_id}/analyze")
async def analyze_factor(
    factor_id: int,
    request: FactorAnalyzeRequest = Body(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Run IC + quantile analysis on a saved factor and persist results.

    Loads the factor's expression, resolves the stock universe, loads
    market data, computes factor values, runs cross-sectional IC analysis
    and quantile group return analysis, saves the results to the
    factor_analysis table, and returns the full report.
    """
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")

    meta = _factor_metadata(factor)
    expression = meta["expression"]
    if not expression:
        raise HTTPException(status_code=400, detail="Factor has no expression")

    symbols = await _resolve_symbols(request.symbols, request.stock_pool or meta["stock_pool"])
    if not symbols:
        raise HTTPException(status_code=400, detail="No symbols resolved from stock pool")

    logger.info(
        "Analyzing factor {} ({}) with {} symbols from {} to {}",
        factor_id, factor.name, len(symbols), request.start_date, request.end_date,
    )

    service = FactorEvaluationService()
    group_count = request.group_count or request.n_groups

    # Run full report (IC + industry IC + turnover + signal decay + quantile)
    try:
        direction = request.direction or meta.get("direction", "desc")
        ic_method = request.ic_method or "spearman"
        if meta["source_type"] == "python":
            from app.models.factor import (
                DecayPoint,
                FactorReport,
                ICPoint,
                IndustryIC,
                TurnoverPoint,
            )
            from app.services.python_factor_runner import run_python_factor

            result = run_python_factor(
                code=expression,
                symbols=symbols,
                start_date=request.start_date,
                end_date=request.end_date,
                params=dict(meta.get("params") or {}),
                stock_pool=request.stock_pool or meta["stock_pool"],
                benchmark=request.benchmark or meta.get("default_benchmark", "000905.SH"),
            )
            if result["errors"]:
                raise HTTPException(status_code=400, detail=result["errors"])
            factor_df = _rows_to_factor_matrix(result["rows"])
            return_df = service._load_return_matrix(symbols, request.start_date, request.end_date)
            ic_points = service._compute_ic_series(factor_df, return_df, EvalConfig(group_count=group_count))
            industry_ic = service._compute_industry_ic(factor_df, return_df)
            turnover = service._compute_turnover(factor_df)
            signal_decay = service._compute_signal_decay(factor_df, return_df)
            latest = pd.Series(dtype=float)
            if not factor_df.empty:
                latest = factor_df.loc[factor_df.index.max()].dropna()
            report_obj = FactorReport(
                ic_series=[ICPoint(date=d, value=v) for d, v in ic_points],
                industry_ic=[IndustryIC(industry=i, value=v) for i, v in industry_ic],
                turnover=[TurnoverPoint(date=d, min_quantile=mn, max_quantile=mx) for d, mn, mx in turnover],
                signal_decay=[DecayPoint(lag=l, min_quantile=mn, max_quantile=mx) for l, mn, mx in signal_decay],
                top20=service._top_n_stocks(latest, 20, ascending=False) if not latest.empty else [],
                bottom20=service._top_n_stocks(latest, 20, ascending=True) if not latest.empty else [],
                update_date=date.today(),
            )
        else:
            report_obj = await service.report(
                FactorConfig(
                    expression=expression,
                    stock_pool=StockPool(meta.get("stock_pool", "hs300")),
                    start_date=request.start_date,
                    end_date=request.end_date,
                    benchmark=request.benchmark or meta.get("default_benchmark", "000905.SH"),
                    direction=FactorDirection(direction),
                ),
                eval_config=EvalConfig(
                    group_count=group_count,
                    ic_method=ICMethod(ic_method),
                    industry_neutralization=bool(request.industry_neutralization)
                    if request.industry_neutralization is not None else False,
                    include_st=bool(request.include_st) if request.include_st is not None else False,
                    include_new=bool(request.include_new) if request.include_new is not None else True,
                ),
            )
    except Exception as e:
        logger.exception("Full report failed for factor {}", factor_id)
        raise HTTPException(status_code=500, detail=f"Report generation failed: {e}")

    # Assemble report
    ic_values = [p.value for p in report_obj.ic_series]
    if ic_values:
        ic_mean = sum(ic_values) / len(ic_values)
        ic_std = (sum((v - ic_mean) ** 2 for v in ic_values) / len(ic_values)) ** 0.5 if len(ic_values) > 1 else 0.0
        icir = ic_mean / ic_std if ic_std > 0 else 0.0
        positive_rate = sum(1 for v in ic_values if v > 0) / len(ic_values)
    else:
        ic_mean = ic_std = icir = positive_rate = 0.0

    summary = {
        "ic_mean": round(ic_mean, 4),
        "ic_std": round(ic_std, 4),
        "icir": round(icir, 4),
        "positive_ic_rate": round(positive_rate, 4),
    }

    report = {
        "expression": expression,
        "parameters": {
            "symbol_count": len(symbols),
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
            "n_groups": group_count,
        },
        "summary": summary,
        "ic_series": [{"date": str(p.date), "value": p.value} for p in report_obj.ic_series],
        "industry_ic": [{"industry": p.industry, "value": p.value} for p in report_obj.industry_ic],
        "turnover": [{"date": str(p.date), "min_quantile": p.min_quantile, "max_quantile": p.max_quantile} for p in report_obj.turnover],
        "signal_decay": [{"lag": p.lag, "min_quantile": p.min_quantile, "max_quantile": p.max_quantile} for p in report_obj.signal_decay],
        "top": [{"symbol": s.symbol, "value": s.value} for s in report_obj.top20],
        "bottom": [{"symbol": s.symbol, "value": s.value} for s in report_obj.bottom20],
    }

    # Persist analysis
    try:
        analysis = FactorAnalysis(
            factor_id=factor_id,
            start_date=request.start_date,
            end_date=request.end_date,
            ic_mean=Decimal(str(round(ic_mean, 4))),
            ic_std=Decimal(str(round(ic_std, 4))),
            ir=Decimal(str(round(icir, 4))),
            details=report,
        )
        session.add(analysis)
        await session.commit()
        logger.info("Factor {} analysis saved (IC mean={:.4f}, IR={:.4f})",
                     factor_id, float(analysis.ic_mean or 0), float(analysis.ir or 0))
    except Exception as e:
        logger.error("Failed to persist analysis for factor {}: {}", factor_id, e)

    return {"code": 0, "message": "success", "data": report}


@router.post("/{factor_id}/evaluate")
async def evaluate_factor(
    factor_id: int,
    request: FactorAnalyzeRequest = Body(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Evaluate a saved factor. Preferred alias for /analyze."""
    return await analyze_factor(factor_id=factor_id, request=request, session=session)

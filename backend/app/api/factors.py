"""Factor CRUD, templates, validation, preview, and precompute endpoints."""

from datetime import date, datetime
from typing import Any

import pandas as pd
from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.factor import (
    FactorTemplate,
    FactorCreate,
    FactorUpdate,
    FactorResponse,
    StockPool,
    ValidateRequest,
    ValidateResponse,
)
from app.db.models import Factor
from app.db.sqlite import get_async_session
from app.services.compute_service import compute_service
from app.services.factor_service import FactorCreate as DbFactorCreate
from app.services.factor_service import FactorUpdate as DbFactorUpdate
from app.services.factor_service import FactorService
from app.services.factor_templates import FactorTemplatesService
from app.services.factor_validator import FactorValidator
from app.services.feature_store import feature_params_hash, get_feature_store
from app.services.factor_evaluation import FactorEvaluationService
from app.db.models.factor import FactorAnalysis
from app.compute.operators import auto_discover
from loguru import logger
from decimal import Decimal

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


class FactorAnalyzeRequest(BaseModel):
    """因子分析请求"""
    symbols: list[str] | None = None
    stock_pool: str | None = None
    start_date: date
    end_date: date
    n_groups: int = Field(default=5, ge=2, le=20, description="分组数量")


def _factor_metadata(factor: Factor) -> dict[str, Any]:
    params = dict(factor.parameters or {})
    return {
        "expression": factor.code or params.get("expression") or "",
        "stock_pool": params.get("stock_pool") or StockPool.HS300.value,
        "params": params,
        "kind": params.get("kind") or "factor",
        "engine": params.get("engine") or "builtin",
    }


def _factor_response(factor: Factor) -> FactorResponse:
    meta = _factor_metadata(factor)
    return FactorResponse(
        id=factor.id,
        name=factor.name,
        expression=meta["expression"],
        stock_pool=meta["stock_pool"],
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


@router.get("/templates", response_model=list[FactorTemplate])
async def list_templates():
    """List all factor creation templates."""
    return templates_service.list_templates()


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
    result = validator.validate(data.expression)
    if not result["valid"]:
        raise HTTPException(status_code=400, detail=result["error"] or "Invalid expression")

    service = FactorService(session)
    existing = await service.get_factor_by_name(data.name)
    if existing is not None:
        raise HTTPException(status_code=400, detail=f"Factor '{data.name}' already exists")

    params = dict(data.params or {})
    params.setdefault("expression", data.expression)
    params.setdefault("stock_pool", str(data.stock_pool))
    params.setdefault("kind", params.get("feature_type") or "factor")
    params.setdefault("engine", params.get("engine") or "builtin")
    factor = await service.create_factor(DbFactorCreate(
        name=data.name,
        category=data.category,
        source="custom",
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
    if expression:
        result = validator.validate(expression)
        if not result["valid"]:
            raise HTTPException(status_code=400, detail=result["error"] or "Invalid expression")

    params = dict(factor.parameters or {})
    if data.params is not None:
        params.update(data.params)
    if data.expression is not None:
        params["expression"] = data.expression
    if data.stock_pool is not None:
        params["stock_pool"] = str(data.stock_pool)

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


@router.post("/{factor_id}/preview")
async def preview_factor(
    factor_id: int,
    request: FactorPreviewRequest = Body(...),
    session: AsyncSession = Depends(get_async_session),
):
    """Preview expression-backed factor values without writing Feature Store data."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    meta = _factor_metadata(factor)
    symbols = await _resolve_symbols(request.symbols, request.stock_pool or meta["stock_pool"])
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
    """Compute and persist factor values into the shared Feature Store."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    meta = _factor_metadata(factor)
    params = dict(request.params or {})
    params.setdefault("factor_id", factor.id)
    params.setdefault("expression", meta["expression"])
    params.setdefault("engine", meta["engine"])
    symbols = await _resolve_symbols(request.symbols, request.stock_pool or meta["stock_pool"])
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
            "feature_name": factor.name,
            "params_hash": feature_params_hash(params),
            "value": row["value"],
            "source": "custom.factor",
            "created_at": created_at,
        }
        for row in rows
        if row["value"] is not None
    ]
    written = get_feature_store().write(pd.DataFrame(feature_rows)) if feature_rows else 0
    return {
        "factor_id": factor.id,
        "feature_name": factor.name,
        "symbols": len(symbols),
        "rows_written": written,
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
    """Inspect Feature Store coverage for a saved factor."""
    factor = await FactorService(session).get_factor(factor_id)
    if factor is None:
        raise HTTPException(status_code=404, detail=f"Factor {factor_id} not found")
    return get_feature_store().coverage(
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

    # Run IC analysis (core)
    try:
        ic_result = await service.run_ic_analysis(
            expression=expression,
            symbols=symbols,
            start_date=request.start_date,
            end_date=request.end_date,
        )
    except Exception as e:
        logger.exception("IC analysis failed for factor {}", factor_id)
        raise HTTPException(status_code=500, detail=f"IC analysis failed: {e}")

    # Quantile backtest (best-effort — may fail if BacktestRunner unavailable)
    qt_result = None
    try:
        qt_result = await service.run_quantile_backtest(
            expression=expression,
            symbols=symbols,
            start_date=request.start_date,
            end_date=request.end_date,
            n_groups=request.n_groups,
        )
    except Exception as e:
        logger.warning("Quantile backtest failed for factor {}: {}", factor_id, e)

    # Assemble report
    ic_stats = ic_result.get("ic_stats", {})
    ic_series = ic_result.get("ic_series", [])
    ic_decay = ic_result.get("ic_decay", [])

    summary = {
        "ic_mean": ic_stats.get("mean"),
        "ic_std": ic_stats.get("std"),
        "icir": ic_stats.get("icir"),
        "positive_rate": ic_stats.get("positive_rate"),
        "long_short_annual_return": qt_result.get("annual_return") if qt_result else None,
        "long_short_sharpe": qt_result.get("sharpe_ratio") if qt_result else None,
    }

    report = {
        "expression": expression,
        "parameters": {
            "symbol_count": len(symbols),
            "start_date": request.start_date.isoformat(),
            "end_date": request.end_date.isoformat(),
            "n_groups": request.n_groups,
        },
        "ic_analysis": ic_result,
        "quantile_backtest": qt_result,
        "summary": summary,
    }

    # Persist analysis
    try:
        analysis = FactorAnalysis(
            factor_id=factor_id,
            start_date=request.start_date,
            end_date=request.end_date,
            ic_mean=Decimal(str(round(ic_stats.get("mean", 0) or 0, 4))),
            ic_std=Decimal(str(round(ic_stats.get("std", 0) or 0, 4))),
            ir=Decimal(str(round(ic_stats.get("icir", 0) or 0, 4))),
            details=report,
        )
        session.add(analysis)
        await session.commit()
        logger.info("Factor {} analysis saved (IC mean={:.4f}, IR={:.4f})",
                     factor_id, float(analysis.ic_mean or 0), float(analysis.ir or 0))
    except Exception as e:
        logger.error("Failed to persist analysis for factor {}: {}", factor_id, e)

    return {"code": 0, "message": "success", "data": report}

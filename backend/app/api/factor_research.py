"""Factor research run API.

This module writes only to the new factor_research_* tables. It does not
persist new results into the legacy factor_analysis table.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from pydantic import BaseModel, Field, field_validator

from app.services.factor_research_runs import factor_research_run_service


router = APIRouter()


class FactorResearchRunRequest(BaseModel):
    factor_name: str = Field(min_length=1, max_length=120)
    stock_pool_value: str = Field(default="zz500", min_length=1, max_length=120)
    start_date: date
    end_date: date
    portfolio_type: Literal["long_only", "long_short_i", "long_short_ii"] = "long_only"
    rebalance_period: Literal["daily", "weekly", "monthly"] = "monthly"
    fee_rate: float = Field(default=0.001, ge=0, le=0.05)
    slippage: float = Field(default=0.001, ge=0, le=0.05)
    filter_limit_up: bool = True
    filter_limit_down: bool = True
    group_count: int = Field(default=5, ge=2, le=20)
    direction: Literal["asc", "desc"] = "desc"
    industry_neutralization: bool = False
    standardize: bool = False
    force: bool = False

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, end_date: date, info):
        start_date = info.data.get("start_date")
        if start_date and end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return end_date

    def to_service_payload(self) -> dict[str, Any]:
        params = self.model_dump()
        params["start_date"] = self.start_date.isoformat()
        params["end_date"] = self.end_date.isoformat()
        return params


class FactorResearchBatchRequest(BaseModel):
    factor_names: list[str] = Field(min_length=1)
    stock_pool_value: str = Field(default="zz500", min_length=1, max_length=120)
    start_date: date
    end_date: date
    portfolio_type: Literal["long_only", "long_short_i", "long_short_ii"] = "long_only"
    rebalance_period: Literal["daily", "weekly", "monthly"] = "monthly"
    fee_rate: float = Field(default=0.001, ge=0, le=0.05)
    slippage: float = Field(default=0.001, ge=0, le=0.05)
    filter_limit_up: bool = True
    filter_limit_down: bool = True
    group_count: int = Field(default=5, ge=2, le=20)
    direction: Literal["asc", "desc"] = "desc"
    industry_neutralization: bool = False
    standardize: bool = False
    force: bool = False

    @field_validator("factor_names")
    @classmethod
    def validate_factor_names(cls, factor_names: list[str]) -> list[str]:
        cleaned = [name.strip() for name in factor_names if name and name.strip()]
        if not cleaned:
            raise ValueError("factor_names cannot be empty")
        return cleaned

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, end_date: date, info):
        start_date = info.data.get("start_date")
        if start_date and end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date")
        return end_date

    def to_service_payload(self) -> dict[str, Any]:
        params = self.model_dump()
        params["start_date"] = self.start_date.isoformat()
        params["end_date"] = self.end_date.isoformat()
        return params


@router.post("/runs/prepare")
async def prepare_factor_research_run(request: FactorResearchRunRequest):
    try:
        data = await factor_research_run_service.prepare(request.to_service_payload())
        return {"code": 0, "message": "success", "data": data}
    except Exception as exc:
        logger.exception("Prepare factor research run failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/run")
async def run_factor_research(request: FactorResearchRunRequest):
    try:
        data = await factor_research_run_service.run(
            request.to_service_payload(),
            force=request.force,
        )
        return {"code": 0, "message": "success", "data": data}
    except Exception as exc:
        logger.exception("Factor research run failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/runs/batch")
async def batch_factor_research(request: FactorResearchBatchRequest):
    try:
        data = await factor_research_run_service.batch(request.to_service_payload())
        return {"code": 0, "message": "success", "data": data}
    except Exception as exc:
        logger.exception("Batch factor research failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/runs/latest")
async def get_latest_factor_research_run(
    factor_name: str = Query(..., min_length=1),
    stock_pool_value: str | None = Query(default=None),
    params_hash: str | None = Query(default=None),
):
    row = await factor_research_run_service.find_latest(
        factor_name=factor_name,
        stock_pool_value=stock_pool_value,
        params_hash=params_hash,
    )
    return {
        "code": 0,
        "message": "success",
        "data": factor_research_run_service._summary_from_model(row) if row else None,
    }


@router.get("/runs/{run_id}")
async def get_factor_research_run(run_id: str):
    data = await factor_research_run_service.get(run_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Factor research run not found")
    return {"code": 0, "message": "success", "data": data}

"""Grid trading signal API."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.grid_trading import GRID_SYMBOLS, grid_trading_service
from app.services.tech_small_cap_live import tech_small_cap_live_service

router = APIRouter(prefix="/grid-trading", tags=["grid-trading"])


class GridSignalRequest(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)
    manual_account: dict[str, Any] | None = None


class GridSubmitPreviewRequest(BaseModel):
    order_preview: dict[str, Any] = Field(default_factory=dict)


class GridSubmitOrderRequest(BaseModel):
    order: dict[str, Any] = Field(default_factory=dict)


class TechSmallCapSignalRequest(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)
    manual_account: dict[str, Any] | None = None


class TechSmallCapSubmitOrdersRequest(BaseModel):
    orders: list[dict[str, Any]] = Field(default_factory=list)
    confirm: bool = False


@router.get("/symbols")
async def get_grid_symbols():
    return {"code": 0, "data": {"symbols": GRID_SYMBOLS}}


@router.get("/status")
async def get_grid_status():
    data = await grid_trading_service.status()
    return {"code": 0, "data": data}


@router.get("/account")
async def get_grid_account():
    account = await grid_trading_service.account()
    return {
        "code": 0,
        "data": {
            "cash": account.cash,
            "total_asset": account.total_asset,
            "market_value": account.market_value,
            "positions": account.positions,
            "source": account.source,
            "error": account.error,
        },
    }


@router.post("/signals")
async def get_grid_signals(req: GridSignalRequest):
    data = await grid_trading_service.signals(
        params=req.params,
        manual_account=req.manual_account,
    )
    return {"code": 0, "data": data}


@router.post("/orders/preview")
async def submit_grid_order_preview(req: GridSubmitPreviewRequest):
    data = await grid_trading_service.submit_order_preview(req.order_preview)
    return {"code": 0, "data": data}


@router.post("/orders/submit")
async def submit_grid_order(req: GridSubmitOrderRequest):
    data = await grid_trading_service.submit_order(req.order)
    return {"code": 0, "data": data}


@router.get("/tech-small-cap/variants")
async def get_tech_small_cap_variants():
    return {"code": 0, "data": tech_small_cap_live_service.variants()}


@router.post("/tech-small-cap/signals")
async def get_tech_small_cap_signals(req: TechSmallCapSignalRequest):
    data = await tech_small_cap_live_service.signals(
        params=req.params,
        manual_account=req.manual_account,
    )
    return {"code": 0, "data": data}


@router.post("/tech-small-cap/orders/submit")
async def submit_tech_small_cap_orders(req: TechSmallCapSubmitOrdersRequest):
    data = await tech_small_cap_live_service.submit_orders(req.orders, confirm=req.confirm)
    return {"code": 0, "data": data}

"""Configurable paper/live trading API."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

from app.services.live_trading import live_trading_service

router = APIRouter(prefix="/live-trading", tags=["live-trading"])


class LiveSignalRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="paper", pattern="^(paper|live)$")
    params: dict[str, Any] = Field(default_factory=dict)
    manual_account: dict[str, Any] | None = None


class LivePreflightRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="paper", pattern="^(paper|live)$")
    params: dict[str, Any] = Field(default_factory=dict)
    manual_account: dict[str, Any] | None = None
    evaluate_pipeline: bool = True


class LiveProfileCreateRequest(BaseModel):
    strategy_id: int
    profile_key: str
    display_name: str | None = None
    description: str | None = None
    enabled: bool = True
    is_default: bool = False
    adapter_type: str = "multi_factor_cash_aware"
    params_override: dict[str, Any] = Field(default_factory=dict)
    universe_config: dict[str, Any] = Field(default_factory=lambda: {"type": "strategy"})
    execution_policy: dict[str, Any] = Field(default_factory=dict)


class LiveProfileUpdateRequest(BaseModel):
    strategy_id: int | None = None
    display_name: str | None = None
    description: str | None = None
    enabled: bool | None = None
    is_default: bool | None = None
    adapter_type: str | None = None
    params_override: dict[str, Any] | None = None
    universe_config: dict[str, Any] | None = None
    execution_policy: dict[str, Any] | None = None


class LiveRunnerStartRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="paper", pattern="^(paper|live)$")
    params: dict[str, Any] = Field(default_factory=dict)
    interval_seconds: int = Field(default=60, ge=10, le=3600)


class LiveRunnerTakeoverRequest(BaseModel):
    reason: str = "human takeover"


class LiveSubmitOrdersRequest(BaseModel):
    mode: str = Field(default="paper", pattern="^(paper|live)$")
    orders: list[dict[str, Any]] = Field(default_factory=list)
    confirm: bool = False


class LiveOrderSyncRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="live", pattern="^(paper|live)$")
    limit: int = Field(default=200, ge=1, le=1000)


class LiveOrderCancelRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="live", pattern="^(paper|live)$")
    limit: int = Field(default=200, ge=1, le=1000)
    min_age_seconds: int = Field(default=0, ge=0, le=86400)
    record_ids: list[str] = Field(default_factory=list)
    order_ids: list[str] = Field(default_factory=list)
    confirm: bool = False


class LiveOrderCancelResubmitRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="live", pattern="^(paper|live)$")
    params: dict[str, Any] = Field(default_factory=dict)
    limit: int = Field(default=200, ge=1, le=1000)
    min_age_seconds: int = Field(default=0, ge=0, le=86400)
    record_ids: list[str] = Field(default_factory=list)
    order_ids: list[str] = Field(default_factory=list)
    confirm_cancel: bool = False
    confirm_submit: bool = False


class LiveOrderLocalCloseRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="live", pattern="^(paper|live)$")
    limit: int = Field(default=200, ge=1, le=1000)
    record_ids: list[str] = Field(default_factory=list)
    order_ids: list[str] = Field(default_factory=list)
    reason: str = "client_cancelled"
    confirm: bool = False


class LiveStrategyAccountInitRequest(BaseModel):
    profile_key: str | None = None
    mode: str = Field(default="paper", pattern="^(paper|live)$")
    capital: float = Field(gt=0)
    reset_existing: bool = False


def _ok(data: Any) -> dict[str, Any]:
    return {"code": 0, "data": data}


def _error(exc: Exception) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))


@router.get("/status")
async def live_status() -> dict[str, Any]:
    try:
        return _ok(await live_trading_service.status())
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/account")
async def live_account(
    mode: str = Query(default="live", pattern="^(paper|live)$"),
    profile_key: str | None = Query(default=None),
    include_broker: bool = Query(default=True),
) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.account_snapshot(
                mode=mode,
                profile_key=profile_key,
                include_broker=include_broker,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/account/stream")
async def live_account_stream(
    mode: str = Query(default="live", pattern="^(paper|live)$"),
    profile_key: str | None = Query(default=None),
    interval_seconds: int = Query(default=5, ge=2, le=60),
) -> StreamingResponse:
    async def event_source():
        async for event in live_trading_service.account_stream(
            mode=mode,
            profile_key=profile_key,
            interval_seconds=interval_seconds,
        ):
            event_name = str(event.get("event") or "account")
            data = json.dumps(event.get("data") or {}, ensure_ascii=False)
            yield f"event: {event_name}\ndata: {data}\n\n"

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/account/initialize")
async def initialize_strategy_account(req: LiveStrategyAccountInitRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.initialize_strategy_account(
                profile_key=req.profile_key,
                mode=req.mode,
                capital=req.capital,
                reset_existing=req.reset_existing,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/strategy-profiles")
async def list_strategy_profiles(include_disabled: bool = Query(default=True)) -> dict[str, Any]:
    try:
        return _ok(await live_trading_service.list_profiles(include_disabled=include_disabled))
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/strategy-profiles")
async def create_strategy_profile(req: LiveProfileCreateRequest) -> dict[str, Any]:
    try:
        return _ok(await live_trading_service.create_profile(req.model_dump()))
    except Exception as exc:
        raise _error(exc) from exc


@router.put("/strategy-profiles/{profile_key}")
async def update_strategy_profile(profile_key: str, req: LiveProfileUpdateRequest) -> dict[str, Any]:
    try:
        payload = req.model_dump(exclude_unset=True)
        return _ok(await live_trading_service.update_profile(profile_key, payload))
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/preflight")
async def live_preflight(req: LivePreflightRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.preflight(
                profile_key=req.profile_key,
                mode=req.mode,
                params=req.params,
                manual_account=req.manual_account,
                evaluate_pipeline=req.evaluate_pipeline,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/signals")
async def generate_live_signals(req: LiveSignalRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.signals(
                profile_key=req.profile_key,
                mode=req.mode,
                params=req.params,
                manual_account=req.manual_account,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/runner/start")
async def start_runner(req: LiveRunnerStartRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.start_runner(
                profile_key=req.profile_key,
                mode=req.mode,
                params=req.params,
                interval_seconds=req.interval_seconds,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/runner/stop")
async def stop_runner() -> dict[str, Any]:
    try:
        return _ok(await live_trading_service.stop_runner())
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/runner/takeover")
async def takeover_runner(req: LiveRunnerTakeoverRequest) -> dict[str, Any]:
    try:
        return _ok(await live_trading_service.takeover(reason=req.reason))
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/orders/submit")
async def submit_orders(req: LiveSubmitOrdersRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.submit_orders(
                req.orders,
                mode=req.mode,
                confirm=req.confirm,
                trigger_source="manual",
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/orders/audit")
async def order_audit(
    profile_key: str | None = Query(default=None),
    mode: str | None = Query(default=None, pattern="^(paper|live)$"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    try:
        return _ok(await live_trading_service.list_audits(limit=limit, profile_key=profile_key, mode=mode))
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/orders/pending")
async def pending_orders(
    profile_key: str | None = Query(default=None),
    mode: str = Query(default="live", pattern="^(paper|live)$"),
    limit: int = Query(default=100, ge=1, le=500),
    sync: bool = Query(default=True),
) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.list_pending_orders(
                limit=limit,
                profile_key=profile_key,
                mode=mode,
                sync=sync,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/orders/sync")
async def sync_orders(req: LiveOrderSyncRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.sync_order_status(
                profile_key=req.profile_key,
                mode=req.mode,
                limit=req.limit,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/orders/cancel")
async def cancel_orders(req: LiveOrderCancelRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.cancel_pending_orders(
                profile_key=req.profile_key,
                mode=req.mode,
                limit=req.limit,
                min_age_seconds=req.min_age_seconds,
                record_ids=req.record_ids,
                order_ids=req.order_ids,
                confirm=req.confirm,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/orders/cancel-resubmit")
async def cancel_and_resubmit_orders(req: LiveOrderCancelResubmitRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.cancel_and_resubmit_pending_orders(
                profile_key=req.profile_key,
                mode=req.mode,
                params=req.params,
                limit=req.limit,
                min_age_seconds=req.min_age_seconds,
                record_ids=req.record_ids,
                order_ids=req.order_ids,
                confirm_cancel=req.confirm_cancel,
                confirm_submit=req.confirm_submit,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/orders/close-local")
async def close_local_orders(req: LiveOrderLocalCloseRequest) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.close_local_pending_orders(
                profile_key=req.profile_key,
                mode=req.mode,
                limit=req.limit,
                record_ids=req.record_ids,
                order_ids=req.order_ids,
                reason=req.reason,
                confirm=req.confirm,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/trades")
async def trade_records(
    profile_key: str | None = Query(default=None),
    mode: str | None = Query(default=None, pattern="^(paper|live)$"),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.list_trade_records(
                limit=limit,
                profile_key=profile_key,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/trades/weekly")
async def weekly_trade_analysis(
    profile_key: str | None = Query(default=None),
    mode: str | None = Query(default=None, pattern="^(paper|live)$"),
    week_start: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return _ok(
            await live_trading_service.weekly_analysis(
                week_start=week_start,
                profile_key=profile_key,
                mode=mode,
            )
        )
    except Exception as exc:
        raise _error(exc) from exc

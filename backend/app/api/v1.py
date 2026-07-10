from __future__ import annotations

import hashlib
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.live_trading import LivePreflightRequest
from app.backtest.api import RunBacktestRequest, run_backtest
from app.core.config import settings
from app.core.contracts import Environment
from app.db.models.research_lineage import DataSnapshot, JobEvent, PersistentJob, StrategyRelease
from app.db.models.strategy import Strategy
from app.db.sqlite import get_async_session
from app.services.dataset_manifest import evaluate_dataset_readiness, read_dataset_manifest
from app.services.live_control import live_control_sessions
from app.services.live_trading import live_trading_service
from app.services.qmt_trading import qmt_trading_service
from app.services.research_lineage import ResearchLineageService
from app.services.tushare_relay_sync import dataset_coverage

router = APIRouter(tags=["v1"])

_READINESS_POLICIES = {
    "klines_daily": ("trade_date", 5),
    "klines_minute_timer": ("datetime", 5),
    "factor_values": ("trade_date", 5),
    "stock_indicators": ("trade_date", 10),
}


class DataSnapshotCreateRequest(BaseModel):
    environment: Environment = Environment.RESEARCH
    datasets: list[str] = Field(default_factory=lambda: list(_READINESS_POLICIES))


class StrategyReleaseCreateRequest(BaseModel):
    strategy_id: int
    data_snapshot_id: str
    engine: str = "akquant"
    engine_version: str
    git_commit: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    universe: dict[str, Any] = Field(default_factory=dict)
    cost_model: dict[str, Any] = Field(default_factory=dict)
    factor_params_hashes: dict[str, str] = Field(default_factory=dict)


class ResearchArtifactCreateRequest(BaseModel):
    kind: Literal["validation", "backtest", "walk_forward", "paper_shadow"]
    validation_status: Literal["valid", "invalid"]
    start_date: date | None = None
    end_date: date | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)
    report_path: str | None = None
    checksum: str | None = None


class ReleasePromoteRequest(BaseModel):
    target_status: Literal["validated", "paper_approved", "live_approved", "retired"]


class LiveControlUnlockRequest(BaseModel):
    secret: str
    expected_account_mask: str


class V1LiveSubmitRequest(BaseModel):
    mode: Literal["paper", "live"] = "paper"
    orders: list[dict[str, Any]] = Field(default_factory=list)
    confirm: bool = False
    release_id: str | None = None
    idempotency_key: str | None = None
    expected_account_mask: str | None = None


class V1BacktestRequest(RunBacktestRequest):
    release_id: str
    data_snapshot_id: str


def _readiness_item(dataset: str, *, as_of: date) -> dict[str, Any]:
    date_column, max_age_days = _READINESS_POLICIES[dataset]
    root = (
        Path(settings.factor_value_store_dir)
        if dataset == "factor_values" and settings.factor_value_store_dir
        else Path(settings.parquet_data_dir) / dataset
    )
    manifest = read_dataset_manifest(root)
    if manifest is not None:
        readiness = evaluate_dataset_readiness(manifest, as_of=as_of, max_age_days=max_age_days)
        return {
            **readiness.to_dict(),
            "row_count": manifest.row_count,
            "file_count": manifest.file_count,
            "source": "manifest",
            "schema_hash": manifest.schema_hash,
        }
    coverage = dataset_coverage(dataset, date_column)
    max_date = coverage.get("max_date")
    if coverage.get("error"):
        status = "invalid"
        age_days = None
    elif not max_date:
        status = "missing"
        age_days = None
    else:
        try:
            age_days = max(0, (as_of - date.fromisoformat(str(max_date)[:10])).days)
            status = "ready" if age_days <= max_age_days else "stale"
        except ValueError:
            status = "invalid"
            age_days = None
    return {
        "dataset": dataset,
        "status": status,
        "age_days": age_days,
        "max_date": max_date,
        "reason": coverage.get("error"),
        "row_count": coverage.get("row_count"),
        "file_count": coverage.get("file_count"),
        "source": coverage.get("source", "coverage"),
        "schema_hash": None,
    }


def build_readiness_payload(*, as_of: date | None = None, datasets: list[str] | None = None) -> dict[str, Any]:
    current_date = as_of or date.today()
    requested = datasets or list(_READINESS_POLICIES)
    unknown = sorted(set(requested) - set(_READINESS_POLICIES))
    if unknown:
        raise ValueError(f"Unknown readiness datasets: {unknown}")
    items = {name: _readiness_item(name, as_of=current_date) for name in requested}
    statuses = {str(item["status"]) for item in items.values()}
    overall = "invalid" if "invalid" in statuses else ("degraded" if statuses & {"missing", "stale"} else "ready")
    return {
        "as_of": current_date.isoformat(),
        "environment": "live" if settings.live_trading_enable_order_submit else "paper",
        "overall_status": overall,
        "datasets": items,
        "trading": {
            "order_submit_enabled": bool(settings.live_trading_enable_order_submit),
            "auto_execute_enabled": bool(settings.live_trading_auto_execute_enabled),
            "control_secret_configured": bool(settings.live_trading_control_secret),
        },
    }


def _snapshot_payload(snapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "environment": snapshot.environment,
        "dataset_versions": snapshot.dataset_versions,
        "freshness": snapshot.freshness,
        "schema_hash": snapshot.schema_hash,
        "status": snapshot.status,
        "created_at": snapshot.created_at.isoformat() if snapshot.created_at else None,
    }


def _release_payload(release: StrategyRelease) -> dict[str, Any]:
    return {
        "id": release.id,
        "strategy_id": release.strategy_id,
        "data_snapshot_id": release.data_snapshot_id,
        "code_hash": release.code_hash,
        "git_commit": release.git_commit,
        "engine": release.engine,
        "engine_version": release.engine_version,
        "parameters": release.parameters,
        "universe": release.universe,
        "cost_model": release.cost_model,
        "factor_params_hashes": release.factor_params_hashes,
        "status": release.status,
        "approved_at": release.approved_at.isoformat() if release.approved_at else None,
    }


@router.get("/readiness")
async def readiness() -> dict[str, Any]:
    return build_readiness_payload()


@router.post("/data-snapshots")
async def create_data_snapshot(
    req: DataSnapshotCreateRequest,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        readiness_payload = build_readiness_payload(datasets=req.datasets)
        dataset_versions = {
            name: item.get("max_date") for name, item in readiness_payload["datasets"].items()
        }
        freshness = {name: item.get("status") for name, item in readiness_payload["datasets"].items()}
        status = "ready" if readiness_payload["overall_status"] == "ready" else "invalid"
        snapshot = await ResearchLineageService(session).create_data_snapshot(
            environment=req.environment,
            dataset_versions=dataset_versions,
            freshness=freshness,
            status=status,
        )
        return _snapshot_payload(snapshot)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/strategy-releases")
async def create_strategy_release(
    req: StrategyReleaseCreateRequest,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        release = await ResearchLineageService(session).create_strategy_release(**req.model_dump())
        return _release_payload(release)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/strategy-releases/{release_id}/artifacts")
async def create_research_artifact(
    release_id: str,
    req: ResearchArtifactCreateRequest,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        artifact = await ResearchLineageService(session).add_artifact(
            release_id=release_id,
            **req.model_dump(),
        )
        return {"id": artifact.id, "release_id": artifact.release_id, "kind": artifact.kind}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/strategy-releases/{release_id}/promote")
async def promote_strategy_release(
    release_id: str,
    req: ReleasePromoteRequest,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        release = await ResearchLineageService(session).promote_release(release_id, req.target_status)
        return _release_payload(release)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backtests")
async def submit_backtest(
    req: V1BacktestRequest,
    session: AsyncSession = Depends(get_async_session),
):
    release = await session.get(StrategyRelease, req.release_id)
    if release is None:
        raise HTTPException(status_code=404, detail="Strategy release not found")
    if release.status not in {"validated", "paper_approved", "live_approved"}:
        raise HTTPException(status_code=409, detail="Strategy release must be validated before backtest")
    if release.data_snapshot_id != req.data_snapshot_id:
        raise HTTPException(status_code=400, detail="Data snapshot does not match strategy release")
    snapshot = await session.get(DataSnapshot, req.data_snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Data snapshot not found")
    if snapshot.status != "ready":
        raise HTTPException(status_code=409, detail="Data snapshot is not ready")
    if req.strategy_id is not None and req.strategy_id != release.strategy_id:
        raise HTTPException(status_code=400, detail="Strategy does not match strategy release")
    if req.engine != release.engine:
        raise HTTPException(status_code=400, detail="Backtest engine does not match strategy release")
    strategy = await session.get(Strategy, release.strategy_id)
    if strategy is None:
        raise HTTPException(status_code=404, detail="Released strategy not found")
    current_code_hash = hashlib.sha256(strategy.code.encode("utf-8")).hexdigest()
    if current_code_hash != release.code_hash:
        raise HTTPException(status_code=409, detail="Strategy code changed after release")
    req.strategy_id = release.strategy_id
    req.strategy_code = strategy.code
    req.strategy_params = dict(release.parameters or {})
    universe = dict(release.universe or {})
    if universe.get("universe_mode") or universe.get("type"):
        req.universe_mode = str(universe.get("universe_mode") or universe["type"])
    if universe.get("index_symbol"):
        req.index_symbol = str(universe["index_symbol"])
    if isinstance(universe.get("symbols"), list):
        req.symbols = [str(symbol) for symbol in universe["symbols"]]
    for field_name in (
        "commission_rate",
        "slippage",
        "stamp_tax_rate",
        "transfer_fee_rate",
        "min_commission",
        "volume_limit_pct",
    ):
        if field_name in release.cost_model:
            setattr(req, field_name, release.cost_model[field_name])
    return await run_backtest(req)


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, session: AsyncSession = Depends(get_async_session)) -> dict[str, Any]:
    job = await session.get(PersistentJob, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "id": job.id,
        "kind": job.kind,
        "title": job.title,
        "status": job.status,
        "progress": job.progress,
        "result_ref": job.result_ref,
        "error": job.error,
        "heartbeat_at": job.heartbeat_at.isoformat() if job.heartbeat_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }


@router.post("/live/control-session")
async def unlock_live_control(req: LiveControlUnlockRequest) -> dict[str, Any]:
    status = await qmt_trading_service.status()
    actual_mask = str(status.get("account_id") or "")
    try:
        control = live_control_sessions.unlock(
            secret=req.secret,
            expected_account_mask=req.expected_account_mask,
            actual_account_mask=actual_mask,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return {
        "token": control.token,
        "account_mask": control.account_mask,
        "ttl_seconds": settings.live_trading_control_session_ttl_seconds,
    }


@router.post("/live/preflight")
async def v1_live_preflight(req: LivePreflightRequest) -> dict[str, Any]:
    return await live_trading_service.preflight(
        profile_key=req.profile_key,
        mode=req.mode,
        params=req.params,
        manual_account=req.manual_account,
        evaluate_pipeline=req.evaluate_pipeline,
        prepare_dependencies=req.prepare_dependencies,
    )


@router.post("/live/orders/submit")
async def v1_submit_live_orders(
    req: V1LiveSubmitRequest,
    x_gaoshou_control_token: str | None = Header(default=None),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    if req.mode == "paper":
        return await live_trading_service.submit_orders(
            req.orders,
            mode="paper",
            confirm=req.confirm,
            trigger_source="manual",
        )
    if not all((req.release_id, req.idempotency_key, req.expected_account_mask, x_gaoshou_control_token)):
        raise HTTPException(
            status_code=400,
            detail="Live submit requires release_id, idempotency_key, expected_account_mask, and control token",
        )
    release = await session.get(StrategyRelease, req.release_id)
    if release is None or release.status != "live_approved":
        raise HTTPException(status_code=403, detail="Strategy release is not live_approved")
    mismatched = [
        order for order in req.orders if int(order.get("strategy_id") or 0) != int(release.strategy_id)
    ]
    if mismatched:
        raise HTTPException(status_code=400, detail="Order strategy_id does not match release")
    qmt_status = await qmt_trading_service.status()
    actual_mask = str(qmt_status.get("account_id") or "")
    try:
        live_control_sessions.validate(
            token=x_gaoshou_control_token,
            expected_account_mask=req.expected_account_mask,
            actual_account_mask=actual_mask,
        )
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    idempotency_hash = hashlib.sha256(req.idempotency_key.encode("utf-8")).hexdigest()[:48]
    job_id = f"live-submit:{idempotency_hash}"
    if await session.get(PersistentJob, job_id) is not None:
        raise HTTPException(status_code=409, detail="Duplicate live submit idempotency key")
    job = PersistentJob(
        id=job_id,
        kind="live_order_submit",
        title=f"Live order submit {release.id}",
        status="running",
        progress=0.0,
        payload={"release_id": release.id, "order_count": len(req.orders)},
        heartbeat_at=datetime.now(),
    )
    session.add(job)
    await session.flush()
    session.add(JobEvent(
        job_id=job.id,
        event_type="started",
        data={"release_id": release.id, "order_count": len(req.orders)},
    ))
    try:
        result = await live_trading_service.submit_orders(
            req.orders,
            mode="live",
            confirm=req.confirm,
            trigger_source="manual",
        )
    except Exception as exc:
        job.status = "failed"
        job.progress = 1.0
        job.error = str(exc)
        job.finished_at = datetime.now()
        session.add(JobEvent(
            job_id=job.id,
            event_type="failed",
            data={"error": str(exc)},
        ))
        await session.commit()
        raise HTTPException(status_code=502, detail="Live order submission failed") from exc
    job.status = "succeeded"
    job.progress = 1.0
    job.finished_at = datetime.now()
    job.result_ref = str(result.get("run_id") or "")
    session.add(JobEvent(
        job_id=job.id,
        event_type="succeeded",
        data={"result_ref": job.result_ref},
    ))
    return result

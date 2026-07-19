"""REST and SSE contracts for the market trend radar."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from typing import Annotated, Any, Literal, cast
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import StreamingResponse

from app.db.models.market_radar import (
    MarketAlertEvent,
    MarketAlertRule,
    MarketRadarSnapshot,
)
from app.db.sqlite import get_async_session
from app.services.market_radar import MarketRadarService
from app.services.market_radar_contracts import (
    FreshnessStatus,
    RadarSnapshotEnvelope,
    SnapshotType,
    StreamEvent,
)
from app.services.market_radar_store import MarketRadarStore, load_json_object
from app.services.runtime_tasks import get_task, register_task, update_task

router = APIRouter()

AlertStatus = Literal["active", "acknowledged", "dismissed", "resolved"]
Severity = Literal["low", "medium", "high"]
Scope = Literal["market", "sector", "symbol", "data"]
BreadthMode = Literal["percent", "count"]
CrowdingScope = Literal["market", "sector", "symbol"]

_rule_mutation_lock = asyncio.Lock()
_REFRESH_ERROR_CODE = "MARKET_RADAR_REFRESH_FAILED"
_REFRESH_ERROR_MESSAGE = "market radar refresh failed"
_CROWDING_SUBJECT_REASON = "crowding subject is not available"


async def _rule_write_guard():
    async with _rule_mutation_lock:
        yield


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    as_of: str | None = None
    status: str
    reason: str | None = None


class MarketRadarEnvelope[DataT](BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: str | None
    computed_at: str
    status: Literal["fresh", "partial", "stale", "unavailable"]
    confidence: float = Field(ge=0, le=1)
    realtime_mode: Literal["push", "polling_30s", "offline", "closed"]
    sources: list[SourceResponse]
    data: DataT


class AlertPageResponse(StrictModel):
    items: list[dict[str, Any]]
    total: int = Field(ge=0)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)


class RulePageResponse(StrictModel):
    items: list[dict[str, Any]]
    total: int = Field(ge=0)


class RefreshResponse(StrictModel):
    task_id: str
    kind: Literal["market_radar_refresh"]
    status: Literal["queued", "running"]
    refresh_kind: Literal["intraday", "eod"]
    trade_date: str | None


class MetricThresholdParameters(StrictModel):
    metric: Literal[
        "return_pct",
        "drawdown_pct",
        "volume_ratio_20d",
        "down_limit_distance_pct",
        "negative_heat_z20",
        "weighted_sentiment",
    ]
    operator: Literal["lte", "gte", "abs_gte"]
    threshold: float = Field(allow_inf_nan=False)


class MetricThresholdRuleCreate(StrictModel):
    rule_key: str = Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    scope: Literal["symbol"] = "symbol"
    subject: str = Field(pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    title: str = Field(min_length=1, max_length=200)
    direction: Literal["up", "down", "either"]
    rule_type: Literal["metric_threshold"]
    parameters: MetricThresholdParameters
    severity: Severity
    cooldown_seconds: int = Field(default=900, ge=0, le=86400)
    enabled: bool = True


class MetricThresholdRulePatch(StrictModel):
    subject: str | None = Field(default=None, pattern=r"^\d{6}\.(SH|SZ|BJ)$")
    title: str | None = Field(default=None, min_length=1, max_length=200)
    direction: Literal["up", "down", "either"] | None = None
    parameters: MetricThresholdParameters | None = None
    severity: Severity | None = None
    cooldown_seconds: int | None = Field(default=None, ge=0, le=86400)
    enabled: bool | None = None

    @model_validator(mode="after")
    def require_change(self) -> MetricThresholdRulePatch:
        if not self.model_fields_set:
            raise ValueError("at least one rule field must be supplied")
        return self


class RefreshRequest(StrictModel):
    kind: Literal["intraday", "eod"]
    trade_date: date | None = None

    @model_validator(mode="after")
    def require_eod_date(self) -> RefreshRequest:
        if self.kind == "eod" and self.trade_date is None:
            raise ValueError("trade_date is required for an eod refresh")
        if self.kind == "intraday" and self.trade_date is not None:
            raise ValueError("trade_date is only valid for an eod refresh")
        return self


def _service(request: Request) -> MarketRadarService:
    service = getattr(request.app.state, "market_radar_service", None)
    if service is None:
        raise HTTPException(status_code=503, detail="market radar service is not initialized")
    return service


def _feed_mode(service: object) -> str:
    feed = getattr(service, "feed", None)
    current = getattr(feed, "status", None)
    if callable(current):
        current = current()
    mode = getattr(current, "mode", "offline")
    return mode if mode in {"push", "polling_30s", "offline", "closed"} else "offline"


def _unavailable_envelope(service: object, reason: str) -> dict[str, Any]:
    return {
        "as_of": None,
        "computed_at": datetime.now().isoformat(),
        "status": "unavailable",
        "confidence": 0.0,
        "realtime_mode": _feed_mode(service),
        "sources": [],
        "data": {"reason": reason},
    }


def _row_envelope(row: MarketRadarSnapshot) -> RadarSnapshotEnvelope:
    return RadarSnapshotEnvelope(
        snapshot_type=cast(SnapshotType, row.snapshot_type),
        as_of=row.as_of,
        computed_at=row.computed_at,
        status=cast(FreshnessStatus, row.status),
        confidence=row.confidence,
        formula_version=row.formula_version,
        metrics=load_json_object(row.metrics_json, field_name="metrics_json"),
        source_freshness=load_json_object(
            row.source_freshness_json,
            field_name="source_freshness_json",
        ),
    )


async def _latest_snapshot(
    request: Request,
    session: AsyncSession,
    *,
    trade_date: date | None = None,
) -> RadarSnapshotEnvelope | None:
    service = _service(request)
    current = service.current_envelope()
    if current is not None and (trade_date is None or current.as_of.date() == trade_date):
        return current
    statement = select(MarketRadarSnapshot)
    if trade_date is not None:
        start = datetime.combine(trade_date, time.min)
        statement = statement.where(
            MarketRadarSnapshot.as_of >= start,
            MarketRadarSnapshot.as_of < start + timedelta(days=1),
        )
    result = await session.execute(
        statement.order_by(
            MarketRadarSnapshot.as_of.desc(),
            MarketRadarSnapshot.computed_at.desc(),
            MarketRadarSnapshot.id.desc(),
        ).limit(1)
    )
    row = result.scalar_one_or_none()
    return _row_envelope(row) if row is not None else None


def _has_component(snapshot: RadarSnapshotEnvelope, name: str) -> bool:
    value = snapshot.metrics.get(name)
    return isinstance(value, Mapping) and bool(value) and value.get("status") != "unavailable"


async def _snapshot_for_component(
    request: Request,
    session: AsyncSession,
    name: str,
    *,
    trade_date: date | None = None,
) -> RadarSnapshotEnvelope | None:
    service = _service(request)
    current = service.current_envelope()
    if (
        current is not None
        and (trade_date is None or current.as_of.date() == trade_date)
        and _has_component(current, name)
    ):
        return current
    statement = select(MarketRadarSnapshot).where(MarketRadarSnapshot.snapshot_type == "eod")
    if trade_date is not None:
        start = datetime.combine(trade_date, time.min)
        statement = statement.where(
            MarketRadarSnapshot.as_of >= start,
            MarketRadarSnapshot.as_of < start + timedelta(days=1),
        )
    result = await session.execute(
        statement.order_by(
            MarketRadarSnapshot.as_of.desc(),
            MarketRadarSnapshot.computed_at.desc(),
            MarketRadarSnapshot.id.desc(),
        ).limit(120)
    )
    for row in result.scalars():
        candidate = _row_envelope(row)
        if _has_component(candidate, name):
            return candidate
    return await _latest_snapshot(request, session, trade_date=trade_date)


async def _projected_component(
    request: Request,
    session: AsyncSession,
    name: str,
    *,
    trade_date: date | None = None,
) -> tuple[dict[str, Any], RadarSnapshotEnvelope | None]:
    service = _service(request)
    snapshot = await _snapshot_for_component(
        request,
        session,
        name,
        trade_date=trade_date,
    )
    if snapshot is None:
        return _unavailable_envelope(service, "no market radar snapshot is available"), None
    return (
        service.project_snapshot(snapshot, realtime_mode=_feed_mode(service)),
        snapshot,
    )


async def _projected(
    request: Request,
    session: AsyncSession,
    *,
    trade_date: date | None = None,
) -> tuple[dict[str, Any], RadarSnapshotEnvelope | None]:
    service = _service(request)
    snapshot = await _latest_snapshot(request, session, trade_date=trade_date)
    if snapshot is None:
        return _unavailable_envelope(service, "no market radar snapshot is available"), None
    return service.project_snapshot(snapshot, realtime_mode=_feed_mode(service)), snapshot


def _with_data(envelope: dict[str, Any], data: Any) -> dict[str, Any]:
    return {**envelope, "data": data}


def _component_payload(snapshot: RadarSnapshotEnvelope, name: str) -> dict[str, Any]:
    value = snapshot.metrics.get(name)
    if isinstance(value, dict) and value:
        return deepcopy(value)
    return {
        "status": "unavailable",
        "reason": f"{name} is not available in the selected snapshot",
    }


def _unavailable_crowding_subject(
    envelope: dict[str, Any],
    *,
    scope: CrowdingScope,
    subject: str,
) -> dict[str, Any]:
    source = {
        "name": "crowding_subject",
        "as_of": envelope.get("as_of"),
        "status": "unavailable",
        "reason": _CROWDING_SUBJECT_REASON,
    }
    return {
        **envelope,
        "status": "unavailable",
        "confidence": 0.0,
        "sources": [*list(envelope.get("sources") or []), source],
        "data": {
            "scope": scope,
            "subject": subject,
            "items": [],
            "reason": _CROWDING_SUBJECT_REASON,
        },
    }


async def _resource_envelope(
    request: Request,
    session: AsyncSession,
    data: Any,
) -> dict[str, Any]:
    envelope, _snapshot = await _projected(request, session)
    return _with_data(envelope, data)


def _event_payload(event: MarketAlertEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "rule_id": event.rule_id,
        "snapshot_id": event.snapshot_id,
        "scope": event.scope,
        "subject": event.subject,
        "direction": event.direction,
        "severity": event.severity,
        "status": event.status,
        "title": event.title,
        "explanation": event.explanation,
        "dedupe_key": event.dedupe_key,
        "evidence": load_json_object(event.evidence_json, field_name="evidence_json"),
        "triggered_at": event.triggered_at.isoformat(),
        "last_seen_at": event.last_seen_at.isoformat(),
        "acknowledged_at": (
            event.acknowledged_at.isoformat() if event.acknowledged_at is not None else None
        ),
        "dismissed_at": event.dismissed_at.isoformat() if event.dismissed_at is not None else None,
        "resolved_at": event.resolved_at.isoformat() if event.resolved_at is not None else None,
        "last_notified_at": (
            event.last_notified_at.isoformat() if event.last_notified_at is not None else None
        ),
        "occurrence_count": event.occurrence_count,
        "clear_streak": event.clear_streak,
    }


def _rule_payload(rule: MarketAlertRule) -> dict[str, Any]:
    parameters = load_json_object(rule.parameters_json, field_name="parameters_json")
    return {
        "id": rule.id,
        "rule_key": rule.rule_key,
        "version": rule.version,
        "scope": rule.scope,
        "subject": rule.subject,
        "title": str(parameters.pop("title", rule.rule_key)),
        "direction": str(parameters.pop("direction", "either")),
        "rule_type": rule.rule_type,
        "parameters": parameters,
        "severity": rule.severity,
        "cooldown_seconds": rule.cooldown_seconds,
        "enabled": rule.enabled,
        "source": rule.source,
        "created_at": rule.created_at.isoformat(),
        "updated_at": rule.updated_at.isoformat(),
    }


@router.get("/overview", response_model=MarketRadarEnvelope[dict[str, Any]])
async def get_overview(
    request: Request,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    envelope, snapshot = await _projected(request, session)
    if snapshot is None:
        return envelope
    counts = {value: 0 for value in ("active", "acknowledged", "dismissed", "resolved")}
    result = await session.execute(
        select(MarketAlertEvent.status, func.count(MarketAlertEvent.id)).group_by(
            MarketAlertEvent.status
        )
    )
    counts.update({str(row[0]): int(row[1]) for row in result})
    active_high = await session.scalar(
        select(func.count(MarketAlertEvent.id)).where(
            MarketAlertEvent.status == "active",
            MarketAlertEvent.severity == "high",
        )
    )
    overview = deepcopy(dict(snapshot.metrics.get("overview") or {}))
    overview["alert_counts"] = {**counts, "active_high": int(active_high or 0)}
    return _with_data(envelope, overview)


@router.get("/breadth", response_model=MarketRadarEnvelope[dict[str, Any]])
async def get_breadth(
    request: Request,
    days: Annotated[int, Query(ge=1, le=120)] = 15,
    mode: BreadthMode = "percent",
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    envelope, snapshot = await _projected(request, session)
    if snapshot is None:
        return envelope
    breadth = _component_payload(snapshot, "breadth")
    merged_days: dict[str, dict[str, Any]] = {}
    for item in list(breadth.get("days") or []):
        trade_date = str(item.get("trade_date") or "")
        if trade_date:
            merged_days[trade_date] = deepcopy(item)
    history_result = await session.execute(
        select(MarketRadarSnapshot)
        .where(MarketRadarSnapshot.snapshot_type == "eod")
        .order_by(MarketRadarSnapshot.as_of.desc(), MarketRadarSnapshot.id.desc())
        .limit(120)
    )
    for row in history_result.scalars():
        history = _row_envelope(row).metrics.get("breadth")
        if not isinstance(history, Mapping):
            continue
        for item in list(history.get("days") or []):
            trade_date = str(item.get("trade_date") or "")
            if trade_date:
                merged_days.setdefault(trade_date, deepcopy(item))
    values = [merged_days[key] for key in sorted(merged_days)][-days:]
    for item in values:
        buckets = (item.get("breadth") or {}).get("buckets") or {}
        for bucket in buckets.values():
            bucket["value"] = bucket.get("count" if mode == "count" else "percentage")
    breadth["days"] = values
    breadth["mode"] = mode
    breadth["indices"] = deepcopy(snapshot.metrics.get("indices") or {})
    return _with_data(envelope, breadth)


@router.get("/limit-ladder", response_model=MarketRadarEnvelope[dict[str, Any]])
async def get_limit_ladder(
    request: Request,
    trade_date: date | None = None,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    envelope, snapshot = await _projected_component(
        request, session, "limit_ladder", trade_date=trade_date
    )
    if snapshot is None:
        return envelope
    return _with_data(envelope, _component_payload(snapshot, "limit_ladder"))


@router.get("/crowding", response_model=MarketRadarEnvelope[dict[str, Any]])
async def get_crowding(
    request: Request,
    scope: CrowdingScope = "market",
    subject: str | None = None,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    if scope != "market" and not subject:
        raise HTTPException(status_code=422, detail="subject is required for non-market crowding")
    envelope, snapshot = await _projected_component(request, session, "crowding")
    if snapshot is None:
        return envelope
    if scope == "market":
        data = _component_payload(snapshot, "crowding")
    elif scope == "sector":
        normalized_subject = subject.strip() if subject is not None else ""
        sectors = deepcopy(dict(snapshot.metrics.get("sectors") or {}))
        items = list(sectors.get("sectors") or sectors.get("items") or [])
        items = [item for item in items if item.get("industry") == normalized_subject]
        if not items:
            return _unavailable_crowding_subject(
                envelope,
                scope=scope,
                subject=normalized_subject,
            )
        data = {"scope": scope, "subject": normalized_subject, "items": items}
    else:
        normalized_subject = subject.strip().upper() if subject is not None else ""
        focus = deepcopy(dict(snapshot.metrics.get("focus") or {}))
        by_symbol = dict(focus.get("metric_status") or {})
        if normalized_subject not in by_symbol or by_symbol[normalized_subject] is None:
            return _unavailable_crowding_subject(
                envelope,
                scope=scope,
                subject=normalized_subject,
            )
        data = {
            "scope": scope,
            "subject": normalized_subject,
            "items": {normalized_subject: by_symbol[normalized_subject]},
        }
    return _with_data(envelope, data)


@router.get("/sectors", response_model=MarketRadarEnvelope[dict[str, Any]])
async def get_sectors(
    request: Request,
    trade_date: date | None = None,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    envelope, snapshot = await _projected_component(
        request, session, "sectors", trade_date=trade_date
    )
    if snapshot is None:
        return envelope
    return _with_data(envelope, _component_payload(snapshot, "sectors"))


@router.get("/alerts", response_model=MarketRadarEnvelope[AlertPageResponse])
async def list_alerts(
    request: Request,
    status_filter: Annotated[AlertStatus | None, Query(alias="status")] = None,
    severity: Severity | None = None,
    scope: Scope | None = None,
    subject: str | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 50,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    filters = []
    if status_filter is not None:
        filters.append(MarketAlertEvent.status == status_filter)
    if severity is not None:
        filters.append(MarketAlertEvent.severity == severity)
    if scope is not None:
        filters.append(MarketAlertEvent.scope == scope)
    if subject is not None:
        filters.append(MarketAlertEvent.subject == subject.strip().upper())
    if start_at is not None:
        filters.append(MarketAlertEvent.triggered_at >= start_at)
    if end_at is not None:
        filters.append(MarketAlertEvent.triggered_at <= end_at)
    total = await session.scalar(select(func.count(MarketAlertEvent.id)).where(*filters))
    result = await session.execute(
        select(MarketAlertEvent)
        .where(*filters)
        .order_by(MarketAlertEvent.triggered_at.desc(), MarketAlertEvent.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    return await _resource_envelope(
        request,
        session,
        {
            "items": [_event_payload(item) for item in result.scalars()],
            "total": int(total or 0),
            "page": page,
            "page_size": page_size,
        },
    )


@router.get("/alerts/{event_id}", response_model=MarketRadarEnvelope[dict[str, Any]])
async def get_alert(
    request: Request,
    event_id: int,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    event = await session.get(MarketAlertEvent, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="market alert event not found")
    return await _resource_envelope(request, session, _event_payload(event))


async def _transition_alert(
    session: AsyncSession,
    event_id: int,
    *,
    action: Literal["acknowledge", "dismiss"],
) -> dict[str, Any]:
    store = MarketRadarStore(session)
    try:
        if action == "acknowledge":
            event = await store.acknowledge_event(event_id, at=datetime.now())
        else:
            event = await store.dismiss_event(event_id, at=datetime.now())
    except ValueError as exc:
        message = str(exc)
        code = 404 if "not found" in message else 409
        raise HTTPException(status_code=code, detail=message) from exc
    return _event_payload(event)


@router.post(
    "/alerts/{event_id}/acknowledge",
    response_model=MarketRadarEnvelope[dict[str, Any]],
)
async def acknowledge_alert(
    request: Request,
    event_id: int,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    payload = await _transition_alert(session, event_id, action="acknowledge")
    return await _resource_envelope(request, session, payload)


@router.post(
    "/alerts/{event_id}/dismiss",
    response_model=MarketRadarEnvelope[dict[str, Any]],
)
async def dismiss_alert(
    request: Request,
    event_id: int,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    payload = await _transition_alert(session, event_id, action="dismiss")
    return await _resource_envelope(request, session, payload)


@router.get("/rules", response_model=MarketRadarEnvelope[RulePageResponse])
async def list_rules(
    request: Request,
    include_disabled: bool = False,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    result = await session.execute(
        select(MarketAlertRule).order_by(
            MarketAlertRule.rule_key,
            MarketAlertRule.version.desc(),
            MarketAlertRule.id.desc(),
        )
    )
    latest: dict[str, MarketAlertRule] = {}
    for item in result.scalars():
        latest.setdefault(item.rule_key, item)
    items = [_rule_payload(item) for item in latest.values() if include_disabled or item.enabled]
    return await _resource_envelope(
        request,
        session,
        {"items": items, "total": len(items)},
    )


@router.post(
    "/rules",
    status_code=status.HTTP_201_CREATED,
    response_model=MarketRadarEnvelope[dict[str, Any]],
)
async def create_rule(
    request: Request,
    payload: MetricThresholdRuleCreate,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    async with _rule_mutation_lock:
        existing = await session.scalar(
            select(func.count(MarketAlertRule.id)).where(
                MarketAlertRule.rule_key == payload.rule_key
            )
        )
        if existing:
            raise HTTPException(status_code=409, detail="rule_key already exists")
        try:
            rule = await MarketRadarStore(session).upsert_rule(
                rule_key=payload.rule_key,
                version=1,
                scope=payload.scope,
                subject=payload.subject,
                rule_type=payload.rule_type,
                parameters={
                    **payload.parameters.model_dump(),
                    "title": payload.title,
                    "direction": payload.direction,
                },
                severity=payload.severity,
                cooldown_seconds=payload.cooldown_seconds,
                enabled=payload.enabled,
                source="user",
            )
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            raise HTTPException(status_code=409, detail="rule_key already exists") from exc
        return await _resource_envelope(request, session, _rule_payload(rule))


async def _resolve_rule_events(
    session: AsyncSession,
    *,
    rule_ids: list[int],
    at: datetime,
) -> None:
    if not rule_ids:
        return
    await session.execute(
        update(MarketAlertEvent)
        .where(
            MarketAlertEvent.rule_id.in_(rule_ids),
            MarketAlertEvent.status.in_(("active", "acknowledged", "dismissed")),
        )
        .values(status="resolved", resolved_at=at, updated_at=at)
        .execution_options(synchronize_session="fetch")
    )


@router.patch("/rules/{rule_id}", response_model=MarketRadarEnvelope[dict[str, Any]])
async def patch_rule(
    request: Request,
    rule_id: int,
    payload: MetricThresholdRulePatch,
    _guard: None = Depends(_rule_write_guard),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    current = await session.get(MarketAlertRule, rule_id)
    if current is None:
        raise HTTPException(status_code=404, detail="market alert rule not found")
    if current.source != "user":
        raise HTTPException(status_code=403, detail="system rules cannot be replaced")
    latest = await session.scalar(
        select(MarketAlertRule)
        .where(MarketAlertRule.rule_key == current.rule_key)
        .order_by(MarketAlertRule.version.desc(), MarketAlertRule.id.desc())
        .limit(1)
    )
    if latest is None or latest.id != current.id:
        raise HTTPException(status_code=409, detail="only the latest rule version can be patched")

    current_enabled = current.enabled
    existing_parameters = load_json_object(current.parameters_json, field_name="parameters_json")
    title = payload.title or str(existing_parameters.pop("title", current.rule_key))
    direction = payload.direction or str(existing_parameters.pop("direction", "either"))
    parameters = (
        payload.parameters.model_dump() if payload.parameters is not None else existing_parameters
    )
    now = datetime.now()
    rows = list(
        (
            await session.execute(
                select(MarketAlertRule).where(MarketAlertRule.rule_key == current.rule_key)
            )
        ).scalars()
    )
    for row in rows:
        row.enabled = False
    await _resolve_rule_events(session, rule_ids=[row.id for row in rows], at=now)
    replacement = await MarketRadarStore(session).upsert_rule(
        rule_key=current.rule_key,
        version=current.version + 1,
        scope=current.scope,
        subject=payload.subject or current.subject,
        rule_type=current.rule_type,
        parameters={**parameters, "title": title, "direction": direction},
        severity=payload.severity or current.severity,
        cooldown_seconds=(
            payload.cooldown_seconds
            if payload.cooldown_seconds is not None
            else current.cooldown_seconds
        ),
        enabled=payload.enabled if payload.enabled is not None else current_enabled,
        source="user",
    )
    await session.commit()
    return await _resource_envelope(request, session, _rule_payload(replacement))


@router.delete("/rules/{rule_id}", response_model=MarketRadarEnvelope[dict[str, Any]])
async def delete_rule(
    request: Request,
    rule_id: int,
    _guard: None = Depends(_rule_write_guard),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    rule = await session.get(MarketAlertRule, rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail="market alert rule not found")
    if rule.source != "user":
        raise HTTPException(status_code=403, detail="system rules cannot be disabled here")
    rule.enabled = False
    rule.updated_at = datetime.now()
    await _resolve_rule_events(session, rule_ids=[rule.id], at=rule.updated_at)
    await session.flush()
    await session.commit()
    return await _resource_envelope(request, session, _rule_payload(rule))


async def _run_refresh(
    service: MarketRadarService,
    task_id: str,
    payload: RefreshRequest,
) -> None:
    update_task(task_id, status="running", progress=0.1)
    try:
        if payload.kind == "intraday":
            snapshot = await service.refresh_intraday()
        else:
            assert payload.trade_date is not None
            snapshot = await service.refresh_eod(payload.trade_date)
        update_task(
            task_id,
            status="succeeded",
            progress=1.0,
            result_ref=snapshot.as_of.isoformat(),
            meta={"snapshot_type": snapshot.snapshot_type, "status": snapshot.status},
        )
    except asyncio.CancelledError:
        update_task(task_id, status="cancelled", error="market radar refresh cancelled")
        raise
    except Exception as exc:
        logger.warning(
            "Market radar refresh task {} failed: {}",
            task_id,
            type(exc).__name__,
        )
        update_task(
            task_id,
            status="failed",
            error=_REFRESH_ERROR_MESSAGE,
            meta={"error_code": _REFRESH_ERROR_CODE},
        )


@router.post(
    "/refresh",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=MarketRadarEnvelope[RefreshResponse],
)
async def refresh_radar(request: Request, payload: RefreshRequest) -> dict[str, Any]:
    service = _service(request)
    trade_date = payload.trade_date.isoformat() if payload.trade_date else None
    refresh_key = (payload.kind, trade_date)
    refresh_lock = getattr(request.app.state, "market_radar_refresh_lock", None)
    if refresh_lock is None:
        refresh_lock = asyncio.Lock()
        request.app.state.market_radar_refresh_lock = refresh_lock
    tasks = getattr(request.app.state, "market_radar_refresh_tasks", None)
    if tasks is None:
        tasks = set()
        request.app.state.market_radar_refresh_tasks = tasks
    refresh_by_key = getattr(request.app.state, "market_radar_refresh_by_key", None)
    if refresh_by_key is None:
        refresh_by_key = {}
        request.app.state.market_radar_refresh_by_key = refresh_by_key

    async with refresh_lock:
        existing = refresh_by_key.get(refresh_key)
        if existing is not None and not existing[1].done():
            task_id = existing[0]
            runtime = get_task(task_id)
            task_status = (
                runtime["status"]
                if runtime is not None and runtime["status"] in {"queued", "running"}
                else "running"
            )
        else:
            if existing is not None:
                refresh_by_key.pop(refresh_key, None)
            task_id = f"market-radar-{uuid4().hex}"
            task_status = "queued"
            register_task(
                task_id=task_id,
                kind="market_radar_refresh",
                title=f"Market radar {payload.kind} refresh",
                status=task_status,
                meta={
                    "refresh_kind": payload.kind,
                    "trade_date": trade_date,
                },
            )
            task = asyncio.create_task(
                _run_refresh(service, task_id, payload),
                name=f"market-radar-refresh-{task_id}",
            )
            tasks.add(task)
            refresh_by_key[refresh_key] = (task_id, task)

            def cleanup_refresh(done: asyncio.Task[None]) -> None:
                tasks.discard(done)
                current = refresh_by_key.get(refresh_key)
                if current is not None and current[0] == task_id and current[1] is done:
                    refresh_by_key.pop(refresh_key, None)

            task.add_done_callback(cleanup_refresh)

    task_payload = {
        "task_id": task_id,
        "kind": "market_radar_refresh",
        "status": task_status,
        "refresh_kind": payload.kind,
        "trade_date": trade_date,
    }
    current = service.current_envelope()
    envelope = (
        service.project_snapshot(current, realtime_mode=_feed_mode(service))
        if current is not None
        else _unavailable_envelope(service, "no market radar snapshot is available")
    )
    return _with_data(envelope, task_payload)


def _sse_frame(event: StreamEvent) -> str:
    payload = {
        **dict(event.data),
        "schema_version": 1,
        "event_id": event.event_id,
        "sequence": event.sequence,
        "occurred_at": event.created_at.isoformat(),
    }
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return f"id: {event.event_id}\nevent: {event.event}\nretry: 5000\ndata: {data}\n\n"


@router.get("/stream")
async def market_radar_stream(
    request: Request,
    session: Annotated[AsyncSession, Depends(get_async_session)],
) -> StreamingResponse:
    service = _service(request)

    async def load_initial():
        snapshot = await _latest_snapshot(request, session)
        result = await session.execute(
            select(MarketAlertEvent)
            .where(
                MarketAlertEvent.status == "active",
                MarketAlertEvent.severity == "high",
            )
            .order_by(MarketAlertEvent.triggered_at, MarketAlertEvent.id)
        )
        return snapshot, tuple(result.scalars())

    try:
        subscription = await service.subscribe_with_initial(
            initial_loader=load_initial,
        )
    finally:
        await session.rollback()

    async def event_source():
        try:
            async for event in subscription:
                yield _sse_frame(event)
        except asyncio.CancelledError:
            raise
        finally:
            await subscription.close()

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


__all__ = ["router"]

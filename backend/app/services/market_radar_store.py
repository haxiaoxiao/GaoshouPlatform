"""Transaction-scoped persistence helpers for market radar state."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.base import _beijing_now
from app.db.models.market_radar import (
    MarketAlertEvent,
    MarketAlertRule,
    MarketRadarSnapshot,
)

_SNAPSHOT_TYPES = frozenset({"intraday", "eod"})
_SNAPSHOT_STATUSES = frozenset({"fresh", "partial", "stale", "unavailable"})
_SCOPES = frozenset({"market", "sector", "symbol", "data"})
_SEVERITIES = frozenset({"low", "medium", "high"})
_RULE_SOURCES = frozenset({"system", "user"})
_OPEN_EVENT_STATUSES = ("active", "acknowledged", "dismissed")


def dump_json_object(
    value: Mapping[str, Any],
    *,
    field_name: str = "value",
) -> str:
    """Serialize an object deterministically for Text-backed JSON columns."""

    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    try:
        return json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must contain only JSON-compatible values") from exc


def load_json_object(value: str, *, field_name: str = "value") -> dict[str, Any]:
    """Deserialize a Text-backed JSON object without hiding corrupt rows."""

    try:
        decoded = json.loads(value, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} contains invalid JSON") from exc
    if not isinstance(decoded, dict):
        raise ValueError(f"{field_name} must contain a JSON object")
    return decoded


class MarketRadarStore:
    """Repository methods that flush but leave commit/rollback to the caller."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_snapshot(
        self,
        *,
        snapshot_type: str,
        as_of: datetime,
        computed_at: datetime,
        status: str,
        confidence: float,
        formula_version: str,
        metrics: Mapping[str, Any],
        source_freshness: Mapping[str, Any],
    ) -> MarketRadarSnapshot:
        _require_choice("snapshot_type", snapshot_type, _SNAPSHOT_TYPES)
        _require_choice("status", status, _SNAPSHOT_STATUSES)
        _require_naive_datetime("as_of", as_of)
        _require_naive_datetime("computed_at", computed_at)
        if not 0 <= confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        if not formula_version:
            raise ValueError("formula_version must not be empty")

        now = _beijing_now()
        values = {
            "snapshot_type": snapshot_type,
            "as_of": as_of,
            "computed_at": computed_at,
            "status": status,
            "confidence": float(confidence),
            "formula_version": formula_version,
            "metrics_json": dump_json_object(metrics, field_name="metrics_json"),
            "source_freshness_json": dump_json_object(
                source_freshness,
                field_name="source_freshness_json",
            ),
            "created_at": now,
            "updated_at": now,
        }
        statement = insert(MarketRadarSnapshot).values(**values)
        await self.session.execute(
            statement.on_conflict_do_update(
                index_elements=[
                    MarketRadarSnapshot.snapshot_type,
                    MarketRadarSnapshot.as_of,
                    MarketRadarSnapshot.formula_version,
                ],
                set_={
                    key: value
                    for key, value in values.items()
                    if key not in {"created_at", "snapshot_type", "as_of", "formula_version"}
                },
            )
        )
        result = await self.session.execute(
            select(MarketRadarSnapshot)
            .where(
                MarketRadarSnapshot.snapshot_type == snapshot_type,
                MarketRadarSnapshot.as_of == as_of,
                MarketRadarSnapshot.formula_version == formula_version,
            )
            .execution_options(populate_existing=True)
        )
        return result.scalar_one()

    async def get_latest_snapshot(
        self,
        *,
        snapshot_type: str | None = None,
        formula_version: str | None = None,
    ) -> MarketRadarSnapshot | None:
        statement = select(MarketRadarSnapshot)
        if snapshot_type is not None:
            _require_choice("snapshot_type", snapshot_type, _SNAPSHOT_TYPES)
            statement = statement.where(MarketRadarSnapshot.snapshot_type == snapshot_type)
        if formula_version is not None:
            statement = statement.where(
                MarketRadarSnapshot.formula_version == formula_version
            )
        result = await self.session.execute(
            statement.order_by(
                MarketRadarSnapshot.as_of.desc(),
                MarketRadarSnapshot.computed_at.desc(),
                MarketRadarSnapshot.id.desc(),
            ).limit(1)
        )
        return result.scalar_one_or_none()

    async def upsert_rule(
        self,
        *,
        rule_key: str,
        version: int,
        scope: str,
        subject: str,
        rule_type: str,
        parameters: Mapping[str, Any],
        severity: str,
        cooldown_seconds: int,
        enabled: bool,
        source: str,
    ) -> MarketAlertRule:
        if not rule_key:
            raise ValueError("rule_key must not be empty")
        if version < 1:
            raise ValueError("version must be at least 1")
        _require_choice("scope", scope, _SCOPES)
        if not subject:
            raise ValueError("subject must not be empty")
        if not rule_type:
            raise ValueError("rule_type must not be empty")
        _require_choice("severity", severity, _SEVERITIES)
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must not be negative")
        _require_choice("source", source, _RULE_SOURCES)

        now = _beijing_now()
        values = {
            "rule_key": rule_key,
            "version": version,
            "scope": scope,
            "subject": subject,
            "rule_type": rule_type,
            "parameters_json": dump_json_object(
                parameters,
                field_name="parameters_json",
            ),
            "severity": severity,
            "cooldown_seconds": cooldown_seconds,
            "enabled": bool(enabled),
            "source": source,
            "created_at": now,
            "updated_at": now,
        }
        statement = insert(MarketAlertRule).values(**values)
        await self.session.execute(
            statement.on_conflict_do_update(
                index_elements=[MarketAlertRule.rule_key, MarketAlertRule.version],
                set_={
                    key: value
                    for key, value in values.items()
                    if key not in {"created_at", "rule_key", "version"}
                },
            )
        )
        result = await self.session.execute(
            select(MarketAlertRule)
            .where(
                MarketAlertRule.rule_key == rule_key,
                MarketAlertRule.version == version,
            )
            .execution_options(populate_existing=True)
        )
        return result.scalar_one()

    async def record_event_hit(
        self,
        *,
        rule_id: int,
        snapshot_id: int | None,
        scope: str,
        subject: str,
        direction: str,
        severity: str,
        title: str,
        explanation: str,
        dedupe_key: str,
        evidence: Mapping[str, Any],
        seen_at: datetime,
        last_notified_at: datetime | None = None,
    ) -> tuple[MarketAlertEvent, bool]:
        _require_choice("scope", scope, _SCOPES)
        _require_choice("severity", severity, _SEVERITIES)
        _require_naive_datetime("seen_at", seen_at)
        if last_notified_at is not None:
            _require_naive_datetime("last_notified_at", last_notified_at)
        if not subject:
            raise ValueError("subject must not be empty")
        for field_name, value in (
            ("direction", direction),
            ("title", title),
            ("explanation", explanation),
            ("dedupe_key", dedupe_key),
        ):
            if not value:
                raise ValueError(f"{field_name} must not be empty")
        if await self.session.get(MarketAlertRule, rule_id) is None:
            raise ValueError(f"Market alert rule {rule_id} not found")
        if (
            snapshot_id is not None
            and await self.session.get(MarketRadarSnapshot, snapshot_id) is None
        ):
            raise ValueError(f"Market radar snapshot {snapshot_id} not found")

        existing = await self._get_open_event(dedupe_key)
        evidence_json = dump_json_object(evidence, field_name="evidence_json")
        if existing is not None:
            if (
                existing.rule_id != rule_id
                or existing.scope != scope
                or existing.subject != subject
            ):
                raise ValueError(f"Open event dedupe key collision: {dedupe_key}")
            if snapshot_id is not None:
                existing.snapshot_id = snapshot_id
            existing.direction = direction
            existing.severity = severity
            existing.title = title
            existing.explanation = explanation
            existing.evidence_json = evidence_json
            existing.last_seen_at = seen_at
            existing.occurrence_count += 1
            existing.clear_streak = 0
            if last_notified_at is not None:
                existing.last_notified_at = last_notified_at
            await self.session.flush()
            return existing, False

        event = MarketAlertEvent(
            rule_id=rule_id,
            snapshot_id=snapshot_id,
            scope=scope,
            subject=subject,
            direction=direction,
            severity=severity,
            status="active",
            title=title,
            explanation=explanation,
            dedupe_key=dedupe_key,
            evidence_json=evidence_json,
            triggered_at=seen_at,
            last_seen_at=seen_at,
            last_notified_at=last_notified_at,
            occurrence_count=1,
            clear_streak=0,
        )
        self.session.add(event)
        await self.session.flush()
        return event, True

    async def acknowledge_event(
        self,
        event_id: int,
        *,
        at: datetime,
    ) -> MarketAlertEvent:
        return await self._transition_event(
            event_id,
            target_status="acknowledged",
            allowed_statuses={"active", "acknowledged"},
            timestamp_field="acknowledged_at",
            at=at,
        )

    async def dismiss_event(
        self,
        event_id: int,
        *,
        at: datetime,
    ) -> MarketAlertEvent:
        return await self._transition_event(
            event_id,
            target_status="dismissed",
            allowed_statuses={"active", "acknowledged", "dismissed"},
            timestamp_field="dismissed_at",
            at=at,
        )

    async def resolve_event(
        self,
        event_id: int,
        *,
        at: datetime,
    ) -> MarketAlertEvent:
        return await self._transition_event(
            event_id,
            target_status="resolved",
            allowed_statuses={*_OPEN_EVENT_STATUSES, "resolved"},
            timestamp_field="resolved_at",
            at=at,
        )

    async def cleanup_intraday_snapshots(self, *, cutoff: datetime) -> int:
        _require_naive_datetime("cutoff", cutoff)
        expired_ids = select(MarketRadarSnapshot.id).where(
            MarketRadarSnapshot.snapshot_type == "intraday",
            MarketRadarSnapshot.as_of < cutoff,
        )
        await self.session.execute(
            update(MarketAlertEvent)
            .where(MarketAlertEvent.snapshot_id.in_(expired_ids))
            .values(snapshot_id=None)
            .execution_options(synchronize_session="fetch")
        )
        result = await self.session.execute(
            delete(MarketRadarSnapshot)
            .where(
                MarketRadarSnapshot.snapshot_type == "intraday",
                MarketRadarSnapshot.as_of < cutoff,
            )
            .execution_options(synchronize_session="fetch")
        )
        await self.session.flush()
        return int(result.rowcount or 0)

    async def _get_open_event(self, dedupe_key: str) -> MarketAlertEvent | None:
        result = await self.session.execute(
            select(MarketAlertEvent)
            .where(
                MarketAlertEvent.dedupe_key == dedupe_key,
                MarketAlertEvent.status.in_(_OPEN_EVENT_STATUSES),
            )
            .order_by(MarketAlertEvent.triggered_at.desc(), MarketAlertEvent.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def _transition_event(
        self,
        event_id: int,
        *,
        target_status: str,
        allowed_statuses: set[str],
        timestamp_field: str,
        at: datetime,
    ) -> MarketAlertEvent:
        _require_naive_datetime("at", at)
        timestamp_column = getattr(MarketAlertEvent, timestamp_field)
        result = await self.session.execute(
            update(MarketAlertEvent)
            .where(
                MarketAlertEvent.id == event_id,
                MarketAlertEvent.status.in_(allowed_statuses),
                MarketAlertEvent.status != target_status,
            )
            .values(
                status=target_status,
                **{
                    timestamp_field: func.coalesce(timestamp_column, at),
                    "updated_at": _beijing_now(),
                },
            )
            .returning(MarketAlertEvent.id)
            .execution_options(synchronize_session=False)
        )
        updated_id = result.scalar_one_or_none()
        event = await self._load_event_from_database(event_id)
        if event is None:
            raise ValueError(f"Market alert event {event_id} not found")
        if updated_id is None:
            if event.status == target_status:
                return event
            raise ValueError(
                "Invalid market alert event transition: "
                f"{event.status} -> {target_status}"
            )
        return event

    async def _load_event_from_database(
        self,
        event_id: int,
    ) -> MarketAlertEvent | None:
        result = await self.session.execute(
            select(MarketAlertEvent)
            .where(MarketAlertEvent.id == event_id)
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()


def _require_choice(field_name: str, value: str, choices: frozenset[str]) -> None:
    if value not in choices:
        expected = ", ".join(sorted(choices))
        raise ValueError(f"{field_name} must be one of: {expected}")


def _require_naive_datetime(field_name: str, value: datetime) -> None:
    if value.tzinfo is not None and value.utcoffset() is not None:
        raise ValueError(f"{field_name} must be a timezone-naive local datetime")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"Non-standard JSON constant is not allowed: {value}")

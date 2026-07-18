from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.services.market_radar_store as market_radar_store_module
from app.db.models.base import Base
from app.db.models.market_radar import MarketAlertEvent, MarketRadarSnapshot
from app.services.market_radar_store import (
    MarketRadarStore,
    dump_json_object,
    load_json_object,
)


async def _sessions(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'market-radar-store.db').as_posix()}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def test_market_radar_json_is_canonical_and_invalid_json_is_explicit():
    payload = {"中文": "值", "a": {"z": 2, "b": 1}}
    encoded = dump_json_object(payload)

    assert encoded == '{"a":{"b":1,"z":2},"中文":"值"}'
    assert load_json_object(encoded, field_name="metrics_json") == payload
    with pytest.raises(ValueError, match="metrics_json contains invalid JSON"):
        load_json_object("{broken", field_name="metrics_json")
    with pytest.raises(ValueError, match="metrics_json must contain a JSON object"):
        load_json_object("[]", field_name="metrics_json")


@pytest.mark.parametrize(
    "encoded",
    (
        '{"value":NaN}',
        '{"value":Infinity}',
        '{"value":-Infinity}',
    ),
)
def test_market_radar_json_rejects_non_standard_numeric_constants(encoded):
    with pytest.raises(ValueError, match="metrics_json contains invalid JSON"):
        load_json_object(encoded, field_name="metrics_json")


@pytest.mark.asyncio
async def test_snapshot_upsert_is_idempotent_and_does_not_auto_commit(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    as_of = datetime(2026, 7, 18, 10, 0)

    async with sessions() as session:
        store = MarketRadarStore(session)
        for invalid_confidence in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValueError, match="confidence must be between 0 and 1"):
                await store.upsert_snapshot(
                    snapshot_type="intraday",
                    as_of=as_of,
                    computed_at=as_of,
                    status="fresh",
                    confidence=invalid_confidence,
                    formula_version="radar-v1",
                    metrics={},
                    source_freshness={},
                )

        original = await store.upsert_snapshot(
            snapshot_type="intraday",
            as_of=as_of,
            computed_at=as_of + timedelta(seconds=1),
            status="partial",
            confidence=0.75,
            formula_version="radar-v1",
            metrics={"breadth": {"valid": 4000}},
            source_freshness={"qmt": {"status": "partial"}},
        )

        async with sessions() as other_session:
            unseen = await other_session.scalar(select(func.count(MarketRadarSnapshot.id)))
            assert unseen == 0

        await session.commit()
        original_id = original.id
        created_at = original.created_at

        updated = await store.upsert_snapshot(
            snapshot_type="intraday",
            as_of=as_of,
            computed_at=as_of + timedelta(seconds=30),
            status="fresh",
            confidence=0.95,
            formula_version="radar-v1",
            metrics={"breadth": {"valid": 5200}},
            source_freshness={"qmt": {"status": "fresh"}},
        )

        assert updated.id == original_id
        assert updated.created_at == created_at
        assert updated.status == "fresh"
        assert updated.confidence == pytest.approx(0.95)
        assert load_json_object(updated.metrics_json, field_name="metrics_json") == {
            "breadth": {"valid": 5200}
        }
        latest = await store.get_latest_snapshot(snapshot_type="intraday")
        assert latest is not None
        assert latest.id == original_id
        assert latest.computed_at == as_of + timedelta(seconds=30)

    await engine.dispose()


@pytest.mark.asyncio
async def test_event_hits_preserve_user_state_and_start_new_cycle_after_resolution(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    first_seen = datetime(2026, 7, 18, 10, 0)

    async with sessions() as session:
        store = MarketRadarStore(session)
        rule = await store.upsert_rule(
            rule_key="market.panic",
            version=1,
            scope="market",
            subject="all_a",
            rule_type="threshold",
            parameters={"operator": "lte", "value": 20},
            severity="medium",
            cooldown_seconds=900,
            enabled=True,
            source="system",
        )
        event, created = await store.record_event_hit(
            rule_id=rule.id,
            snapshot_id=None,
            scope="market",
            subject="all_a",
            direction="negative",
            severity="medium",
            title="Market panic",
            explanation="Breadth fell below threshold",
            dedupe_key="market.panic:all_a",
            evidence={"value": 18, "threshold": 20},
            seen_at=first_seen,
        )
        assert created is True
        assert event.status == "active"
        assert event.occurrence_count == 1

        acknowledged = await store.acknowledge_event(event.id, at=first_seen + timedelta(minutes=1))
        acknowledged_again = await store.acknowledge_event(
            event.id, at=first_seen + timedelta(minutes=2)
        )
        assert acknowledged_again.id == acknowledged.id
        assert acknowledged_again.status == "acknowledged"
        assert acknowledged_again.acknowledged_at == first_seen + timedelta(minutes=1)

        repeated, created = await store.record_event_hit(
            rule_id=rule.id,
            snapshot_id=None,
            scope="market",
            subject="all_a",
            direction="negative",
            severity="high",
            title="Market panic worsened",
            explanation="Breadth deteriorated",
            dedupe_key="market.panic:all_a",
            evidence={"value": 12, "threshold": 20},
            seen_at=first_seen + timedelta(minutes=3),
        )
        assert created is False
        assert repeated.id == event.id
        assert repeated.status == "acknowledged"
        assert repeated.severity == "high"
        assert repeated.occurrence_count == 2
        assert repeated.last_seen_at == first_seen + timedelta(minutes=3)
        assert repeated.clear_streak == 0
        assert load_json_object(repeated.evidence_json, field_name="evidence_json") == {
            "threshold": 20,
            "value": 12,
        }

        dismissed = await store.dismiss_event(event.id, at=first_seen + timedelta(minutes=4))
        assert dismissed.status == "dismissed"
        repeated, created = await store.record_event_hit(
            rule_id=rule.id,
            snapshot_id=None,
            scope="market",
            subject="all_a",
            direction="negative",
            severity="high",
            title="Still panicking",
            explanation="Condition remains true",
            dedupe_key="market.panic:all_a",
            evidence={"value": 10},
            seen_at=first_seen + timedelta(minutes=5),
        )
        assert created is False
        assert repeated.status == "dismissed"

        resolved = await store.resolve_event(event.id, at=first_seen + timedelta(minutes=6))
        resolved_again = await store.resolve_event(event.id, at=first_seen + timedelta(minutes=7))
        assert resolved_again.id == resolved.id
        assert resolved_again.resolved_at == first_seen + timedelta(minutes=6)

        new_cycle, created = await store.record_event_hit(
            rule_id=rule.id,
            snapshot_id=None,
            scope="market",
            subject="all_a",
            direction="negative",
            severity="medium",
            title="Market panic returned",
            explanation="A new unresolved cycle",
            dedupe_key="market.panic:all_a",
            evidence={"value": 19},
            seen_at=first_seen + timedelta(minutes=8),
        )
        assert created is True
        assert new_cycle.id != event.id
        assert new_cycle.status == "active"
        unresolved_count = await session.scalar(
            select(func.count(MarketAlertEvent.id)).where(
                MarketAlertEvent.dedupe_key == "market.panic:all_a",
                MarketAlertEvent.status.in_(("active", "acknowledged", "dismissed")),
            )
        )
        assert unresolved_count == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_event_transitions_reject_invalid_or_missing_events(tmp_path, monkeypatch):
    engine, sessions = await _sessions(tmp_path)
    now = datetime(2026, 7, 18, 11, 0)

    async with sessions() as session:
        store = MarketRadarStore(session)
        rule = await store.upsert_rule(
            rule_key="symbol.limit",
            version=1,
            scope="symbol",
            subject="600000.SH",
            rule_type="price_threshold",
            parameters={"operator": "gte", "value": 12.5},
            severity="high",
            cooldown_seconds=0,
            enabled=True,
            source="user",
        )
        event, _ = await store.record_event_hit(
            rule_id=rule.id,
            snapshot_id=None,
            scope="symbol",
            subject="600000.SH",
            direction="positive",
            severity="high",
            title="Price threshold",
            explanation="Price crossed the configured threshold",
            dedupe_key="symbol.limit:600000.SH",
            evidence={"value": 12.6},
            seen_at=now,
        )

        clock = datetime(2026, 7, 18, 12, 0)
        monkeypatch.setattr(market_radar_store_module, "_beijing_now", lambda: clock)

        acknowledged = await store.acknowledge_event(
            event.id,
            at=now + timedelta(seconds=1),
        )
        acknowledged_updated_at = acknowledged.updated_at
        clock += timedelta(seconds=1)
        acknowledged_again = await store.acknowledge_event(
            event.id,
            at=now + timedelta(seconds=2),
        )
        assert acknowledged_again.acknowledged_at == acknowledged.acknowledged_at
        assert acknowledged_again.updated_at == acknowledged_updated_at

        clock += timedelta(seconds=1)
        dismissed = await store.dismiss_event(event.id, at=now + timedelta(seconds=3))
        dismissed_updated_at = dismissed.updated_at
        clock += timedelta(seconds=1)
        dismissed_again = await store.dismiss_event(event.id, at=now + timedelta(seconds=4))
        assert dismissed_again.dismissed_at == dismissed.dismissed_at
        assert dismissed_again.updated_at == dismissed_updated_at
        with pytest.raises(ValueError, match="Invalid market alert event transition"):
            await store.acknowledge_event(event.id, at=now + timedelta(seconds=5))
        with pytest.raises(ValueError, match="not found"):
            await store.dismiss_event(999_999, at=now)

        clock += timedelta(seconds=1)
        resolved = await store.resolve_event(event.id, at=now + timedelta(seconds=6))
        resolved_updated_at = resolved.updated_at
        clock += timedelta(seconds=1)
        resolved_again = await store.resolve_event(
            event.id,
            at=now + timedelta(seconds=7),
        )
        assert resolved_again.resolved_at == resolved.resolved_at
        assert resolved_again.updated_at == resolved_updated_at
        with pytest.raises(ValueError, match="Invalid market alert event transition"):
            await store.dismiss_event(event.id, at=now + timedelta(seconds=8))

    await engine.dispose()


@pytest.mark.asyncio
async def test_stale_session_cannot_revive_an_event_resolved_by_another_session(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    now = datetime(2026, 7, 18, 11, 30)

    async with sessions() as session_a:
        store_a = MarketRadarStore(session_a)
        rule = await store_a.upsert_rule(
            rule_key="market.concurrent",
            version=1,
            scope="market",
            subject="all_a",
            rule_type="threshold",
            parameters={"operator": "lte", "value": 20},
            severity="high",
            cooldown_seconds=0,
            enabled=True,
            source="system",
        )
        event, _ = await store_a.record_event_hit(
            rule_id=rule.id,
            snapshot_id=None,
            scope="market",
            subject="all_a",
            direction="negative",
            severity="high",
            title="Concurrent transition",
            explanation="Used to verify atomic event transitions",
            dedupe_key="market.concurrent:all_a",
            evidence={"value": 18},
            seen_at=now,
        )
        await session_a.commit()
        assert event.status == "active"

        resolved_at = now + timedelta(seconds=1)
        async with sessions() as session_b:
            resolved = await MarketRadarStore(session_b).resolve_event(
                event.id,
                at=resolved_at,
            )
            assert resolved.status == "resolved"
            await session_b.commit()

        with pytest.raises(ValueError, match="Invalid market alert event transition"):
            await store_a.dismiss_event(event.id, at=now + timedelta(seconds=2))
        assert event.status == "resolved"
        assert event.resolved_at == resolved_at
        assert event.dismissed_at is None

    async with sessions() as verification_session:
        persisted = await verification_session.get(MarketAlertEvent, event.id)
        assert persisted is not None
        assert persisted.status == "resolved"
        assert persisted.resolved_at == resolved_at
        assert persisted.dismissed_at is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_cleanup_removes_only_old_intraday_snapshots_and_preserves_events(tmp_path):
    engine, sessions = await _sessions(tmp_path)
    cutoff = datetime(2026, 4, 19, 0, 0)

    async with sessions() as session:
        store = MarketRadarStore(session)
        old_intraday = await store.upsert_snapshot(
            snapshot_type="intraday",
            as_of=cutoff - timedelta(seconds=1),
            computed_at=cutoff - timedelta(seconds=1),
            status="fresh",
            confidence=0.9,
            formula_version="radar-v1",
            metrics={"kind": "old-intraday"},
            source_freshness={},
        )
        recent_intraday = await store.upsert_snapshot(
            snapshot_type="intraday",
            as_of=cutoff,
            computed_at=cutoff,
            status="fresh",
            confidence=0.9,
            formula_version="radar-v1",
            metrics={"kind": "recent-intraday"},
            source_freshness={},
        )
        old_eod = await store.upsert_snapshot(
            snapshot_type="eod",
            as_of=cutoff - timedelta(days=365),
            computed_at=cutoff - timedelta(days=365),
            status="fresh",
            confidence=0.9,
            formula_version="radar-v1",
            metrics={"kind": "old-eod"},
            source_freshness={},
        )
        rule = await store.upsert_rule(
            rule_key="data.stale",
            version=1,
            scope="data",
            subject="qmt",
            rule_type="freshness",
            parameters={"max_age_seconds": 5},
            severity="high",
            cooldown_seconds=60,
            enabled=True,
            source="system",
        )
        event, _ = await store.record_event_hit(
            rule_id=rule.id,
            snapshot_id=old_intraday.id,
            scope="data",
            subject="qmt",
            direction="negative",
            severity="high",
            title="QMT stale",
            explanation="Realtime quotes are stale",
            dedupe_key="data.stale:qmt",
            evidence={"age_seconds": 6},
            seen_at=cutoff,
        )

        deleted = await store.cleanup_intraday_snapshots(cutoff=cutoff)
        assert deleted == 1
        assert await session.get(MarketRadarSnapshot, old_intraday.id) is None
        assert await session.get(MarketRadarSnapshot, recent_intraday.id) is not None
        assert await session.get(MarketRadarSnapshot, old_eod.id) is not None
        await session.refresh(event)
        assert event.snapshot_id is None
        assert await session.get(MarketAlertEvent, event.id) is not None

    await engine.dispose()

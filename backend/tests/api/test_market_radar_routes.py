from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import date, datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request

from app.api.market_radar import router
from app.db.models.base import Base
from app.db.models.market_radar import (
    MarketAlertEvent,
    MarketAlertRule,
)
from app.db.models.stock import Stock
from app.db.sqlite import get_async_session
from app.services.market_radar import MarketRadarService
from app.services.market_radar_contracts import (
    MetricValue,
    RadarObservation,
    RadarSnapshotEnvelope,
    StreamEvent,
)
from app.services.market_radar_store import MarketRadarStore
from app.services.runtime_tasks import get_task

NOW = datetime(2026, 7, 19, 10, 0)


class FakeSubscription:
    def __init__(self, events: list[StreamEvent]) -> None:
        self._events = list(events)
        self.closed = False

    async def get(self) -> StreamEvent:
        if self._events:
            return self._events.pop(0)
        await asyncio.Future()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True

    def __aiter__(self) -> FakeSubscription:
        return self

    async def __anext__(self) -> StreamEvent:
        return await self.get()


class FakeRadarService:
    def __init__(
        self,
        snapshot: RadarSnapshotEnvelope | None = None,
        *,
        subscription: FakeSubscription | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.subscription = subscription
        self.initial_snapshot: RadarSnapshotEnvelope | None = None
        self.initial_alerts: tuple[MarketAlertEvent, ...] = ()
        self.intraday_refreshes = 0
        self.eod_refreshes: list[date] = []
        self.feed = SimpleNamespace(
            status=SimpleNamespace(
                mode="offline",
                changed_at=NOW,
                last_quote_at=None,
                connection_generation=0,
                reason="miniQMT unavailable",
                market_coverage={},
            )
        )

    def current_envelope(self) -> RadarSnapshotEnvelope | None:
        return self.snapshot

    project_snapshot = staticmethod(MarketRadarService.project_snapshot)

    async def subscribe_with_initial(
        self,
        *,
        initial_loader,
    ) -> FakeSubscription:
        snapshot, alerts = await initial_loader()
        self.initial_snapshot = snapshot
        self.initial_alerts = tuple(alerts)
        assert self.subscription is not None
        return self.subscription

    async def refresh_intraday(self) -> RadarSnapshotEnvelope:
        self.intraday_refreshes += 1
        assert self.snapshot is not None
        return self.snapshot

    async def refresh_eod(self, target_date: date) -> RadarSnapshotEnvelope:
        self.eod_refreshes.append(target_date)
        assert self.snapshot is not None
        return self.snapshot


@pytest_asyncio.fixture
async def radar_factory(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'radar-api.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest_asyncio.fixture
async def radar_session(radar_factory):
    async with radar_factory() as session:
        yield session


@pytest_asyncio.fixture
async def client(radar_factory):
    app = FastAPI()
    app.include_router(router, prefix="/api/market-radar")
    app.state.market_radar_service = FakeRadarService()

    async def session_override() -> AsyncIterator[AsyncSession]:
        async with radar_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_async_session] = session_override
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as test_client:
        test_client.app = app  # type: ignore[attr-defined]
        yield test_client


def snapshot() -> RadarSnapshotEnvelope:
    return RadarSnapshotEnvelope(
        snapshot_type="eod",
        as_of=datetime(2026, 7, 18, 15, 20),
        computed_at=datetime(2026, 7, 18, 15, 21),
        status="partial",
        confidence=0.72,
        formula_version="market-radar-v1",
        metrics={
            "overview": {"risk_level": "high", "trade_date": "2026-07-18"},
            "breadth": {
                "status": "partial",
                "days": [
                    {
                        "trade_date": "2026-07-17",
                        "breadth": {"buckets": {"down": {"count": 100, "percentage": 20.0}}},
                    },
                    {
                        "trade_date": "2026-07-18",
                        "breadth": {"buckets": {"down": {"count": 200, "percentage": 40.0}}},
                    },
                ],
            },
            "indices": {"000001.SH": {"return_pct": -2.1}},
            "limit_ladder": {
                "status": "stale",
                "source_date": "2026-07-17",
                "reason": "official list has not updated",
                "rows": [{"symbol": "600000.SH", "consecutive_limit": 2}],
            },
            "crowding": {"status": "fresh", "score": {"value": 82.0}},
            "sectors": {
                "status": "fresh",
                "source_date": "2026-07-18",
                "sectors": [{"industry": "银行", "median_return": -1.2}],
            },
        },
        source_freshness={
            "daily": {"status": "fresh", "source_date": "2026-07-18"},
            "limit_detail": {
                "status": "stale",
                "source_date": "2026-07-17",
                "reason": "official list has not updated",
            },
        },
    )


def assert_envelope(payload: dict[str, Any]) -> None:
    assert set(payload) == {
        "as_of",
        "computed_at",
        "status",
        "confidence",
        "realtime_mode",
        "sources",
        "data",
    }


async def persist_snapshot(session: AsyncSession, value: RadarSnapshotEnvelope) -> None:
    await MarketRadarStore(session).upsert_snapshot(
        snapshot_type=value.snapshot_type,
        as_of=value.as_of,
        computed_at=value.computed_at,
        status=value.status,
        confidence=value.confidence,
        formula_version=value.formula_version,
        metrics=value.metrics,
        source_freshness=value.source_freshness,
    )
    await session.commit()


async def persist_event(
    session: AsyncSession,
    *,
    severity: str = "high",
    status: str = "active",
    subject: str = "ALL",
    scope: str = "market",
) -> MarketAlertEvent:
    store = MarketRadarStore(session)
    rule = await store.upsert_rule(
        rule_key=f"seed-{severity}-{scope}-{subject}-{status}",
        version=1,
        scope=scope,
        subject="*",
        rule_type="metric_threshold",
        parameters={"metric": "return_pct", "operator": "lte", "threshold": -2},
        severity=severity,
        cooldown_seconds=900,
        enabled=True,
        source="user",
    )
    event, _ = await store.record_event_hit(
        rule_id=rule.id,
        snapshot_id=None,
        scope=scope,
        subject=subject,
        direction="down",
        severity=severity,
        title="风险预警",
        explanation="跌幅达到阈值",
        dedupe_key=f"event-{severity}-{scope}-{subject}-{status}",
        evidence={"value": -3.0},
        seen_at=NOW,
    )
    if status == "acknowledged":
        event = await store.acknowledge_event(event.id, at=NOW)
    elif status == "dismissed":
        event = await store.dismiss_event(event.id, at=NOW)
    elif status == "resolved":
        event = await store.resolve_event(event.id, at=NOW)
    await session.commit()
    return event


@pytest.mark.asyncio
async def test_empty_analytics_return_200_unavailable_envelopes(client):
    for path in (
        "/api/market-radar/overview",
        "/api/market-radar/breadth?days=15&mode=percent",
        "/api/market-radar/limit-ladder",
        "/api/market-radar/crowding?scope=market",
        "/api/market-radar/sectors",
    ):
        response = await client.get(path)
        assert response.status_code == 200, path
        payload = response.json()
        assert_envelope(payload)
        assert payload["as_of"] is None
        assert payload["status"] == "unavailable"
        assert payload["confidence"] == 0
        assert payload["data"]["reason"] == "no market radar snapshot is available"

    assert (await client.get("/api/market-radar/breadth?days=0")).status_code == 422
    assert (await client.get("/api/market-radar/breadth?days=121")).status_code == 422
    assert (await client.get("/api/market-radar/breadth?mode=raw")).status_code == 422


@pytest.mark.asyncio
async def test_analytics_project_latest_snapshot_and_preserve_freshness(client, radar_session):
    value = snapshot()
    await persist_snapshot(radar_session, value)
    active = await persist_event(radar_session)
    await persist_event(radar_session, severity="medium", status="acknowledged")

    overview = (await client.get("/api/market-radar/overview")).json()
    assert_envelope(overview)
    assert overview["data"]["risk_level"] == "high"
    assert overview["data"]["alert_counts"] == {
        "active": 1,
        "acknowledged": 1,
        "dismissed": 0,
        "resolved": 0,
        "active_high": 1,
    }

    breadth = (await client.get("/api/market-radar/breadth?days=1&mode=count")).json()
    assert [item["trade_date"] for item in breadth["data"]["days"]] == ["2026-07-18"]
    assert breadth["data"]["days"][0]["breadth"]["buckets"]["down"]["value"] == 200
    assert breadth["data"]["indices"] == value.metrics["indices"]

    ladder = (await client.get("/api/market-radar/limit-ladder")).json()
    assert ladder["status"] == "partial"
    assert ladder["data"]["status"] == "stale"
    assert ladder["data"]["source_date"] == "2026-07-17"
    assert ladder["data"]["reason"] == "official list has not updated"
    assert (
        next(item for item in ladder["sources"] if item["name"] == "limit_detail")["reason"]
        == "official list has not updated"
    )

    assert (await client.get("/api/market-radar/crowding?scope=market")).json()["data"]["score"][
        "value"
    ] == 82
    sectors = (await client.get("/api/market-radar/sectors?trade_date=2026-07-18")).json()
    assert sectors["data"]["sectors"][0]["industry"] == "银行"
    assert sectors["data"]["source_date"] == "2026-07-18"

    assert active.id is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scope", "subject"),
    [("sector", "不存在行业"), ("symbol", "600999.SH")],
)
async def test_missing_crowding_subject_returns_structured_unavailable(
    client,
    radar_session,
    scope,
    subject,
):
    await persist_snapshot(radar_session, snapshot())

    response = await client.get(
        "/api/market-radar/crowding",
        params={"scope": scope, "subject": subject},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "unavailable"
    assert payload["confidence"] == 0
    assert payload["data"] == {
        "scope": scope,
        "subject": subject,
        "items": [],
        "reason": "crowding subject is not available",
    }
    subject_source = next(
        source for source in payload["sources"] if source["name"] == "crowding_subject"
    )
    assert subject_source["status"] == "unavailable"
    assert subject_source["reason"] == "crowding subject is not available"


@pytest.mark.asyncio
async def test_alert_list_filters_pages_gets_and_transitions(client, radar_session):
    matching = await persist_event(radar_session, subject="600000.SH", scope="symbol")
    await persist_event(radar_session, severity="medium", subject="000001.SZ", scope="symbol")

    response = await client.get(
        "/api/market-radar/alerts",
        params={
            "status": "active",
            "severity": "high",
            "scope": "symbol",
            "subject": "600000.SH",
            "page": 1,
            "page_size": 1,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert_envelope(payload)
    assert payload["data"]["total"] == 1
    assert payload["data"]["page"] == 1
    assert payload["data"]["page_size"] == 1
    assert payload["data"]["items"][0]["id"] == matching.id
    assert payload["data"]["items"][0]["evidence"] == {"value": -3.0}

    single = await client.get(f"/api/market-radar/alerts/{matching.id}")
    assert single.status_code == 200
    assert single.json()["data"]["id"] == matching.id

    acknowledged = await client.post(f"/api/market-radar/alerts/{matching.id}/acknowledge")
    assert acknowledged.status_code == 200
    assert acknowledged.json()["data"]["status"] == "acknowledged"
    dismissed = await client.post(f"/api/market-radar/alerts/{matching.id}/dismiss")
    assert dismissed.status_code == 200
    assert dismissed.json()["data"]["status"] == "dismissed"
    assert (await client.get("/api/market-radar/alerts/999999")).status_code == 404


RULE_BODY = {
    "rule_key": "user-600000-return",
    "scope": "symbol",
    "subject": "600000.SH",
    "title": "浦发银行跌幅预警",
    "direction": "down",
    "rule_type": "metric_threshold",
    "parameters": {"metric": "return_pct", "operator": "lte", "threshold": -3.0},
    "severity": "high",
    "cooldown_seconds": 900,
    "enabled": True,
}


@pytest.mark.asyncio
async def test_rule_crud_is_typed_versioned_and_soft_deleted(client, radar_factory):
    invalid_extra = {**RULE_BODY, "unexpected": True}
    assert (await client.post("/api/market-radar/rules", json=invalid_extra)).status_code == 422
    invalid_parameters = {
        **RULE_BODY,
        "parameters": {"metric": "return_pct", "operator": "lte", "threshold": -3, "x": 1},
    }
    assert (
        await client.post("/api/market-radar/rules", json=invalid_parameters)
    ).status_code == 422
    unsupported = {**RULE_BODY, "rule_type": "python", "parameters": {"code": "pass"}}
    assert (await client.post("/api/market-radar/rules", json=unsupported)).status_code == 422

    created_response = await client.post("/api/market-radar/rules", json=RULE_BODY)
    assert created_response.status_code == 201
    created = created_response.json()["data"]
    assert created["version"] == 1
    assert created["source"] == "user"
    assert created["parameters"]["metric"] == "return_pct"

    async with radar_factory() as session:
        old_event = await persist_event_for_rule(session, created["id"])

    patched = await client.patch(
        f"/api/market-radar/rules/{created['id']}",
        json={"parameters": {"metric": "return_pct", "operator": "lte", "threshold": -5}},
    )
    assert patched.status_code == 200
    replacement = patched.json()["data"]
    assert replacement["id"] != created["id"]
    assert replacement["version"] == 2
    assert replacement["parameters"]["threshold"] == -5

    async with radar_factory() as session:
        old = await session.get(MarketAlertRule, created["id"])
        event = await session.get(MarketAlertEvent, old_event.id)
        assert old is not None and old.enabled is False
        assert event is not None and event.status == "resolved"
        assert event.resolved_at is not None

    listed = (await client.get("/api/market-radar/rules")).json()["data"]
    assert [item["id"] for item in listed["items"]] == [replacement["id"]]

    deleted = await client.delete(f"/api/market-radar/rules/{replacement['id']}")
    assert deleted.status_code == 200
    assert deleted.json()["data"]["enabled"] is False
    async with radar_factory() as session:
        assert await session.get(MarketAlertRule, replacement["id"]) is not None


async def persist_event_for_rule(session: AsyncSession, rule_id: int) -> MarketAlertEvent:
    event, _ = await MarketRadarStore(session).record_event_hit(
        rule_id=rule_id,
        snapshot_id=None,
        scope="symbol",
        subject="600000.SH",
        direction="down",
        severity="high",
        title="浦发银行跌幅预警",
        explanation="跌幅达到阈值",
        dedupe_key=f"rule-event-{rule_id}",
        evidence={"value": -4},
        seen_at=NOW,
    )
    await session.commit()
    return event


@pytest.mark.asyncio
async def test_refresh_returns_202_and_updates_runtime_task(client):
    service = FakeRadarService(snapshot())
    client.app.state.market_radar_service = service  # type: ignore[attr-defined]
    response = await client.post("/api/market-radar/refresh", json={"kind": "intraday"})
    assert response.status_code == 202
    payload = response.json()
    assert_envelope(payload)
    task_payload = payload["data"]
    assert task_payload["kind"] == "market_radar_refresh"
    assert task_payload["status"] in {"queued", "running"}

    for _ in range(20):
        task = get_task(task_payload["task_id"])
        if task is not None and task["status"] == "succeeded":
            break
        await asyncio.sleep(0)
    assert task is not None and task["status"] == "succeeded"
    assert service.intraday_refreshes == 1

    invalid = await client.post(
        "/api/market-radar/refresh", json={"kind": "eod", "trade_date": None}
    )
    assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_refresh_deduplicates_concurrent_and_repeated_requests(client):
    class BlockingRefreshService(FakeRadarService):
        def __init__(self):
            super().__init__(snapshot())
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def refresh_intraday(self) -> RadarSnapshotEnvelope:
            self.intraday_refreshes += 1
            self.started.set()
            await self.release.wait()
            assert self.snapshot is not None
            return self.snapshot

    service = BlockingRefreshService()
    client.app.state.market_radar_service = service  # type: ignore[attr-defined]
    first, second = await asyncio.gather(
        client.post("/api/market-radar/refresh", json={"kind": "intraday"}),
        client.post("/api/market-radar/refresh", json={"kind": "intraday"}),
    )
    await service.started.wait()
    third = await client.post("/api/market-radar/refresh", json={"kind": "intraday"})

    try:
        task_ids = {
            first.json()["data"]["task_id"],
            second.json()["data"]["task_id"],
            third.json()["data"]["task_id"],
        }
        assert len(task_ids) == 1
        assert service.intraday_refreshes == 1
    finally:
        service.release.set()
        tasks = tuple(client.app.state.market_radar_refresh_tasks)  # type: ignore[attr-defined]
        if tasks:
            await asyncio.gather(*tasks)

    assert client.app.state.market_radar_refresh_by_key == {}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_refresh_failure_persists_stable_error_without_sensitive_details(
    client,
    monkeypatch,
):
    import app.api.market_radar as market_radar_api

    secret = "QMT_ACCOUNT=998877 path=C:/private/miniQMT"
    log_messages: list[str] = []

    class FailingRefreshService(FakeRadarService):
        async def refresh_intraday(self) -> RadarSnapshotEnvelope:
            raise RuntimeError(secret)

    monkeypatch.setattr(
        market_radar_api,
        "logger",
        SimpleNamespace(
            warning=lambda message, *args: log_messages.append(message.format(*args)),
            exception=lambda message, *args: log_messages.append(message.format(*args)),
        ),
    )
    client.app.state.market_radar_service = FailingRefreshService(snapshot())  # type: ignore[attr-defined]

    response = await client.post("/api/market-radar/refresh", json={"kind": "intraday"})
    task_id = response.json()["data"]["task_id"]
    for _ in range(20):
        task = get_task(task_id)
        if task is not None and task["status"] == "failed":
            break
        await asyncio.sleep(0)

    assert task is not None
    assert task["status"] == "failed"
    assert task["error"] == "market radar refresh failed"
    assert task["meta"]["error_code"] == "MARKET_RADAR_REFRESH_FAILED"
    persisted = json.dumps(task, ensure_ascii=False)
    assert "998877" not in persisted
    assert "private" not in persisted
    assert log_messages and "RuntimeError" in log_messages[0]
    assert "998877" not in log_messages[0]
    assert "private" not in log_messages[0]


@pytest.mark.asyncio
async def test_sse_has_fixed_frames_headers_atomic_initial_events_and_closes(radar_factory):
    events = [
        StreamEvent(1, "1", "mode", {"mode": "offline"}, NOW),
        StreamEvent(
            2,
            "2",
            "snapshot",
            MarketRadarService.project_snapshot(snapshot()),
            NOW,
        ),
        StreamEvent(3, "3", "alert", {"id": 42, "severity": "high"}, NOW),
    ]
    subscription = FakeSubscription(events)
    app = FastAPI()
    app.include_router(router, prefix="/api/market-radar")
    service = FakeRadarService(subscription=subscription)
    app.state.market_radar_service = service
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/market-radar/stream",
        "raw_path": b"/api/market-radar/stream",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 1),
        "server": ("test", 80),
        "app": app,
    }
    request = Request(scope)
    route = next(route for route in router.routes if getattr(route, "path", "") == "/stream")
    async with radar_factory() as session:
        response = await route.endpoint(request, session=session)
        assert session.in_transaction() is False
    assert service.initial_snapshot is None
    assert service.initial_alerts == ()
    assert response.headers["cache-control"] == "no-cache, no-transform"
    assert response.headers["x-accel-buffering"] == "no"
    assert response.headers["content-type"].startswith("text/event-stream")

    iterator = response.body_iterator
    frames = [await iterator.__anext__() for _ in range(3)]
    parsed = [parse_sse_frame(frame) for frame in frames]
    assert [item["event"] for item in parsed] == ["mode", "snapshot", "alert"]
    assert [item["id"] for item in parsed] == ["1", "2", "3"]
    assert all(item["retry"] == "5000" for item in parsed)
    assert parsed[1]["data"]["schema_version"] == 1
    assert parsed[1]["data"]["sequence"] == 2
    assert parsed[1]["data"]["event_id"] == "2"
    assert parsed[1]["data"]["occurred_at"] == NOW.isoformat()
    assert parsed[1]["data"]["as_of"] == "2026-07-18T15:20:00"
    assert parsed[2]["data"]["id"] == 42

    await iterator.aclose()
    assert subscription.closed is True


def parse_sse_frame(raw: str | bytes) -> dict[str, Any]:
    text = raw.decode() if isinstance(raw, bytes) else raw
    fields: dict[str, Any] = {}
    for line in text.strip().splitlines():
        key, value = line.split(":", 1)
        fields[key] = value.strip()
    fields["data"] = json.loads(fields["data"])
    return fields


@pytest.mark.asyncio
async def test_alert_query_rejects_invalid_enums_and_pagination(client):
    assert (await client.get("/api/market-radar/alerts?status=unknown")).status_code == 422
    assert (await client.get("/api/market-radar/alerts?severity=urgent")).status_code == 422
    assert (await client.get("/api/market-radar/alerts?page=0")).status_code == 422
    assert (await client.get("/api/market-radar/alerts?page_size=101")).status_code == 422


@pytest.mark.asyncio
async def test_patched_latest_rule_is_loaded_by_alert_engine(client, radar_factory):
    created = (await client.post("/api/market-radar/rules", json=RULE_BODY)).json()["data"]
    replacement = (
        await client.patch(
            f"/api/market-radar/rules/{created['id']}",
            json={"severity": "medium"},
        )
    ).json()["data"]
    async with radar_factory() as session:
        from app.services.market_radar import MarketAlertEngine

        loaded = await MarketAlertEngine().load_rules(MarketRadarStore(session))
    by_key = {item.key: item for item in loaded}
    assert by_key[RULE_BODY["rule_key"]].version == replacement["version"]
    assert by_key[RULE_BODY["rule_key"]].severity == "medium"

    observation = RadarObservation(
        scope="symbol",
        subject="600000.SH",
        metrics={"return_pct": MetricValue(-4, "fresh", NOW, "qmt_realtime")},
    )
    evaluation = MarketAlertEngine().evaluate(
        RadarSnapshotEnvelope(
            snapshot_type="intraday",
            as_of=NOW,
            computed_at=NOW,
            status="fresh",
            confidence=1,
            formula_version="market-radar-v1",
            metrics={},
            source_freshness={},
            observations=(observation,),
        ),
        rules=loaded,
    )
    async with radar_factory() as session:
        await MarketAlertEngine().persist(MarketRadarStore(session), evaluation, seen_at=NOW)
        await session.commit()
        event = await session.scalar(select(MarketAlertEvent))
    assert event is not None
    assert event.rule_id == replacement["id"]


def test_market_radar_router_is_registered_once():
    from app.api.router import api_router

    included = [
        route
        for route in api_router.routes
        if getattr(getattr(route, "include_context", None), "prefix", None) == "/market-radar"
    ]
    assert len(included) == 1
    paths = [route.path for route in included[0].original_router.routes]
    assert paths.count("/overview") == 1
    assert paths.count("/stream") == 1


@pytest.mark.asyncio
async def test_realtime_universe_loader_filters_non_equity_and_delisted_symbols():
    from app.db.sqlite import async_session_factory
    from app.main import _load_market_radar_universe

    async with async_session_factory() as session:
        session.add_all(
            [
                Stock(symbol="600000.SH", exchange="SH", is_delist=0),
                Stock(symbol="300001.SZ", exchange="SZ", is_delist=0),
                Stock(symbol="430001.BJ", exchange="BJ", is_delist=0),
                Stock(symbol="000001.SZ", exchange="SZ", is_delist=1),
                Stock(symbol="600001.SH", exchange="SH", is_delist=0, is_suspend=1),
                Stock(
                    symbol="600002.SH",
                    exchange="SH",
                    is_delist=0,
                    list_date=date(2099, 1, 1),
                ),
                Stock(
                    symbol="600003.SH",
                    exchange="SH",
                    is_delist=0,
                    delist_date=date(2020, 1, 1),
                ),
                Stock(symbol="510300.SH", exchange="SH", is_delist=0),
                Stock(symbol="BAD", exchange="SH", is_delist=0),
            ]
        )
        await session.commit()

    assert await _load_market_radar_universe() == (
        "300001.SZ",
        "430001.BJ",
        "600000.SH",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("degraded_mode", ["offline", "polling_30s"])
async def test_market_radar_runtime_starts_once_logs_degraded_mode_without_details(
    monkeypatch,
    degraded_mode,
):
    import app.main as main

    warnings: list[str] = []

    class OwnedSession:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    class Feed:
        status = SimpleNamespace(
            mode=degraded_mode,
            reason="account=SECRET path=C:/private/qmt",
            market_coverage={"SH": 0.5},
        )

    class Context:
        def __init__(self, session, market_store=None):
            assert session is owned_session
            assert market_store is parquet_store

        async def load_eligible_universe(self):
            return ()

        async def load_symbol_context(self, *args):
            return {}

    class RadarService:
        instances: list[RadarService] = []

        def __init__(self, **kwargs):
            self.feed = kwargs["feed"]
            self.start_calls = 0
            self.stop_calls = 0
            self.instances.append(self)

        async def start(self):
            self.start_calls += 1

        async def stop(self):
            self.stop_calls += 1

    owned_session = OwnedSession()
    parquet_store = object()
    monkeypatch.setattr(main, "async_session_factory", lambda: owned_session)
    monkeypatch.setattr(main, "get_market_data_store", lambda: parquet_store)
    monkeypatch.setattr(main, "QmtRealtimeFeed", lambda **kwargs: Feed())
    monkeypatch.setattr(main, "MarketRadarDataService", lambda session, store: object())
    monkeypatch.setattr(main, "MarketRadarStore", lambda session: object())
    monkeypatch.setattr(main, "FocusUniverseResolver", lambda session: object())
    monkeypatch.setattr(main, "MarketRadarIntradayContextLoader", Context)
    monkeypatch.setattr(main, "MarketRadarService", RadarService)
    monkeypatch.setattr(
        main,
        "logger",
        SimpleNamespace(warning=lambda message, *args: warnings.append(message.format(*args))),
    )

    app = FastAPI()
    await main._start_market_radar_runtime(app)
    await main._start_market_radar_runtime(app)
    assert len(RadarService.instances) == 1
    service = RadarService.instances[0]
    assert service.start_calls == 1
    assert app.state.market_radar_service is service
    assert app.state.market_radar_feed is service.feed
    assert warnings == [f"Market radar realtime feed degraded: mode={degraded_mode}"]
    assert "SECRET" not in warnings[0]
    assert "private" not in warnings[0]

    await main._stop_market_radar_runtime(app)
    assert service.stop_calls == 1
    assert owned_session.close_calls == 1


@pytest.mark.asyncio
async def test_market_radar_runtime_does_not_abort_when_qmt_start_is_unavailable(monkeypatch):
    import app.main as main

    class OwnedSession:
        async def close(self) -> None:
            return None

    class RadarService:
        def __init__(self, **kwargs):
            self.feed = kwargs["feed"]

        async def start(self):
            raise ModuleNotFoundError("No module named 'xtquant'")

        async def stop(self):
            return None

    monkeypatch.setattr(main, "async_session_factory", lambda: OwnedSession())
    monkeypatch.setattr(main, "get_market_data_store", lambda: object())
    monkeypatch.setattr(main, "QmtRealtimeFeed", lambda **kwargs: object())
    monkeypatch.setattr(main, "MarketRadarDataService", lambda session, store: object())
    monkeypatch.setattr(main, "MarketRadarStore", lambda session: object())
    monkeypatch.setattr(main, "FocusUniverseResolver", lambda session: object())
    monkeypatch.setattr(
        main,
        "MarketRadarIntradayContextLoader",
        lambda *args, **kwargs: SimpleNamespace(
            load_eligible_universe=lambda: (),
            load_symbol_context=lambda *args: {},
        ),
    )
    monkeypatch.setattr(main, "MarketRadarService", RadarService)

    app = FastAPI()
    await main._start_market_radar_runtime(app)
    assert app.state.market_radar_service is not None
    assert app.state.market_radar_start_error == "realtime market data unavailable"
    await main._stop_market_radar_runtime(app)


@pytest.mark.asyncio
async def test_application_lifespan_always_stops_radar_and_executor(monkeypatch):
    import app.api.ai as ai_api
    import app.db.sqlite as sqlite
    import app.main as main
    import app.services.ai_native as ai_native

    calls: list[str] = []

    class SessionContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *args):
            return None

    class AIService:
        def __init__(self, session):
            pass

        async def cleanup_expired(self):
            return 0

        async def reconcile_approval_states(self):
            return 0

    async def init_db():
        calls.append("init_db")

    async def start_radar(app):
        calls.append("start_radar")

    async def stop_radar(app):
        calls.append("stop_radar")

    monkeypatch.setattr(main, "init_db", init_db)
    monkeypatch.setattr(main, "install_default_executor", lambda: calls.append("executor_start"))
    monkeypatch.setattr(main, "shutdown_default_executor", lambda: calls.append("executor_stop"))
    monkeypatch.setattr(main, "mark_stale_runtime_tasks_failed", lambda **kwargs: 0)
    monkeypatch.setattr(main, "get_redis_client", lambda: SimpleNamespace(available=False))
    monkeypatch.setattr(main, "_start_market_radar_runtime", start_radar)
    monkeypatch.setattr(main, "_stop_market_radar_runtime", stop_radar)
    monkeypatch.setattr(sqlite, "async_session_factory", lambda: SessionContext())
    monkeypatch.setattr(ai_native, "AINativeService", AIService)
    monkeypatch.setattr(ai_api, "resume_ai_workflows", lambda: 0)

    with pytest.raises(RuntimeError, match="request loop failed"):
        async with main.lifespan(FastAPI()):
            raise RuntimeError("request loop failed")

    assert calls == [
        "executor_start",
        "init_db",
        "start_radar",
        "stop_radar",
        "executor_stop",
    ]


@pytest.mark.asyncio
async def test_application_lifespan_cleans_up_when_radar_start_fails(monkeypatch):
    import app.api.ai as ai_api
    import app.db.sqlite as sqlite
    import app.main as main
    import app.services.ai_native as ai_native

    calls: list[str] = []

    class SessionContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *args):
            return None

    class AIService:
        def __init__(self, session):
            pass

        async def cleanup_expired(self):
            return 0

        async def reconcile_approval_states(self):
            return 0

    async def init_db():
        return None

    async def start_radar(app):
        calls.append("start_radar")
        raise RuntimeError("radar database failed")

    async def stop_radar(app):
        calls.append("stop_radar")

    monkeypatch.setattr(main, "init_db", init_db)
    monkeypatch.setattr(main, "install_default_executor", lambda: calls.append("executor_start"))
    monkeypatch.setattr(main, "shutdown_default_executor", lambda: calls.append("executor_stop"))
    monkeypatch.setattr(main, "mark_stale_runtime_tasks_failed", lambda **kwargs: 0)
    monkeypatch.setattr(main, "get_redis_client", lambda: SimpleNamespace(available=False))
    monkeypatch.setattr(main, "_start_market_radar_runtime", start_radar)
    monkeypatch.setattr(main, "_stop_market_radar_runtime", stop_radar)
    monkeypatch.setattr(sqlite, "async_session_factory", lambda: SessionContext())
    monkeypatch.setattr(ai_native, "AINativeService", AIService)
    monkeypatch.setattr(ai_api, "resume_ai_workflows", lambda: 0)

    with pytest.raises(RuntimeError, match="radar database failed"):
        async with main.lifespan(FastAPI()):
            pass

    assert calls == ["executor_start", "start_radar", "stop_radar", "executor_stop"]


@pytest.mark.asyncio
async def test_actual_service_sse_always_starts_with_mode_and_snapshot(radar_factory):
    class Feed:
        status = SimpleNamespace(
            mode="offline",
            changed_at=NOW,
            last_quote_at=None,
            connection_generation=0,
            reason="miniQMT unavailable",
            market_coverage={},
        )

    async with radar_factory() as session:
        service = MarketRadarService(
            feed=Feed(), data_service=SimpleNamespace(), store=MarketRadarStore(session)
        )
        subscription = await service.subscribe_with_initial(snapshot=None, alerts=())
        first = await subscription.get()
        second = await subscription.get()
        await subscription.close()

    assert first.event == "mode"
    assert second.event == "snapshot"
    assert second.data["status"] == "unavailable"
    assert second.data["as_of"] is None
    assert second.data["data"]["reason"] == "no market radar snapshot is available"


@pytest.mark.asyncio
async def test_sse_initial_high_alerts_are_complete_and_ordered(radar_factory):
    class Feed:
        status = SimpleNamespace(
            mode="offline",
            changed_at=NOW,
            last_quote_at=None,
            connection_generation=0,
            reason="miniQMT unavailable",
            market_coverage={},
        )

    class LifespanSessionMustStayIdle:
        async def execute(self, *args, **kwargs):
            raise AssertionError("SSE initialization queried the lifespan session")

    async with radar_factory() as session:
        rules = [
            MarketAlertRule(
                rule_key=f"stress-{index}",
                version=1,
                scope="market",
                subject="*",
                rule_type="metric_threshold",
                parameters_json=json.dumps(
                    {"metric": "return_pct", "operator": "lte", "threshold": -2}
                ),
                severity="high",
                cooldown_seconds=900,
                enabled=True,
                source="user",
            )
            for index in range(70)
        ]
        session.add_all(rules)
        await session.flush()
        session.add_all(
            [
                MarketAlertEvent(
                    rule_id=rule.id,
                    snapshot_id=None,
                    scope="market",
                    subject=f"STRESS-{index}",
                    direction="down",
                    severity="high",
                    title="风险预警",
                    explanation="压力测试",
                    dedupe_key=f"stress-event-{index}",
                    evidence_json=json.dumps({"index": index}),
                    triggered_at=NOW,
                    last_seen_at=NOW,
                    occurrence_count=1,
                    clear_streak=0,
                )
                for index, rule in enumerate(rules)
            ]
        )
        await session.commit()
        result = await session.execute(
            select(MarketAlertEvent).order_by(
                MarketAlertEvent.triggered_at,
                MarketAlertEvent.id,
            )
        )
        alerts = tuple(result.scalars())
        store = MarketRadarStore(LifespanSessionMustStayIdle())  # type: ignore[arg-type]
        service = MarketRadarService(feed=Feed(), data_service=SimpleNamespace(), store=store)
        subscription = await service.subscribe_with_initial(snapshot=None, alerts=alerts)
        assert subscription.pending == 72
        events = [await subscription.get() for _ in range(72)]
        assert [event.event for event in events] == [
            "mode",
            "snapshot",
            *(["alert"] * 70),
        ]
        assert [event.data["subject"] for event in events[2:]] == [
            f"STRESS-{index}" for index in range(70)
        ]
        assert [event.sequence for event in events] == sorted(event.sequence for event in events)
        await subscription.close()


@pytest.mark.asyncio
async def test_concurrent_duplicate_rule_create_has_single_winner(client):
    first, second = await asyncio.gather(
        client.post("/api/market-radar/rules", json=RULE_BODY),
        client.post("/api/market-radar/rules", json=RULE_BODY),
    )
    assert sorted((first.status_code, second.status_code)) == [201, 409]

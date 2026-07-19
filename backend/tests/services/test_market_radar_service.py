from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.db.models.base import Base
from app.db.models.market_radar import MarketAlertEvent
from app.services.market_radar import (
    DEFAULT_RULE_VERSION,
    BrokerDisconnected,
    FocusUniverse,
    FocusUniverseResolver,
    MarketAlertEngine,
    MarketRadarService,
    MarketRadarStreamBroker,
    MetricValue,
    RadarObservation,
    RadarSnapshotEnvelope,
    RuleDefinition,
)
from app.services.market_radar_calculator import QuoteTick
from app.services.market_radar_data import RawComponent, SourceFreshness
from app.services.market_radar_store import MarketRadarStore
from app.services.qmt_realtime_feed import RealtimeFeedStatus

NOW = datetime(2026, 7, 17, 10, 0, 0)


def metric(
    value: float | bool | None,
    *,
    status: str = "fresh",
    source: str = "synthetic",
    baseline: float | str | None = None,
) -> MetricValue:
    return MetricValue(
        value=value,
        status=status,  # type: ignore[arg-type]
        as_of=NOW,
        source=source,
        baseline=baseline,
    )


def observation(
    scope: str,
    subject: str,
    metrics: dict[str, MetricValue],
    *,
    sources: tuple[str, ...] = (),
) -> RadarObservation:
    return RadarObservation(
        scope=scope,  # type: ignore[arg-type]
        subject=subject,
        metrics=metrics,
        sources=sources,
    )


def envelope(
    *observations: RadarObservation,
    status: str = "fresh",
    as_of: datetime = NOW,
    metrics: dict[str, Any] | None = None,
) -> RadarSnapshotEnvelope:
    return RadarSnapshotEnvelope(
        snapshot_type="intraday",
        as_of=as_of,
        computed_at=as_of,
        status=status,  # type: ignore[arg-type]
        confidence=1.0 if status == "fresh" else 0.5,
        formula_version="market-radar-v1",
        metrics=metrics or {"summary": {"status": status}},
        source_freshness={"synthetic": {"status": status, "as_of": as_of.isoformat()}},
        observations=observations,
    )


@pytest.mark.asyncio
async def test_broker_coalesces_snapshots_and_keeps_sequence_monotonic():
    broker = MarketRadarStreamBroker(queue_size=3)
    subscription = await broker.subscribe()

    await broker.publish("snapshot", {"value": 1})
    await broker.publish("snapshot", {"value": 2})
    await broker.publish("mode", {"mode": "push"})
    await broker.publish("alert", {"id": 7})

    events = [await subscription.get() for _ in range(3)]
    assert [(event.event, event.data) for event in events] == [
        ("snapshot", {"value": 2}),
        ("mode", {"mode": "push"}),
        ("alert", {"id": 7}),
    ]
    assert [event.sequence for event in events] == sorted(event.sequence for event in events)
    assert len({event.event_id for event in events}) == 3

    await subscription.close()
    assert broker.subscriber_count == 0


@pytest.mark.asyncio
async def test_broker_drops_heartbeat_before_critical_and_disconnects_chronic_slow_client():
    broker = MarketRadarStreamBroker(queue_size=1, overflow_disconnect_threshold=2)
    subscription = await broker.subscribe()

    await broker.publish("heartbeat", {"at": "first"})
    await broker.publish("alert", {"id": 1})
    assert (await subscription.get()).event == "alert"

    await broker.publish("mode", {"mode": "push"})
    await broker.publish("alert", {"id": 2})
    await broker.publish("alert", {"id": 3})
    with pytest.raises(BrokerDisconnected):
        await subscription.get()
    assert subscription.disconnect_reason == "slow_subscriber"

    await subscription.close()
    assert broker.subscriber_count == 0


@pytest.mark.asyncio
async def test_broker_heartbeat_interval_and_cleanup():
    broker = MarketRadarStreamBroker(queue_size=4, heartbeat_seconds=15)
    subscription = await broker.subscribe()

    assert await broker.heartbeat(NOW) is True
    assert await broker.heartbeat(NOW + timedelta(seconds=14)) is False
    assert await broker.heartbeat(NOW + timedelta(seconds=15)) is True
    first = await subscription.get()
    second = await subscription.get()
    assert first.event == second.event == "heartbeat"
    assert second.sequence > first.sequence

    await broker.unsubscribe(subscription.id)
    assert broker.subscriber_count == 0


@pytest.mark.asyncio
async def test_broker_initial_events_are_atomic_with_global_sequence():
    broker = MarketRadarStreamBroker(queue_size=8)
    await broker.publish("mode", {"mode": "offline"})
    subscription = await broker.subscribe(
        initial_events=(
            ("mode", {"mode": "push"}),
            ("snapshot", {"value": 1}),
        )
    )
    await broker.publish("alert", {"id": 9})
    events = [await subscription.get() for _ in range(3)]
    assert [event.event for event in events] == ["mode", "snapshot", "alert"]
    assert [event.sequence for event in events] == sorted(event.sequence for event in events)


@dataclass(frozen=True)
class RuleCase:
    name: str
    observation: RadarObservation
    expected_key: str
    expected_severity: str
    outside: RadarObservation
    outside_severity: str | None = None


RULE_CASES = (
    RuleCase(
        "market median",
        observation("market", "ALL", {"median_return_pct": metric(-2.5)}),
        "market_median_return_down",
        "high",
        observation("market", "ALL", {"median_return_pct": metric(-2.499)}),
    ),
    RuleCase(
        "decline ratio",
        observation("market", "ALL", {"decline_ratio": metric(0.8)}),
        "market_decline_ratio_high",
        "high",
        observation("market", "ALL", {"decline_ratio": metric(0.799)}),
    ),
    RuleCase(
        "index daily",
        observation("market", "000001.SH", {"return_pct": metric(-2.0)}),
        "core_index_return_down",
        "high",
        observation("market", "000001.SH", {"return_pct": metric(-1.999)}),
    ),
    RuleCase(
        "index five minute",
        observation("market", "399001.SZ", {"return_5m_pct": metric(-1.0)}),
        "core_index_5m_down",
        "high",
        observation("market", "399001.SZ", {"return_5m_pct": metric(-0.999)}),
    ),
    RuleCase(
        "limit down expansion",
        observation(
            "market",
            "ALL",
            {
                "limit_down_count": metric(30),
                "limit_down_median_5d": metric(15, baseline=15),
            },
        ),
        "market_limit_down_expansion",
        "high",
        observation(
            "market",
            "ALL",
            {
                "limit_down_count": metric(29),
                "limit_down_median_5d": metric(15),
            },
        ),
    ),
    RuleCase(
        "crowding weakness",
        observation(
            "market",
            "ALL",
            {"crowding_score": metric(80), "median_return_pct": metric(-1)},
        ),
        "market_crowding_weakness",
        "high",
        observation(
            "market",
            "ALL",
            {"crowding_score": metric(79.99), "median_return_pct": metric(-1)},
        ),
    ),
    RuleCase(
        "emotion down cross",
        observation(
            "market",
            "ALL",
            {"emotion_score": metric(30), "previous_emotion_score": metric(30.01)},
        ),
        "market_emotion_cross_down",
        "medium",
        observation(
            "market",
            "ALL",
            {"emotion_score": metric(30.01), "previous_emotion_score": metric(30.02)},
        ),
    ),
    RuleCase(
        "emotion up cross",
        observation(
            "market",
            "ALL",
            {"emotion_score": metric(85), "previous_emotion_score": metric(84.99)},
        ),
        "market_emotion_cross_up",
        "medium",
        observation(
            "market",
            "ALL",
            {"emotion_score": metric(84.99), "previous_emotion_score": metric(84.98)},
        ),
    ),
    RuleCase(
        "sector breadth",
        observation(
            "sector",
            "电子",
            {"median_return_pct": metric(-2), "decline_ratio": metric(0.8)},
        ),
        "sector_breadth_down",
        "medium",
        observation(
            "sector",
            "电子",
            {"median_return_pct": metric(-1.999), "decline_ratio": metric(0.8)},
        ),
    ),
    RuleCase(
        "sector share crowding",
        observation(
            "sector",
            "电子",
            {"amount_share_z20": metric(2.5), "crowding_score": metric(80)},
        ),
        "sector_share_crowding",
        "medium",
        observation(
            "sector",
            "电子",
            {"amount_share_z20": metric(2.499), "crowding_score": metric(80)},
        ),
    ),
    RuleCase(
        "holding medium return",
        observation("symbol", "600000.SH", {"return_pct": metric(-3)}, sources=("qmt_holding",)),
        "holding_return_down",
        "medium",
        observation(
            "symbol", "600000.SH", {"return_pct": metric(-2.999)}, sources=("qmt_holding",)
        ),
    ),
    RuleCase(
        "holding high return",
        observation("symbol", "600000.SH", {"return_pct": metric(-5)}, sources=("qmt_holding",)),
        "holding_return_down",
        "high",
        observation(
            "symbol", "600000.SH", {"return_pct": metric(-4.999)}, sources=("qmt_holding",)
        ),
        "medium",
    ),
    RuleCase(
        "holding medium drawdown",
        observation("symbol", "600000.SH", {"drawdown_pct": metric(3)}, sources=("qmt_holding",)),
        "holding_intraday_drawdown",
        "medium",
        observation(
            "symbol", "600000.SH", {"drawdown_pct": metric(2.999)}, sources=("qmt_holding",)
        ),
    ),
    RuleCase(
        "holding high drawdown",
        observation("symbol", "600000.SH", {"drawdown_pct": metric(5)}, sources=("qmt_holding",)),
        "holding_intraday_drawdown",
        "high",
        observation(
            "symbol", "600000.SH", {"drawdown_pct": metric(4.999)}, sources=("qmt_holding",)
        ),
        "medium",
    ),
    RuleCase(
        "holding volume",
        observation(
            "symbol",
            "600000.SH",
            {"volume_ratio_20d": metric(2.5), "return_pct": metric(2)},
            sources=("qmt_holding",),
        ),
        "holding_volume_price_anomaly",
        "medium",
        observation(
            "symbol",
            "600000.SH",
            {"volume_ratio_20d": metric(2.499), "return_pct": metric(2)},
            sources=("qmt_holding",),
        ),
    ),
    RuleCase(
        "holding limit down distance",
        observation(
            "symbol",
            "600000.SH",
            {"down_limit_distance_pct": metric(0.5)},
            sources=("qmt_holding",),
        ),
        "holding_near_limit_down",
        "high",
        observation(
            "symbol",
            "600000.SH",
            {"down_limit_distance_pct": metric(0.501)},
            sources=("qmt_holding",),
        ),
    ),
    RuleCase(
        "holding broken limit up",
        observation(
            "symbol",
            "600000.SH",
            {"limit_up_broken": metric(True)},
            sources=("qmt_holding",),
        ),
        "holding_limit_up_broken",
        "high",
        observation(
            "symbol",
            "600000.SH",
            {"limit_up_broken": metric(False)},
            sources=("qmt_holding",),
        ),
    ),
    RuleCase(
        "watchlist return",
        observation("symbol", "000001.SZ", {"return_pct": metric(-7)}, sources=("watchlist",)),
        "watchlist_return_down",
        "high",
        observation("symbol", "000001.SZ", {"return_pct": metric(-6.999)}, sources=("watchlist",)),
    ),
    RuleCase(
        "watchlist drawdown",
        observation("symbol", "000001.SZ", {"drawdown_pct": metric(5)}, sources=("watchlist",)),
        "watchlist_intraday_drawdown",
        "high",
        observation("symbol", "000001.SZ", {"drawdown_pct": metric(4.999)}, sources=("watchlist",)),
    ),
    RuleCase(
        "watchlist limit distance",
        observation(
            "symbol",
            "000001.SZ",
            {"down_limit_distance_pct": metric(0.5)},
            sources=("watchlist",),
        ),
        "watchlist_near_limit_down",
        "high",
        observation(
            "symbol",
            "000001.SZ",
            {"down_limit_distance_pct": metric(0.501)},
            sources=("watchlist",),
        ),
    ),
    RuleCase(
        "symbol sentiment",
        observation(
            "symbol",
            "000001.SZ",
            {"negative_heat_z20": metric(2), "weighted_sentiment": metric(-0.35)},
            sources=("vault_active",),
        ),
        "symbol_negative_sentiment_heat",
        "medium",
        observation(
            "symbol",
            "000001.SZ",
            {"negative_heat_z20": metric(1.999), "weighted_sentiment": metric(-0.35)},
            sources=("vault_active",),
        ),
    ),
)


@pytest.mark.parametrize("case", RULE_CASES, ids=lambda case: case.name)
def test_default_rule_thresholds_are_inclusive_and_just_outside_clear(case: RuleCase):
    engine = MarketAlertEngine()
    hit = engine.evaluate(envelope(case.observation))
    clear = engine.evaluate(envelope(case.outside))

    match = next(item for item in hit.matches if item.rule.key == case.expected_key)
    assert match.severity == case.expected_severity
    assert match.dedupe_key in hit.evaluated_dedupe_keys
    assert match.evidence["value"] is not None
    assert match.evidence["threshold"] is not None
    assert "baseline" in match.evidence
    assert match.evidence["source_time"] == NOW.isoformat()
    assert match.evidence["source"] == "synthetic"
    assert match.evidence["rule_version"] == DEFAULT_RULE_VERSION
    assert match.evidence["formula_version"] == "market-radar-v1"
    assert match.explanation
    assert match.dedupe_key in clear.evaluated_dedupe_keys
    outside_match = next(
        (item for item in clear.matches if item.dedupe_key == match.dedupe_key),
        None,
    )
    if case.outside_severity is None:
        assert outside_match is None
    else:
        assert outside_match is not None
        assert outside_match.severity == case.outside_severity


def test_rule_engine_requires_fresh_fields_and_never_guesses_limit_price():
    engine = MarketAlertEngine()
    stale = observation("market", "ALL", {"median_return_pct": metric(-10, status="stale")})
    missing_limit = observation(
        "symbol", "600000.SH", {"return_pct": metric(-1)}, sources=("qmt_holding",)
    )
    result = engine.evaluate(envelope(stale, missing_limit))

    assert result.matches == ()
    assert not any("limit" in key for key in result.evaluated_dedupe_keys)
    assert result.skipped_reasons


def test_holding_and_watchlist_sensitivities_do_not_leak_between_sources():
    engine = MarketAlertEngine()
    watch = observation("symbol", "000001.SZ", {"return_pct": metric(-4)}, sources=("watchlist",))
    holding = observation(
        "symbol", "600000.SH", {"return_pct": metric(-4)}, sources=("qmt_holding",)
    )
    result = engine.evaluate(envelope(watch, holding))

    assert {(item.rule.key, item.subject) for item in result.matches} == {
        ("holding_return_down", "600000.SH")
    }


def test_typed_user_rule_executes_and_disabled_system_override_is_respected():
    engine = MarketAlertEngine()
    custom = RuleDefinition(
        key="user_600000_return",
        scope="symbol",
        severity="medium",
        title="用户价格阈值",
        direction="down",
        rule_type="metric_threshold",
        parameters={"metric": "return_pct", "operator": "lte", "threshold": -1.5},
        subject="600000.SH",
        source="user",
    )
    disabled_system = RuleDefinition(
        key="holding_return_down",
        scope="symbol",
        severity="medium",
        title="持仓股下跌",
        direction="down",
        rule_type="holding_return_down",
        parameters={"medium_lte": -3, "high_lte": -5},
        enabled=False,
    )
    item = observation("symbol", "600000.SH", {"return_pct": metric(-4)}, sources=("qmt_holding",))
    result = engine.evaluate(envelope(item), rules=(custom, disabled_system))
    assert [match.rule.key for match in result.matches] == ["user_600000_return"]


@pytest.mark.asyncio
async def test_persist_syncs_default_rules_and_never_reenables_disabled_rule(radar_session):
    from app.db.models.market_radar import MarketAlertRule

    store = MarketRadarStore(radar_session)
    engine = MarketAlertEngine()
    await store.upsert_rule(
        rule_key="market_median_return_down",
        version=DEFAULT_RULE_VERSION,
        scope="market",
        subject="*",
        rule_type="market_median_return_down",
        parameters={"lte": -2.5},
        severity="high",
        cooldown_seconds=900,
        enabled=False,
        source="system",
    )
    await radar_session.commit()

    loaded = await engine.load_rules(store)
    result = engine.evaluate(
        envelope(observation("market", "ALL", {"median_return_pct": metric(-3)})),
        rules=loaded,
    )
    assert result.matches == ()
    await engine.persist(store, result, seen_at=NOW)
    await radar_session.commit()

    rows = list((await radar_session.execute(select(MarketAlertRule))).scalars())
    disabled = next(row for row in rows if row.rule_key == "market_median_return_down")
    assert disabled.enabled is False
    assert len(rows) >= len(engine.default_rules)
    assert json.loads(disabled.parameters_json) == {"lte": -2.5}


@pytest_asyncio.fixture
async def radar_session(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'radar-service.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


async def event_rows(session: AsyncSession) -> list[MarketAlertEvent]:
    result = await session.execute(select(MarketAlertEvent).order_by(MarketAlertEvent.id))
    return list(result.scalars())


@pytest.mark.asyncio
async def test_lifecycle_high_immediate_cooldown_and_escalation(radar_session):
    store = MarketRadarStore(radar_session)
    engine = MarketAlertEngine()
    medium_obs = observation(
        "symbol", "600000.SH", {"return_pct": metric(-3)}, sources=("qmt_holding",)
    )
    high_obs = observation(
        "symbol", "600000.SH", {"return_pct": metric(-5)}, sources=("qmt_holding",)
    )

    first = await engine.persist(store, engine.evaluate(envelope(high_obs)), seen_at=NOW)
    await radar_session.commit()
    second = await engine.persist(
        store, engine.evaluate(envelope(high_obs)), seen_at=NOW + timedelta(minutes=1)
    )
    await radar_session.commit()
    later = await engine.persist(
        store, engine.evaluate(envelope(high_obs)), seen_at=NOW + timedelta(minutes=15)
    )
    await radar_session.commit()
    assert len(first.notifications) == 1
    assert second.notifications == ()
    assert len(later.notifications) == 1

    # A medium event can worsen to high and bypass its normal medium cadence.
    await engine.persist(
        store,
        engine.evaluate(envelope(medium_obs)),
        seen_at=NOW + timedelta(hours=1),
    )
    await radar_session.commit()
    escalated = await engine.persist(
        store,
        engine.evaluate(envelope(high_obs)),
        seen_at=NOW + timedelta(hours=1, minutes=1),
    )
    await radar_session.commit()
    assert len(escalated.notifications) == 1


@pytest.mark.asyncio
async def test_lifecycle_medium_second_hit_two_clear_resolution_and_new_cycle(radar_session):
    store = MarketRadarStore(radar_session)
    engine = MarketAlertEngine()
    hit_obs = observation(
        "market", "ALL", {"emotion_score": metric(30), "previous_emotion_score": metric(31)}
    )
    clear_obs = observation(
        "market", "ALL", {"emotion_score": metric(31), "previous_emotion_score": metric(32)}
    )

    first = await engine.persist(store, engine.evaluate(envelope(hit_obs)), seen_at=NOW)
    await radar_session.commit()
    second = await engine.persist(
        store, engine.evaluate(envelope(hit_obs)), seen_at=NOW + timedelta(seconds=1)
    )
    await radar_session.commit()
    assert first.notifications == ()
    assert len(second.notifications) == 1

    one_clear = await engine.persist(
        store, engine.evaluate(envelope(clear_obs)), seen_at=NOW + timedelta(seconds=2)
    )
    await radar_session.commit()
    assert one_clear.resolved_event_ids == ()
    rows = await event_rows(radar_session)
    assert rows[-1].clear_streak == 1

    two_clear = await engine.persist(
        store, engine.evaluate(envelope(clear_obs)), seen_at=NOW + timedelta(seconds=3)
    )
    await radar_session.commit()
    assert two_clear.resolved_event_ids == (rows[-1].id,)
    assert (await event_rows(radar_session))[-1].status == "resolved"

    await engine.persist(
        store, engine.evaluate(envelope(hit_obs)), seen_at=NOW + timedelta(seconds=4)
    )
    await radar_session.commit()
    rows = await event_rows(radar_session)
    assert len(rows) == 2
    assert rows[-1].status == "active"


@pytest.mark.asyncio
async def test_lifecycle_unavailable_does_not_clear_and_ack_dismiss_are_preserved(radar_session):
    store = MarketRadarStore(radar_session)
    engine = MarketAlertEngine()
    hit_obs = observation("market", "ALL", {"median_return_pct": metric(-3)})
    stale_obs = observation("market", "ALL", {"median_return_pct": metric(0, status="unavailable")})

    await engine.persist(store, engine.evaluate(envelope(hit_obs)), seen_at=NOW)
    await radar_session.commit()
    row = (await event_rows(radar_session))[0]
    await store.acknowledge_event(row.id, at=NOW + timedelta(seconds=1))
    await radar_session.commit()
    await engine.persist(
        store, engine.evaluate(envelope(hit_obs)), seen_at=NOW + timedelta(seconds=2)
    )
    await radar_session.commit()
    assert (await event_rows(radar_session))[0].status == "acknowledged"

    await store.dismiss_event(row.id, at=NOW + timedelta(seconds=3))
    await radar_session.commit()
    await engine.persist(
        store, engine.evaluate(envelope(stale_obs)), seen_at=NOW + timedelta(seconds=4)
    )
    await radar_session.commit()
    row = (await event_rows(radar_session))[0]
    assert row.status == "dismissed"
    assert row.clear_streak == 0


@pytest.mark.asyncio
async def test_focus_universe_unions_holdings_watchlist_and_vault_without_sensitive_fields(
    radar_session,
):
    from app.db.models.watchlist import WatchlistGroup, WatchlistStock

    group = WatchlistGroup(name="test")
    radar_session.add(group)
    await radar_session.flush()
    radar_session.add_all(
        [
            WatchlistStock(group_id=group.id, symbol="000001.SZ"),
            WatchlistStock(group_id=group.id, symbol="600000.SH"),
        ]
    )
    await radar_session.commit()

    focus = SimpleNamespace(
        targets=(
            SimpleNamespace(symbol="300001.SZ", sources=("vault_active",)),
            SimpleNamespace(symbol="600000.SH", sources=("qmt_holding", "vault_active")),
        ),
        warning_code=None,
    )

    async def holdings():
        return ("600000.SH", "688001.SH")

    async def focus_loader():
        return focus

    resolver = FocusUniverseResolver(
        radar_session,
        holding_symbols_loader=holdings,
        focus_loader=focus_loader,
    )
    resolved = await resolver.resolve()
    assert resolved.holdings == ("600000.SH", "688001.SH")
    assert resolved.watchlist == ("000001.SZ", "600000.SH")
    assert resolved.focus == ("300001.SZ", "600000.SH")
    assert resolved.symbols == ("000001.SZ", "300001.SZ", "600000.SH", "688001.SH")
    assert resolved.sources["600000.SH"] == (
        "qmt_holding",
        "vault_active",
        "watchlist",
    )
    encoded = json.dumps(resolved.as_dict())
    assert all(secret not in encoded for secret in ("quantity", "cost", "account"))


@pytest.mark.asyncio
async def test_focus_universe_degrades_when_holdings_and_focus_fail_but_keeps_watchlist(
    radar_session,
):
    from app.db.models.watchlist import WatchlistGroup, WatchlistStock

    group = WatchlistGroup(name="fallback")
    radar_session.add(group)
    await radar_session.flush()
    radar_session.add(WatchlistStock(group_id=group.id, symbol="000001.SZ"))
    await radar_session.commit()

    async def fail():
        raise RuntimeError("private upstream detail")

    resolver = FocusUniverseResolver(
        radar_session,
        holding_symbols_loader=fail,
        focus_loader=fail,
    )
    resolved = await resolver.resolve()
    assert resolved.symbols == ("000001.SZ",)
    assert resolved.warnings == (
        "holdings_unavailable",
        "sentiment_focus_unavailable",
    )


class FakeClock:
    def __init__(self, now: datetime = NOW):
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeFeed:
    def __init__(self):
        self.health_calls = 0
        self._status = RealtimeFeedStatus(
            mode="push",
            changed_at=NOW,
            last_quote_at=NOW,
            connection_generation=1,
            reason=None,
            market_coverage={"SH": 1.0, "SZ": 1.0, "BJ": 1.0, "INDEX": 1.0},
        )
        self._ticks = {
            "600000.SH": QuoteTick("600000.SH", NOW, 9.5, 10.0, high_price=10.0),
        }

    async def run_health_cycle(self) -> None:
        self.health_calls += 1

    def latest_ticks(self):
        return dict(self._ticks)

    @property
    def status(self):
        return self._status


class CountingStore(MarketRadarStore):
    def __init__(self, session):
        super().__init__(session)
        self.snapshot_calls = 0

    async def upsert_snapshot(self, **kwargs):
        self.snapshot_calls += 1
        return await super().upsert_snapshot(**kwargs)


@pytest.mark.asyncio
async def test_service_coalesces_one_second_throttles_snapshot_and_persists_high_immediately(
    radar_session,
):
    clock = FakeClock()
    feed = FakeFeed()
    store = CountingStore(radar_session)
    broker = MarketRadarStreamBroker(queue_size=20)
    subscriber = await broker.subscribe()
    build_calls = 0

    async def builder(*, ticks, feed_status, focus, now):
        nonlocal build_calls
        build_calls += 1
        assert ticks
        assert feed_status.mode == "push"
        return envelope(
            observation("market", "ALL", {"median_return_pct": metric(-3)}),
            as_of=now,
        )

    service = MarketRadarService(
        feed=feed,
        data_service=SimpleNamespace(),
        store=store,
        alert_engine=MarketAlertEngine(),
        broker=broker,
        snapshot_builder=builder,
        focus_resolver=None,
        clock=clock,
    )

    first = await service.refresh_intraday()
    again = await service.refresh_intraday()
    assert first is again
    assert build_calls == feed.health_calls == 1
    assert store.snapshot_calls == 1
    rows = await event_rows(radar_session)
    assert len(rows) == 1
    assert rows[0].last_notified_at == NOW

    clock.advance(1)
    await service.refresh_intraday()
    assert build_calls == 2
    assert store.snapshot_calls == 1

    events = []
    while subscriber.pending:
        events.append(await subscriber.get())
    assert "mode" in {item.event for item in events}
    assert "snapshot" in {item.event for item in events}
    assert "alert" in {item.event for item in events}

    clock.advance(29)
    await service.refresh_intraday()
    assert store.snapshot_calls == 2
    assert not any(key in rows[0].evidence_json for key in ("ticks", "quantity", "cost"))


@pytest.mark.asyncio
async def test_service_rejects_raw_ticks_or_sensitive_position_data_before_persistence(
    radar_session,
):
    async def unsafe_builder(**_kwargs):
        return envelope(metrics={"raw_ticks": [], "position_quantity": 12})

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=SimpleNamespace(),
        store=MarketRadarStore(radar_session),
        snapshot_builder=unsafe_builder,
        clock=FakeClock(),
    )
    with pytest.raises(ValueError, match="unsafe snapshot field"):
        await service.refresh_intraday()
    assert (await event_rows(radar_session)) == []


@pytest.mark.asyncio
async def test_service_propagates_status_and_publishes_mode_changes(radar_session):
    clock = FakeClock()
    feed = FakeFeed()
    broker = MarketRadarStreamBroker(queue_size=10)
    subscriber = await broker.subscribe()

    async def builder(**kwargs):
        return envelope(status="partial", as_of=kwargs["now"])

    service = MarketRadarService(
        feed=feed,
        data_service=SimpleNamespace(),
        store=MarketRadarStore(radar_session),
        broker=broker,
        snapshot_builder=builder,
        clock=clock,
    )
    result = await service.run_once()
    assert result.status == "partial"
    initial = [await subscriber.get(), await subscriber.get()]
    assert [item.event for item in initial] == ["mode", "snapshot"]

    feed._status = RealtimeFeedStatus(
        mode="polling_30s",
        changed_at=clock.now,
        last_quote_at=clock.now,
        connection_generation=1,
        reason="push stale",
        market_coverage={"SH": 1.0, "SZ": 1.0, "BJ": 0.0, "INDEX": 1.0},
    )
    clock.advance(1)
    await service.run_once()
    assert (await subscriber.get()).event == "mode"


class FakeEodData:
    def __init__(self):
        self.calls: list[str] = []

    async def load_daily_market(self, **kwargs):
        self.calls.append("daily")
        return SimpleNamespace(status="fresh", expected_date=kwargs["target_date"])

    async def load_limit_ladder(self, **kwargs):
        self.calls.append("limit")
        return SimpleNamespace(status="stale", trade_date=kwargs["target_date"])

    async def load_crowding_inputs(self, **kwargs):
        self.calls.append("crowding")
        return SimpleNamespace(status="partial", as_of=kwargs["target_date"])

    async def load_sector_inputs(self, **kwargs):
        self.calls.append("sector")
        return SimpleNamespace(status="fresh", as_of=kwargs["target_date"])

    async def load_sentiment_inputs(self, **kwargs):
        self.calls.append("sentiment")
        return SimpleNamespace(status="unavailable", as_of=kwargs["as_of"])


@pytest.mark.asyncio
async def test_refresh_eod_loads_each_source_once_and_persists_serialized_snapshot(radar_session):
    data = FakeEodData()
    captured: dict[str, Any] = {}

    async def eod_builder(*, target_date, daily, limit, crowding, sectors, sentiment, now):
        captured.update(
            daily=daily,
            limit=limit,
            crowding=crowding,
            sectors=sectors,
            sentiment=sentiment,
        )
        return RadarSnapshotEnvelope(
            snapshot_type="eod",
            as_of=datetime.combine(target_date, datetime.min.time()),
            computed_at=now,
            status="partial",
            confidence=0.6,
            formula_version="market-radar-v1",
            metrics={"sources": sorted(data.calls)},
            source_freshness={name: {"status": "partial"} for name in data.calls},
            observations=(),
        )

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=data,
        store=MarketRadarStore(radar_session),
        eod_snapshot_builder=eod_builder,
        clock=FakeClock(),
    )
    result = await service.refresh_eod(date(2026, 7, 17))
    assert result.snapshot_type == "eod"
    assert sorted(data.calls) == ["crowding", "daily", "limit", "sector", "sentiment"]
    assert set(captured) == {"daily", "limit", "crowding", "sectors", "sentiment"}
    latest = await service.store.get_latest_snapshot(snapshot_type="eod")
    assert latest is not None
    assert json.loads(latest.metrics_json)["sources"] == sorted(data.calls)


def test_default_eod_builder_compacts_daily_facts_and_preserves_freshness_details(radar_session):
    service = MarketRadarService(
        feed=FakeFeed(), data_service=SimpleNamespace(), store=MarketRadarStore(radar_session)
    )
    freshness = SimpleNamespace(
        source="klines_daily",
        status="stale",
        expected_date=date(2026, 7, 17),
        source_date=date(2026, 7, 16),
        lag_trading_days=1,
        row_count=2,
        coverage=0.5,
        reason="source is one trading day old",
    )
    breadth = SimpleNamespace(
        status="fresh",
        flat_count=0,
        coverage=SimpleNamespace(
            requested=2,
            eligible=2,
            valid=2,
            excluded=0,
            coverage=1.0,
            status="fresh",
            missing=0,
            stale=0,
            invalid=0,
            suspended=0,
        ),
        buckets={},
    )
    market = SimpleNamespace(
        key="all",
        label="全A",
        eligible=2,
        valid=2,
        excluded=0,
        advance=1,
        decline=1,
        flat=0,
        median_return=-0.5,
        amount=100,
    )
    daily = SimpleNamespace(
        status="stale",
        expected_date=date(2026, 7, 17),
        source_freshness=freshness,
        universe_freshness=freshness,
        calendar=SimpleNamespace(freshness=freshness),
        universe=(SimpleNamespace(symbol="600000.SH"),),
        slices=(
            SimpleNamespace(
                trade_date=date(2026, 7, 16),
                previous_trade_date=date(2026, 7, 15),
                breadth=breadth,
                breakdowns=(market,),
                exclusion_counts=(),
                facts=(SimpleNamespace(symbol="600000.SH", close=10),),
            ),
        ),
    )
    limit = SimpleNamespace(status="stale", detail_freshness=freshness, step_freshness=freshness)
    crowding = SimpleNamespace(status="partial", as_of=date(2026, 7, 17), components=())
    sectors = SimpleNamespace(
        status="fresh", as_of=date(2026, 7, 17), sectors=(), source_freshness=freshness
    )
    sentiment = SimpleNamespace(status="unavailable", as_of=NOW, freshness=freshness)

    result = service._build_eod_snapshot(
        target_date=date(2026, 7, 17),
        daily=daily,
        limit=limit,
        crowding=crowding,
        sectors=sectors,
        sentiment=sentiment,
        now=NOW,
    )
    encoded = json.dumps(result.as_dict()["metrics"])
    assert "facts" not in encoded
    assert "universe" not in encoded
    assert "600000.SH" not in encoded
    assert result.metrics["breadth"]["days"][0]["trade_date"] == "2026-07-16"
    assert result.source_freshness["daily"]["reason"] == "source is one trading day old"
    assert result.source_freshness["daily"]["source_date"] == "2026-07-16"
    assert result.observations[0].subject == "ALL"


def test_default_intraday_builder_excludes_core_indices_from_all_a_breadth(radar_session):
    service = MarketRadarService(
        feed=FakeFeed(), data_service=SimpleNamespace(), store=MarketRadarStore(radar_session)
    )
    ticks = {
        "600000.SH": QuoteTick("600000.SH", NOW, 9.0, 10.0),
        "000001.SH": QuoteTick("000001.SH", NOW, 110.0, 100.0),
    }
    result = service._build_intraday_snapshot(
        ticks=ticks,
        feed_status=FakeFeed().status,
        focus=FocusUniverse((), (), (), (), {}, ()),
        now=NOW,
    )
    assert result.metrics["breadth"]["coverage"]["requested"] == 1
    market = next(item for item in result.observations if item.subject == "ALL")
    assert market.metrics["median_return_pct"].value == pytest.approx(-10)


def test_default_eod_builder_outputs_versioned_crowding_and_emotion_scores(radar_session):
    service = MarketRadarService(
        feed=FakeFeed(), data_service=SimpleNamespace(), store=MarketRadarStore(radar_session)
    )
    fresh = SourceFreshness(
        source="synthetic",
        status="fresh",
        expected_date=date(2026, 7, 17),
        source_date=date(2026, 7, 17),
        lag_trading_days=0,
        row_count=120,
        coverage=1,
    )
    keys = (
        "top_1_amount_share",
        "top_5_amount_share",
        "top_3_sector_share",
        "market_amount_vs_20d",
        "high_liquidity_correlation",
        "margin_balance_5d_change",
    )

    def crowding_inputs(available: int):
        return SimpleNamespace(
            status="fresh" if available == 6 else "partial",
            as_of=date(2026, 7, 17),
            components=tuple(
                RawComponent(
                    key=key,
                    current_value=120.0 if index < available else None,
                    history=tuple(float(value) for value in range(120)),
                    freshness=fresh,
                    excluded_reason=None if index < available else "missing_source",
                )
                for index, key in enumerate(keys)
            ),
        )

    def daily_slice(trade_day, median_return):
        market = SimpleNamespace(
            key="all",
            label="全市场",
            eligible=10,
            valid=10,
            excluded=0,
            advance=5,
            decline=5,
            flat=0,
            median_return=median_return,
            amount=100,
        )
        breadth = SimpleNamespace(
            status="fresh",
            flat_count=0,
            coverage=SimpleNamespace(
                requested=10,
                eligible=10,
                valid=10,
                excluded=0,
                coverage=1,
                status="fresh",
                missing=0,
                stale=0,
                invalid=0,
                suspended=0,
            ),
            buckets={},
        )
        return SimpleNamespace(
            trade_date=trade_day,
            previous_trade_date=trade_day - timedelta(days=1),
            breadth=breadth,
            breakdowns=(market,),
            exclusion_counts=(),
            facts=(),
        )

    daily = SimpleNamespace(
        status="fresh",
        expected_date=date(2026, 7, 17),
        slices=(daily_slice(date(2026, 7, 16), -2), daily_slice(date(2026, 7, 17), 1)),
        source_freshness=fresh,
        universe_freshness=fresh,
        calendar=SimpleNamespace(freshness=fresh),
    )
    limit = SimpleNamespace(
        status="fresh",
        down_count=5,
        promotion_rate=0.5,
        detail_freshness=fresh,
        step_freshness=fresh,
    )
    sectors = SimpleNamespace(status="fresh", sectors=(), source_freshness=fresh)
    sentiment = SimpleNamespace(
        status="fresh",
        weighted_score=0.5,
        daily_history=((date(2026, 7, 15), -0.5), (date(2026, 7, 16), 0.0)),
        freshness=fresh,
    )

    result = service._build_eod_snapshot(
        target_date=date(2026, 7, 17),
        daily=daily,
        limit=limit,
        crowding=crowding_inputs(6),
        sectors=sectors,
        sentiment=sentiment,
        now=NOW,
    )
    assert result.metrics["crowding"]["score"]["status"] == "fresh"
    assert result.metrics["crowding"]["score"]["value"] > 90
    assert result.metrics["crowding"]["label"] == "极端拥挤"
    assert result.metrics["overview"]["emotion"]["status"] == "fresh"
    assert len(result.metrics["overview"]["emotion"]["components"]) == 4
    market = next(item for item in result.observations if item.subject == "ALL")
    assert market.metrics["crowding_score"].status == "fresh"
    assert market.metrics["emotion_score"].status == "fresh"

    insufficient = service._build_eod_snapshot(
        target_date=date(2026, 7, 17),
        daily=daily,
        limit=limit,
        crowding=crowding_inputs(2),
        sectors=sectors,
        sentiment=sentiment,
        now=NOW,
    )
    assert insufficient.metrics["crowding"]["score"]["status"] == "insufficient"
    assert insufficient.metrics["crowding"]["score"]["value"] is None


@pytest.mark.asyncio
async def test_refresh_eod_does_not_concurrently_use_one_data_service_session(radar_session):
    class GuardedData(FakeEodData):
        def __init__(self):
            super().__init__()
            self.active = 0
            self.max_active = 0

        async def _call(self, name, result):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            await asyncio.sleep(0)
            self.calls.append(name)
            self.active -= 1
            return result

        async def load_daily_market(self, **kwargs):
            return await self._call(
                "daily", SimpleNamespace(status="fresh", expected_date=kwargs["target_date"])
            )

        async def load_limit_ladder(self, **kwargs):
            return await self._call(
                "limit", SimpleNamespace(status="fresh", trade_date=kwargs["target_date"])
            )

        async def load_crowding_inputs(self, **kwargs):
            return await self._call(
                "crowding", SimpleNamespace(status="fresh", as_of=kwargs["target_date"])
            )

        async def load_sector_inputs(self, **kwargs):
            return await self._call(
                "sector", SimpleNamespace(status="fresh", as_of=kwargs["target_date"])
            )

        async def load_sentiment_inputs(self, **kwargs):
            return await self._call(
                "sentiment", SimpleNamespace(status="fresh", as_of=kwargs["as_of"])
            )

    data = GuardedData()

    async def builder(**kwargs):
        return RadarSnapshotEnvelope(
            snapshot_type="eod",
            as_of=NOW,
            computed_at=NOW,
            status="fresh",
            confidence=1,
            formula_version="market-radar-v1",
            metrics={"overview": {}},
            source_freshness={},
        )

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=data,
        store=MarketRadarStore(radar_session),
        eod_snapshot_builder=builder,
    )
    await service.refresh_eod(date(2026, 7, 17))
    assert data.max_active == 1


@pytest.mark.asyncio
async def test_closed_market_reuses_latest_persisted_snapshot_without_intraday_write(radar_session):
    store = CountingStore(radar_session)
    await store.upsert_snapshot(
        snapshot_type="eod",
        as_of=datetime(2026, 7, 17, 15, 20),
        computed_at=datetime(2026, 7, 17, 15, 21),
        status="partial",
        confidence=0.6,
        formula_version="market-radar-v1",
        metrics={"overview": {"trade_date": "2026-07-17"}},
        source_freshness={"daily": {"status": "fresh"}},
    )
    await radar_session.commit()
    store.snapshot_calls = 0
    feed = FakeFeed()
    feed._status = RealtimeFeedStatus(
        mode="closed",
        changed_at=NOW,
        last_quote_at=None,
        connection_generation=1,
        reason="market session closed",
        market_coverage={"SH": 0, "SZ": 0, "BJ": 0, "INDEX": 0},
    )
    service = MarketRadarService(
        feed=feed, data_service=SimpleNamespace(), store=store, clock=FakeClock()
    )
    result = await service.refresh_intraday()
    assert result.snapshot_type == "eod"
    assert result.metrics["overview"]["trade_date"] == "2026-07-17"
    assert store.snapshot_calls == 0


@pytest.mark.asyncio
async def test_start_rolls_back_on_feed_failure_and_loop_recovers_after_refresh_error(
    radar_session,
):
    class FlakyFeed(FakeFeed):
        def __init__(self):
            super().__init__()
            self.start_calls = 0
            self.fail_start = True
            self.fail_health = True

        async def start(self):
            self.start_calls += 1
            if self.fail_start:
                raise RuntimeError("start failed")

        async def stop(self):
            return None

        async def run_health_cycle(self):
            self.health_calls += 1
            if self.fail_health:
                self.fail_health = False
                raise RuntimeError("transient refresh")

    feed = FlakyFeed()

    async def builder(**kwargs):
        return envelope(as_of=kwargs["now"])

    service = MarketRadarService(
        feed=feed,
        data_service=SimpleNamespace(),
        store=MarketRadarStore(radar_session),
        snapshot_builder=builder,
        intraday_coalesce_seconds=0.01,
    )
    with pytest.raises(RuntimeError, match="start failed"):
        await service.start()
    assert service.started is False

    feed.fail_start = False
    await service.start()
    for _ in range(100):
        if service.current_envelope() is not None:
            break
        await asyncio.sleep(0.01)
    assert feed.health_calls >= 2
    assert service.last_loop_error is not None
    assert service.current_envelope() is not None, service.last_loop_error
    await service.stop()


@pytest.mark.asyncio
async def test_focus_resolution_is_cached_between_realtime_frames(radar_session):
    calls = 0

    class Resolver:
        async def resolve(self):
            nonlocal calls
            calls += 1
            return FocusUniverse((), (), (), (), {}, ())

    clock = FakeClock()

    async def builder(**kwargs):
        return envelope(as_of=kwargs["now"])

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=SimpleNamespace(),
        store=MarketRadarStore(radar_session),
        snapshot_builder=builder,
        focus_resolver=Resolver(),
        clock=clock,
    )
    await service.refresh_intraday()
    clock.advance(1)
    await service.refresh_intraday()
    assert calls == 1
    clock.advance(30)
    await service.refresh_intraday()
    assert calls == 2

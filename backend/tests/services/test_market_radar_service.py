from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest
import pytest_asyncio
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.db.models.base import Base
from app.db.models.market_radar import MarketAlertEvent, MarketRadarSnapshot
from app.db.models.sentiment import SentimentPost
from app.db.models.stock import Stock
from app.services.market_radar import (
    DEFAULT_RULE_VERSION,
    BrokerDisconnected,
    EligibleUniverse,
    FocusUniverse,
    FocusUniverseResolver,
    IntradaySymbolContext,
    MarketAlertEngine,
    MarketRadarService,
    MarketRadarStreamBroker,
    MetricValue,
    RadarHistoryContext,
    RadarObservation,
    RadarSnapshotEnvelope,
    RuleDefinition,
)
from app.services.market_radar_calculator import QuoteTick
from app.services.market_radar_data import RawComponent, SourceFreshness
from app.services.market_radar_intraday_context import MarketRadarIntradayContextLoader
from app.services.market_radar_store import MarketRadarStore
from app.services.qmt_realtime_feed import RealtimeFeedStatus

NOW = datetime(2026, 7, 17, 10, 0, 0)


def test_market_radar_reexports_split_contracts_and_broker():
    from app.services import market_radar
    from app.services.market_radar_broker import (
        BrokerDisconnected as SplitBrokerDisconnected,
    )
    from app.services.market_radar_broker import (
        MarketRadarStreamBroker as SplitMarketRadarStreamBroker,
    )
    from app.services.market_radar_contracts import (
        MetricValue as SplitMetricValue,
    )
    from app.services.market_radar_contracts import (
        RadarSnapshotEnvelope as SplitRadarSnapshotEnvelope,
    )

    assert market_radar.BrokerDisconnected is SplitBrokerDisconnected
    assert market_radar.MarketRadarStreamBroker is SplitMarketRadarStreamBroker
    assert market_radar.MetricValue is SplitMetricValue
    assert market_radar.RadarSnapshotEnvelope is SplitRadarSnapshotEnvelope


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
    assert broker.subscriber_count == 0
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


@pytest.mark.asyncio
async def test_initial_subscription_loads_persisted_snapshot_before_atomic_alerts(radar_session):
    store = MarketRadarStore(radar_session)
    await store.upsert_snapshot(
        snapshot_type="eod",
        as_of=datetime(2026, 7, 17, 15, 20),
        computed_at=datetime(2026, 7, 17, 15, 21),
        status="fresh",
        confidence=1,
        formula_version="market-radar-v1",
        metrics={"overview": {"status": "fresh"}},
        source_freshness={
            "daily": {
                "status": "fresh",
                "source_date": "2026-07-17",
                "reason": None,
            }
        },
    )
    rule = await store.upsert_rule(
        rule_key="initial_high",
        version=1,
        scope="market",
        subject="*",
        rule_type="metric_threshold",
        parameters={"metric": "x", "operator": "lte", "threshold": 0},
        severity="high",
        cooldown_seconds=900,
        enabled=True,
        source="user",
    )
    await store.record_event_hit(
        rule_id=rule.id,
        snapshot_id=None,
        scope="market",
        subject="ALL",
        direction="down",
        severity="high",
        title="初始高风险",
        explanation="证据",
        dedupe_key="initial-high",
        evidence={"value": -1},
        seen_at=NOW,
    )
    await radar_session.commit()

    service = MarketRadarService(
        feed=FakeFeed(), data_service=SimpleNamespace(), store=store, clock=FakeClock()
    )
    subscription = await service.subscribe_with_initial()
    events = [await subscription.get() for _ in range(3)]
    assert [event.event for event in events] == ["mode", "snapshot", "alert"]
    projected = service.project_snapshot(service.current_envelope())
    assert events[1].data == projected
    assert set(projected) == {
        "as_of",
        "computed_at",
        "status",
        "confidence",
        "realtime_mode",
        "sources",
        "data",
    }
    assert projected["realtime_mode"] == "closed"
    assert projected["data"]["overview"]["status"] == "fresh"
    assert projected["sources"][0]["name"] == "daily"
    assert projected["sources"][0]["as_of"] == "2026-07-17"
    assert set(projected["sources"][0]).issuperset({"name", "as_of", "status", "reason"})
    assert [event.sequence for event in events] == sorted(event.sequence for event in events)


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
        eligible_universe=EligibleUniverse(("600000.SH",), "fresh", NOW, "sqlite_stocks"),
        symbol_context={},
        now=NOW,
    )
    assert result.metrics["breadth"]["coverage"]["requested"] == 1
    market = next(item for item in result.observations if item.subject == "ALL")
    assert market.metrics["median_return_pct"].value == pytest.approx(-10)


@pytest.mark.asyncio
async def test_intraday_uses_full_eligible_universe_and_gates_partial_market_rules(
    radar_session,
):
    feed = FakeFeed()
    feed._ticks["600000.SH"] = QuoteTick("600000.SH", NOW, 9.0, 10.0)

    async def eligible():
        return EligibleUniverse(
            symbols=("600000.SH", "000001.SZ"),
            status="fresh",
            as_of=NOW,
            source="sqlite_stocks",
        )

    service = MarketRadarService(
        feed=feed,
        data_service=SimpleNamespace(),
        store=MarketRadarStore(radar_session),
        eligible_universe_loader=eligible,
        clock=FakeClock(),
    )
    result = await service.refresh_intraday()
    assert result.metrics["breadth"]["coverage"]["requested"] == 2
    assert result.metrics["breadth"]["coverage"]["valid"] == 1
    assert result.status == "partial"
    market = next(item for item in result.observations if item.subject == "ALL")
    assert market.metrics["median_return_pct"].value == pytest.approx(-10)
    assert market.metrics["median_return_pct"].status == "partial"
    assert await event_rows(radar_session) == []


def test_realtime_tick_validation_uses_push_five_seconds_and_polling_forty_five_seconds(
    radar_session,
):
    service = MarketRadarService(
        feed=FakeFeed(), data_service=SimpleNamespace(), store=MarketRadarStore(radar_session)
    )
    focus = FocusUniverse(
        holdings=("600000.SH",),
        watchlist=(),
        focus=("600000.SH",),
        symbols=("600000.SH",),
        sources={"600000.SH": ("qmt_holding",)},
    )
    eligible = EligibleUniverse(("600000.SH",), "fresh", NOW, "sqlite_stocks")
    stale_for_push = QuoteTick("600000.SH", NOW - timedelta(seconds=6), 9.0, 10.0, high_price=10.0)
    index = QuoteTick("000001.SH", NOW - timedelta(seconds=6), 90.0, 100.0)

    push = service._build_intraday_snapshot(
        ticks={"600000.SH": stale_for_push, "000001.SH": index},
        feed_status=FakeFeed().status,
        focus=focus,
        eligible_universe=eligible,
        symbol_context={},
        now=NOW,
    )
    market = next(item for item in push.observations if item.subject == "ALL")
    symbol = next(item for item in push.observations if item.subject == "600000.SH")
    core = next(item for item in push.observations if item.subject == "000001.SH")
    assert market.metrics["median_return_pct"].status == "partial"
    assert symbol.metrics["return_pct"].status == "unavailable"
    assert core.metrics["return_pct"].status == "unavailable"

    poll_status = RealtimeFeedStatus(
        mode="polling_30s",
        changed_at=NOW,
        last_quote_at=NOW - timedelta(seconds=6),
        connection_generation=1,
        reason="push stale",
        market_coverage={"SH": 1, "SZ": 1, "BJ": 1, "INDEX": 1},
    )
    polling = service._build_intraday_snapshot(
        ticks={"600000.SH": stale_for_push, "000001.SH": index},
        feed_status=poll_status,
        focus=focus,
        eligible_universe=eligible,
        symbol_context={},
        now=NOW,
    )
    polled_symbol = next(item for item in polling.observations if item.subject == "600000.SH")
    assert polling.status == "fresh"
    assert polled_symbol.metrics["return_pct"].status == "fresh"

    offline_status = RealtimeFeedStatus(
        mode="offline",
        changed_at=NOW,
        last_quote_at=NOW,
        connection_generation=1,
        reason="QMT unavailable",
        market_coverage={"SH": 1, "SZ": 1, "BJ": 1, "INDEX": 1},
    )
    offline = service._build_intraday_snapshot(
        ticks={"600000.SH": QuoteTick("600000.SH", NOW, 9, 10)},
        feed_status=offline_status,
        focus=focus,
        eligible_universe=eligible,
        symbol_context={},
        now=NOW,
    )
    assert offline.status == "unavailable"


def test_intraday_enrichment_activates_holding_limit_volume_and_sentiment_rules(radar_session):
    service = MarketRadarService(
        feed=FakeFeed(), data_service=SimpleNamespace(), store=MarketRadarStore(radar_session)
    )
    focus = FocusUniverse(
        holdings=("600000.SH",),
        watchlist=(),
        focus=("600000.SH",),
        symbols=("600000.SH",),
        sources={"600000.SH": ("qmt_holding",)},
    )
    tick = QuoteTick("600000.SH", NOW, 9.045, 10.0, high_price=11.0, volume=1_000_000)
    context = IntradaySymbolContext(
        metrics={
            "volume_ratio_20d": metric(3.0, source="klines_daily_20d"),
            "down_limit_price": metric(9.0, source="stock_limit_prices"),
            "up_limit_price": metric(11.0, source="stock_limit_prices"),
            "negative_heat_z20": metric(2.1, source="sentiment_analysis"),
            "weighted_sentiment": metric(-0.5, source="sentiment_analysis"),
        }
    )
    snapshot = service._build_intraday_snapshot(
        ticks={"600000.SH": tick},
        feed_status=FakeFeed().status,
        focus=focus,
        eligible_universe=EligibleUniverse(("600000.SH",), "fresh", NOW, "sqlite_stocks"),
        symbol_context={"600000.SH": context},
        now=NOW,
    )
    item = next(value for value in snapshot.observations if value.subject == "600000.SH")
    assert item.metrics["volume_ratio_20d"].status == "fresh"
    assert item.metrics["down_limit_distance_pct"].value == pytest.approx(0.5)
    assert item.metrics["limit_up_broken"].value is True
    matches = MarketAlertEngine().evaluate(snapshot).matches
    assert {
        "holding_volume_price_anomaly",
        "holding_near_limit_down",
        "holding_limit_up_broken",
        "symbol_negative_sentiment_heat",
    }.issubset({match.rule.key for match in matches})


def test_intraday_missing_enrichment_is_structured_unavailable(radar_session):
    service = MarketRadarService(
        feed=FakeFeed(), data_service=SimpleNamespace(), store=MarketRadarStore(radar_session)
    )
    focus = FocusUniverse(
        holdings=("600000.SH",),
        watchlist=(),
        focus=("600000.SH",),
        symbols=("600000.SH",),
        sources={"600000.SH": ("qmt_holding",)},
    )
    snapshot = service._build_intraday_snapshot(
        ticks={"600000.SH": QuoteTick("600000.SH", NOW, 10.0, 10.0)},
        feed_status=FakeFeed().status,
        focus=focus,
        eligible_universe=EligibleUniverse(("600000.SH",), "fresh", NOW, "sqlite_stocks"),
        symbol_context={},
        now=NOW,
    )
    item = next(value for value in snapshot.observations if value.subject == "600000.SH")
    assert item.metrics["volume_ratio_20d"].status == "unavailable"
    assert item.metrics["down_limit_distance_pct"].reason == "exact down-limit price is unavailable"
    assert (
        snapshot.metrics["focus"]["metric_status"]["600000.SH"]["weighted_sentiment"]["status"]
        == "unavailable"
    )
    projected_sources = service.project_snapshot(snapshot)["sources"]
    assert [source["name"] for source in projected_sources] == [
        "qmt_realtime",
        "eligible_universe",
        "klines_daily_20d",
        "stock_limit_prices",
        "sentiment_posts",
    ]
    assert all(
        set(source).issuperset({"name", "as_of", "status", "reason"})
        for source in projected_sources
    )
    unavailable = {source["name"]: source for source in projected_sources}
    assert unavailable["klines_daily_20d"]["status"] == "unavailable"
    assert unavailable["stock_limit_prices"]["reason"]
    assert unavailable["sentiment_posts"]["as_of"] is None


@pytest.mark.asyncio
async def test_default_intraday_context_loader_batches_universe_volume_limits_and_sentiment(
    radar_session,
):
    radar_session.add_all(
        [
            Stock(
                symbol="600000.SH",
                exchange="SH",
                list_date=date(2000, 1, 1),
                is_delist=0,
                is_suspend=0,
            ),
            Stock(
                symbol="600001.SH",
                exchange="SH",
                list_date=date(2000, 1, 1),
                is_delist=0,
                is_suspend=0,
            ),
            Stock(
                symbol="000001.SZ",
                exchange="SZ",
                list_date=date(2000, 1, 1),
                is_delist=0,
                is_suspend=1,
            ),
        ]
    )
    await radar_session.execute(
        text(
            "CREATE TABLE stock_limit_prices ("
            "symbol TEXT, trade_date DATE, up_limit REAL, down_limit REAL)"
        )
    )
    await radar_session.execute(
        text(
            "INSERT INTO stock_limit_prices(symbol, trade_date, up_limit, down_limit) "
            "VALUES (:symbol, :trade_date, :up, :down)"
        ),
        {
            "symbol": "600000.SH",
            "trade_date": NOW.date().isoformat(),
            "up": 11.0,
            "down": 9.0,
        },
    )
    for offset in range(1, 21):
        radar_session.add(
            SentimentPost(
                source="test",
                source_post_id=f"history-{offset}",
                symbol="600000.SH",
                sentiment_score=-0.5,
                reply_count=offset,
                like_count=offset % 3,
                comment_count=0,
                published_at=NOW - timedelta(days=offset),
            )
        )
    radar_session.add(
        SentimentPost(
            source="test",
            source_post_id="current",
            symbol="600000.SH",
            sentiment_score=-0.5,
            reply_count=100,
            like_count=20,
            comment_count=10,
            published_at=NOW - timedelta(hours=1),
        )
    )
    await radar_session.commit()

    class DailyStore:
        def load_daily(self, symbols, start_date, end_date, columns):
            days = pd.date_range(end=NOW.date() - timedelta(days=1), periods=20)
            return pd.DataFrame(
                {
                    "symbol": ["600000.SH"] * 20 + ["600001.SH"] * 20,
                    "volume": [1_000.0] * 20 + [100_000.0] * 20,
                    "close": [10.0] * 40,
                    "amount": [1_000_000.0] * 40,
                },
                index=days.append(days),
            )

    loader = MarketRadarIntradayContextLoader(radar_session, market_store=DailyStore())
    universe = await loader.load_eligible_universe()
    assert universe.symbols == ("600000.SH", "600001.SH")
    context = await loader.load_symbol_context(
        ("600000.SH", "600001.SH"),
        {
            "600000.SH": QuoteTick("600000.SH", NOW, 10, 10, volume=300_000),
            "600001.SH": QuoteTick("600001.SH", NOW, 10, 10, volume=300_000),
        },
        NOW,
    )
    metrics = context["600000.SH"].metrics
    assert metrics["volume_ratio_20d"].value == pytest.approx(3.0)
    assert context["600001.SH"].metrics["volume_ratio_20d"].value == pytest.approx(3.0)
    assert metrics["down_limit_price"].value == 9.0
    assert metrics["up_limit_price"].value == 11.0
    assert metrics["weighted_sentiment"].value == pytest.approx(-0.5)
    assert metrics["negative_heat_z20"].status == "fresh"


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
    sectors = SimpleNamespace(
        status="fresh",
        sectors=(
            SimpleNamespace(
                industry="电子",
                median_return=-2.0,
                advance_ratio=0.2,
                share_z20=2.5,
                amount_vs_20d=2.0,
            ),
        ),
        source_freshness=fresh,
    )
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
        history=RadarHistoryContext(
            limit_down_counts=(10, 20, 30, 40, 50),
            limit_down_median_5d=30,
            previous_emotion_score=29,
            previous_as_of=datetime(2026, 7, 16, 15, 20),
        ),
        now=NOW,
    )
    assert result.metrics["crowding"]["score"]["status"] == "fresh"
    assert result.metrics["crowding"]["score"]["value"] > 90
    assert result.metrics["crowding"]["label"] == "极端拥挤"
    assert result.metrics["overview"]["emotion"]["status"] == "partial"
    assert result.metrics["overview"]["emotion"]["label"] is None
    assert result.metrics["overview"]["emotion"]["formula_version"].endswith("reduced-v1")
    assert len(result.metrics["overview"]["emotion"]["components"]) == 4
    market = next(item for item in result.observations if item.subject == "ALL")
    assert market.metrics["crowding_score"].status == "fresh"
    assert market.metrics["emotion_score"].status == "partial"
    assert market.metrics["limit_down_median_5d"].value == 30
    assert market.metrics["previous_emotion_score"].value == 29
    sector = next(item for item in result.observations if item.scope == "sector")
    assert sector.metrics["crowding_score"].status == "unavailable"
    assert sector.metrics["crowding_score"].reason == "independent sector crowding is unavailable"
    eod_rule_keys = {match.rule.key for match in MarketAlertEngine().evaluate(result).matches}
    assert "sector_share_crowding" not in eod_rule_keys
    assert "market_emotion_cross_up" not in eod_rule_keys
    assert "market_emotion_cross_down" not in eod_rule_keys

    insufficient = service._build_eod_snapshot(
        target_date=date(2026, 7, 17),
        daily=daily,
        limit=limit,
        crowding=crowding_inputs(2),
        sectors=sectors,
        sentiment=sentiment,
        history=RadarHistoryContext(),
        now=NOW,
    )
    assert insufficient.metrics["crowding"]["score"]["status"] == "insufficient"
    assert insufficient.metrics["crowding"]["score"]["value"] is None


@pytest.mark.asyncio
async def test_eod_history_context_reads_five_prior_snapshots_from_persistence(radar_session):
    store = MarketRadarStore(radar_session)
    for offset, down_count in enumerate((10, 20, 30, 40, 50), start=1):
        day = datetime(2026, 7, 11 + offset, 15, 20)
        await store.upsert_snapshot(
            snapshot_type="eod",
            as_of=day,
            computed_at=day,
            status="fresh",
            confidence=1,
            formula_version="market-radar-v1",
            metrics={
                "overview": {"emotion": {"status": "fresh", "value": 20 + offset}},
                "limit_ladder": {"status": "fresh", "down_count": down_count},
            },
            source_freshness={"limit_detail": {"status": "fresh"}},
        )
    await radar_session.commit()
    service = MarketRadarService(feed=FakeFeed(), data_service=SimpleNamespace(), store=store)
    history = await service._load_history_context(date(2026, 7, 17))
    assert history.limit_down_counts == (50, 40, 30, 20, 10)
    assert history.limit_down_median_5d == 30
    assert history.previous_emotion_score == 25
    assert history.previous_as_of == datetime(2026, 7, 16, 15, 20)


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
async def test_intraday_and_eod_refreshes_share_one_session_lock(radar_session):
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingEodData(FakeEodData):
        async def load_daily_market(self, **kwargs):
            entered.set()
            await release.wait()
            return await super().load_daily_market(**kwargs)

    feed = FakeFeed()

    async def intraday_builder(**kwargs):
        return envelope(as_of=kwargs["now"])

    async def eod_builder(**kwargs):
        return RadarSnapshotEnvelope(
            snapshot_type="eod",
            as_of=NOW,
            computed_at=NOW,
            status="partial",
            confidence=0.5,
            formula_version="market-radar-v1",
            metrics={"overview": {"status": "partial"}},
            source_freshness={},
        )

    service = MarketRadarService(
        feed=feed,
        data_service=BlockingEodData(),
        store=MarketRadarStore(radar_session),
        snapshot_builder=intraday_builder,
        eod_snapshot_builder=eod_builder,
        clock=FakeClock(),
    )
    eod_task = asyncio.create_task(service.refresh_eod(date(2026, 7, 17)))
    await asyncio.wait_for(entered.wait(), timeout=1)
    intraday_task = asyncio.create_task(service.refresh_intraday())
    await asyncio.sleep(0)
    assert feed.health_calls == 0
    release.set()
    await asyncio.gather(eod_task, intraday_task)
    assert feed.health_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("refresh_kind", ["intraday", "eod"])
async def test_cancelled_refresh_rolls_back_flushed_transaction_and_can_restart(
    radar_session,
    refresh_kind,
):
    class BlockingSnapshotStore(MarketRadarStore):
        def __init__(self, session):
            super().__init__(session)
            self.flushed = asyncio.Event()
            self.block_once = True

        async def upsert_snapshot(self, **kwargs):
            row = await super().upsert_snapshot(**kwargs)
            if self.block_once:
                self.block_once = False
                self.flushed.set()
                await asyncio.Event().wait()
            return row

    store = BlockingSnapshotStore(radar_session)

    async def intraday_builder(**kwargs):
        return envelope(as_of=kwargs["now"])

    async def eod_builder(**kwargs):
        return RadarSnapshotEnvelope(
            snapshot_type="eod",
            as_of=NOW,
            computed_at=NOW,
            status="partial",
            confidence=0.5,
            formula_version="market-radar-v1",
            metrics={"overview": {"status": "partial"}},
            source_freshness={},
        )

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=FakeEodData(),
        store=store,
        snapshot_builder=intraday_builder,
        eod_snapshot_builder=eod_builder,
        clock=FakeClock(),
    )
    refresh = (
        service.refresh_intraday
        if refresh_kind == "intraday"
        else lambda: service.refresh_eod(date(2026, 7, 17))
    )
    task = asyncio.create_task(refresh())
    await asyncio.wait_for(store.flushed.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    rows = list((await radar_session.execute(select(MarketRadarSnapshot))).scalars())
    assert rows == []
    restarted = await refresh()
    assert restarted.snapshot_type == refresh_kind


@pytest.mark.asyncio
async def test_cancelled_intraday_context_read_rolls_back_before_restart(radar_session):
    entered = asyncio.Event()
    block_once = True

    async def eligible():
        nonlocal block_once
        await radar_session.execute(select(MarketRadarSnapshot).limit(1))
        if block_once:
            block_once = False
            entered.set()
            await asyncio.Event().wait()
        return EligibleUniverse(("600000.SH",), "fresh", NOW, "sqlite_stocks")

    async def contexts(_symbols, _ticks, _now):
        return {}

    async def builder(**kwargs):
        return envelope(as_of=kwargs["now"])

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=SimpleNamespace(),
        store=MarketRadarStore(radar_session),
        snapshot_builder=builder,
        eligible_universe_loader=eligible,
        symbol_context_loader=contexts,
        clock=FakeClock(),
    )
    task = asyncio.create_task(service.refresh_intraday())
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not radar_session.in_transaction()
    assert (await service.refresh_intraday()).snapshot_type == "intraday"


@pytest.mark.asyncio
async def test_cancelled_eod_source_read_rolls_back_before_restart(radar_session):
    entered = asyncio.Event()

    class BlockingEodData(FakeEodData):
        def __init__(self):
            super().__init__()
            self.block_once = True

        async def load_daily_market(self, **kwargs):
            await radar_session.execute(select(MarketRadarSnapshot).limit(1))
            if self.block_once:
                self.block_once = False
                entered.set()
                await asyncio.Event().wait()
            return await super().load_daily_market(**kwargs)

    async def builder(**kwargs):
        return RadarSnapshotEnvelope(
            snapshot_type="eod",
            as_of=NOW,
            computed_at=NOW,
            status="partial",
            confidence=0.5,
            formula_version="market-radar-v1",
            metrics={"overview": {"status": "partial"}},
            source_freshness={},
        )

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=BlockingEodData(),
        store=MarketRadarStore(radar_session),
        eod_snapshot_builder=builder,
        clock=FakeClock(),
    )
    task = asyncio.create_task(service.refresh_eod(date(2026, 7, 17)))
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not radar_session.in_transaction()
    assert (await service.refresh_eod(date(2026, 7, 17))).snapshot_type == "eod"


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

    offline_feed = FakeFeed()
    offline_feed._status = RealtimeFeedStatus(
        mode="offline",
        changed_at=NOW,
        last_quote_at=NOW,
        connection_generation=2,
        reason="QMT unavailable with residual ticks",
        market_coverage={"SH": 1, "SZ": 0, "BJ": 0, "INDEX": 0},
    )
    offline = MarketRadarService(
        feed=offline_feed, data_service=SimpleNamespace(), store=store, clock=FakeClock()
    )
    offline_result = await offline.refresh_intraday()
    assert offline_result.snapshot_type == "eod"
    assert store.snapshot_calls == 0


@pytest.mark.asyncio
async def test_start_rolls_back_on_feed_failure_and_loop_recovers_after_refresh_error(
    radar_session,
):
    class FlakyFeed(FakeFeed):
        def __init__(self):
            super().__init__()
            self.start_calls = 0
            self.stop_calls = 0
            self.partially_started = False
            self.fail_start = True
            self.fail_stop = True
            self.fail_health = True

        async def start(self):
            self.start_calls += 1
            self.partially_started = True
            if self.fail_start:
                raise RuntimeError("start failed")

        async def stop(self):
            self.stop_calls += 1
            self.partially_started = False
            if self.fail_stop:
                raise RuntimeError("feed cleanup failed")

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
    assert feed.stop_calls == 1
    assert feed.partially_started is False
    assert service.last_cleanup_error == "RuntimeError: feed cleanup failed"

    feed.fail_start = False
    feed.fail_stop = False
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
async def test_cancelled_feed_start_still_cleans_up_partial_start(radar_session):
    entered = asyncio.Event()

    class BlockingStartFeed(FakeFeed):
        def __init__(self):
            super().__init__()
            self.stop_calls = 0
            self.partially_started = False

        async def start(self):
            self.partially_started = True
            entered.set()
            await asyncio.Event().wait()

        async def stop(self):
            self.stop_calls += 1
            self.partially_started = False

    feed = BlockingStartFeed()
    service = MarketRadarService(
        feed=feed,
        data_service=SimpleNamespace(),
        store=MarketRadarStore(radar_session),
    )
    task = asyncio.create_task(service.start())
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert service.started is False
    assert feed.stop_calls == 1
    assert feed.partially_started is False


@pytest.mark.asyncio
async def test_start_preserves_setup_error_when_rollback_fails_and_cleans_feed():
    class Feed(FakeFeed):
        def __init__(self):
            super().__init__()
            self.stop_calls = 0

        async def start(self):
            return None

        async def stop(self):
            self.stop_calls += 1

    class Session:
        async def commit(self):
            return None

        async def rollback(self):
            raise RuntimeError("rollback cleanup failed")

    class FailingAlertEngine(MarketAlertEngine):
        async def persist(self, *_args, **_kwargs):
            raise ValueError("startup setup failed")

    feed = Feed()
    store = SimpleNamespace(session=Session())
    service = MarketRadarService(
        feed=feed,
        data_service=SimpleNamespace(),
        store=store,
        alert_engine=FailingAlertEngine(),
    )

    with pytest.raises(ValueError, match="startup setup failed"):
        await service.start()
    assert feed.stop_calls == 1
    assert service.last_cleanup_error == "RuntimeError: session rollback failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback_fails", [False, True])
async def test_loop_rolls_back_failed_transaction_and_survives_rollback_failure(rollback_fails):
    class Session:
        def __init__(self):
            self.calls = 0

        async def rollback(self):
            self.calls += 1
            if rollback_fails and self.calls == 1:
                raise RuntimeError("rollback failed")

    session = Session()

    async def eligible():
        return EligibleUniverse((), "unavailable", NOW, "test")

    async def contexts(_symbols, _ticks, _now):
        return {}

    service = MarketRadarService(
        feed=FakeFeed(),
        data_service=SimpleNamespace(),
        store=SimpleNamespace(session=session),
        eligible_universe_loader=eligible,
        symbol_context_loader=contexts,
        intraday_coalesce_seconds=0.01,
    )
    recovered = asyncio.Event()
    refresh_calls = 0

    async def refresh():
        nonlocal refresh_calls
        refresh_calls += 1
        if refresh_calls == 1:
            raise RuntimeError("failed transaction")
        recovered.set()
        return envelope()

    service.refresh_intraday = refresh
    task = asyncio.create_task(service._run_loop())
    await asyncio.wait_for(recovered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert session.calls >= 1
    assert refresh_calls >= 2
    assert service.last_loop_error is not None


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

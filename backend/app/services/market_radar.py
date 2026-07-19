"""Market-radar orchestration, typed alerts, and bounded stream delivery."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time
from statistics import median
from types import MappingProxyType
from typing import Any, Literal, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.market_radar import MarketAlertEvent, MarketAlertRule
from app.db.models.watchlist import WatchlistStock
from app.services.market_radar_calculator import (
    CROWDING_COMPONENT_WEIGHTS,
    EMOTION_COMPONENT_WEIGHTS,
    QuoteTick,
    ScoreComponent,
    calculate_breadth,
    composite_score,
    crowding_label,
    emotion_label,
    serialize_breadth_result,
)
from app.services.market_radar_data import serialize_market_radar_data
from app.services.market_radar_store import (
    MarketRadarStore,
    dump_json_object,
    load_json_object,
)

FreshnessStatus = Literal["fresh", "partial", "stale", "unavailable"]
RadarScope = Literal["market", "sector", "symbol", "data"]
SnapshotType = Literal["intraday", "eod"]
Severity = Literal["low", "medium", "high"]
StreamEventType = Literal["mode", "snapshot", "alert", "heartbeat"]

DEFAULT_RULE_VERSION = 1
DEFAULT_FORMULA_VERSION = "market-radar-v1"
DEFAULT_COOLDOWN_SECONDS = 15 * 60
CORE_INDICES = frozenset({"000001.SH", "399001.SZ", "000985.SH"})
_OPEN_EVENT_STATUSES = ("active", "acknowledged", "dismissed")
_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}
_UNSAFE_SNAPSHOT_TERMS = ("tick", "quantity", "cost", "account", "position_size")


@dataclass(frozen=True, slots=True)
class MetricValue:
    value: float | bool | None
    status: FreshnessStatus
    as_of: datetime
    source: str
    baseline: float | str | None = None


@dataclass(frozen=True, slots=True)
class RadarObservation:
    scope: RadarScope
    subject: str
    metrics: Mapping[str, MetricValue]
    sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))
        object.__setattr__(self, "sources", tuple(dict.fromkeys(self.sources)))


@dataclass(frozen=True, slots=True)
class RadarSnapshotEnvelope:
    snapshot_type: SnapshotType
    as_of: datetime
    computed_at: datetime
    status: FreshnessStatus
    confidence: float
    formula_version: str
    metrics: Mapping[str, Any]
    source_freshness: Mapping[str, Any]
    observations: tuple[RadarObservation, ...] = ()

    def __post_init__(self) -> None:
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))
        object.__setattr__(
            self,
            "source_freshness",
            MappingProxyType(dict(self.source_freshness)),
        )
        object.__setattr__(self, "observations", tuple(self.observations))

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot_type": self.snapshot_type,
            "as_of": self.as_of.isoformat(),
            "computed_at": self.computed_at.isoformat(),
            "status": self.status,
            "confidence": self.confidence,
            "formula_version": self.formula_version,
            "metrics": serialize_market_radar_data(self.metrics),
            "source_freshness": serialize_market_radar_data(self.source_freshness),
        }


@dataclass(frozen=True, slots=True)
class RuleDefinition:
    key: str
    scope: RadarScope
    severity: Severity
    title: str
    direction: str
    rule_type: str = ""
    parameters: Mapping[str, Any] = field(default_factory=dict)
    subject: str = "*"
    enabled: bool = True
    source: Literal["system", "user"] = "system"
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS
    version: int = DEFAULT_RULE_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        if self.rule_type == "metric_threshold":
            metric_name = self.parameters.get("metric")
            operator = self.parameters.get("operator")
            threshold = self.parameters.get("threshold")
            if not isinstance(metric_name, str) or not metric_name:
                raise ValueError("metric_threshold requires a metric")
            if operator not in {"lte", "gte", "abs_gte"}:
                raise ValueError("metric_threshold operator is invalid")
            if not isinstance(threshold, (int, float)) or not math.isfinite(float(threshold)):
                raise ValueError("metric_threshold requires a finite threshold")


_DEFAULT_RULE_SPECS: tuple[tuple[str, RadarScope, Severity, str, str, Mapping[str, Any]], ...] = (
    ("market_median_return_down", "market", "high", "全A中位数急跌", "down", {"lte": -2.5}),
    ("market_decline_ratio_high", "market", "high", "市场普跌", "down", {"gte": 0.8}),
    ("core_index_return_down", "market", "high", "核心指数急跌", "down", {"lte": -2}),
    ("core_index_5m_down", "market", "high", "核心指数五分钟急跌", "down", {"lte": -1}),
    (
        "market_limit_down_expansion",
        "market",
        "high",
        "跌停家数扩张",
        "down",
        {"limit_down_count_gte": 30, "multiple_gte": 2},
    ),
    (
        "market_crowding_weakness",
        "market",
        "high",
        "高拥挤下市场转弱",
        "down",
        {"crowding_gte": 80, "median_return_lte": -1},
    ),
    (
        "market_emotion_cross_down",
        "market",
        "medium",
        "情绪温度跌破恐慌线",
        "down",
        {"cross_down": 30},
    ),
    ("market_emotion_cross_up", "market", "medium", "情绪温度升破极端线", "up", {"cross_up": 85}),
    (
        "sector_breadth_down",
        "sector",
        "medium",
        "行业普跌",
        "down",
        {"median_return_lte": -2, "decline_ratio_gte": 0.8},
    ),
    (
        "sector_share_crowding",
        "sector",
        "medium",
        "行业成交拥挤",
        "up",
        {"share_z20_gte": 2.5, "crowding_gte": 80},
    ),
    (
        "holding_return_down",
        "symbol",
        "medium",
        "持仓股下跌",
        "down",
        {"medium_lte": -3, "high_lte": -5},
    ),
    (
        "holding_intraday_drawdown",
        "symbol",
        "medium",
        "持仓股日内回撤",
        "down",
        {"medium_gte": 3, "high_gte": 5},
    ),
    (
        "holding_volume_price_anomaly",
        "symbol",
        "medium",
        "持仓股量价异常",
        "either",
        {"volume_ratio_gte": 2.5, "abs_return_gte": 2},
    ),
    ("holding_near_limit_down", "symbol", "high", "持仓股接近跌停", "down", {"lte": 0.5}),
    ("holding_limit_up_broken", "symbol", "high", "持仓股涨停开板", "down", {"eq": True}),
    ("watchlist_return_down", "symbol", "high", "自选股大跌", "down", {"lte": -7}),
    ("watchlist_intraday_drawdown", "symbol", "high", "自选股日内大幅回撤", "down", {"gte": 5}),
    ("watchlist_near_limit_down", "symbol", "high", "自选股接近跌停", "down", {"lte": 0.5}),
    (
        "symbol_negative_sentiment_heat",
        "symbol",
        "medium",
        "个股负面舆情升温",
        "down",
        {"negative_heat_z20_gte": 2, "weighted_sentiment_lte": -0.35},
    ),
)
DEFAULT_RULE_DEFINITIONS = tuple(
    RuleDefinition(
        key=key,
        scope=scope,
        severity=severity,
        title=title,
        direction=direction,
        rule_type=key,
        parameters=parameters,
    )
    for key, scope, severity, title, direction, parameters in _DEFAULT_RULE_SPECS
)
_DEFAULT_RULES_BY_KEY = MappingProxyType({rule.key: rule for rule in DEFAULT_RULE_DEFINITIONS})


@dataclass(frozen=True, slots=True)
class RuleMatch:
    rule: RuleDefinition
    scope: RadarScope
    subject: str
    direction: str
    severity: Severity
    title: str
    explanation: str
    dedupe_key: str
    evidence: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))


@dataclass(frozen=True, slots=True)
class RuleEvaluation:
    matches: tuple[RuleMatch, ...]
    evaluated_dedupe_keys: frozenset[str]
    evaluated_rules: Mapping[str, RuleDefinition]
    skipped_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "matches", tuple(self.matches))
        object.__setattr__(
            self,
            "evaluated_rules",
            MappingProxyType(dict(self.evaluated_rules)),
        )
        object.__setattr__(self, "skipped_reasons", tuple(self.skipped_reasons))


@dataclass(frozen=True, slots=True)
class AlertPersistenceResult:
    notifications: tuple[MarketAlertEvent, ...]
    touched_event_ids: tuple[int, ...]
    resolved_event_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class StreamEvent:
    sequence: int
    event_id: str
    event: StreamEventType
    data: Mapping[str, Any]
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", MappingProxyType(dict(self.data)))


class BrokerDisconnected(RuntimeError):
    """Raised when a stream client is closed or too slow to consume safely."""


class RadarSnapshotUnavailable(RuntimeError):
    """Raised when the market is closed and no prior snapshot exists."""


@dataclass(slots=True)
class _SubscriberState:
    id: int
    queue: deque[StreamEvent]
    condition: asyncio.Condition
    overflow_count: int = 0
    disconnected: bool = False
    disconnect_reason: str | None = None


class MarketRadarSubscription:
    def __init__(self, broker: MarketRadarStreamBroker, state: _SubscriberState) -> None:
        self._broker = broker
        self._state = state

    @property
    def id(self) -> int:
        return self._state.id

    @property
    def pending(self) -> int:
        return len(self._state.queue)

    @property
    def disconnect_reason(self) -> str | None:
        return self._state.disconnect_reason

    async def get(self) -> StreamEvent:
        state = self._state
        async with state.condition:
            await state.condition.wait_for(lambda: bool(state.queue) or state.disconnected)
            if state.disconnected:
                raise BrokerDisconnected(state.disconnect_reason or "subscription_closed")
            return state.queue.popleft()

    async def close(self) -> None:
        await self._broker.unsubscribe(self.id)

    def __aiter__(self) -> MarketRadarSubscription:
        return self

    async def __anext__(self) -> StreamEvent:
        try:
            return await self.get()
        except BrokerDisconnected as exc:
            raise StopAsyncIteration from exc


class MarketRadarStreamBroker:
    """Fan out aggregate events without allowing slow clients to block publishers."""

    def __init__(
        self,
        *,
        queue_size: int = 64,
        overflow_disconnect_threshold: int = 3,
        heartbeat_seconds: float = 15.0,
        clock: Callable[[], datetime] = datetime.now,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        if overflow_disconnect_threshold < 1:
            raise ValueError("overflow_disconnect_threshold must be positive")
        if not math.isfinite(heartbeat_seconds) or heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be finite and positive")
        self._queue_size = queue_size
        self._hard_limit = queue_size + overflow_disconnect_threshold - 1
        self._overflow_disconnect_threshold = overflow_disconnect_threshold
        self._heartbeat_seconds = heartbeat_seconds
        self._clock = clock
        self._subscribers: dict[int, _SubscriberState] = {}
        self._subscriber_lock = asyncio.Lock()
        self._next_subscriber_id = 1
        self._sequence = 0
        self._last_heartbeat_at: datetime | None = None

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def subscribe(
        self,
        *,
        initial_events: Iterable[tuple[StreamEventType, Mapping[str, Any]]] = (),
    ) -> MarketRadarSubscription:
        async with self._subscriber_lock:
            subscriber_id = self._next_subscriber_id
            self._next_subscriber_id += 1
            state = _SubscriberState(
                id=subscriber_id,
                queue=deque(maxlen=self._hard_limit),
                condition=asyncio.Condition(),
            )
            for event_type, data in initial_events:
                if len(state.queue) >= self._hard_limit:
                    raise ValueError("initial stream state exceeds subscriber queue capacity")
                state.queue.append(self._new_event(event_type, data, self._clock()))
            self._subscribers[subscriber_id] = state
        return MarketRadarSubscription(self, state)

    async def unsubscribe(self, subscriber_id: int) -> None:
        async with self._subscriber_lock:
            state = self._subscribers.pop(subscriber_id, None)
        if state is None:
            return
        async with state.condition:
            state.disconnected = True
            state.disconnect_reason = state.disconnect_reason or "subscription_closed"
            state.queue.clear()
            state.condition.notify_all()

    async def publish(
        self,
        event: StreamEventType,
        data: Mapping[str, Any],
        *,
        created_at: datetime | None = None,
    ) -> StreamEvent:
        async with self._subscriber_lock:
            stream_event = self._new_event(event, data, created_at or self._clock())
            states = tuple(self._subscribers.values())
        for state in states:
            await self._offer(state, stream_event)
        return stream_event

    def _new_event(
        self,
        event: StreamEventType,
        data: Mapping[str, Any],
        created_at: datetime,
    ) -> StreamEvent:
        self._sequence += 1
        return StreamEvent(
            sequence=self._sequence,
            event_id=str(self._sequence),
            event=event,
            data=data,
            created_at=created_at,
        )

    async def heartbeat(self, now: datetime | None = None) -> bool:
        current = now or self._clock()
        if (
            self._last_heartbeat_at is not None
            and (current - self._last_heartbeat_at).total_seconds() < self._heartbeat_seconds
        ):
            return False
        self._last_heartbeat_at = current
        await self.publish("heartbeat", {"at": current.isoformat()}, created_at=current)
        return True

    async def _offer(self, state: _SubscriberState, event: StreamEvent) -> None:
        async with state.condition:
            if state.disconnected:
                return
            if event.event == "snapshot":
                for index, pending in enumerate(state.queue):
                    if pending.event == "snapshot":
                        state.queue[index] = event
                        state.condition.notify()
                        return
            if len(state.queue) >= self._queue_size:
                dropped = self._drop_disposable(state.queue)
                if event.event in {"snapshot", "heartbeat"} and not dropped:
                    return
                if not dropped:
                    state.overflow_count += 1
                    if state.overflow_count >= self._overflow_disconnect_threshold:
                        state.disconnected = True
                        state.disconnect_reason = "slow_subscriber"
                        state.queue.clear()
                        state.condition.notify_all()
                        return
            if len(state.queue) >= self._hard_limit:
                state.disconnected = True
                state.disconnect_reason = "slow_subscriber"
                state.queue.clear()
                state.condition.notify_all()
                return
            state.queue.append(event)
            state.condition.notify()

    @staticmethod
    def _drop_disposable(queue: deque[StreamEvent]) -> bool:
        for index, pending in enumerate(queue):
            if pending.event in {"snapshot", "heartbeat"}:
                del queue[index]
                return True
        return False


class MarketAlertEngine:
    """Evaluate the fixed v1 rules and maintain persistent alert cycles."""

    def __init__(self, *, rule_version: int = DEFAULT_RULE_VERSION) -> None:
        if rule_version != DEFAULT_RULE_VERSION:
            raise ValueError("unsupported market radar rule version")
        self.rule_version = rule_version

    @property
    def default_rules(self) -> tuple[RuleDefinition, ...]:
        return DEFAULT_RULE_DEFINITIONS

    async def load_rules(self, store: MarketRadarStore) -> tuple[RuleDefinition, ...]:
        result = await store.session.execute(
            select(MarketAlertRule).where(MarketAlertRule.version == self.rule_version)
        )
        loaded: list[RuleDefinition] = []
        for row in result.scalars():
            parameters = load_json_object(row.parameters_json)
            base = _DEFAULT_RULES_BY_KEY.get(row.rule_key)
            loaded.append(
                RuleDefinition(
                    key=row.rule_key,
                    scope=cast(RadarScope, row.scope),
                    severity=cast(Severity, row.severity),
                    title=base.title if base is not None else row.rule_key,
                    direction=str(
                        parameters.get("direction")
                        or (base.direction if base is not None else "either")
                    ),
                    rule_type=row.rule_type,
                    parameters=parameters,
                    subject=row.subject,
                    enabled=row.enabled,
                    source=cast(Literal["system", "user"], row.source),
                    cooldown_seconds=row.cooldown_seconds,
                    version=row.version,
                )
            )
        return tuple(loaded)

    def evaluate(
        self,
        snapshot: RadarSnapshotEnvelope,
        *,
        rules: Iterable[RuleDefinition] = (),
    ) -> RuleEvaluation:
        supplied = tuple(rules)
        matches: list[RuleMatch] = []
        evaluated: set[str] = set()
        evaluated_rules: dict[str, RuleDefinition] = {}
        skipped: list[str] = []
        overrides = {rule.key: rule for rule in supplied if rule.source == "system"}
        for item in snapshot.observations:
            self._evaluate_observation(
                snapshot,
                item,
                matches=matches,
                evaluated=evaluated,
                rules=evaluated_rules,
                skipped=skipped,
                overrides=overrides,
            )
            for custom in supplied:
                if custom.source == "user" and custom.enabled:
                    self._evaluate_typed_rule(
                        snapshot,
                        item,
                        custom,
                        matches=matches,
                        evaluated=evaluated,
                        rules=evaluated_rules,
                        skipped=skipped,
                    )
        matches.sort(key=lambda item: (-_SEVERITY_RANK[item.severity], item.dedupe_key))
        return RuleEvaluation(
            tuple(matches),
            frozenset(evaluated),
            evaluated_rules,
            tuple(skipped),
        )

    def _evaluate_observation(
        self,
        snapshot: RadarSnapshotEnvelope,
        item: RadarObservation,
        *,
        matches: list[RuleMatch],
        evaluated: set[str],
        rules: dict[str, RuleDefinition],
        skipped: list[str],
        overrides: Mapping[str, RuleDefinition],
    ) -> None:
        def check(
            rule: RuleDefinition,
            required: tuple[str, ...],
            condition: Callable[[Mapping[str, float | bool]], bool],
            *,
            severity: Severity | Callable[[Mapping[str, float | bool]], Severity] | None = None,
            threshold: Any,
            primary: str,
        ) -> None:
            override = overrides.get(rule.key)
            if override is not None and not override.enabled:
                return
            if override is not None:
                rule = replace(
                    rule,
                    severity=override.severity,
                    cooldown_seconds=override.cooldown_seconds,
                    enabled=True,
                )
            if rule.scope != item.scope:
                return
            values: dict[str, float | bool] = {}
            for key in required:
                current = item.metrics.get(key)
                if current is None or current.status != "fresh" or current.value is None:
                    skipped.append(f"{rule.key}:{item.subject}:{key}_not_fresh")
                    return
                values[key] = current.value
            dedupe_key = _dedupe_key(rule, item.subject)
            evaluated.add(dedupe_key)
            rules[dedupe_key] = rule
            if not condition(values):
                return
            actual_severity = (
                severity(values) if callable(severity) else (severity or rule.severity)
            )
            metric_value = item.metrics[primary]
            explanation = _explanation(rule.key, item.subject, values, threshold)
            evidence = {
                "metric": primary,
                "value": metric_value.value,
                "threshold": threshold,
                "baseline": metric_value.baseline,
                "components": {
                    name: {
                        "value": item.metrics[name].value,
                        "baseline": item.metrics[name].baseline,
                        "source": item.metrics[name].source,
                        "source_time": item.metrics[name].as_of.isoformat(),
                    }
                    for name in required
                },
                "source_time": metric_value.as_of.isoformat(),
                "source": metric_value.source,
                "rule_version": self.rule_version,
                "formula_version": snapshot.formula_version,
                "explanation": explanation,
            }
            matches.append(
                RuleMatch(
                    rule=rule,
                    scope=item.scope,
                    subject=item.subject,
                    direction=rule.direction,
                    severity=actual_severity,
                    title=rule.title,
                    explanation=explanation,
                    dedupe_key=dedupe_key,
                    evidence=evidence,
                )
            )

        if item.scope == "market" and item.subject == "ALL":
            check(
                _rule("market_median_return_down", "market", "high", "全A中位数急跌"),
                ("median_return_pct",),
                lambda x: float(x["median_return_pct"]) <= -2.5,
                threshold={"lte": -2.5},
                primary="median_return_pct",
            )
            check(
                _rule("market_decline_ratio_high", "market", "high", "市场普跌"),
                ("decline_ratio",),
                lambda x: float(x["decline_ratio"]) >= 0.8,
                threshold={"gte": 0.8},
                primary="decline_ratio",
            )
            check(
                _rule("market_limit_down_expansion", "market", "high", "跌停家数扩张"),
                ("limit_down_count", "limit_down_median_5d"),
                lambda x: (
                    float(x["limit_down_count"]) >= 30
                    and float(x["limit_down_count"]) >= 2 * float(x["limit_down_median_5d"])
                ),
                threshold={"limit_down_count_gte": 30, "multiple_gte": 2},
                primary="limit_down_count",
            )
            check(
                _rule("market_crowding_weakness", "market", "high", "高拥挤下市场转弱"),
                ("crowding_score", "median_return_pct"),
                lambda x: float(x["crowding_score"]) >= 80 and float(x["median_return_pct"]) <= -1,
                threshold={"crowding_gte": 80, "median_return_lte": -1},
                primary="crowding_score",
            )
            check(
                _rule("market_emotion_cross_down", "market", "medium", "情绪温度跌破恐慌线"),
                ("emotion_score", "previous_emotion_score"),
                lambda x: (
                    float(x["previous_emotion_score"]) > 30 and float(x["emotion_score"]) <= 30
                ),
                threshold={"cross_down": 30},
                primary="emotion_score",
            )
            check(
                _rule("market_emotion_cross_up", "market", "medium", "情绪温度升破极端线"),
                ("emotion_score", "previous_emotion_score"),
                lambda x: (
                    float(x["previous_emotion_score"]) < 85 and float(x["emotion_score"]) >= 85
                ),
                threshold={"cross_up": 85},
                primary="emotion_score",
            )
        elif item.scope == "market" and item.subject in CORE_INDICES:
            check(
                _rule("core_index_return_down", "market", "high", "核心指数急跌"),
                ("return_pct",),
                lambda x: float(x["return_pct"]) <= -2,
                threshold={"lte": -2},
                primary="return_pct",
            )
            check(
                _rule("core_index_5m_down", "market", "high", "核心指数五分钟急跌"),
                ("return_5m_pct",),
                lambda x: float(x["return_5m_pct"]) <= -1,
                threshold={"lte": -1},
                primary="return_5m_pct",
            )
        elif item.scope == "sector":
            check(
                _rule("sector_breadth_down", "sector", "medium", "行业普跌"),
                ("median_return_pct", "decline_ratio"),
                lambda x: float(x["median_return_pct"]) <= -2 and float(x["decline_ratio"]) >= 0.8,
                threshold={"median_return_lte": -2, "decline_ratio_gte": 0.8},
                primary="median_return_pct",
            )
            check(
                _rule("sector_share_crowding", "sector", "medium", "行业成交拥挤"),
                ("amount_share_z20", "crowding_score"),
                lambda x: float(x["amount_share_z20"]) >= 2.5 and float(x["crowding_score"]) >= 80,
                threshold={"share_z20_gte": 2.5, "crowding_gte": 80},
                primary="amount_share_z20",
            )
        elif item.scope == "symbol":
            holding = any(source in {"qmt_holding", "qmt_holding_stale"} for source in item.sources)
            watchlist = "watchlist" in item.sources
            if holding:
                check(
                    _rule("holding_return_down", "symbol", "medium", "持仓股下跌"),
                    ("return_pct",),
                    lambda x: float(x["return_pct"]) <= -3,
                    severity=lambda x: "high" if float(x["return_pct"]) <= -5 else "medium",
                    threshold={"medium_lte": -3, "high_lte": -5},
                    primary="return_pct",
                )
                check(
                    _rule("holding_intraday_drawdown", "symbol", "medium", "持仓股日内回撤"),
                    ("drawdown_pct",),
                    lambda x: float(x["drawdown_pct"]) >= 3,
                    severity=lambda x: "high" if float(x["drawdown_pct"]) >= 5 else "medium",
                    threshold={"medium_gte": 3, "high_gte": 5},
                    primary="drawdown_pct",
                )
                check(
                    _rule("holding_volume_price_anomaly", "symbol", "medium", "持仓股量价异常"),
                    ("volume_ratio_20d", "return_pct"),
                    lambda x: (
                        float(x["volume_ratio_20d"]) >= 2.5 and abs(float(x["return_pct"])) >= 2
                    ),
                    threshold={"volume_ratio_gte": 2.5, "abs_return_gte": 2},
                    primary="volume_ratio_20d",
                )
                check(
                    _rule("holding_near_limit_down", "symbol", "high", "持仓股接近跌停"),
                    ("down_limit_distance_pct",),
                    lambda x: float(x["down_limit_distance_pct"]) <= 0.5,
                    threshold={"lte": 0.5},
                    primary="down_limit_distance_pct",
                )
                check(
                    _rule("holding_limit_up_broken", "symbol", "high", "持仓股涨停开板"),
                    ("limit_up_broken",),
                    lambda x: bool(x["limit_up_broken"]),
                    threshold={"eq": True},
                    primary="limit_up_broken",
                )
            if watchlist:
                check(
                    _rule("watchlist_return_down", "symbol", "high", "自选股大跌"),
                    ("return_pct",),
                    lambda x: float(x["return_pct"]) <= -7,
                    threshold={"lte": -7},
                    primary="return_pct",
                )
                check(
                    _rule("watchlist_intraday_drawdown", "symbol", "high", "自选股日内大幅回撤"),
                    ("drawdown_pct",),
                    lambda x: float(x["drawdown_pct"]) >= 5,
                    threshold={"gte": 5},
                    primary="drawdown_pct",
                )
                check(
                    _rule("watchlist_near_limit_down", "symbol", "high", "自选股接近跌停"),
                    ("down_limit_distance_pct",),
                    lambda x: float(x["down_limit_distance_pct"]) <= 0.5,
                    threshold={"lte": 0.5},
                    primary="down_limit_distance_pct",
                )
            check(
                _rule("symbol_negative_sentiment_heat", "symbol", "medium", "个股负面舆情升温"),
                ("negative_heat_z20", "weighted_sentiment"),
                lambda x: (
                    float(x["negative_heat_z20"]) >= 2 and float(x["weighted_sentiment"]) <= -0.35
                ),
                threshold={"negative_heat_z20_gte": 2, "weighted_sentiment_lte": -0.35},
                primary="negative_heat_z20",
            )

    def _evaluate_typed_rule(
        self,
        snapshot: RadarSnapshotEnvelope,
        item: RadarObservation,
        rule: RuleDefinition,
        *,
        matches: list[RuleMatch],
        evaluated: set[str],
        rules: dict[str, RuleDefinition],
        skipped: list[str],
    ) -> None:
        if rule.rule_type != "metric_threshold" or rule.scope != item.scope:
            return
        if rule.subject not in {"*", item.subject}:
            return
        metric_name = cast(str, rule.parameters["metric"])
        current = item.metrics.get(metric_name)
        if current is None or current.status != "fresh" or current.value is None:
            skipped.append(f"{rule.key}:{item.subject}:{metric_name}_not_fresh")
            return
        dedupe = _dedupe_key(rule, item.subject)
        evaluated.add(dedupe)
        rules[dedupe] = rule
        value = float(current.value)
        threshold = float(rule.parameters["threshold"])
        operator = rule.parameters["operator"]
        hit = (
            (operator == "lte" and value <= threshold)
            or (operator == "gte" and value >= threshold)
            or (operator == "abs_gte" and abs(value) >= threshold)
        )
        if not hit:
            return
        explanation = _explanation(rule.key, item.subject, {metric_name: value}, rule.parameters)
        matches.append(
            RuleMatch(
                rule=rule,
                scope=item.scope,
                subject=item.subject,
                direction=rule.direction,
                severity=rule.severity,
                title=rule.title,
                explanation=explanation,
                dedupe_key=dedupe,
                evidence={
                    "metric": metric_name,
                    "value": value,
                    "threshold": dict(rule.parameters),
                    "baseline": current.baseline,
                    "source_time": current.as_of.isoformat(),
                    "source": current.source,
                    "rule_version": rule.version,
                    "formula_version": snapshot.formula_version,
                    "explanation": explanation,
                },
            )
        )

    async def persist(
        self,
        store: MarketRadarStore,
        evaluation: RuleEvaluation,
        *,
        seen_at: datetime,
        snapshot_id: int | None = None,
    ) -> AlertPersistenceResult:
        matches_by_key = {item.dedupe_key: item for item in evaluation.matches}
        all_keys = set(evaluation.evaluated_dedupe_keys)
        existing_by_key: dict[str, MarketAlertEvent] = {}
        if all_keys:
            result = await store.session.execute(
                select(MarketAlertEvent).where(
                    MarketAlertEvent.dedupe_key.in_(all_keys),
                    MarketAlertEvent.status.in_(_OPEN_EVENT_STATUSES),
                )
            )
            existing_by_key = {event.dedupe_key: event for event in result.scalars()}

        definitions_by_key = {rule.key: rule for rule in self.default_rules}
        definitions_by_key.update((rule.key, rule) for rule in evaluation.evaluated_rules.values())
        existing_rule_result = await store.session.execute(
            select(MarketAlertRule).where(
                MarketAlertRule.version == self.rule_version,
                MarketAlertRule.rule_key.in_(definitions_by_key),
            )
        )
        existing_rules = {row.rule_key: row for row in existing_rule_result.scalars()}
        rule_ids: dict[str, int] = {key: row.id for key, row in existing_rules.items()}
        new_rules: list[MarketAlertRule] = []
        for key, definition in definitions_by_key.items():
            if key in existing_rules:
                continue
            parameters = dict(definition.parameters)
            if definition.source == "user":
                parameters.setdefault("direction", definition.direction)
            stored_rule = MarketAlertRule(
                rule_key=definition.key,
                version=definition.version,
                scope=definition.scope,
                subject=definition.subject,
                rule_type=definition.rule_type,
                parameters_json=dump_json_object(parameters, field_name="parameters_json"),
                severity=definition.severity,
                cooldown_seconds=definition.cooldown_seconds,
                enabled=definition.enabled,
                source=definition.source,
            )
            new_rules.append(stored_rule)
        if new_rules:
            store.session.add_all(new_rules)
            await store.session.flush()
            rule_ids.update({row.rule_key: row.id for row in new_rules})

        notifications: list[MarketAlertEvent] = []
        touched: list[int] = []
        for match in evaluation.matches:
            rule_id = rule_ids[match.rule.key]

            previous = existing_by_key.get(match.dedupe_key)
            should_notify = _should_notify(match, previous, seen_at)
            event, _created = await store.record_event_hit(
                rule_id=rule_id,
                snapshot_id=snapshot_id,
                scope=match.scope,
                subject=match.subject,
                direction=match.direction,
                severity=match.severity,
                title=match.title,
                explanation=match.explanation,
                dedupe_key=match.dedupe_key,
                evidence=match.evidence,
                seen_at=seen_at,
                last_notified_at=seen_at if should_notify else None,
            )
            touched.append(event.id)
            if should_notify:
                notifications.append(event)

        resolved: list[int] = []
        clear_keys = all_keys - set(matches_by_key)
        for dedupe_key in sorted(clear_keys):
            event = existing_by_key.get(dedupe_key)
            if event is None:
                continue
            event.clear_streak += 1
            touched.append(event.id)
            if event.clear_streak >= 2:
                await store.resolve_event(event.id, at=seen_at)
                resolved.append(event.id)
        await store.session.flush()
        return AlertPersistenceResult(
            notifications=tuple(notifications),
            touched_event_ids=tuple(dict.fromkeys(touched)),
            resolved_event_ids=tuple(resolved),
        )


@dataclass(frozen=True, slots=True)
class FocusUniverse:
    holdings: tuple[str, ...]
    watchlist: tuple[str, ...]
    focus: tuple[str, ...]
    symbols: tuple[str, ...]
    sources: Mapping[str, tuple[str, ...]]
    warnings: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "holdings": list(self.holdings),
            "watchlist": list(self.watchlist),
            "focus": list(self.focus),
            "symbols": list(self.symbols),
            "sources": {key: list(value) for key, value in self.sources.items()},
            "warnings": list(self.warnings),
        }


class FocusUniverseResolver:
    def __init__(
        self,
        session: AsyncSession,
        *,
        holding_symbols_loader: Callable[[], Awaitable[Iterable[str]]] | None = None,
        focus_loader: Callable[[], Awaitable[object]] | None = None,
    ) -> None:
        self._session = session
        self._holding_symbols_loader = holding_symbols_loader
        self._focus_loader = focus_loader

    async def resolve(self) -> FocusUniverse:
        watchlist_result = await self._session.execute(
            select(WatchlistStock.symbol).order_by(WatchlistStock.symbol)
        )
        watchlist = _symbols(watchlist_result.scalars())
        warnings: list[str] = []
        holdings: tuple[str, ...] = ()
        if self._holding_symbols_loader is not None:
            try:
                holdings = _symbols(await self._holding_symbols_loader())
            except Exception:
                warnings.append("holdings_unavailable")

        try:
            resolved_focus = await self._load_focus()
            targets = tuple(getattr(resolved_focus, "targets", ()) or ())
            focus = _symbols(getattr(target, "symbol", "") for target in targets)
            warning_code = getattr(resolved_focus, "warning_code", None)
            if isinstance(warning_code, str) and warning_code:
                warnings.append(warning_code)
            if self._holding_symbols_loader is None:
                holdings = _symbols(
                    getattr(target, "symbol", "")
                    for target in targets
                    if any(
                        source in {"qmt_holding", "qmt_holding_stale"}
                        for source in tuple(getattr(target, "sources", ()) or ())
                    )
                )
        except Exception:
            focus = ()
            warnings.append("sentiment_focus_unavailable")
            if self._holding_symbols_loader is None:
                warnings.append("holdings_unavailable")
            targets = ()

        source_sets: dict[str, list[str]] = {}
        for symbol in holdings:
            source_sets.setdefault(symbol, []).append("qmt_holding")
        for target in targets:
            symbol = _symbol(getattr(target, "symbol", ""))
            if symbol is None:
                continue
            for source in tuple(getattr(target, "sources", ()) or ()):
                if (
                    isinstance(source, str)
                    and source
                    and source not in source_sets.setdefault(symbol, [])
                ):
                    source_sets[symbol].append(source)
        for symbol in watchlist:
            if "watchlist" not in source_sets.setdefault(symbol, []):
                source_sets[symbol].append("watchlist")

        symbols = tuple(sorted(set(holdings) | set(watchlist) | set(focus)))
        return FocusUniverse(
            holdings=holdings,
            watchlist=watchlist,
            focus=focus,
            symbols=symbols,
            sources=MappingProxyType(
                {symbol: tuple(source_sets.get(symbol, ())) for symbol in symbols}
            ),
            warnings=tuple(dict.fromkeys(warnings)),
        )

    async def _load_focus(self) -> object:
        if self._focus_loader is not None:
            return await self._focus_loader()
        from app.services.sentiment_focus_pool import SentimentFocusPoolResolver

        return await SentimentFocusPoolResolver(self._session).resolve()


class MarketRadarService:
    def __init__(
        self,
        *,
        feed: object,
        data_service: object,
        store: MarketRadarStore,
        alert_engine: MarketAlertEngine | None = None,
        broker: MarketRadarStreamBroker | None = None,
        snapshot_builder: Callable[..., object] | None = None,
        eod_snapshot_builder: Callable[..., object] | None = None,
        focus_resolver: FocusUniverseResolver | None = None,
        clock: Callable[[], datetime] = datetime.now,
        intraday_coalesce_seconds: float = 1.0,
        snapshot_persist_seconds: float = 30.0,
    ) -> None:
        if intraday_coalesce_seconds <= 0 or snapshot_persist_seconds <= 0:
            raise ValueError("radar service intervals must be positive")
        self.feed = feed
        self.data_service = data_service
        self.store = store
        self.alert_engine = alert_engine or MarketAlertEngine()
        self.broker = broker or MarketRadarStreamBroker(clock=clock)
        self._snapshot_builder = snapshot_builder or self._build_intraday_snapshot
        self._eod_snapshot_builder = eod_snapshot_builder or self._build_eod_snapshot
        self._focus_resolver = focus_resolver
        self._clock = clock
        self._intraday_coalesce_seconds = intraday_coalesce_seconds
        self._snapshot_persist_seconds = snapshot_persist_seconds
        self._refresh_lock = asyncio.Lock()
        self._current: RadarSnapshotEnvelope | None = None
        self._last_refresh_at: datetime | None = None
        self._last_snapshot_persisted_at: datetime | None = None
        self._last_mode: str | None = None
        self._last_heartbeat_at = clock()
        self._focus_cache: FocusUniverse | None = None
        self._focus_cached_at: datetime | None = None
        self._focus_cache_seconds = 30.0
        self._loop_task: asyncio.Task[None] | None = None
        self._started = False
        self._last_loop_error: str | None = None

    @property
    def started(self) -> bool:
        return self._started

    @property
    def last_loop_error(self) -> str | None:
        return self._last_loop_error

    def current_envelope(self) -> RadarSnapshotEnvelope | None:
        return self._current

    async def start(self) -> None:
        if self._started:
            return
        start = getattr(self.feed, "start", None)
        if callable(start):
            await _await_result(start())
        try:
            await self.alert_engine.persist(
                self.store,
                RuleEvaluation((), frozenset(), {}),
                seen_at=self._clock(),
            )
            await self.store.session.commit()
        except Exception:
            await self.store.session.rollback()
            stop = getattr(self.feed, "stop", None)
            if callable(stop):
                await _await_result(stop())
            raise
        self._started = True
        self._loop_task = asyncio.create_task(self._run_loop(), name="market-radar-loop")

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        task = self._loop_task
        self._loop_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        stop = getattr(self.feed, "stop", None)
        if callable(stop):
            await _await_result(stop())

    async def subscribe_with_initial(self) -> MarketRadarSubscription:
        async with self._refresh_lock:
            initial: list[tuple[StreamEventType, Mapping[str, Any]]] = []
            status = self.feed.status
            if callable(status):
                status = status()
            initial.append(("mode", _feed_status_dict(status)))
            if self._current is not None:
                initial.append(("snapshot", self._current.as_dict()))
            result = await self.store.session.execute(
                select(MarketAlertEvent)
                .where(
                    MarketAlertEvent.status == "active",
                    MarketAlertEvent.severity == "high",
                )
                .order_by(MarketAlertEvent.triggered_at, MarketAlertEvent.id)
            )
            initial.extend(("alert", _event_dict(event)) for event in result.scalars())
            return await self.broker.subscribe(initial_events=initial)

    async def refresh_intraday(self) -> RadarSnapshotEnvelope:
        async with self._refresh_lock:
            now = self._clock()
            if (
                self._current is not None
                and self._last_refresh_at is not None
                and (now - self._last_refresh_at).total_seconds() < self._intraday_coalesce_seconds
            ):
                return self._current

            await _await_result(self.feed.run_health_cycle())
            feed_status = self.feed.status
            if callable(feed_status):
                feed_status = feed_status()
            ticks = dict(self.feed.latest_ticks())
            if feed_status.mode == "closed" or (feed_status.mode == "offline" and not ticks):
                snapshot = self._current or await self._load_latest_snapshot()
                if self._last_mode != feed_status.mode:
                    await self.broker.publish(
                        "mode", _feed_status_dict(feed_status), created_at=now
                    )
                    self._last_mode = feed_status.mode
                self._last_refresh_at = now
                if snapshot is None:
                    raise RadarSnapshotUnavailable(
                        "no persisted market radar snapshot is available"
                    )
                self._current = snapshot
                if (now - self._last_heartbeat_at).total_seconds() >= 15:
                    await self.broker.heartbeat(now)
                    self._last_heartbeat_at = now
                return snapshot
            focus = await self._resolve_focus(now)
            snapshot = cast(
                RadarSnapshotEnvelope,
                await _await_result(
                    self._snapshot_builder(
                        ticks=ticks,
                        feed_status=feed_status,
                        focus=focus,
                        now=now,
                    )
                ),
            )
            _validate_snapshot(snapshot)
            configured_rules = await self.alert_engine.load_rules(self.store)
            evaluation = self.alert_engine.evaluate(snapshot, rules=configured_rules)
            mode_changed = self._last_mode != feed_status.mode
            due_snapshot = (
                self._last_snapshot_persisted_at is None
                or (now - self._last_snapshot_persisted_at).total_seconds()
                >= self._snapshot_persist_seconds
            )
            persisted_snapshot = None
            try:
                if due_snapshot:
                    persisted_snapshot = await self._persist_snapshot(snapshot)
                persistence = await self.alert_engine.persist(
                    self.store,
                    evaluation,
                    seen_at=now,
                    snapshot_id=persisted_snapshot.id if persisted_snapshot is not None else None,
                )
                await self.store.session.commit()
            except Exception:
                await self.store.session.rollback()
                raise

            if due_snapshot:
                self._last_snapshot_persisted_at = now
            self._current = snapshot
            self._last_refresh_at = now
            if mode_changed:
                await self.broker.publish("mode", _feed_status_dict(feed_status), created_at=now)
                self._last_mode = feed_status.mode
            await self.broker.publish("snapshot", snapshot.as_dict(), created_at=now)
            for event in persistence.notifications:
                await self.broker.publish("alert", _event_dict(event), created_at=now)
            if (now - self._last_heartbeat_at).total_seconds() >= 15:
                await self.broker.heartbeat(now)
                self._last_heartbeat_at = now
            return snapshot

    async def run_once(self) -> RadarSnapshotEnvelope:
        return await self.refresh_intraday()

    async def refresh_eod(self, target_date: date) -> RadarSnapshotEnvelope:
        now = self._clock()
        sentiment_as_of = datetime.combine(target_date, time(15, 20))
        # MarketRadarDataService owns one AsyncSession, so its SQL calls stay sequential.
        daily = await self.data_service.load_daily_market(target_date=target_date, days=120)
        limit = await self.data_service.load_limit_ladder(target_date=target_date)
        crowding = await self.data_service.load_crowding_inputs(target_date=target_date)
        sectors = await self.data_service.load_sector_inputs(target_date=target_date)
        sentiment = await self.data_service.load_sentiment_inputs(as_of=sentiment_as_of, mode="eod")
        snapshot = cast(
            RadarSnapshotEnvelope,
            await _await_result(
                self._eod_snapshot_builder(
                    target_date=target_date,
                    daily=daily,
                    limit=limit,
                    crowding=crowding,
                    sectors=sectors,
                    sentiment=sentiment,
                    now=now,
                )
            ),
        )
        _validate_snapshot(snapshot)
        configured_rules = await self.alert_engine.load_rules(self.store)
        evaluation = self.alert_engine.evaluate(snapshot, rules=configured_rules)
        try:
            persisted = await self._persist_snapshot(snapshot)
            persistence = await self.alert_engine.persist(
                self.store,
                evaluation,
                seen_at=now,
                snapshot_id=persisted.id,
            )
            await self.store.session.commit()
        except Exception:
            await self.store.session.rollback()
            raise
        self._current = snapshot
        self._last_snapshot_persisted_at = now
        await self.broker.publish("snapshot", snapshot.as_dict(), created_at=now)
        for event in persistence.notifications:
            await self.broker.publish("alert", _event_dict(event), created_at=now)
        return snapshot

    async def _persist_snapshot(self, snapshot: RadarSnapshotEnvelope) -> object:
        return await self.store.upsert_snapshot(
            snapshot_type=snapshot.snapshot_type,
            as_of=snapshot.as_of,
            computed_at=snapshot.computed_at,
            status=snapshot.status,
            confidence=snapshot.confidence,
            formula_version=snapshot.formula_version,
            metrics=cast(Mapping[str, Any], serialize_market_radar_data(snapshot.metrics)),
            source_freshness=cast(
                Mapping[str, Any],
                serialize_market_radar_data(snapshot.source_freshness),
            ),
        )

    async def _run_loop(self) -> None:
        while True:
            try:
                await self.refresh_intraday()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_loop_error = f"{type(exc).__name__}: market radar refresh failed"
            await asyncio.sleep(self._intraday_coalesce_seconds)

    async def _resolve_focus(self, now: datetime) -> FocusUniverse:
        if self._focus_resolver is None:
            return FocusUniverse((), (), (), (), MappingProxyType({}), ())
        if (
            self._focus_cache is not None
            and self._focus_cached_at is not None
            and (now - self._focus_cached_at).total_seconds() < self._focus_cache_seconds
        ):
            return self._focus_cache
        resolved = await self._focus_resolver.resolve()
        self._focus_cache = resolved
        self._focus_cached_at = now
        return resolved

    async def _load_latest_snapshot(self) -> RadarSnapshotEnvelope | None:
        row = await self.store.get_latest_snapshot()
        if row is None:
            return None
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

    def _build_intraday_snapshot(
        self,
        *,
        ticks: Mapping[str, QuoteTick],
        feed_status: object,
        focus: FocusUniverse,
        now: datetime,
    ) -> RadarSnapshotEnvelope:
        equity_ticks = {
            symbol: tick for symbol, tick in ticks.items() if symbol not in CORE_INDICES
        }
        breadth = calculate_breadth(equity_ticks, equity_ticks, now=now)
        freshness: FreshnessStatus = cast(FreshnessStatus, breadth.status)
        returns = [
            (tick.last_price / tick.previous_close - 1) * 100
            for tick in equity_ticks.values()
            if tick.last_price > 0
            and tick.previous_close > 0
            and 0 <= (now - tick.quote_time).total_seconds() <= 5
            and tick.stock_status != 1
        ]
        market_metrics = {
            "median_return_pct": MetricValue(
                median(returns) if returns else None,
                freshness,
                now,
                "qmt_realtime",
            ),
            "decline_ratio": MetricValue(
                sum(value < 0 for value in returns) / len(returns) if returns else None,
                freshness,
                now,
                "qmt_realtime",
            ),
        }
        observations: list[RadarObservation] = [RadarObservation("market", "ALL", market_metrics)]
        indices: dict[str, Any] = {}
        for symbol in sorted(CORE_INDICES):
            tick = ticks.get(symbol)
            status: FreshnessStatus = "fresh" if tick is not None else "unavailable"
            index_metrics = {
                "return_pct": MetricValue(
                    (tick.last_price / tick.previous_close - 1) * 100
                    if tick is not None and tick.previous_close > 0
                    else None,
                    status,
                    tick.quote_time if tick is not None else now,
                    "qmt_realtime",
                ),
                "return_5m_pct": MetricValue(
                    tick.speed_5m if tick is not None else None,
                    status if tick is not None and tick.speed_5m is not None else "unavailable",
                    tick.quote_time if tick is not None else now,
                    "qmt_realtime",
                ),
            }
            observations.append(RadarObservation("market", symbol, index_metrics))
            indices[symbol] = {
                key: {
                    "value": value.value,
                    "status": value.status,
                    "as_of": value.as_of.isoformat(),
                }
                for key, value in index_metrics.items()
            }
        for symbol in focus.symbols:
            tick = ticks.get(symbol)
            status = "fresh" if tick is not None else "unavailable"
            observations.append(
                RadarObservation(
                    "symbol",
                    symbol,
                    {
                        "return_pct": MetricValue(
                            (tick.last_price / tick.previous_close - 1) * 100
                            if tick is not None and tick.previous_close > 0
                            else None,
                            status,
                            tick.quote_time if tick is not None else now,
                            "qmt_realtime",
                        ),
                        "drawdown_pct": MetricValue(
                            (tick.high_price - tick.last_price) / tick.high_price * 100
                            if tick is not None and tick.high_price and tick.high_price > 0
                            else None,
                            status if tick is not None and tick.high_price else "unavailable",
                            tick.quote_time if tick is not None else now,
                            "qmt_realtime",
                        ),
                    },
                    sources=focus.sources.get(symbol, ()),
                )
            )
        mode = str(getattr(feed_status, "mode", "offline"))
        metrics = {
            "overview": {
                "mode": mode,
                "market_median_return_pct": market_metrics["median_return_pct"].value,
                "decline_ratio": market_metrics["decline_ratio"].value,
                "status": freshness,
            },
            "breadth": serialize_breadth_result(breadth),
            "indices": indices,
            "limit_ladder": {"status": "unavailable", "reason": "intraday source not loaded"},
            "crowding": {"status": "unavailable", "reason": "daily baseline not loaded"},
            "sectors": {"status": "unavailable", "items": []},
            "sentiment": {"status": "unavailable", "reason": "intraday source not loaded"},
            "focus": focus.as_dict(),
        }
        return RadarSnapshotEnvelope(
            snapshot_type="intraday",
            as_of=now,
            computed_at=now,
            status=freshness,
            confidence=breadth.coverage.coverage,
            formula_version=DEFAULT_FORMULA_VERSION,
            metrics=metrics,
            source_freshness={
                "qmt_realtime": {
                    "status": freshness,
                    "mode": mode,
                    "as_of": (
                        getattr(feed_status, "last_quote_at", None).isoformat()
                        if getattr(feed_status, "last_quote_at", None) is not None
                        else None
                    ),
                    "coverage": dict(getattr(feed_status, "market_coverage", {})),
                    "reason": getattr(feed_status, "reason", None),
                }
            },
            observations=tuple(observations),
        )

    def _build_eod_snapshot(
        self,
        *,
        target_date: date,
        daily: object,
        limit: object,
        crowding: object,
        sectors: object,
        sentiment: object,
        now: datetime,
    ) -> RadarSnapshotEnvelope:
        inputs = {
            "daily": daily,
            "limit_ladder": limit,
            "crowding": crowding,
            "sectors": sectors,
            "sentiment": sentiment,
        }
        statuses = [
            cast(FreshnessStatus, getattr(value, "status", "unavailable"))
            for value in inputs.values()
        ]
        overall = _combined_status(statuses)
        observations: list[RadarObservation] = []
        market_metrics: dict[str, MetricValue] = {}
        slices = tuple(getattr(daily, "slices", ()) or ())
        latest_slice = slices[-1] if slices else None
        if latest_slice is not None:
            market = next(
                (
                    item
                    for item in tuple(getattr(latest_slice, "breakdowns", ()) or ())
                    if str(getattr(item, "key", "")).upper() == "ALL"
                ),
                None,
            )
            if market is not None:
                market_status = cast(FreshnessStatus, getattr(daily, "status", "unavailable"))
                market_metrics.update(
                    {
                        "median_return_pct": MetricValue(
                            getattr(market, "median_return", None),
                            market_status,
                            now,
                            "klines_daily",
                        ),
                        "decline_ratio": MetricValue(
                            getattr(market, "decline", 0) / max(1, getattr(market, "valid", 0)),
                            market_status,
                            now,
                            "klines_daily",
                        ),
                    }
                )
                limit_status = cast(FreshnessStatus, getattr(limit, "status", "unavailable"))
                market_metrics["limit_down_count"] = MetricValue(
                    getattr(limit, "down_count", None), limit_status, now, "tushare_limit_list_d"
                )
        crowding_score = _crowding_score(crowding)
        emotion_score = _emotion_score(
            daily=daily,
            limit=limit,
            crowding=crowding,
            sentiment=sentiment,
        )
        crowding_status: FreshnessStatus = (
            "fresh" if crowding_score.status == "fresh" else "unavailable"
        )
        emotion_status: FreshnessStatus = (
            "fresh" if emotion_score.status == "fresh" else "unavailable"
        )
        market_metrics["crowding_score"] = MetricValue(
            crowding_score.value,
            crowding_status,
            now,
            "market_radar_crowding_v1",
            crowding_score.formula_version,
        )
        market_metrics["emotion_score"] = MetricValue(
            emotion_score.value,
            emotion_status,
            now,
            "market_radar_emotion_v1",
            emotion_score.formula_version,
        )
        previous_emotion = _nested_number(
            self._current.metrics if self._current is not None else {},
            "overview",
            "emotion",
            "value",
        )
        market_metrics["previous_emotion_score"] = MetricValue(
            previous_emotion,
            "fresh" if previous_emotion is not None else "unavailable",
            self._current.as_of if self._current is not None else now,
            "previous_market_radar_snapshot",
        )
        if market_metrics:
            observations.append(RadarObservation("market", "ALL", market_metrics))
        sector_status = cast(FreshnessStatus, getattr(sectors, "status", "unavailable"))
        for item in tuple(getattr(sectors, "sectors", ()) or ()):
            observations.append(
                RadarObservation(
                    "sector",
                    str(item.industry),
                    {
                        "median_return_pct": MetricValue(
                            getattr(item, "median_return", None),
                            sector_status,
                            now,
                            "klines_daily_sector_inputs",
                        ),
                        "decline_ratio": MetricValue(
                            1 - float(getattr(item, "advance_ratio", 0)),
                            sector_status,
                            now,
                            "klines_daily_sector_inputs",
                        ),
                        "amount_share_z20": MetricValue(
                            getattr(item, "share_z20", None),
                            sector_status
                            if getattr(item, "share_z20", None) is not None
                            else "unavailable",
                            now,
                            "klines_daily_sector_inputs",
                        ),
                    },
                )
            )
        serialized = {
            "daily": _compact_daily(daily),
            "limit_ladder": _serialize_contract(limit),
            "crowding": _serialize_contract(crowding),
            "sectors": _serialize_contract(sectors),
            "sentiment": _serialize_contract(sentiment),
        }
        freshness_payload = _collect_eod_freshness(inputs, target_date)
        crowding_payload = cast(dict[str, Any], serialize_market_radar_data(crowding_score))
        emotion_payload = cast(dict[str, Any], serialize_market_radar_data(emotion_score))
        crowding_value = crowding_score.value
        emotion_value = emotion_score.value
        return RadarSnapshotEnvelope(
            snapshot_type="eod",
            as_of=datetime.combine(target_date, time(15, 20)),
            computed_at=now,
            status=overall,
            confidence=sum(_status_confidence(value) for value in statuses) / len(statuses),
            formula_version=DEFAULT_FORMULA_VERSION,
            metrics={
                "overview": {
                    "status": overall,
                    "trade_date": target_date.isoformat(),
                    "market_breadth": {
                        "median_return_pct": (
                            market_metrics.get("median_return_pct").value
                            if "median_return_pct" in market_metrics
                            else None
                        ),
                        "decline_ratio": (
                            market_metrics.get("decline_ratio").value
                            if "decline_ratio" in market_metrics
                            else None
                        ),
                    },
                    "crowding": {
                        **crowding_payload,
                        "label": crowding_label(crowding_value)
                        if crowding_value is not None
                        else None,
                    },
                    "emotion": {
                        **emotion_payload,
                        "label": emotion_label(emotion_value)
                        if emotion_value is not None
                        else None,
                    },
                    "risk_level": _risk_level(market_metrics),
                },
                "breadth": serialized["daily"],
                "indices": {},
                "limit_ladder": serialized["limit_ladder"],
                "crowding": {
                    "status": getattr(crowding, "status", "unavailable"),
                    "score": crowding_payload,
                    "label": crowding_label(crowding_value) if crowding_value is not None else None,
                    "inputs": serialized["crowding"],
                },
                "sectors": serialized["sectors"],
                "sentiment": serialized["sentiment"],
                "focus": {"status": "unavailable", "symbols": []},
            },
            source_freshness=freshness_payload,
            observations=tuple(observations),
        )


def _rule(key: str, scope: RadarScope, severity: Severity, title: str) -> RuleDefinition:
    rule = _DEFAULT_RULES_BY_KEY.get(key)
    if rule is None:
        raise KeyError(f"unknown default market alert rule: {key}")
    return rule


def _dedupe_key(rule: RuleDefinition, subject: str) -> str:
    raw = json.dumps(
        [rule.version, rule.key, rule.scope, subject, rule.direction],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode("ascii")).hexdigest()


def _explanation(
    rule_key: str,
    subject: str,
    values: Mapping[str, float | bool],
    threshold: Any,
) -> str:
    shown = ", ".join(f"{key}={value}" for key, value in values.items())
    return f"{subject} 命中 {rule_key}: {shown}; threshold={threshold}"


def _should_notify(
    match: RuleMatch,
    previous: MarketAlertEvent | None,
    seen_at: datetime,
) -> bool:
    if match.severity == "low":
        return False
    if previous is None:
        return match.severity == "high"
    if _SEVERITY_RANK[match.severity] > _SEVERITY_RANK[cast(Severity, previous.severity)]:
        return True
    if (
        match.severity == "medium"
        and previous.occurrence_count == 1
        and previous.last_notified_at is None
    ):
        return True
    if previous.last_notified_at is None:
        return match.severity == "high"
    elapsed = (seen_at - previous.last_notified_at).total_seconds()
    return elapsed >= match.rule.cooldown_seconds


def _symbol(value: object) -> str | None:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 9 or normalized[6] != ".":
        return None
    code, market = normalized.split(".", 1)
    if not code.isdigit() or market not in {"SH", "SZ", "BJ"}:
        return None
    return normalized


def _symbols(values: Iterable[object]) -> tuple[str, ...]:
    return tuple(sorted({symbol for value in values if (symbol := _symbol(value)) is not None}))


async def _await_result(value: object) -> object:
    if inspect.isawaitable(value):
        return await cast(Awaitable[object], value)
    return value


def _validate_snapshot(snapshot: RadarSnapshotEnvelope) -> None:
    if not isinstance(snapshot, RadarSnapshotEnvelope):
        raise TypeError("snapshot builder must return RadarSnapshotEnvelope")

    def visit(value: object, path: tuple[str, ...]) -> None:
        if isinstance(value, Mapping):
            for raw_key, item in value.items():
                key = str(raw_key).lower()
                if any(term in key for term in _UNSAFE_SNAPSHOT_TERMS):
                    raise ValueError(f"unsafe snapshot field: {'.'.join((*path, str(raw_key)))}")
                visit(item, (*path, str(raw_key)))
        elif isinstance(value, (tuple, list)):
            for index, item in enumerate(value):
                visit(item, (*path, str(index)))

    visit(snapshot.metrics, ("metrics",))
    visit(snapshot.source_freshness, ("source_freshness",))


def _feed_status_dict(status: object) -> dict[str, Any]:
    changed_at = getattr(status, "changed_at", None)
    last_quote_at = getattr(status, "last_quote_at", None)
    return {
        "mode": getattr(status, "mode", "offline"),
        "changed_at": changed_at.isoformat() if isinstance(changed_at, datetime) else None,
        "last_quote_at": last_quote_at.isoformat() if isinstance(last_quote_at, datetime) else None,
        "connection_generation": getattr(status, "connection_generation", 0),
        "reason": getattr(status, "reason", None),
        "market_coverage": dict(getattr(status, "market_coverage", {})),
    }


def _event_dict(event: MarketAlertEvent) -> dict[str, Any]:
    return {
        "id": event.id,
        "scope": event.scope,
        "subject": event.subject,
        "severity": event.severity,
        "status": event.status,
        "title": event.title,
        "explanation": event.explanation,
        "triggered_at": event.triggered_at.isoformat(),
        "last_seen_at": event.last_seen_at.isoformat(),
        "evidence": load_json_object(event.evidence_json, field_name="evidence_json"),
    }


def _status_confidence(status: FreshnessStatus) -> float:
    return {"fresh": 1.0, "partial": 0.65, "stale": 0.25, "unavailable": 0.0}[status]


def _combined_status(statuses: Iterable[FreshnessStatus]) -> FreshnessStatus:
    values = tuple(statuses)
    if values and all(value == "fresh" for value in values):
        return "fresh"
    if not values or all(value == "unavailable" for value in values):
        return "unavailable"
    if all(value == "stale" for value in values):
        return "stale"
    return "partial"


def _crowding_score(crowding: object) -> Any:
    by_key = {
        str(getattr(component, "key", "")): component
        for component in tuple(getattr(crowding, "components", ()) or ())
    }
    components: list[ScoreComponent] = []
    for key, weight in CROWDING_COMPONENT_WEIGHTS.items():
        source = by_key.get(key)
        freshness = getattr(getattr(source, "freshness", None), "status", "unavailable")
        reason = getattr(source, "excluded_reason", None)
        if reason is None and freshness != "fresh":
            reason = f"source_{freshness}"
        components.append(
            ScoreComponent(
                name=key,
                raw_value=getattr(source, "current_value", None),
                history=tuple(getattr(source, "history", ()) or ()),
                weight=weight,
                excluded_reason=reason,
            )
        )
    return composite_score(components, formula_version="market-radar-crowding-v1")


def _emotion_score(
    *,
    daily: object,
    limit: object,
    crowding: object,
    sentiment: object,
) -> Any:
    slices = tuple(getattr(daily, "slices", ()) or ())
    breadth_values: list[float] = []
    for item in slices:
        market = _all_market_breakdown(item)
        value = _finite_or_none(getattr(market, "median_return", None))
        if value is not None:
            breadth_values.append(value)
    breadth_current = breadth_values[-1] if breadth_values else None
    breadth_history = tuple(breadth_values[:-1])

    liquidity = next(
        (
            component
            for component in tuple(getattr(crowding, "components", ()) or ())
            if getattr(component, "key", None) == "market_amount_vs_20d"
        ),
        None,
    )
    sentiment_history = tuple(
        float(value)
        for _trade_day, value in tuple(getattr(sentiment, "daily_history", ()) or ())
        if _finite_or_none(value) is not None
    )
    raw = {
        "market_breadth": ScoreComponent(
            name="market_breadth",
            raw_value=breadth_current,
            history=breadth_history,
            weight=EMOTION_COMPONENT_WEIGHTS["market_breadth"],
            excluded_reason=(
                None if getattr(daily, "status", "unavailable") == "fresh" else "daily_not_fresh"
            ),
        ),
        "limit_ladder": ScoreComponent(
            name="limit_ladder",
            raw_value=_finite_or_none(getattr(limit, "promotion_rate", None)),
            history=(),
            weight=EMOTION_COMPONENT_WEIGHTS["limit_ladder"],
            excluded_reason=(
                None if getattr(limit, "status", "unavailable") == "fresh" else "limit_not_fresh"
            ),
        ),
        "liquidity_risk_appetite": ScoreComponent(
            name="liquidity_risk_appetite",
            raw_value=getattr(liquidity, "current_value", None),
            history=tuple(getattr(liquidity, "history", ()) or ()),
            weight=EMOTION_COMPONENT_WEIGHTS["liquidity_risk_appetite"],
            excluded_reason=(
                getattr(liquidity, "excluded_reason", None)
                if getattr(getattr(liquidity, "freshness", None), "status", "unavailable")
                == "fresh"
                else "liquidity_not_fresh"
            ),
        ),
        "sentiment": ScoreComponent(
            name="sentiment",
            raw_value=_finite_or_none(getattr(sentiment, "weighted_score", None)),
            history=sentiment_history,
            weight=EMOTION_COMPONENT_WEIGHTS["sentiment"],
            excluded_reason=(
                None
                if getattr(sentiment, "status", "unavailable") == "fresh"
                else "sentiment_not_fresh"
            ),
        ),
    }
    return composite_score(raw.values(), formula_version="market-radar-emotion-v1")


def _all_market_breakdown(item: object) -> object | None:
    return next(
        (
            breakdown
            for breakdown in tuple(getattr(item, "breakdowns", ()) or ())
            if str(getattr(breakdown, "key", "")).upper() == "ALL"
        ),
        None,
    )


def _finite_or_none(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if math.isfinite(numeric) else None


def _nested_number(value: Mapping[str, Any], *keys: str) -> float | None:
    current: object = value
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return _finite_or_none(current)


def _risk_level(metrics: Mapping[str, MetricValue]) -> str:
    median_return = metrics.get("median_return_pct")
    decline_ratio = metrics.get("decline_ratio")
    crowding = metrics.get("crowding_score")
    if (
        median_return is not None
        and median_return.status == "fresh"
        and median_return.value is not None
        and float(median_return.value) <= -2.5
    ) or (
        decline_ratio is not None
        and decline_ratio.status == "fresh"
        and decline_ratio.value is not None
        and float(decline_ratio.value) >= 0.8
    ):
        return "high"
    if (
        crowding is not None
        and crowding.status == "fresh"
        and crowding.value is not None
        and float(crowding.value) >= 80
    ):
        return "medium"
    return "normal"


def _compact_daily(daily: object) -> dict[str, Any]:
    days: list[dict[str, Any]] = []
    for item in tuple(getattr(daily, "slices", ()) or ())[-120:]:
        days.append(
            {
                "trade_date": _iso_value(getattr(item, "trade_date", None)),
                "previous_trade_date": _iso_value(getattr(item, "previous_trade_date", None)),
                "breadth": _compact_breadth(getattr(item, "breadth", None)),
                "breakdowns": [
                    _named_attributes(
                        breakdown,
                        (
                            "key",
                            "label",
                            "eligible",
                            "valid",
                            "excluded",
                            "advance",
                            "decline",
                            "flat",
                            "median_return",
                            "amount",
                        ),
                    )
                    for breakdown in tuple(getattr(item, "breakdowns", ()) or ())
                ],
                "exclusion_counts": [
                    list(value) for value in tuple(getattr(item, "exclusion_counts", ()) or ())
                ],
            }
        )
    return {
        "status": getattr(daily, "status", "unavailable"),
        "expected_date": _iso_value(getattr(daily, "expected_date", None)),
        "days": days,
    }


def _compact_breadth(value: object) -> dict[str, Any]:
    if value is None:
        return {"status": "unavailable", "flat_count": 0, "coverage": {}, "buckets": {}}
    coverage = getattr(value, "coverage", None)
    buckets = getattr(value, "buckets", {})
    return {
        "status": getattr(value, "status", "unavailable"),
        "flat_count": int(getattr(value, "flat_count", 0)),
        "coverage": _named_attributes(
            coverage,
            (
                "requested",
                "eligible",
                "valid",
                "excluded",
                "coverage",
                "status",
                "missing",
                "stale",
                "invalid",
                "suspended",
            ),
        ),
        "buckets": {
            str(key): _named_attributes(
                bucket,
                (
                    "key",
                    "label",
                    "lower_bound",
                    "upper_bound",
                    "lower_inclusive",
                    "upper_inclusive",
                    "count",
                    "percentage",
                ),
            )
            for key, bucket in dict(buckets).items()
        },
    }


def _collect_eod_freshness(
    inputs: Mapping[str, object],
    target_date: date,
) -> dict[str, Any]:
    daily = inputs["daily"]
    limit = inputs["limit_ladder"]
    crowding = inputs["crowding"]
    sectors = inputs["sectors"]
    sentiment = inputs["sentiment"]
    payload: dict[str, Any] = {
        "daily": _freshness_payload(
            getattr(daily, "source_freshness", None),
            fallback_status=getattr(daily, "status", "unavailable"),
            fallback_date=target_date,
        ),
        "daily_universe": _freshness_payload(
            getattr(daily, "universe_freshness", None),
            fallback_status="unavailable",
            fallback_date=target_date,
        ),
        "trading_calendar": _freshness_payload(
            getattr(getattr(daily, "calendar", None), "freshness", None),
            fallback_status="unavailable",
            fallback_date=target_date,
        ),
        "limit_detail": _freshness_payload(
            getattr(limit, "detail_freshness", None),
            fallback_status=getattr(limit, "status", "unavailable"),
            fallback_date=target_date,
        ),
        "limit_step": _freshness_payload(
            getattr(limit, "step_freshness", None),
            fallback_status=getattr(limit, "status", "unavailable"),
            fallback_date=target_date,
        ),
        "sectors": _freshness_payload(
            getattr(sectors, "source_freshness", None),
            fallback_status=getattr(sectors, "status", "unavailable"),
            fallback_date=target_date,
        ),
        "sentiment": _freshness_payload(
            getattr(sentiment, "freshness", None),
            fallback_status=getattr(sentiment, "status", "unavailable"),
            fallback_date=target_date,
        ),
    }
    components = tuple(getattr(crowding, "components", ()) or ())
    payload["crowding"] = {
        "status": getattr(crowding, "status", "unavailable"),
        "as_of": _iso_value(getattr(crowding, "as_of", target_date)),
        "components": {
            str(getattr(component, "key", "unknown")): _freshness_payload(
                getattr(component, "freshness", None),
                fallback_status="unavailable",
                fallback_date=target_date,
            )
            for component in components
        },
    }
    return payload


def _freshness_payload(
    value: object,
    *,
    fallback_status: object,
    fallback_date: date,
) -> dict[str, Any]:
    if value is None:
        return {
            "source": None,
            "status": fallback_status,
            "expected_date": fallback_date.isoformat(),
            "source_date": None,
            "lag_trading_days": None,
            "row_count": 0,
            "coverage": None,
            "reason": "source freshness details unavailable",
        }
    return {
        key: _iso_value(getattr(value, key, None))
        for key in (
            "source",
            "status",
            "expected_date",
            "source_date",
            "lag_trading_days",
            "row_count",
            "coverage",
            "reason",
        )
    }


def _named_attributes(value: object, names: Iterable[str]) -> dict[str, Any]:
    if value is None:
        return {}
    return {name: _iso_value(getattr(value, name, None)) for name in names}


def _iso_value(value: object) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _serialize_contract(value: object) -> Any:
    if hasattr(value, "__dict__"):
        return {
            str(key): _serialize_contract(item)
            for key, item in vars(value).items()
            if not str(key).startswith("_")
        }
    if isinstance(value, Mapping):
        return {str(key): _serialize_contract(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_serialize_contract(item) for item in value]
    return serialize_market_radar_data(value)

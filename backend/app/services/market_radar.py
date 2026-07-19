"""Market-radar orchestration, typed alerts, and bounded stream delivery."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import date, datetime, time
from statistics import median
from types import MappingProxyType
from typing import Any, Literal, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.market_radar import MarketAlertEvent, MarketAlertRule, MarketRadarSnapshot
from app.db.models.watchlist import WatchlistStock
from app.services.market_radar_broker import (
    BrokerDisconnected as BrokerDisconnected,
)
from app.services.market_radar_broker import (
    MarketRadarStreamBroker,
    MarketRadarSubscription,
)
from app.services.market_radar_calculator import (
    CROWDING_COMPONENT_WEIGHTS,
    EMOTION_COMPONENT_WEIGHTS,
    QuoteTick,
    ScoreComponent,
    calculate_breadth,
    composite_score,
    crowding_label,
    serialize_breadth_result,
)
from app.services.market_radar_contracts import (
    DEFAULT_COOLDOWN_SECONDS as DEFAULT_COOLDOWN_SECONDS,
)
from app.services.market_radar_contracts import (
    DEFAULT_FORMULA_VERSION,
    DEFAULT_RULE_VERSION,
    AlertPersistenceResult,
    EligibleUniverse,
    FocusUniverse,
    FreshnessStatus,
    IntradaySymbolContext,
    MetricValue,
    RadarHistoryContext,
    RadarObservation,
    RadarScope,
    RadarSnapshotEnvelope,
    RuleDefinition,
    RuleEvaluation,
    RuleMatch,
    Severity,
    SnapshotType,
    StreamEventType,
)
from app.services.market_radar_contracts import (
    StreamEvent as StreamEvent,
)
from app.services.market_radar_data import serialize_market_radar_data
from app.services.market_radar_store import (
    MarketRadarStore,
    dump_json_object,
    load_json_object,
)

CORE_INDICES = frozenset({"000001.SH", "399001.SZ", "000985.SH"})
_OPEN_EVENT_STATUSES = ("active", "acknowledged", "dismissed")
_SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}
_UNSAFE_SNAPSHOT_TERMS = ("tick", "quantity", "cost", "account", "position_size")
_SYMBOL_CONTEXT_KEYS = (
    "volume_ratio_20d",
    "down_limit_price",
    "up_limit_price",
    "negative_heat_z20",
    "weighted_sentiment",
)
_INTRADAY_SOURCE_ORDER = (
    "qmt_realtime",
    "eligible_universe",
    "klines_daily_20d",
    "stock_limit_prices",
    "sentiment_posts",
)
_SOURCE_DEFAULT_REASONS = {
    "qmt_realtime": "QMT realtime quotes are unavailable",
    "eligible_universe": "eligible A-share universe is unavailable",
    "klines_daily_20d": "20-day volume baseline is unavailable",
    "stock_limit_prices": "exact stock limit prices are unavailable",
    "sentiment_posts": "symbol sentiment posts are unavailable",
}
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


class RadarSnapshotUnavailable(RuntimeError):
    """Raised when the market is closed and no prior snapshot exists."""


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
        eligible_universe_loader: Callable[[], Awaitable[EligibleUniverse | Iterable[str]]]
        | None = None,
        symbol_context_loader: Callable[
            [tuple[str, ...], Mapping[str, QuoteTick], datetime],
            Awaitable[Mapping[str, IntradaySymbolContext]],
        ]
        | None = None,
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
        self._uses_default_snapshot_builder = snapshot_builder is None
        self._uses_default_eod_builder = eod_snapshot_builder is None
        self._snapshot_builder = snapshot_builder or self._build_intraday_snapshot
        self._eod_snapshot_builder = eod_snapshot_builder or self._build_eod_snapshot
        self._focus_resolver = focus_resolver
        if eligible_universe_loader is None or symbol_context_loader is None:
            from app.services.market_radar_intraday_context import (
                MarketRadarIntradayContextLoader,
            )

            default_context_loader = MarketRadarIntradayContextLoader(store.session, clock=clock)
            eligible_universe_loader = (
                eligible_universe_loader or default_context_loader.load_eligible_universe
            )
            symbol_context_loader = (
                symbol_context_loader or default_context_loader.load_symbol_context
            )
        self._eligible_universe_loader = eligible_universe_loader
        self._symbol_context_loader = symbol_context_loader
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
        self._eligible_cache: EligibleUniverse | None = None
        self._eligible_cached_at: datetime | None = None
        self._eligible_cache_seconds = 60.0
        self._loop_task: asyncio.Task[None] | None = None
        self._started = False
        self._last_loop_error: str | None = None
        self._last_cleanup_error: str | None = None

    @property
    def started(self) -> bool:
        return self._started

    @property
    def last_loop_error(self) -> str | None:
        return self._last_loop_error

    @property
    def last_cleanup_error(self) -> str | None:
        return self._last_cleanup_error

    def current_envelope(self) -> RadarSnapshotEnvelope | None:
        return self._current

    @staticmethod
    def project_snapshot(snapshot: RadarSnapshotEnvelope | None) -> dict[str, Any]:
        if snapshot is None:
            raise RadarSnapshotUnavailable("no market radar snapshot is available")
        freshness = cast(
            Mapping[str, Any],
            serialize_market_radar_data(snapshot.source_freshness),
        )
        overview = snapshot.metrics.get("overview")
        mode = overview.get("mode") if isinstance(overview, Mapping) else None
        realtime_mode = (
            str(mode)
            if mode in {"push", "polling_30s", "offline", "closed"}
            else ("closed" if snapshot.snapshot_type == "eod" else "offline")
        )
        source_names = list(freshness)
        if snapshot.snapshot_type == "intraday":
            source_names = [
                *_INTRADAY_SOURCE_ORDER,
                *sorted(set(source_names) - set(_INTRADAY_SOURCE_ORDER)),
            ]
        else:
            source_names.sort()
        sources = [
            _project_source(
                name,
                freshness.get(name),
                default_reason=_SOURCE_DEFAULT_REASONS.get(name),
            )
            for name in source_names
        ]
        return {
            "as_of": snapshot.as_of.isoformat(),
            "computed_at": snapshot.computed_at.isoformat(),
            "status": snapshot.status,
            "confidence": snapshot.confidence,
            "realtime_mode": realtime_mode,
            "sources": sources,
            "data": cast(dict[str, Any], serialize_market_radar_data(snapshot.metrics)),
        }

    async def start(self) -> None:
        if self._started:
            return
        start = getattr(self.feed, "start", None)
        if callable(start):
            try:
                await _await_result(start())
            except BaseException:
                await self._cleanup_feed()
                raise
        try:
            await self.alert_engine.persist(
                self.store,
                RuleEvaluation((), frozenset(), {}),
                seen_at=self._clock(),
            )
            await self.store.session.commit()
        except BaseException:
            await self._rollback_session()
            await self._cleanup_feed()
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
            if self._current is None:
                self._current = await self._load_latest_snapshot()
            if self._current is not None:
                initial.append(("snapshot", self.project_snapshot(self._current)))
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
        async with self._locked_refresh():
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
            if feed_status.mode in {"closed", "offline"}:
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
            eligible_universe = await self._resolve_eligible_universe(now)
            symbol_context = await self._resolve_symbol_context(focus.symbols, ticks, now)
            builder_kwargs: dict[str, Any] = {
                "ticks": ticks,
                "feed_status": feed_status,
                "focus": focus,
                "now": now,
            }
            if self._uses_default_snapshot_builder:
                builder_kwargs.update(
                    eligible_universe=eligible_universe,
                    symbol_context=symbol_context,
                )
            snapshot = cast(
                RadarSnapshotEnvelope,
                await _await_result(self._snapshot_builder(**builder_kwargs)),
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
            if due_snapshot:
                persisted_snapshot = await self._persist_snapshot(snapshot)
            persistence = await self.alert_engine.persist(
                self.store,
                evaluation,
                seen_at=now,
                snapshot_id=persisted_snapshot.id if persisted_snapshot is not None else None,
            )
            await self.store.session.commit()

            if due_snapshot:
                self._last_snapshot_persisted_at = now
            self._current = snapshot
            self._last_refresh_at = now
            if mode_changed:
                await self.broker.publish("mode", _feed_status_dict(feed_status), created_at=now)
                self._last_mode = feed_status.mode
            await self.broker.publish("snapshot", self.project_snapshot(snapshot), created_at=now)
            for event in persistence.notifications:
                await self.broker.publish("alert", _event_dict(event), created_at=now)
            if (now - self._last_heartbeat_at).total_seconds() >= 15:
                await self.broker.heartbeat(now)
                self._last_heartbeat_at = now
            return snapshot

    async def run_once(self) -> RadarSnapshotEnvelope:
        return await self.refresh_intraday()

    async def refresh_eod(self, target_date: date) -> RadarSnapshotEnvelope:
        async with self._locked_refresh():
            return await self._refresh_eod_locked(target_date)

    async def _refresh_eod_locked(self, target_date: date) -> RadarSnapshotEnvelope:
        now = self._clock()
        sentiment_as_of = datetime.combine(target_date, time(15, 20))
        # MarketRadarDataService owns one AsyncSession, so its SQL calls stay sequential.
        daily = await self.data_service.load_daily_market(target_date=target_date, days=120)
        limit = await self.data_service.load_limit_ladder(target_date=target_date)
        crowding = await self.data_service.load_crowding_inputs(target_date=target_date)
        sectors = await self.data_service.load_sector_inputs(target_date=target_date)
        sentiment = await self.data_service.load_sentiment_inputs(as_of=sentiment_as_of, mode="eod")
        history = await self._load_history_context(target_date)
        builder_kwargs: dict[str, Any] = {
            "target_date": target_date,
            "daily": daily,
            "limit": limit,
            "crowding": crowding,
            "sectors": sectors,
            "sentiment": sentiment,
            "now": now,
        }
        if self._uses_default_eod_builder:
            builder_kwargs["history"] = history
        snapshot = cast(
            RadarSnapshotEnvelope,
            await _await_result(self._eod_snapshot_builder(**builder_kwargs)),
        )
        _validate_snapshot(snapshot)
        configured_rules = await self.alert_engine.load_rules(self.store)
        evaluation = self.alert_engine.evaluate(snapshot, rules=configured_rules)
        persisted = await self._persist_snapshot(snapshot)
        persistence = await self.alert_engine.persist(
            self.store,
            evaluation,
            seen_at=now,
            snapshot_id=persisted.id,
        )
        await self.store.session.commit()
        self._current = snapshot
        self._last_snapshot_persisted_at = now
        await self.broker.publish("snapshot", self.project_snapshot(snapshot), created_at=now)
        for event in persistence.notifications:
            await self.broker.publish("alert", _event_dict(event), created_at=now)
        return snapshot

    async def _rollback_session(self) -> None:
        try:
            await self.store.session.rollback()
        except BaseException as exc:
            self._last_cleanup_error = f"{type(exc).__name__}: session rollback failed"

    @asynccontextmanager
    async def _locked_refresh(self) -> AsyncIterator[None]:
        async with self._refresh_lock:
            try:
                yield
            except BaseException:
                await self._rollback_session()
                raise

    async def _cleanup_feed(self) -> None:
        stop = getattr(self.feed, "stop", None)
        if not callable(stop):
            return
        try:
            await _await_result(stop())
        except BaseException as exc:
            self._last_cleanup_error = f"{type(exc).__name__}: {exc}"

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
                try:
                    await self.store.session.rollback()
                except Exception as rollback_exc:
                    self._last_loop_error = (
                        f"{self._last_loop_error}; {type(rollback_exc).__name__}: rollback failed"
                    )
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

    async def _resolve_eligible_universe(self, now: datetime) -> EligibleUniverse:
        if (
            self._eligible_cache is not None
            and self._eligible_cached_at is not None
            and (now - self._eligible_cached_at).total_seconds() < self._eligible_cache_seconds
        ):
            return self._eligible_cache
        loader = self._eligible_universe_loader
        if loader is None:
            public_loader = getattr(self.feed, "eligible_symbols", None)
            loader = public_loader if callable(public_loader) else None
        if loader is None:
            resolved = EligibleUniverse(
                (),
                "unavailable",
                now,
                "eligible_universe",
                "eligible universe loader is not configured",
            )
        else:
            try:
                raw = await _await_result(loader())
                if isinstance(raw, EligibleUniverse):
                    resolved = raw
                else:
                    resolved = EligibleUniverse(
                        _symbols(cast(Iterable[object], raw)),
                        "fresh",
                        now,
                        "eligible_universe_loader",
                    )
            except Exception:
                resolved = EligibleUniverse(
                    (),
                    "unavailable",
                    now,
                    "eligible_universe_loader",
                    "eligible universe loading failed",
                )
        self._eligible_cache = resolved
        self._eligible_cached_at = now
        return resolved

    async def _resolve_symbol_context(
        self,
        symbols: tuple[str, ...],
        ticks: Mapping[str, QuoteTick],
        now: datetime,
    ) -> Mapping[str, IntradaySymbolContext]:
        if not symbols or self._symbol_context_loader is None:
            return MappingProxyType({})
        try:
            resolved = await self._symbol_context_loader(symbols, ticks, now)
        except Exception:
            return MappingProxyType(
                {
                    symbol: IntradaySymbolContext(
                        metrics={
                            key: MetricValue(
                                None,
                                "unavailable",
                                now,
                                "symbol_context_loader",
                                reason="symbol enrichment loading failed",
                            )
                            for key in _SYMBOL_CONTEXT_KEYS
                        }
                    )
                    for symbol in symbols
                }
            )
        return MappingProxyType(dict(resolved))

    async def _load_history_context(self, target_date: date) -> RadarHistoryContext:
        target = datetime.combine(target_date, time.min)
        result = await self.store.session.execute(
            select(MarketRadarSnapshot)
            .where(
                MarketRadarSnapshot.snapshot_type == "eod",
                MarketRadarSnapshot.formula_version == DEFAULT_FORMULA_VERSION,
                MarketRadarSnapshot.as_of < target,
            )
            .order_by(MarketRadarSnapshot.as_of.desc(), MarketRadarSnapshot.id.desc())
            .limit(5)
        )
        rows = tuple(result.scalars())
        counts: list[float] = []
        previous_emotion: float | None = None
        previous_as_of: datetime | None = None
        for index, row in enumerate(rows):
            metrics = load_json_object(row.metrics_json, field_name="metrics_json")
            limit_status = _nested_value(metrics, "limit_ladder", "status")
            count = _nested_number(metrics, "limit_ladder", "down_count")
            if limit_status == "fresh" and count is not None:
                counts.append(count)
            if index == 0:
                emotion_status = _nested_value(metrics, "overview", "emotion", "status")
                value = _nested_number(metrics, "overview", "emotion", "value")
                if emotion_status == "fresh" and value is not None:
                    previous_emotion = value
                    previous_as_of = row.as_of
        baseline = float(median(counts)) if len(counts) == 5 else None
        return RadarHistoryContext(
            limit_down_counts=tuple(counts),
            limit_down_median_5d=baseline,
            previous_emotion_score=previous_emotion,
            previous_as_of=previous_as_of,
        )

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
        eligible_universe: EligibleUniverse | None = None,
        symbol_context: Mapping[str, IntradaySymbolContext] | None = None,
        now: datetime,
    ) -> RadarSnapshotEnvelope:
        mode = str(getattr(feed_status, "mode", "offline"))
        max_age = _quote_max_age(mode)
        universe = eligible_universe or EligibleUniverse(
            (),
            "unavailable",
            now,
            "eligible_universe",
            "eligible universe is unavailable",
        )
        contexts = symbol_context or {}
        breadth = calculate_breadth(
            ticks,
            universe.symbols,
            now=now,
            max_age_seconds=max_age,
        )
        if mode not in {"push", "polling_30s"}:
            freshness = "unavailable"
        elif universe.status == "unavailable":
            freshness: FreshnessStatus = "unavailable"
        elif universe.status != "fresh" and breadth.status == "fresh":
            freshness = "partial"
        else:
            freshness = cast(FreshnessStatus, breadth.status)
        valid_equity_ticks = [
            ticks[symbol]
            for symbol in universe.symbols
            if symbol in ticks and _valid_realtime_tick(ticks[symbol], now, max_age)
        ]
        returns = [(tick.last_price / tick.previous_close - 1) * 100 for tick in valid_equity_ticks]
        market_as_of = max(
            (tick.quote_time for tick in valid_equity_ticks),
            default=now,
        )
        market_metrics = {
            "median_return_pct": MetricValue(
                median(returns) if returns else None,
                freshness,
                market_as_of,
                "qmt_realtime",
                reason=None if freshness == "fresh" else "full-market coverage is incomplete",
            ),
            "decline_ratio": MetricValue(
                sum(value < 0 for value in returns) / len(returns) if returns else None,
                freshness,
                market_as_of,
                "qmt_realtime",
                reason=None if freshness == "fresh" else "full-market coverage is incomplete",
            ),
        }
        observations: list[RadarObservation] = [RadarObservation("market", "ALL", market_metrics)]
        indices: dict[str, Any] = {}
        for symbol in sorted(CORE_INDICES):
            tick = ticks.get(symbol)
            valid = tick is not None and _valid_realtime_tick(tick, now, max_age)
            status: FreshnessStatus = "fresh" if valid else "unavailable"
            quote_time = tick.quote_time if tick is not None else now
            reason = None if valid else "index quote is missing, stale, or invalid"
            index_metrics = {
                "return_pct": MetricValue(
                    (tick.last_price / tick.previous_close - 1) * 100
                    if valid and tick is not None
                    else None,
                    status,
                    quote_time,
                    "qmt_realtime",
                    reason=reason,
                ),
                "return_5m_pct": MetricValue(
                    tick.speed_5m
                    if valid and tick is not None and _finite_or_none(tick.speed_5m) is not None
                    else None,
                    "fresh"
                    if valid and tick is not None and _finite_or_none(tick.speed_5m) is not None
                    else "unavailable",
                    quote_time,
                    "qmt_realtime",
                    reason=(
                        None
                        if valid and tick is not None and _finite_or_none(tick.speed_5m) is not None
                        else "five-minute index return is unavailable"
                    ),
                ),
            }
            observations.append(RadarObservation("market", symbol, index_metrics))
            indices[symbol] = {
                key: {
                    "value": value.value,
                    "status": value.status,
                    "as_of": value.as_of.isoformat(),
                    "reason": value.reason,
                }
                for key, value in index_metrics.items()
            }
        focus_metric_status: dict[str, dict[str, Any]] = {}
        for symbol in focus.symbols:
            tick = ticks.get(symbol)
            valid = tick is not None and _valid_realtime_tick(tick, now, max_age)
            tick_status: FreshnessStatus = "fresh" if valid else "unavailable"
            quote_time = tick.quote_time if tick is not None else now
            context = contexts.get(symbol)
            volume_ratio = _context_metric(
                context,
                "volume_ratio_20d",
                now,
                reason="symbol enrichment unavailable",
            )
            negative_heat = _context_metric(
                context,
                "negative_heat_z20",
                now,
                reason="symbol enrichment unavailable",
            )
            weighted_sentiment = _context_metric(
                context,
                "weighted_sentiment",
                now,
                reason="symbol enrichment unavailable",
            )
            down_limit = _context_metric(
                context,
                "down_limit_price",
                now,
                reason="exact down-limit price is unavailable",
            )
            up_limit = _context_metric(
                context,
                "up_limit_price",
                now,
                reason="exact up-limit price is unavailable",
            )
            down_limit_value = _finite_or_none(down_limit.value)
            up_limit_value = _finite_or_none(up_limit.value)
            down_distance = MetricValue(
                (
                    (tick.last_price - down_limit_value) / down_limit_value * 100
                    if valid
                    and tick is not None
                    and down_limit.status == "fresh"
                    and down_limit_value is not None
                    and down_limit_value > 0
                    else None
                ),
                (
                    "fresh"
                    if valid
                    and down_limit.status == "fresh"
                    and down_limit_value is not None
                    and down_limit_value > 0
                    else "unavailable"
                ),
                quote_time,
                down_limit.source,
                baseline=down_limit_value,
                reason=(
                    None
                    if valid
                    and down_limit.status == "fresh"
                    and down_limit_value is not None
                    and down_limit_value > 0
                    else down_limit.reason or "exact down-limit price or fresh quote is unavailable"
                ),
            )
            broken = MetricValue(
                (
                    bool(
                        tick.high_price is not None
                        and tick.high_price >= up_limit_value - 1e-4
                        and tick.last_price < up_limit_value - 1e-4
                    )
                    if valid
                    and tick is not None
                    and up_limit.status == "fresh"
                    and up_limit_value is not None
                    and up_limit_value > 0
                    and _finite_or_none(tick.high_price) is not None
                    else None
                ),
                (
                    "fresh"
                    if valid
                    and up_limit.status == "fresh"
                    and up_limit_value is not None
                    and up_limit_value > 0
                    and tick is not None
                    and _finite_or_none(tick.high_price) is not None
                    else "unavailable"
                ),
                quote_time,
                up_limit.source,
                baseline=up_limit_value,
                reason=(
                    None
                    if valid
                    and up_limit.status == "fresh"
                    and up_limit_value is not None
                    and up_limit_value > 0
                    and tick is not None
                    and _finite_or_none(tick.high_price) is not None
                    else up_limit.reason
                    or "exact up-limit price or fresh high price is unavailable"
                ),
            )
            symbol_metrics = {
                "return_pct": MetricValue(
                    (tick.last_price / tick.previous_close - 1) * 100
                    if valid and tick is not None
                    else None,
                    tick_status,
                    quote_time,
                    "qmt_realtime",
                    reason=None if valid else "symbol quote is missing, stale, or invalid",
                ),
                "drawdown_pct": MetricValue(
                    (tick.high_price - tick.last_price) / tick.high_price * 100
                    if valid
                    and tick is not None
                    and _finite_or_none(tick.high_price) is not None
                    and cast(float, tick.high_price) > 0
                    else None,
                    (
                        "fresh"
                        if valid
                        and tick is not None
                        and _finite_or_none(tick.high_price) is not None
                        and cast(float, tick.high_price) > 0
                        else "unavailable"
                    ),
                    quote_time,
                    "qmt_realtime",
                    reason=(
                        None
                        if valid
                        and tick is not None
                        and _finite_or_none(tick.high_price) is not None
                        and cast(float, tick.high_price) > 0
                        else "fresh intraday high is unavailable"
                    ),
                ),
                "volume_ratio_20d": volume_ratio,
                "down_limit_distance_pct": down_distance,
                "limit_up_broken": broken,
                "negative_heat_z20": negative_heat,
                "weighted_sentiment": weighted_sentiment,
            }
            observations.append(
                RadarObservation(
                    "symbol",
                    symbol,
                    symbol_metrics,
                    sources=focus.sources.get(symbol, ()),
                )
            )
            focus_metric_status[symbol] = {
                key: {
                    "value": metric.value,
                    "status": metric.status,
                    "as_of": metric.as_of.isoformat(),
                    "source": metric.source,
                    "reason": metric.reason,
                }
                for key, metric in symbol_metrics.items()
            }
        breadth_payload = serialize_breadth_result(breadth)
        breadth_payload["status"] = freshness
        breadth_payload["universe"] = {
            "status": universe.status,
            "as_of": universe.as_of.isoformat(),
            "source": universe.source,
            "reason": universe.reason,
            "eligible": len(universe.symbols),
        }
        metrics = {
            "overview": {
                "mode": mode,
                "market_median_return_pct": market_metrics["median_return_pct"].value,
                "decline_ratio": market_metrics["decline_ratio"].value,
                "status": freshness,
            },
            "breadth": breadth_payload,
            "indices": indices,
            "limit_ladder": {"status": "unavailable", "reason": "intraday source not loaded"},
            "crowding": {"status": "unavailable", "reason": "daily baseline not loaded"},
            "sectors": {"status": "unavailable", "items": []},
            "sentiment": {"status": "unavailable", "reason": "intraday source not loaded"},
            "focus": {**focus.as_dict(), "metric_status": focus_metric_status},
        }
        enrichment_freshness = {
            "klines_daily_20d": _aggregate_intraday_source(
                focus_metric_status,
                ("volume_ratio_20d",),
                default_reason=_SOURCE_DEFAULT_REASONS["klines_daily_20d"],
            ),
            "stock_limit_prices": _aggregate_intraday_source(
                focus_metric_status,
                ("down_limit_distance_pct", "limit_up_broken"),
                default_reason=_SOURCE_DEFAULT_REASONS["stock_limit_prices"],
            ),
            "sentiment_posts": _aggregate_intraday_source(
                focus_metric_status,
                ("negative_heat_z20", "weighted_sentiment"),
                default_reason=_SOURCE_DEFAULT_REASONS["sentiment_posts"],
            ),
        }
        return RadarSnapshotEnvelope(
            snapshot_type="intraday",
            as_of=now,
            computed_at=now,
            status=freshness,
            confidence=(
                breadth.coverage.coverage
                if mode in {"push", "polling_30s"} and universe.status != "unavailable"
                else 0.0
            ),
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
                },
                "eligible_universe": {
                    "status": universe.status,
                    "as_of": universe.as_of.isoformat(),
                    "source": universe.source,
                    "reason": universe.reason,
                    "row_count": len(universe.symbols),
                },
                **enrichment_freshness,
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
        history: RadarHistoryContext | None = None,
        now: datetime,
    ) -> RadarSnapshotEnvelope:
        historical = history or RadarHistoryContext()
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
        if overall == "fresh":
            overall = "partial"
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
                market_metrics["limit_down_median_5d"] = MetricValue(
                    historical.limit_down_median_5d,
                    "fresh"
                    if historical.limit_down_median_5d is not None
                    and len(historical.limit_down_counts) == 5
                    else "unavailable",
                    historical.previous_as_of or now,
                    "market_radar_eod_history",
                    baseline="previous_5_eod_snapshots",
                    reason=(
                        None
                        if historical.limit_down_median_5d is not None
                        and len(historical.limit_down_counts) == 5
                        else "five fresh prior limit-down counts are unavailable"
                    ),
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
            "partial" if emotion_score.value is not None else "unavailable"
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
            "market_radar_emotion_reduced_v1",
            emotion_score.formula_version,
            reason="limit-ladder history and several v1 subcomponents are unavailable",
        )
        previous_emotion = historical.previous_emotion_score
        market_metrics["previous_emotion_score"] = MetricValue(
            previous_emotion,
            "fresh" if previous_emotion is not None else "unavailable",
            historical.previous_as_of or now,
            "previous_eod_market_radar_snapshot",
            reason=None
            if previous_emotion is not None
            else "previous fresh EOD emotion is unavailable",
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
                        "crowding_score": MetricValue(
                            None,
                            "unavailable",
                            now,
                            "sector_crowding",
                            reason="independent sector crowding is unavailable",
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
        if emotion_score.value is not None:
            emotion_payload["status"] = "partial"
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
                        "label": None,
                        "reason": (
                            "reduced formula excludes unavailable v1 subcomponent histories"
                            if emotion_value is not None
                            else "effective emotion weight is insufficient"
                        ),
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


def _project_source(
    name: str,
    raw: object,
    *,
    default_reason: str | None,
) -> dict[str, Any]:
    details = dict(raw) if isinstance(raw, Mapping) else {}
    status = str(details.get("status") or "unavailable")
    as_of = details.get("as_of") or details.get("source_date") or details.get("latest_at")
    reason = details.get("reason")
    if reason is None and status != "fresh":
        reason = default_reason or f"{name} is {status}"
    return {
        "name": name,
        "as_of": as_of,
        "status": status,
        "reason": reason,
        **{
            key: value
            for key, value in details.items()
            if key not in {"name", "as_of", "status", "reason"}
        },
    }


def _aggregate_intraday_source(
    metric_status: Mapping[str, Mapping[str, Any]],
    metric_names: tuple[str, ...],
    *,
    default_reason: str,
) -> dict[str, Any]:
    values = [
        metrics[name]
        for metrics in metric_status.values()
        for name in metric_names
        if name in metrics
    ]
    if not values:
        return {
            "as_of": None,
            "status": "unavailable",
            "reason": default_reason,
        }
    fresh = [value for value in values if value.get("status") == "fresh"]
    status = "fresh" if len(fresh) == len(values) else ("partial" if fresh else "unavailable")
    fresh_times = [str(value["as_of"]) for value in fresh if value.get("as_of")]
    reasons = tuple(
        dict.fromkeys(
            str(value["reason"])
            for value in values
            if value.get("status") != "fresh" and value.get("reason")
        )
    )
    return {
        "as_of": max(fresh_times, default=None),
        "status": status,
        "reason": None if status == "fresh" else ("; ".join(reasons) or default_reason),
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
    return composite_score(raw.values(), formula_version="market-radar-emotion-reduced-v1")


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
    if isinstance(value, bool):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if math.isfinite(numeric) else None


def _quote_max_age(mode: str) -> float:
    if mode == "push":
        return 5.0
    if mode == "polling_30s":
        return 45.0
    return 0.0


def _valid_realtime_tick(tick: QuoteTick, now: datetime, max_age_seconds: float) -> bool:
    if max_age_seconds <= 0 or tick.stock_status == 1:
        return False
    age = (now - tick.quote_time).total_seconds()
    return (
        0 <= age <= max_age_seconds
        and (last := _finite_or_none(tick.last_price)) is not None
        and last > 0
        and (previous := _finite_or_none(tick.previous_close)) is not None
        and previous > 0
    )


def _context_metric(
    context: IntradaySymbolContext | None,
    key: str,
    now: datetime,
    *,
    reason: str,
) -> MetricValue:
    if context is not None:
        value = context.metrics.get(key)
        if value is not None:
            return value
    return MetricValue(None, "unavailable", now, "symbol_enrichment", reason=reason)


def _nested_number(value: Mapping[str, Any], *keys: str) -> float | None:
    current: object = value
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return _finite_or_none(current)


def _nested_value(value: Mapping[str, Any], *keys: str) -> object | None:
    current: object = value
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


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

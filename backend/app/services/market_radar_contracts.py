"""Immutable contracts shared by market-radar services."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any, Literal

from app.db.models.market_radar import MarketAlertEvent
from app.services.market_radar_data import serialize_market_radar_data

FreshnessStatus = Literal["fresh", "partial", "stale", "unavailable"]
RadarScope = Literal["market", "sector", "symbol", "data"]
SnapshotType = Literal["intraday", "eod"]
Severity = Literal["low", "medium", "high"]
StreamEventType = Literal["mode", "snapshot", "alert", "heartbeat"]

DEFAULT_RULE_VERSION = 1
DEFAULT_FORMULA_VERSION = "market-radar-v1"
DEFAULT_COOLDOWN_SECONDS = 15 * 60


@dataclass(frozen=True, slots=True)
class MetricValue:
    value: float | bool | None
    status: FreshnessStatus
    as_of: datetime
    source: str
    baseline: float | str | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class EligibleUniverse:
    symbols: tuple[str, ...]
    status: FreshnessStatus
    as_of: datetime
    source: str
    reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbols", tuple(dict.fromkeys(self.symbols)))


@dataclass(frozen=True, slots=True)
class IntradaySymbolContext:
    metrics: Mapping[str, MetricValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))


@dataclass(frozen=True, slots=True)
class RadarHistoryContext:
    limit_down_counts: tuple[float, ...] = ()
    limit_down_median_5d: float | None = None
    previous_emotion_score: float | None = None
    previous_as_of: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "limit_down_counts", tuple(self.limit_down_counts))


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

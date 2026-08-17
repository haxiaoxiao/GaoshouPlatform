"""Persistent snapshots, rules, and alert events for the market radar."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class MarketRadarSnapshot(Base, TimestampMixin):
    __tablename__ = "market_radar_snapshots"
    __table_args__ = (
        CheckConstraint(
            "snapshot_type IN ('intraday', 'eod')",
            name="ck_market_radar_snapshots_snapshot_type",
        ),
        CheckConstraint(
            "status IN ('fresh', 'partial', 'stale', 'unavailable')",
            name="ck_market_radar_snapshots_status",
        ),
        CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_market_radar_snapshots_confidence",
        ),
        Index(
            "uq_market_radar_snapshot_identity",
            "snapshot_type",
            "as_of",
            "formula_version",
            unique=True,
        ),
        Index("ix_market_radar_snapshots_type_as_of", "snapshot_type", "as_of"),
        Index("ix_market_radar_snapshots_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_type: Mapped[str] = mapped_column(String(16), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    formula_version: Mapped[str] = mapped_column(String(64), nullable=False)
    metrics_json: Mapped[str] = mapped_column(Text, nullable=False)
    source_freshness_json: Mapped[str] = mapped_column(Text, nullable=False)


class MarketAlertRule(Base, TimestampMixin):
    __tablename__ = "market_alert_rules"
    __table_args__ = (
        CheckConstraint("version >= 1", name="ck_market_alert_rules_version"),
        CheckConstraint(
            "scope IN ('market', 'sector', 'symbol', 'data')",
            name="ck_market_alert_rules_scope",
        ),
        CheckConstraint(
            "severity IN ('low', 'medium', 'high')",
            name="ck_market_alert_rules_severity",
        ),
        CheckConstraint(
            "cooldown_seconds >= 0",
            name="ck_market_alert_rules_cooldown_seconds",
        ),
        CheckConstraint(
            "source IN ('system', 'user')",
            name="ck_market_alert_rules_source",
        ),
        Index(
            "uq_market_alert_rule_key_version",
            "rule_key",
            "version",
            unique=True,
        ),
        Index(
            "ix_market_alert_rules_enabled_scope_subject",
            "enabled",
            "scope",
            "subject",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_key: Mapped[str] = mapped_column(String(120), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    subject: Mapped[str] = mapped_column(String(160), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(80), nullable=False)
    parameters_json: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    cooldown_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source: Mapped[str] = mapped_column(String(16), nullable=False)


class MarketAlertEvent(Base, TimestampMixin):
    __tablename__ = "market_alert_events"
    __table_args__ = (
        CheckConstraint(
            "scope IN ('market', 'sector', 'symbol', 'data')",
            name="ck_market_alert_events_scope",
        ),
        CheckConstraint(
            "severity IN ('low', 'medium', 'high')",
            name="ck_market_alert_events_severity",
        ),
        CheckConstraint(
            "status IN ('active', 'acknowledged', 'dismissed', 'resolved')",
            name="ck_market_alert_events_status",
        ),
        CheckConstraint(
            "occurrence_count >= 1",
            name="ck_market_alert_events_occurrence_count",
        ),
        CheckConstraint(
            "clear_streak >= 0",
            name="ck_market_alert_events_clear_streak",
        ),
        Index(
            "ux_market_alert_events_open_dedupe_key",
            "dedupe_key",
            unique=True,
            sqlite_where=text("status IN ('active', 'acknowledged', 'dismissed')"),
        ),
        Index(
            "ix_market_alert_events_status_triggered_at",
            "status",
            "triggered_at",
        ),
        Index(
            "ix_market_alert_events_scope_subject_status",
            "scope",
            "subject",
            "status",
        ),
        Index("ix_market_alert_events_rule_id", "rule_id"),
        Index("ix_market_alert_events_snapshot_id", "snapshot_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    rule_id: Mapped[int] = mapped_column(
        ForeignKey("market_alert_rules.id"),
        nullable=False,
    )
    snapshot_id: Mapped[int | None] = mapped_column(
        ForeignKey("market_radar_snapshots.id", ondelete="SET NULL")
    )
    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    subject: Mapped[str] = mapped_column(String(160), nullable=False)
    direction: Mapped[str] = mapped_column(String(24), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    explanation: Mapped[str] = mapped_column(Text, nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(240), nullable=False)
    evidence_json: Mapped[str] = mapped_column(Text, nullable=False)
    triggered_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime)
    dismissed_at: Mapped[datetime | None] = mapped_column(DateTime)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_notified_at: Mapped[datetime | None] = mapped_column(DateTime)
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    clear_streak: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

"""Add persistent market radar snapshots, rules, and alert events."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260718_0001"
down_revision = "20260714_0001"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    tables = _table_names()
    if "market_radar_snapshots" not in tables:
        op.create_table(
            "market_radar_snapshots",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("snapshot_type", sa.String(16), nullable=False),
            sa.Column("as_of", sa.DateTime(), nullable=False),
            sa.Column("computed_at", sa.DateTime(), nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("confidence", sa.Float(), nullable=False),
            sa.Column("formula_version", sa.String(64), nullable=False),
            sa.Column("metrics_json", sa.Text(), nullable=False),
            sa.Column("source_freshness_json", sa.Text(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.CheckConstraint(
                "snapshot_type IN ('intraday', 'eod')",
                name="ck_market_radar_snapshots_snapshot_type",
            ),
            sa.CheckConstraint(
                "status IN ('fresh', 'partial', 'stale', 'unavailable')",
                name="ck_market_radar_snapshots_status",
            ),
            sa.CheckConstraint(
                "confidence >= 0 AND confidence <= 1",
                name="ck_market_radar_snapshots_confidence",
            ),
        )
        op.create_index(
            "uq_market_radar_snapshot_identity",
            "market_radar_snapshots",
            ["snapshot_type", "as_of", "formula_version"],
            unique=True,
        )
        op.create_index(
            "ix_market_radar_snapshots_type_as_of",
            "market_radar_snapshots",
            ["snapshot_type", "as_of"],
        )
        op.create_index(
            "ix_market_radar_snapshots_status",
            "market_radar_snapshots",
            ["status"],
        )

    tables = _table_names()
    if "market_alert_rules" not in tables:
        op.create_table(
            "market_alert_rules",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("rule_key", sa.String(120), nullable=False),
            sa.Column("version", sa.Integer(), nullable=False),
            sa.Column("scope", sa.String(16), nullable=False),
            sa.Column("subject", sa.String(160), nullable=False),
            sa.Column("rule_type", sa.String(80), nullable=False),
            sa.Column("parameters_json", sa.Text(), nullable=False),
            sa.Column("severity", sa.String(16), nullable=False),
            sa.Column("cooldown_seconds", sa.Integer(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False),
            sa.Column("source", sa.String(16), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.CheckConstraint("version >= 1", name="ck_market_alert_rules_version"),
            sa.CheckConstraint(
                "scope IN ('market', 'sector', 'symbol', 'data')",
                name="ck_market_alert_rules_scope",
            ),
            sa.CheckConstraint(
                "severity IN ('low', 'medium', 'high')",
                name="ck_market_alert_rules_severity",
            ),
            sa.CheckConstraint(
                "cooldown_seconds >= 0",
                name="ck_market_alert_rules_cooldown_seconds",
            ),
            sa.CheckConstraint(
                "source IN ('system', 'user')",
                name="ck_market_alert_rules_source",
            ),
        )
        op.create_index(
            "uq_market_alert_rule_key_version",
            "market_alert_rules",
            ["rule_key", "version"],
            unique=True,
        )
        op.create_index(
            "ix_market_alert_rules_enabled_scope_subject",
            "market_alert_rules",
            ["enabled", "scope", "subject"],
        )

    tables = _table_names()
    if "market_alert_events" not in tables:
        op.create_table(
            "market_alert_events",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "rule_id",
                sa.Integer(),
                sa.ForeignKey("market_alert_rules.id"),
                nullable=False,
            ),
            sa.Column(
                "snapshot_id",
                sa.Integer(),
                sa.ForeignKey("market_radar_snapshots.id", ondelete="SET NULL"),
            ),
            sa.Column("scope", sa.String(16), nullable=False),
            sa.Column("subject", sa.String(160), nullable=False),
            sa.Column("direction", sa.String(24), nullable=False),
            sa.Column("severity", sa.String(16), nullable=False),
            sa.Column("status", sa.String(16), nullable=False),
            sa.Column("title", sa.String(200), nullable=False),
            sa.Column("explanation", sa.Text(), nullable=False),
            sa.Column("dedupe_key", sa.String(240), nullable=False),
            sa.Column("evidence_json", sa.Text(), nullable=False),
            sa.Column("triggered_at", sa.DateTime(), nullable=False),
            sa.Column("last_seen_at", sa.DateTime(), nullable=False),
            sa.Column("acknowledged_at", sa.DateTime()),
            sa.Column("dismissed_at", sa.DateTime()),
            sa.Column("resolved_at", sa.DateTime()),
            sa.Column("last_notified_at", sa.DateTime()),
            sa.Column("occurrence_count", sa.Integer(), nullable=False),
            sa.Column("clear_streak", sa.Integer(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.CheckConstraint(
                "scope IN ('market', 'sector', 'symbol', 'data')",
                name="ck_market_alert_events_scope",
            ),
            sa.CheckConstraint(
                "severity IN ('low', 'medium', 'high')",
                name="ck_market_alert_events_severity",
            ),
            sa.CheckConstraint(
                "status IN ('active', 'acknowledged', 'dismissed', 'resolved')",
                name="ck_market_alert_events_status",
            ),
            sa.CheckConstraint(
                "occurrence_count >= 1",
                name="ck_market_alert_events_occurrence_count",
            ),
            sa.CheckConstraint(
                "clear_streak >= 0",
                name="ck_market_alert_events_clear_streak",
            ),
        )
        op.create_index(
            "ux_market_alert_events_open_dedupe_key",
            "market_alert_events",
            ["dedupe_key"],
            unique=True,
            sqlite_where=sa.text(
                "status IN ('active', 'acknowledged', 'dismissed')"
            ),
        )
        op.create_index(
            "ix_market_alert_events_status_triggered_at",
            "market_alert_events",
            ["status", "triggered_at"],
        )
        op.create_index(
            "ix_market_alert_events_scope_subject_status",
            "market_alert_events",
            ["scope", "subject", "status"],
        )
        op.create_index(
            "ix_market_alert_events_rule_id",
            "market_alert_events",
            ["rule_id"],
        )
        op.create_index(
            "ix_market_alert_events_snapshot_id",
            "market_alert_events",
            ["snapshot_id"],
        )


def downgrade() -> None:
    tables = _table_names()
    if "market_alert_events" in tables:
        op.drop_table("market_alert_events")
    tables = _table_names()
    if "market_alert_rules" in tables:
        op.drop_table("market_alert_rules")
    tables = _table_names()
    if "market_radar_snapshots" in tables:
        op.drop_table("market_radar_snapshots")

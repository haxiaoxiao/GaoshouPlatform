"""Add research lineage, persistent jobs, and versioned backtests."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260710_0001"
down_revision = None
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def _column_names(table: str) -> set[str]:
    return {str(column["name"]) for column in sa.inspect(op.get_bind()).get_columns(table)}


def _index_names(table: str) -> set[str]:
    return {str(index["name"]) for index in sa.inspect(op.get_bind()).get_indexes(table)}


def upgrade() -> None:
    tables = _table_names()
    if "data_snapshots" not in tables:
        op.create_table(
            "data_snapshots",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("environment", sa.String(20), nullable=False),
            sa.Column("dataset_versions", sa.JSON(), nullable=False),
            sa.Column("freshness", sa.JSON(), nullable=False),
            sa.Column("schema_hash", sa.String(64), nullable=False),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_data_snapshots_environment", "data_snapshots", ["environment"])
        op.create_index("ix_data_snapshots_status", "data_snapshots", ["status"])

    tables = _table_names()
    if "strategy_releases" not in tables:
        op.create_table(
            "strategy_releases",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("strategy_id", sa.Integer(), sa.ForeignKey("strategies.id"), nullable=False),
            sa.Column("data_snapshot_id", sa.String(36), sa.ForeignKey("data_snapshots.id"), nullable=False),
            sa.Column("code_hash", sa.String(64), nullable=False),
            sa.Column("git_commit", sa.String(64), nullable=False),
            sa.Column("engine", sa.String(30), nullable=False),
            sa.Column("engine_version", sa.String(40), nullable=False),
            sa.Column("parameters", sa.JSON(), nullable=False),
            sa.Column("universe", sa.JSON(), nullable=False),
            sa.Column("cost_model", sa.JSON(), nullable=False),
            sa.Column("factor_params_hashes", sa.JSON(), nullable=False),
            sa.Column("status", sa.String(24), nullable=False),
            sa.Column("approved_at", sa.DateTime()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_strategy_releases_strategy_id", "strategy_releases", ["strategy_id"])
        op.create_index("ix_strategy_releases_data_snapshot_id", "strategy_releases", ["data_snapshot_id"])
        op.create_index("ix_strategy_releases_code_hash", "strategy_releases", ["code_hash"])
        op.create_index("ix_strategy_releases_status", "strategy_releases", ["status"])

    tables = _table_names()
    if "research_artifacts" not in tables:
        op.create_table(
            "research_artifacts",
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("release_id", sa.String(36), sa.ForeignKey("strategy_releases.id"), nullable=False),
            sa.Column("kind", sa.String(30), nullable=False),
            sa.Column("validation_status", sa.String(20), nullable=False),
            sa.Column("start_date", sa.Date()),
            sa.Column("end_date", sa.Date()),
            sa.Column("metrics", sa.JSON(), nullable=False),
            sa.Column("report_path", sa.Text()),
            sa.Column("checksum", sa.String(64)),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_research_artifacts_release_id", "research_artifacts", ["release_id"])
        op.create_index("ix_research_artifacts_kind", "research_artifacts", ["kind"])
        op.create_index("ix_research_artifacts_validation_status", "research_artifacts", ["validation_status"])

    tables = _table_names()
    if "jobs" not in tables:
        op.create_table(
            "jobs",
            sa.Column("id", sa.String(64), primary_key=True),
            sa.Column("kind", sa.String(40), nullable=False),
            sa.Column("title", sa.String(200), nullable=False),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("progress", sa.Float(), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=False),
            sa.Column("result_ref", sa.Text()),
            sa.Column("error", sa.Text()),
            sa.Column("heartbeat_at", sa.DateTime()),
            sa.Column("finished_at", sa.DateTime()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_jobs_kind", "jobs", ["kind"])
        op.create_index("ix_jobs_status", "jobs", ["status"])

    tables = _table_names()
    if "job_events" not in tables:
        op.create_table(
            "job_events",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("job_id", sa.String(64), sa.ForeignKey("jobs.id"), nullable=False),
            sa.Column("event_type", sa.String(40), nullable=False),
            sa.Column("data", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
        )
        op.create_index("ix_job_events_job_id", "job_events", ["job_id"])
        op.create_index("ix_job_events_created_at", "job_events", ["created_at"])

    if "backtests" in _table_names():
        columns = _column_names("backtests")
        additions = {
            "run_id": sa.Column("run_id", sa.String(64)),
            "release_id": sa.Column("release_id", sa.String(36)),
            "data_snapshot_id": sa.Column("data_snapshot_id", sa.String(36)),
            "engine": sa.Column("engine", sa.String(30)),
            "result_schema_version": sa.Column("result_schema_version", sa.Integer()),
            "code_hash": sa.Column("code_hash", sa.String(64)),
            "warnings": sa.Column("warnings", sa.JSON()),
        }
        for name, column in additions.items():
            if name not in columns:
                op.add_column("backtests", column)
        indexes = _index_names("backtests")
        for name, column, unique in (
            ("ix_backtests_run_id", "run_id", True),
            ("ix_backtests_release_id", "release_id", False),
            ("ix_backtests_data_snapshot_id", "data_snapshot_id", False),
        ):
            if name not in indexes:
                op.create_index(name, "backtests", [column], unique=unique)


def downgrade() -> None:
    if "backtests" in _table_names():
        indexes = _index_names("backtests")
        for name in ("ix_backtests_data_snapshot_id", "ix_backtests_release_id", "ix_backtests_run_id"):
            if name in indexes:
                op.drop_index(name, table_name="backtests")
        columns = _column_names("backtests")
        with op.batch_alter_table("backtests") as batch:
            for name in (
                "warnings",
                "code_hash",
                "result_schema_version",
                "engine",
                "data_snapshot_id",
                "release_id",
                "run_id",
            ):
                if name in columns:
                    batch.drop_column(name)
    for table in ("job_events", "jobs", "research_artifacts", "strategy_releases", "data_snapshots"):
        if table in _table_names():
            op.drop_table(table)

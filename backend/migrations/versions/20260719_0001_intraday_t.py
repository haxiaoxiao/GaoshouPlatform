"""Add persistent intraday T paper sessions and simulated trades."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260719_0001"
down_revision = "20260718_0001"
branch_labels = None
depends_on = None


def _table_names() -> set[str]:
    return set(sa.inspect(op.get_bind()).get_table_names())


def upgrade() -> None:
    tables = _table_names()
    if "intraday_t_sessions" not in tables:
        op.create_table(
            "intraday_t_sessions",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("session_id", sa.String(80), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("status", sa.String(30), nullable=False),
            sa.Column("mode", sa.String(20), nullable=False),
            sa.Column("account_source", sa.String(30), nullable=False),
            sa.Column("strategy_params", sa.JSON(), nullable=False),
            sa.Column("baseline", sa.JSON(), nullable=False),
            sa.Column("runtime_state", sa.JSON(), nullable=False),
            sa.Column("last_evaluated_at", sa.DateTime()),
            sa.Column("last_error", sa.Text()),
            sa.Column("runner_active", sa.Boolean(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index(
            "ix_intraday_t_sessions_session_id",
            "intraday_t_sessions",
            ["session_id"],
            unique=True,
        )
        op.create_index(
            "ix_intraday_t_sessions_trade_date",
            "intraday_t_sessions",
            ["trade_date"],
        )
        op.create_index(
            "ix_intraday_t_sessions_status",
            "intraday_t_sessions",
            ["status"],
        )
        op.create_index(
            "ix_intraday_t_sessions_last_evaluated_at",
            "intraday_t_sessions",
            ["last_evaluated_at"],
        )

    tables = _table_names()
    if "intraday_t_trades" not in tables:
        op.create_table(
            "intraday_t_trades",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("trade_id", sa.String(80), nullable=False),
            sa.Column("session_id", sa.String(80), nullable=False),
            sa.Column("trade_date", sa.Date(), nullable=False),
            sa.Column("symbol", sa.String(20), nullable=False),
            sa.Column("pair_id", sa.String(80), nullable=False),
            sa.Column("direction", sa.String(20), nullable=False),
            sa.Column("leg", sa.String(20), nullable=False),
            sa.Column("side", sa.String(10), nullable=False),
            sa.Column("quantity", sa.Integer(), nullable=False),
            sa.Column("signal_at", sa.DateTime(), nullable=False),
            sa.Column("fill_at", sa.DateTime(), nullable=False),
            sa.Column("reference_price", sa.Float(), nullable=False),
            sa.Column("fill_price", sa.Float(), nullable=False),
            sa.Column("fees", sa.Float(), nullable=False),
            sa.Column("gross_pnl", sa.Float(), nullable=False),
            sa.Column("net_pnl", sa.Float(), nullable=False),
            sa.Column("reason", sa.String(80), nullable=False),
            sa.Column("status", sa.String(30), nullable=False),
            sa.Column("idempotency_key", sa.String(160), nullable=False),
            sa.Column("payload", sa.JSON()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        op.create_index(
            "ix_intraday_t_trades_trade_id",
            "intraday_t_trades",
            ["trade_id"],
            unique=True,
        )
        op.create_index(
            "ix_intraday_t_trades_session_id",
            "intraday_t_trades",
            ["session_id"],
        )
        op.create_index(
            "ix_intraday_t_trades_trade_date",
            "intraday_t_trades",
            ["trade_date"],
        )
        op.create_index("ix_intraday_t_trades_symbol", "intraday_t_trades", ["symbol"])
        op.create_index("ix_intraday_t_trades_pair_id", "intraday_t_trades", ["pair_id"])
        op.create_index("ix_intraday_t_trades_fill_at", "intraday_t_trades", ["fill_at"])
        op.create_index(
            "ix_intraday_t_trades_idempotency_key",
            "intraday_t_trades",
            ["idempotency_key"],
            unique=True,
        )


def downgrade() -> None:
    tables = _table_names()
    if "intraday_t_trades" in tables:
        op.drop_table("intraday_t_trades")
    tables = _table_names()
    if "intraday_t_sessions" in tables:
        op.drop_table("intraday_t_sessions")

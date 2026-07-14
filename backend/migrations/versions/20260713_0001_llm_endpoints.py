"""Add persisted encrypted LLM endpoint configuration."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260713_0001"
down_revision = "20260712_0001"
branch_labels = None
depends_on = None

_TABLE_NAME = "llm_endpoints"
_PRIORITY_INDEX = "ix_llm_endpoints_enabled_priority"
_COOLDOWN_INDEX = "ix_llm_endpoints_cooldown_until"
_UNIQUE_PRIORITY_INDEX = "ux_llm_endpoints_priority"


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _TABLE_NAME not in set(inspector.get_table_names()):
        op.create_table(
            _TABLE_NAME,
            sa.Column("id", sa.String(36), primary_key=True),
            sa.Column("name", sa.String(100), nullable=False),
            sa.Column("api_base", sa.String(500), nullable=False),
            sa.Column("api_key_encrypted", sa.Text(), nullable=False),
            sa.Column("api_key_hint", sa.String(32), nullable=False),
            sa.Column("model", sa.String(200), nullable=False),
            sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
            sa.Column("consecutive_failures", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("cooldown_until", sa.DateTime()),
            sa.Column("last_success_at", sa.DateTime()),
            sa.Column("last_failure_at", sa.DateTime()),
            sa.Column("last_error", sa.Text()),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
        )
        inspector = sa.inspect(bind)
    indexes = {index["name"] for index in inspector.get_indexes(_TABLE_NAME)}
    if _PRIORITY_INDEX not in indexes:
        op.create_index(_PRIORITY_INDEX, _TABLE_NAME, ["enabled", "priority"])
    if _COOLDOWN_INDEX not in indexes:
        op.create_index(_COOLDOWN_INDEX, _TABLE_NAME, ["cooldown_until"])
    if _UNIQUE_PRIORITY_INDEX not in indexes:
        endpoint_ids = bind.execute(
            sa.text("SELECT id FROM llm_endpoints ORDER BY priority, id")
        ).scalars()
        for priority, endpoint_id in enumerate(endpoint_ids):
            bind.execute(
                sa.text("UPDATE llm_endpoints SET priority = :priority WHERE id = :endpoint_id"),
                {"priority": priority, "endpoint_id": endpoint_id},
            )
        op.create_index(_UNIQUE_PRIORITY_INDEX, _TABLE_NAME, ["priority"], unique=True)


def downgrade() -> None:
    if _TABLE_NAME in set(sa.inspect(op.get_bind()).get_table_names()):
        op.drop_table(_TABLE_NAME)

"""Persist normalized LLM JSON configuration."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260714_0001"
down_revision = "20260713_0001"
branch_labels = None
depends_on = None

_TABLE_NAME = "llm_endpoints"


def upgrade() -> None:
    with op.batch_alter_table(_TABLE_NAME) as batch_op:
        batch_op.add_column(sa.Column("provider", sa.String(100)))
        batch_op.add_column(sa.Column("review_model", sa.String(200)))
        batch_op.add_column(
            sa.Column(
                "wire_api",
                sa.String(32),
                nullable=False,
                server_default="chat_completions",
            )
        )
        batch_op.add_column(sa.Column("reasoning_effort", sa.String(16)))
        batch_op.add_column(
            sa.Column(
                "disable_response_storage",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch_op.add_column(
            sa.Column(
                "requires_openai_auth",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            )
        )
        batch_op.add_column(sa.Column("config_json", sa.Text()))

    if op.get_bind().dialect.name != "sqlite":
        with op.batch_alter_table(_TABLE_NAME) as batch_op:
            batch_op.alter_column("wire_api", server_default=None)
            batch_op.alter_column("disable_response_storage", server_default=None)
            batch_op.alter_column("requires_openai_auth", server_default=None)


def downgrade() -> None:
    with op.batch_alter_table(_TABLE_NAME) as batch_op:
        batch_op.drop_column("config_json")
        batch_op.drop_column("requires_openai_auth")
        batch_op.drop_column("disable_response_storage")
        batch_op.drop_column("reasoning_effort")
        batch_op.drop_column("wire_api")
        batch_op.drop_column("review_model")
        batch_op.drop_column("provider")

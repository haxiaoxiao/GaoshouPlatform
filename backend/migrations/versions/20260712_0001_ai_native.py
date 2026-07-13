"""Add the minimal persistent AI conversation store."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260712_0001"
down_revision = "20260710_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    tables = set(sa.inspect(op.get_bind()).get_table_names())
    if "ai_conversations" in tables:
        return
    op.create_table(
        "ai_conversations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("messages", sa.JSON(), nullable=False),
        sa.Column("context", sa.JSON(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_index("ix_ai_conversations_expires_at", "ai_conversations", ["expires_at"])


def downgrade() -> None:
    if "ai_conversations" in set(sa.inspect(op.get_bind()).get_table_names()):
        op.drop_table("ai_conversations")

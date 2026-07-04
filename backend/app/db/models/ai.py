from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, _beijing_now


class AIArtifact(Base):
    """Persistent evidence record for AI-native tool and chat flows."""

    __tablename__ = "ai_artifacts"

    artifact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="created", index=True)
    input_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    tool_calls: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    result_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    key_outputs: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_beijing_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=_beijing_now,
        onupdate=_beijing_now,
        nullable=False,
    )

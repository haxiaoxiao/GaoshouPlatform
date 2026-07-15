from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class LlmEndpoint(Base, TimestampMixin):
    """Persisted configuration for an OpenAI-compatible LLM endpoint."""

    __tablename__ = "llm_endpoints"
    __table_args__ = (
        Index("ix_llm_endpoints_enabled_priority", "enabled", "priority"),
        Index("ix_llm_endpoints_cooldown_until", "cooldown_until"),
        Index("ux_llm_endpoints_priority", "priority", unique=True),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    api_base: Mapped[str] = mapped_column(String(500), nullable=False)
    api_key_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    api_key_hint: Mapped[str] = mapped_column(String(32), nullable=False)
    model: Mapped[str] = mapped_column(String(200), nullable=False)
    provider: Mapped[str | None] = mapped_column(String(100))
    review_model: Mapped[str | None] = mapped_column(String(200))
    wire_api: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="chat_completions",
        server_default="chat_completions",
    )
    reasoning_effort: Mapped[str | None] = mapped_column(String(16))
    disable_response_storage: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    requires_openai_auth: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    config_json: Mapped[str | None] = mapped_column(Text)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_error: Mapped[str | None] = mapped_column(Text)


LLMEndpoint = LlmEndpoint

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class DataSnapshot(Base, TimestampMixin):
    __tablename__ = "data_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    dataset_versions: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    freshness: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    schema_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)


class StrategyRelease(Base, TimestampMixin):
    __tablename__ = "strategy_releases"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    data_snapshot_id: Mapped[str] = mapped_column(ForeignKey("data_snapshots.id"), nullable=False, index=True)
    code_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    git_commit: Mapped[str] = mapped_column(String(64), nullable=False)
    engine: Mapped[str] = mapped_column(String(30), nullable=False)
    engine_version: Mapped[str] = mapped_column(String(40), nullable=False)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    universe: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    cost_model: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    factor_params_hashes: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="draft", index=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime)

    artifacts: Mapped[list[ResearchArtifact]] = relationship(
        back_populates="release",
        cascade="all, delete-orphan",
    )


class ResearchArtifact(Base, TimestampMixin):
    __tablename__ = "research_artifacts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    release_id: Mapped[str] = mapped_column(ForeignKey("strategy_releases.id"), nullable=False, index=True)
    kind: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    validation_status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    metrics: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    report_path: Mapped[str | None] = mapped_column(Text)
    checksum: Mapped[str | None] = mapped_column(String(64))

    release: Mapped[StrategyRelease] = relationship(back_populates="artifacts")


class PersistentJob(Base, TimestampMixin):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued", index=True)
    progress: Mapped[float] = mapped_column(nullable=False, default=0.0)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    result_ref: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime)


class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False, index=True)

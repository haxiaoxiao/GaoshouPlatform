"""Persistence models for the dedicated intraday T paper service."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Float, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class IntradayTSession(Base, TimestampMixin):
    __tablename__ = "intraday_t_sessions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="RUNNING", index=True)
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="paper")
    account_source: Mapped[str] = mapped_column(String(30), nullable=False, default="manual")
    strategy_params: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    baseline: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    runtime_state: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime, index=True)
    last_error: Mapped[str | None] = mapped_column(Text)
    runner_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class IntradayTTrade(Base, TimestampMixin):
    __tablename__ = "intraday_t_trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trade_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    session_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    pair_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(20), nullable=False)
    leg: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    signal_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    fill_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    reference_price: Mapped[float] = mapped_column(Float, nullable=False)
    fill_price: Mapped[float] = mapped_column(Float, nullable=False)
    fees: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    gross_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    net_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    reason: Mapped[str] = mapped_column(String(80), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="FILLED")
    idempotency_key: Mapped[str] = mapped_column(
        String(160), nullable=False, unique=True, index=True
    )
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)

"""Models for configurable paper/live trading."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class LiveStrategyProfile(Base, TimestampMixin):
    """Whitelisted strategy configuration for paper/live execution."""

    __tablename__ = "live_strategy_profiles"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    profile_key: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    adapter_type: Mapped[str] = mapped_column(String(80), nullable=False, default="multi_factor_cash_aware")
    params_override: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    universe_config: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    execution_policy: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class LiveTradingRun(Base, TimestampMixin):
    """Process-local runner state persisted for the UI and audit trail."""

    __tablename__ = "live_trading_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    profile_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="paper")
    status: Mapped[str] = mapped_column(String(40), nullable=False, default="stopped")
    params: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    last_signal_hash: Mapped[str | None] = mapped_column(String(96), index=True)
    last_cycle_at: Mapped[datetime | None] = mapped_column(DateTime)
    last_error: Mapped[str | None] = mapped_column(Text)
    takeover_reason: Mapped[str | None] = mapped_column(Text)


class LiveOrderAudit(Base, TimestampMixin):
    """Audit row for generated, skipped, paper-filled, and live-submitted orders."""

    __tablename__ = "live_order_audits"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    audit_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    run_id: Mapped[str | None] = mapped_column(String(80), index=True)
    profile_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    trade_date: Mapped[date | None] = mapped_column(Date)
    signal_hash: Mapped[str | None] = mapped_column(String(96), index=True)
    trigger_source: Mapped[str] = mapped_column(String(30), nullable=False, default="manual")
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="paper")
    status: Mapped[str] = mapped_column(String(40), nullable=False)
    order_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    skip_reason: Mapped[str | None] = mapped_column(Text)


class LiveTradeRecord(Base, TimestampMixin):
    """Standalone paper/live trade journal used for weekly review."""

    __tablename__ = "live_trade_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    record_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    run_id: Mapped[str | None] = mapped_column(String(80), index=True)
    profile_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    trade_date: Mapped[date | None] = mapped_column(Date, index=True)
    signal_hash: Mapped[str | None] = mapped_column(String(96), index=True)
    trigger_source: Mapped[str] = mapped_column(String(30), nullable=False, default="manual")
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="paper", index=True)
    status: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    stock_name: Mapped[str | None] = mapped_column(String(120))
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    reference_price: Mapped[Decimal] = mapped_column(Numeric(18, 4), nullable=False, default=Decimal("0"))
    order_value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    order_id: Mapped[str | None] = mapped_column(String(80), index=True)
    message: Mapped[str | None] = mapped_column(Text)
    order_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    result_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    account_snapshot: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class LivePositionState(Base, TimestampMixin):
    """Small persisted state bag for live adapters."""

    __tablename__ = "live_position_state"
    __table_args__ = (UniqueConstraint("profile_key", "mode", "symbol", name="uq_live_position_state"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_key: Mapped[str] = mapped_column(String(80), nullable=False)
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="paper")
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    state: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class LivePaperAccount(Base, TimestampMixin):
    """Persisted paper account used by the live trading console."""

    __tablename__ = "live_paper_accounts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    account_key: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("1000000"))
    total_asset: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("1000000"))
    market_value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    positions: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class LiveEquitySnapshot(Base, TimestampMixin):
    """Strategy-pool equity point used for review and drawdown metrics."""

    __tablename__ = "live_equity_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    snapshot_id: Mapped[str] = mapped_column(String(80), nullable=False, unique=True, index=True)
    profile_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    strategy_id: Mapped[int | None] = mapped_column(ForeignKey("strategies.id"), index=True)
    mode: Mapped[str] = mapped_column(String(20), nullable=False, default="paper", index=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    cash: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    market_value: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    total_asset: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    realized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    source: Mapped[str | None] = mapped_column(String(80))
    meta: Mapped[dict[str, Any] | None] = mapped_column(JSON)

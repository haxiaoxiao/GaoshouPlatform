# backend/app/db/models/stock.py
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import Date, DateTime, Index, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class Stock(Base, TimestampMixin):
    """股票基础信息表"""

    __tablename__ = "stocks"

    symbol: Mapped[str] = mapped_column(String(20), primary_key=True, comment="股票代码")
    name: Mapped[str | None] = mapped_column(String(50), comment="股票名称")
    exchange: Mapped[str | None] = mapped_column(String(10), comment="交易所")
    industry: Mapped[str | None] = mapped_column(String(50), comment="所属行业")
    list_date: Mapped[date | None] = mapped_column(Date, comment="上市日期")


class KlineDaily(Base):
    """日K线数据表"""

    __tablename__ = "klines_daily"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="股票代码")
    trade_date: Mapped[date] = mapped_column(Date, nullable=False, index=True, comment="交易日期")
    open: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="开盘价")
    high: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最高价")
    low: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最低价")
    close: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="收盘价")
    volume: Mapped[int | None] = mapped_column(comment="成交量")
    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="成交额")
    turnover_rate: Mapped[Decimal | None] = mapped_column(Numeric(8, 4), comment="换手率")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_klines_daily_symbol_date", "symbol", "trade_date", unique=True),
    )


class KlineMinute(Base):
    """分钟K线数据表"""

    __tablename__ = "klines_minute"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True, comment="股票代码")
    datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True, comment="分钟时间戳")
    open: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="开盘价")
    high: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最高价")
    low: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="最低价")
    close: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="收盘价")
    volume: Mapped[int | None] = mapped_column(comment="成交量")
    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="成交额")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), nullable=False)

    __table_args__ = (
        Index("idx_klines_minute_symbol_datetime", "symbol", "datetime", unique=True),
    )

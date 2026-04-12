# backend/app/db/models/strategy.py
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Index, Numeric, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class Strategy(Base, TimestampMixin):
    """策略表"""

    __tablename__ = "strategies"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="策略名称")
    code: Mapped[str] = mapped_column(Text, nullable=False, comment="策略代码")
    parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="策略参数")
    description: Mapped[str | None] = mapped_column(Text, comment="策略描述")


class Backtest(Base, TimestampMixin):
    """回测记录表"""

    __tablename__ = "backtests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    strategy_id: Mapped[int] = mapped_column(ForeignKey("strategies.id"), nullable=False, comment="关联策略")
    status: Mapped[str] = mapped_column(String(20), default="pending", comment="状态: pending/running/completed/failed")
    start_date: Mapped[date] = mapped_column(Date, nullable=False, comment="回测起始日期")
    end_date: Mapped[date] = mapped_column(Date, nullable=False, comment="回测结束日期")
    initial_capital: Mapped[Decimal | None] = mapped_column(Numeric(15, 2), comment="初始资金")
    parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="回测参数")
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="回测结果摘要")
    report_path: Mapped[str | None] = mapped_column(String(255), comment="详细报告路径")

    strategy: Mapped["Strategy"] = relationship()


class Order(Base, TimestampMixin):
    """交易订单表"""

    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[str | None] = mapped_column(String(50), unique=True, comment="交易所订单号")
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, comment="股票代码")
    direction: Mapped[str] = mapped_column(String(10), nullable=False, comment="方向: buy/sell")
    order_type: Mapped[str] = mapped_column(String(10), nullable=False, comment="类型: limit/market")
    price: Mapped[Decimal | None] = mapped_column(Numeric(10, 3), comment="委托价格")
    quantity: Mapped[int] = mapped_column(nullable=False, comment="委托数量")
    filled_quantity: Mapped[int] = mapped_column(default=0, comment="成交数量")
    status: Mapped[str] = mapped_column(String(20), nullable=False, comment="状态: pending/filled/cancelled")
    strategy_id: Mapped[int | None] = mapped_column(ForeignKey("strategies.id"), comment="关联策略")
    signal_time: Mapped[datetime | None] = mapped_column(DateTime, comment="信号产生时间")
    order_time: Mapped[datetime | None] = mapped_column(DateTime, comment="下单时间")


class Trade(Base, TimestampMixin):
    """成交记录表"""

    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    trade_id: Mapped[str | None] = mapped_column(String(50), unique=True, comment="成交编号")
    order_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True, comment="关联订单")
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, comment="股票代码")
    direction: Mapped[str] = mapped_column(String(10), nullable=False, comment="方向: buy/sell")
    price: Mapped[Decimal] = mapped_column(Numeric(10, 3), nullable=False, comment="成交价格")
    quantity: Mapped[int] = mapped_column(nullable=False, comment="成交数量")
    commission: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), comment="手续费")
    trade_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="成交时间")

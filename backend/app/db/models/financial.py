# backend/app/db/models/financial.py
from datetime import date

from sqlalchemy import Date, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class FinancialData(Base, TimestampMixin):
    """财务数据表 - 按季度存储"""

    __tablename__ = "financial_data"
    __table_args__ = (
        UniqueConstraint("symbol", "report_date", name="uq_financial_symbol_report"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), index=True, comment="股票代码")
    report_date: Mapped[date] = mapped_column(Date, index=True, comment="报告期 如 2025-03-31")
    report_type: Mapped[str | None] = mapped_column(String(10), default=None, comment="报告类型: Q1/H1/Q3/Annual")

    eps: Mapped[float | None] = mapped_column(Float, comment="基本每股收益")
    bvps: Mapped[float | None] = mapped_column(Float, comment="每股净资产")
    roe: Mapped[float | None] = mapped_column(Float, comment="净资产收益率(%)")
    revenue: Mapped[float | None] = mapped_column(Float, comment="营业收入(万元)")
    net_profit: Mapped[float | None] = mapped_column(Float, comment="净利润(万元)")
    revenue_yoy: Mapped[float | None] = mapped_column(Float, comment="营收同比增长率(%)")
    profit_yoy: Mapped[float | None] = mapped_column(Float, comment="净利润同比增长率(%)")
    gross_margin: Mapped[float | None] = mapped_column(Float, comment="毛利率(%)")

    total_assets: Mapped[float | None] = mapped_column(Float, comment="总资产(万元)")
    total_liability: Mapped[float | None] = mapped_column(Float, comment="总负债(万元)")
    total_equity: Mapped[float | None] = mapped_column(Float, comment="股东权益(万元)")

    total_shares: Mapped[float | None] = mapped_column(Float, comment="总股本(万股)")
    float_shares: Mapped[float | None] = mapped_column(Float, comment="流通股本(万股)")
    a_float_shares: Mapped[float | None] = mapped_column(Float, comment="A股流通股本(万股)")
    limit_sell_shares: Mapped[float | None] = mapped_column(Float, comment="限售流通股(万股)")

    total_mv: Mapped[float | None] = mapped_column(Float, comment="总市值(万元)")
    circ_mv: Mapped[float | None] = mapped_column(Float, comment="流通市值(万元)")

    pe_ttm: Mapped[float | None] = mapped_column(Float, comment="市盈率TTM(计算)")
    pb: Mapped[float | None] = mapped_column(Float, comment="市净率(计算)")

    raw_data: Mapped[str | None] = mapped_column(Text, comment="原始数据JSON")

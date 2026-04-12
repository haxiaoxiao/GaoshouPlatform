# backend/app/db/models/stock.py
from datetime import date

from sqlalchemy import Date, String
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

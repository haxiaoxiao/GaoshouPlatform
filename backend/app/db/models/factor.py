# backend/app/db/models/factor.py
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class Factor(Base, TimestampMixin):
    """因子定义表"""

    __tablename__ = "factors"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, comment="因子名称")
    category: Mapped[str | None] = mapped_column(String(50), comment="因子分类")
    source: Mapped[str | None] = mapped_column(String(20), comment="来源: qmt/custom")
    code: Mapped[str | None] = mapped_column(Text, comment="因子计算代码")
    parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="默认参数")
    description: Mapped[str | None] = mapped_column(Text, comment="因子描述")


class FactorAnalysis(Base, TimestampMixin):
    """因子分析结果表"""

    __tablename__ = "factor_analysis"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    factor_id: Mapped[int] = mapped_column(ForeignKey("factors.id"), nullable=False, comment="关联因子")
    start_date: Mapped[date] = mapped_column(Date, nullable=False, comment="分析起始日期")
    end_date: Mapped[date] = mapped_column(Date, nullable=False, comment="分析结束日期")
    ic_mean: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="IC均值")
    ic_std: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="IC标准差")
    ir: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="信息比率")
    turnover_rate: Mapped[Decimal | None] = mapped_column(Numeric(6, 4), comment="换手率")
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="详细分析结果")

    factor: Mapped["Factor"] = relationship()

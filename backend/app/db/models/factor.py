# backend/app/db/models/factor.py
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, Integer, Numeric, String, Text
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


class FactorResearchRun(Base):
    """新版因子研究结果表，不复用旧 factor_analysis。"""

    __tablename__ = "factor_research_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    factor_name: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    factor_display_name: Mapped[str | None] = mapped_column(String(200))
    stock_pool_type: Mapped[str] = mapped_column(String(30), nullable=False)
    stock_pool_value: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    params_hash: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    params: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running", index=True)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    summary: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    detail: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class FactorResearchRunItem(Base):
    """批量因子研究任务的子结果。"""

    __tablename__ = "factor_research_run_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    batch_run_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    factor_name: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    run_id: Mapped[str | None] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued")
    error_message: Mapped[str | None] = mapped_column(Text)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)

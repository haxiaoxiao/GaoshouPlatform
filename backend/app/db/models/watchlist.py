# backend/app/db/models/watchlist.py
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class WatchlistGroup(Base, TimestampMixin):
    """自选股分组表"""

    __tablename__ = "watchlist_groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False, comment="分组名称")
    description: Mapped[str | None] = mapped_column(Text, comment="分组描述")

    # 关联股票
    stocks: Mapped[list["WatchlistStock"]] = relationship(
        back_populates="group", cascade="all, delete-orphan"
    )


class WatchlistStock(Base):
    """自选股关联表"""

    __tablename__ = "watchlist_stocks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(
        ForeignKey("watchlist_groups.id"), nullable=False, comment="分组ID"
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, comment="股票代码")
    added_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), nullable=False
    )

    # 关联分组
    group: Mapped["WatchlistGroup"] = relationship(back_populates="stocks")

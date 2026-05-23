# backend/app/db/models/sync.py
from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin, _beijing_now


class SyncTask(Base, TimestampMixin):
    """同步任务配置表"""

    __tablename__ = "sync_tasks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="任务名称")
    cron_expression: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="Cron 表达式"
    )
    sync_type: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="同步类型: stock_info/kline_daily/kline_minute"
    )
    symbols: Mapped[str | None] = mapped_column(Text, comment="股票代码列表(JSON)")
    start_date: Mapped[date | None] = mapped_column(Date, comment="历史数据起始日期")
    end_date: Mapped[date | None] = mapped_column(Date, comment="历史数据结束日期")
    failure_strategy: Mapped[str] = mapped_column(
        String(20), default="skip", comment="失败策略: skip/retry/stop"
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=3, comment="重试次数")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, comment="是否启用")
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, comment="上次执行时间")
    next_run_at: Mapped[datetime | None] = mapped_column(DateTime, comment="下次执行时间")

    # 关联执行记录
    logs: Mapped[list["SyncLog"]] = relationship(
        back_populates="task", cascade="all, delete-orphan"
    )


class SyncLog(Base):
    """同步执行记录表"""

    __tablename__ = "sync_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    task_id: Mapped[int | None] = mapped_column(
        ForeignKey("sync_tasks.id"), comment="关联任务ID(手动同步为空)"
    )
    sync_type: Mapped[str] = mapped_column(String(50), nullable=False, comment="同步类型")
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, comment="状态: running/completed/failed"
    )
    total_count: Mapped[int | None] = mapped_column(Integer, comment="总数量")
    success_count: Mapped[int | None] = mapped_column(Integer, comment="成功数量")
    failed_count: Mapped[int | None] = mapped_column(Integer, comment="失败数量")
    start_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, comment="开始时间")
    end_time: Mapped[datetime | None] = mapped_column(DateTime, comment="结束时间")
    error_message: Mapped[str | None] = mapped_column(Text, comment="错误信息")
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, comment="详细结果")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=_beijing_now, nullable=False
    )

    # 关联任务
    task: Mapped["SyncTask | None"] = relationship(back_populates="logs")


class SyncRun(Base):
    """Persisted runtime state for manual and scheduled sync runs."""

    __tablename__ = "sync_runs"

    run_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    sync_task_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sync_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="queued")
    total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    current: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    success_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    progress_percent: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    start_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    request: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=_beijing_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_beijing_now, onupdate=_beijing_now, nullable=False
    )

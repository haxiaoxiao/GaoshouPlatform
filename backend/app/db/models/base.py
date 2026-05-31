# backend/app/db/models/base.py
from datetime import datetime, timedelta, timezone

from sqlalchemy import DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# 北京时区 UTC+8
_BEIJING_TZ = timezone(timedelta(hours=8))


def _beijing_now() -> datetime:
    return datetime.now(_BEIJING_TZ).replace(tzinfo=None)


class Base(DeclarativeBase):
    """SQLAlchemy 声明式基类"""

    pass


class TimestampMixin:
    """时间戳混入类（北京时间）"""

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=_beijing_now, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_beijing_now, onupdate=_beijing_now, nullable=False
    )

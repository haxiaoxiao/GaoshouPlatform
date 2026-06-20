# backend/app/db/sqlite.py
from __future__ import annotations

from threading import RLock
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.core.dev_data_mode import apply_dev_data_mode_to_settings


class DynamicAsyncSessionFactory:
    """Callable async session factory that follows the active dev data mode."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._url: str | None = None
        self._engine: AsyncEngine | None = None
        self._maker: async_sessionmaker[AsyncSession] | None = None

    def _ensure(self) -> async_sessionmaker[AsyncSession]:
        apply_dev_data_mode_to_settings()
        url = settings.database_url
        with self._lock:
            if self._maker is None or self._url != url:
                self._engine = create_async_engine(
                    url,
                    echo=settings.debug,
                    future=True,
                )
                self._maker = async_sessionmaker(
                    self._engine,
                    class_=AsyncSession,
                    expire_on_commit=False,
                )
                self._url = url
            return self._maker

    @property
    def engine(self) -> AsyncEngine:
        self._ensure()
        assert self._engine is not None
        return self._engine

    def __call__(self, *args, **kwargs) -> AsyncSession:
        return self._ensure()(*args, **kwargs)


async_session_factory = DynamicAsyncSessionFactory()


async def init_db():
    """初始化数据库（创建所有表）"""
    from app.db.models.base import Base

    apply_dev_data_mode_to_settings()
    async with async_session_factory.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        columns = await conn.exec_driver_sql("PRAGMA table_info(financial_data)")
        financial_columns = {str(row[1]) for row in columns.fetchall()}
        if "ann_date" not in financial_columns:
            await conn.exec_driver_sql("ALTER TABLE financial_data ADD COLUMN ann_date DATE")
        await conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_financial_data_ann_date ON financial_data (ann_date)"
        )


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库会话（用于 FastAPI Depends）"""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

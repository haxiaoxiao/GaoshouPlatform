from __future__ import annotations

import hashlib
from datetime import date

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.backtest.api import _save_backtest_result
from app.backtest.config import BacktestConfig
from app.db.models.base import Base
from app.db.models.strategy import Backtest, Strategy


@pytest.mark.asyncio
async def test_saved_backtest_contains_release_snapshot_and_metric_schema_v2(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        strategy = Strategy(name="TSMF", code="def init(context): pass", parameters={})
        session.add(strategy)
        await session.commit()
        strategy_id = strategy.id

    monkeypatch.setattr("app.backtest.api.async_session_factory", sessions)
    config = BacktestConfig(
        strategy_id=strategy_id,
        start_date=date(2025, 1, 2),
        end_date=date(2025, 12, 31),
        engine="akquant",
        commission_rate=0.0003,
        slippage=0.001,
    )
    config.release_id = "release-1"
    config.data_snapshot_id = "snapshot-1"

    await _save_backtest_result(
        "run-1",
        config,
        {"total_return": 0.12, "max_drawdown": -0.2, "warnings": ["stale factor"]},
        success=True,
    )

    async with sessions() as session:
        saved = (await session.execute(select(Backtest))).scalar_one()
    await engine.dispose()

    assert saved.run_id == "run-1"
    assert saved.release_id == "release-1"
    assert saved.data_snapshot_id == "snapshot-1"
    assert saved.engine == "akquant"
    assert saved.result_schema_version == 2
    assert saved.code_hash == hashlib.sha256(b"def init(context): pass").hexdigest()
    assert saved.warnings == ["stale factor"]
    assert saved.result["max_drawdown"] == 0.2
    assert saved.result["result_schema_version"] == 2

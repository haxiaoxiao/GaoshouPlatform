from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.db.models.base import Base
from app.db.models.strategy import Strategy
from app.services.research_lineage import ResearchLineageService


@pytest.mark.asyncio
async def test_release_promotion_requires_ordered_valid_evidence():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async with sessions() as session:
        strategy = Strategy(name="TSMF", code="class Strategy: pass", parameters={})
        session.add(strategy)
        await session.flush()
        service = ResearchLineageService(session)
        snapshot = await service.create_data_snapshot(
            environment="research",
            dataset_versions={"klines_daily": "2026-07-09"},
            freshness={"klines_daily": "ready"},
            status="ready",
        )
        release = await service.create_strategy_release(
            strategy_id=strategy.id,
            data_snapshot_id=snapshot.id,
            engine="akquant",
            engine_version="0.2.40",
            git_commit="abc123",
            parameters={"rebalance_days": 5},
            universe={"mode": "all_a"},
            cost_model={"commission_rate": 0.0003},
            factor_params_hashes={"market_cap": "empty"},
        )

        with pytest.raises(ValueError, match="validation artifact"):
            await service.promote_release(release.id, "validated")

        await service.add_artifact(
            release_id=release.id,
            kind="validation",
            validation_status="valid",
            start_date=date(2020, 1, 2),
            end_date=date(2026, 7, 9),
            metrics={"pit_checks": "passed"},
        )
        with pytest.raises(ValueError, match="required checks"):
            await service.promote_release(release.id, "validated")

        await service.add_artifact(
            release_id=release.id,
            kind="validation",
            validation_status="valid",
            start_date=date(2020, 1, 2),
            end_date=date(2026, 7, 9),
            metrics={
                "data_integrity": True,
                "point_in_time": True,
                "no_lookahead": True,
                "execution_consistency": True,
            },
        )
        assert (await service.promote_release(release.id, "validated")).status == "validated"

        await service.add_artifact(
            release_id=release.id,
            kind="backtest",
            validation_status="valid",
            start_date=date(2020, 1, 2),
            end_date=date(2026, 7, 9),
            metrics={"result_schema_version": 2},
        )
        assert (await service.promote_release(release.id, "paper_approved")).status == "paper_approved"

        with pytest.raises(ValueError, match="paper shadow"):
            await service.promote_release(release.id, "live_approved")

        await service.add_artifact(
            release_id=release.id,
            kind="paper_shadow",
            validation_status="valid",
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 7),
            metrics={
                "trading_days": 5,
                "reconciliation_complete": True,
                "guardrail_violations": 0,
                "duplicate_orders": 0,
            },
        )
        approved = await service.promote_release(release.id, "live_approved")

        assert approved.status == "live_approved"
        assert approved.code_hash
        assert approved.data_snapshot_id == snapshot.id

    await engine.dispose()


@pytest.mark.asyncio
async def test_release_promotion_rejects_incomplete_paper_shadow():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session:
        strategy = Strategy(name="TSMF", code="pass", parameters={})
        session.add(strategy)
        await session.flush()
        service = ResearchLineageService(session)
        snapshot = await service.create_data_snapshot(
            environment="research", dataset_versions={}, freshness={}, status="ready"
        )
        release = await service.create_strategy_release(
            strategy_id=strategy.id,
            data_snapshot_id=snapshot.id,
            engine="akquant",
            engine_version="0.2.40",
            git_commit="abc123",
            parameters={},
            universe={},
            cost_model={},
            factor_params_hashes={},
        )
        release.status = "paper_approved"
        await service.add_artifact(
            release_id=release.id,
            kind="paper_shadow",
            validation_status="valid",
            metrics={
                "trading_days": 4,
                "reconciliation_complete": True,
                "guardrail_violations": 0,
                "duplicate_orders": 0,
            },
        )

        with pytest.raises(ValueError, match="at least 5 trading days"):
            await service.promote_release(release.id, "live_approved")

    await engine.dispose()

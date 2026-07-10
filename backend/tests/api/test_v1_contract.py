from __future__ import annotations

import hashlib
from datetime import datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import settings
from app.db.models.base import Base
from app.db.models.research_lineage import DataSnapshot, JobEvent, StrategyRelease
from app.db.models.strategy import Strategy
from app.db.sqlite import get_async_session
from app.main import app


def test_v1_public_routes_are_registered():
    schema = app.openapi()

    expected = {
        "/api/v1/readiness": "get",
        "/api/v1/data-snapshots": "post",
        "/api/v1/backtests": "post",
        "/api/v1/jobs/{job_id}": "get",
        "/api/v1/strategy-releases": "post",
        "/api/v1/strategy-releases/{release_id}/artifacts": "post",
        "/api/v1/strategy-releases/{release_id}/promote": "post",
        "/api/v1/live/preflight": "post",
        "/api/v1/live/orders/submit": "post",
    }
    for path, method in expected.items():
        assert path in schema["paths"]
        assert method in schema["paths"][path]

    request_schema = schema["paths"]["/api/v1/backtests"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    component_name = request_schema["$ref"].rsplit("/", 1)[-1]
    required = set(schema["components"]["schemas"][component_name]["required"])
    assert {"release_id", "data_snapshot_id"} <= required


@pytest.mark.asyncio
async def test_v1_live_submit_requires_approved_release_and_is_idempotent(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as seed:
        strategy = Strategy(name="TSMF", code="pass", parameters={})
        seed.add(strategy)
        await seed.flush()
        snapshot = DataSnapshot(
            id="snapshot-1",
            environment="research",
            dataset_versions={},
            freshness={},
            schema_hash="hash",
            status="ready",
        )
        release = StrategyRelease(
            id="release-1",
            strategy_id=strategy.id,
            data_snapshot_id=snapshot.id,
            code_hash="code",
            git_commit="commit",
            engine="akquant",
            engine_version="0.2.40",
            parameters={},
            universe={},
            cost_model={},
            factor_params_hashes={},
            status="paper_approved",
        )
        seed.add_all([snapshot, release])
        await seed.commit()
        strategy_id = strategy.id

    async def session_override():
        async with sessions() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def fake_qmt_status():
        return {"account_id": "66***80"}

    async def fake_submit(*_args, **_kwargs):
        return {"submitted": True, "run_id": "run-1"}

    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr("app.api.v1.qmt_trading_service.status", fake_qmt_status)
    monkeypatch.setattr("app.api.v1.live_trading_service.submit_orders", fake_submit)
    monkeypatch.setattr("app.api.v1.live_control_sessions.validate", lambda **_kwargs: None)
    app.dependency_overrides[get_async_session] = session_override
    payload = {
        "mode": "live",
        "orders": [{"strategy_id": strategy_id, "symbol": "600519.SH"}],
        "confirm": True,
        "release_id": "release-1",
        "idempotency_key": "same-command",
        "expected_account_mask": "66***80",
    }
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            blocked = await client.post(
                "/api/v1/live/orders/submit",
                json=payload,
                headers={"X-Gaoshou-Control-Token": "token"},
            )
            assert blocked.status_code == 403

            async with sessions() as session:
                approved = await session.get(StrategyRelease, "release-1")
                approved.status = "live_approved"
                approved.approved_at = datetime.now()
                await session.commit()

            submitted = await client.post(
                "/api/v1/live/orders/submit",
                json=payload,
                headers={"X-Gaoshou-Control-Token": "token"},
            )
            duplicate = await client.post(
                "/api/v1/live/orders/submit",
                json=payload,
                headers={"X-Gaoshou-Control-Token": "token"},
            )
            async with sessions() as session:
                event_count = await session.scalar(select(func.count()).select_from(JobEvent))
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert submitted.status_code == 200
    assert submitted.json()["submitted"] is True
    assert duplicate.status_code == 409
    assert event_count == 2


@pytest.mark.asyncio
async def test_legacy_api_returns_deprecation_and_request_id_headers():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            "/api/backtest/engines",
            headers={"X-Request-ID": "review-contract-1"},
        )

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "review-contract-1"
    assert response.headers["Deprecation"] == "true"
    assert response.headers["Sunset"]
    assert response.headers["Link"] == '</api/v1>; rel="successor-version"'


@pytest.mark.asyncio
async def test_v1_backtest_uses_release_configuration_and_blocks_code_drift(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    code = "def init(context): pass"
    async with sessions() as seed:
        strategy = Strategy(name="TSMF", code=code, parameters={})
        seed.add(strategy)
        await seed.flush()
        snapshot = DataSnapshot(
            id="snapshot-backtest",
            environment="research",
            dataset_versions={},
            freshness={},
            schema_hash="snapshot-hash",
            status="ready",
        )
        release = StrategyRelease(
            id="release-backtest",
            strategy_id=strategy.id,
            data_snapshot_id=snapshot.id,
            code_hash=hashlib.sha256(code.encode()).hexdigest(),
            git_commit="commit",
            engine="akquant",
            engine_version="0.2.40",
            parameters={"top_n": 8},
            universe={"universe_mode": "index", "index_symbol": "399101.SZ"},
            cost_model={"commission_rate": 0.0002, "slippage": 0.0005},
            factor_params_hashes={},
            status="validated",
        )
        seed.add_all([snapshot, release])
        await seed.commit()
        strategy_id = strategy.id

    async def session_override():
        async with sessions() as session:
            yield session

    captured = {}

    async def fake_run_backtest(request):
        captured.update(request.model_dump())
        return {"task_id": "run-1"}

    monkeypatch.setattr("app.api.v1.run_backtest", fake_run_backtest)
    app.dependency_overrides[get_async_session] = session_override
    payload = {
        "start_date": "2025-01-02",
        "end_date": "2025-12-31",
        "engine": "akquant",
        "release_id": "release-backtest",
        "data_snapshot_id": "snapshot-backtest",
        "strategy_code": "malicious override",
        "commission_rate": 0.9,
    }
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/api/v1/backtests", json=payload)
            async with sessions() as session:
                current = await session.get(Strategy, strategy_id)
                current.code = "changed after release"
                await session.commit()
            drifted = await client.post("/api/v1/backtests", json=payload)
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    assert captured["strategy_code"] == code
    assert captured["strategy_params"] == {"top_n": 8}
    assert captured["universe_mode"] == "index"
    assert captured["index_symbol"] == "399101.SZ"
    assert captured["commission_rate"] == 0.0002
    assert captured["slippage"] == 0.0005
    assert drifted.status_code == 409

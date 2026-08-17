from __future__ import annotations

import hashlib
from datetime import datetime

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import v1 as v1_api
from app.core.config import settings
from app.db.models.base import Base
from app.db.models.live_trading import LiveStrategyProfile
from app.db.models.research_lineage import DataSnapshot, JobEvent, StrategyRelease
from app.db.models.strategy import Strategy
from app.db.sqlite import get_async_session
from app.main import app
from app.services.live_control import LiveControlSessionManager
from app.services.live_trading import live_trading_service


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
async def test_legacy_live_submit_is_gone_without_calling_service(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    async def fake_submit(*args, **kwargs):
        calls.append((args, kwargs))
        return {"submitted": True}

    monkeypatch.setattr("app.api.live_trading.live_trading_service.submit_orders", fake_submit)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/live-trading/orders/submit",
            json={
                "mode": "live",
                "orders": [{"symbol": "600519.SH"}],
                "confirm": True,
                "live_authorization": "request-forgery",
            },
        )

    assert response.status_code == 410
    assert response.json()["detail"] == (
        "Live order submission moved to /api/v1/live/orders/submit"
    )
    assert calls == []


@pytest.mark.asyncio
async def test_legacy_paper_submit_remains_compatible(monkeypatch):
    calls: list[tuple[tuple, dict]] = []

    async def fake_submit(*args, **kwargs):
        calls.append((args, kwargs))
        return {"submitted": True, "mode": "paper"}

    monkeypatch.setattr("app.api.live_trading.live_trading_service.submit_orders", fake_submit)
    orders = [{"symbol": "600519.SH", "side": "BUY", "quantity": 100}]

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/live-trading/orders/submit",
            json={"mode": "paper", "orders": orders, "confirm": True},
        )

    assert response.status_code == 200
    assert response.json() == {
        "code": 0,
        "data": {"submitted": True, "mode": "paper"},
    }
    assert calls == [
        (
            (orders,),
            {"mode": "paper", "confirm": True, "trigger_source": "manual"},
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger_source", ["manual", "auto"])
async def test_live_trading_service_rejects_live_submit_without_internal_authorization(
    monkeypatch,
    trigger_source,
):
    profile_loaded = False

    async def fail_if_profile_is_loaded(_profile_key):
        nonlocal profile_loaded
        profile_loaded = True
        raise AssertionError("live authorization must be checked before loading a profile")

    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fail_if_profile_is_loaded)

    with pytest.raises(PermissionError, match=r"/api/v1/live/orders/submit"):
        await live_trading_service.submit_orders(
            [{"profile_key": "tsmf", "symbol": "600519.SH"}],
            mode="live",
            confirm=True,
            trigger_source=trigger_source,
        )

    assert profile_loaded is False


@pytest.mark.asyncio
async def test_live_trading_service_rejects_forged_live_authorization(monkeypatch):
    async def fail_if_profile_is_loaded(_profile_key):
        raise AssertionError("live authorization must be checked before loading a profile")

    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fail_if_profile_is_loaded)

    with pytest.raises(PermissionError, match=r"/api/v1/live/orders/submit"):
        await live_trading_service.submit_orders(
            [{"profile_key": "tsmf", "symbol": "600519.SH"}],
            mode="live",
            confirm=True,
            live_authorization=object(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token_kind", "expected_account_mask", "expected_detail"),
    [
        ("invalid", "66***80", "Invalid live control session"),
        ("valid", "11***22", "Live account mask does not match"),
    ],
)
async def test_v1_live_submit_control_failure_blocks_authorization_and_service(
    monkeypatch,
    token_kind,
    expected_account_mask,
    expected_detail,
):
    release = StrategyRelease(id="release-control", strategy_id=7, status="live_approved")

    class ReleaseOnlySession:
        async def get(self, model, identifier):
            assert model is StrategyRelease
            assert identifier == release.id
            return release

    manager = LiveControlSessionManager(secret="control-secret", ttl_seconds=60)
    control_session = manager.unlock(
        secret="control-secret",
        expected_account_mask="66***80",
        actual_account_mask="66***80",
    )

    async def fake_qmt_status():
        return {"account_id": "66***80"}

    issue_calls: list[dict] = []
    service_calls: list[dict] = []

    def fake_issue(**kwargs):
        issue_calls.append(kwargs)
        return object()

    async def fake_submit(*_args, **kwargs):
        service_calls.append(kwargs)
        return {"submitted": True}

    monkeypatch.setattr(v1_api, "live_control_sessions", manager)
    monkeypatch.setattr(manager, "issue_submission_authorization", fake_issue)
    monkeypatch.setattr(v1_api, "live_trading_service", type("Service", (), {"submit_orders": fake_submit})())
    monkeypatch.setattr(v1_api.qmt_trading_service, "status", fake_qmt_status)
    request = v1_api.V1LiveSubmitRequest(
        mode="live",
        orders=[
            {
                "strategy_id": release.strategy_id,
                "profile_key": "stable-profile",
                "symbol": "600519.SH",
                "side": "BUY",
                "quantity": 100,
            }
        ],
        confirm=True,
        release_id=release.id,
        idempotency_key="control-failure",
        expected_account_mask=expected_account_mask,
    )
    token = control_session.token if token_kind == "valid" else "invalid-token"

    with pytest.raises(HTTPException) as exc_info:
        await v1_api.v1_submit_live_orders(
            request,
            x_gaoshou_control_token=token,
            session=ReleaseOnlySession(),
        )

    assert exc_info.value.status_code == 403
    assert exc_info.value.detail == expected_detail
    assert issue_calls == []
    assert service_calls == []


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
        profile = LiveStrategyProfile(
            strategy_id=strategy.id,
            profile_key="stable-profile",
            display_name="Stable Profile",
            execution_policy={"allow_manual_submit": True, "allow_live_submit": True},
        )
        seed.add_all([snapshot, release, profile])
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

    manager = LiveControlSessionManager(secret="control-secret", ttl_seconds=60)
    control_session = manager.unlock(
        secret="control-secret",
        expected_account_mask="66***80",
        actual_account_mask="66***80",
    )
    issue_calls: list[tuple[dict, object]] = []
    service_authorizations: list[object | None] = []

    def fake_issue(**kwargs):
        authorization = object()
        issue_calls.append((kwargs, authorization))
        return authorization

    async def fake_submit(*_args, **kwargs):
        if kwargs.get("mode") == "live":
            service_authorizations.append(kwargs.get("live_authorization"))
        return {"submitted": True, "run_id": "run-1"}

    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr("app.api.v1.qmt_trading_service.status", fake_qmt_status)
    monkeypatch.setattr("app.api.v1.live_trading_service.submit_orders", fake_submit)
    monkeypatch.setattr(v1_api, "live_control_sessions", manager)
    monkeypatch.setattr(manager, "issue_submission_authorization", fake_issue)
    monkeypatch.setattr(v1_api, "async_session_factory", sessions)
    app.dependency_overrides[get_async_session] = session_override
    payload = {
        "mode": "live",
        "orders": [
            {
                "strategy_id": strategy_id,
                "profile_key": "stable-profile",
                "symbol": "600519.SH",
                "side": "BUY",
                "quantity": 100,
            }
        ],
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
                headers={"X-Gaoshou-Control-Token": control_session.token},
            )
            assert blocked.status_code == 403
            assert issue_calls == []
            assert service_authorizations == []

            async with sessions() as session:
                approved = await session.get(StrategyRelease, "release-1")
                approved.status = "live_approved"
                approved.approved_at = datetime.now()
                await session.commit()

            submitted = await client.post(
                "/api/v1/live/orders/submit",
                json=payload,
                headers={"X-Gaoshou-Control-Token": control_session.token},
            )
            duplicate = await client.post(
                "/api/v1/live/orders/submit",
                json=payload,
                headers={"X-Gaoshou-Control-Token": control_session.token},
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
    assert len(issue_calls) == 1
    assert len(service_authorizations) == 1
    issue_kwargs, authorization = issue_calls[0]
    assert issue_kwargs["control_session"] is control_session
    assert issue_kwargs["release_id"] == "release-1"
    assert issue_kwargs["strategy_id"] == strategy_id
    assert issue_kwargs["profile_key"] == "stable-profile"
    assert issue_kwargs["account_mask"] == "66***80"
    assert issue_kwargs["idempotency_hash"] == hashlib.sha256(b"same-command").hexdigest()[:48]
    assert issue_kwargs["reservation_id"] == (
        f"live-submit:{hashlib.sha256(b'same-command').hexdigest()[:48]}"
    )
    assert service_authorizations[0] is authorization


@pytest.mark.asyncio
async def test_active_non_v1_api_returns_request_id_without_false_sunset_headers():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            "/api/backtest/engines",
            headers={"X-Request-ID": "review-contract-1"},
        )

    assert response.status_code == 200
    assert response.headers["X-Request-ID"] == "review-contract-1"
    assert "Deprecation" not in response.headers
    assert "Sunset" not in response.headers
    assert "Link" not in response.headers


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

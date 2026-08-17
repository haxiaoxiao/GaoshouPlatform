from __future__ import annotations

import hashlib
from datetime import datetime

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import v1 as v1_api
from app.db.models.base import Base
from app.db.models.live_trading import LiveStrategyProfile
from app.db.models.research_lineage import DataSnapshot, JobEvent, PersistentJob, StrategyRelease
from app.db.models.strategy import Strategy
from app.db.sqlite import async_session_factory
from app.main import app
from app.services.live_control import LiveControlSessionManager


def _valid_payload(*, strategy_id: int = 41, idempotency_key: str = "durable-command") -> dict:
    return {
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
        "release_id": "release-live-safety",
        "idempotency_key": idempotency_key,
        "expected_account_mask": "66***80",
    }


async def _create_sessions(tmp_path):
    database = tmp_path / "v1-live-safety.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database.as_posix()}",
        connect_args={"timeout": 0.05},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


async def _seed_live_release(sessions, *, strategy_id: int = 41) -> None:
    async with sessions() as session:
        strategy = Strategy(id=strategy_id, name="Live Safety", code="pass", parameters={})
        snapshot = DataSnapshot(
            id="snapshot-live-safety",
            environment="research",
            dataset_versions={},
            freshness={},
            schema_hash="snapshot-hash",
            status="ready",
        )
        release = StrategyRelease(
            id="release-live-safety",
            strategy_id=strategy_id,
            data_snapshot_id=snapshot.id,
            code_hash="code-hash",
            git_commit="commit",
            engine="akquant",
            engine_version="0.2.40",
            parameters={},
            universe={},
            cost_model={},
            factor_params_hashes={},
            status="live_approved",
            approved_at=datetime.now(),
        )
        profile = LiveStrategyProfile(
            strategy_id=strategy_id,
            profile_key="stable-profile",
            display_name="Stable profile",
            enabled=True,
            execution_policy={"allow_manual_submit": True, "allow_live_submit": True},
        )
        session.add_all([strategy, snapshot, release, profile])
        await session.commit()


def _install_control_manager(monkeypatch):
    manager = LiveControlSessionManager(secret="control-secret", ttl_seconds=60)
    control = manager.unlock(
        secret="control-secret",
        expected_account_mask="66***80",
        actual_account_mask="66***80",
    )
    monkeypatch.setattr(v1_api, "live_control_sessions", manager)
    return manager, control.token


def _install_session_override(monkeypatch, sessions) -> None:
    async def session_override():
        async with sessions() as session:
            try:
                yield session
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    app.dependency_overrides[v1_api.get_async_session] = session_override
    monkeypatch.setattr(v1_api, "async_session_factory", sessions, raising=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("orders", "expected_status"),
    [
        ([], 422),
        ([{"profile_key": "stable-profile", "symbol": "600519.SH", "side": "BUY", "quantity": 100}], 422),
        (
            [
                {
                    "strategy_id": "not-a-number",
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 0,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": True,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": 123,
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "   ",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "not-a-symbol",
                    "side": "BUY",
                    "quantity": 100,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "HOLD",
                    "quantity": 100,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 0,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": True,
                }
            ],
            422,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                },
                {
                    "strategy_id": 41,
                    "profile_key": "other-profile",
                    "symbol": "000001.SZ",
                    "side": "SELL",
                    "quantity": 100,
                },
            ],
            400,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                    "signal_hash": "signal-a",
                },
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "000001.SZ",
                    "side": "SELL",
                    "quantity": 100,
                    "signal_hash": "signal-b",
                },
            ],
            400,
        ),
        (
            [
                {
                    "strategy_id": 41,
                    "profile_key": "stable-profile",
                    "symbol": "600519.SH",
                    "side": "BUY",
                    "quantity": 100,
                },
                {
                    "strategy_id": 42,
                    "profile_key": "stable-profile",
                    "symbol": "000001.SZ",
                    "side": "SELL",
                    "quantity": 100,
                },
            ],
            400,
        ),
    ],
)
async def test_v1_rejects_invalid_or_mixed_orders_before_reservation(orders, expected_status):
    payload = _valid_payload()
    payload["orders"] = orders

    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/v1/live/orders/submit",
            json=payload,
            headers={"X-Gaoshou-Control-Token": "unused"},
        )

    async with async_session_factory() as session:
        job_count = await session.scalar(select(func.count()).select_from(PersistentJob))
    assert response.status_code == expected_status
    assert job_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("confirm", 1),
        ("idempotency_key", "   "),
        ("expected_account_mask", "   "),
    ],
)
async def test_v1_rejects_non_strict_or_blank_live_commands_before_reservation(field, value):
    payload = _valid_payload()
    payload[field] = value

    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(
            "/api/v1/live/orders/submit",
            json=payload,
            headers={"X-Gaoshou-Control-Token": "unused"},
        )

    async with async_session_factory() as session:
        job_count = await session.scalar(select(func.count()).select_from(PersistentJob))
    assert response.status_code == 422
    assert job_count == 0


@pytest.mark.asyncio
async def test_v1_commits_reservation_before_service_writes_audit(monkeypatch, tmp_path):
    engine, sessions = await _create_sessions(tmp_path)
    await _seed_live_release(sessions)
    _manager, token = _install_control_manager(monkeypatch)
    _install_session_override(monkeypatch, sessions)
    idempotency_hash = hashlib.sha256(b"durable-command").hexdigest()[:48]
    job_id = f"live-submit:{idempotency_hash}"

    async def fake_qmt_status():
        return {"account_id": "66***80"}

    async def fake_submit(*_args, **_kwargs):
        async with sessions() as audit_session:
            assert await audit_session.get(PersistentJob, job_id) is not None
            audit_session.add(
                JobEvent(job_id=job_id, event_type="service_audit", data={"writer": "service"})
            )
            await audit_session.commit()
        return {"submitted": True, "run_id": "run-1"}

    monkeypatch.setattr(v1_api.qmt_trading_service, "status", fake_qmt_status)
    monkeypatch.setattr(v1_api.live_trading_service, "submit_orders", fake_submit)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/api/v1/live/orders/submit",
                json=_valid_payload(),
                headers={"X-Gaoshou-Control-Token": token},
            )
        async with sessions() as session:
            job = await session.get(PersistentJob, job_id)
            event_types = list(
                await session.scalars(
                    select(JobEvent.event_type)
                    .where(JobEvent.job_id == job_id)
                    .order_by(JobEvent.id)
                )
            )
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    assert job is not None
    assert job.status == "succeeded"
    assert event_types == ["started", "service_audit", "succeeded"]


@pytest.mark.asyncio
async def test_v1_finalizes_failure_in_short_transaction_after_service_audit(monkeypatch, tmp_path):
    engine, sessions = await _create_sessions(tmp_path)
    await _seed_live_release(sessions)
    _manager, token = _install_control_manager(monkeypatch)
    _install_session_override(monkeypatch, sessions)
    idempotency_hash = hashlib.sha256(b"durable-command").hexdigest()[:48]
    job_id = f"live-submit:{idempotency_hash}"

    async def fake_qmt_status():
        return {"account_id": "66***80"}

    async def failing_submit(*_args, **_kwargs):
        async with sessions() as audit_session:
            assert await audit_session.get(PersistentJob, job_id) is not None
            audit_session.add(
                JobEvent(job_id=job_id, event_type="service_audit", data={"writer": "service"})
            )
            await audit_session.commit()
        raise RuntimeError("mocked service failure")

    monkeypatch.setattr(v1_api.qmt_trading_service, "status", fake_qmt_status)
    monkeypatch.setattr(v1_api.live_trading_service, "submit_orders", failing_submit)
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/api/v1/live/orders/submit",
                json=_valid_payload(),
                headers={"X-Gaoshou-Control-Token": token},
            )
        async with sessions() as session:
            job = await session.get(PersistentJob, job_id)
            event_types = list(
                await session.scalars(
                    select(JobEvent.event_type)
                    .where(JobEvent.job_id == job_id)
                    .order_by(JobEvent.id)
                )
            )
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 502
    assert job is not None
    assert job.status == "failed"
    assert job.error == "mocked service failure"
    assert event_types == ["started", "service_audit", "failed"]


@pytest.mark.asyncio
async def test_v1_crash_after_reservation_cannot_retry_external_submit(monkeypatch, tmp_path):
    engine, sessions = await _create_sessions(tmp_path)
    await _seed_live_release(sessions)
    _manager, token = _install_control_manager(monkeypatch)
    monkeypatch.setattr(v1_api, "async_session_factory", sessions, raising=False)
    idempotency_hash = hashlib.sha256(b"crash-command").hexdigest()[:48]
    job_id = f"live-submit:{idempotency_hash}"
    calls = 0

    class SimulatedProcessCrash(BaseException):
        pass

    async def fake_qmt_status():
        return {"account_id": "66***80"}

    async def fake_submit(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise SimulatedProcessCrash
        return {"submitted": True, "run_id": "run-2"}

    monkeypatch.setattr(v1_api.qmt_trading_service, "status", fake_qmt_status)
    monkeypatch.setattr(v1_api.live_trading_service, "submit_orders", fake_submit)
    request = v1_api.V1LiveSubmitRequest(**_valid_payload(idempotency_key="crash-command"))
    try:
        async with sessions() as request_session:
            with pytest.raises(SimulatedProcessCrash):
                await v1_api.v1_submit_live_orders(
                    request,
                    x_gaoshou_control_token=token,
                    session=request_session,
                )
        async with sessions() as session:
            reserved_job = await session.get(PersistentJob, job_id)

        async with sessions() as retry_session:
            with pytest.raises(HTTPException) as exc_info:
                await v1_api.v1_submit_live_orders(
                    request,
                    x_gaoshou_control_token=token,
                    session=retry_session,
                )
    finally:
        await engine.dispose()

    assert reserved_job is not None
    assert reserved_job.status == "running"
    assert exc_info.value.status_code == 409
    assert calls == 1

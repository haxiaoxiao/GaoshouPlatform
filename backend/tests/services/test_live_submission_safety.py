from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest

import app.services.live_trading as live_trading_module
from app.core.config import settings
from app.db.models.live_trading import LiveStrategyProfile
from app.db.models.research_lineage import DataSnapshot, PersistentJob, StrategyRelease
from app.db.models.strategy import Strategy
from app.db.sqlite import async_session_factory
from app.services.live_control import LiveControlSessionManager
from app.services.live_trading import StrategyProfileBundle, live_trading_service


def _bundle(*, profile_key: str = "stable-profile", strategy_id: int = 43) -> StrategyProfileBundle:
    profile = LiveStrategyProfile(
        id=strategy_id,
        strategy_id=strategy_id,
        profile_key=profile_key,
        display_name="Stable Profile",
        description=None,
        enabled=True,
        is_default=True,
        adapter_type="multi_factor_cash_aware",
        params_override={},
        universe_config={},
        execution_policy={"allow_manual_submit": True, "allow_live_submit": True},
    )
    strategy = Strategy(
        id=strategy_id,
        name="Stable Strategy",
        code="FACTOR_CONFIGS = []\nFILTER_FACTORS = []\n",
        parameters={},
        description=None,
    )
    return StrategyProfileBundle(profile=profile, strategy=strategy, constants={}, params={})


def _authorization(
    *,
    strategy_id: int = 43,
    profile_key: str = "stable-profile",
    account_mask: str = "66***80",
):
    manager = LiveControlSessionManager(secret="control-secret", ttl_seconds=60)
    control = manager.unlock(
        secret="control-secret",
        expected_account_mask=account_mask,
        actual_account_mask=account_mask,
    )
    authorization = manager.issue_submission_authorization(
        control_session=control,
        release_id="release-43",
        strategy_id=strategy_id,
        profile_key=profile_key,
        account_mask=account_mask,
        idempotency_hash="command-43",
        reservation_id="live-submit:command-43",
    )
    return manager, authorization


def _order(*, strategy_id: int = 43, profile_key: str = "stable-profile") -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "profile_key": profile_key,
        "symbol": "600519.SH",
        "side": "BUY",
        "quantity": 100,
        "reference_price": 100.0,
    }


async def _allow_scope(**_kwargs):
    return {"ok": True}


async def _no_op(*_args, **_kwargs):
    return None


async def _seed_reservation(
    *,
    payload_override: dict[str, Any] | None = None,
    release_status: str = "live_approved",
) -> None:
    payload = {
        "release_id": "release-43",
        "strategy_id": 43,
        "profile_key": "stable-profile",
        "account_mask": "66***80",
        "idempotency_hash": "command-43",
        "order_count": 1,
    }
    payload.update(payload_override or {})
    async with async_session_factory() as session:
        session.add_all(
            [
                Strategy(id=43, name="Stable Strategy", code="pass", parameters={}),
                DataSnapshot(
                    id="snapshot-43",
                    environment="research",
                    dataset_versions={},
                    freshness={},
                    schema_hash="snapshot-hash",
                    status="ready",
                ),
                StrategyRelease(
                    id="release-43",
                    strategy_id=43,
                    data_snapshot_id="snapshot-43",
                    code_hash="code-hash",
                    git_commit="commit",
                    engine="akquant",
                    engine_version="0.2.40",
                    parameters={},
                    universe={},
                    cost_model={},
                    factor_params_hashes={},
                    status=release_status,
                    approved_at=datetime.now() if release_status == "live_approved" else None,
                ),
            ]
        )
        session.add(
            PersistentJob(
                id="live-submit:command-43",
                kind="live_order_submit",
                title="Live order submit release-43",
                status="running",
                progress=0.0,
                payload=payload,
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_real_live_submission_authorization_is_consumed_once(monkeypatch):
    manager, authorization = _authorization()
    bundle = _bundle()
    await _seed_reservation()

    async def fake_load_profile_bundle(_profile_key):
        return bundle

    async def fake_status():
        return {
            "account_id": "66***80",
            "account_configured": True,
            "xttrader_available": True,
            "quote_connected": True,
        }

    async def fake_submit_batch(*_args, **_kwargs):
        return [{"submitted": True}]

    monkeypatch.setattr(live_trading_module, "live_control_sessions", manager, raising=False)
    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fake_load_profile_bundle)
    monkeypatch.setattr(live_trading_service, "_validate_strategy_account_orders", _allow_scope)
    monkeypatch.setattr(live_trading_service, "_attach_stock_names", _no_op)
    monkeypatch.setattr(live_trading_service, "_submit_live_order_batch", fake_submit_batch)
    monkeypatch.setattr(live_trading_module.qmt_trading_service, "status", fake_status)

    result = await live_trading_service.submit_orders(
        [_order()],
        mode="live",
        confirm=True,
        run_id="live-submit:command-43",
        live_authorization=authorization,
    )

    assert result["submitted"] is True
    with pytest.raises(PermissionError, match="already consumed"):
        await live_trading_service.submit_orders(
            [_order()],
            mode="live",
            confirm=True,
            run_id="live-submit:command-43",
            live_authorization=authorization,
        )


@pytest.mark.asyncio
async def test_live_submission_rejects_missing_strategy_before_profile_load(monkeypatch):
    manager, authorization = _authorization()
    profile_loaded = False

    async def fail_if_profile_loaded(_profile_key):
        nonlocal profile_loaded
        profile_loaded = True
        raise AssertionError("missing strategy must be rejected first")

    monkeypatch.setattr(live_trading_module, "live_control_sessions", manager, raising=False)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fail_if_profile_loaded)
    order = _order()
    order.pop("strategy_id")

    with pytest.raises(PermissionError, match="strategy_id"):
        await live_trading_service.submit_orders(
            [order],
            mode="live",
            confirm=True,
            run_id="live-submit:command-43",
            live_authorization=authorization,
        )

    assert profile_loaded is False


@pytest.mark.asyncio
async def test_live_submission_requires_current_live_approved_release(monkeypatch):
    manager, authorization = _authorization()
    await _seed_reservation(release_status="paper_approved")
    profile_loaded = False

    async def fail_if_profile_loaded(_profile_key):
        nonlocal profile_loaded
        profile_loaded = True
        raise AssertionError("release approval must be checked before profile load")

    monkeypatch.setattr(live_trading_module, "live_control_sessions", manager, raising=False)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fail_if_profile_loaded)

    with pytest.raises(PermissionError, match="live_approved"):
        await live_trading_service.submit_orders(
            [_order()],
            mode="live",
            confirm=True,
            run_id="live-submit:command-43",
            live_authorization=authorization,
        )

    assert profile_loaded is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("loaded_profile", "loaded_strategy", "expected_message"),
    [
        ("other-profile", 43, "profile"),
        ("stable-profile", 44, "strategy"),
    ],
)
async def test_live_submission_context_must_match_loaded_profile(
    monkeypatch,
    loaded_profile,
    loaded_strategy,
    expected_message,
):
    manager, authorization = _authorization()
    await _seed_reservation()

    async def fake_load_profile_bundle(_profile_key):
        return _bundle(profile_key=loaded_profile, strategy_id=loaded_strategy)

    monkeypatch.setattr(live_trading_module, "live_control_sessions", manager, raising=False)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fake_load_profile_bundle)

    with pytest.raises(PermissionError, match=expected_message):
        await live_trading_service.submit_orders(
            [_order()],
            mode="live",
            confirm=True,
            run_id="live-submit:command-43",
            live_authorization=authorization,
        )


@pytest.mark.asyncio
async def test_live_submission_rechecks_account_before_broker_submit(monkeypatch):
    manager, authorization = _authorization()
    await _seed_reservation()
    submitted = False

    async def fake_load_profile_bundle(_profile_key):
        return _bundle()

    async def switched_account_status():
        return {
            "account_id": "11***22",
            "account_configured": True,
            "xttrader_available": True,
            "quote_connected": True,
        }

    async def fail_if_submitted(*_args, **_kwargs):
        nonlocal submitted
        submitted = True
        raise AssertionError("account switch must block broker submission")

    monkeypatch.setattr(live_trading_module, "live_control_sessions", manager, raising=False)
    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fake_load_profile_bundle)
    monkeypatch.setattr(live_trading_service, "_validate_strategy_account_orders", _allow_scope)
    monkeypatch.setattr(live_trading_service, "_attach_stock_names", _no_op)
    monkeypatch.setattr(live_trading_service, "_submit_live_order_batch", fail_if_submitted)
    monkeypatch.setattr(live_trading_module.qmt_trading_service, "status", switched_account_status)

    with pytest.raises(PermissionError, match="account"):
        await live_trading_service.submit_orders(
            [_order()],
            mode="live",
            confirm=True,
            run_id="live-submit:command-43",
            live_authorization=authorization,
        )

    assert submitted is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload_override", "expected_message"),
    [
        ({"release_id": "other-release"}, "release"),
        ({"account_mask": "11***22"}, "account"),
        ({"idempotency_hash": "other-command"}, "idempotency"),
    ],
)
async def test_live_submission_context_must_match_durable_reservation(
    monkeypatch,
    payload_override,
    expected_message,
):
    manager, authorization = _authorization()
    await _seed_reservation(payload_override=payload_override)
    profile_loaded = False

    async def fail_if_profile_loaded(_profile_key):
        nonlocal profile_loaded
        profile_loaded = True
        raise AssertionError("reservation binding must be checked before profile load")

    monkeypatch.setattr(live_trading_module, "live_control_sessions", manager, raising=False)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fail_if_profile_loaded)

    with pytest.raises(PermissionError, match=expected_message):
        await live_trading_service.submit_orders(
            [_order()],
            mode="live",
            confirm=True,
            run_id="live-submit:command-43",
            live_authorization=authorization,
        )

    assert profile_loaded is False


@pytest.mark.asyncio
async def test_live_cancel_is_disabled_before_loading_orders(monkeypatch):
    loaded = False

    async def fail_if_loaded(**_kwargs):
        nonlocal loaded
        loaded = True
        raise AssertionError("live cancellation must be rejected before loading orders")

    monkeypatch.setattr(live_trading_service, "_load_live_trade_rows", fail_if_loaded)

    with pytest.raises(PermissionError, match="disabled"):
        await live_trading_service.cancel_pending_orders(mode="live", confirm=True)

    assert loaded is False


@pytest.mark.asyncio
async def test_live_cancel_resubmit_is_disabled_before_cancel(monkeypatch):
    cancelled = False

    async def fail_if_cancelled(**_kwargs):
        nonlocal cancelled
        cancelled = True
        raise AssertionError("cancel-resubmit must be rejected before cancellation")

    monkeypatch.setattr(live_trading_service, "cancel_pending_orders", fail_if_cancelled)

    with pytest.raises(PermissionError, match="disabled"):
        await live_trading_service.cancel_and_resubmit_pending_orders(
            mode="live",
            confirm_cancel=True,
            confirm_submit=True,
        )

    assert cancelled is False


@pytest.mark.asyncio
async def test_live_runner_is_disabled_before_profile_or_task_creation(monkeypatch):
    profile_loaded = False

    async def fail_if_profile_loaded(_profile_key):
        nonlocal profile_loaded
        profile_loaded = True
        raise AssertionError("live runner must be rejected before profile load")

    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr(settings, "live_trading_auto_execute_enabled", True)
    monkeypatch.setattr(live_trading_service, "_load_profile_bundle", fail_if_profile_loaded)

    with pytest.raises(PermissionError, match="disabled"):
        await live_trading_service.start_runner(
            profile_key="stable-profile",
            mode="live",
            params={},
        )

    assert profile_loaded is False

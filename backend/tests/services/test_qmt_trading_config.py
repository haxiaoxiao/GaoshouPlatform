from __future__ import annotations

from pathlib import Path

import pytest

from app.core.config import settings
from app.services import qmt_trading as qmt_trading_module
from app.services.live_control import LiveControlSessionManager
from app.services.qmt_trading import QmtRuntimeConfig, QmtTradingService


def test_runtime_config_auto_detects_unique_miniqmt_account(monkeypatch, tmp_path: Path):
    trader_path = tmp_path / "userdata_mini"
    user_dir = trader_path / "users" / "demo"
    user_dir.mkdir(parents=True)
    (user_dir / "authAndConfig.xml").write_text(
        '<AccountAuth key="2____10007____8888____49____666629911180____" strategys="" />',
        encoding="utf-8",
    )

    service = QmtTradingService()
    monkeypatch.setattr(settings, "qmt_account_id", "")
    monkeypatch.setattr(settings, "qmt_trader_path", "")
    monkeypatch.setattr(service, "_discover_trader_path", lambda: str(trader_path))

    config = service._runtime_config()

    assert config.account_id == "666629911180"
    assert config.trader_path == str(trader_path)
    assert "auto_account" in config.source
    assert "auto_trader_path" in config.source


def test_runtime_config_explicit_env_wins(monkeypatch, tmp_path: Path):
    trader_path = tmp_path / "userdata_mini"
    (trader_path / "users").mkdir(parents=True)

    service = QmtTradingService()
    monkeypatch.setattr(settings, "qmt_account_id", "12345678")
    monkeypatch.setattr(settings, "qmt_trader_path", str(trader_path))

    config = service._runtime_config()

    assert config.account_id == "12345678"
    assert config.trader_path == str(trader_path)
    assert config.source == "env_account,env_trader_path"


@pytest.mark.asyncio
async def test_submit_order_rejects_direct_broker_access_without_v1_permit(monkeypatch):
    service = QmtTradingService()
    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)

    with pytest.raises(PermissionError, match="only through /api/v1/live/orders/submit"):
        await service.submit_order(
            {
                "symbol": "600000.SH",
                "side": "BUY",
                "quantity": 100,
                "price": 10,
                "confirm": True,
            }
        )


@pytest.mark.asyncio
async def test_submit_order_rechecks_exact_runtime_account_at_broker_boundary(monkeypatch):
    manager = LiveControlSessionManager(secret="control-secret", ttl_seconds=60)
    control = manager.unlock(
        secret="control-secret",
        expected_account_mask="66***80",
        actual_account_mask="66***80",
    )
    authorization = manager.issue_submission_authorization(
        control_session=control,
        release_id="release-1",
        strategy_id=1,
        profile_key="profile-1",
        account_mask="66***80",
        idempotency_hash="hash-1",
        reservation_id="live-submit:hash-1",
    )
    permit = manager.consume_submission_authorization(authorization)
    service = QmtTradingService()
    monkeypatch.setattr(qmt_trading_module, "live_control_sessions", manager)
    monkeypatch.setattr(settings, "live_trading_enable_order_submit", True)
    monkeypatch.setattr(
        service,
        "_runtime_config",
        lambda: QmtRuntimeConfig(
            account_id="11000022",
            account_type="STOCK",
            trader_path="unused",
            source="test",
        ),
    )

    with pytest.raises(PermissionError, match="account changed before broker submission"):
        await service.submit_order(
            {
                "symbol": "600000.SH",
                "side": "BUY",
                "quantity": 100,
                "price": 10,
                "confirm": True,
            },
            broker_permit=permit,
        )

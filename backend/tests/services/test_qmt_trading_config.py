from __future__ import annotations

from pathlib import Path

from app.core.config import settings
from app.services.qmt_trading import QmtTradingService


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

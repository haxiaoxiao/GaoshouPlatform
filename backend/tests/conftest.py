from __future__ import annotations

import pytest

from app.core.config import settings


@pytest.fixture(autouse=True)
def isolate_live_trading_settings(monkeypatch: pytest.MonkeyPatch):
    """Never let tests inherit live-order capability from the prod env file."""
    monkeypatch.setattr(settings, "live_trading_enable_order_submit", False)
    monkeypatch.setattr(settings, "live_trading_auto_execute_enabled", False)

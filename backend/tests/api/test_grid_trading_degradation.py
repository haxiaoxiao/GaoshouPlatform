from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.services.grid_trading import GridAccountSnapshot, grid_trading_service


@pytest.mark.asyncio
async def test_grid_signals_degrade_to_no_quote_when_qmt_unavailable(monkeypatch):
    async def fake_quotes(_symbols):
        raise RuntimeError("xtquant unavailable")

    async def fake_account_snapshot(_manual_account):
        return GridAccountSnapshot(
            cash=100000.0,
            positions={},
            source="manual-test",
            total_asset=100000.0,
            market_value=0.0,
            error="account unavailable",
        )

    monkeypatch.setattr("app.services.grid_trading.qmt_gateway.get_realtime_quotes", fake_quotes)
    monkeypatch.setattr(grid_trading_service, "_account_snapshot", fake_account_snapshot)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/api/grid-trading/signals", json={"params": {}})

    body = resp.json()
    assert resp.status_code == 200
    assert body["code"] == 0
    assert body["data"]["quote_error"].startswith("RuntimeError")
    assert {signal["action"] for signal in body["data"]["signals"]} == {"NO_QUOTE"}
    assert body["data"]["account"]["error"] == "account unavailable"

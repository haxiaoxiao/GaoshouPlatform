from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from app.db.sqlite import get_async_session
from app.main import app
from app.services.data_service import DataService


@pytest.mark.asyncio
async def test_kline_response_exposes_pagination_metadata(monkeypatch):
    async def fake_get_klines(self, **_kwargs):
        return SimpleNamespace(
            items=[
                SimpleNamespace(
                    symbol="600519.SH",
                    datetime=date(2025, 7, 18),
                    open=1400,
                    high=1410,
                    low=1390,
                    close=1405,
                    volume=100,
                    amount=200,
                )
            ],
            total=479,
            page=2,
            page_size=250,
            total_pages=2,
        )

    async def fake_session():
        yield object()

    monkeypatch.setattr(DataService, "get_klines", fake_get_klines)
    app.dependency_overrides[get_async_session] = fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/api/data/klines",
                params={
                    "symbol": "600519.SH",
                    "period": "daily",
                    "page": 2,
                    "page_size": 250,
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "symbol": "600519.SH",
                "trade_date": "2025-07-18",
                "open": 1400.0,
                "high": 1410.0,
                "low": 1390.0,
                "close": 1405.0,
                "volume": 100,
                "amount": 200.0,
            }
        ],
        "total": 479,
        "page": 2,
        "page_size": 250,
        "total_pages": 2,
    }


@pytest.mark.asyncio
async def test_kline_default_page_covers_about_one_trading_year(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_klines(self, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            items=[],
            total=0,
            page=kwargs["page"],
            page_size=kwargs["page_size"],
            total_pages=0,
        )

    async def fake_session():
        yield object()

    monkeypatch.setattr(DataService, "get_klines", fake_get_klines)
    app.dependency_overrides[get_async_session] = fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/api/data/klines",
                params={"symbol": "600519.SH", "period": "daily"},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["page_size"] == 250

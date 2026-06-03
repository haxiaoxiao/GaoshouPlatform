from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pytest
from httpx import ASGITransport, AsyncClient

from app.db.sqlite import get_async_session
from app.main import app


class _ScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeSession:
    def __init__(self, stock, financial):
        self._results = [_ScalarResult(stock), _ScalarResult(financial)]

    async def execute(self, _query):
        return self._results.pop(0)


@pytest.mark.asyncio
async def test_stock_detail_uses_symbol_and_exposes_frontend_fields():
    stock = SimpleNamespace(
        symbol="600519.SH",
        name="贵州茅台",
        exchange="SH",
        industry="食品饮料",
        industry2="白酒",
        industry3="高端白酒",
        sector="主板",
        concept="消费,白酒",
        list_date=date(2001, 8, 27),
        is_st=False,
        is_suspend=False,
        is_delist=False,
        total_mv=21000000.0,
        circ_mv=20500000.0,
        pe_ttm=25.0,
        pb=8.0,
        roe=30.0,
        eps=20.0,
        bvps=80.0,
        revenue_yoy=12.0,
        profit_yoy=14.0,
        gross_margin=91.0,
        total_assets=1000.0,
        total_liability=200.0,
        revenue=500.0,
        net_profit=250.0,
        created_at=datetime(2026, 1, 1, 9, 0, 0),
        updated_at=datetime(2026, 5, 29, 15, 0, 0),
    )
    financial = SimpleNamespace(
        report_date=date(2026, 3, 31),
        ann_date=date(2026, 4, 25),
        pe_ttm=24.0,
        pb=7.8,
        roe=31.0,
        eps=21.0,
        bvps=82.0,
        revenue_yoy=13.0,
        profit_yoy=15.0,
        gross_margin=92.0,
        total_assets=1200.0,
        total_liability=300.0,
        revenue=600.0,
        net_profit=300.0,
    )

    async def fake_session():
        yield _FakeSession(stock, financial)

    app.dependency_overrides[get_async_session] = fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/api/data/stocks/600519.SH")
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    data = body["data"]
    assert data["symbol"] == "600519.SH"
    assert data["market"] == "SH"
    assert data["is_active"] is True
    assert data["market_cap"] == 2100
    assert data["pe_ratio"] == 24.0
    assert data["roe"] == 0.31
    assert data["revenue_growth"] == 0.13
    assert data["debt_ratio"] == 0.25
    assert data["net_margin"] == 0.5
    assert data["latest_ann_date"] == "2026-04-25"
    assert data["updated_at"] == "2026-05-29T15:00:00"

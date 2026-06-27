from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.db.models.financial import FinancialData
from app.db.models.stock import Stock
from app.db.sqlite import get_async_session
from app.main import app


async def _fake_session():
    yield object()


@pytest.mark.asyncio
async def test_data_summary_returns_unified_frontend_contract(monkeypatch):
    async def fake_dataset_coverages(specs):
        return {
            key: {
                "row_count": 100,
                "min_date": "2026-05-01",
                "max_date": "2026-05-29 15:00:00" if spec[1] == "datetime" else "2026-05-29",
                "estimated": key == "market_minute",
            }
            for key, spec in specs.items()
        }

    async def fake_sqlite_summary(_session, *, model, **_kwargs):
        if model is Stock:
            return {"row_count": 3, "max_date": "2026-05-29T10:00:00", "estimated": False}
        if model is FinancialData:
            return {"row_count": 4, "max_date": "2026-03-31", "estimated": False}
        raise AssertionError(f"unexpected model {model}")

    class FakeSentimentService:
        def __init__(self, _session):
            pass

        async def overview(self, _sources):
            return {"total_posts": 9, "latest_published_at": "2026-05-28T09:30:00"}

    monkeypatch.setattr("app.api.system._dataset_coverages", fake_dataset_coverages)
    monkeypatch.setattr("app.api.system._sqlite_summary", fake_sqlite_summary)
    monkeypatch.setattr("app.api.system.SentimentService", FakeSentimentService)
    app.dependency_overrides[get_async_session] = _fake_session
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/api/system/data-summary")
    finally:
        app.dependency_overrides.clear()

    body = resp.json()
    assert resp.status_code == 200
    assert body["overall_status"] in {"good", "degraded"}
    assert {"market_daily", "market_minute", "stocks", "financial", "sentiment"} <= set(body["by_key"])
    assert body["by_key"]["market_minute"]["row_count_estimated"] is True
    assert body["by_key"]["stocks"]["status_text"]

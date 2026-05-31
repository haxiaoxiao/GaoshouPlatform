import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_sentiment_routes_are_registered_and_validate_sources():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/sentiment/summary/600519.SH?sources=reddit")
        overview_resp = await client.get("/api/sentiment/overview?sources=reddit")

    assert resp.status_code == 200
    assert resp.json()["code"] == 1
    assert "unsupported sentiment source" in resp.json()["message"]
    assert overview_resp.status_code == 200
    assert overview_resp.json()["code"] == 1
    assert "unsupported sentiment source" in overview_resp.json()["message"]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_accepts_merged_sources(monkeypatch):
    async def fake_run_many(self, symbol: str | None, **kwargs):
        return {
            "symbol": symbol,
            "requested_sources": kwargs["sources"],
            "results": [],
            "all_succeeded": True,
            "succeeded_sources": kwargs["sources"],
            "failed_sources": [],
            "total_upserted": 0,
            "total_collected": 0,
            "total_matched": 0,
        }

    monkeypatch.setattr("app.api.sentiment.SentimentIngestService.run_many", fake_run_many)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={
                "symbol": "600519.SH",
                "sources": ["xueqiu", "nga"],
                "max_pages": 2,
                "min_reply": 10,
            },
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert resp.json()["data"]["requested_sources"] == ["xueqiu_spyder", "flocktrader"]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_allows_flocktrader_without_symbol(monkeypatch):
    async def fake_run_many(self, symbol: str | None, **kwargs):
        assert symbol is None
        return {
            "symbol": symbol,
            "requested_sources": kwargs["sources"],
            "results": [],
            "all_succeeded": True,
            "succeeded_sources": kwargs["sources"],
            "failed_sources": [],
            "total_upserted": 0,
            "total_collected": 0,
            "total_matched": 0,
        }

    monkeypatch.setattr("app.api.sentiment.SentimentIngestService.run_many", fake_run_many)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={
                "sources": ["nga"],
                "start_date": "2026-05-01",
                "end_date": "2026-05-02",
            },
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    assert resp.json()["data"]["requested_sources"] == ["flocktrader"]

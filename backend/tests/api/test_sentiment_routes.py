import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_sentiment_routes_are_registered_and_validate_sources():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/sentiment/summary/600519.SH?sources=reddit")

    assert resp.status_code == 200
    assert resp.json()["code"] == 1
    assert "unsupported sentiment source" in resp.json()["message"]

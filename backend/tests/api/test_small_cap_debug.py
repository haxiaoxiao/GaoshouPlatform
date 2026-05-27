"""Small-cap debug API route tests."""

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_small_cap_yearly_debug_routes_registered():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        missing = await client.get("/api/v2/small-cap-debug/yearly/not-found")

    assert missing.status_code == 200
    assert missing.json()["code"] == 1

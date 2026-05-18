"""Factor research API route compatibility tests."""

import pytest
from httpx import ASGITransport, AsyncClient

from app.compute.operators.ta_ops import get_ta_capabilities
from app.main import app


@pytest.mark.asyncio
async def test_non_v2_and_v2_compute_routes_are_registered():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        new_resp = await client.get("/api/compute/operators")
        old_resp = await client.get("/api/v2/compute/operators")

    assert new_resp.status_code == 200
    assert old_resp.status_code == 200
    assert new_resp.json()["code"] == 0
    assert old_resp.json()["code"] == 0


@pytest.mark.asyncio
async def test_non_v2_factor_template_route_is_registered():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/factors/templates")

    assert resp.status_code == 200
    assert isinstance(resp.json(), list)


@pytest.mark.asyncio
async def test_non_v2_feature_definitions_route_is_registered():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/features/definitions")

    assert resp.status_code == 200
    assert isinstance(resp.json()["data"], list)


@pytest.mark.asyncio
async def test_non_v2_evaluation_board_route_is_registered():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/api/evaluation/board", json={"page": 1, "page_size": 5})

    assert resp.status_code == 200
    assert resp.json()["code"] in (0, 1)


def test_ta_capabilities_shape():
    caps = get_ta_capabilities()

    assert "ta_provider" in caps
    assert "akquant_available" in caps
    assert "talib_available" in caps
    assert "stable_ta_functions" in caps
    assert "MACD_HIST" in caps["stable_ta_functions"]

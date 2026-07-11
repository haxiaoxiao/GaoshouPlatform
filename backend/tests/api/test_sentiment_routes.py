from __future__ import annotations

from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_sentiment_routes_validate_sources():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        summary = await client.get("/api/sentiment/summary/600519.SH?sources=reddit")
        overview = await client.get("/api/sentiment/overview?sources=reddit")
        threads = await client.get("/api/sentiment/threads?sources=reddit")

    assert summary.json()["code"] == 1
    assert overview.json()["code"] == 1
    assert threads.json()["code"] == 1
    assert "unsupported sentiment source" in summary.json()["message"]


@pytest.mark.asyncio
async def test_sentiment_ingest_submits_unified_sync_run(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_proxy(method, path, *, json_body=None, params=None):
        captured.update({"method": method, "path": path, "body": json_body, "params": params})
        return {
            "code": 0,
            "message": "success",
            "data": {
                "task_id": "sync-sentiment",
                "run_id": "sync-sentiment",
                "sync_type": "sentiment",
                "status": "queued",
                "details": {},
            },
        }

    monkeypatch.setattr("app.api.sentiment.proxy_sync_request", fake_proxy)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/sentiment/ingest/run",
            json={
                "sources": ["xueqiu", "eastmoney", "taoguba", "tieba", "laohu8", "jisilu", "wechat", "nga"],
                "symbol": "600519.SH",
                "max_pages": 2,
                "min_reply": 10,
            },
        )

    assert response.json()["code"] == 0
    assert captured["path"] == "/api/data/sync"
    assert captured["body"]["sync_type"] == "sentiment"
    assert captured["body"]["symbols"] == ["600519.SH"]
    assert captured["body"]["sentiment_sources"] == [
        "xueqiu_spyder",
        "eastmoney_guba",
        "taoguba",
        "tieba_stock",
        "laohu8_stock",
        "jisilu",
        "wechat_sogou",
        "flocktrader",
    ]


@pytest.mark.asyncio
async def test_sentiment_ingest_requires_symbol_for_xueqiu(monkeypatch):
    called = False

    async def fake_proxy(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("proxy must not be called")

    monkeypatch.setattr("app.api.sentiment.proxy_sync_request", fake_proxy)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/sentiment/ingest/run", json={"sources": ["xueqiu"]})

    assert response.json()["code"] == 1
    assert "requires a symbol" in response.json()["message"]
    assert called is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("alias", "canonical"),
    [
        ("eastmoney", "eastmoney_guba"),
        ("淘股吧", "taoguba"),
        ("百度贴吧", "tieba_stock"),
        ("老虎社区", "laohu8_stock"),
        ("集思录", "jisilu"),
        ("微信", "wechat_sogou"),
        ("nga", "flocktrader"),
    ],
)
async def test_non_xueqiu_sources_allow_global_ingest(monkeypatch, alias, canonical):
    captured: dict[str, Any] = {}

    async def fake_proxy(method, path, *, json_body=None, params=None):
        captured["body"] = json_body
        return {"code": 0, "data": {"run_id": "sync-1", "task_id": "sync-1", "status": "queued"}}

    monkeypatch.setattr("app.api.sentiment.proxy_sync_request", fake_proxy)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/sentiment/ingest/run",
            json={"sources": [alias], "max_pages": 1, "min_reply": 0},
        )

    assert response.json()["code"] == 0
    assert captured["body"]["symbols"] is None
    assert captured["body"]["sentiment_sources"] == [canonical]


@pytest.mark.asyncio
async def test_sentiment_run_status_proxies_by_run_id(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_proxy(method, path, *, json_body=None, params=None):
        captured.update({"method": method, "path": path})
        return {"code": 0, "data": {"run_id": "sync-abc", "status": "completed"}}

    monkeypatch.setattr("app.api.sentiment.proxy_sync_request", fake_proxy)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/api/sentiment/ingest/runs/sync-abc")

    assert response.json()["data"]["status"] == "completed"
    assert captured == {"method": "GET", "path": "/api/data/sync/runs/sync-abc"}

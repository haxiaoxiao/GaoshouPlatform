import pytest
from httpx import ASGITransport, AsyncClient

from app.api.sentiment import IngestRunRequest, _run_sentiment_ingest_task
from app.main import app
from app.services.runtime_tasks import get_task, register_task


@pytest.mark.asyncio
async def test_sentiment_routes_are_registered_and_validate_sources():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/api/sentiment/summary/600519.SH?sources=reddit")
        overview_resp = await client.get("/api/sentiment/overview?sources=reddit")
        threads_resp = await client.get("/api/sentiment/threads?sources=reddit")

    assert resp.status_code == 200
    assert resp.json()["code"] == 1
    assert "unsupported sentiment source" in resp.json()["message"]
    assert overview_resp.status_code == 200
    assert overview_resp.json()["code"] == 1
    assert "unsupported sentiment source" in overview_resp.json()["message"]
    assert threads_resp.status_code == 200
    assert threads_resp.json()["code"] == 1
    assert "unsupported sentiment source" in threads_resp.json()["message"]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_accepts_merged_sources(monkeypatch):
    scheduled: list[tuple[str, list[str]]] = []

    async def fake_schedule(task_id, request, sources):
        scheduled.append((task_id, sources))

    monkeypatch.setattr("app.api.sentiment._schedule_sentiment_ingest_task", fake_schedule)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={
                "symbol": "600519.SH",
                "sources": ["xueqiu", "eastmoney", "taoguba", "tieba", "jisilu", "wechat", "nga"],
                "max_pages": 2,
                "min_reply": 10,
            },
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    data = resp.json()["data"]
    assert data["status"] == "queued"
    assert data["sources"] == ["xueqiu_spyder", "eastmoney_guba", "taoguba", "tieba_stock", "jisilu", "wechat_sogou", "flocktrader"]
    assert scheduled == [(data["task_id"], ["xueqiu_spyder", "eastmoney_guba", "taoguba", "tieba_stock", "jisilu", "wechat_sogou", "flocktrader"])]
    assert get_task(data["task_id"])["status"] == "queued"


@pytest.mark.asyncio
async def test_sentiment_ingest_route_allows_flocktrader_without_symbol(monkeypatch):
    scheduled: list[tuple[str, list[str]]] = []

    async def fake_schedule(task_id, request, sources):
        assert request.symbol is None
        scheduled.append((task_id, sources))

    monkeypatch.setattr("app.api.sentiment._schedule_sentiment_ingest_task", fake_schedule)

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
    data = resp.json()["data"]
    assert data["sources"] == ["flocktrader"]
    assert scheduled == [(data["task_id"], ["flocktrader"])]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_allows_jisilu_without_symbol(monkeypatch):
    scheduled: list[tuple[str, list[str]]] = []

    async def fake_schedule(task_id, request, sources):
        assert request.symbol is None
        scheduled.append((task_id, sources))

    monkeypatch.setattr("app.api.sentiment._schedule_sentiment_ingest_task", fake_schedule)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={"sources": ["jisilu"], "max_pages": 1, "min_reply": 0},
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    data = resp.json()["data"]
    assert data["sources"] == ["jisilu"]
    assert scheduled == [(data["task_id"], ["jisilu"])]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_requires_symbol_for_xueqiu():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={"sources": ["xueqiu"]},
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 1
    assert "requires a symbol" in resp.json()["message"]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_allows_eastmoney_without_symbol(monkeypatch):
    scheduled: list[tuple[str, list[str]]] = []

    async def fake_schedule(task_id, request, sources):
        assert request.symbol is None
        scheduled.append((task_id, sources))

    monkeypatch.setattr("app.api.sentiment._schedule_sentiment_ingest_task", fake_schedule)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={"sources": ["eastmoney_guba"], "max_pages": 1, "min_reply": 0},
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    data = resp.json()["data"]
    assert data["sources"] == ["eastmoney_guba"]
    assert scheduled == [(data["task_id"], ["eastmoney_guba"])]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_allows_taoguba_without_symbol(monkeypatch):
    scheduled: list[tuple[str, list[str]]] = []

    async def fake_schedule(task_id, request, sources):
        assert request.symbol is None
        scheduled.append((task_id, sources))

    monkeypatch.setattr("app.api.sentiment._schedule_sentiment_ingest_task", fake_schedule)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={"sources": ["淘股吧"], "max_pages": 1, "min_reply": 0},
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    data = resp.json()["data"]
    assert data["sources"] == ["taoguba"]
    assert scheduled == [(data["task_id"], ["taoguba"])]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_allows_tieba_without_symbol(monkeypatch):
    scheduled: list[tuple[str, list[str]]] = []

    async def fake_schedule(task_id, request, sources):
        assert request.symbol is None
        scheduled.append((task_id, sources))

    monkeypatch.setattr("app.api.sentiment._schedule_sentiment_ingest_task", fake_schedule)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={"sources": ["百度贴吧"], "max_pages": 1, "min_reply": 0},
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    data = resp.json()["data"]
    assert data["sources"] == ["tieba_stock"]
    assert scheduled == [(data["task_id"], ["tieba_stock"])]


@pytest.mark.asyncio
async def test_sentiment_ingest_route_allows_wechat_without_symbol(monkeypatch):
    scheduled: list[tuple[str, list[str]]] = []

    async def fake_schedule(task_id, request, sources):
        assert request.symbol is None
        scheduled.append((task_id, sources))

    monkeypatch.setattr("app.api.sentiment._schedule_sentiment_ingest_task", fake_schedule)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post(
            "/api/sentiment/ingest/run",
            json={"sources": ["wechat_sogou"], "max_pages": 1, "min_reply": 0},
        )

    assert resp.status_code == 200
    assert resp.json()["code"] == 0
    data = resp.json()["data"]
    assert data["sources"] == ["wechat_sogou"]
    assert scheduled == [(data["task_id"], ["wechat_sogou"])]


@pytest.mark.asyncio
async def test_sentiment_ingest_task_stores_result(monkeypatch):
    async def fake_run_many(self, symbol: str | None, **kwargs):
        return {
            "symbol": symbol,
            "requested_sources": kwargs["sources"],
            "results": [],
            "all_succeeded": True,
            "succeeded_sources": kwargs["sources"],
            "failed_sources": [],
            "total_upserted": 1,
            "total_collected": 2,
            "total_matched": 1,
        }

    monkeypatch.setattr("app.api.sentiment.SentimentIngestService.run_many", fake_run_many)
    task_id = "sentiment-test-task"
    register_task(task_id=task_id, kind="sentiment_ingest", title="Sentiment ingest")

    await _run_sentiment_ingest_task(
        task_id,
        IngestRunRequest(sources=["nga"], start_date="2026-05-01", end_date="2026-05-02"),
        ["flocktrader"],
    )

    task = get_task(task_id)
    assert task["status"] == "completed"
    assert task["progress"] == 1.0
    assert task["meta"]["result"]["requested_sources"] == ["flocktrader"]

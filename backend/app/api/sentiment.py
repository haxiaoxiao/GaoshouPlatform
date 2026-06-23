from __future__ import annotations

from datetime import date
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import async_session_factory, get_async_session
from app.services.runtime_tasks import register_task, update_task
from app.services.sentiment import (
    DEFAULT_SOURCE_ORDER,
    SentimentIngestService,
    SentimentService,
    ordered_sentiment_sources,
    parse_sources,
    serialize_post,
    serialize_thread,
)
from app.services.task_queue import QueuedTask, get_task_queue

router = APIRouter()


class IngestRunRequest(BaseModel):
    source: str | None = Field(None, description="Single sentiment source for backward compatibility")
    sources: list[str] | None = Field(None, description="Optional source list; defaults to all configured sources")
    symbol: str | None = Field(None, description="Security symbol for source-specific crawlers like xueqiu_spyder")
    max_pages: int = Field(3, ge=1, le=30)
    min_reply: int = Field(20, ge=0, le=10000)
    start_date: date | None = Field(None, description="NGA daily crawl/cache start date")
    end_date: date | None = Field(None, description="NGA daily crawl/cache end date")
    force_refresh: bool = Field(False, description="Re-crawl NGA daily files even when cached")


def _resolve_ingest_sources(request: IngestRunRequest) -> list[str]:
    if request.sources:
        return ordered_sentiment_sources(request.sources)
    if request.source:
        return ordered_sentiment_sources([request.source])
    return list(DEFAULT_SOURCE_ORDER)


def _validate_ingest_request(request: IngestRunRequest, sources: list[str]) -> str | None:
    symbol_sources = [source for source in sources if source in {"xueqiu_spyder"}]
    if symbol_sources and not request.symbol:
        return (
            f"{', '.join(symbol_sources)} ingest requires a symbol. "
            "Eastmoney, Jisilu, WeChat/Sogou and NGA/flocktrader can run without a symbol."
        )
    return None


def _nga_event_progress(event: dict[str, Any]) -> float:
    stage = str(event.get("stage") or "")
    page = event.get("current_page") or event.get("board_page")
    page_limit = event.get("page_limit")
    try:
        if page and page_limit:
            return max(0.05, min(0.9, float(page) / max(float(page_limit), 1.0) * 0.85))
    except (TypeError, ValueError):
        pass
    if stage.endswith(".done"):
        return 0.95
    if "cache.write" in stage:
        return 0.9
    if "crawl" in stage:
        return 0.2
    return 0.1


async def _schedule_sentiment_ingest_task(task_id: str, request: IngestRunRequest, sources: list[str]) -> None:
    await get_task_queue("sentiment_ingest").submit(
        QueuedTask(
            task_id=task_id,
            title="sentiment ingest",
            handler=lambda: _run_sentiment_ingest_task(task_id, request, sources),
            metadata={"sources": sources, "symbol": request.symbol},
        )
    )


async def _run_sentiment_ingest_task(
    task_id: str,
    request: IngestRunRequest,
    sources: list[str],
) -> None:
    update_task(
        task_id,
        status="running",
        progress=0.05,
        meta={"sources": sources, "symbol": request.symbol},
    )
    try:
        async with async_session_factory() as session:
            def on_progress(event: dict[str, Any]) -> None:
                progress_meta: dict[str, Any] = {
                    "sources": sources,
                    "symbol": request.symbol,
                    "crawler_progress": event,
                    "stage": event.get("stage"),
                    "current_step": event.get("current_step"),
                    "current_date": event.get("current_date"),
                    "current_page": event.get("current_page"),
                    "current_tid": event.get("current_tid"),
                    "current_title": event.get("current_title"),
                }
                if event.get("source") == "nga":
                    progress_meta["nga_progress"] = event
                update_task(
                    task_id,
                    status="running",
                    progress=_nga_event_progress(event),
                    meta=progress_meta,
                )

            ingest_service = SentimentIngestService(session, progress_callback=on_progress)
            if request.source and not request.sources and len(sources) == 1:
                result = await ingest_service.run(
                    sources[0],
                    request.symbol,
                    max_pages=request.max_pages,
                    min_reply=request.min_reply,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    force_refresh=request.force_refresh,
                )
            else:
                result = await ingest_service.run_many(
                    request.symbol,
                    sources=sources,
                    max_pages=request.max_pages,
                    min_reply=request.min_reply,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    force_refresh=request.force_refresh,
                )
            await session.commit()
        update_task(
            task_id,
            status="completed",
            progress=1.0,
            result_ref=f"/api/system/tasks/{task_id}",
            meta={"result": result},
        )
    except Exception as exc:
        update_task(
            task_id,
            status="failed",
            progress=1.0,
            error=str(exc),
            meta={"sources": sources, "symbol": request.symbol},
        )


@router.get("/overview", summary="Get unified sentiment module overview")
async def get_sentiment_overview(
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,jisilu,wechat_sogou,flocktrader"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        parsed_sources = parse_sources(sources)
        data = await SentimentService(session).overview(parsed_sources)
        return {"code": 0, "data": data}
    except ValueError as exc:
        return {"code": 1, "message": str(exc)}


@router.get("/summary/{symbol}", summary="Get cached sentiment summary")
async def get_sentiment_summary(
    symbol: str,
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,jisilu,wechat_sogou,flocktrader"),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        parsed_sources = parse_sources(sources)
        data = await SentimentService(session).summary(symbol, start_date, end_date, parsed_sources)
        return {"code": 0, "data": data}
    except ValueError as exc:
        return {"code": 1, "message": str(exc)}


@router.get("/posts/{symbol}", summary="List cached sentiment posts")
async def get_sentiment_posts(
    symbol: str,
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,jisilu,wechat_sogou,flocktrader"),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        parsed_sources = parse_sources(sources)
        posts = await SentimentService(session).list_posts(
            symbol, start_date, end_date, parsed_sources, limit
        )
        return {"code": 0, "data": [serialize_post(post) for post in posts]}
    except ValueError as exc:
        return {"code": 1, "message": str(exc)}


@router.get("/threads", summary="List cached sentiment threads before symbol expansion")
async def get_sentiment_threads(
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,jisilu,wechat_sogou,flocktrader"),
    symbol: str | None = Query(None, description="Optional symbol filter for matched NGA threads"),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        parsed_sources = parse_sources(sources)
        threads = await SentimentService(session).list_threads(
            start_date, end_date, parsed_sources, symbol, limit
        )
        return {"code": 0, "data": [serialize_thread(thread) for thread in threads]}
    except ValueError as exc:
        return {"code": 1, "message": str(exc)}


@router.post("/ingest/run", summary="Run a local external sentiment crawler")
async def run_sentiment_ingest(
    request: IngestRunRequest,
) -> dict[str, Any]:
    try:
        sources = _resolve_ingest_sources(request)
        validation_error = _validate_ingest_request(request, sources)
        if validation_error:
            return {"code": 1, "message": validation_error}
        task_id = f"sentiment-{uuid4().hex[:12]}"
        register_task(
            task_id=task_id,
            kind="sentiment_ingest",
            title="Sentiment ingest",
            status="queued",
            progress=0.0,
            result_ref=f"/api/system/tasks/{task_id}",
            meta={
                "sources": sources,
                "symbol": request.symbol,
                "start_date": request.start_date.isoformat() if request.start_date else None,
                "end_date": request.end_date.isoformat() if request.end_date else None,
                "max_pages": request.max_pages,
                "min_reply": request.min_reply,
                "force_refresh": request.force_refresh,
            },
        )
        await _schedule_sentiment_ingest_task(task_id, request, sources)
        return {
            "code": 0,
            "data": {
                "task_id": task_id,
                "status": "queued",
                "kind": "sentiment_ingest",
                "sources": sources,
                "symbol": request.symbol,
                "result_ref": f"/api/system/tasks/{task_id}",
            },
        }
    except Exception as exc:
        return {"code": 1, "message": str(exc)}

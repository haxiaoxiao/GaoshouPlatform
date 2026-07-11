from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.services.sentiment import (
    DEFAULT_SOURCE_ORDER,
    SentimentService,
    ordered_sentiment_sources,
    parse_sources,
    serialize_post,
    serialize_thread,
)
from app.services.sync_proxy import proxy_sync_request

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
            "Eastmoney, Taoguba, Baidu Tieba, Laohu8, Jisilu, WeChat/Sogou and NGA/flocktrader can run without a symbol."
        )
    return None


@router.get("/overview", summary="Get unified sentiment module overview")
async def get_sentiment_overview(
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,taoguba,tieba_stock,laohu8_stock,jisilu,wechat_sogou,flocktrader"),
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
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,taoguba,tieba_stock,laohu8_stock,jisilu,wechat_sogou,flocktrader"),
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
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,taoguba,tieba_stock,laohu8_stock,jisilu,wechat_sogou,flocktrader"),
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
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,eastmoney_guba,taoguba,tieba_stock,laohu8_stock,jisilu,wechat_sogou,flocktrader"),
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
        response = await proxy_sync_request(
            "POST",
            "/api/data/sync",
            json_body={
                "sync_type": "sentiment",
                "symbols": [request.symbol] if request.symbol else None,
                "start_date": request.start_date.isoformat() if request.start_date else None,
                "end_date": request.end_date.isoformat() if request.end_date else None,
                "sync_mode": "range" if request.start_date or request.end_date else "incremental",
                "failure_strategy": "skip",
                "sentiment_sources": sources,
                "max_pages": request.max_pages,
                "min_reply": request.min_reply,
                "force_refresh": request.force_refresh,
            },
        )
        return response
    except Exception as exc:
        return {"code": 1, "message": str(exc)}


@router.get("/ingest/runs/{run_id}", summary="Get a sentiment ingest run")
async def get_sentiment_ingest_run(run_id: str) -> dict[str, Any]:
    return await proxy_sync_request("GET", f"/api/data/sync/runs/{run_id}")

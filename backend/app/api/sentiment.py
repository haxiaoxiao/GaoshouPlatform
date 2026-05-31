from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.sqlite import get_async_session
from app.services.sentiment import (
    DEFAULT_SOURCE_ORDER,
    SentimentIngestService,
    SentimentService,
    ordered_sentiment_sources,
    parse_sources,
    serialize_post,
)

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


@router.get("/overview", summary="Get unified sentiment module overview")
async def get_sentiment_overview(
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,flocktrader"),
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
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,flocktrader"),
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
    sources: str | None = Query(None, description="Comma-separated source list: xueqiu_spyder,flocktrader"),
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


@router.post("/ingest/run", summary="Run a local external sentiment crawler")
async def run_sentiment_ingest(
    request: IngestRunRequest,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    try:
        sources = _resolve_ingest_sources(request)
        ingest_service = SentimentIngestService(session)
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
        return {"code": 0, "data": result}
    except Exception as exc:
        return {"code": 1, "message": str(exc)}

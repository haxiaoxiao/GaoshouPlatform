from __future__ import annotations

from typing import Any

from .base import AdapterRequest, AdapterResult

SOURCE = "flocktrader"


async def run(owner: Any, request: AdapterRequest) -> AdapterResult:
    posts, threads, stats = await owner._collect_flocktrader_by_date(
        request.symbol,
        max_pages=request.max_pages,
        start_date=request.start_date,
        end_date=request.end_date,
        force_refresh=request.force_refresh,
    )
    stats.thread_upserted = await owner.service.upsert_threads(threads)
    return AdapterResult(posts, {
        "mode": stats.mode,
        "collected": stats.total_posts,
        "analyzed": stats.analyzed_posts,
        "matched": stats.matched_posts,
        "threads_upserted": stats.thread_upserted,
        "loaded_dates": stats.loaded_dates,
        "crawled_dates": stats.crawled_dates,
        "date_files": stats.date_files,
        "extra_date_files": stats.extra_date_files,
        "empty_dates": stats.empty_dates,
        "search_queries": stats.search_queries,
        "search_pages": stats.search_pages,
        "scan_time_basis": stats.scan_time_basis,
        "cache_partition": stats.cache_partition,
    })


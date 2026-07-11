from __future__ import annotations

import asyncio
from typing import Any

from .base import AdapterRequest, AdapterResult

SOURCE = "laohu8_stock"


async def run(owner: Any, request: AdapterRequest) -> AdapterResult:
    from app.services.sentiment import _laohu8_stock_symbol_values

    symbols = _laohu8_stock_symbol_values(request.symbol)
    posts, threads, raw_stats = await asyncio.to_thread(
        owner._collect_laohu8_stock,
        symbols,
        request.min_reply,
        request.start_date,
        request.end_date,
    )
    threads_upserted = await owner.service.upsert_threads(threads)
    return AdapterResult(posts, {
        "mode": "stock_pages",
        "collected": raw_stats["collected"],
        "matched": len(posts),
        "threads_upserted": threads_upserted,
        "page_url": "https://www.laohu8.com/stock/{code}",
        "min_reply_applied": request.min_reply,
        **raw_stats,
    })


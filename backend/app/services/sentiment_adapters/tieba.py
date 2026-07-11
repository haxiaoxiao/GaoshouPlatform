from __future__ import annotations

import asyncio
from typing import Any

from .base import AdapterRequest, AdapterResult

SOURCE = "tieba_stock"


async def run(owner: Any, request: AdapterRequest) -> AdapterResult:
    from app.db.models.stock import Stock
    from app.services.sentiment import _symbol_aliases_from_parts, _tieba_stock_bars

    stock = await owner.session.get(Stock, request.symbol) if request.symbol else None
    aliases = _symbol_aliases_from_parts(request.symbol, stock) if request.symbol else None
    stock_aliases = await owner._load_stock_aliases() if not request.symbol else None
    posts, threads, raw_stats = await asyncio.to_thread(
        owner._collect_tieba_stock,
        request.symbol,
        aliases,
        stock_aliases,
        _tieba_stock_bars(request.symbol, stock),
        request.max_pages,
        request.min_reply,
        request.start_date,
        request.end_date,
    )
    threads_upserted = await owner.service.upsert_threads(threads)
    return AdapterResult(posts, {
        "mode": "stock_forum_bars",
        "collected": raw_stats["collected"],
        "matched": len(posts),
        "threads_upserted": threads_upserted,
        "page_url": "https://tieba.baidu.com/mg/f/getFrsData",
        **raw_stats,
    })


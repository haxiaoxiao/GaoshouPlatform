from __future__ import annotations

import asyncio
from typing import Any

from .base import AdapterRequest, AdapterResult

SOURCE = "jisilu"


async def run(owner: Any, request: AdapterRequest) -> AdapterResult:
    from app.db.models.stock import Stock
    from app.services.sentiment import _symbol_aliases_from_parts

    aliases = None
    stock_aliases = await owner._load_stock_aliases() if not request.symbol else None
    if request.symbol:
        stock = await owner.session.get(Stock, request.symbol)
        aliases = _symbol_aliases_from_parts(request.symbol, stock)
    posts, raw_stats = await asyncio.to_thread(
        owner._collect_jisilu,
        request.symbol,
        aliases,
        stock_aliases,
        request.max_pages,
        request.min_reply,
        request.start_date,
        request.end_date,
    )
    return AdapterResult(posts, {
        "mode": "topic_board",
        "collected": raw_stats["collected"],
        "matched": len(posts),
        "page_url": "https://www.jisilu.cn/category/8",
        **raw_stats,
    })


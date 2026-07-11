from __future__ import annotations

import asyncio
from typing import Any

from .base import AdapterRequest, AdapterResult

SOURCE = "xueqiu_spyder"


async def run(owner: Any, request: AdapterRequest) -> AdapterResult:
    from app.services.sentiment import _to_xueqiu_symbol

    if not request.symbol:
        raise ValueError("xueqiu_spyder ingest requires a symbol")
    posts, raw_stats = await asyncio.to_thread(
        owner._collect_xueqiu,
        request.symbol,
        request.max_pages,
        request.min_reply,
        request.start_date,
        request.end_date,
    )
    return AdapterResult(posts, {
        "mode": "stock_page",
        "collected": int(raw_stats.get("raw_count") or len(posts)),
        "matched": len(posts),
        "page_url": f"https://xueqiu.com/S/{_to_xueqiu_symbol(request.symbol)}",
        **raw_stats,
    })

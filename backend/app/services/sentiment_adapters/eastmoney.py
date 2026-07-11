from __future__ import annotations

import asyncio
from typing import Any

from .base import AdapterRequest, AdapterResult

SOURCE = "eastmoney_guba"


async def run(owner: Any, request: AdapterRequest) -> AdapterResult:
    if request.symbol:
        posts, raw_stats = await asyncio.to_thread(
            owner._collect_eastmoney_guba,
            request.symbol,
            request.max_pages,
            request.min_reply,
            request.start_date,
            request.end_date,
        )
        mode = "stock_bar"
        page_url = f"https://guba.eastmoney.com/list,{request.symbol.split('.', 1)[0]}.html"
    else:
        posts, raw_stats = await asyncio.to_thread(
            owner._collect_eastmoney_guba_hot_bars,
            request.max_pages,
            request.min_reply,
            request.start_date,
            request.end_date,
        )
        mode = "hot_bars"
        page_url = "https://guba.eastmoney.com/remenba.aspx"
    return AdapterResult(posts, {
        "mode": mode,
        "collected": raw_stats["collected"],
        "matched": len(posts),
        "page_url": page_url,
        **raw_stats,
    })


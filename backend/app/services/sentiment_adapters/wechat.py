from __future__ import annotations

import asyncio
from typing import Any

from .base import AdapterRequest, AdapterResult

SOURCE = "wechat_sogou"


async def run(owner: Any, request: AdapterRequest) -> AdapterResult:
    from app.db.models.stock import Stock
    from app.services.sentiment import _symbol_aliases_from_parts, _wechat_sogou_queries

    stock = await owner.session.get(Stock, request.symbol) if request.symbol else None
    aliases = _symbol_aliases_from_parts(request.symbol, stock) if request.symbol else None
    stock_aliases = await owner._load_stock_aliases() if not request.symbol else None
    posts, threads, raw_stats = await asyncio.to_thread(
        owner._collect_wechat_sogou,
        request.symbol,
        aliases,
        stock_aliases,
        _wechat_sogou_queries(request.symbol, stock),
        request.max_pages,
        request.start_date,
        request.end_date,
    )
    threads_upserted = await owner.service.upsert_threads(threads)
    return AdapterResult(posts, {
        "mode": "public_account_search",
        "collected": raw_stats["collected"],
        "matched": len(posts),
        "threads_upserted": threads_upserted,
        "page_url": "https://weixin.sogou.com/weixin?type=2",
        **raw_stats,
    })


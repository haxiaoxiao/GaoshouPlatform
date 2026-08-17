from __future__ import annotations

import argparse
import asyncio
import json

from sqlalchemy import select

from app.db.models.sentiment import SentimentPost, SentimentThread
from app.db.sqlite import async_session_factory, init_db
from app.services.sentiment import (
    SentimentPostInput,
    SentimentService,
    SentimentThreadInput,
    _loads_list,
)


def _raw(value: str | None) -> dict:
    try:
        parsed = json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


async def backfill(batch_size: int) -> dict[str, int]:
    await init_db()
    totals = {"posts": 0, "threads": 0}
    async with async_session_factory() as session:
        service = SentimentService(session)
        post_rows = list((await session.execute(select(SentimentPost))).scalars().all())
        for offset in range(0, len(post_rows), batch_size):
            batch = post_rows[offset:offset + batch_size]
            await service.upsert_posts([
                SentimentPostInput(
                    source=row.source,
                    source_post_id=row.source_post_id,
                    symbol=row.symbol,
                    title=row.title,
                    content=row.content,
                    author=row.author,
                    published_at=row.published_at,
                    url=row.url,
                    reply_count=row.reply_count,
                    like_count=row.like_count,
                    comment_count=row.comment_count,
                    keywords=_loads_list(row.keywords_json),
                    raw=_raw(row.raw_json),
                )
                for row in batch
            ])
            await session.commit()
            totals["posts"] += len(batch)

        thread_rows = list((await session.execute(select(SentimentThread))).scalars().all())
        for offset in range(0, len(thread_rows), batch_size):
            batch = thread_rows[offset:offset + batch_size]
            await service.upsert_threads([
                SentimentThreadInput(
                    source=row.source,
                    source_thread_id=row.source_thread_id,
                    title=row.title,
                    content=row.content,
                    author=row.author,
                    published_at=row.published_at,
                    last_reply_at=row.last_reply_at,
                    url=row.url,
                    reply_count=row.reply_count,
                    comment_count=row.comment_count,
                    symbols=_loads_list(row.symbols_json),
                    keywords=_loads_list(row.keywords_json),
                    raw=_raw(row.raw_json),
                )
                for row in batch
            ])
            await session.commit()
            totals["threads"] += len(batch)
    return totals


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill sentiment mentions and finance_lexicon_v2 analyses")
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(backfill(max(1, args.batch_size))), ensure_ascii=False))


if __name__ == "__main__":
    main()

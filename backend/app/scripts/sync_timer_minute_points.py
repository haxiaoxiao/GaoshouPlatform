"""CLI for syncing sparse timer-minute bars needed by timer backtests."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime

from app.services.timer_minute_sync import (
    DEFAULT_TIMER_TIMES,
    parse_timer_times,
    sync_timer_minute_points,
)


def _parse_yyyymmdd(value: str):
    return datetime.strptime(value, "%Y%m%d").date()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--index-symbol", default="")
    parser.add_argument("--times", default=",".join(DEFAULT_TIMER_TIMES))
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    start = _parse_yyyymmdd(args.start)
    end = _parse_yyyymmdd(args.end)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    timer_times = parse_timer_times(args.times)

    def on_progress(event: dict) -> None:
        if event.get("done", 0) == 0:
            print(
                f"symbols={event.get('total', 0)} range={start}..{end} "
                f"times={event.get('times', [])}",
                flush=True,
            )
            return
        last = event.get("last") or {}
        print(
            f"[{event['done']}/{event['total']}] {event.get('symbol')} "
            f"fetched={last.get('fetched', 0)} inserted={last.get('inserted', 0)} "
            f"total_inserted={event.get('inserted', 0)}",
            flush=True,
        )

    result = await sync_timer_minute_points(
        symbols=symbols,
        index_symbol=args.index_symbol or None,
        start=start,
        end=end,
        timer_times=timer_times,
        limit=args.limit,
        progress_callback=on_progress,
    )
    print(
        f"done symbols={result['symbols']} fetched={result['fetched']} "
        f"inserted={result['inserted']} failures={len(result['failures'])}",
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())

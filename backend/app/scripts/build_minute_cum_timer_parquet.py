"""Build sparse cumulative minute-volume timer dataset from full 1m Parquet.

The output dataset is intended for strategies that need JQ-style
get_bars(..., fields=['volume'], include_now=True) at a few fixed intraday
times. It stores one row per symbol/date/timer with cumulative volume from
market open through that timer, so backtests do not repeatedly scan raw 1m
bars.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from app.data_stores.factory import get_market_data_store
from app.db.duckdb import get_duckdb


def _parse_yyyymmdd(value: str):
    return datetime.strptime(value, "%Y%m%d").date()


def _parse_times(value: str) -> list[str]:
    result: list[str] = []
    for item in value.split(","):
        text = item.strip()
        if not text:
            continue
        parts = text.split(":")
        if len(parts) < 2:
            raise ValueError(f"invalid time: {text}")
        result.append(f"{int(parts[0]):02d}:{int(parts[1]):02d}")
    return result


def _month_starts(start, end):
    if end <= start:
        return
    current = start.replace(day=1)
    last = (end - timedelta(days=1)).replace(day=1)
    while current <= last:
        yield current
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def _next_month(value):
    if value.month == 12:
        return value.replace(year=value.year + 1, month=1)
    return value.replace(month=value.month + 1)


def _quote_list(values: list[str]) -> str:
    return "(" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + ")"


def build(start, end, times: list[str], batch_months: bool = True) -> int:
    store = get_market_data_store()
    if not hasattr(store, "_glob_pattern"):
        raise RuntimeError("build_minute_cum_timer_parquet currently supports Parquet store only")

    db = get_duckdb()
    total = 0
    time_list = _quote_list(times)

    for month_start in _month_starts(start, end):
        month_end = _next_month(month_start)
        window_start = max(start, month_start)
        window_end = min(end, month_end)
        src_pattern = store._glob_pattern("klines_minute")  # type: ignore[attr-defined]

        sql = f"""
            WITH base AS (
                SELECT
                    symbol,
                    datetime,
                    CAST(datetime AS DATE) AS trade_date,
                    strftime(datetime, '%H:%M') AS timer_time,
                    sum(volume) OVER (
                        PARTITION BY symbol, CAST(datetime AS DATE)
                        ORDER BY datetime
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS volume
                FROM read_parquet('{src_pattern}', hive_partitioning=true)
                WHERE datetime >= '{window_start}'
                  AND datetime < '{window_end}'
                  AND year = {month_start.year}
                  AND month = '{month_start.month:02d}'
            )
            SELECT
                symbol,
                datetime,
                volume,
                timer_time,
                trade_date,
                'jq_minute_cum' AS source
            FROM base
            WHERE timer_time IN {time_list}
        """
        df = db.execute(sql).df()
        if df.empty:
            print(f"{month_start:%Y-%m}: no rows", flush=True)
            continue
        written = store.write_minute(df, dataset="klines_minute_cum_timer")  # type: ignore[attr-defined]
        total += written
        print(f"{month_start:%Y-%m}: rows={len(df)} written={written} total={total}", flush=True)
        if not batch_months:
            break
    return total


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYYMMDD")
    parser.add_argument("--end", required=True, help="YYYYMMDD, inclusive calendar date")
    parser.add_argument("--times", default="14:30")
    parser.add_argument("--one-month", action="store_true")
    args = parser.parse_args()

    start = _parse_yyyymmdd(args.start)
    end = _parse_yyyymmdd(args.end)
    end_exclusive = _next_month(end.replace(day=1)) if args.one_month else end
    if not args.one_month:
        end_exclusive = end + timedelta(days=1)
    total = build(start, end_exclusive, _parse_times(args.times), batch_months=not args.one_month)
    print(f"done rows={total}", flush=True)


if __name__ == "__main__":
    main()

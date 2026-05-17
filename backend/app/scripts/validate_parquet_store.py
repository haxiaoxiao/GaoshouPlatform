#!/usr/bin/env python
"""验证 Parquet 数据与 ClickHouse 数据的一致性

用法:
    python -m app.scripts.validate_parquet_store --dataset klines_daily --start 20250101 --end 20251231
    python -m app.scripts.validate_parquet_store --dataset klines_minute_timer --symbols 000001.SZ,600000.SH
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

_BACKEND = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from loguru import logger

from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.clickhouse import get_ch_client


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def validate_daily(
    store: ParquetMarketDataStore,
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    sample_size: int = 5,
) -> dict:
    """比对日线数据"""
    ch = get_ch_client()
    results = {"dataset": "klines_daily", "checks": []}

    # --- 行数比对 ---
    conditions = []
    params: dict = {}
    if symbols:
        conditions.append("symbol IN %(syms)s")
        params["syms"] = tuple(symbols)
    if start_date:
        conditions.append("trade_date >= %(start)s")
        params["start"] = start_date
    if end_date:
        conditions.append("trade_date <= %(end)s")
        params["end"] = end_date
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    ch_count = ch.execute(f"SELECT COUNT(*) FROM klines_daily {where}", params)[0][0]
    pq_count = 0
    if symbols and start_date and end_date:
        pq_count = store.coverage(symbols, start_date, end_date, dataset="klines_daily")["total_rows"]
    results["ch_rows"] = ch_count
    results["pq_rows"] = pq_count
    results["checks"].append({
        "check": "row_count",
        "ch": ch_count,
        "pq": pq_count,
        "match": ch_count == pq_count if pq_count > 0 else None,
    })

    # --- 抽样 OHLC 比对 ---
    sample_rows = ch.execute(
        f"SELECT symbol, trade_date, open, high, low, close, volume, amount "
        f"FROM klines_daily {where} "
        f"ORDER BY rand() LIMIT {sample_size}",
        params,
    )
    for row in sample_rows:
        sym, td = row[0], row[1]
        df = store.load_daily([sym], td, td)
        if df.empty:
            results["checks"].append({"check": "sample", "symbol": sym, "trade_date": str(td), "error": "parquet 无此记录"})
        else:
            r = df.iloc[0]
            match = abs(float(r["close"]) - float(row[4])) < 0.01
            results["checks"].append({
                "check": "sample",
                "symbol": sym,
                "trade_date": str(td),
                "ch_close": float(row[4]),
                "pq_close": float(r["close"]),
                "match": match,
            })

    ch.disconnect()
    return results


def validate_minute_timer(
    store: ParquetMarketDataStore,
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    timer_times: list[str] | None = None,
    *,
    sample_size: int = 5,
) -> dict:
    """比对分钟 timer 数据"""
    ch = get_ch_client()
    results = {"dataset": "klines_minute_timer", "checks": []}

    time_filter = ""
    params: dict = {}
    if timer_times:
        timer_minutes = []
        for text in timer_times:
            try:
                h, m, *_ = str(text).split(":")
                timer_minutes.append(int(h) * 60 + int(m))
            except Exception:
                continue
        if timer_minutes:
            time_filter = "AND (toHour(datetime) * 60 + toMinute(datetime)) IN %(timer_minutes)s"
            params["timer_minutes"] = tuple(timer_minutes)

    conditions = []
    if symbols:
        conditions.append("symbol IN %(syms)s")
        params["syms"] = tuple(symbols)
    if start_date:
        conditions.append("datetime >= %(start)s")
        params["start"] = datetime.combine(start_date, datetime.min.time())
    if end_date:
        conditions.append("datetime < %(end_plus)s")
        params["end_plus"] = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    if time_filter:
        where = f"{where} {time_filter}"

    ch_count = ch.execute(f"SELECT COUNT(*) FROM klines_minute {where}", params)[0][0]
    pq_count = 0
    if symbols and start_date and end_date:
        pq_start = datetime.combine(start_date, datetime.min.time())
        pq_end = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
        info = store.coverage(symbols, start_date, end_date, dataset="klines_minute_timer", timer_times=timer_times)
        pq_count = info["total_rows"]

    results["ch_rows"] = ch_count
    results["pq_rows"] = pq_count
    results["checks"].append({
        "check": "row_count",
        "ch": ch_count,
        "pq": pq_count,
        "match": ch_count == pq_count if pq_count > 0 else None,
    })

    ch.disconnect()
    return results


def main():
    parser = argparse.ArgumentParser(description="验证 Parquet 与 ClickHouse 数据一致性")
    parser.add_argument("--dataset", default="klines_daily", choices=["klines_daily", "klines_minute_timer"])
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--symbols", type=str, default=None)
    parser.add_argument("--timer-times", type=str, default=None)
    parser.add_argument("--parquet-dir", type=str, default="E:/Projects/GaoshouPlatform/data/parquet")
    parser.add_argument("--sample-size", type=int, default=5)
    args = parser.parse_args()

    start_date = parse_date(args.start) if args.start else None
    end_date = parse_date(args.end) if args.end else None
    symbols = [s.strip() for s in args.symbols.split(",")] if args.symbols else None
    timer_times = [t.strip() for t in args.timer_times.split(",")] if args.timer_times else None

    store = ParquetMarketDataStore(data_dir=args.parquet_dir)

    if args.dataset == "klines_daily":
        result = validate_daily(store, symbols, start_date, end_date, sample_size=args.sample_size)
    else:
        result = validate_minute_timer(store, symbols, start_date, end_date, timer_times, sample_size=args.sample_size)

    import json
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

    all_match = all(c.get("match", True) for c in result["checks"])
    return 0 if all_match else 1


if __name__ == "__main__":
    sys.exit(main())

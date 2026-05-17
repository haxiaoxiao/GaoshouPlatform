"""Batch pre-compute V4GV + MACD indicators into Redis.

The cache is intentionally coarse grained: one compressed key per
symbol/parameter pair. The previous stock/date/indicator key layout can create
millions of Redis keys and exhaust memory on broad universes.

Usage:
    python -m app.scripts.batch_indicator_redis \
        --symbols "000001.SZ,000002.SZ" \
        --start 2015-01-01 --end 2026-04-11 --batch-size 20
"""
from __future__ import annotations

import argparse
import gc
import json
import sys
import time
import zlib
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def get_clickhouse_client():
    from clickhouse_driver import Client
    from app.core.config import settings

    return Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def get_redis_client():
    import redis
    from app.core.config import settings

    r = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db,
        password=settings.redis_password or None,
        socket_connect_timeout=5,
    )
    r.ping()
    return r


def load_daily_kline(ch, symbols: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    rows = ch.execute(
        """
        SELECT
            symbol,
            trade_date,
            anyLast(open) AS open,
            anyLast(high) AS high,
            anyLast(low) AS low,
            anyLast(close) AS close,
            anyLast(volume) AS volume,
            anyLast(amount) AS amount
        FROM klines_daily
        WHERE symbol IN %(syms)s
          AND trade_date >= %(start)s AND trade_date <= %(end)s
        GROUP BY symbol, trade_date
        ORDER BY symbol, trade_date
        """,
        {"syms": symbols, "start": start, "end": end},
    )
    if not rows:
        return {}

    df = pd.DataFrame(
        rows,
        columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
    )
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = df[col].astype("float32")
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    result = {}
    for sym, grp in df.groupby("symbol", sort=False):
        result[str(sym)] = grp.set_index("trade_date").sort_index()
    return result


def fill_nan(arr):
    arr = np.asarray(arr, dtype=np.float64).copy()
    if len(arr) == 0:
        return arr
    finite = np.isfinite(arr)
    if not finite.any():
        return arr
    first = int(np.argmax(finite))
    arr[:first] = arr[first]
    for i in range(first + 1, len(arr)):
        if not np.isfinite(arr[i]):
            arr[i] = arr[i - 1]
    return arr


def ema(values, period):
    arr = np.asarray(values, dtype=np.float64)
    out = np.full(len(arr), np.nan)
    alpha = 2.0 / (period + 1.0)
    prev = np.nan
    for i, value in enumerate(arr):
        if not np.isfinite(value):
            out[i] = prev
            continue
        prev = value if not np.isfinite(prev) else alpha * value + (1 - alpha) * prev
        out[i] = prev
    return out


def sma(values, period):
    arr = np.asarray(values, dtype=np.float64)
    out = np.full(len(arr), np.nan)
    for i in range(period - 1, len(arr)):
        window = arr[i - period + 1 : i + 1]
        finite = window[np.isfinite(window)]
        if len(finite):
            out[i] = float(np.mean(finite))
    return out


def rolling_min(values, period):
    arr = np.asarray(values, dtype=np.float64)
    out = np.full(len(arr), np.nan)
    for i in range(period - 1, len(arr)):
        out[i] = np.nanmin(arr[i - period + 1 : i + 1])
    return out


def rolling_max(values, period):
    arr = np.asarray(values, dtype=np.float64)
    out = np.full(len(arr), np.nan)
    for i in range(period - 1, len(arr)):
        out[i] = np.nanmax(arr[i - period + 1 : i + 1])
    return out


def compute_v4gv(close, high, low, n=55, m=34):
    need = max(n, m, 55, 34) + 20
    if len(close) < need:
        return None, None

    close = fill_nan(close)
    high = fill_nan(high)
    low = fill_nan(low)

    llv34 = rolling_min(low, 34)
    hhv34 = rolling_max(high, 34)
    denominator34 = np.where(hhv34 - llv34 == 0, 1e-6, hhv34 - llv34)
    rsv = 100 * (close - llv34) / denominator34
    var1 = sma(rsv, 5) - 20

    llv55 = rolling_min(low, 55)
    hhv55 = rolling_max(high, 55)
    denominator55 = np.where(hhv55 - llv55 == 0, 1e-6, hhv55 - llv55)
    rsv2 = 100 * (close - llv55) / denominator55
    sma1 = ema(rsv2, 5)
    a1 = 3 * sma1 - 2 * sma1
    a12 = (a1 + var1) / 2

    rsv3 = 100 * (close - llv34) / denominator34
    main_fund = ema(rsv3, 3)
    rsv4 = -100 * (hhv34 - close) / denominator34
    d0 = ema(rsv4, 4) + 100
    v4gv = ((main_fund + d0) / 2 + a12) / 2
    v4gv21 = (v4gv + sma(v4gv, 2)) / 2
    return v4gv, v4gv21


def compute_macd_signal(close):
    if len(close) < 35:
        return np.full(len(close), False)
    close = fill_nan(close)
    macd_line = ema(close, 12) - ema(close, 26)
    signal = ema(macd_line, 9)
    return np.isfinite(macd_line) & np.isfinite(signal) & (macd_line > signal) & (macd_line > 0)


def encode_payload(dates, v4gv, v4gv21, macd_flags) -> bytes:
    rows = []
    for i, d in enumerate(dates):
        if isinstance(d, pd.Timestamp):
            d = d.date()
        date_str = d.isoformat() if isinstance(d, date) else str(d)[:10]
        rows.append([
            date_str,
            None if not np.isfinite(v4gv[i]) else round(float(v4gv[i]), 6),
            None if not np.isfinite(v4gv21[i]) else round(float(v4gv21[i]), 6),
            bool(macd_flags[i]),
        ])
    raw = json.dumps(rows, separators=(",", ":")).encode("utf-8")
    return zlib.compress(raw, level=6)


def compute_strategy_style_rows(df: pd.DataFrame, n=55, m=34):
    dates = list(df.index)
    close_all = df["close"].values
    high_all = df["high"].values
    low_all = df["low"].values
    need = max(n, m, 55, 34) + 20
    rows = []
    for i, d in enumerate(dates):
        end = i + 1
        start = max(0, end - need)
        close = close_all[start:end]
        high = high_all[start:end]
        low = low_all[start:end]
        if len(close) < max(n, m, 55, 34) + 5:
            v4 = None
            v421 = None
        else:
            v4_arr, v421_arr = compute_v4gv(close, high, low, n, m)
            if v4_arr is None or not np.isfinite(v4_arr[-1]) or not np.isfinite(v421_arr[-1]):
                v4 = None
                v421 = None
            else:
                v4 = round(float(v4_arr[-1]), 6)
                v421 = round(float(v421_arr[-1]), 6)

        macd_start = max(0, end - 60)
        macd_close = close_all[macd_start:end]
        if len(macd_close) < 35:
            macd_flag = False
        else:
            flags = compute_macd_signal(macd_close)
            macd_flag = bool(flags[-1])

        date_str = d.date().isoformat() if isinstance(d, pd.Timestamp) else str(d)[:10]
        rows.append([date_str, v4, v421, macd_flag])
    return rows


def encode_rows(rows) -> bytes:
    raw = json.dumps(rows, separators=(",", ":")).encode("utf-8")
    return zlib.compress(raw, level=6)


def process_batch(redis_conn, symbols, daily_data, n=55, m=34, ttl=86400 * 30, max_value_bytes=512 * 1024):
    stored = 0
    skipped = 0
    pipe = redis_conn.pipeline(transaction=False)
    for sym_str in symbols:
        df = daily_data.get(sym_str)
        if df is None or df.empty or len(df) < 55:
            continue

        rows = compute_strategy_style_rows(df, n, m)
        if not rows:
            continue

        payload = encode_rows(rows)
        if len(payload) > max_value_bytes:
            skipped += 1
            continue
        pipe.setex(f"ind:v4gv_macd:{sym_str}:{n}:{m}", ttl, payload)
        stored += 1

    if stored:
        pipe.execute()
    return stored, skipped


def resolve_symbols(args) -> list[str]:
    if getattr(args, "symbols_file", ""):
        text = Path(args.symbols_file).read_text(encoding="utf-8")
        return [s.strip() for s in text.replace("\n", ",").split(",") if s.strip()]
    if args.watchlist:
        import sqlite3

        db = Path(__file__).parent.parent.parent / "data" / "gaoshou.db"
        with sqlite3.connect(db) as conn:
            rows = conn.execute(
                "SELECT symbol FROM watchlist_stocks WHERE group_id = ? ORDER BY symbol",
                (args.watchlist,),
            ).fetchall()
        return [r[0] for r in rows]
    if args.symbols:
        return [s.strip() for s in args.symbols.split(",") if s.strip()]
    return []


def main():
    parser = argparse.ArgumentParser(description="Batch pre-compute indicators into Redis")
    parser.add_argument("--symbols", default="", help="Comma-separated stock symbols")
    parser.add_argument("--symbols-file", default="", help="File containing comma/newline separated stock symbols")
    parser.add_argument("--watchlist", type=int, default=0, help="Watchlist group ID")
    parser.add_argument("--start", default="2015-01-01")
    parser.add_argument("--end", default="2026-04-11")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--n", type=int, default=55)
    parser.add_argument("--m", type=int, default=34)
    parser.add_argument("--ttl-days", type=int, default=30)
    parser.add_argument("--max-value-kb", type=int, default=512)
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)
    symbols = resolve_symbols(args)
    if not symbols:
        print("ERROR: provide --symbols or --watchlist")
        return 1

    print(f"Batch compute: {len(symbols)} stocks, {start_date} -> {end_date}")
    print(f"Batch size: {args.batch_size}, params: n={args.n} m={args.m}")
    print("Redis layout: one compressed key per symbol/parameter pair")

    ch = get_clickhouse_client()
    redis_conn = get_redis_client()

    total_stored = 0
    total_skipped = 0
    t0 = time.time()
    batches = [symbols[i:i + args.batch_size] for i in range(0, len(symbols), args.batch_size)]

    for bi, batch in enumerate(batches):
        t_batch = time.time()
        daily_data = load_daily_kline(ch, batch, start_date, end_date)
        stored, skipped = process_batch(
            redis_conn,
            batch,
            daily_data,
            n=args.n,
            m=args.m,
            ttl=args.ttl_days * 86400,
            max_value_bytes=args.max_value_kb * 1024,
        )
        total_stored += stored
        total_skipped += skipped

        del daily_data
        gc.collect()

        elapsed = time.time() - t_batch
        progress = (bi + 1) / len(batches) * 100
        total_elapsed = time.time() - t0
        eta = total_elapsed / (bi + 1) * (len(batches) - bi - 1) if bi + 1 < len(batches) else 0
        print(
            f"[{bi + 1}/{len(batches)}] {progress:.0f}% | stored {stored} | "
            f"skipped {skipped} | batch {elapsed:.1f}s | ETA {eta:.0f}s"
        )

    ch.disconnect()
    print(f"\nDone: {total_stored} keys, skipped {total_skipped}, elapsed {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())

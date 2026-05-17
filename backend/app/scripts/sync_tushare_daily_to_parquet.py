"""Sync Tushare A-share daily bars into platform Parquet/SQLite stores.

The script fetches market-wide daily bars by trade_date, which is much faster
than requesting each stock separately. It also optionally fetches daily_basic
into SQLite for point-in-time market-cap ranking used by small-cap strategies.
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from loguru import logger

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.scripts.fill_small_cap_missing_data import ensure_reference_tables


DEFAULT_STATE_DB = Path(r"E:\Projects\GaoshouPlatform\data\parquet\import_state\tushare_daily_sync.sqlite")


def parse_yyyymmdd(value: str) -> date:
    value = value.replace("-", "")
    return datetime.strptime(value, "%Y%m%d").date()


def yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def iter_days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def init_state(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_days (
            trade_date TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            daily_rows INTEGER NOT NULL DEFAULT 0,
            daily_basic_rows INTEGER NOT NULL DEFAULT 0,
            limit_rows INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(sync_days)").fetchall()}
    if "limit_rows" not in columns:
        conn.execute("ALTER TABLE sync_days ADD COLUMN limit_rows INTEGER NOT NULL DEFAULT 0")
    conn.commit()
    return conn


def get_status(conn: sqlite3.Connection, trade_date: date) -> str | None:
    row = conn.execute("SELECT status FROM sync_days WHERE trade_date=?", (trade_date.isoformat(),)).fetchone()
    return str(row[0]) if row else None


def mark_day(
    conn: sqlite3.Connection,
    trade_date: date,
    *,
    status: str,
    daily_rows: int = 0,
    daily_basic_rows: int = 0,
    limit_rows: int = 0,
    error: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO sync_days(trade_date, status, daily_rows, daily_basic_rows, limit_rows, error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_date) DO UPDATE SET
            status=excluded.status,
            daily_rows=excluded.daily_rows,
            daily_basic_rows=excluded.daily_basic_rows,
            limit_rows=excluded.limit_rows,
            error=excluded.error,
            updated_at=excluded.updated_at
        """,
        (
            trade_date.isoformat(),
            status,
            int(daily_rows),
            int(daily_basic_rows),
            int(limit_rows),
            error,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()


def tushare_client(token: str):
    import tushare as ts

    ts.set_token(token)
    return ts.pro_api()


def fetch_daily(pro: Any, trade_date: date) -> pd.DataFrame:
    df = pro.daily(
        trade_date=yyyymmdd(trade_date),
        fields="ts_code,trade_date,open,high,low,close,vol,amount",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame(
        {
            "symbol": df["ts_code"].astype(str),
            "trade_date": pd.to_datetime(df["trade_date"].astype(str)).dt.date,
            "open": pd.to_numeric(df["open"], errors="coerce"),
            "high": pd.to_numeric(df["high"], errors="coerce"),
            "low": pd.to_numeric(df["low"], errors="coerce"),
            "close": pd.to_numeric(df["close"], errors="coerce"),
            "volume": pd.to_numeric(df["vol"], errors="coerce").fillna(0),
            "amount": pd.to_numeric(df["amount"], errors="coerce").fillna(0) * 1000.0,
        }
    )
    out = out.dropna(subset=["open", "high", "low", "close"])
    out = out[(out["open"] > 0) & (out["high"] > 0) & (out["low"] > 0) & (out["close"] > 0)]
    out = out[out["high"] >= out["low"]]
    return out


def fetch_index_daily(pro: Any, symbols: list[str], trade_date: date) -> pd.DataFrame:
    frames = []
    for symbol in symbols:
        df = pro.index_daily(ts_code=symbol, start_date=yyyymmdd(trade_date), end_date=yyyymmdd(trade_date))
        if df is None or df.empty:
            continue
        frames.append(
            pd.DataFrame(
                {
                    "symbol": df["ts_code"].astype(str),
                    "trade_date": pd.to_datetime(df["trade_date"].astype(str)).dt.date,
                    "open": pd.to_numeric(df["open"], errors="coerce"),
                    "high": pd.to_numeric(df["high"], errors="coerce"),
                    "low": pd.to_numeric(df["low"], errors="coerce"),
                    "close": pd.to_numeric(df["close"], errors="coerce"),
                    "volume": pd.to_numeric(df["vol"], errors="coerce").fillna(0),
                    "amount": pd.to_numeric(df["amount"], errors="coerce").fillna(0) * 1000.0,
                }
            )
        )
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).dropna(subset=["open", "high", "low", "close"])


def fetch_daily_basic(pro: Any, trade_date: date) -> pd.DataFrame:
    df = pro.daily_basic(
        trade_date=yyyymmdd(trade_date),
        fields="ts_code,trade_date,total_share,float_share,total_mv,circ_mv,turnover_rate,pe_ttm,pb",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame(
        {
            "symbol": df["ts_code"].astype(str),
            "trade_date": pd.to_datetime(df["trade_date"].astype(str)).dt.date,
            "total_share": pd.to_numeric(df["total_share"], errors="coerce"),
            "float_share": pd.to_numeric(df["float_share"], errors="coerce"),
            "total_mv": pd.to_numeric(df["total_mv"], errors="coerce"),
            "circ_mv": pd.to_numeric(df["circ_mv"], errors="coerce"),
            "turnover_rate": pd.to_numeric(df["turnover_rate"], errors="coerce"),
            "pe_ttm": pd.to_numeric(df["pe_ttm"], errors="coerce"),
            "pb": pd.to_numeric(df["pb"], errors="coerce"),
        }
    )
    return out


def fetch_limit_prices(pro: Any, trade_date: date) -> pd.DataFrame:
    df = pro.stk_limit(
        trade_date=yyyymmdd(trade_date),
        fields="ts_code,trade_date,up_limit,down_limit",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    out = pd.DataFrame(
        {
            "symbol": df["ts_code"].astype(str),
            "trade_date": pd.to_datetime(df["trade_date"].astype(str)).dt.date,
            "up_limit": pd.to_numeric(df["up_limit"], errors="coerce"),
            "down_limit": pd.to_numeric(df["down_limit"], errors="coerce"),
        }
    )
    return out.dropna(subset=["up_limit", "down_limit"])


def write_limit_prices_sqlite(db_path: Path, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    ensure_reference_tables(db_path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                str(row["symbol"]),
                row["trade_date"].isoformat() if hasattr(row["trade_date"], "isoformat") else str(row["trade_date"]),
                none_if_nan(row.get("up_limit")),
                none_if_nan(row.get("down_limit")),
                "tushare.stk_limit",
                now,
                now,
            )
        )
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO stock_limit_prices (
                symbol, trade_date, up_limit, down_limit, source, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, trade_date) DO UPDATE SET
                up_limit = COALESCE(excluded.up_limit, stock_limit_prices.up_limit),
                down_limit = COALESCE(excluded.down_limit, stock_limit_prices.down_limit),
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def write_daily_basic_sqlite(db_path: Path, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    ensure_reference_tables(db_path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                str(row["symbol"]),
                row["trade_date"].isoformat() if hasattr(row["trade_date"], "isoformat") else str(row["trade_date"]),
                none_if_nan(row.get("total_share")),
                none_if_nan(row.get("float_share")),
                none_if_nan(row.get("total_mv")),
                none_if_nan(row.get("circ_mv")),
                none_if_nan(row.get("turnover_rate")),
                none_if_nan(row.get("pe_ttm")),
                none_if_nan(row.get("pb")),
                "tushare.daily_basic",
                now,
                now,
            )
        )
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO stock_daily_basic (
                symbol, trade_date, total_share, float_share, total_mv, circ_mv,
                turnover_rate, pe_ttm, pb, source, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, trade_date) DO UPDATE SET
                total_share = COALESCE(excluded.total_share, stock_daily_basic.total_share),
                float_share = COALESCE(excluded.float_share, stock_daily_basic.float_share),
                total_mv = COALESCE(excluded.total_mv, stock_daily_basic.total_mv),
                circ_mv = COALESCE(excluded.circ_mv, stock_daily_basic.circ_mv),
                turnover_rate = COALESCE(excluded.turnover_rate, stock_daily_basic.turnover_rate),
                pe_ttm = COALESCE(excluded.pe_ttm, stock_daily_basic.pe_ttm),
                pb = COALESCE(excluded.pb, stock_daily_basic.pb),
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def none_if_nan(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="20200101")
    parser.add_argument("--end", default="20260515")
    parser.add_argument("--token", default=os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN") or "")
    parser.add_argument("--state-db", default=str(DEFAULT_STATE_DB))
    parser.add_argument("--sqlite-db", default=str(Path(settings.data_dir) / "gaoshou.db"))
    parser.add_argument("--index-symbols", default="399101.SZ,000001.SH")
    parser.add_argument("--pause", type=float, default=0.16)
    parser.add_argument("--limit-days", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-daily-basic", action="store_true")
    parser.add_argument("--skip-limit-prices", action="store_true")
    parser.add_argument("--only-limit-prices", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def setup_logger(level: str) -> None:
    logger.remove()
    logger.add(sys.stderr, level=level)


def main() -> int:
    args = parse_args()
    setup_logger(args.log_level)
    start = parse_yyyymmdd(args.start)
    end = parse_yyyymmdd(args.end)
    token = args.token.strip()
    if not token:
        try:
            import tushare as ts

            token = (ts.get_token() or "").strip()
        except Exception:
            token = ""
    if not token:
        raise RuntimeError("Tushare token is required. Set TUSHARE_TOKEN or pass --token.")
    pro = tushare_client(token)
    state = init_state(Path(args.state_db))
    store = get_market_data_store()
    sqlite_db = Path(args.sqlite_db)
    index_symbols = [s.strip() for s in args.index_symbols.split(",") if s.strip()]

    done = 0
    skipped = 0
    failed = 0
    total_daily = 0
    total_basic = 0
    total_limits = 0
    days = list(iter_days(start, end))
    if args.limit_days > 0:
        days = days[: args.limit_days]
    for trade_date in days:
        if not args.force and get_status(state, trade_date) == "success":
            skipped += 1
            continue
        try:
            if args.only_limit_prices:
                limits = fetch_limit_prices(pro, trade_date)
                limit_rows = write_limit_prices_sqlite(sqlite_db, limits)
                total_limits += limit_rows
                done += 1
                mark_day(state, trade_date, status="success", limit_rows=limit_rows)
                logger.info("{} limits={} total_limits={}", trade_date, limit_rows, total_limits)
                if args.pause > 0:
                    time.sleep(args.pause)
                continue
            daily = fetch_daily(pro, trade_date)
            idx = fetch_index_daily(pro, index_symbols, trade_date)
            if not idx.empty:
                daily = pd.concat([daily, idx], ignore_index=True)
            basic_rows = 0
            limit_rows = 0
            if daily.empty:
                if not args.skip_limit_prices:
                    limits = fetch_limit_prices(pro, trade_date)
                    limit_rows = write_limit_prices_sqlite(sqlite_db, limits)
                mark_day(
                    state,
                    trade_date,
                    status="success",
                    daily_rows=0,
                    daily_basic_rows=0,
                    limit_rows=limit_rows,
                )
                skipped += 1
                continue
            written = store.write_daily(daily)
            if not args.skip_daily_basic:
                basic = fetch_daily_basic(pro, trade_date)
                basic_rows = write_daily_basic_sqlite(sqlite_db, basic)
            if not args.skip_limit_prices:
                limits = fetch_limit_prices(pro, trade_date)
                limit_rows = write_limit_prices_sqlite(sqlite_db, limits)
            total_daily += written
            total_basic += basic_rows
            total_limits += limit_rows
            done += 1
            mark_day(
                state,
                trade_date,
                status="success",
                daily_rows=written,
                daily_basic_rows=basic_rows,
                limit_rows=limit_rows,
            )
            logger.info(
                "{} daily={} daily_basic={} limits={} total_daily={}",
                trade_date,
                written,
                basic_rows,
                limit_rows,
                total_daily,
            )
            if args.pause > 0:
                time.sleep(args.pause)
        except Exception as exc:
            failed += 1
            mark_day(state, trade_date, status="failed", error=str(exc)[:2000])
            logger.exception("{} failed", trade_date)
            if args.pause > 0:
                time.sleep(max(args.pause, 1.0))
    logger.info(
        "done={} skipped={} failed={} daily_rows={} daily_basic_rows={} limit_rows={} state_db={}",
        done,
        skipped,
        failed,
        total_daily,
        total_basic,
        total_limits,
        args.state_db,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

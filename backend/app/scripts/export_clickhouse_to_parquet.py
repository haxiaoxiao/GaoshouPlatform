#!/usr/bin/env python
"""将 ClickHouse 行情数据导出为 Parquet 文件

用法:
    python -m app.scripts.export_clickhouse_to_parquet \
        --dataset klines_daily \
        --start 20250101 \
        --end 20251231 \
        --output E:/Projects/GaoshouPlatform/data/parquet

    python -m app.scripts.export_clickhouse_to_parquet \
        --dataset klines_minute_timer \
        --start 20250101 \
        --end 20251231 \
        --symbols 000001.SZ,600000.SH \
        --timer-times 10:00,10:30,14:30,14:50 \
        --overwrite
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

# 确保 backend 在 path 中
_BACKEND = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from app.data_stores.parquet_store import ParquetMarketDataStore
from app.db.clickhouse import get_ch_client


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def export_daily(
    store: ParquetMarketDataStore,
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    chunk_size: int = 500,
) -> int:
    """导出 klines_daily 到 Parquet"""
    ch = get_ch_client()
    total = 0

    # 获取列
    cols_info = ch.execute("SELECT name FROM system.columns WHERE table = 'klines_daily'")
    available = [r[0] for r in cols_info]
    wanted = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]
    extras = [c for c in ["turnover_rate"] if c in available]
    all_cols = wanted + extras
    col_str = ", ".join(all_cols)

    # 构建查询条件
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

    # 分批导出
    offset = 0
    while True:
        rows = ch.execute(
            f"SELECT {col_str} FROM klines_daily {where} "
            f"ORDER BY trade_date, symbol "
            f"LIMIT {chunk_size} OFFSET {offset}",
            params,
        )
        if not rows:
            break
        df = pd.DataFrame(rows, columns=all_cols)
        for col in ["open", "high", "low", "close", "amount"] + extras:
            if col in df.columns:
                df[col] = df[col].astype(float)
        n = store.write_daily(df)
        total += n
        offset += chunk_size
        logger.info(f"  日线已导出 {total} 行...")

    ch.disconnect()
    return total


def export_minute_timer(
    store: ParquetMarketDataStore,
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    timer_times: list[str] | None = None,
    *,
    chunk_size: int = 2000,
) -> int:
    """导出 klines_minute timer 点到 Parquet"""
    ch = get_ch_client()
    total = 0

    # 构建 timer 过滤
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
        where += f" {time_filter}" if not where else f" {time_filter}"

    offset = 0
    while True:
        rows = ch.execute(
            f"SELECT symbol, datetime, open, high, low, close, volume, amount "
            f"FROM klines_minute {where} "
            f"ORDER BY datetime, symbol "
            f"LIMIT {chunk_size} OFFSET {offset}",
            params,
        )
        if not rows:
            break
        df = pd.DataFrame(
            rows,
            columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"],
        )
        for col in ["open", "high", "low", "close", "amount"]:
            df[col] = df[col].astype(float)
        n = store.write_minute(df, dataset="klines_minute_timer")
        total += n
        offset += chunk_size
        logger.info(f"  分钟线已导出 {total} 行...")

    ch.disconnect()
    return total


def export_factor_cache(
    store: ParquetMarketDataStore,
    symbols: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    *,
    chunk_size: int = 5000,
) -> int:
    """导出 factor_cache 到 Parquet"""
    ch = get_ch_client()
    total = 0

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

    offset = 0
    while True:
        rows = ch.execute(
            f"SELECT symbol, trade_date, expr_hash, value, "
            f"       COALESCE(engine, '') AS engine, "
            f"       COALESCE(expression, '') AS expression, "
            f"       created_at AS updated_at "
            f"FROM factor_cache {where} "
            f"ORDER BY trade_date, expr_hash, symbol "
            f"LIMIT {chunk_size} OFFSET {offset}",
            params,
        )
        if not rows:
            break
        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "expr_hash", "value", "engine", "expression", "updated_at"],
        )
        n = store._write_partitioned(df, dataset="factor_cache", date_col="trade_date")
        total += n
        offset += chunk_size
        logger.info(f"  因子缓存已导出 {total} 行...")

    ch.disconnect()
    return total


def main():
    parser = argparse.ArgumentParser(description="导出 ClickHouse 行情数据为 Parquet")
    parser.add_argument("--dataset", default="all", choices=["klines_daily", "klines_minute_timer", "factor_cache", "all"])
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--symbols", type=str, default=None, help="股票代码，逗号分隔")
    parser.add_argument("--timer-times", type=str, default=None, help="分钟时间点，逗号分隔，如 10:00,10:30")
    parser.add_argument("--output", type=str, default="E:/Projects/GaoshouPlatform/data/parquet", help="Parquet 输出目录")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有文件")
    args = parser.parse_args()

    start_date = parse_date(args.start) if args.start else None
    end_date = parse_date(args.end) if args.end else None
    symbols = [s.strip() for s in args.symbols.split(",")] if args.symbols else None
    timer_times = [t.strip() for t in args.timer_times.split(",")] if args.timer_times else None

    store = ParquetMarketDataStore(data_dir=args.output)

    if args.overwrite:
        import shutil
        for d in ["klines_daily", "klines_minute_timer", "factor_cache"]:
            p = store._dataset_path(d)
            if p.exists():
                shutil.rmtree(p)
                logger.info(f"已删除旧数据: {p}")

    results = {}

    if args.dataset in ("klines_daily", "all"):
        logger.info(f"导出 klines_daily: {start_date} ~ {end_date}")
        n = export_daily(store, symbols, start_date, end_date)
        results["klines_daily"] = n
        logger.info(f"klines_daily 完成: {n} 行")

    if args.dataset in ("klines_minute_timer", "all"):
        logger.info(f"导出 klines_minute_timer: {start_date} ~ {end_date}")
        n = export_minute_timer(store, symbols, start_date, end_date, timer_times)
        results["klines_minute_timer"] = n
        logger.info(f"klines_minute_timer 完成: {n} 行")

    if args.dataset in ("factor_cache", "all"):
        logger.info(f"导出 factor_cache: {start_date} ~ {end_date}")
        n = export_factor_cache(store, symbols, start_date, end_date)
        results["factor_cache"] = n
        logger.info(f"factor_cache 完成: {n} 行")

    logger.info(f"全部导出完成: {results}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

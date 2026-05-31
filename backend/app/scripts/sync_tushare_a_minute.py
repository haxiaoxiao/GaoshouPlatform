#!/usr/bin/env python
"""轮询 Tushare token 同步 A 股历史分钟 K 线到 Parquet。

默认输出目录：
    E:/Projects/QuantData/tushare_a_minute

数据布局：
    parquet/
      klines_minute/
        freq=1min/year=2016/month=01/000001.SZ.parquet
    meta/
      stocks.parquet
      trade_cal.parquet
    state/
      sync_state.sqlite
    logs/
      sync_tushare_a_minute.log

典型用法：
    python -m app.scripts.sync_tushare_a_minute --start 20160101 --end 20260515

小范围冒烟：
    python -m app.scripts.sync_tushare_a_minute --symbols 000001.SZ,600000.SH --start 20250101 --end 20250131 --force

说明：
    Tushare stk_mins 接口单次最多 8000 行，1 分钟 A 股一个月约 5000 行，
    因此本脚本默认按「单股票 x 单月」分块请求，便于断点续跑。
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
from loguru import logger

DEFAULT_ROOT = Path(r"E:\Projects\QuantData\tushare_a_minute")
DEFAULT_TOKENS_FILE = Path(r"E:\Projects\GaoshouPlatform\TushareTokensForA.md")
PREFERRED_TOKEN_PREFIX = "c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325"
MINUTE_FREQS = {"1min", "5min", "15min", "30min", "60min"}


def parse_yyyymmdd(value: str) -> date:
    return datetime.strptime(value, "%Y%m%d").date()


def yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def month_start(value: date) -> date:
    return date(value.year, value.month, 1)


def next_month(value: date) -> date:
    return date(value.year + (1 if value.month == 12 else 0), 1 if value.month == 12 else value.month + 1, 1)


def month_end(value: date) -> date:
    return next_month(value) - timedelta(days=1)


def iter_months(start: date, end: date) -> Iterable[tuple[date, date]]:
    cursor = month_start(start)
    while cursor <= end:
        s = max(start, cursor)
        e = min(end, month_end(cursor))
        yield s, e
        cursor = next_month(cursor)


def token_label(token: str) -> str:
    return f"{token[:6]}...{token[-4:]}"


def read_tokens(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    tokens = re.findall(r"\b[0-9a-fA-F]{40,}\b", text)
    seen: set[str] = set()
    result: list[str] = []
    for token in tokens:
        token = token.strip()
        if token and token not in seen:
            seen.add(token)
            result.append(token)
    result.sort(key=lambda t: 0 if t == PREFERRED_TOKEN_PREFIX else 1)
    if not result:
        raise RuntimeError(f"no tushare tokens found in {path}")
    return result


class TokenPool:
    """Round-robin Tushare token manager."""

    def __init__(self, tokens: list[str], cooldown_seconds: float = 60.0):
        self._tokens = tokens
        self._cooldown_seconds = cooldown_seconds
        self._index = 0
        self._cooldown_until: dict[str, float] = {}
        self._disabled: set[str] = set()

    def next(self) -> str:
        now = time.time()
        for _ in range(len(self._tokens)):
            token = self._tokens[self._index % len(self._tokens)]
            self._index += 1
            if token in self._disabled:
                continue
            if self._cooldown_until.get(token, 0) <= now:
                return token
        active_cooldowns = [v for t, v in self._cooldown_until.items() if t not in self._disabled]
        if not active_cooldowns:
            raise RuntimeError("no active Tushare tokens available")
        sleep_for = min(active_cooldowns) - now
        if sleep_for > 0:
            logger.warning("all tokens cooling down, sleep {:.1f}s", sleep_for)
            time.sleep(sleep_for)
        return self.next()

    def penalize(self, token: str, seconds: float | None = None) -> None:
        self._cooldown_until[token] = time.time() + (seconds or self._cooldown_seconds)

    def disable(self, token: str) -> None:
        self._disabled.add(token)
        logger.warning("disabled invalid token {}", token_label(token))


def classify_tushare_error(error: Exception) -> str:
    text = str(error)
    if "token不对" in text or "token无效" in text:
        return "invalid_token"
    if "频率超限" in text:
        if "次/天" in text or "每天" in text:
            return "daily_rate_limit"
        return "rate_limit"
    return "other"


def handle_tushare_error(token_pool: TokenPool, token: str, error: Exception, *, daily_cooldown: float = 24 * 3600) -> None:
    kind = classify_tushare_error(error)
    if kind == "invalid_token":
        token_pool.disable(token)
    elif kind == "daily_rate_limit":
        token_pool.penalize(token, daily_cooldown)
    else:
        token_pool.penalize(token)


@dataclass(frozen=True)
class Task:
    ts_code: str
    freq: str
    start: date
    end: date

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.ts_code, self.freq, yyyymmdd(self.start), yyyymmdd(self.end))


class StateDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_tasks (
                ts_code TEXT NOT NULL,
                freq TEXT NOT NULL,
                chunk_start TEXT NOT NULL,
                chunk_end TEXT NOT NULL,
                status TEXT NOT NULL,
                rows INTEGER DEFAULT 0,
                attempts INTEGER DEFAULT 0,
                token_label TEXT,
                output_file TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (ts_code, freq, chunk_start, chunk_end)
            )
            """
        )
        self.conn.commit()

    def is_done(self, task: Task) -> bool:
        row = self.conn.execute(
            """
            SELECT status FROM sync_tasks
            WHERE ts_code=? AND freq=? AND chunk_start=? AND chunk_end=?
            """,
            task.key,
        ).fetchone()
        return bool(row and row[0] == "done")

    def mark_running(self, task: Task, token: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            """
            INSERT INTO sync_tasks
              (ts_code, freq, chunk_start, chunk_end, status, attempts, token_label, updated_at)
            VALUES (?, ?, ?, ?, 'running', 1, ?, ?)
            ON CONFLICT(ts_code, freq, chunk_start, chunk_end) DO UPDATE SET
              status='running',
              attempts=attempts+1,
              token_label=excluded.token_label,
              updated_at=excluded.updated_at
            """,
            (*task.key, token_label(token), now),
        )
        self.conn.commit()

    def mark_done(self, task: Task, rows: int, output_file: Path | None, token: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            """
            INSERT INTO sync_tasks
              (ts_code, freq, chunk_start, chunk_end, status, rows, attempts, token_label, output_file, updated_at)
            VALUES (?, ?, ?, ?, 'done', ?, 1, ?, ?, ?)
            ON CONFLICT(ts_code, freq, chunk_start, chunk_end) DO UPDATE SET
              status='done',
              rows=excluded.rows,
              token_label=excluded.token_label,
              output_file=excluded.output_file,
              last_error=NULL,
              updated_at=excluded.updated_at
            """,
            (*task.key, rows, token_label(token), str(output_file) if output_file else None, now),
        )
        self.conn.commit()

    def mark_failed(self, task: Task, error: str, token: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            """
            INSERT INTO sync_tasks
              (ts_code, freq, chunk_start, chunk_end, status, attempts, token_label, last_error, updated_at)
            VALUES (?, ?, ?, ?, 'failed', 1, ?, ?, ?)
            ON CONFLICT(ts_code, freq, chunk_start, chunk_end) DO UPDATE SET
              status='failed',
              attempts=attempts+1,
              token_label=excluded.token_label,
              last_error=excluded.last_error,
              updated_at=excluded.updated_at
            """,
            (*task.key, token_label(token), error[:1000], now),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def make_pro(token: str):
    import tushare as ts

    try:
        return ts.pro_api(token)
    except TypeError:
        ts.set_token(token)
        return ts.pro_api()


def fetch_stock_basic(token_pool: TokenPool, retries: int = 3) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    statuses = ["L", "D", "P"]
    for status in statuses:
        last_error: Exception | None = None
        for _ in range(retries):
            token = token_pool.next()
            try:
                pro = make_pro(token)
                df = pro.stock_basic(
                    exchange="",
                    list_status=status,
                    fields="ts_code,symbol,name,area,industry,market,exchange,list_status,list_date,delist_date,is_hs",
                )
                frames.append(df)
                logger.info("stock_basic list_status={} rows={} token={}", status, len(df), token_label(token))
                break
            except Exception as exc:
                last_error = exc
                handle_tushare_error(token_pool, token, exc)
                logger.warning("stock_basic failed status={} token={} error={}", status, token_label(token), exc)
        else:
            raise RuntimeError(f"stock_basic failed for list_status={status}: {last_error}")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts_code"], keep="first")
    return df.sort_values("ts_code").reset_index(drop=True)


def fetch_trade_cal(token_pool: TokenPool, start: date, end: date, retries: int = 3) -> pd.DataFrame:
    last_error: Exception | None = None
    for _ in range(retries):
        token = token_pool.next()
        try:
            pro = make_pro(token)
            df = pro.trade_cal(
                exchange="",
                start_date=yyyymmdd(start),
                end_date=yyyymmdd(end),
                fields="exchange,cal_date,is_open,pretrade_date",
            )
            logger.info("trade_cal rows={} token={}", len(df), token_label(token))
            return df
        except Exception as exc:
            last_error = exc
            handle_tushare_error(token_pool, token, exc)
            logger.warning("trade_cal failed token={} error={}", token_label(token), exc)
    raise RuntimeError(f"trade_cal failed: {last_error}")


def active_symbols(stocks: pd.DataFrame, start: date, end: date) -> list[str]:
    if stocks.empty:
        return []
    df = stocks.copy()
    list_dt = pd.to_datetime(df["list_date"], errors="coerce")
    delist_dt = pd.to_datetime(df["delist_date"], errors="coerce")
    df["list_dt"] = [v.date() if pd.notna(v) else None for v in list_dt]
    df["delist_dt"] = [v.date() if pd.notna(v) else None for v in delist_dt]
    mask = (df["list_dt"].isna() | (df["list_dt"] <= end)) & (df["delist_dt"].isna() | (df["delist_dt"] >= start))
    return df.loc[mask, "ts_code"].dropna().astype(str).sort_values().tolist()


def task_overlaps_symbol_life(stocks_by_code: dict[str, tuple[date | None, date | None]], task: Task) -> bool:
    list_date, delist_date = stocks_by_code.get(task.ts_code, (None, None))
    if list_date and list_date > task.end:
        return False
    if delist_date and delist_date < task.start:
        return False
    return True


def output_path(root: Path, task: Task) -> Path:
    return (
        root
        / "parquet"
        / "klines_minute"
        / f"freq={task.freq}"
        / f"year={task.start.year:04d}"
        / f"month={task.start.month:02d}"
        / f"{task.ts_code}.parquet"
    )


def normalize_minute_df(df: pd.DataFrame, task: Task) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rename = {
        "ts_code": "symbol",
        "trade_time": "datetime",
        "vol": "volume",
    }
    df = df.rename(columns=rename).copy()
    required = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"missing columns from tushare response: {missing}")

    df = df[required]
    df["symbol"] = df["symbol"].fillna(task.ts_code).astype(str)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])
    df = df[(df["datetime"].dt.date >= task.start) & (df["datetime"].dt.date <= task.end)]
    if df.empty:
        return df
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    df["trade_date"] = df["datetime"].dt.date
    df["minute"] = df["datetime"].dt.hour * 60 + df["datetime"].dt.minute
    df["source"] = "tushare.stk_mins"
    df["updated_at"] = pd.Timestamp.now()
    df = df.drop_duplicates(subset=["symbol", "datetime"], keep="last")
    return df.sort_values(["symbol", "datetime"]).reset_index(drop=True)


def write_parquet(df: pd.DataFrame, path: Path) -> int:
    if df.empty:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp.parquet")
    df.to_parquet(tmp, index=False, engine="pyarrow", compression="zstd")
    tmp.replace(path)
    return len(df)


def load_or_fetch_stock_basic(root: Path, token_pool: TokenPool, retries: int, refresh: bool) -> pd.DataFrame:
    path = root / "meta" / "stocks.parquet"
    if path.exists() and not refresh:
        df = pd.read_parquet(path)
        logger.info("loaded cached stock_basic rows={} from {}", len(df), path)
        return df
    df = fetch_stock_basic(token_pool, retries=retries)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
    return df


def load_or_fetch_trade_cal(root: Path, token_pool: TokenPool, start: date, end: date, retries: int, refresh: bool) -> pd.DataFrame:
    path = root / "meta" / "trade_cal.parquet"
    if path.exists() and not refresh:
        df = pd.read_parquet(path)
        if "cal_date" in df.columns:
            cal_dates = pd.to_datetime(df["cal_date"], errors="coerce")
            if cal_dates.notna().any():
                min_date = cal_dates.min().date()
                max_date = cal_dates.max().date()
                if min_date <= start and max_date >= end:
                    logger.info("loaded cached trade_cal rows={} from {}", len(df), path)
                    return df
    df = fetch_trade_cal(token_pool, start, end, retries=retries)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
    return df


def fetch_minutes(task: Task, token_pool: TokenPool, api: str, retries: int, sleep_seconds: float) -> tuple[pd.DataFrame, str]:
    last_error: Exception | None = None
    start_text = f"{task.start:%Y-%m-%d} 00:00:00"
    end_text = f"{task.end:%Y-%m-%d} 23:59:59"
    for _ in range(retries):
        token = token_pool.next()
        try:
            if api == "stk_mins":
                pro = make_pro(token)
                df = pro.stk_mins(
                    ts_code=task.ts_code,
                    freq=task.freq,
                    start_date=start_text,
                    end_date=end_text,
                )
            elif api == "pro_bar":
                import tushare as ts

                ts.set_token(token)
                df = ts.pro_bar(
                    ts_code=task.ts_code,
                    asset="E",
                    freq=task.freq,
                    start_date=start_text,
                    end_date=end_text,
                )
            else:
                raise ValueError(f"unsupported api: {api}")
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            return normalize_minute_df(df, task), token
        except Exception as exc:
            last_error = exc
            handle_tushare_error(token_pool, token, exc)
            logger.warning(
                "fetch failed symbol={} {}~{} token={} error={}",
                task.ts_code,
                yyyymmdd(task.start),
                yyyymmdd(task.end),
                token_label(token),
                exc,
            )
    raise RuntimeError(f"fetch failed after {retries} retries: {last_error}")


def setup_logger(root: Path) -> None:
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add(log_dir / "sync_tushare_a_minute.log", level="INFO", rotation="100 MB", retention=20, encoding="utf-8")


def build_tasks(symbols: list[str], stocks: pd.DataFrame, start: date, end: date, freq: str) -> list[Task]:
    stocks_by_code: dict[str, tuple[date | None, date | None]] = {}
    if not stocks.empty:
        tmp = stocks.copy()
        list_dt = pd.to_datetime(tmp["list_date"], errors="coerce")
        delist_dt = pd.to_datetime(tmp["delist_date"], errors="coerce")
        tmp["list_dt"] = [v.date() if pd.notna(v) else None for v in list_dt]
        tmp["delist_dt"] = [v.date() if pd.notna(v) else None for v in delist_dt]
        for row in tmp.itertuples(index=False):
            stocks_by_code[str(row.ts_code)] = (row.list_dt, row.delist_dt)
    tasks: list[Task] = []
    for symbol in symbols:
        for s, e in iter_months(start, end):
            task = Task(symbol, freq, s, e)
            if task_overlaps_symbol_life(stocks_by_code, task):
                tasks.append(task)
    return tasks


def main() -> int:
    parser = argparse.ArgumentParser(description="轮询 Tushare token 同步 A股历史分钟 K线到 Parquet")
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="输出根目录")
    parser.add_argument("--tokens-file", default=str(DEFAULT_TOKENS_FILE), help="Tushare token 文件")
    parser.add_argument("--start", default="20160101", help="开始日期 YYYYMMDD")
    parser.add_argument("--end", default=yyyymmdd(date.today()), help="结束日期 YYYYMMDD")
    parser.add_argument("--freq", default="1min", choices=sorted(MINUTE_FREQS), help="分钟频率")
    parser.add_argument("--api", default="stk_mins", choices=["stk_mins", "pro_bar"], help="Tushare 分钟接口")
    parser.add_argument("--symbols", default="", help="指定股票代码，逗号分隔；为空则全 A")
    parser.add_argument("--limit-symbols", type=int, default=0, help="仅处理前 N 个股票，用于测试")
    parser.add_argument("--max-tasks", type=int, default=0, help="最多执行 N 个任务，用于测试")
    parser.add_argument("--retries", type=int, default=4, help="每个任务最大重试次数")
    parser.add_argument("--sleep", type=float, default=0.25, help="成功请求后的暂停秒数")
    parser.add_argument("--cooldown", type=float, default=60.0, help="token 出错后的冷却秒数")
    parser.add_argument("--force", action="store_true", help="忽略状态库，强制重抓并覆盖文件")
    parser.add_argument("--refresh-meta", action="store_true", help="强制刷新股票列表和交易日历")
    parser.add_argument("--dry-run", action="store_true", help="只生成任务统计，不请求接口")
    args = parser.parse_args()

    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    setup_logger(root)

    start = parse_yyyymmdd(args.start)
    end = parse_yyyymmdd(args.end)
    if start > end:
        raise ValueError("--start must be <= --end")

    tokens = read_tokens(Path(args.tokens_file))
    token_pool = TokenPool(tokens, cooldown_seconds=args.cooldown)
    logger.info("loaded {} tushare tokens from {}", len(tokens), args.tokens_file)

    state = StateDB(root / "state" / "sync_state.sqlite")
    try:
        stocks = load_or_fetch_stock_basic(root, token_pool, args.retries, args.refresh_meta)
        try:
            load_or_fetch_trade_cal(root, token_pool, start, end, args.retries, args.refresh_meta)
        except Exception as exc:
            logger.warning("trade calendar fetch skipped: {}", exc)

        if args.symbols.strip():
            symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        else:
            symbols = active_symbols(stocks, start, end)
        if args.limit_symbols > 0:
            symbols = symbols[: args.limit_symbols]
        tasks = build_tasks(symbols, stocks, start, end, args.freq)
        if args.max_tasks > 0:
            tasks = tasks[: args.max_tasks]

        logger.info(
            "task plan: symbols={} tasks={} freq={} range={}~{} root={}",
            len(symbols),
            len(tasks),
            args.freq,
            yyyymmdd(start),
            yyyymmdd(end),
            root,
        )
        if args.dry_run:
            return 0

        done = skipped = failed = rows_total = 0
        for idx, task in enumerate(tasks, start=1):
            out = output_path(root, task)
            if not args.force and state.is_done(task) and out.exists():
                skipped += 1
                continue
            token = token_pool.next()
            state.mark_running(task, token)
            try:
                # fetch_minutes 会自行轮询 token；mark_running 的 token 只记录首次调度 token。
                df, used_token = fetch_minutes(task, token_pool, args.api, args.retries, args.sleep)
                rows = write_parquet(df, out) if not df.empty else 0
                state.mark_done(task, rows, out if rows else None, used_token)
                done += 1
                rows_total += rows
                if idx == 1 or idx % 100 == 0:
                    logger.info(
                        "progress {}/{} done={} skipped={} failed={} rows={} current={} {}~{}",
                        idx,
                        len(tasks),
                        done,
                        skipped,
                        failed,
                        rows_total,
                        task.ts_code,
                        yyyymmdd(task.start),
                        yyyymmdd(task.end),
                    )
            except Exception as exc:
                failed += 1
                state.mark_failed(task, str(exc), token)
                logger.error(
                    "task failed {}/{} symbol={} {}~{} error={}",
                    idx,
                    len(tasks),
                    task.ts_code,
                    yyyymmdd(task.start),
                    yyyymmdd(task.end),
                    exc,
                )

        logger.info("finished done={} skipped={} failed={} rows={}", done, skipped, failed, rows_total)
        return 0 if failed == 0 else 2
    finally:
        state.close()


if __name__ == "__main__":
    raise SystemExit(main())

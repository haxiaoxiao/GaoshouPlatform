"""Import JoinQuant-style minute CSV archives into platform parquet store.

The 2026-04+ files are zip/tar.gz archives containing one CSV per symbol:
    <date>/<symbol>.csv
    datetime,open,high,low,close,volume,amount
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from app.data_stores.parquet_store import ParquetMarketDataStore

DEFAULT_SOURCE = Path(
    r"E:\Projects\QuantData\JQ_a_minute\闲鱼商品_A股1分钟数据_聚宽版\01_数据文件\202604开始的数据"
)
DEFAULT_TARGET = Path(r"E:\Projects\GaoshouPlatform\data\parquet")


@dataclass(frozen=True)
class ArchiveFile:
    path: Path
    size: int
    mtime_ns: int

    @property
    def key(self) -> str:
        return f"{self.path.resolve()}|{self.size}|{self.mtime_ns}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--target", default=str(DEFAULT_TARGET))
    parser.add_argument("--dataset", default="klines_minute", choices=["klines_minute", "klines_minute_timer"])
    parser.add_argument("--state-db", default=None)
    parser.add_argument("--pattern", default="*.zip", help="*.zip or *.tar.gz")
    parser.add_argument("--start", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD")
    parser.add_argument("--limit-archives", type=int, default=0)
    parser.add_argument("--symbols", default="", help="Comma-separated symbols for testing")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chunk-symbols", type=int, default=500)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def init_state(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS import_archives (
            archive_key TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            status TEXT NOT NULL,
            rows_written INTEGER NOT NULL DEFAULT 0,
            started_at TEXT,
            finished_at TEXT,
            error TEXT
        )
        """
    )
    conn.commit()
    return conn


def mark(conn: sqlite3.Connection, archive: ArchiveFile, status: str, rows: int = 0, error: str | None = None) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    if status == "running":
        conn.execute(
            """
            INSERT INTO import_archives(archive_key, path, status, rows_written, started_at, error)
            VALUES (?, ?, ?, 0, ?, NULL)
            ON CONFLICT(archive_key) DO UPDATE SET status=excluded.status, rows_written=0, started_at=excluded.started_at, error=NULL
            """,
            (archive.key, str(archive.path), status, now),
        )
    else:
        conn.execute(
            "UPDATE import_archives SET status=?, rows_written=?, finished_at=?, error=? WHERE archive_key=?",
            (status, rows, now, error, archive.key),
        )
    conn.commit()


def status_of(conn: sqlite3.Connection, archive: ArchiveFile) -> str | None:
    row = conn.execute("SELECT status FROM import_archives WHERE archive_key=?", (archive.key,)).fetchone()
    return row[0] if row else None


def discover(source: Path, pattern: str) -> list[ArchiveFile]:
    files = sorted(source.glob(pattern))
    out = []
    for p in files:
        if p.is_file():
            st = p.stat()
            out.append(ArchiveFile(p, st.st_size, st.st_mtime_ns))
    return out


def in_date_range(name: str, start: str | None, end: str | None) -> bool:
    # Accept paths containing YYYY-MM-DD.
    import re

    m = re.search(r"20\d{2}-\d{2}-\d{2}", name)
    if not m:
        return True
    d = m.group(0)
    if start and d < start:
        return False
    if end and d > end:
        return False
    return True


def symbol_from_name(name: str) -> str | None:
    stem = Path(name).name
    if not stem.lower().endswith(".csv"):
        return None
    return stem[:-4]


def normalize_frame(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.rename(columns={c: c.lower() for c in df.columns})
    required = ["datetime", "open", "high", "low", "close", "volume", "amount"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{symbol} missing columns: {missing}")
    df = df[required].copy()
    df["symbol"] = symbol
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    for c in ["open", "high", "low", "close", "volume", "amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["datetime", "open", "high", "low", "close", "volume", "amount"])
    df = df[(df["open"] >= 0) & (df["high"] >= 0) & (df["low"] >= 0) & (df["close"] >= 0) & (df["high"] >= df["low"])]
    df["source"] = "jq_archive"
    return df[["symbol", "datetime", "open", "high", "low", "close", "volume", "amount", "source"]]


def read_csv_from_bytes(data, symbol: str) -> pd.DataFrame:
    from io import BytesIO

    df = pd.read_csv(BytesIO(data))
    return normalize_frame(df, symbol)


def flush(store: ParquetMarketDataStore, frames: list[pd.DataFrame], dataset: str) -> int:
    if not frames:
        return 0
    df = pd.concat(frames, ignore_index=True)
    return store.write_minute(df, dataset=dataset)


def import_zip(archive: ArchiveFile, store: ParquetMarketDataStore, args: argparse.Namespace, symbols: set[str]) -> int:
    rows = 0
    frames: list[pd.DataFrame] = []
    with zipfile.ZipFile(archive.path) as zf:
        members = [m for m in zf.infolist() if not m.is_dir() and m.filename.lower().endswith(".csv")]
        for member in members:
            if not in_date_range(member.filename, args.start, args.end):
                continue
            symbol = symbol_from_name(member.filename)
            if not symbol or (symbols and symbol not in symbols):
                continue
            frame = read_csv_from_bytes(zf.read(member), symbol)
            if not frame.empty:
                frames.append(frame)
            if len(frames) >= args.chunk_symbols:
                rows += flush(store, frames, args.dataset)
                frames.clear()
    rows += flush(store, frames, args.dataset)
    return rows


def import_targz(archive: ArchiveFile, store: ParquetMarketDataStore, args: argparse.Namespace, symbols: set[str]) -> int:
    rows = 0
    frames: list[pd.DataFrame] = []
    with tarfile.open(archive.path, mode="r:gz") as tf:
        for member in tf:
            if not member.isfile() or not member.name.lower().endswith(".csv"):
                continue
            if not in_date_range(member.name, args.start, args.end):
                continue
            symbol = symbol_from_name(member.name)
            if not symbol or (symbols and symbol not in symbols):
                continue
            f = tf.extractfile(member)
            if f is None:
                continue
            frame = normalize_frame(pd.read_csv(f), symbol)
            if not frame.empty:
                frames.append(frame)
            if len(frames) >= args.chunk_symbols:
                rows += flush(store, frames, args.dataset)
                frames.clear()
    rows += flush(store, frames, args.dataset)
    return rows


def main() -> int:
    args = parse_args()
    logger.remove()
    logger.add(sys.stderr, level=args.log_level)

    source = Path(args.source)
    target = Path(args.target)
    archives = discover(source, args.pattern)
    if args.limit_archives:
        archives = archives[: args.limit_archives]
    symbols = {s.strip() for s in args.symbols.split(",") if s.strip()}
    logger.info("发现归档文件: {}", len(archives))

    state = Path(args.state_db) if args.state_db else target / "import_state" / "jq_minute_archives.sqlite"
    conn = init_state(state)
    store = ParquetMarketDataStore(str(target))

    if args.dry_run:
        print({"source": str(source), "target": str(target), "archives": [str(a.path) for a in archives[:10]]})
        return 0

    total = 0
    done = skipped = failed = 0
    for i, archive in enumerate(archives, 1):
        if status_of(conn, archive) == "success" and not args.force:
            skipped += 1
            logger.info("[{}/{}] 跳过已完成: {}", i, len(archives), archive.path.name)
            continue
        logger.info("[{}/{}] 导入: {} ({:.2f} MB)", i, len(archives), archive.path.name, archive.size / 1024 / 1024)
        mark(conn, archive, "running")
        try:
            if archive.path.suffix.lower() == ".zip":
                rows = import_zip(archive, store, args, symbols)
            elif archive.path.name.lower().endswith(".tar.gz"):
                rows = import_targz(archive, store, args, symbols)
            else:
                rows = 0
            mark(conn, archive, "success", rows)
            total += rows
            done += 1
            logger.info("完成: {} rows={}", archive.path.name, rows)
        except Exception as exc:
            failed += 1
            mark(conn, archive, "failed", error=str(exc)[:2000])
            logger.exception("导入失败: {}", archive.path)
    logger.info("归档导入结束 done={} skipped={} failed={} rows={} state={}", done, skipped, failed, total, state)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

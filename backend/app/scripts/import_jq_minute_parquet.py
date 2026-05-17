"""Import local JoinQuant-style A-share 1m parquet files into platform parquet store.

Source layout observed:
    time, code, open, close, high, low, volume, money[, paused]
    code examples: 000001.XSHE, 600000.XSHG

Target layout:
    data/parquet/klines_minute/year=YYYY/month=MM/part-*.parquet
    columns: symbol, datetime, open, high, low, close, volume, amount, source

Examples:
    python -m app.scripts.import_jq_minute_parquet --dry-run --limit-files 3
    python -m app.scripts.import_jq_minute_parquet --start 2021-03-01 --end 2021-03-31 --limit-files 2
    python -m app.scripts.import_jq_minute_parquet --start 2020-01-01 --end 2026-05-15
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
from loguru import logger


DEFAULT_SOURCE = Path(
    r"E:\Projects\QuantData\JQ_a_minute\闲鱼商品_A股1分钟数据_聚宽版\01_数据文件"
)
DEFAULT_TARGET = Path(r"E:\Projects\GaoshouPlatform\data\parquet")


@dataclass(frozen=True)
class SourceFile:
    path: Path
    size: int
    mtime_ns: int

    @property
    def key(self) -> str:
        raw = f"{self.path.resolve()}|{self.size}|{self.mtime_ns}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=str(DEFAULT_SOURCE), help="源 parquet 根目录")
    parser.add_argument("--target", default=str(DEFAULT_TARGET), help="平台 parquet 根目录")
    parser.add_argument("--dataset", default="klines_minute", choices=["klines_minute", "klines_minute_timer"])
    parser.add_argument("--state-db", default=None, help="断点状态库，默认 target/import_state/jq_minute_import.sqlite")
    parser.add_argument("--start", default=None, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD，含当天")
    parser.add_argument("--limit-files", type=int, default=0, help="仅处理前 N 个源文件，用于测试")
    parser.add_argument("--file-glob", default="*.parquet", help="源文件 glob，默认 *.parquet")
    parser.add_argument("--force", action="store_true", help="重新处理已成功的源文件")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划和源 schema，不写目标")
    parser.add_argument("--duckdb-memory-limit", default="12GB")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def setup_logger(level: str) -> None:
    logger.remove()
    logger.add(sys.stderr, level=level)


def init_state(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS import_files (
            file_key TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
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


def file_status(conn: sqlite3.Connection, file: SourceFile) -> str | None:
    row = conn.execute("SELECT status FROM import_files WHERE file_key = ?", (file.key,)).fetchone()
    return row[0] if row else None


def mark_file(
    conn: sqlite3.Connection,
    file: SourceFile,
    *,
    status: str,
    rows_written: int = 0,
    error: str | None = None,
) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    if status == "running":
        conn.execute(
            """
            INSERT INTO import_files(file_key, path, size, mtime_ns, status, rows_written, started_at, error)
            VALUES (?, ?, ?, ?, ?, 0, ?, NULL)
            ON CONFLICT(file_key) DO UPDATE SET
                status=excluded.status,
                rows_written=0,
                started_at=excluded.started_at,
                error=NULL
            """,
            (file.key, str(file.path), file.size, file.mtime_ns, status, now),
        )
    else:
        conn.execute(
            """
            UPDATE import_files
            SET status = ?, rows_written = ?, finished_at = ?, error = ?
            WHERE file_key = ?
            """,
            (status, rows_written, now, error, file.key),
        )
    conn.commit()


def discover_files(source: Path, pattern: str) -> list[SourceFile]:
    files = []
    for path in sorted(source.rglob(pattern)):
        if not path.is_file():
            continue
        stat = path.stat()
        files.append(SourceFile(path=path, size=stat.st_size, mtime_ns=stat.st_mtime_ns))
    return files


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_where(start: str | None, end: str | None) -> str:
    clauses = []
    if start:
        clauses.append(f"time >= TIMESTAMP {sql_literal(start + ' 00:00:00')}")
    if end:
        clauses.append(f"time < TIMESTAMP {sql_literal(end + ' 23:59:59.999999')}")
    return " AND ".join(clauses) if clauses else "TRUE"


def inspect_file(con: duckdb.DuckDBPyConnection, file: SourceFile, start: str | None, end: str | None) -> dict:
    path = str(file.path).replace("\\", "/").replace("'", "''")
    where = build_where(start, end)
    info = con.execute(
        f"""
        SELECT
            count(*) AS total_rows,
            count(*) FILTER (WHERE {where}) AS selected_rows,
            min(time) AS min_time,
            max(time) AS max_time,
            count(DISTINCT code) AS symbols
        FROM read_parquet('{path}')
        """
    ).fetchone()
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    return {
        "path": str(file.path),
        "size": file.size,
        "total_rows": info[0],
        "selected_rows": info[1],
        "min_time": str(info[2]) if info[2] else None,
        "max_time": str(info[3]) if info[3] else None,
        "symbols": info[4],
        "columns": [row[0] for row in cols],
    }


def import_file(
    con: duckdb.DuckDBPyConnection,
    file: SourceFile,
    target: Path,
    dataset: str,
    start: str | None,
    end: str | None,
) -> int:
    src = str(file.path).replace("\\", "/").replace("'", "''")
    dest = str((target / dataset).resolve()).replace("\\", "/").replace("'", "''")
    where = build_where(start, end)
    valid_where = f"""
        {where}
        AND code IS NOT NULL
        AND time IS NOT NULL
        AND open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
        AND open >= 0 AND high >= 0 AND low >= 0 AND close >= 0
        AND high >= low
        AND volume IS NOT NULL AND money IS NOT NULL
    """
    rows_to_write = con.execute(
        f"""
        SELECT count(*)
        FROM read_parquet('{src}')
        WHERE {valid_where}
        """
    ).fetchone()[0]
    if rows_to_write == 0:
        return 0

    # COPY PARTITION_BY creates hive-like year/month folders. filename_pattern with uuid
    # avoids overwriting existing files when importing multiple source shards.
    sql = f"""
    COPY (
        WITH normalized AS (
            SELECT
                CASE
                    WHEN ends_with(code, '.XSHE') THEN replace(code, '.XSHE', '.SZ')
                    WHEN ends_with(code, '.XSHG') THEN replace(code, '.XSHG', '.SH')
                    WHEN ends_with(code, '.BJSE') THEN replace(code, '.BJSE', '.BJ')
                    ELSE code
                END AS symbol,
                CAST(time AS TIMESTAMP) AS datetime,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(volume AS DOUBLE) AS volume,
                CAST(money AS DOUBLE) AS amount,
                'jq' AS source,
                CAST(year(CAST(time AS TIMESTAMP)) AS VARCHAR) AS year,
                strftime(CAST(time AS TIMESTAMP), '%m') AS month
            FROM read_parquet('{src}')
            WHERE {valid_where}
        )
        SELECT DISTINCT
            symbol, datetime, open, high, low, close, volume, amount, source, year, month
        FROM normalized
    )
    TO '{dest}'
    (FORMAT PARQUET, PARTITION_BY (year, month), COMPRESSION ZSTD, OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'jq_{{uuid}}');
    """
    con.execute(sql)
    return rows_to_write


def main() -> None:
    args = parse_args()
    setup_logger(args.log_level)

    source = Path(args.source)
    target = Path(args.target)
    if not source.exists():
        raise FileNotFoundError(source)

    files = discover_files(source, args.file_glob)
    if args.limit_files:
        files = files[: args.limit_files]
    logger.info("发现源 parquet 文件: {}", len(files))
    if not files:
        return

    state_db = Path(args.state_db) if args.state_db else target / "import_state" / "jq_minute_import.sqlite"
    conn = init_state(state_db)

    con = duckdb.connect(database=":memory:")
    con.execute(f"SET memory_limit = {sql_literal(args.duckdb_memory_limit)}")
    con.execute(f"SET threads = {int(args.threads)}")

    if args.dry_run:
        samples = []
        for file in files[: min(len(files), 10)]:
            samples.append(inspect_file(con, file, args.start, args.end))
        print(json.dumps({"source": str(source), "target": str(target), "files": len(files), "samples": samples}, ensure_ascii=False, indent=2))
        return

    target_dataset = target / args.dataset
    target_dataset.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    done_files = 0
    skipped_files = 0
    failed_files = 0
    for idx, file in enumerate(files, 1):
        status = file_status(conn, file)
        if status == "success" and not args.force:
            skipped_files += 1
            logger.info("[{}/{}] 跳过已完成: {}", idx, len(files), file.path.name)
            continue
        logger.info("[{}/{}] 导入: {} ({:.2f} MB)", idx, len(files), file.path.name, file.size / 1024 / 1024)
        mark_file(conn, file, status="running")
        try:
            rows = import_file(con, file, target, args.dataset, args.start, args.end)
            mark_file(conn, file, status="success", rows_written=rows)
            total_rows += rows
            done_files += 1
            logger.info("完成: {} rows={}", file.path.name, rows)
        except Exception as exc:
            failed_files += 1
            mark_file(conn, file, status="failed", error=str(exc)[:2000])
            logger.exception("导入失败: {}", file.path)

    logger.info(
        "导入结束 done={} skipped={} failed={} rows={} state_db={}",
        done_files,
        skipped_files,
        failed_files,
        total_rows,
        state_db,
    )


if __name__ == "__main__":
    main()

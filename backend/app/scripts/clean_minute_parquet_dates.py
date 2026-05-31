"""Clean abnormal dates from a partitioned minute Parquet dataset.

This is useful for third-party minute archives that accidentally include
holiday fragments. By default the script only reports suspicious dates; pass
--apply to rewrite affected month partitions without those dates.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import uuid
from pathlib import Path

import duckdb
from loguru import logger

DEFAULT_ROOT = Path(r"E:\Projects\GaoshouPlatform\data\parquet\klines_minute")


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="klines_minute dataset root")
    parser.add_argument("--start", default=None, help="inclusive start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="inclusive end date YYYY-MM-DD")
    parser.add_argument("--min-symbols", type=int, default=1000)
    parser.add_argument("--min-positive-volume-rows", type=int, default=100000)
    parser.add_argument("--apply", action="store_true", help="rewrite partitions and remove suspicious dates")
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--threads", type=int, default=4)
    return parser.parse_args()


def date_filter(start: str | None, end: str | None, col: str = "datetime") -> str:
    clauses = []
    if start:
        clauses.append(f"{col} >= TIMESTAMP {sql_literal(start + ' 00:00:00')}")
    if end:
        clauses.append(f"{col} < TIMESTAMP {sql_literal(end + ' 23:59:59.999999')}")
    return " AND ".join(clauses) if clauses else "TRUE"


def partition_dir(root: Path, year: str, month: str) -> Path:
    return root / f"year={year}" / f"month={month}"


def rewrite_month_without_dates(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    year: str,
    month: str,
    bad_dates: list[str],
) -> int:
    part_dir = partition_dir(root, year, month)
    if not part_dir.exists():
        return 0
    src = str((part_dir / "*.parquet").resolve()).replace("\\", "/")
    excluded = ", ".join(sql_literal(d) for d in bad_dates)
    tmp_dir = part_dir.with_name(f"{part_dir.name}.clean-{uuid.uuid4().hex}")
    tmp = str(tmp_dir.resolve()).replace("\\", "/")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    row_count = con.execute(
        f"""
        SELECT count(*)
        FROM read_parquet('{src}')
        WHERE CAST(datetime AS DATE)::VARCHAR NOT IN ({excluded})
        """
    ).fetchone()[0]

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM read_parquet('{src}')
            WHERE CAST(datetime AS DATE)::VARCHAR NOT IN ({excluded})
            ORDER BY symbol, datetime
        )
        TO '{tmp}/part-0.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )
    shutil.rmtree(part_dir)
    tmp_dir.replace(part_dir)
    return row_count


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    if not root.exists():
        raise FileNotFoundError(root)

    pattern = str((root / "**" / "*.parquet").resolve()).replace("\\", "/")
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = {sql_literal(args.memory_limit)}")
    con.execute(f"SET threads = {int(args.threads)}")

    where = date_filter(args.start, args.end)
    bad = con.execute(
        f"""
        WITH base AS (
            SELECT
                CAST(datetime AS DATE)::VARCHAR AS trade_date,
                strftime(datetime, '%Y') AS year,
                strftime(datetime, '%m') AS month,
                symbol,
                datetime,
                volume
            FROM read_parquet('{pattern}', hive_partitioning=true)
            WHERE {where}
        ),
        daily AS (
            SELECT
                trade_date,
                year,
                month,
                count(*) AS n_rows,
                count(DISTINCT symbol) AS n_symbols,
                sum(CASE WHEN volume > 0 THEN 1 ELSE 0 END) AS positive_volume_rows,
                sum(volume) AS total_volume,
                min(datetime) AS min_dt,
                max(datetime) AS max_dt
            FROM base
            GROUP BY trade_date, year, month
        )
        SELECT *
        FROM daily
        WHERE n_symbols < {int(args.min_symbols)}
           OR positive_volume_rows < {int(args.min_positive_volume_rows)}
        ORDER BY trade_date
        """
    ).fetchdf()

    if bad.empty:
        logger.info("No suspicious dates found")
        return 0

    logger.warning("Suspicious dates:\n{}", bad.to_string(index=False))
    if not args.apply:
        logger.info("Dry run only. Pass --apply to rewrite affected partitions.")
        return 0

    for (year, month), group in bad.groupby(["year", "month"], sort=True):
        dates = group["trade_date"].astype(str).tolist()
        rows_left = rewrite_month_without_dates(con, root, str(year), str(month), dates)
        logger.info("Cleaned year={} month={} removed_dates={} rows_left={}", year, month, dates, rows_left)
    return 0


if __name__ == "__main__":
    sys.exit(main())

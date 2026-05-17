"""Compact and de-duplicate minute parquet partitions using DuckDB.

This is safer for huge minute partitions than the pandas-based generic
compact_parquet_dataset script.

Example:
    python -m app.scripts.compact_jq_minute_duckdb --year 2026 --month 03
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="klines_minute dataset root")
    parser.add_argument("--year", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--memory-limit", default="16GB")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def lit(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def main() -> int:
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    args = parse_args()

    root = Path(args.root)
    part_dir = root / f"year={args.year}" / f"month={args.month}"
    files = sorted(part_dir.glob("*.parquet"))
    if not files:
        logger.error("No parquet files found at {}", part_dir)
        return 1

    pattern = str(part_dir / "*.parquet").replace("\\", "/")
    con = duckdb.connect(database=":memory:")
    con.execute(f"SET memory_limit = {lit(args.memory_limit)}")
    con.execute(f"SET threads = {int(args.threads)}")

    before = con.execute(f"SELECT count(*) FROM read_parquet({lit(pattern)})").fetchone()[0]
    distinct_keys = con.execute(
        f"SELECT count(*) FROM (SELECT DISTINCT symbol, datetime FROM read_parquet({lit(pattern)}))"
    ).fetchone()[0]
    logger.info(
        "Partition {}-{} files={} rows_before={} distinct_keys={}",
        args.year,
        args.month,
        len(files),
        before,
        distinct_keys,
    )
    if args.dry_run:
        return 0

    tmp = part_dir.parent / f"{part_dir.name}.compact-{uuid.uuid4().hex}"
    tmp.mkdir(parents=True, exist_ok=False)
    out = str((tmp / "part-0.parquet").resolve()).replace("\\", "/")
    con.execute(
        f"""
        COPY (
            SELECT
                symbol,
                datetime,
                any_value(open) AS open,
                any_value(high) AS high,
                any_value(low) AS low,
                any_value(close) AS close,
                any_value(volume) AS volume,
                any_value(amount) AS amount,
                any_value(source) AS source
            FROM read_parquet({lit(pattern)})
            GROUP BY symbol, datetime
            ORDER BY symbol, datetime
        )
        TO {lit(out)}
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    backup = part_dir.parent / f"{part_dir.name}.backup-{uuid.uuid4().hex}"
    part_dir.replace(backup)
    tmp.replace(part_dir)
    shutil.rmtree(backup)

    after_pattern = str(part_dir / "*.parquet").replace("\\", "/")
    after = con.execute(f"SELECT count(*) FROM read_parquet({lit(after_pattern)})").fetchone()[0]
    logger.info("Compacted {}-{} rows {} -> {}", args.year, args.month, before, after)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

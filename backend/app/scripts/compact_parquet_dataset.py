#!/usr/bin/env python
"""合并 Parquet dataset 小文件，按唯一键去重

用法:
    python -m app.scripts.compact_parquet_dataset --dataset klines_daily
    python -m app.scripts.compact_parquet_dataset --dataset klines_daily --start 20250101 --end 20251231
    python -m app.scripts.compact_parquet_dataset --dataset klines_minute_timer --max-files-per-partition 1
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from loguru import logger

_BACKEND = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from app.core.config import settings

_UNIQUE_KEYS = {
    "klines_daily": ["symbol", "trade_date"],
    "klines_minute": ["symbol", "datetime"],
    "klines_minute_timer": ["symbol", "datetime"],
    "factor_cache": ["symbol", "trade_date", "expr_hash"],
    "stock_indicators": ["symbol", "indicator_name", "trade_date"],
    "indicator_timeseries": ["symbol", "indicator_name", "datetime"],
}


def compact_dataset(
    dataset: str,
    data_dir: Path | None = None,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    max_files_per_partition: int = 1,
    dry_run: bool = False,
) -> dict:
    """合并 dataset 下的 Parquet 文件，去重"""
    root = (data_dir or Path(settings.parquet_data_dir)) / dataset
    if not root.exists():
        return {"error": f"Dataset {dataset} not found at {root}"}

    unique_keys = _UNIQUE_KEYS.get(dataset, ["symbol", "trade_date"])

    # 收集分区
    partitions = {}
    for f in sorted(root.rglob("*.parquet")):
        if ".tmp-" in str(f) or "_compact_tmp_" in str(f):
            continue
        rel = f.relative_to(root)
        parts = rel.parts
        # 提取分区键值对 year=YYYY/month=MM
        partition_key = "/".join(
            p for p in parts[:-1] if "=" in p
        )
        # 提取年份用于过滤
        year_str = None
        for p in parts:
            if p.startswith("year="):
                year_str = p.replace("year=", "")

        # 日期过滤
        if start_date and year_str:
            try:
                if int(year_str) < start_date.year:
                    continue
            except ValueError:
                pass
        if end_date and year_str:
            try:
                if int(year_str) > end_date.year:
                    continue
            except ValueError:
                pass

        partitions.setdefault(partition_key, []).append(f)

    if not partitions:
        logger.info("No partitions found matching criteria")
        return {"partitions_found": 0}

    stats = {"partitions_found": len(partitions), "partitions_compacted": 0,
             "files_before": 0, "files_after": 0, "rows_before": 0, "rows_after": 0}

    for part_key, files in partitions.items():
        n_files = len(files)
        if n_files <= max_files_per_partition:
            continue

        stats["files_before"] += n_files

        if dry_run:
            logger.info(f"[DRY RUN] {part_key}: {n_files} files would be merged")
            stats["partitions_compacted"] += 1
            continue

        # 读取并合并
        dfs = []
        for f in files:
            try:
                df = pd.read_parquet(f)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Failed to read {f}: {e}")

        if not dfs:
            continue

        combined = pd.concat(dfs, ignore_index=True)

        # 去重
        available_keys = [k for k in unique_keys if k in combined.columns]
        if available_keys:
            before = len(combined)
            combined = combined.drop_duplicates(subset=available_keys, keep="last")
            after = len(combined)
            logger.info(f"{part_key}: dedup {before} -> {after}")

        stats["rows_before"] += sum(len(d) for d in dfs)
        stats["rows_after"] += len(combined)

        # 用临时目录写入新文件
        import pyarrow as pa
        import pyarrow.parquet as pq

        tmp_dir = root.parent / f"_compact_tmp_{dataset}"
        tmp_dir.mkdir(exist_ok=True)
        part_dir = tmp_dir / part_key
        part_dir.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(combined, preserve_index=False)
        out_file = part_dir / "part-0.parquet"
        pq.write_table(table, str(out_file))

        # 替换原文件
        dest_dir = root / part_key
        for old_file in files:
            old_file.unlink(missing_ok=True)
        for f in part_dir.rglob("*.parquet"):
            dest = dest_dir / f.name
            shutil.move(str(f), str(dest))

        shutil.rmtree(tmp_dir, ignore_errors=True)
        stats["files_after"] += 1
        stats["partitions_compacted"] += 1
        logger.info(f"Compacted {part_key}: {n_files} -> 1 file")

    return stats


def main():
    parser = argparse.ArgumentParser(description="合并 Parquet dataset 小文件")
    parser.add_argument("--dataset", required=True, choices=list(_UNIQUE_KEYS))
    parser.add_argument("--start", type=str, default=None, help="起始日期 YYYYMMDD")
    parser.add_argument("--end", type=str, default=None, help="结束日期 YYYYMMDD")
    parser.add_argument("--max-files-per-partition", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--data-dir", type=str, default=None)
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y%m%d").date() if args.start else None
    end = datetime.strptime(args.end, "%Y%m%d").date() if args.end else None

    data_dir = Path(args.data_dir) if args.data_dir else None

    stats = compact_dataset(
        args.dataset, data_dir,
        start_date=start, end_date=end,
        max_files_per_partition=args.max_files_per_partition,
        dry_run=args.dry_run,
    )
    logger.info(f"Compaction result: {stats}")
    return 0 if "error" not in stats else 1


if __name__ == "__main__":
    sys.exit(main())

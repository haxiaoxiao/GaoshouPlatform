#!/usr/bin/env python
"""Remove stale Parquet temporary partition directories."""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

from loguru import logger

_BACKEND = Path(__file__).resolve().parent.parent.parent
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from app.core.config import settings


def cleanup_temp_dirs(
    data_dir: Path | None = None,
    *,
    dataset: str | None = None,
    min_age_minutes: int = 60,
    dry_run: bool = False,
) -> dict[str, object]:
    root = Path(data_dir or settings.parquet_data_dir)
    search_root = root / dataset if dataset else root
    if not search_root.exists():
        return {"error": f"path not found: {search_root}"}

    cutoff = datetime.now() - timedelta(minutes=min_age_minutes)
    candidates = [
        path
        for path in search_root.rglob("*")
        if path.is_dir()
        and (".tmp-" in path.name or path.name.startswith("_compact_tmp_"))
        and datetime.fromtimestamp(path.stat().st_mtime) <= cutoff
    ]

    removed: list[str] = []
    for path in sorted(candidates):
        if dry_run:
            logger.info("[DRY RUN] would remove {}", path)
            continue
        shutil.rmtree(path)
        removed.append(str(path))
        logger.info("Removed stale parquet temp directory {}", path)

    return {
        "root": str(search_root),
        "min_age_minutes": min_age_minutes,
        "dry_run": dry_run,
        "candidates": len(candidates),
        "removed": removed,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean stale parquet temp directories")
    parser.add_argument("--data-dir", type=str, default=None)
    parser.add_argument("--dataset", type=str, default=None)
    parser.add_argument("--min-age-minutes", type=int, default=60)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = cleanup_temp_dirs(
        Path(args.data_dir) if args.data_dir else None,
        dataset=args.dataset,
        min_age_minutes=args.min_age_minutes,
        dry_run=args.dry_run,
    )
    logger.info("Cleanup result: {}", result)
    return 0 if "error" not in result else 1


if __name__ == "__main__":
    raise SystemExit(main())

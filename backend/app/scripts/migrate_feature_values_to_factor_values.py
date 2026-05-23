"""Migrate legacy feature_values Parquet data to factor_values.

The script is intentionally non-destructive: it reads `feature_values`, maps
legacy `smallcap_*` names to generic factor names, writes `factor_values`, and
leaves the old dataset untouched.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from app.core.config import settings


NAME_MAP = {
    "smallcap_market_cap": "market_cap",
    "smallcap_market_cap_rank": "market_cap_rank",
    "smallcap_is_st": "is_st",
    "smallcap_is_paused": "is_paused",
    "smallcap_is_limit_up": "is_limit_up",
    "smallcap_is_limit_down": "is_limit_down",
    "smallcap_yesterday_limit_up": "yesterday_limit_up",
    "smallcap_v4gv": "v4gv",
    "smallcap_v4gv21": "v4gv_signal",
    "smallcap_macd_signal": "macd_positive",
    "smallcap_indicator_buy_signal": "indicator_buy_signal",
    "smallcap_v4gv_dead_cross": "v4gv_dead_cross",
    "max_volume_nd": "rolling_max_volume",
}


def migrate(*, overwrite: bool = False) -> tuple[int, int]:
    root = Path(settings.parquet_data_dir)
    source = root / "feature_values"
    target = root / "factor_values"
    if not source.exists():
        raise FileNotFoundError(f"Legacy dataset not found: {source}")
    if target.exists():
        if not overwrite:
            raise FileExistsError(f"Target dataset already exists: {target}")
        shutil.rmtree(target)

    files = list(source.rglob("*.parquet"))
    rows = 0
    written_files = 0
    for file in files:
        relative = file.relative_to(source)
        out_file = target / relative
        out_file.parent.mkdir(parents=True, exist_ok=True)
        df = pd.read_parquet(file)
        if "feature_name" in df.columns:
            df = df.rename(columns={"feature_name": "factor_name"})
        if "factor_name" in df.columns:
            df["factor_name"] = df["factor_name"].astype(str).map(lambda name: NAME_MAP.get(name, name))
        df.to_parquet(out_file, index=False)
        rows += len(df)
        written_files += 1
    return rows, written_files


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    row_count, file_count = migrate(overwrite=args.overwrite)
    print(f"Migrated {row_count:,} rows in {file_count} files")


"""Build exact manifests for selected, stable Parquet datasets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.core.config import settings
from app.services.dataset_manifest import build_dataset_manifest, write_dataset_manifest
from app.services.parquet_dataset_catalog import get_parquet_dataset_spec


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("datasets", nargs="+", help="Dataset directory names")
    parser.add_argument("--data-dir", default=settings.parquet_data_dir)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    reports = []
    for name in args.datasets:
        spec = get_parquet_dataset_spec(name)
        if spec is None or not spec.date_column:
            raise ValueError(f"Dataset {name!r} has no registered date column")
        root = data_dir / name
        manifest = build_dataset_manifest(root, dataset=name, date_column=spec.date_column)
        path = write_dataset_manifest(root, manifest)
        reports.append({"dataset": name, "manifest": str(path), **manifest.to_dict()})
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

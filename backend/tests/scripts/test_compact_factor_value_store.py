from __future__ import annotations

import inspect
import shutil
from datetime import date, datetime

import pandas as pd

from app.scripts.compact_factor_value_store import compact_factor_value_store
from app.services.dataset_manifest import read_dataset_manifest


def _write_part(root, month: int, name: str, rows: list[dict]):
    partition = root / "year=2026" / f"month={month:02d}"
    partition.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(partition / name, index=False)


def test_compaction_writes_new_validated_store_without_touching_source(tmp_path):
    source = tmp_path / "factor_values"
    output = tmp_path / "factor_values_compacted"
    base = {
        "symbol": "000001.SZ",
        "trade_date": date(2026, 1, 5),
        "as_of_time": "",
        "factor_name": "market_cap",
        "params_hash": "empty",
        "source": "test",
    }
    _write_part(source, 1, "part-a.parquet", [{**base, "value": 1.0, "created_at": datetime(2026, 1, 5, 9, 0)}])
    _write_part(source, 1, "part-b.parquet", [{**base, "value": 2.0, "created_at": datetime(2026, 1, 5, 10, 0)}])
    _write_part(
        source,
        2,
        "part-c.parquet",
        [{**base, "trade_date": date(2026, 2, 5), "value": 3.0, "created_at": datetime(2026, 2, 5, 10, 0)}],
    )
    source_files = sorted(source.rglob("*.parquet"))

    report = compact_factor_value_store(source, output)

    assert report["validated"] is True
    assert report["files_before"] == 3
    assert report["files_after"] == 2
    assert report["rows_before"] == 3
    assert report["rows_after"] == 2
    assert sorted(source.rglob("*.parquet")) == source_files
    compacted = pd.concat(pd.read_parquet(path) for path in sorted(output.rglob("*.parquet")))
    assert compacted.loc[compacted["trade_date"].eq(date(2026, 1, 5)), "value"].item() == 2.0
    manifest = read_dataset_manifest(output)
    assert manifest is not None
    assert manifest.validation_status == "valid"
    assert manifest.row_count == 2
    assert manifest.details["factor_coverage"]["market_cap"] == {
        "total_rows": 2,
        "symbol_count": 1,
        "date_count": 2,
        "min_date": "2026-01-05",
        "max_date": "2026-02-05",
    }


def test_compaction_refuses_nonempty_output_directory(tmp_path):
    source = tmp_path / "factor_values"
    output = tmp_path / "factor_values_compacted"
    source.mkdir()
    output.mkdir()
    (output / "keep.txt").write_text("owned", encoding="utf-8")

    try:
        compact_factor_value_store(source, output)
    except ValueError as exc:
        assert "empty" in str(exc).lower()
    else:
        raise AssertionError("expected non-empty output protection")


def test_compaction_resume_revalidates_existing_partitions_and_finishes_missing_month(tmp_path):
    assert "resume" in inspect.signature(compact_factor_value_store).parameters

    source = tmp_path / "factor_values"
    output = tmp_path / "factor_values_compacted"
    base = {
        "symbol": "000001.SZ",
        "as_of_time": "",
        "factor_name": "market_cap",
        "params_hash": "empty",
        "source": "test",
        "created_at": datetime(2026, 2, 5, 10, 0),
    }
    _write_part(source, 1, "part-a.parquet", [{**base, "trade_date": date(2026, 1, 5), "value": 1.0}])
    _write_part(source, 2, "part-b.parquet", [{**base, "trade_date": date(2026, 2, 5), "value": 2.0}])
    compact_factor_value_store(source, output)
    shutil.rmtree(output / "year=2026" / "month=02")
    (output / "_manifest.json").unlink()

    report = compact_factor_value_store(source, output, resume=True)

    assert report["validated"] is True
    assert report["files_after"] == 2
    assert read_dataset_manifest(output) is not None

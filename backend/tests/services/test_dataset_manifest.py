from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from app.services.dataset_manifest import (
    DatasetManifest,
    build_dataset_manifest,
    evaluate_dataset_readiness,
    read_dataset_manifest,
    write_dataset_manifest,
)


def test_build_manifest_reads_exact_parquet_metadata(tmp_path):
    dataset_root = tmp_path / "klines_daily"
    partition = dataset_root / "year=2026" / "month=07"
    partition.mkdir(parents=True)
    pd.DataFrame(
        [
            {"symbol": "000001.SZ", "trade_date": date(2026, 7, 8), "close": 10.0},
            {"symbol": "000002.SZ", "trade_date": date(2026, 7, 9), "close": 11.0},
        ]
    ).to_parquet(partition / "part-000.parquet", index=False)

    manifest = build_dataset_manifest(dataset_root, dataset="klines_daily", date_column="trade_date")

    assert manifest.file_count == 1
    assert manifest.row_count == 2
    assert manifest.min_date == "2026-07-08"
    assert manifest.max_date == "2026-07-09"
    assert manifest.validation_status == "valid"
    assert len(manifest.schema_hash) == 64


def test_build_manifest_supports_nested_hive_partitions(tmp_path):
    dataset_root = tmp_path / "stock_indicators"
    partition = dataset_root / "indicator_name=roe" / "year=2026" / "month=07"
    partition.mkdir(parents=True)
    pd.DataFrame(
        [{"symbol": "000001.SZ", "trade_date": date(2026, 7, 9), "value": 12.5}]
    ).to_parquet(partition / "part-000.parquet", index=False)

    manifest = build_dataset_manifest(
        dataset_root,
        dataset="stock_indicators",
        date_column="trade_date",
    )

    assert manifest.file_count == 1
    assert manifest.partition_count == 1
    assert manifest.max_date == "2026-07-09"


def test_manifest_round_trip_and_ready_status(tmp_path):
    dataset_root = tmp_path / "klines_daily"
    manifest = DatasetManifest(
        dataset="klines_daily",
        generated_at=datetime(2026, 7, 10, 10, 0),
        file_count=2,
        byte_size=1024,
        row_count=100,
        partition_count=1,
        min_date="2026-07-01",
        max_date="2026-07-09",
        schema_hash="abc123",
    )

    path = write_dataset_manifest(dataset_root, manifest)
    loaded = read_dataset_manifest(dataset_root)
    readiness = evaluate_dataset_readiness(loaded, as_of=date(2026, 7, 10), max_age_days=3)

    assert path == dataset_root / "_manifest.json"
    assert loaded == manifest
    assert readiness.status == "ready"
    assert readiness.age_days == 1


def test_manifest_readiness_reports_stale_missing_and_invalid(tmp_path):
    stale = DatasetManifest(
        dataset="factor_values",
        generated_at=datetime(2026, 7, 10, 10, 0),
        file_count=1,
        byte_size=100,
        row_count=10,
        partition_count=1,
        min_date="2026-01-01",
        max_date="2026-06-18",
        schema_hash="abc123",
    )

    assert evaluate_dataset_readiness(stale, as_of=date(2026, 7, 10), max_age_days=5).status == "stale"
    assert evaluate_dataset_readiness(None, as_of=date(2026, 7, 10), max_age_days=5).status == "missing"

    invalid = DatasetManifest(**{**stale.to_dict(), "validation_status": "invalid"})
    assert evaluate_dataset_readiness(invalid, as_of=date(2026, 7, 10), max_age_days=5).status == "invalid"

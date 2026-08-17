from datetime import date, datetime

import pandas as pd

from app.core.config import settings
from app.services import factor_value_store
from app.services.dataset_manifest import DatasetManifest, write_dataset_manifest
from app.services.factor_value_store import FactorValueStore


def test_factor_value_store_metadata_cache_invalidates_after_append(tmp_path):
    store = FactorValueStore(str(tmp_path))

    assert store.exists() is False

    written = store.append(pd.DataFrame([
        {
            "symbol": "000001.SZ",
            "trade_date": date(2026, 5, 1),
            "factor_name": "demo_factor",
            "value": 1.0,
        }
    ]))

    assert written == 1
    assert store.exists() is True
    assert "factor_name" in store._schema_columns()


def test_factor_value_store_uses_configured_directory_override(tmp_path, monkeypatch):
    override = tmp_path / "factor_values_compacted"
    monkeypatch.setattr(settings, "factor_value_store_dir", str(override), raising=False)

    store = FactorValueStore()

    assert store._dataset_path() == override


def test_coverage_many_uses_valid_manifest_index_for_full_range(tmp_path, monkeypatch):
    store = FactorValueStore(str(tmp_path))
    dataset = store._dataset_path()
    partition = dataset / "year=2026" / "month=01"
    partition.mkdir(parents=True)
    pd.DataFrame([
        {
            "symbol": "000001.SZ",
            "trade_date": date(2026, 1, 5),
            "factor_name": "market_cap",
            "value": 1.0,
        }
    ]).to_parquet(partition / "part-000.parquet", index=False)
    write_dataset_manifest(
        dataset,
        DatasetManifest(
            dataset="factor_values",
            generated_at=datetime(2026, 1, 6),
            file_count=1,
            byte_size=1,
            row_count=1,
            partition_count=1,
            min_date="2026-01-05",
            max_date="2026-01-05",
            schema_hash="test",
            details={
                "factor_coverage": {
                    "market_cap": {
                        "total_rows": 1,
                        "symbol_count": 1,
                        "date_count": 1,
                        "min_date": "2026-01-05",
                        "max_date": "2026-01-05",
                    }
                }
            },
        ),
    )

    def fail_if_scanned():
        raise AssertionError("coverage query should use the manifest index")

    monkeypatch.setattr(factor_value_store, "get_duckdb", fail_if_scanned)

    result = store.coverage_many(
        ["market_cap", "missing_factor"],
        start_date=date(2020, 1, 1),
        end_date=date(2026, 12, 31),
    )

    assert result["market_cap"]["total_rows"] == 1
    assert result["market_cap"]["min_date"] == "2026-01-05"
    assert result["missing_factor"]["total_rows"] == 0


def test_coverage_uses_distinct_key_aggregate_instead_of_window_dedup(tmp_path, monkeypatch):
    store = FactorValueStore(str(tmp_path))
    store.append(pd.DataFrame([{
        "symbol": "000001.SZ",
        "trade_date": date(2026, 1, 5),
        "factor_name": "market_cap",
        "value": 1.0,
    }]))
    captured: list[str] = []

    class FakeResult:
        def fetchone(self):
            return (1, 1, 1, date(2026, 1, 5), date(2026, 1, 5))

    class FakeDuckDB:
        def execute(self, sql):
            captured.append(sql)
            return FakeResult()

    monkeypatch.setattr(factor_value_store, "get_duckdb", lambda: FakeDuckDB())

    result = store.coverage(
        "market_cap",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        symbols=["000001.SZ"],
        include_symbols_sample=False,
    )

    assert result["total_rows"] == 1
    assert captured
    assert "ROW_NUMBER()" not in captured[0]
    assert "COUNT(DISTINCT (symbol, trade_date" in captured[0]

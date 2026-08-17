"""Cache invalidation service tests."""

from pathlib import Path

import pytest


def _stub_external_caches(monkeypatch) -> None:
    class FakeComputeCache:
        def clear(self):
            return 0

    class FakeBacktestCache:
        available = False

    monkeypatch.setattr("app.services.cache_invalidation.get_compute_cache", lambda: FakeComputeCache())
    monkeypatch.setattr("app.services.cache_invalidation.get_backtest_cache", lambda: FakeBacktestCache())


def test_invalidate_after_sync_clears_compute_and_matching_redis(monkeypatch):
    from app.services.cache_invalidation import invalidate_after_sync

    cleared = {"all": False}
    deleted = []

    class FakeComputeCache:
        def clear(self):
            cleared["all"] = True
            return 4

    class FakeBinaryClient:
        def scan_iter(self, match, count):
            assert match == "bt:test:*"
            yield b"bt:test:timer_coverage:abc"
            yield b"bt:test:index_components:399101.SZ:2025-01-02"
            yield b"bt:test:other:keep"

        def delete(self, key):
            deleted.append(key)
            return 1

    class FakeBacktestCache:
        available = True
        namespace = "bt:test"

        def _binary_client(self):
            return FakeBinaryClient()

    monkeypatch.setattr("app.services.cache_invalidation.get_compute_cache", lambda: FakeComputeCache())
    monkeypatch.setattr("app.services.cache_invalidation.get_backtest_cache", lambda: FakeBacktestCache())

    result = invalidate_after_sync("kline_minute")

    assert cleared["all"] is True
    assert result["compute_redis_deleted"] == 4
    assert result["redis_deleted"] == 1
    assert deleted == [b"bt:test:timer_coverage:abc"]


@pytest.mark.parametrize(
    ("sync_type", "affected_datasets"),
    [
        ("kline_daily", {"klines_daily"}),
        ("index_daily", {"klines_daily"}),
        ("kline_minute", {"klines_minute"}),
        ("datasync", {"klines_daily", "klines_minute"}),
        ("kline_weekly", {"klines_weekly"}),
    ],
)
def test_invalidate_after_sync_clears_only_affected_dataset_metadata(
    monkeypatch,
    tmp_path,
    sync_type,
    affected_datasets,
):
    from app.services import tushare_relay_sync
    from app.services.cache_invalidation import invalidate_after_sync

    _stub_external_caches(monkeypatch)
    monkeypatch.setattr(tushare_relay_sync.settings, "parquet_data_dir", str(tmp_path))
    datasets = {"klines_daily", "klines_minute", "klines_weekly", "factor_values"}
    date_columns = {
        "klines_daily": "trade_date",
        "klines_minute": "datetime",
        "klines_weekly": "trade_date",
        "factor_values": "trade_date",
    }
    manifest_paths = {}
    tushare_relay_sync._COVERAGE_CACHE.clear()
    for dataset in datasets:
        manifest = tmp_path / dataset / "_manifest.json"
        manifest.parent.mkdir(parents=True)
        manifest.write_text("{}", encoding="utf-8")
        manifest_paths[dataset] = manifest
        date_col = date_columns[dataset]
        tushare_relay_sync._COVERAGE_CACHE[(str(tmp_path), dataset, date_col, False)] = (
            float("inf"),
            {"dataset": dataset},
        )
        tushare_relay_sync._COVERAGE_CACHE[(str(tmp_path), dataset, date_col, True)] = (
            float("inf"),
            {"dataset": dataset},
        )
    tushare_relay_sync._CATALOG_CACHE.update({"expires_at": float("inf"), "value": {"stale": True}})

    result = invalidate_after_sync(sync_type)

    metadata = result["dataset_metadata"]
    assert set(metadata["datasets"]) == affected_datasets
    assert metadata["coverage_cache_entries_deleted"] == 2 * len(affected_datasets)
    assert metadata["catalog_cache_cleared"] is True
    assert set(metadata["manifests_deleted"]) == affected_datasets
    assert metadata["manifest_delete_failed"] == []
    assert tushare_relay_sync._CATALOG_CACHE == {"expires_at": 0.0, "value": None}
    assert {
        cache_key[1] for cache_key in tushare_relay_sync._COVERAGE_CACHE
    } == datasets - affected_datasets
    for dataset, manifest in manifest_paths.items():
        assert manifest.exists() is (dataset not in affected_datasets)


def test_invalidate_after_sync_reports_manifest_deletion_errors_without_raising(monkeypatch, tmp_path):
    from app.services import tushare_relay_sync
    from app.services.cache_invalidation import invalidate_after_sync

    _stub_external_caches(monkeypatch)
    monkeypatch.setattr(tushare_relay_sync.settings, "parquet_data_dir", str(tmp_path))
    manifest = tmp_path / "klines_daily" / "_manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}", encoding="utf-8")
    unrelated_manifest = tmp_path / "factor_values" / "_manifest.json"
    unrelated_manifest.parent.mkdir(parents=True)
    unrelated_manifest.write_text("{}", encoding="utf-8")
    tushare_relay_sync._COVERAGE_CACHE.clear()
    affected_key = (str(tmp_path), "klines_daily", "trade_date", False)
    unrelated_key = (str(tmp_path), "factor_values", "trade_date", False)
    tushare_relay_sync._COVERAGE_CACHE[affected_key] = (float("inf"), {"stale": True})
    tushare_relay_sync._COVERAGE_CACHE[unrelated_key] = (float("inf"), {"keep": True})
    tushare_relay_sync._CATALOG_CACHE.update({"expires_at": float("inf"), "value": {"stale": True}})
    warnings = []

    class FakeLogger:
        def warning(self, message, *args):
            warnings.append((message, args))

    original_unlink = Path.unlink

    def fail_manifest_unlink(path, *args, **kwargs):
        if path == manifest:
            raise PermissionError("manifest is locked")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(tushare_relay_sync, "logger", FakeLogger())
    monkeypatch.setattr(Path, "unlink", fail_manifest_unlink)

    result = invalidate_after_sync("kline_daily")

    metadata = result["dataset_metadata"]
    assert metadata["datasets"] == ["klines_daily"]
    assert metadata["coverage_cache_entries_deleted"] == 1
    assert metadata["catalog_cache_cleared"] is True
    assert metadata["manifests_deleted"] == []
    assert metadata["manifest_delete_failed"] == [
        {
            "dataset": "klines_daily",
            "path": str(manifest),
            "error": "manifest is locked",
        }
    ]
    assert warnings
    assert manifest.exists()
    assert unrelated_manifest.exists()
    assert affected_key not in tushare_relay_sync._COVERAGE_CACHE
    assert unrelated_key in tushare_relay_sync._COVERAGE_CACHE
    assert tushare_relay_sync._CATALOG_CACHE == {"expires_at": 0.0, "value": None}


def test_coverage_cache_observes_invalidation_marker_from_another_process(monkeypatch, tmp_path):
    from app.services import tushare_relay_sync

    _stub_external_caches(monkeypatch)
    monkeypatch.setattr(tushare_relay_sync.settings, "parquet_data_dir", str(tmp_path))
    dataset_root = tmp_path / "klines_daily"
    dataset_root.mkdir(parents=True)
    monkeypatch.setattr(tushare_relay_sync.ParquetMarketDataStore, "_exists", lambda _self, _dataset: True)
    values = iter([
        {"row_count": 1, "min_date": "2026-07-01", "max_date": "2026-07-01", "estimated": True},
        {"row_count": 2, "min_date": "2026-07-01", "max_date": "2026-07-20", "estimated": True},
    ])
    monkeypatch.setattr(tushare_relay_sync, "_fast_dataset_coverage", lambda _dataset, _date_col: next(values))
    marker = dataset_root / ".coverage-invalidation"
    marker.touch()
    tushare_relay_sync._COVERAGE_CACHE.clear()
    first = tushare_relay_sync.dataset_coverage("klines_daily", "trade_date")
    assert first["max_date"] == "2026-07-01"
    marker.write_text("new generation", encoding="utf-8")
    refreshed = tushare_relay_sync.dataset_coverage("klines_daily", "trade_date")
    assert refreshed["max_date"] == "2026-07-20"

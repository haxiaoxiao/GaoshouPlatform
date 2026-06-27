from __future__ import annotations

from datetime import date

import pytest

from app.services import sync_service as sync_service_module
from app.services.sync_service import SyncService


class FakeSession:
    async def commit(self) -> None:
        return None


class FakeAsyncSession(FakeSession):
    pass


async def noop_persist_sync_progress(self, *args, **kwargs) -> None:
    return None


async def noop_create_sync_log(self, *args, **kwargs) -> object:
    return object()


@pytest.mark.asyncio
async def test_sync_sentiment_nga_tracks_flocktrader_progress(monkeypatch):
    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.progress_callback = progress_callback

        async def run(self, source, symbol, **kwargs):
            assert source == "flocktrader"
            assert symbol is None
            assert kwargs["force_refresh"] is True
            if self.progress_callback:
                self.progress_callback(
                    {
                        "stage": "nga.cache.write",
                        "source": "flocktrader",
                        "current_step": "cache_write",
                        "current_date": "2026-06-27",
                        "cache_posts": 140,
                        "scan_time_basis": "last_reply_time",
                        "cache_partition": "last_reply_time",
                    }
                )
            return {"source": "flocktrader", "mode": "daily_cache", "upserted": 3}

    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeAsyncSession()).sync_sentiment_nga(
        start_date=date(2026, 6, 27),
        end_date=date(2026, 6, 27),
    )

    assert progress.status == "completed"
    assert progress.details["source"] == "flocktrader"
    assert progress.details["nga_progress"]["source"] == "flocktrader"
    assert progress.details["current_date"] == "2026-06-27"
    assert progress.details["cache_posts"] == 140
    assert progress.details["scan_time_basis"] == "last_reply_time"
    assert progress.details["cache_partition"] == "last_reply_time"
    assert progress.details["result"]["upserted"] == 3


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_overlaps_latest_local_date(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_minute_batch(symbols, start_date, end_date):
        captured["symbols"] = symbols
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 4),
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fake_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["symbols"] == ["000001.SZ"]
    assert captured["start_date"] == date(2026, 6, 3)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.details["start_date"] == "2026-06-03"
    assert progress.details["latest_local_date"] == "2026-06-04"
    assert progress.details["auto_incremental"] is True
    assert progress.details["incremental_overlap_days"] == 1


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_refreshes_overlap_when_latest_is_end(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_minute_batch(symbols, start_date, end_date):
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 6),
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fake_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["start_date"] == date(2026, 6, 5)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.status == "completed"
    assert "skipped" not in progress.details


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_skips_when_local_data_after_target_end(monkeypatch):
    async def fail_get_kline_minute_batch(*args, **kwargs):
        raise AssertionError("sync should skip when local overlap start is after target end")

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 8),
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fail_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert progress.status == "completed"
    assert progress.details["skipped"] is True
    assert progress.details["skip_reason"] == "already up to date"
    assert progress.details["start_date"] == "2026-06-07"


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_preserves_explicit_start_without_local_data(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_minute_batch(symbols, start_date, end_date):
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: None,
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fake_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        start_date=date(2026, 5, 1),
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["start_date"] == date(2026, 5, 1)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.details["start_date"] == "2026-05-01"


@pytest.mark.asyncio
async def test_sync_kline_minute_cleans_cache_when_cancelled(monkeypatch):
    cleaned: dict[str, object] = {}

    monkeypatch.setattr(sync_service_module.app_settings, "qmt_minute_clean_cache_after_sync", True)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)
    monkeypatch.setattr(sync_service_module, "_sync_cancelled", lambda progress: True)

    def fake_clean_local_cache(**kwargs):
        cleaned.update(kwargs)
        return {"deleted": 1, "freed_mb": 0.1}

    monkeypatch.setattr(sync_service_module.qmt_gateway, "clean_local_cache", fake_clean_local_cache)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        start_date=date(2026, 6, 6),
        end_date=date(2026, 6, 6),
    )

    assert progress.status == "cancelled"
    assert cleaned == {"symbols": ["000001.SZ"], "data_type": "kline"}
    assert progress.details["cache_cleaned"] == {"deleted": 1, "freed_mb": 0.1}


@pytest.mark.asyncio
async def test_sync_kline_minute_skips_cache_cleanup_when_no_symbols(monkeypatch):
    cleaned = []

    monkeypatch.setattr(sync_service_module.app_settings, "qmt_minute_clean_cache_after_sync", True)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    def fake_clean_local_cache(**kwargs):
        cleaned.append(kwargs)
        return {"deleted": 0, "freed_mb": 0}

    monkeypatch.setattr(sync_service_module.qmt_gateway, "clean_local_cache", fake_clean_local_cache)

    progress = type("P", (), {"details": {}})()
    sync_service_module._clean_qmt_kline_cache_after_download(progress, [])

    assert cleaned == []
    assert progress.details["cache_cleaned"] == "skipped"
    assert progress.details["cache_clean_skipped_reason"] == "no_symbols"

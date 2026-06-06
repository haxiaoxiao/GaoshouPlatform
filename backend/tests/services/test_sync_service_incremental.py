from __future__ import annotations

from datetime import date

import pytest

from app.services import sync_service as sync_service_module
from app.services.sync_service import SyncService


class FakeSession:
    async def commit(self) -> None:
        return None


async def noop_persist_sync_progress(self, *args, **kwargs) -> None:
    return None


async def noop_create_sync_log(self, *args, **kwargs) -> object:
    return object()


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

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from app.core.config import settings
from app.services import index_components
from app.services.factor_dependency_sync import _index_components_cover_request


def _use_temp_index_db(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    db_path = tmp_path / "gaoshou.db"
    monkeypatch.setattr(settings, "database_url", f"sqlite+aiosqlite:///{db_path.as_posix()}")
    index_components.init_index_component_table()


def _insert_snapshot(index_symbol: str, trade_date: str, symbols: list[str]) -> None:
    idx = index_components.normalize_index_symbol(index_symbol) or index_symbol
    jq_idx = index_components.jq_index_symbol(idx)
    with sqlite3.connect(settings.sqlite_db_path) as conn:
        conn.executemany(
            """
            INSERT INTO index_components (index_symbol, jq_index_symbol, symbol, trade_date, source)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(idx, jq_idx, symbol, trade_date, "test") for symbol in symbols],
        )
        conn.commit()


@pytest.mark.asyncio
async def test_load_index_symbols_rejects_future_only_snapshot(monkeypatch, tmp_path):
    _use_temp_index_db(monkeypatch, tmp_path)
    _insert_snapshot("399006.SZ", "2026-05-29", ["300001.SZ", "300002.SZ"])

    async def noop_ensure(*_args, **_kwargs):
        return {"index_symbol": "399006.SZ", "inserted": 0, "source": "cache"}

    monkeypatch.setattr(index_components, "ensure_index_components", noop_ensure)

    symbols = await index_components.load_index_symbols(
        "chinext",
        date(2020, 1, 1),
        date(2020, 12, 31),
    )

    assert symbols == []


def test_index_component_prepare_rejects_future_only_snapshot(monkeypatch, tmp_path):
    _use_temp_index_db(monkeypatch, tmp_path)
    _insert_snapshot("000688.SH", "2026-05-29", ["688001.SH"])

    assert not _index_components_cover_request("star50", date(2020, 1, 1), date(2020, 12, 31))


@pytest.mark.asyncio
async def test_index_symbols_use_start_snapshot_and_in_range_changes_only(monkeypatch, tmp_path):
    _use_temp_index_db(monkeypatch, tmp_path)
    _insert_snapshot("399101.SZ", "2019-12-31", ["000001.SZ", "000002.SZ"])
    _insert_snapshot("399101.SZ", "2020-06-30", ["000002.SZ", "000003.SZ"])
    _insert_snapshot("399101.SZ", "2026-05-29", ["000004.SZ"])

    async def noop_ensure(*_args, **_kwargs):
        return {"index_symbol": "399101.SZ", "inserted": 0, "source": "cache"}

    monkeypatch.setattr(index_components, "ensure_index_components", noop_ensure)

    symbols = await index_components.load_index_symbols(
        "399101.SZ",
        date(2020, 1, 1),
        date(2020, 12, 31),
    )
    as_of_symbols = await index_components.load_index_symbols_as_of(
        "399101.SZ",
        date(2020, 3, 31),
    )

    assert symbols == ["000001.SZ", "000002.SZ", "000003.SZ"]
    assert as_of_symbols == ["000001.SZ", "000002.SZ"]


def test_index_component_prepare_accepts_fresh_historical_snapshots(monkeypatch, tmp_path):
    _use_temp_index_db(monkeypatch, tmp_path)
    _insert_snapshot("000688.SH", "2019-12-31", ["688001.SH"])
    _insert_snapshot("000688.SH", "2020-12-15", ["688001.SH", "688002.SH"])

    assert _index_components_cover_request("star50", date(2020, 1, 1), date(2020, 12, 31))

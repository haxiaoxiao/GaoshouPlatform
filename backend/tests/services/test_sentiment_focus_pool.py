from __future__ import annotations

import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.db.models.sentiment import SentimentFocusSnapshot
from app.db.sqlite import async_session_factory
from app.services.sentiment_focus_pool import (
    FOCUS_SNAPSHOT_KEY,
    SentimentFocusPoolResolver,
)


def test_focus_snapshot_schema_excludes_sensitive_position_fields() -> None:
    columns = set(SentimentFocusSnapshot.__table__.columns.keys())

    assert {
        "snapshot_key",
        "status",
        "captured_at",
        "symbols_json",
        "provenance_json",
    } <= columns
    assert columns.isdisjoint(
        {"account_id", "quantity", "avg_cost", "market_value", "cash", "total_asset"}
    )


def _write_note(path, frontmatter: str, body: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\n{frontmatter}\n---\n{body}", encoding="utf-8")


def test_vault_targets_only_include_active_company_notes_in_configured_root(tmp_path) -> None:
    vault_root = tmp_path / "待观察"
    _write_note(
        vault_root / "行业" / "valid.md",
        "type: company-research\nstatus: active\nsymbol: 002138.SZ",
        "# 公司\n## 2026-07-15 跟踪\n## 2026-07-16 跟踪",
    )
    _write_note(
        vault_root / "inactive.md",
        "type: company-research\nstatus: archived\nsymbol: 600114.SH",
    )
    _write_note(
        vault_root / "wrong-type.md",
        "type: theme-research\nstatus: active\nsymbol: 688190.SH",
    )
    _write_note(
        vault_root / "malformed-symbol.md",
        "type: company-research\nstatus: active\nsymbol: '002138'",
    )
    _write_note(
        tmp_path / "其他目录" / "outside.md",
        "type: company-research\nstatus: active\nsymbol: 000969.SZ",
    )

    notes = SentimentFocusPoolResolver(session=None, vault_root=vault_root).read_vault_targets()

    assert [note.symbol for note in notes] == ["002138.SZ"]
    assert notes[0].note_path == "行业/valid.md"
    assert notes[0].latest_tracking_heading == "2026-07-16 跟踪"


def test_vault_targets_ignore_invalid_yaml(tmp_path) -> None:
    vault_root = tmp_path / "待观察"
    _write_note(
        vault_root / "invalid-yaml.md",
        "type: [company-research\nstatus: active\nsymbol: 002138.SZ",
    )

    notes = SentimentFocusPoolResolver(session=None, vault_root=vault_root).read_vault_targets()

    assert notes == []


def _position(quantity: float, **sensitive_values):
    return SimpleNamespace(quantity=quantity, **sensitive_values)


@pytest.mark.asyncio
async def test_fresh_qmt_holdings_are_ordered_before_deduplicated_vault_targets(tmp_path) -> None:
    vault_root = tmp_path / "待观察"
    _write_note(
        vault_root / "overlap.md",
        "type: company-research\nstatus: active\nsymbol: 600114.SH",
    )
    _write_note(
        vault_root / "vault-only.md",
        "type: company-research\nstatus: active\nsymbol: 002138.SZ",
    )
    captured_at = datetime(2026, 7, 17, 10, 0)

    async def account_snapshot():
        return SimpleNamespace(
            account_id="sensitive-account",
            cash=123456.0,
            positions={
                "600114.SH": _position(100, avg_cost=21.5, market_value=2150),
                "002313.SZ": _position(200, avg_cost=8.5, market_value=1700),
                "000001.SZ": _position(0, avg_cost=10, market_value=0),
            },
        )

    async with async_session_factory() as session:
        pool = await SentimentFocusPoolResolver(
            session,
            vault_root=vault_root,
            account_snapshot=account_snapshot,
            now=lambda: captured_at,
        ).resolve()
        await session.commit()
        row = (
            await session.execute(select(SentimentFocusSnapshot))
        ).scalar_one()

    assert [target.symbol for target in pool.targets] == [
        "002313.SZ",
        "600114.SH",
        "002138.SZ",
    ]
    assert pool.qmt_status == "fresh"
    assert pool.overlap_count == 1
    assert pool.targets[1].sources == ("qmt_holding", "vault_active")
    assert row.snapshot_key == FOCUS_SNAPSHOT_KEY
    assert row.status == "fresh"
    assert json.loads(row.symbols_json) == ["002313.SZ", "600114.SH", "002138.SZ"]
    persisted = row.symbols_json + row.provenance_json + str(row.error_summary or "")
    assert "sensitive-account" not in persisted
    assert "avg_cost" not in persisted
    assert "market_value" not in persisted
    assert "123456" not in persisted


@pytest.mark.asyncio
async def test_qmt_failure_uses_successful_snapshot_younger_than_24_hours(tmp_path) -> None:
    vault_root = tmp_path / "待观察"
    _write_note(
        vault_root / "overlap.md",
        "type: company-research\nstatus: active\nsymbol: 600114.SH",
    )
    _write_note(
        vault_root / "vault-only.md",
        "type: company-research\nstatus: active\nsymbol: 002138.SZ",
    )
    now = datetime(2026, 7, 17, 10, 0)
    captured_at = now - timedelta(hours=23)

    async def unavailable_snapshot():
        raise RuntimeError("account sensitive-account disconnected")

    async with async_session_factory() as session:
        session.add(
            SentimentFocusSnapshot(
                snapshot_key=FOCUS_SNAPSHOT_KEY,
                status="fresh",
                captured_at=captured_at,
                symbols_json=json.dumps(["002313.SZ", "600114.SH", "000969.SZ"]),
                provenance_json=json.dumps(
                    {
                        "002313.SZ": {"sources": ["qmt_holding"]},
                        "600114.SH": {"sources": ["qmt_holding", "vault_active"]},
                        "000969.SZ": {"sources": ["vault_active"]},
                    }
                ),
            )
        )
        await session.commit()
        pool = await SentimentFocusPoolResolver(
            session,
            vault_root=vault_root,
            account_snapshot=unavailable_snapshot,
            now=lambda: now,
        ).resolve()
        await session.commit()

    assert [target.symbol for target in pool.targets] == [
        "002313.SZ",
        "600114.SH",
        "002138.SZ",
    ]
    assert pool.qmt_status == "stale"
    assert pool.snapshot_captured_at == captured_at
    assert pool.targets[0].sources == ("qmt_holding_stale",)
    assert pool.targets[1].sources == ("qmt_holding_stale", "vault_active")


@pytest.mark.asyncio
async def test_expired_qmt_snapshot_falls_back_to_vault_only(tmp_path) -> None:
    vault_root = tmp_path / "待观察"
    _write_note(
        vault_root / "vault-only.md",
        "type: company-research\nstatus: active\nsymbol: 002138.SZ",
    )
    now = datetime(2026, 7, 17, 10, 0)

    async def unavailable_snapshot():
        raise RuntimeError("account sensitive-account disconnected")

    async with async_session_factory() as session:
        session.add(
            SentimentFocusSnapshot(
                snapshot_key=FOCUS_SNAPSHOT_KEY,
                status="fresh",
                captured_at=now - timedelta(hours=25),
                symbols_json=json.dumps(["002313.SZ"]),
                provenance_json=json.dumps(
                    {"002313.SZ": {"sources": ["qmt_holding"]}}
                ),
            )
        )
        await session.commit()
        pool = await SentimentFocusPoolResolver(
            session,
            vault_root=vault_root,
            account_snapshot=unavailable_snapshot,
            now=lambda: now,
        ).resolve()
        await session.commit()
        rows = (
            await session.execute(
                select(SentimentFocusSnapshot).order_by(SentimentFocusSnapshot.id)
            )
        ).scalars().all()

    assert [target.symbol for target in pool.targets] == ["002138.SZ"]
    assert pool.qmt_status == "unavailable"
    assert pool.warning_code == "qmt_holdings_unavailable"
    assert rows[-1].status == "unavailable"
    assert "sensitive-account" not in str(rows[-1].error_summary)

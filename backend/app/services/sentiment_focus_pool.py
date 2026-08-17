from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Awaitable, Callable

import yaml
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models.sentiment import SentimentFocusSnapshot
from app.services.sentiment import normalize_sentiment_symbol

FOCUS_SNAPSHOT_KEY = "sentiment_xueqiu_focus_v1"
_QMT_SNAPSHOT_MAX_AGE = timedelta(hours=24)
_CANONICAL_SYMBOL = re.compile(r"^\d{6}\.(?:SH|SZ|BJ)$")
_DATED_HEADING = re.compile(r"^#{1,6}\s+(\d{4}-\d{2}-\d{2}\b.*)$", re.MULTILINE)


@dataclass(frozen=True)
class VaultFocusTarget:
    symbol: str
    note_path: str
    latest_tracking_heading: str | None = None


@dataclass(frozen=True)
class FocusTarget:
    symbol: str
    sources: tuple[str, ...]
    note_path: str | None = None
    latest_tracking_heading: str | None = None

    def as_dict(self) -> dict[str, object]:
        details: dict[str, object] = {
            "symbol": self.symbol,
            "sources": list(self.sources),
        }
        if self.note_path is not None:
            details["vault_note_path"] = self.note_path
        if self.latest_tracking_heading is not None:
            details["latest_tracking_heading"] = self.latest_tracking_heading
        return details


@dataclass(frozen=True)
class ResolvedFocusPool:
    targets: tuple[FocusTarget, ...]
    qmt_status: str
    vault_count: int
    overlap_count: int
    snapshot_captured_at: datetime | None
    warning_code: str | None = None

    def as_details(self) -> dict[str, object]:
        sources = list(
            dict.fromkeys(source for target in self.targets for source in target.sources)
        )
        details: dict[str, object] = {
            "sources": sources,
            "symbol_count": len(self.targets),
            "qmt_status": self.qmt_status,
            "vault_count": self.vault_count,
            "overlap_count": self.overlap_count,
            "snapshot_captured_at": (
                self.snapshot_captured_at.isoformat()
                if self.snapshot_captured_at is not None
                else None
            ),
            "targets": [target.as_dict() for target in self.targets],
        }
        if self.warning_code is not None:
            details["warning_code"] = self.warning_code
        return details


class SentimentFocusPoolResolver:
    def __init__(
        self,
        session: AsyncSession | None,
        *,
        vault_root: Path | str | None = None,
        account_snapshot: Callable[[], Awaitable[object]] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._session = session
        self._vault_root = Path(vault_root or settings.sentiment_focus_vault_dir)
        if account_snapshot is None:
            from app.services.qmt_trading import qmt_trading_service

            account_snapshot = qmt_trading_service.account_snapshot
        self._account_snapshot = account_snapshot
        self._now = now or datetime.now

    def read_vault_targets(self) -> list[VaultFocusTarget]:
        if not self._vault_root.is_dir():
            logger.warning("Sentiment focus Vault directory is unavailable: {}", self._vault_root)
            return []

        targets: dict[str, VaultFocusTarget] = {}
        for note_path in sorted(self._vault_root.rglob("*.md")):
            try:
                text = note_path.read_text(encoding="utf-8")
                metadata = self._parse_frontmatter(text)
            except (OSError, UnicodeError, yaml.YAMLError) as exc:
                logger.warning("Skipping invalid sentiment focus note {}: {}", note_path, exc)
                continue
            if metadata is None:
                continue

            raw_symbol = metadata.get("symbol")
            if (
                metadata.get("type") != "company-research"
                or metadata.get("status") != "active"
                or not isinstance(raw_symbol, str)
                or _CANONICAL_SYMBOL.fullmatch(raw_symbol) is None
            ):
                continue

            symbol = normalize_sentiment_symbol(raw_symbol)
            headings = _DATED_HEADING.findall(text)
            latest_heading = max(headings, default=None)
            targets.setdefault(
                symbol,
                VaultFocusTarget(
                    symbol=symbol,
                    note_path=note_path.relative_to(self._vault_root).as_posix(),
                    latest_tracking_heading=latest_heading,
                ),
            )
        return [targets[symbol] for symbol in sorted(targets)]

    async def resolve(self) -> ResolvedFocusPool:
        if self._session is None:
            raise RuntimeError("A database session is required to resolve sentiment focus targets")

        vault_targets = self.read_vault_targets()
        vault_by_symbol = {target.symbol: target for target in vault_targets}
        now = self._now()
        qmt_status = "fresh"
        qmt_source = "qmt_holding"
        qmt_symbols: list[str]
        captured_at: datetime | None = now
        warning_code: str | None = None
        error_summary: str | None = None

        try:
            account = await self._account_snapshot()
            if getattr(account, "error", None):
                raise RuntimeError("QMT account snapshot reported an error")
            qmt_symbols = self._positive_holding_symbols(account)
        except Exception as exc:
            error_summary = f"{type(exc).__name__}: QMT account snapshot unavailable"
            latest_fresh = await self._latest_fresh_snapshot()
            qmt_symbols = []
            if latest_fresh is not None and latest_fresh.captured_at is not None:
                age = now - latest_fresh.captured_at
                if timedelta(0) <= age < _QMT_SNAPSHOT_MAX_AGE:
                    qmt_symbols = self._qmt_symbols_from_snapshot(latest_fresh)
                    captured_at = latest_fresh.captured_at
            if qmt_symbols:
                qmt_status = "stale"
                qmt_source = "qmt_holding_stale"
                logger.warning("Using a recent sanitized QMT holdings snapshot for sentiment focus")
            else:
                qmt_status = "unavailable"
                captured_at = None
                warning_code = "qmt_holdings_unavailable"
                logger.warning("QMT holdings are unavailable; sentiment focus is Vault-only")

        qmt_symbol_set = set(qmt_symbols)
        targets: list[FocusTarget] = []
        for symbol in qmt_symbols:
            note = vault_by_symbol.get(symbol)
            sources = (qmt_source, "vault_active") if note is not None else (qmt_source,)
            targets.append(
                FocusTarget(
                    symbol=symbol,
                    sources=sources,
                    note_path=note.note_path if note is not None else None,
                    latest_tracking_heading=(
                        note.latest_tracking_heading if note is not None else None
                    ),
                )
            )
        for symbol in sorted(set(vault_by_symbol) - qmt_symbol_set):
            note = vault_by_symbol[symbol]
            targets.append(
                FocusTarget(
                    symbol=symbol,
                    sources=("vault_active",),
                    note_path=note.note_path,
                    latest_tracking_heading=note.latest_tracking_heading,
                )
            )

        resolved = ResolvedFocusPool(
            targets=tuple(targets),
            qmt_status=qmt_status,
            vault_count=len(vault_by_symbol),
            overlap_count=len(qmt_symbol_set & set(vault_by_symbol)),
            snapshot_captured_at=captured_at,
            warning_code=warning_code,
        )
        self._session.add(
            SentimentFocusSnapshot(
                snapshot_key=FOCUS_SNAPSHOT_KEY,
                status=qmt_status,
                captured_at=captured_at,
                symbols_json=json.dumps(
                    [target.symbol for target in targets],
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                provenance_json=json.dumps(
                    {target.symbol: target.as_dict() for target in targets},
                    ensure_ascii=True,
                    separators=(",", ":"),
                ),
                error_summary=error_summary,
            )
        )
        return resolved

    @staticmethod
    def _positive_holding_symbols(account: object) -> list[str]:
        positions = getattr(account, "positions", {})
        symbols: set[str] = set()
        for raw_symbol, position in positions.items():
            try:
                if float(getattr(position, "quantity", 0)) <= 0:
                    continue
                symbols.add(normalize_sentiment_symbol(str(raw_symbol)))
            except (TypeError, ValueError):
                logger.warning("Skipping an invalid QMT position in sentiment focus resolution")
        return sorted(symbols)

    async def _latest_fresh_snapshot(self) -> SentimentFocusSnapshot | None:
        result = await self._session.execute(
            select(SentimentFocusSnapshot)
            .where(
                SentimentFocusSnapshot.snapshot_key == FOCUS_SNAPSHOT_KEY,
                SentimentFocusSnapshot.status == "fresh",
            )
            .order_by(
                SentimentFocusSnapshot.captured_at.desc(),
                SentimentFocusSnapshot.id.desc(),
            )
            .limit(1)
        )
        return result.scalar_one_or_none()

    @staticmethod
    def _qmt_symbols_from_snapshot(snapshot: SentimentFocusSnapshot) -> list[str]:
        try:
            ordered_symbols = json.loads(snapshot.symbols_json)
            provenance = json.loads(snapshot.provenance_json)
        except (json.JSONDecodeError, TypeError):
            return []
        if not isinstance(ordered_symbols, list) or not isinstance(provenance, dict):
            return []

        result: list[str] = []
        for raw_symbol in ordered_symbols:
            item = provenance.get(raw_symbol)
            sources = item.get("sources") if isinstance(item, dict) else None
            if not isinstance(sources, list) or "qmt_holding" not in sources:
                continue
            try:
                result.append(normalize_sentiment_symbol(str(raw_symbol)))
            except ValueError:
                continue
        return list(dict.fromkeys(result))

    @staticmethod
    def _parse_frontmatter(text: str) -> dict[str, object] | None:
        lines = text.splitlines()
        if not lines or lines[0].strip() != "---":
            return None
        try:
            closing_index = next(
                index for index, line in enumerate(lines[1:], start=1) if line.strip() == "---"
            )
        except StopIteration:
            return None
        payload = yaml.safe_load("\n".join(lines[1:closing_index]))
        return payload if isinstance(payload, dict) else None

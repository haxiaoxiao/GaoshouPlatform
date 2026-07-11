from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class AdapterRequest:
    symbol: str | None
    max_pages: int
    min_reply: int
    start_date: date | None
    end_date: date | None
    force_refresh: bool


@dataclass
class AdapterResult:
    posts: list[Any]
    stats: dict[str, Any]


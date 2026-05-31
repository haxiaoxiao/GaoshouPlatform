"""Unified data view for factor computation.

The first implementation is intentionally lightweight: it wraps the existing
`dict[symbol, DataFrame]` structure and exposes stable panel-style helpers.
This gives DSL, Python factors, and future engines a shared abstraction without
forcing an immediate rewrite to xarray or a custom matrix store.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Mapping, Sequence

import pandas as pd


@dataclass
class FactorDataView:
    """Entity-time-field panel plus point-in-time metadata."""

    data: Mapping[str, pd.DataFrame]
    metadata: Mapping[str, pd.DataFrame] = field(default_factory=dict)
    as_of_date: date | None = None
    as_of_time: str | None = None
    frequency: str = "1d"
    data_policy: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_frames(
        cls,
        data: Mapping[str, pd.DataFrame],
        *,
        metadata: Mapping[str, pd.DataFrame] | None = None,
        as_of_date: date | None = None,
        as_of_time: str | None = None,
        frequency: str = "1d",
        data_policy: dict[str, Any] | None = None,
    ) -> FactorDataView:
        return cls(
            data={str(symbol): frame.copy() for symbol, frame in data.items()},
            metadata=dict(metadata or {}),
            as_of_date=as_of_date,
            as_of_time=as_of_time,
            frequency=frequency,
            data_policy=dict(data_policy or {}),
        )

    @property
    def symbols(self) -> list[str]:
        return sorted(self.data.keys())

    def field(self, name: str) -> dict[str, pd.Series]:
        """Return one field as `{symbol: Series}`."""
        result: dict[str, pd.Series] = {}
        for symbol, frame in self.data.items():
            if name not in frame.columns:
                continue
            result[str(symbol)] = pd.to_numeric(frame[name], errors="coerce")
        return result

    def panel(self, field: str) -> pd.DataFrame:
        """Return a date x symbol matrix for one field."""
        series_map = self.field(field)
        if not series_map:
            return pd.DataFrame()
        matrix = pd.DataFrame(series_map)
        return matrix.sort_index()

    def cross_section(self, field: str, trade_date: date | datetime | str) -> pd.Series:
        """Return symbol-indexed values at one date/time."""
        target = pd.Timestamp(trade_date)
        values: dict[str, float] = {}
        for symbol, series in self.field(field).items():
            if series.empty:
                continue
            index = pd.DatetimeIndex(pd.to_datetime(series.index))
            local = series.copy()
            local.index = index
            matches = local.loc[index.normalize() == target.normalize()]
            if not matches.empty and pd.notna(matches.iloc[-1]):
                values[symbol] = float(matches.iloc[-1])
        return pd.Series(values, dtype=float)

    def meta(self, name: str, symbols: Sequence[str] | None = None) -> pd.DataFrame:
        frame = self.metadata.get(name)
        if frame is None:
            return pd.DataFrame()
        if symbols is None or "symbol" not in frame.columns:
            return frame.copy()
        return frame[frame["symbol"].astype(str).isin([str(s) for s in symbols])].copy()


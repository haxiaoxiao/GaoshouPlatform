"""Reusable factor preprocessing and scoring helpers.

This module is intentionally independent from any single strategy. Strategies
should request a prepared cross-section here, then focus on portfolio decisions.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Protocol, Sequence

import numpy as np
import pandas as pd
from loguru import logger

from app.core.config import settings
from app.services.factor_value_store import FactorValueStore, get_factor_value_store


@dataclass(frozen=True)
class FactorSpec:
    """One factor input and its preprocessing policy."""

    name: str
    weight: float = 1.0
    direction: str = "higher_better"
    transform: str = "rank_pct"
    as_of_time: str | None = None
    params: dict[str, Any] | None = None
    neutralize_market_cap: bool = False
    industry_zscore: bool = False
    alias: str | None = None

    @classmethod
    def from_raw(cls, raw: FactorSpec | dict[str, Any]) -> FactorSpec:
        if isinstance(raw, cls):
            return raw
        return cls(
            name=str(raw["name"]),
            weight=float(raw.get("weight", 1.0)),
            direction=str(raw.get("direction", "higher_better")),
            transform=str(raw.get("transform", "rank_pct")),
            as_of_time=raw.get("as_of_time"),
            params=dict(raw.get("params") or {}) if raw.get("params") is not None else None,
            neutralize_market_cap=bool(raw.get("neutralize_market_cap", False)),
            industry_zscore=bool(raw.get("industry_zscore", False)),
            alias=raw.get("alias"),
        )

    @property
    def column(self) -> str:
        return str(self.alias or self.name)

    @property
    def direction_sign(self) -> float:
        text = self.direction.strip().lower()
        if text in {"lower_better", "low", "ascending", "asc", "-1"}:
            return -1.0
        return 1.0


@dataclass(frozen=True)
class FilterSpec:
    """One exclusion rule loaded from factor values."""

    name: str
    value: float = 0.5
    operator: str = ">="
    as_of_time: str | None = None
    params: dict[str, Any] | None = None

    @classmethod
    def from_raw(cls, raw: FilterSpec | dict[str, Any]) -> FilterSpec:
        if isinstance(raw, cls):
            return raw
        return cls(
            name=str(raw["name"]),
            value=float(raw.get("value", raw.get("threshold", 0.5))),
            operator=str(raw.get("operator", ">=")),
            as_of_time=raw.get("as_of_time"),
            params=dict(raw.get("params") or {}) if raw.get("params") is not None else None,
        )


@dataclass
class FactorPipelineResult:
    """Prepared factor cross-section for a single rebalance date."""

    frame: pd.DataFrame
    raw: pd.DataFrame
    metadata: pd.DataFrame
    excluded_symbols: set[str] = field(default_factory=set)


class ModelScorer(Protocol):
    """Scoring protocol reserved for future ML models."""

    def score(self, frame: pd.DataFrame, factor_specs: Sequence[FactorSpec]) -> pd.Series:
        """Return one score per symbol in ``frame``."""


class LinearFactorScorer:
    """Default weighted linear scorer over processed factor columns."""

    def score(self, frame: pd.DataFrame, factor_specs: Sequence[FactorSpec]) -> pd.Series:
        score = pd.Series(0.0, index=frame.index, dtype="float64")
        weight_sum = pd.Series(0.0, index=frame.index, dtype="float64")
        for spec in factor_specs:
            column = f"processed__{spec.column}"
            if column not in frame.columns:
                continue
            values = pd.to_numeric(frame[column], errors="coerce")
            weight = float(spec.weight)
            valid = values.notna()
            score.loc[valid] += values.loc[valid] * weight
            weight_sum.loc[valid] += abs(weight)
        return score.where(weight_sum <= 0, score / weight_sum).where(weight_sum > 0)


class CustomModelScorer:
    """Placeholder for future sklearn/PyTorch/LightGBM scorers."""

    def score(self, frame: pd.DataFrame, factor_specs: Sequence[FactorSpec]) -> pd.Series:
        raise NotImplementedError(
            "CustomModelScorer is a reserved extension point. "
            "Use LinearFactorScorer or implement a project-specific scorer."
        )


class FactorPreprocessor:
    """Apply cross-sectional preprocessing to raw factor matrices."""

    def preprocess(
        self,
        raw: pd.DataFrame,
        factor_specs: Sequence[FactorSpec | dict[str, Any]],
        *,
        metadata: pd.DataFrame | None = None,
        min_factor_coverage: float = 0.6,
    ) -> pd.DataFrame:
        specs = [FactorSpec.from_raw(item) for item in factor_specs]
        if raw.empty or not specs:
            return pd.DataFrame(index=raw.index)

        metadata = self._align_metadata(raw.index, metadata)
        output = pd.DataFrame(index=raw.index)
        valid_count = pd.Series(0, index=raw.index, dtype="int64")

        for spec in specs:
            if spec.name not in raw.columns:
                output[f"processed__{spec.column}"] = np.nan
                continue
            series = pd.to_numeric(raw[spec.name], errors="coerce").astype("float64")
            if spec.neutralize_market_cap:
                series = self.market_cap_neutralize(series, metadata)
            if spec.industry_zscore:
                series = self.industry_zscore(series, metadata)
                if spec.transform.strip().lower() in {"rank", "rank_pct", "percentile"}:
                    series = self.transform(series, spec.transform)
            else:
                series = self.transform(series, spec.transform)
            if spec.direction_sign < 0:
                if spec.transform.strip().lower() in {"rank", "rank_pct", "percentile"}:
                    series = 1.0 - series
                else:
                    series = -series
            column = f"processed__{spec.column}"
            output[column] = series
            valid_count += series.notna().astype("int64")

        required = max(1, math.ceil(len(specs) * max(0.0, min(1.0, min_factor_coverage))))
        output["valid_factor_count"] = valid_count
        output["factor_coverage"] = valid_count / float(len(specs))
        output = output.where(valid_count >= required)
        output["valid_factor_count"] = valid_count
        output["factor_coverage"] = valid_count / float(len(specs))
        return output

    def transform(self, series: pd.Series, method: str) -> pd.Series:
        method = method.strip().lower()
        if method in {"raw", "none"}:
            return series
        if method in {"rank", "rank_pct", "percentile"}:
            return series.rank(pct=True)
        if method in {"zscore", "z_score"}:
            return self.zscore(series)
        raise ValueError(f"Unsupported factor transform: {method}")

    def zscore(self, series: pd.Series) -> pd.Series:
        values = pd.to_numeric(series, errors="coerce").astype("float64")
        std = values.std(ddof=0)
        if not np.isfinite(std) or std == 0:
            return pd.Series(np.nan, index=series.index, dtype="float64")
        return (values - values.mean()) / std

    def industry_zscore(self, series: pd.Series, metadata: pd.DataFrame) -> pd.Series:
        metadata = self._align_metadata(series.index, metadata)
        industry = metadata.get("industry")
        if industry is None:
            return pd.Series(np.nan, index=series.index, dtype="float64")
        values = pd.to_numeric(series, errors="coerce").astype("float64")
        grouped = values.groupby(industry)
        mean = grouped.transform("mean")
        std = grouped.transform(lambda item: item.std(ddof=0))
        return ((values - mean) / std.replace(0, np.nan)).astype("float64")

    def market_cap_neutralize(self, series: pd.Series, metadata: pd.DataFrame) -> pd.Series:
        metadata = self._align_metadata(series.index, metadata)
        x = pd.to_numeric(metadata.get("log_circ_mv"), errors="coerce")
        y = pd.to_numeric(series, errors="coerce").astype("float64")
        valid = y.notna() & x.notna() & np.isfinite(x)
        result = pd.Series(np.nan, index=series.index, dtype="float64")
        if int(valid.sum()) < 3 or x.loc[valid].nunique(dropna=True) < 2:
            return result
        design = np.column_stack([np.ones(int(valid.sum())), x.loc[valid].to_numpy(dtype=float)])
        target = y.loc[valid].to_numpy(dtype=float)
        beta, *_ = np.linalg.lstsq(design, target, rcond=None)
        fitted = design @ beta
        result.loc[valid] = target - fitted
        return result

    def _align_metadata(
        self,
        index: pd.Index,
        metadata: pd.DataFrame | None,
    ) -> pd.DataFrame:
        if metadata is None or metadata.empty:
            return pd.DataFrame(index=index)
        body = metadata.copy()
        if "symbol" in body.columns:
            body = body.set_index("symbol")
        body.index = body.index.astype(str)
        return body.reindex(index.astype(str))


class FactorPipeline:
    """Load factor values, attach metadata, preprocess, then score."""

    def __init__(self, store: FactorValueStore | None = None):
        self.store = store or get_factor_value_store()
        self.preprocessor = FactorPreprocessor()
        self._cross_section_cache: dict[tuple[Any, ...], dict[str, float]] = {}
        self._metadata_cache: dict[frozenset[str], pd.DataFrame] = {}

    def build_cross_section(
        self,
        *,
        factor_specs: Sequence[FactorSpec | dict[str, Any]],
        trade_date: date,
        symbols: Sequence[str] | None = None,
        filters: Sequence[FilterSpec | dict[str, Any]] | None = None,
        min_factor_coverage: float = 0.6,
        scorer: ModelScorer | None = None,
    ) -> FactorPipelineResult:
        specs = [FactorSpec.from_raw(item) for item in factor_specs]
        symbol_list = [str(item) for item in symbols or [] if str(item).strip()]
        raw = self._load_raw_matrix(specs, trade_date=trade_date, symbols=symbol_list or None)
        if raw.empty:
            return FactorPipelineResult(
                frame=pd.DataFrame(),
                raw=raw,
                metadata=pd.DataFrame(),
                excluded_symbols=set(),
            )

        metadata = self.load_metadata(list(raw.index))
        excluded = self._load_exclusions(
            filters or [],
            trade_date=trade_date,
            symbols=list(raw.index),
        )
        if excluded:
            raw = raw.drop(index=[symbol for symbol in excluded if symbol in raw.index], errors="ignore")
            metadata = metadata.drop(index=[symbol for symbol in excluded if symbol in metadata.index], errors="ignore")
        if raw.empty:
            return FactorPipelineResult(
                frame=pd.DataFrame(),
                raw=raw,
                metadata=metadata,
                excluded_symbols=excluded,
            )

        processed = self.preprocessor.preprocess(
            raw,
            specs,
            metadata=metadata,
            min_factor_coverage=min_factor_coverage,
        )
        frame = pd.concat([raw.add_prefix("raw__"), metadata, processed], axis=1)
        score = (scorer or LinearFactorScorer()).score(frame, specs)
        frame["score"] = score
        frame = frame.dropna(subset=["score"]).sort_values(["score"], ascending=False)
        return FactorPipelineResult(
            frame=frame,
            raw=raw,
            metadata=metadata,
            excluded_symbols=excluded,
        )

    def load_metadata(self, symbols: Sequence[str]) -> pd.DataFrame:
        symbol_list = [str(item) for item in symbols if str(item).strip()]
        if not symbol_list:
            return pd.DataFrame()
        cache_key = frozenset(symbol_list)
        cached = self._metadata_cache.get(cache_key)
        if cached is not None:
            return cached.reindex(symbol_list)
        db_path = Path(settings.data_dir) / "gaoshou.db"
        if not db_path.exists():
            return pd.DataFrame(index=pd.Index(symbol_list, name="symbol"))
        placeholders = ",".join(["?"] * len(symbol_list))
        sql = f"""
            SELECT symbol, industry, industry2, industry3, sector, concept, circ_mv, total_mv
            FROM stocks
            WHERE symbol IN ({placeholders})
        """
        try:
            with sqlite3.connect(db_path) as conn:
                metadata = pd.read_sql_query(sql, conn, params=symbol_list)
        except Exception as exc:
            logger.warning("Failed to load factor metadata from SQLite: {}", exc)
            return pd.DataFrame(index=pd.Index(symbol_list, name="symbol"))
        if metadata.empty:
            return pd.DataFrame(index=pd.Index(symbol_list, name="symbol"))
        metadata["symbol"] = metadata["symbol"].astype(str)
        metadata = metadata.drop_duplicates(subset=["symbol"], keep="last").set_index("symbol")
        circ_mv = pd.to_numeric(metadata.get("circ_mv"), errors="coerce")
        total_mv = pd.to_numeric(metadata.get("total_mv"), errors="coerce")
        market_cap = circ_mv.where(circ_mv > 0, total_mv)
        metadata["log_circ_mv"] = np.log(market_cap.where(market_cap > 0))
        result = metadata.reindex(symbol_list)
        self._metadata_cache[cache_key] = result.copy()
        return result

    def _load_raw_matrix(
        self,
        specs: Sequence[FactorSpec],
        *,
        trade_date: date,
        symbols: Sequence[str] | None,
    ) -> pd.DataFrame:
        values_by_factor: dict[str, dict[str, float]] = {}
        all_symbols: set[str] = set()
        for spec in specs:
            values = self._load_cross_section_cached(spec, trade_date=trade_date, symbols=symbols)
            values = {str(symbol): float(value) for symbol, value in values.items()}
            values_by_factor[spec.name] = values
            all_symbols.update(values)
        if symbols:
            index = [str(symbol) for symbol in symbols]
        else:
            index = sorted(all_symbols)
        if not index:
            return pd.DataFrame()
        raw = pd.DataFrame(index=pd.Index(index, name="symbol"))
        for spec in specs:
            raw[spec.name] = pd.Series(values_by_factor.get(spec.name, {}), dtype="float64")
        return raw

    def _load_exclusions(
        self,
        filters: Sequence[FilterSpec | dict[str, Any]],
        *,
        trade_date: date,
        symbols: Sequence[str],
    ) -> set[str]:
        excluded: set[str] = set()
        for raw_filter in filters:
            spec = FilterSpec.from_raw(raw_filter)
            values = self._load_cross_section_cached(spec, trade_date=trade_date, symbols=symbols)
            for symbol, value in values.items():
                if self._matches_filter(float(value), spec.operator, spec.value):
                    excluded.add(str(symbol))
        return excluded

    def _load_cross_section_cached(
        self,
        spec: FactorSpec | FilterSpec,
        *,
        trade_date: date,
        symbols: Sequence[str] | None,
    ) -> dict[str, float]:
        symbol_key = tuple(str(item) for item in symbols or [])
        params_items = tuple(sorted((spec.params or {}).items())) if spec.params else ()
        cache_key = (
            spec.name,
            trade_date.isoformat(),
            symbol_key,
            spec.as_of_time or "",
            params_items,
        )
        cached = self._cross_section_cache.get(cache_key)
        if cached is not None:
            return cached
        values = self.store.load_cross_section(
            spec.name,
            trade_date,
            symbols=symbols,
            as_of_time=spec.as_of_time,
            params=spec.params,
        )
        self._cross_section_cache[cache_key] = values
        return values

    @staticmethod
    def _matches_filter(value: float, operator: str, threshold: float) -> bool:
        op = operator.strip()
        if op == ">=":
            return value >= threshold
        if op == ">":
            return value > threshold
        if op == "<=":
            return value <= threshold
        if op == "<":
            return value < threshold
        if op in {"==", "="}:
            return value == threshold
        if op == "!=":
            return value != threshold
        raise ValueError(f"Unsupported filter operator: {operator}")

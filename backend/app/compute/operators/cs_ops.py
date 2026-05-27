"""Cross-sectional factor operators."""

from __future__ import annotations

import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class _CsOp(Operator):
    level = 2
    category = "cross_section"

    def _series(self, kwargs: dict) -> pd.Series:
        series = kwargs.get("series")
        if not isinstance(series, pd.Series):
            raise TypeError(f"{self.name} requires a pd.Series")
        return series.astype(float)


class CsRankOp(_CsOp):
    name = "cs_rank"
    signature = "cs_rank(series)"
    description = "Cross-sectional percentile rank at each timestamp."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = self._series(kwargs)
        if isinstance(series.index, pd.MultiIndex):
            return series.groupby(level=0).rank(pct=True)
        return series.rank(pct=True)


class CsZScoreOp(_CsOp):
    name = "cs_zscore"
    signature = "cs_zscore(series)"
    description = "Cross-sectional z-score at each timestamp."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = self._series(kwargs)
        if isinstance(series.index, pd.MultiIndex):
            grouped = series.groupby(level=0)
            return (series - grouped.transform("mean")) / grouped.transform("std").replace(0, pd.NA)
        std = series.std()
        return (series - series.mean()) / (std if std else pd.NA)


class CsWinsorizeOp(_CsOp):
    name = "cs_winsorize"
    signature = "cs_winsorize(series, limit)"
    description = "Cross-sectional quantile clipping."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = self._series(kwargs)
        limit = float(kwargs.get("period", kwargs.get("limit", 0.025)))

        def _clip(group: pd.Series) -> pd.Series:
            return group.clip(group.quantile(limit), group.quantile(1 - limit))

        if isinstance(series.index, pd.MultiIndex):
            return series.groupby(level=0, group_keys=False).apply(_clip)
        return _clip(series)


class CsPercentileOp(_CsOp):
    name = "cs_percentile"
    signature = "cs_percentile(series)"
    description = "Alias of cross-sectional percentile rank."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        return CsRankOp().evaluate(df, **kwargs)


for op in [CsRankOp(), CsZScoreOp(), CsWinsorizeOp(), CsPercentileOp()]:
    OperatorRegistry.register(op)


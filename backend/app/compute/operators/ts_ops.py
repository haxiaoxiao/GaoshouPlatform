"""Time-series factor operators."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class _TsOp(Operator):
    level = 2
    category = "time_series"

    def _series(self, kwargs: dict) -> pd.Series:
        series = kwargs.get("series")
        if not isinstance(series, pd.Series):
            raise TypeError(f"{self.name} requires a pd.Series")
        return series

    def _window(self, kwargs: dict, default: int = 5) -> int:
        return max(int(kwargs.get("period", kwargs.get("window", default))), 1)


class TsDelayOp(_TsOp):
    name = "ts_delay"
    signature = "ts_delay(series, periods)"
    description = "Shift a time series by N periods."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        return self._series(kwargs).shift(self._window(kwargs, 1))


class TsDeltaOp(_TsOp):
    name = "ts_delta"
    signature = "ts_delta(series, periods)"
    description = "Current value minus value N periods ago."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = self._series(kwargs)
        return series - series.shift(self._window(kwargs, 1))


class TsSumOp(_TsOp):
    name = "ts_sum"
    signature = "ts_sum(series, window)"
    description = "Rolling time-series sum."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        window = self._window(kwargs)
        return self._series(kwargs).rolling(window=window, min_periods=max(1, window // 2)).sum()


class TsMeanOp(_TsOp):
    name = "ts_mean"
    signature = "ts_mean(series, window)"
    description = "Rolling time-series mean."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        window = self._window(kwargs)
        return self._series(kwargs).rolling(window=window, min_periods=max(1, window // 2)).mean()


class TsStdOp(_TsOp):
    name = "ts_std"
    signature = "ts_std(series, window)"
    description = "Rolling time-series standard deviation."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        window = self._window(kwargs)
        return self._series(kwargs).rolling(window=window, min_periods=max(1, window // 2)).std()


class TsRankOp(_TsOp):
    name = "ts_rank"
    signature = "ts_rank(series, window)"
    description = "Percentile rank of the latest value inside a rolling window."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        window = self._window(kwargs)

        def _rank(values):
            if len(values) == 0 or np.isnan(values[-1]):
                return np.nan
            return pd.Series(values).rank(pct=True).iloc[-1]

        return self._series(kwargs).rolling(window=window, min_periods=max(1, window // 2)).apply(_rank, raw=True)


class TsCorrOp(Operator):
    name = "ts_corr"
    level = 2
    category = "time_series"
    signature = "ts_corr(series_a, series_b, window)"
    description = "Rolling time-series correlation."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        a = kwargs.get("series_a")
        b = kwargs.get("series_b")
        window = int(kwargs.get("period", kwargs.get("window", 20)))
        if not isinstance(a, pd.Series) or not isinstance(b, pd.Series):
            raise TypeError("ts_corr requires two pd.Series arguments")
        return a.rolling(window=window, min_periods=max(1, window // 2)).corr(b)


class TsArgMaxOp(_TsOp):
    name = "ts_argmax"
    signature = "ts_argmax(series, window)"
    description = "1-based position of the maximum value inside a rolling window."

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        window = self._window(kwargs)
        return self._series(kwargs).rolling(window=window, min_periods=max(1, window // 2)).apply(
            lambda values: float(np.argmax(values) + 1) if len(values) else np.nan,
            raw=True,
        )


for op in [
    TsDelayOp(),
    TsDeltaOp(),
    TsSumOp(),
    TsMeanOp(),
    TsStdOp(),
    TsRankOp(),
    TsCorrOp(),
    TsArgMaxOp(),
]:
    OperatorRegistry.register(op)

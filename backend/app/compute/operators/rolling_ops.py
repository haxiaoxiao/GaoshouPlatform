"""L2 滚动窗口算子 — 时间序列统计函数"""
import numpy as np
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class _RollingOp(Operator):
    """滚动窗口算子基类"""

    level: int = 2
    category: str = "rolling"

    def _get_series(self, kwargs: dict) -> pd.Series:
        series = kwargs.get("series")
        if series is None: raise ValueError(f"{self.name} requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"{self.name} 'series' must be pd.Series, got {type(series)}")
        return series

    def _get_window(self, kwargs: dict) -> int:
        w = kwargs.get("period", kwargs.get("window", 5))
        return int(w)


class MeanOp(_RollingOp):
    name: str = "Mean"
    signature: str = "Mean(series, period)"
    description: str = "N 期滚动均值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).mean()


class StdOp(_RollingOp):
    name: str = "Std"
    signature: str = "Std(series, period)"
    description: str = "N 期滚动标准差"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).std()


class MaxOp(_RollingOp):
    name: str = "Max"
    signature: str = "Max(series, period)"
    description: str = "N 期滚动最大值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).max()


class MinOp(_RollingOp):
    name: str = "Min"
    signature: str = "Min(series, period)"
    description: str = "N 期滚动最小值"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).min()


class SumOp(_RollingOp):
    name: str = "Sum"
    signature: str = "Sum(series, period)"
    description: str = "N 期滚动求和"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)
        return s.rolling(window=w, min_periods=max(1, w // 2)).sum()


class CorrOp(Operator):
    name: str = "Corr"
    level: int = 2
    category: str = "rolling"
    signature: str = "Corr(series_a, series_b, period)"
    description: str = "N 期两序列滚动相关系数"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        a = kwargs.get("series_a")
        b = kwargs.get("series_b")
        w = int(kwargs.get("period", 20))
        if a is None or b is None:
            raise ValueError("Corr requires 'series_a' and 'series_b' arguments")
        if not isinstance(a, pd.Series) or not isinstance(b, pd.Series):
            raise TypeError("Corr series arguments must be pd.Series")
        return a.rolling(window=w, min_periods=max(1, w // 2)).corr(b)


class CovOp(Operator):
    name: str = "Cov"
    level: int = 2
    category: str = "rolling"
    signature: str = "Cov(series_a, series_b, period)"
    description: str = "N 期两序列滚动协方差"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        a = kwargs.get("series_a")
        b = kwargs.get("series_b")
        w = int(kwargs.get("period", 20))
        if a is None or b is None:
            raise ValueError("Cov requires 'series_a' and 'series_b'")
        if not isinstance(a, pd.Series) or not isinstance(b, pd.Series):
            raise TypeError("Cov series arguments must be pd.Series")
        return a.rolling(window=w, min_periods=max(1, w // 2)).cov(b)


class SlopeOp(_RollingOp):
    name: str = "Slope"
    signature: str = "Slope(series, period)"
    description: str = "N 期滚动线性回归斜率"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        s = self._get_series(kwargs)
        w = self._get_window(kwargs)

        def _slope(x):
            if len(x) < 2:
                return np.nan
            y = x.values
            t = np.arange(len(y))
            return np.polyfit(t, y, 1)[0]

        return s.rolling(window=w, min_periods=2).apply(_slope, raw=False)


for op in [MeanOp(), StdOp(), MaxOp(), MinOp(), SumOp(), CorrOp(), CovOp(), SlopeOp()]:
    OperatorRegistry.register(op)

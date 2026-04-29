"""L1 数学/统计算子 — 逐元素运算 + 横截面排序"""
import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class UnaryMathOp(Operator):
    """一元数学算子"""

    level: int = 1
    category: str = "math"

    def __init__(self, name: str, fn, signature: str, description: str = ""):
        self.name = name
        self._fn = fn
        self.signature = signature
        self.description = description

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        raise NotImplementedError("UnaryMathOp 由 Evaluator 在表达式求值阶段调用，不直接通过 evaluate(df)")


class DelayOp(Operator):
    """延时算子 — Shift series by N periods"""

    name: str = "Delay"
    level: int = 1
    category: str = "math"
    signature: str = "Delay(series, period)"
    description: str = "将序列向前移动 N 期"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = kwargs.get("series")
        period = kwargs.get("period", 1)
        if series is None:
            raise ValueError("Delay requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"Delay 'series' must be pd.Series, got {type(series)}")
        return series.shift(period)


class DeltaOp(Operator):
    """变动算子 — 当期值减 N 期前值"""

    name: str = "Delta"
    level: int = 1
    category: str = "math"
    signature: str = "Delta(series, period)"
    description: str = "当期值与 N 期前值的差"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = kwargs.get("series")
        period = kwargs.get("period", 1)
        if series is None:
            raise ValueError("Delta requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"Delta 'series' must be pd.Series, got {type(series)}")
        return series - series.shift(period)


class RankOp(Operator):
    """横截面排名算子 — 在某个交易日所有股票中排名"""

    name: str = "Rank"
    level: int = 1
    category: str = "math"
    signature: str = "Rank(series)"
    description: str = "横截面排名(百分位)"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        series = kwargs.get("series")
        if series is None:
            raise ValueError("Rank requires 'series' argument")
        if not isinstance(series, pd.Series):
            raise TypeError(f"Rank 'series' must be pd.Series, got {type(series)}")
        return series.rank(pct=True)


for op in [
    DelayOp(),
    DeltaOp(),
    RankOp(),
]:
    OperatorRegistry.register(op)

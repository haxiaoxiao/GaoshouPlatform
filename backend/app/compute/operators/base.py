"""算子基类 — 统一计算接口"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Operator(ABC):
    """算子基类 — 所有 L0-L3 算子的父类"""

    name: str = ""
    description: str = ""
    level: int = 0
    signature: str = ""
    category: str = ""

    @abstractmethod
    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        """对 DataFrame 计算该算子，返回一维 Series"""
        ...

    def __repr__(self):
        return f"<Operator {self.signature or self.name} L{self.level}>"


class RawFieldOperator(Operator):
    """L0 原始字段算子 — 直接提取 DataFrame 列"""

    level: int = 0
    category: str = "raw_field"

    def __init__(self, name: str, column: str, description: str = ""):
        self.name = name
        self.column = column
        self.signature = f"${name}"
        self.description = description or f"原始字段: {column}"

    def evaluate(self, df: pd.DataFrame, **kwargs) -> pd.Series:
        if self.column not in df.columns:
            raise KeyError(f"Column '{self.column}' not found in DataFrame")
        return df[self.column].astype(float)

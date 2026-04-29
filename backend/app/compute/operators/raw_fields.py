"""L0 原始字段算子 — OHLCV + 成交额 + 换手率"""
from app.compute.operators.base import RawFieldOperator
from app.compute.operators.registry import OperatorRegistry

RAW_FIELDS = [
    ("open", "open", "开盘价"),
    ("high", "high", "最高价"),
    ("low", "low", "最低价"),
    ("close", "close", "收盘价"),
    ("volume", "volume", "成交量(股)"),
    ("amount", "amount", "成交额(元)"),
    ("turnover", "turnover_rate", "换手率"),
]

for name, column, desc in RAW_FIELDS:
    OperatorRegistry.register(RawFieldOperator(name, column, desc))

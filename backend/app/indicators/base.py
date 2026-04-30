from __future__ import annotations

from datetime import date
from typing import Any


class IndicatorContext:
    """指标计算上下文"""

    def __init__(
        self,
        symbol: str | None = None,
        trade_date: date | None = None,
        stock_info: dict[str, Any] | None = None,
        kline_data: list[dict[str, Any]] | None = None,
    ):
        self.symbol = symbol
        self.trade_date = trade_date or date.today()
        self.stock_info = stock_info
        self.kline_data = kline_data or []


class IndicatorBase:
    """指标基类"""

    name: str = ""
    display_name: str = ""
    category: str = ""
    tags: list[str] = []
    data_type: str = "截面"
    is_precomputed: bool = True
    dependencies: list[str] = []
    description: str = ""
    unit: str = ""  # 单位: % 百分比, 万元, 倍, 元

    def compute(self, context: IndicatorContext) -> float | None:
        raise NotImplementedError

    def compute_batch(
        self,
        symbols: list[str],
        context: IndicatorContext,
    ) -> dict[str, float | None]:
        results: dict[str, float | None] = {}
        for symbol in symbols:
            ctx = IndicatorContext(
                symbol=symbol,
                trade_date=context.trade_date,
                stock_info=context.stock_info,
                kline_data=context.kline_data,
            )
            try:
                results[symbol] = self.compute(ctx)
            except Exception:
                results[symbol] = None
        return results


_CATEGORY_LABELS: dict[str, str] = {
    "valuation": "估值",
    "growth": "成长",
    "quality": "质量",
    "momentum": "动量",
    "volatility": "波动",
    "liquidity": "流动性",
    "technical": "技术",
    "theme": "主题",
}


class IndicatorRegistry:
    """指标注册表"""

    _registry: dict[str, type[IndicatorBase]] = {}

    @classmethod
    def register(cls, indicator_cls: type[IndicatorBase]) -> type[IndicatorBase]:
        cls._registry[indicator_cls.name] = indicator_cls
        return indicator_cls

    @classmethod
    def get(cls, name: str) -> type[IndicatorBase] | None:
        return cls._registry.get(name)

    @classmethod
    def all(cls) -> list[type[IndicatorBase]]:
        return list(cls._registry.values())

    @classmethod
    def by_data_type(cls, data_type: str) -> list[type[IndicatorBase]]:
        return [i for i in cls._registry.values() if i.data_type == data_type]

    @classmethod
    def by_category(cls, category: str) -> list[type[IndicatorBase]]:
        return [i for i in cls._registry.values() if i.category == category]

    @classmethod
    def categories(cls) -> list[dict[str, Any]]:
        cat_counts: dict[str, int] = {}
        for indicator_cls in cls._registry.values():
            cat_counts[indicator_cls.category] = cat_counts.get(indicator_cls.category, 0) + 1
        return [
            {
                "key": key,
                "label": _CATEGORY_LABELS.get(key, key),
                "count": count,
            }
            for key, count in sorted(cat_counts.items())
        ]

    @classmethod
    def auto_discover(cls) -> None:
        import importlib
        import pkgutil
        from app import indicators as pkg

        for _importer, modname, _ispkg in pkgutil.iter_modules(pkg.__path__):
            if modname in ("base", "scheduler", "__init__"):
                continue
            importlib.import_module(f"app.indicators.{modname}")

"""回测引擎注册表"""
from __future__ import annotations

from typing import Any, Type

from app.backtest.engine.interface import IBacktestEngine, IDataProvider
from loguru import logger


class EngineRegistry:
    """引擎注册表 — 通过装饰器注册"""

    _engines: dict[str, Type[IBacktestEngine]] = {}
    _discovered: bool = False

    @classmethod
    def register(cls, engine_class: Type[IBacktestEngine]) -> Type[IBacktestEngine]:
        name = engine_class.name or engine_class.__name__
        cls._engines[name] = engine_class
        return engine_class

    @classmethod
    def get(cls, name: str) -> Type[IBacktestEngine]:
        cls.ensure_discovered()
        if name not in cls._engines:
            available = list(cls._engines.keys())
            raise ValueError(f"Unknown engine '{name}'. Available: {available}")
        return cls._engines[name]

    @classmethod
    def list_all(cls) -> list[dict[str, Any]]:
        cls.ensure_discovered()
        return [
            {
                "name": engine.name,
                "label": engine.label,
                "modes": engine.supported_modes,
            }
            for engine in cls._engines.values()
        ]

    @classmethod
    def ensure_discovered(cls):
        """确保所有引擎模块已导入并注册"""
        if cls._discovered:
            return
        cls._discovered = True

        # 内置引擎
        from app.backtest.engine.builtin import BuiltinEngine  # noqa

        # akquant 引擎 — 未安装时静默跳过
        try:
            from app.backtest.engine.akquant.engine import AkquantEngine  # noqa
        except (ImportError, RuntimeError) as e:
            logger.debug("AkquantEngine not available: {}", e)

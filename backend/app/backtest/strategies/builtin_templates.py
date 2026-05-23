"""Registry for built-in strategy templates shown in the backtest UI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.backtest.strategies.dual_stock_grid_akquant import (
    DEFAULT_DUAL_STOCK_GRID_PARAM_GRID,
    DEFAULT_DUAL_STOCK_GRID_PARAMS,
    DUAL_STOCK_GRID_STRATEGY_CODE,
    DUAL_STOCK_GRID_SYMBOLS,
)
from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)


@dataclass(frozen=True)
class BuiltinStrategyTemplate:
    key: str
    name: str
    description: str
    code: str
    parameters: dict[str, Any] = field(default_factory=dict)
    engine: str = "akquant"
    mode: str = "event_driven"
    bar_type: str = "daily"
    symbols: list[str] = field(default_factory=list)
    param_grid: dict[str, list[Any]] = field(default_factory=dict)
    walk_forward: dict[str, Any] = field(default_factory=dict)

    def to_preset_dict(self) -> dict[str, Any]:
        return {
            "strategy_key": self.key,
            "name": self.name,
            "description": self.description,
            "symbols": self.symbols,
            "engine": self.engine,
            "mode": self.mode,
            "bar_type": self.bar_type,
            "strategy_code": self.code,
            "strategy_params": self.parameters,
            "param_grid": self.param_grid,
            "walk_forward": self.walk_forward,
        }

    def to_list_item(self) -> dict[str, Any]:
        return {
            "strategy_key": self.key,
            "name": self.name,
            "description": self.description,
            "engine": self.engine,
            "mode": self.mode,
            "bar_type": self.bar_type,
            "symbols": self.symbols,
            "has_param_grid": bool(self.param_grid),
            "has_walk_forward_defaults": bool(self.walk_forward),
        }


MULTI_FACTOR_PARAMETERS = {
    **DEFAULT_MULTI_FACTOR_PARAMS,
    "risk_config": DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    "backtest_settings": {
        "engine": "akquant",
        "barType": "daily",
        "showOptimizationPanel": False,
    },
}


BUILTIN_STRATEGY_TEMPLATES: dict[str, BuiltinStrategyTemplate] = {
    "dual_stock_grid": BuiltinStrategyTemplate(
        key="dual_stock_grid",
        name="双标的底仓网格",
        description="完美世界 + 昆仑万维分钟级底仓网格策略。",
        code=DUAL_STOCK_GRID_STRATEGY_CODE,
        parameters=DEFAULT_DUAL_STOCK_GRID_PARAMS,
        bar_type="minute",
        symbols=DUAL_STOCK_GRID_SYMBOLS,
        param_grid=DEFAULT_DUAL_STOCK_GRID_PARAM_GRID,
        walk_forward={
            "train_period": 14400,
            "test_period": 2400,
            "metric": "calmar_ratio",
            "ascending": False,
        },
    ),
    "multi_factor": BuiltinStrategyTemplate(
        key="multi_factor",
        name="通用多因子模型",
        description="AKQuant 通用多因子选股模板；因子、权重、标准化和 ML 接口在策略代码顶部配置。",
        code=MULTI_FACTOR_STRATEGY_CODE,
        parameters=MULTI_FACTOR_PARAMETERS,
        bar_type="daily",
    ),
}


def list_builtin_strategy_templates() -> list[dict[str, Any]]:
    return [item.to_list_item() for item in BUILTIN_STRATEGY_TEMPLATES.values()]


def get_builtin_strategy_template(key: str) -> BuiltinStrategyTemplate | None:
    normalized = str(key or "").strip().replace("-", "_")
    return BUILTIN_STRATEGY_TEMPLATES.get(normalized)


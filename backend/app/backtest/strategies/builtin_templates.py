"""Registry for built-in strategy templates shown in the backtest UI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.backtest.strategies.cn_paper_factor_akquant import (
    CN_PAPER_FACTOR_STRATEGY_CODE,
    DEFAULT_CN_PAPER_FACTOR_PARAMS,
    DEFAULT_CN_PAPER_FACTOR_RISK_CONFIG,
)
from app.backtest.strategies.cn_paper_style_rotation_akquant import (
    CN_PAPER_DEFENSIVE_ALLOCATION_STRATEGY_CODE,
    CN_PAPER_STYLE_ROTATION_STRATEGY_CODE,
    DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_PARAMS,
    DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_RISK_CONFIG,
    DEFAULT_CN_PAPER_STYLE_ROTATION_PARAMS,
    DEFAULT_CN_PAPER_STYLE_ROTATION_RISK_CONFIG,
)
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
from app.backtest.strategies.tech_small_cap_akquant import (
    TECH_SMALL_CAP_STRATEGY_CODE,
    get_tech_small_cap_params,
    get_tech_small_cap_risk_config,
    get_tech_small_cap_variant,
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


CN_PAPER_FACTOR_PARAMETERS = {
    **DEFAULT_CN_PAPER_FACTOR_PARAMS,
    "risk_config": DEFAULT_CN_PAPER_FACTOR_RISK_CONFIG,
    "backtest_settings": {
        "engine": "akquant",
        "barType": "daily",
        "showOptimizationPanel": False,
    },
    "required_factor_group": "cn_paper_implemented",
}


CN_PAPER_STYLE_ROTATION_PARAMETERS = {
    **DEFAULT_CN_PAPER_STYLE_ROTATION_PARAMS,
    "risk_config": DEFAULT_CN_PAPER_STYLE_ROTATION_RISK_CONFIG,
    "backtest_settings": {
        "engine": "akquant",
        "barType": "daily",
        "showOptimizationPanel": False,
    },
    "required_factor_group": "cn_paper_style_rotation",
}


CN_PAPER_DEFENSIVE_ALLOCATION_PARAMETERS = {
    **DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_PARAMS,
    "risk_config": DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_RISK_CONFIG,
    "backtest_settings": {
        "engine": "akquant",
        "barType": "daily",
        "showOptimizationPanel": False,
    },
    "required_factor_group": "cn_paper_style_rotation",
}


TECH_SMALL_CAP_PARAMETERS = {
    **get_tech_small_cap_params(),
    "risk_config": get_tech_small_cap_risk_config(),
    "backtest_settings": {
        "engine": "akquant",
        "barType": "minute_timer",
        "benchmarkSymbol": "399101.SZ",
        "poolSource": {
            "type": "index",
            "label": "中小综指 / 399101.SZ",
            "count": 0,
            "symbols": [],
            "indexSymbol": "399101.SZ",
        },
        "showOptimizationPanel": False,
    },
    "index_symbol": "399101.SZ",
    "required_factor_groups": [
        "small_cap_v4_core",
        "cn_paper_implemented",
        "cn_paper_style_rotation",
    ],
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
    "cn_paper_factor": BuiltinStrategyTemplate(
        key="cn_paper_factor",
        name="研报因子组合",
        description="基于已落地非 Tick 量化研报因子的 AKQuant 月频多因子组合；运行前先预计算 cn_paper_implemented 因子组。",
        code=CN_PAPER_FACTOR_STRATEGY_CODE,
        parameters=CN_PAPER_FACTOR_PARAMETERS,
        bar_type="daily",
    ),
    "cn_paper_style_rotation": BuiltinStrategyTemplate(
        key="cn_paper_style_rotation",
        name="研报风格轮动组合",
        description="基于大小盘、成长价值和行业动量的 A 股风格轮动模板；运行前先预计算 cn_paper_style_rotation 因子组。",
        code=CN_PAPER_STYLE_ROTATION_STRATEGY_CODE,
        parameters=CN_PAPER_STYLE_ROTATION_PARAMETERS,
        bar_type="daily",
    ),
    "cn_paper_defensive_allocation": BuiltinStrategyTemplate(
        key="cn_paper_defensive_allocation",
        name="研报防御配置组合",
        description="中国版全天候增强的权益代理模板，偏向财务健康、低波动和动量回撤质量；宏观和多资产信号仍待数据源。",
        code=CN_PAPER_DEFENSIVE_ALLOCATION_STRATEGY_CODE,
        parameters=CN_PAPER_DEFENSIVE_ALLOCATION_PARAMETERS,
        bar_type="daily",
    ),
    "tech_small_cap": BuiltinStrategyTemplate(
        key="tech_small_cap",
        name="科技小市值多因子",
        description="面向 A 股科技主线的小市值多因子 AKQuant 策略；周频 10:30 调仓，10:00/14:30 使用 minute_timer 做止损与异常放量风控。",
        code=TECH_SMALL_CAP_STRATEGY_CODE,
        parameters=TECH_SMALL_CAP_PARAMETERS,
        bar_type="minute_timer",
    ),
}


def list_builtin_strategy_templates() -> list[dict[str, Any]]:
    return [item.to_list_item() for item in BUILTIN_STRATEGY_TEMPLATES.values()]


def get_builtin_strategy_template(key: str) -> BuiltinStrategyTemplate | None:
    normalized = str(key or "").strip().replace("-", "_")
    return BUILTIN_STRATEGY_TEMPLATES.get(normalized)

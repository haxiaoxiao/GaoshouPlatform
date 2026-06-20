"""回测 UI 中内置策略模板的注册表。

这里统一维护每个预设策略的名称、中文说明、默认参数、回测模式和策略代码。
前端列表、后端创建接口和已保存策略记录都依赖这里作为单一事实来源。
"""

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
    # 通用模板保持引擎原生风格，只向 UI 暴露完成一次标准多因子回测所
    # 需要的最小控制项。
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
    # 科技小市值预设使用 minute_timer，是因为它的风控钩子需要固定盘中
    # 时点来做止损和异常放量检查。
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
        # 显式校验研究链路：如果缺少必须的因子缓存，就快速失败，而不是
        # 悄悄回落到一个更弱、但表面上还能跑的股票池。
        "small_cap_v4_core",
        "cn_paper_implemented",
        "cn_paper_style_rotation",
    ],
}


BUILTIN_STRATEGY_TEMPLATES: dict[str, BuiltinStrategyTemplate] = {
    "dual_stock_grid": BuiltinStrategyTemplate(
        key="dual_stock_grid",
        name="双标的底仓网格",
        description="完美世界 + 昆仑万维的分钟级底仓网格执行策略。",
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
        description="AKQuant 通用多因子选股模板；因子、权重、标准化和模型接口都在策略代码顶部配置。",
        code=MULTI_FACTOR_STRATEGY_CODE,
        parameters=MULTI_FACTOR_PARAMETERS,
        bar_type="daily",
    ),
    "cn_paper_factor": BuiltinStrategyTemplate(
        key="cn_paper_factor",
        name="研报因子组合",
        description="基于已落地非 Tick 研报因子的 AKQuant 多因子组合；运行前先预计算 cn_paper_implemented 因子组。",
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
        description="中国版全天候增强的权益代理模板，偏向财务健康、低波动和动量回撤质量；宏观和多资产信号仍待数据源补齐。",
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
    # UI 消费的是稳定的字典快照，而不是直接把 dataclass 对象吐给前端。
    return [item.to_list_item() for item in BUILTIN_STRATEGY_TEMPLATES.values()]


def get_builtin_strategy_template(key: str) -> BuiltinStrategyTemplate | None:
    # 兼容旧版 UI key：把连字符写法规范成下划线，保证老名称还能解析到
    # 现有模板。
    normalized = str(key or "").strip().replace("-", "_")
    return BUILTIN_STRATEGY_TEMPLATES.get(normalized)

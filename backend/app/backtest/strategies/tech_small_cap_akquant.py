"""科技主线小市值多因子 AKQuant 预设。

这份文件负责把通用多因子模板改造成“科技主线 + 小市值 + 研报因子”
的标准研究版本。它本身不承载交易逻辑，而是定义：
1. 主题行业和关键词范围；
2. 采用哪些研报因子与技术因子；
3. 采用什么节奏调仓；
4. 如何在回测和实盘信号里保持一致。
"""

from __future__ import annotations

import re
from pprint import pformat

from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)
from app.services.us_market import default_us_market_file


# 这个预设保留轻度小市值倾向，但排序主力交给研报类质量、成长、
# 风格轮动和技术信号。生成出来的策略代码会直接出现在回测列表里，
# 所以这里的注释本身也是策略说明的一部分。
TECH_SMALL_CAP_FACTOR_CONFIGS = [
    {
        "name": "market_cap",
        "weight": 0.18,
        "direction": "lower_better",
        "transform": "zscore",
    },
    {
        "name": "market_cap_rank",
        "weight": 0.12,
        "direction": "lower_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_growth_quality_score",
        "weight": 0.16,
        "direction": "higher_better",
        "transform": "zscore",
        "industry_zscore": True,
    },
    {
        "name": "paper_financial_health_score",
        "weight": 0.10,
        "direction": "higher_better",
        "transform": "zscore",
        "industry_zscore": True,
    },
    {
        "name": "paper_defensive_quality_lowvol",
        "weight": 0.12,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
    {
        "name": "paper_industry_momentum_20d",
        "weight": 0.10,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_value_growth_rotation_score",
        "weight": 0.08,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
    {
        "name": "indicator_buy_signal",
        "weight": 0.06,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "v4gv",
        "weight": 0.04,
        "direction": "higher_better",
        "transform": "zscore",
    },
    {
        "name": "paper_composite_value",
        "weight": 0.04,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
]

_TECH_SMALL_CAP_FACTOR_CONFIGS = "FACTOR_CONFIGS = " + pformat(TECH_SMALL_CAP_FACTOR_CONFIGS, sort_dicts=False)


# 主题主线的核心行业：科技和先进制造。显式列出行业有两个好处：
# 一是便于审计，二是后续做版本对比时能一眼看出主题边界有没有变。
_TECH_INCLUDE_INDUSTRIES = [
    "电子",
    "计算机",
    "通信",
    "传媒",
    "国防军工",
    "机械设备",
    "电力设备",
]

# 概念关键词用来捕捉那些无法完整映射到行业字段的主题标签。
# 这是一个“宽松覆盖层”，用于在行业标签不完整时仍尽量贴住科技主线。
_TECH_INCLUDE_KEYWORDS = [
    "半导体",
    "芯片",
    "集成电路",
    "AI",
    "人工智能",
    "算力",
    "服务器",
    "光模块",
    "CPO",
    "通信设备",
    "软件",
    "信息安全",
    "云计算",
    "数据中心",
    "机器人",
    "自动化",
    "智能驾驶",
    "低空经济",
    "卫星",
    "消费电子",
    "PCB",
    "元件",
    "军工电子",
    "储能",
]

# 显式排除传统周期和防御板块，避免科技主线策略在行业标签模糊时
# 不小心滑回旧经济方向。
_OLD_ECONOMY_EXCLUDES = [
    "银行",
    "非银金融",
    "房地产",
    "建筑装饰",
    "建筑材料",
    "钢铁",
    "煤炭",
    "石油石化",
    "交通运输",
    "公用事业",
    "农林牧渔",
    "商贸零售",
    "食品饮料",
    "纺织服饰",
    "轻工制造",
    "社会服务",
    "美容护理",
    "家用电器",
]

# 可交易性过滤与通用模板保持一致，保证导出的预设仍然能独立阅读、
# 独立运行，不依赖隐式外部规则。
TECH_SMALL_CAP_FILTER_FACTORS = [
    {"name": "is_st", "operator": ">=", "value": 0.5},
    {"name": "is_paused", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_up", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_down", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
]

# 这个预设依赖基础小市值核心、研报因子组和风格轮动因子组。
# 也就是说，它不是纯技术面策略，而是“主题 + 基础面 + 技术面”的融合版。
TECH_SMALL_CAP_REQUIRED_FACTOR_GROUPS = [
    "small_cap_v4_core",
    "cn_paper_implemented",
    "cn_paper_style_rotation",
]

TECH_SMALL_CAP_US_MARKET_PATH = str(default_us_market_file())


def _replace_assignment_block(source: str, name: str, replacement: str) -> str:
    pattern = rf"{name} = \[.*?\]\n\n"
    result, count = re.subn(pattern, replacement + "\n\n", source, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"Could not replace {name} block in multi-factor strategy template")
    return result


def _replace_assignment(source: str, name: str, value: object) -> str:
    replacement = f"{name} = {value!r}"
    result, count = re.subn(rf"^{name} = .*$", lambda _: replacement, source, count=1, flags=re.M)
    if count != 1:
        raise RuntimeError(f"Could not replace {name} assignment in multi-factor strategy template")
    return result


def _localize_generated_code(source: str) -> str:
    """把生成出来的策略脚本注释统一成中文，方便前端和数据库保持一致口径。"""
    replacements = [
        (
            "# Generic multi-factor template: keep the execution shell simple, and let\n"
            "# factor definitions plus portfolio/risk parameters drive most behavior.\n",
            "# 通用多因子模板：执行外壳保持简洁，把主要行为交给因子定义、组合参数和风控参数来决定。\n",
        ),
        (
            "# Core size tilt stays dominant in the generic preset, then the other\n"
            "# factors can be swapped or extended without rewriting execution.\n",
            "# 通用预设里市值偏好仍是主骨架，其它因子可以替换或扩展，而不用重写执行流程。\n",
        ),
        (
            "# Hard tradeability filters remove ST, suspended, and limit-locked\n"
            "# names before the strategy spends time scoring them.\n",
            "# 先用硬性可交易过滤把 ST、停牌和涨跌停锁死的标的剔除，再让策略去打分。\n",
        ),
        (
            "# Optional static universe. Leave empty to use the symbols passed by the\n"
            "# platform/AKQuant, or the symbols discovered from bars.\n",
            "# 可选静态股票池：留空时使用平台或 AKQuant 传入的标的，或者使用 bar 自动发现的标的。\n",
        ),
        (
            "# Default sizing/rebalance settings are intentionally conservative so a\n"
            "# newly cloned preset behaves safely before custom tuning.\n",
            "# 默认仓位和调仓参数刻意设得保守，这样新克隆的预设在自定义调整前也能相对稳妥地运行。\n",
        ),
        (
            "# Risk and fees are intentionally controlled by the platform backtest config:\n"
            "# risk_config / max_positions / volume_limit_pct, commission_rate,\n"
            "# stamp_tax_rate, transfer_fee_rate, min_commission, slippage.\n",
            "# 风控和费用统一交给平台回测配置控制：risk_config、max_positions、volume_limit_pct、commission_rate、stamp_tax_rate、transfer_fee_rate、min_commission 和 slippage 都应由外层面板决定，不要在策略里硬编码。\n",
        ),
        (
            "# Parameter hydration keeps the saved strategy asset self-contained:\n"
            "# a database copy can run on its own without preset helper imports.\n",
            "# 参数注入让保存后的策略文件保持自包含：即使只从数据库拷贝一份，也能独立运行，不依赖预设辅助模块。\n",
        ),
        (
            "# Startup wires the factor pipeline and timers only; actual stock\n"
            "# selection waits for bars so the script works on daily/minute_timer feeds.\n",
            "# 启动阶段只初始化因子流水线和定时器；真正的选股等到 bar 到来再做，这样日线和 minute_timer 两种数据源都能工作。\n",
        ),
        (
            "# The bar handler continuously refreshes price state, then delegates\n"
            "# cross-sectional decisions to rebalance and risk helper functions.\n",
            "# bar 处理函数持续刷新价格状态，然后把截面决策交给调仓和风控辅助函数。\n",
        ),
        (
            "# Rebalance builds the active universe for the day, scores it, then\n"
            "# turns the ranked names into target holdings under current risk rules.\n",
            "# 调仓阶段先构建当日可交易股票池，再完成打分排序，最后在当前风控规则下把排名结果转换成目标持仓。\n",
        ),
        (
            "# Execution prefers engine-native target-weight rebalance; the manual\n"
            "# lot-based path exists as a compatibility fallback for lean adapters.\n",
            "# 执行时优先使用引擎原生的目标权重调仓接口；手工按手数计算的路径只是给能力较弱的适配器准备的兼容兜底。\n",
        ),
        (
            "# Portfolio-level drawdown checks run first so a broad de-risk event\n"
            "# can flatten exposure before the code drills into single-name exits.\n",
            "# 组合级回撤检查优先执行，这样一旦需要整体降风险，就能先把整体敞口压平，再进入单票退出逻辑。\n",
        ),
        (
            "# Single-name risk combines hard stop, trailing stop, take-profit, and\n"
            "# abnormal-volume exits using locally tracked cost/peak state.\n",
            "# 单票风控把硬止损、移动止盈、止盈和异常放量退出合在一起，并使用本地维护的成本价和峰值价状态来判断。\n",
        ),
        (
            "# Daily bars do not have intraday timers in some feeds. Once we have a\n"
            "# usable cross-section for the date, run one rebalance for that date.\n",
            "# 某些日线数据源不会显式提供盘中 timer。只要当天的截面已经足够完整，就在 bar 驱动下补做一次当天调仓。\n",
        ),
        (
            "# Tradeability gates are applied before scoring so the selector avoids\n"
            "# obvious non-tradable names at the time of rebalance.\n",
            "# 在打分前先套可交易门槛，这样调仓时就不会把明显不可交易的标的放进候选池。\n",
        ),
        (
            "# The preset still keeps a small-cap tilt, but paper-derived quality and\n"
            "# rotation factors do most of the ranking work on the tech mainline.\n",
            "# 这个预设仍保留轻度小市值倾向，但真正负责排序的是研报派生的质量、成长和轮动因子。\n",
        ),
        (
            "# This preset is intentionally more concentrated and slower-moving than\n"
            "# the generic template because it trades a themed, higher-conviction book.\n",
            "# 这个预设刻意比通用模板更集中、换手更慢，因为它交易的是主题更明确、确定性更高的组合。\n",
        ),
        (
            "# Risk and fees are intentionally controlled by the platform backtest config:\n"
            "# risk_config / max_positions / volume_limit_pct, commission_rate,\n"
            "# stamp_tax_rate, transfer_fee_rate, min_commission, slippage.\n",
            "# 风控和费用统一由平台回测配置控制：risk_config、max_positions、volume_limit_pct、commission_rate、stamp_tax_rate、transfer_fee_rate、min_commission 和 slippage 都应放在外层配置里管理。\n",
        ),
        (
            "# The preset still keeps a small-cap tilt, but paper-derived quality and\n"
            "# rotation factors do most of the ranking work on the tech mainline.\n",
            "# 这个预设仍保留轻度小市值倾向，但排序主力交给研报类质量、成长、风格轮动和技术因子。\n",
        ),
    ]
    for old, new in replacements:
        source = source.replace(old, new)
    return source


TECH_SMALL_CAP_STRATEGY_CODE = _replace_assignment_block(
    MULTI_FACTOR_STRATEGY_CODE,
    "FACTOR_CONFIGS",
    _TECH_SMALL_CAP_FACTOR_CONFIGS,
)
TECH_SMALL_CAP_STRATEGY_CODE = _replace_assignment_block(
    TECH_SMALL_CAP_STRATEGY_CODE,
    "FILTER_FACTORS",
    "FILTER_FACTORS = " + pformat(TECH_SMALL_CAP_FILTER_FACTORS, sort_dicts=False),
)
# 这个预设刻意做得更集中、换手更慢，因为科技主线策略更看重中期确定性，
# 不是通用模板那种“每天都能换仓”的风格。
for _name, _value in {
    "TOP_N": 20,
    "MIN_CANDIDATES": 25,
    "MIN_FACTOR_COVERAGE": 0.55,
    "REBALANCE_EVERY_N_DAYS": 5,
    "REBALANCE_TIME": "10:30",
    "CASH_BUFFER_PCT": 0.08,
    "MAX_POSITION_PCT": 0.06,
    "THEME_INCLUDE_INDUSTRIES": _TECH_INCLUDE_INDUSTRIES,
    "THEME_INCLUDE_KEYWORDS": _TECH_INCLUDE_KEYWORDS,
    "THEME_EXCLUDE_INDUSTRIES": _OLD_ECONOMY_EXCLUDES,
    "THEME_EXCLUDE_KEYWORDS": _OLD_ECONOMY_EXCLUDES,
    "THEME_MIN_CANDIDATES": 20,
    "STRICT_THEME_FILTER": True,
    "STOP_LOSS_PCT": 0.08,
    "TRAILING_STOP_PCT": 0.12,
    "PORTFOLIO_DRAWDOWN_STOP_PCT": 0.15,
    "HIGH_VOLUME_RISK_FACTOR": "high_volume_ratio",
    "HIGH_VOLUME_RISK_AS_OF_TIME": "14:30",
    "HIGH_VOLUME_RISK_MAX": 0.90,
    "HIGH_VOLUME_RISK_PARAMS": {"time": "14:30", "window": 120, "daily_volume_to_share_multiplier": 100.0},
    "RISK_CHECK_TIMES": ["10:00", "14:30"],
    "HOLD_RANK_BUFFER": 0,
    "USE_TARGET_WEIGHT_REBALANCE": True,
    "CANCEL_OPEN_ORDERS_ON_REBALANCE": True,
    "US_OVERNIGHT_ENTRY_FILTER": "none",
    "US_OVERNIGHT_DATA_PATH": TECH_SMALL_CAP_US_MARKET_PATH,
    "US_OVERNIGHT_MAX_LAG_DAYS": 5,
    "US_OVERNIGHT_CAUTION_EXPOSURE": 0.85,
    "US_OVERNIGHT_DEFENSIVE_EXPOSURE": 0.70,
}.items():
    TECH_SMALL_CAP_STRATEGY_CODE = _replace_assignment(TECH_SMALL_CAP_STRATEGY_CODE, _name, _value)

TECH_SMALL_CAP_STRATEGY_CODE = _localize_generated_code(TECH_SMALL_CAP_STRATEGY_CODE)

TECH_SMALL_CAP_STRATEGY_CODE = (
    "# TSMF 预设：科技主线小市值多因子策略。\n"
    "# 组合特点：周频调仓，盘中用 minute_timer 做止损、追踪止损和异常放量风控。\n"
    "# 适用场景：研究侧想要一条主题明确、风控钩子固定、便于和实盘节奏对齐的 A 股科技小市值组合。\n"
    + TECH_SMALL_CAP_STRATEGY_CODE
)


# 让导出预设、生成代码、前端展示和保存配置描述同一套行为，避免用户在
# 列表里看到的是一个版本，实际执行的却是另一个版本。
DEFAULT_TECH_SMALL_CAP_PARAMS = {
    **DEFAULT_MULTI_FACTOR_PARAMS,
    "top_n": 20,
    "min_candidates": 25,
    "min_factor_coverage": 0.55,
    "rebalance_every_n_days": 5,
    "rebalance_time": "10:30",
    "cash_buffer_pct": 0.08,
    "max_position_pct": 0.06,
    "timer_times": ["10:00", "10:30", "14:30"],
    "risk_check_times": ["10:00", "14:30"],
    "stop_loss_pct": 0.08,
    "trailing_stop_pct": 0.12,
    "portfolio_drawdown_stop_pct": 0.15,
    "high_volume_risk_factor": "high_volume_ratio",
    "high_volume_risk_as_of_time": "14:30",
    "high_volume_risk_max": 0.90,
    "high_volume_risk_params": {"time": "14:30", "window": 120, "daily_volume_to_share_multiplier": 100.0},
    "theme_include_industries": _TECH_INCLUDE_INDUSTRIES,
    "theme_include_keywords": _TECH_INCLUDE_KEYWORDS,
    "theme_exclude_industries": _OLD_ECONOMY_EXCLUDES,
    "theme_exclude_keywords": _OLD_ECONOMY_EXCLUDES,
    "theme_min_candidates": 20,
    "strict_theme_filter": True,
    "scorer_type": "linear",
    "hold_rank_buffer": 0,
    "use_target_weight_rebalance": True,
    "cancel_open_orders_on_rebalance": True,
    "us_overnight_entry_filter": "none",
    "us_overnight_data_path": TECH_SMALL_CAP_US_MARKET_PATH,
    "us_overnight_max_lag_days": 5,
    "us_overnight_caution_exposure": 0.85,
    "us_overnight_defensive_exposure": 0.70,
    "us_overnight_qqq_caution_ret": -0.01,
    "us_overnight_qqq_defensive_ret": -0.02,
    "us_overnight_semi_caution_ret": -0.02,
    "us_overnight_semi_defensive_ret": -0.03,
    "us_overnight_nvda_caution_ret": -0.03,
    "us_overnight_nvda_defensive_ret": -0.04,
    "required_factor_groups": TECH_SMALL_CAP_REQUIRED_FACTOR_GROUPS,
    "required_external_data": {
        "us_market_daily": TECH_SMALL_CAP_US_MARKET_PATH,
    },
}


DEFAULT_TECH_SMALL_CAP_RISK_CONFIG = {
    **DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    "max_position_pct": 0.06,
    "max_positions": 20,
}


DEFAULT_TECH_SMALL_CAP_PRODUCTION_VARIANT = "entry_filter_relaxed_risk"

TECH_SMALL_CAP_VARIANTS = {
    "us_entry_filter_combined": {
    "name": "科技小市值 TSMF - 美股入场过滤",
    "description": "美股隔夜风险偏弱时，只限制次日新开仓和加仓，不主动削减既有持仓。",
        "params": {
            "strategy_variant": "us_entry_filter_combined",
            "us_overnight_entry_filter": "combined_downside",
            "stop_loss_pct": 0.08,
            "trailing_stop_pct": 0.12,
            "portfolio_drawdown_stop_pct": 0.15,
            "high_volume_risk_max": 0.90,
        },
        "risk_config": {
            "max_position_pct": 0.06,
            "max_positions": 20,
        },
    },
    "entry_filter_relaxed_risk": {
    "name": "科技小市值 TSMF - 入场过滤放松风控",
    "description": "当前稳健默认版本：美股隔夜入场过滤叠加更宽松的个股止损、移动止盈、组合回撤和放量阈值。",
        "params": {
            "strategy_variant": "entry_filter_relaxed_risk",
            "us_overnight_entry_filter": "combined_downside",
            "stop_loss_pct": 0.10,
            "trailing_stop_pct": 0.16,
            "portfolio_drawdown_stop_pct": 0.20,
            "high_volume_risk_max": 0.95,
        },
        "risk_config": {
            "max_position_pct": 0.06,
            "max_positions": 20,
        },
    },
}


def normalize_tech_small_cap_variant(variant: str | None) -> str:
    normalized = str(variant or DEFAULT_TECH_SMALL_CAP_PRODUCTION_VARIANT).strip().replace("-", "_")
    if normalized in TECH_SMALL_CAP_VARIANTS:
        return normalized
    return DEFAULT_TECH_SMALL_CAP_PRODUCTION_VARIANT


def get_tech_small_cap_variant(variant: str | None = None) -> dict[str, object]:
    key = normalize_tech_small_cap_variant(variant)
    item = TECH_SMALL_CAP_VARIANTS[key]
    return {"key": key, **item}


def get_tech_small_cap_params(variant: str | None = None) -> dict[str, object]:
    item = get_tech_small_cap_variant(variant)
    return {
        **DEFAULT_TECH_SMALL_CAP_PARAMS,
        **dict(item["params"]),
    }


def get_tech_small_cap_risk_config(variant: str | None = None) -> dict[str, object]:
    item = get_tech_small_cap_variant(variant)
    return {
        **DEFAULT_TECH_SMALL_CAP_RISK_CONFIG,
        **dict(item["risk_config"]),
    }

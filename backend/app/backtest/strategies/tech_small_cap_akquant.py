"""Technology-mainline small-cap multi-factor AKQuant preset."""

from __future__ import annotations

import re
from pprint import pformat

from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)
from app.services.us_market import default_us_market_file


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


_TECH_INCLUDE_INDUSTRIES = [
    "电子",
    "计算机",
    "通信",
    "传媒",
    "国防军工",
    "机械设备",
    "电力设备",
]

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

TECH_SMALL_CAP_FILTER_FACTORS = [
    {"name": "is_st", "operator": ">=", "value": 0.5},
    {"name": "is_paused", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_up", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
    {"name": "is_limit_down", "operator": ">=", "value": 0.5, "as_of_time": "10:30", "params": {"time": "10:30"}},
]

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
        "description": "QQQ/SMH/SOXX/NVDA 前一美股交易日负冲击时，仅阻止次日新买和加仓，不主动降低老仓。",
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
        "description": "当前最优候选：美股隔夜入场过滤 + 更宽的个股止损、移动止盈、组合回撤和放量阈值。",
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

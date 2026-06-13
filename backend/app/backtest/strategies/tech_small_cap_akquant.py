"""Technology-mainline small-cap multi-factor AKQuant preset."""

from __future__ import annotations

import re

from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)


_TECH_SMALL_CAP_FACTOR_CONFIGS = '''FACTOR_CONFIGS = [
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
]'''


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


def _replace_assignment_block(source: str, name: str, replacement: str) -> str:
    pattern = rf"{name} = \[.*?\]\n\n"
    result, count = re.subn(pattern, replacement + "\n\n", source, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"Could not replace {name} block in multi-factor strategy template")
    return result


def _replace_assignment(source: str, name: str, value: object) -> str:
    replacement = f"{name} = {value!r}"
    result, count = re.subn(rf"^{name} = .*$", replacement, source, count=1, flags=re.M)
    if count != 1:
        raise RuntimeError(f"Could not replace {name} assignment in multi-factor strategy template")
    return result


TECH_SMALL_CAP_STRATEGY_CODE = _replace_assignment_block(
    MULTI_FACTOR_STRATEGY_CODE,
    "FACTOR_CONFIGS",
    _TECH_SMALL_CAP_FACTOR_CONFIGS,
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
}


DEFAULT_TECH_SMALL_CAP_RISK_CONFIG = {
    **DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    "max_position_pct": 0.06,
    "max_positions": 20,
}


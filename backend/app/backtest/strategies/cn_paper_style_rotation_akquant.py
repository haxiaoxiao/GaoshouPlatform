"""AKQuant strategy presets for domestic paper style-rotation factors."""

from __future__ import annotations

import re

from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)


_STYLE_ROTATION_FACTOR_CONFIGS = '''FACTOR_CONFIGS = [
    {
        "name": "paper_size_rotation_score",
        "weight": 0.24,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_value_growth_rotation_score",
        "weight": 0.24,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
    {
        "name": "paper_industry_momentum_20d",
        "weight": 0.22,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_reversal_20d",
        "weight": 0.12,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_growth_quality_score",
        "weight": 0.10,
        "direction": "higher_better",
        "transform": "zscore",
        "industry_zscore": True,
    },
    {
        "name": "paper_composite_value",
        "weight": 0.08,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
]'''


_DEFENSIVE_FACTOR_CONFIGS = '''FACTOR_CONFIGS = [
    {
        "name": "paper_defensive_quality_lowvol",
        "weight": 0.36,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
    {
        "name": "paper_asset_allocation_proxy",
        "weight": 0.28,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_financial_health_score",
        "weight": 0.20,
        "direction": "higher_better",
        "transform": "zscore",
        "industry_zscore": True,
    },
    {
        "name": "paper_composite_value",
        "weight": 0.16,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
]'''


def _replace_assignment_block(source: str, name: str, replacement: str) -> str:
    pattern = rf"{name} = \[.*?\]\n\n"
    result, count = re.subn(pattern, replacement + "\n\n", source, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"Could not replace {name} block in multi-factor strategy template")
    return result


def _preset_code(factor_configs: str, *, top_n: int, rebalance_days: int, max_position_pct: float) -> str:
    code = _replace_assignment_block(MULTI_FACTOR_STRATEGY_CODE, "FACTOR_CONFIGS", factor_configs)
    code = code.replace("TOP_N = 10", f"TOP_N = {top_n}")
    code = code.replace("MIN_CANDIDATES = 5", "MIN_CANDIDATES = 20")
    code = code.replace("REBALANCE_EVERY_N_DAYS = 1", f"REBALANCE_EVERY_N_DAYS = {rebalance_days}")
    code = code.replace("MAX_POSITION_PCT = 0.12", f"MAX_POSITION_PCT = {max_position_pct}")
    return code


CN_PAPER_STYLE_ROTATION_STRATEGY_CODE = _preset_code(
    _STYLE_ROTATION_FACTOR_CONFIGS,
    top_n=40,
    rebalance_days=20,
    max_position_pct=0.06,
)

CN_PAPER_DEFENSIVE_ALLOCATION_STRATEGY_CODE = _preset_code(
    _DEFENSIVE_FACTOR_CONFIGS,
    top_n=25,
    rebalance_days=20,
    max_position_pct=0.08,
)


DEFAULT_CN_PAPER_STYLE_ROTATION_PARAMS = {
    **DEFAULT_MULTI_FACTOR_PARAMS,
    "top_n": 40,
    "min_candidates": 20,
    "min_factor_coverage": 0.55,
    "rebalance_every_n_days": 20,
    "max_position_pct": 0.06,
    "scorer_type": "linear",
}


DEFAULT_CN_PAPER_STYLE_ROTATION_RISK_CONFIG = {
    **DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    "max_position_pct": 0.06,
}


DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_PARAMS = {
    **DEFAULT_MULTI_FACTOR_PARAMS,
    "top_n": 25,
    "min_candidates": 20,
    "min_factor_coverage": 0.50,
    "rebalance_every_n_days": 20,
    "max_position_pct": 0.08,
    "scorer_type": "linear",
}


DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_RISK_CONFIG = {
    **DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    "max_position_pct": 0.08,
}

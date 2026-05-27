"""AKQuant strategy presets for implemented domestic paper factors."""

from __future__ import annotations

import re

from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)


_CN_PAPER_FACTOR_CONFIGS = '''FACTOR_CONFIGS = [
    {
        "name": "paper_composite_value",
        "weight": 0.18,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
    {
        "name": "paper_pb_roe_residual",
        "weight": 0.14,
        "direction": "higher_better",
        "transform": "zscore",
        "industry_zscore": True,
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
        "weight": 0.14,
        "direction": "higher_better",
        "transform": "zscore",
        "industry_zscore": True,
    },
    {
        "name": "paper_overnight_turnover_corr",
        "weight": 0.08,
        "direction": "lower_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_rsi_reversal_score",
        "weight": 0.08,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_high_low_volume_event",
        "weight": 0.08,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_new_high_anchor",
        "weight": 0.06,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
    {
        "name": "paper_reversal_20d",
        "weight": 0.08,
        "direction": "higher_better",
        "transform": "rank_pct",
    },
]'''


def _replace_assignment_block(source: str, name: str, replacement: str) -> str:
    pattern = rf"{name} = \[.*?\]\n\n"
    result, count = re.subn(pattern, replacement + "\n\n", source, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"Could not replace {name} block in multi-factor strategy template")
    return result


CN_PAPER_FACTOR_STRATEGY_CODE = _replace_assignment_block(
    MULTI_FACTOR_STRATEGY_CODE,
    "FACTOR_CONFIGS",
    _CN_PAPER_FACTOR_CONFIGS,
)
CN_PAPER_FACTOR_STRATEGY_CODE = CN_PAPER_FACTOR_STRATEGY_CODE.replace("TOP_N = 10", "TOP_N = 30")
CN_PAPER_FACTOR_STRATEGY_CODE = CN_PAPER_FACTOR_STRATEGY_CODE.replace("MIN_CANDIDATES = 5", "MIN_CANDIDATES = 20")
CN_PAPER_FACTOR_STRATEGY_CODE = CN_PAPER_FACTOR_STRATEGY_CODE.replace("REBALANCE_EVERY_N_DAYS = 1", "REBALANCE_EVERY_N_DAYS = 20")


DEFAULT_CN_PAPER_FACTOR_PARAMS = {
    **DEFAULT_MULTI_FACTOR_PARAMS,
    "top_n": 30,
    "min_candidates": 20,
    "min_factor_coverage": 0.55,
    "rebalance_every_n_days": 20,
    "scorer_type": "linear",
}


DEFAULT_CN_PAPER_FACTOR_RISK_CONFIG = {
    **DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    "max_position_pct": 0.08,
}

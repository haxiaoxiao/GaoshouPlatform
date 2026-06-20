"""国内已落地研报因子组合的 AKQuant 预设。

这类预设不重新发明策略框架，而是复用通用多因子执行模板，把已经在研究
和预计算侧验证过的研报因子放进同一套长仓调仓骨架里，形成可回测、可复现
的标准组合。
"""

from __future__ import annotations

import re

from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)

_CN_PAPER_FACTOR_CONFIGS = '''FACTOR_CONFIGS = [
    # 核心价值锚：把综合估值信号放在最显眼的位置，确保这个预设一眼看上去
    # 仍然是“研报因子组合”，而不是被技术信号带偏的杂糅版本。
    {
        "name": "paper_composite_value",
        "weight": 0.18,
        "direction": "higher_better",
        "transform": "rank_pct",
        "industry_zscore": True,
    },
    # 残差质量项：用 PB 和 ROE 的关系搭一座更干净的质量-价值桥梁，
    # 比只看单纯估值或单纯质量更容易保留组合解释性。
    {
        "name": "paper_pb_roe_residual",
        "weight": 0.14,
        "direction": "higher_better",
        "transform": "zscore",
        "industry_zscore": True,
    },
    # 成长与资产负债表强度放在中间权重，目的不是追最便宜，而是奖励更
    # 可持续的基本面质量。
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
    # 时序/节奏型覆盖层权重更小，只负责修剪拥挤和短线过热，不喧宾夺主。
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
CN_PAPER_FACTOR_STRATEGY_CODE = (
    "# 中文研报因子组合预设：候选池更宽、调仓更慢，但仍沿用通用多因子\n"
    "# 的执行骨架，方便和其它版本直接对比。\n"
    + CN_PAPER_FACTOR_STRATEGY_CODE
)


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

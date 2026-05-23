from __future__ import annotations

import akquant as aq

from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)


def test_multi_factor_strategy_code_shape():
    assert "class MultiFactorStrategy" in MULTI_FACTOR_STRATEGY_CODE
    assert "aq.Strategy" in MULTI_FACTOR_STRATEGY_CODE
    assert "FactorPipeline" in MULTI_FACTOR_STRATEGY_CODE
    assert "stamp_tax_rate" in MULTI_FACTOR_STRATEGY_CODE
    assert DEFAULT_MULTI_FACTOR_PARAMS["scorer_type"] == "linear"
    assert DEFAULT_MULTI_FACTOR_RISK_CONFIG["max_position_pct"] > 0


def test_multi_factor_strategy_code_executes():
    namespace = {"aq": aq}
    exec(MULTI_FACTOR_STRATEGY_CODE, namespace)
    strategy_cls = namespace["MultiFactorStrategy"]

    assert issubclass(strategy_cls, aq.Strategy)
    strategy = strategy_cls()
    assert strategy.top_n > 0
    assert strategy.scorer_type == "linear"
    assert callable(getattr(strategy, "on_bar"))
    assert callable(getattr(strategy, "on_timer"))


from __future__ import annotations

import akquant as aq

from app.backtest.strategies.builtin_templates import get_builtin_strategy_template
from app.backtest.strategies.cn_paper_factor_akquant import (
    CN_PAPER_FACTOR_STRATEGY_CODE,
    DEFAULT_CN_PAPER_FACTOR_PARAMS,
)
from app.backtest.strategies.cn_paper_style_rotation_akquant import (
    CN_PAPER_DEFENSIVE_ALLOCATION_STRATEGY_CODE,
    CN_PAPER_STYLE_ROTATION_STRATEGY_CODE,
    DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_PARAMS,
    DEFAULT_CN_PAPER_STYLE_ROTATION_PARAMS,
)
from app.backtest.strategies.multi_factor_akquant import (
    DEFAULT_MULTI_FACTOR_PARAMS,
    DEFAULT_MULTI_FACTOR_RISK_CONFIG,
    MULTI_FACTOR_STRATEGY_CODE,
)
from app.backtest.strategies.tech_small_cap_akquant import (
    DEFAULT_TECH_SMALL_CAP_PARAMS,
    TECH_SMALL_CAP_STRATEGY_CODE,
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
    assert callable(strategy.on_bar)
    assert callable(strategy.on_timer)


def test_cn_paper_factor_strategy_preset_executes():
    assert "paper_composite_value" in CN_PAPER_FACTOR_STRATEGY_CODE
    assert "paper_pb_roe_residual" in CN_PAPER_FACTOR_STRATEGY_CODE
    assert DEFAULT_CN_PAPER_FACTOR_PARAMS["rebalance_every_n_days"] == 20

    namespace = {"aq": aq}
    exec(CN_PAPER_FACTOR_STRATEGY_CODE, namespace)
    strategy_cls = namespace["MultiFactorStrategy"]
    strategy = strategy_cls()

    assert issubclass(strategy_cls, aq.Strategy)
    assert strategy.top_n == 30
    assert strategy.rebalance_every_n_days == 20


def test_cn_paper_factor_builtin_template_is_registered():
    template = get_builtin_strategy_template("cn_paper_factor")
    assert template is not None
    payload = template.to_preset_dict()
    assert payload["strategy_key"] == "cn_paper_factor"
    assert payload["strategy_params"]["required_factor_group"] == "cn_paper_implemented"


def test_cn_paper_style_rotation_presets_execute():
    assert "paper_size_rotation_score" in CN_PAPER_STYLE_ROTATION_STRATEGY_CODE
    assert "paper_defensive_quality_lowvol" in CN_PAPER_DEFENSIVE_ALLOCATION_STRATEGY_CODE
    assert DEFAULT_CN_PAPER_STYLE_ROTATION_PARAMS["top_n"] == 40
    assert DEFAULT_CN_PAPER_DEFENSIVE_ALLOCATION_PARAMS["top_n"] == 25

    for code, top_n in [
        (CN_PAPER_STYLE_ROTATION_STRATEGY_CODE, 40),
        (CN_PAPER_DEFENSIVE_ALLOCATION_STRATEGY_CODE, 25),
    ]:
        namespace = {"aq": aq}
        exec(code, namespace)
        strategy_cls = namespace["MultiFactorStrategy"]
        strategy = strategy_cls()
        assert issubclass(strategy_cls, aq.Strategy)
        assert strategy.top_n == top_n
        assert strategy.rebalance_every_n_days == 20


def test_cn_paper_style_rotation_builtin_templates_are_registered():
    style_template = get_builtin_strategy_template("cn_paper_style_rotation")
    defensive_template = get_builtin_strategy_template("cn_paper_defensive_allocation")
    assert style_template is not None
    assert defensive_template is not None
    assert style_template.to_preset_dict()["strategy_params"]["required_factor_group"] == "cn_paper_style_rotation"
    assert defensive_template.to_preset_dict()["strategy_params"]["required_factor_group"] == "cn_paper_style_rotation"


def test_tech_small_cap_preset_executes():
    assert "paper_growth_quality_score" in TECH_SMALL_CAP_STRATEGY_CODE
    assert "high_volume_ratio" in TECH_SMALL_CAP_STRATEGY_CODE
    assert DEFAULT_TECH_SMALL_CAP_PARAMS["rebalance_every_n_days"] == 5
    assert DEFAULT_TECH_SMALL_CAP_PARAMS["timer_times"] == ["10:00", "10:30", "14:30"]

    namespace = {"aq": aq}
    exec(TECH_SMALL_CAP_STRATEGY_CODE, namespace)
    strategy_cls = namespace["MultiFactorStrategy"]
    strategy = strategy_cls()

    assert issubclass(strategy_cls, aq.Strategy)
    assert strategy.top_n == 20
    assert strategy.rebalance_every_n_days == 5
    assert strategy.stop_loss_pct == 0.08
    assert strategy.high_volume_risk_factor == "high_volume_ratio"
    assert strategy.strict_theme_filter is True


def test_tech_small_cap_builtin_template_is_registered():
    template = get_builtin_strategy_template("tech_small_cap")
    assert template is not None
    payload = template.to_preset_dict()
    assert payload["strategy_key"] == "tech_small_cap"
    assert payload["bar_type"] == "minute_timer"
    assert payload["strategy_params"]["index_symbol"] == "399101.SZ"
    assert "small_cap_v4_core" in payload["strategy_params"]["required_factor_groups"]

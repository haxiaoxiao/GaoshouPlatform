"""Pure market-radar calculation tests."""

from __future__ import annotations

import gc
from datetime import datetime, timedelta
from weakref import ref

import pytest
from fastapi.encoders import jsonable_encoder

from app.services import market_radar_calculator as calculator
from app.services.market_radar_calculator import (
    CROWDING_COMPONENT_WEIGHTS,
    EMOTION_COMPONENT_WEIGHTS,
    BreadthBucket,
    BreadthResult,
    CompositeScore,
    Coverage,
    QuoteTick,
    ScoreComponent,
    calculate_breadth,
    composite_score,
    crowding_label,
    emotion_label,
    robust_percentile_rank,
)

NOW = datetime(2026, 7, 18, 10, 0, 0)


def _tick(
    symbol: str,
    return_percentage: float,
    *,
    age_seconds: float = 0,
    stock_status: int | None = 0,
) -> QuoteTick:
    return QuoteTick(
        symbol=symbol,
        quote_time=NOW - timedelta(seconds=age_seconds),
        last_price=100.0 + return_percentage,
        previous_close=100.0,
        stock_status=stock_status,
    )


@pytest.mark.parametrize(
    ("return_percentage", "expected_bucket"),
    [
        (-8.001, "le_neg_8"),
        (-8.0, "le_neg_8"),
        (-7.999, "neg_8_to_neg_6"),
        (-6.0, "neg_8_to_neg_6"),
        (-5.999, "neg_6_to_neg_4"),
        (-4.0, "neg_6_to_neg_4"),
        (-3.999, "neg_4_to_neg_2"),
        (-2.0, "neg_4_to_neg_2"),
        (-1.999, "neg_2_to_0"),
        (-0.001, "neg_2_to_0"),
        (0.001, "pos_0_to_2"),
        (1.999, "pos_0_to_2"),
        (2.0, "pos_2_to_4"),
        (3.999, "pos_2_to_4"),
        (4.0, "pos_4_to_6"),
        (5.999, "pos_4_to_6"),
        (6.0, "pos_6_to_8"),
        (7.999, "pos_6_to_8"),
        (8.0, "ge_pos_8"),
        (8.001, "ge_pos_8"),
    ],
)
def test_calculate_breadth_uses_exact_percentage_point_boundaries(
    return_percentage: float,
    expected_bucket: str,
) -> None:
    symbol = "000001.SZ"

    result = calculate_breadth([_tick(symbol, return_percentage)], [symbol], now=NOW)

    assert result.status == "fresh"
    assert result.buckets[expected_bucket].count == 1
    assert sum(bucket.count for bucket in result.buckets.values()) == 1
    assert sum(bucket.percentage for bucket in result.buckets.values()) == pytest.approx(100.0)
    assert result.flat_count == 0


def test_calculate_breadth_keeps_flat_quotes_separate() -> None:
    symbol = "600000.SH"

    result = calculate_breadth([_tick(symbol, 0.0)], [symbol], now=NOW)

    assert result.flat_count == 1
    assert result.buckets["pos_0_to_2"].count == 1
    assert sum(bucket.count for bucket in result.buckets.values()) == result.coverage.valid
    assert sum(bucket.percentage for bucket in result.buckets.values()) == pytest.approx(100.0)


def test_calculate_breadth_exposes_an_immutable_bucket_mapping() -> None:
    symbol = "600000.SH"
    result = calculate_breadth([_tick(symbol, 1.0)], [symbol], now=NOW)

    with pytest.raises(TypeError):
        result.buckets["pos_0_to_2"] = result.buckets["pos_0_to_2"]  # type: ignore[index]


def test_serialize_breadth_result_returns_a_fastapi_json_payload() -> None:
    symbol = "600000.SH"
    result = calculate_breadth([_tick(symbol, 0.0)], [symbol], now=NOW)

    payload = calculator.serialize_breadth_result(result)
    encoded = jsonable_encoder(payload)

    assert encoded == payload
    assert encoded["status"] == "fresh"
    assert encoded["flat_count"] == 1
    assert encoded["coverage"] == {
        "requested": 1,
        "eligible": 1,
        "valid": 1,
        "excluded": 0,
        "coverage": 1.0,
        "status": "fresh",
        "missing": 0,
        "stale": 0,
        "invalid": 0,
        "suspended": 0,
    }
    assert encoded["buckets"]["pos_0_to_2"]["count"] == 1


def test_calculate_breadth_accepts_documented_positional_options() -> None:
    symbol = "600000.SH"

    result = calculate_breadth([_tick(symbol, 1.0)], [symbol], NOW, 5, 0.8)

    assert result.status == "fresh"


@pytest.mark.parametrize(
    "max_age_seconds",
    [float("nan"), float("inf"), float("-inf")],
)
def test_calculate_breadth_rejects_non_finite_max_age(max_age_seconds: float) -> None:
    with pytest.raises(ValueError, match="finite and non-negative"):
        calculate_breadth([], [], NOW, max_age_seconds)


def test_calculate_breadth_excludes_suspended_stale_invalid_and_missing_quotes() -> None:
    valid_symbols = [f"VALID{index}" for index in range(8)]
    eligible_symbols = [
        *valid_symbols,
        "SUSPENDED",
        "STALE",
        "INVALID",
        "MISSING",
    ]
    ticks = [_tick(symbol, 1.0) for symbol in valid_symbols]
    ticks.extend(
        [
            _tick("SUSPENDED", 1.0, stock_status=1),
            _tick("STALE", 1.0, age_seconds=6),
            QuoteTick(
                symbol="INVALID",
                quote_time=NOW,
                last_price=float("nan"),
                previous_close=100.0,
            ),
        ]
    )

    result = calculate_breadth(ticks, eligible_symbols, now=NOW, max_age_seconds=5)

    assert result.status == "partial"
    assert result.coverage.requested == 12
    assert result.coverage.eligible == 11
    assert result.coverage.valid == 8
    assert result.coverage.excluded == 4
    assert result.coverage.suspended == 1
    assert result.coverage.stale == 1
    assert result.coverage.invalid == 1
    assert result.coverage.missing == 1
    assert result.coverage.coverage == pytest.approx(8 / 11)
    assert result.buckets["pos_0_to_2"].count == 8
    assert result.buckets["pos_0_to_2"].percentage == pytest.approx(100.0)


def test_calculate_breadth_accepts_exactly_eighty_percent_coverage() -> None:
    eligible_symbols = ["A", "B", "C", "D", "E"]
    ticks = [_tick(symbol, 1.0) for symbol in eligible_symbols[:4]]
    ticks.append(
        QuoteTick(
            symbol="E",
            quote_time=NOW,
            last_price=100.0,
            previous_close=0.0,
        )
    )

    result = calculate_breadth(ticks, eligible_symbols, now=NOW)

    assert result.coverage.coverage == pytest.approx(0.8)
    assert result.status == "fresh"


def test_suspended_quotes_do_not_reduce_coverage_denominator() -> None:
    eligible_symbols = ["A", "B", "C", "D", "SUSPENDED"]
    ticks = [_tick(symbol, 1.0) for symbol in eligible_symbols[:4]]
    ticks.append(_tick("SUSPENDED", 1.0, stock_status=1))

    result = calculate_breadth(ticks, eligible_symbols, now=NOW)

    assert result.coverage.requested == 5
    assert result.coverage.eligible == 4
    assert result.coverage.valid == 4
    assert result.coverage.coverage == pytest.approx(1.0)
    assert result.status == "fresh"


def test_qmt_normal_stock_status_three_is_eligible_and_valid() -> None:
    symbol = "600000.SH"

    result = calculate_breadth(
        [_tick(symbol, 1.0, stock_status=3)],
        [symbol],
        now=NOW,
    )

    assert result.coverage.requested == 1
    assert result.coverage.eligible == 1
    assert result.coverage.valid == 1
    assert result.coverage.suspended == 0
    assert result.buckets["pos_0_to_2"].count == 1
    assert result.status == "fresh"


@pytest.mark.parametrize(
    "contract",
    [QuoteTick, Coverage, BreadthBucket, BreadthResult, ScoreComponent, CompositeScore],
)
def test_calculation_contracts_are_frozen_dataclasses(contract: type[object]) -> None:
    assert contract.__dataclass_params__.frozen is True


def test_formula_component_weights_are_fixed() -> None:
    assert dict(CROWDING_COMPONENT_WEIGHTS) == {
        "top_1_amount_share": 0.25,
        "top_5_amount_share": 0.20,
        "top_3_sector_share": 0.15,
        "market_amount_vs_20d": 0.15,
        "high_liquidity_correlation": 0.15,
        "margin_balance_5d_change": 0.10,
    }
    assert dict(EMOTION_COMPONENT_WEIGHTS) == {
        "market_breadth": 0.30,
        "limit_ladder": 0.25,
        "liquidity_risk_appetite": 0.20,
        "sentiment": 0.25,
    }
    assert sum(CROWDING_COMPONENT_WEIGHTS.values()) == pytest.approx(1.0)
    assert sum(EMOTION_COMPONENT_WEIGHTS.values()) == pytest.approx(1.0)


def test_robust_percentile_rank_uses_deterministic_midrank_for_ties() -> None:
    assert robust_percentile_rank(2.0, [1.0, 2.0, 2.0, 3.0]) == pytest.approx(50.0)


def test_robust_percentile_rank_uses_only_latest_120_observations() -> None:
    history = [10_000.0, *(float(value) for value in range(120))]

    result = robust_percentile_rank(119.0, history)

    assert result == pytest.approx((119.5 / 120.0) * 100.0)


def test_robust_percentile_rank_bounds_generator_retention_to_lookback() -> None:
    class TrackedNumber:
        __slots__ = ("value", "__weakref__")

        def __init__(self, value: float) -> None:
            self.value = value

        def __float__(self) -> float:
            return self.value

    oldest_reference = None

    def history():
        nonlocal oldest_reference
        oldest = TrackedNumber(10_000.0)
        oldest_reference = ref(oldest)
        yield oldest
        del oldest
        for value in range(121):
            if value == 120:
                gc.collect()
                assert oldest_reference() is None
            yield TrackedNumber(float(value))

    result = robust_percentile_rank(120.0, history())

    assert result == pytest.approx((119.5 / 120.0) * 100.0)


def test_robust_percentile_rank_reverses_cold_direction() -> None:
    hot = robust_percentile_rank(3.0, [0.0, 1.0, 2.0, 3.0])
    cold = robust_percentile_rank(
        3.0,
        [0.0, 1.0, 2.0, 3.0],
        higher_is_hotter=False,
    )

    assert hot == pytest.approx(87.5)
    assert cold == pytest.approx(12.5)
    assert hot + cold == pytest.approx(100.0)


def _emotion_components(
    *,
    market_breadth: float | None,
    limit_ladder: float | None,
    liquidity_risk_appetite: float | None,
    sentiment: float | None,
) -> list[ScoreComponent]:
    values = {
        "market_breadth": market_breadth,
        "limit_ladder": limit_ladder,
        "liquidity_risk_appetite": liquidity_risk_appetite,
        "sentiment": sentiment,
    }
    history = tuple(float(value) for value in range(120))
    return [
        ScoreComponent(
            name=name,
            raw_value=values[name],
            history=history,
            weight=weight,
        )
        for name, weight in EMOTION_COMPONENT_WEIGHTS.items()
    ]


def test_composite_score_reweights_missing_components_and_explains_contributions() -> None:
    components = _emotion_components(
        market_breadth=90.0,
        limit_ladder=None,
        liquidity_risk_appetite=60.0,
        sentiment=30.0,
    )

    score = composite_score(components)
    by_name = {component.name: component for component in score.components}

    assert score.status == "fresh"
    assert score.effective_weight == pytest.approx(0.75)
    assert score.value is not None
    assert sum(component.contribution for component in score.components) == pytest.approx(
        score.value
    )
    assert by_name["market_breadth"].normalized == pytest.approx((90.5 / 120) * 100)
    assert by_name["market_breadth"].weight == pytest.approx(0.30)
    assert by_name["market_breadth"].effective_weight == pytest.approx(0.30 / 0.75)
    assert by_name["limit_ladder"].normalized is None
    assert by_name["limit_ladder"].effective_weight == 0.0
    assert by_name["limit_ladder"].contribution == 0.0
    assert by_name["limit_ladder"].excluded_reason == "missing_value"


def test_crowding_composite_reweights_a_missing_component() -> None:
    values = {
        "top_1_amount_share": 90.0,
        "top_5_amount_share": 80.0,
        "top_3_sector_share": 70.0,
        "market_amount_vs_20d": 60.0,
        "high_liquidity_correlation": 50.0,
        "margin_balance_5d_change": None,
    }
    history = tuple(float(value) for value in range(120))
    components = [
        ScoreComponent(
            name=name,
            raw_value=values[name],
            history=history,
            weight=weight,
        )
        for name, weight in CROWDING_COMPONENT_WEIGHTS.items()
    ]

    score = composite_score(components)
    by_name = {component.name: component for component in score.components}

    assert score.status == "fresh"
    assert score.effective_weight == pytest.approx(0.90)
    assert score.value is not None
    assert by_name["top_1_amount_share"].effective_weight == pytest.approx(0.25 / 0.90)
    assert by_name["margin_balance_5d_change"].excluded_reason == "missing_value"
    assert by_name["margin_balance_5d_change"].effective_weight == 0.0
    assert by_name["margin_balance_5d_change"].contribution == 0.0
    assert sum(component.contribution for component in score.components) == pytest.approx(
        score.value
    )


def test_composite_score_accepts_exactly_seventy_percent_effective_weight() -> None:
    components = _emotion_components(
        market_breadth=None,
        limit_ladder=80.0,
        liquidity_risk_appetite=70.0,
        sentiment=60.0,
    )

    score = composite_score(components, minimum_weight=0.70)

    assert score.status == "fresh"
    assert score.effective_weight == pytest.approx(0.70)
    assert score.value is not None


def test_composite_score_suppresses_value_below_seventy_percent_effective_weight() -> None:
    components = _emotion_components(
        market_breadth=80.0,
        limit_ladder=70.0,
        liquidity_risk_appetite=None,
        sentiment=None,
    )

    score = composite_score(components, minimum_weight=0.70)

    assert score.status == "insufficient"
    assert score.effective_weight == pytest.approx(0.55)
    assert score.value is None


def test_composite_score_does_not_allow_the_minimum_below_seventy_percent() -> None:
    components = _emotion_components(
        market_breadth=80.0,
        limit_ladder=70.0,
        liquidity_risk_appetite=None,
        sentiment=None,
    )

    score = composite_score(components, minimum_weight=0.50)

    assert score.minimum_weight == pytest.approx(0.70)
    assert score.status == "insufficient"
    assert score.value is None


def test_composite_score_rejects_weights_that_do_not_match_the_fixed_formula() -> None:
    components = _emotion_components(
        market_breadth=80.0,
        limit_ladder=70.0,
        liquidity_risk_appetite=60.0,
        sentiment=50.0,
    )
    components[0] = ScoreComponent(
        name="market_breadth",
        raw_value=80.0,
        history=components[0].history,
        weight=0.35,
    )
    components[1] = ScoreComponent(
        name="limit_ladder",
        raw_value=70.0,
        history=components[1].history,
        weight=0.20,
    )

    with pytest.raises(ValueError, match="fixed formula"):
        composite_score(components)


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (0.0, "绝望"),
        (14.999, "绝望"),
        (15.0, "恐慌"),
        (29.999, "恐慌"),
        (30.0, "悲观"),
        (44.999, "悲观"),
        (45.0, "中性"),
        (54.999, "中性"),
        (55.0, "乐观"),
        (69.999, "乐观"),
        (70.0, "狂热"),
        (84.999, "狂热"),
        (85.0, "极端狂热"),
        (100.0, "极端狂热"),
    ],
)
def test_emotion_label_boundaries(score: float, expected: str) -> None:
    assert emotion_label(score) == expected


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (0.0, "宽松"),
        (29.999, "宽松"),
        (30.0, "正常"),
        (54.999, "正常"),
        (55.0, "拥挤"),
        (74.999, "拥挤"),
        (75.0, "高拥挤"),
        (89.999, "高拥挤"),
        (90.0, "极端拥挤"),
        (100.0, "极端拥挤"),
    ],
)
def test_crowding_label_boundaries(score: float, expected: str) -> None:
    assert crowding_label(score) == expected

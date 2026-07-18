"""Pure calculations and immutable contracts for the market radar."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from math import isfinite
from types import MappingProxyType
from typing import Literal

BreadthStatus = Literal["fresh", "partial", "unavailable"]
ScoreStatus = Literal["fresh", "insufficient"]
_MINIMUM_COMPOSITE_WEIGHT = 0.70

CROWDING_COMPONENT_WEIGHTS: Mapping[str, float] = MappingProxyType(
    {
        "top_1_amount_share": 0.25,
        "top_5_amount_share": 0.20,
        "top_3_sector_share": 0.15,
        "market_amount_vs_20d": 0.15,
        "high_liquidity_correlation": 0.15,
        "margin_balance_5d_change": 0.10,
    }
)
EMOTION_COMPONENT_WEIGHTS: Mapping[str, float] = MappingProxyType(
    {
        "market_breadth": 0.30,
        "limit_ladder": 0.25,
        "liquidity_risk_appetite": 0.20,
        "sentiment": 0.25,
    }
)


@dataclass(frozen=True, slots=True)
class QuoteTick:
    symbol: str
    quote_time: datetime
    last_price: float
    previous_close: float
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    volume: float | None = None
    amount: float | None = None
    stock_status: int | None = None
    speed_1m: float | None = None
    speed_5m: float | None = None


@dataclass(frozen=True, slots=True)
class Coverage:
    requested: int
    eligible: int
    valid: int
    excluded: int
    coverage: float
    status: BreadthStatus
    missing: int
    stale: int
    invalid: int
    suspended: int

    @property
    def ratio(self) -> float:
        """Compatibility alias for consumers that call the coverage value a ratio."""
        return self.coverage


@dataclass(frozen=True, slots=True)
class BreadthBucket:
    key: str
    label: str
    lower_bound: float | None
    upper_bound: float | None
    lower_inclusive: bool
    upper_inclusive: bool
    count: int
    percentage: float


@dataclass(frozen=True, slots=True)
class BreadthResult:
    buckets: Mapping[str, BreadthBucket]
    flat_count: int
    coverage: Coverage
    status: BreadthStatus

    def __post_init__(self) -> None:
        object.__setattr__(self, "buckets", MappingProxyType(dict(self.buckets)))


@dataclass(frozen=True, slots=True)
class ScoreComponent:
    name: str
    raw_value: float | None
    history: tuple[float, ...]
    weight: float
    higher_is_hotter: bool = True
    normalized: float | None = None
    effective_weight: float = 0.0
    contribution: float = 0.0
    excluded_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "history", tuple(self.history))


@dataclass(frozen=True, slots=True)
class CompositeScore:
    value: float | None
    status: ScoreStatus
    effective_weight: float
    minimum_weight: float
    components: tuple[ScoreComponent, ...]
    formula_version: str = "market-radar-v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "components", tuple(self.components))


_BUCKET_SPECS = (
    ("le_neg_8", "<=-8%", None, -8.0, False, True),
    ("neg_8_to_neg_6", "(-8%,-6%]", -8.0, -6.0, False, True),
    ("neg_6_to_neg_4", "(-6%,-4%]", -6.0, -4.0, False, True),
    ("neg_4_to_neg_2", "(-4%,-2%]", -4.0, -2.0, False, True),
    ("neg_2_to_0", "(-2%,0%)", -2.0, 0.0, False, False),
    ("pos_0_to_2", "[0%,2%)", 0.0, 2.0, True, False),
    ("pos_2_to_4", "[2%,4%)", 2.0, 4.0, True, False),
    ("pos_4_to_6", "[4%,6%)", 4.0, 6.0, True, False),
    ("pos_6_to_8", "[6%,8%)", 6.0, 8.0, True, False),
    ("ge_pos_8", ">=8%", 8.0, None, True, False),
)


def calculate_breadth(
    ticks: Mapping[str, QuoteTick] | Iterable[QuoteTick],
    eligible_symbols: Iterable[str],
    now: datetime,
    max_age_seconds: float = 5,
    minimum_coverage: float = 0.8,
) -> BreadthResult:
    """Calculate a fresh-tick market breadth distribution in percentage points."""
    normalized_max_age = _finite_float(max_age_seconds)
    if normalized_max_age is None or normalized_max_age < 0:
        raise ValueError("max_age_seconds must be finite and non-negative")
    if not 0 <= minimum_coverage <= 1:
        raise ValueError("minimum_coverage must be between 0 and 1")

    universe = tuple(dict.fromkeys(eligible_symbols))
    tick_values = ticks.values() if isinstance(ticks, Mapping) else ticks
    latest_by_symbol = {tick.symbol: tick for tick in tick_values}
    counts = {spec[0]: 0 for spec in _BUCKET_SPECS}
    flat_count = 0
    missing = 0
    stale = 0
    invalid = 0
    suspended = 0
    valid = 0

    for symbol in universe:
        tick = latest_by_symbol.get(symbol)
        if tick is None:
            missing += 1
            continue
        if tick.stock_status == 1:
            suspended += 1
            continue

        age_seconds = _age_seconds(now, tick.quote_time)
        if age_seconds is None or age_seconds < 0:
            invalid += 1
            continue
        if age_seconds > normalized_max_age:
            stale += 1
            continue
        if not _is_valid_price(tick.last_price) or not _is_valid_price(tick.previous_close):
            invalid += 1
            continue

        return_percentage = _return_percentage(tick.last_price, tick.previous_close)
        if return_percentage == 0:
            flat_count += 1
        counts[_bucket_key(return_percentage)] += 1
        valid += 1

    requested = len(universe)
    eligible = requested - suspended
    excluded = missing + stale + invalid + suspended
    coverage_value = valid / eligible if eligible else 0.0
    if eligible == 0:
        status: BreadthStatus = "unavailable"
    elif valid > 0 and coverage_value >= minimum_coverage:
        status = "fresh"
    else:
        status = "partial"

    coverage = Coverage(
        requested=requested,
        eligible=eligible,
        valid=valid,
        excluded=excluded,
        coverage=coverage_value,
        status=status,
        missing=missing,
        stale=stale,
        invalid=invalid,
        suspended=suspended,
    )
    buckets = {
        key: BreadthBucket(
            key=key,
            label=label,
            lower_bound=lower,
            upper_bound=upper,
            lower_inclusive=lower_inclusive,
            upper_inclusive=upper_inclusive,
            count=counts[key],
            percentage=(counts[key] / valid * 100.0) if valid else 0.0,
        )
        for key, label, lower, upper, lower_inclusive, upper_inclusive in _BUCKET_SPECS
    }
    return BreadthResult(
        buckets=buckets,
        flat_count=flat_count,
        coverage=coverage,
        status=status,
    )


def serialize_breadth_result(result: BreadthResult) -> dict[str, object]:
    """Convert an immutable breadth result into a JSON-ready API payload."""
    coverage = result.coverage
    return {
        "status": result.status,
        "flat_count": result.flat_count,
        "coverage": {
            "requested": coverage.requested,
            "eligible": coverage.eligible,
            "valid": coverage.valid,
            "excluded": coverage.excluded,
            "coverage": coverage.coverage,
            "status": coverage.status,
            "missing": coverage.missing,
            "stale": coverage.stale,
            "invalid": coverage.invalid,
            "suspended": coverage.suspended,
        },
        "buckets": {
            key: {
                "key": bucket.key,
                "label": bucket.label,
                "lower_bound": bucket.lower_bound,
                "upper_bound": bucket.upper_bound,
                "lower_inclusive": bucket.lower_inclusive,
                "upper_inclusive": bucket.upper_inclusive,
                "count": bucket.count,
                "percentage": bucket.percentage,
            }
            for key, bucket in result.buckets.items()
        },
    }


def robust_percentile_rank(
    value: float | None,
    history: Iterable[float],
    *,
    higher_is_hotter: bool = True,
    lookback: int = 120,
) -> float | None:
    """Return a 0-100 historical midrank, ignoring non-finite observations."""
    if lookback <= 0:
        raise ValueError("lookback must be positive")
    current = _finite_float(value)
    if current is None:
        return None
    window = history[-lookback:] if isinstance(history, Sequence) else deque(history, maxlen=lookback)
    valid_history = [number for item in window if (number := _finite_float(item)) is not None]
    if not valid_history:
        return None

    less = sum(item < current for item in valid_history)
    equal = sum(item == current for item in valid_history)
    percentile = (less + 0.5 * equal) / len(valid_history) * 100.0
    return percentile if higher_is_hotter else 100.0 - percentile


def composite_score(
    components: Iterable[ScoreComponent],
    *,
    minimum_weight: float = 0.70,
    formula_version: str = "market-radar-v1",
) -> CompositeScore:
    """Normalize components and combine them with missing-value reweighting."""
    if not 0 <= minimum_weight <= 1:
        raise ValueError("minimum_weight must be between 0 and 1")
    minimum_weight = max(_MINIMUM_COMPOSITE_WEIGHT, minimum_weight)

    source_components = tuple(components)
    names = [component.name for component in source_components]
    if len(names) != len(set(names)):
        raise ValueError("component names must be unique")
    for component in source_components:
        if not isfinite(component.weight) or not 0 <= component.weight <= 1:
            raise ValueError(f"invalid weight for component {component.name!r}")
    total_weight = sum(component.weight for component in source_components)
    if not isfinite(total_weight) or not abs(total_weight - 1.0) <= 1e-12:
        raise ValueError("component weights must sum to 1")
    _validate_fixed_formula(source_components)

    normalized_components: list[ScoreComponent] = []
    available_weight = 0.0
    for component in source_components:
        reason = component.excluded_reason
        normalized: float | None = None
        if reason is None:
            if component.raw_value is None:
                reason = "missing_value"
            elif _finite_float(component.raw_value) is None:
                reason = "invalid_value"
            else:
                normalized = robust_percentile_rank(
                    component.raw_value,
                    component.history,
                    higher_is_hotter=component.higher_is_hotter,
                )
                if normalized is None:
                    reason = "missing_history"
        if normalized is not None:
            available_weight += component.weight
        normalized_components.append(
            replace(
                component,
                normalized=normalized,
                effective_weight=0.0,
                contribution=0.0,
                excluded_reason=reason,
            )
        )

    scored_components: list[ScoreComponent] = []
    for component in normalized_components:
        effective_weight = (
            component.weight / available_weight
            if component.normalized is not None and available_weight > 0
            else 0.0
        )
        contribution = (component.normalized or 0.0) * effective_weight
        scored_components.append(
            replace(
                component,
                effective_weight=effective_weight,
                contribution=contribution,
            )
        )

    has_sufficient_weight = available_weight + 1e-12 >= minimum_weight
    status: ScoreStatus = "fresh" if has_sufficient_weight else "insufficient"
    value = (
        sum(component.contribution for component in scored_components)
        if has_sufficient_weight and available_weight > 0
        else None
    )
    return CompositeScore(
        value=value,
        status=status,
        effective_weight=available_weight,
        minimum_weight=minimum_weight,
        components=tuple(scored_components),
        formula_version=formula_version,
    )


def crowding_label(score: float) -> str:
    """Map a 0-100 crowding score to its fixed display label."""
    value = _validated_score(score)
    if value < 30:
        return "宽松"
    if value < 55:
        return "正常"
    if value < 75:
        return "拥挤"
    if value < 90:
        return "高拥挤"
    return "极端拥挤"


def emotion_label(score: float) -> str:
    """Map a 0-100 emotion score to its fixed display label."""
    value = _validated_score(score)
    if value < 15:
        return "绝望"
    if value < 30:
        return "恐慌"
    if value < 45:
        return "悲观"
    if value < 55:
        return "中性"
    if value < 70:
        return "乐观"
    if value < 85:
        return "狂热"
    return "极端狂热"


def _age_seconds(now: datetime, quote_time: datetime) -> float | None:
    try:
        return (now - quote_time).total_seconds()
    except (AttributeError, TypeError):
        return None


def _is_valid_price(value: float) -> bool:
    numeric = _finite_float(value)
    return numeric is not None and numeric > 0


def _finite_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return numeric if isfinite(numeric) else None


def _validated_score(score: float) -> float:
    value = _finite_float(score)
    if value is None or not 0 <= value <= 100:
        raise ValueError("score must be a finite number between 0 and 100")
    return value


def _validate_fixed_formula(components: tuple[ScoreComponent, ...]) -> None:
    actual = {component.name: component.weight for component in components}
    formulas = (CROWDING_COMPONENT_WEIGHTS, EMOTION_COMPONENT_WEIGHTS)
    expected = next((formula for formula in formulas if formula.keys() == actual.keys()), None)
    if expected is None or any(
        abs(actual[name] - weight) > 1e-12 for name, weight in expected.items()
    ):
        raise ValueError("components and weights must match a fixed formula")


def _return_percentage(last_price: float, previous_close: float) -> Decimal:
    last = Decimal(str(float(last_price)))
    previous = Decimal(str(float(previous_close)))
    return (last / previous - Decimal(1)) * Decimal(100)


def _bucket_key(return_percentage: Decimal) -> str:
    if return_percentage <= Decimal("-8"):
        return "le_neg_8"
    if return_percentage <= Decimal("-6"):
        return "neg_8_to_neg_6"
    if return_percentage <= Decimal("-4"):
        return "neg_6_to_neg_4"
    if return_percentage <= Decimal("-2"):
        return "neg_4_to_neg_2"
    if return_percentage < 0:
        return "neg_2_to_0"
    if return_percentage < Decimal("2"):
        return "pos_0_to_2"
    if return_percentage < Decimal("4"):
        return "pos_2_to_4"
    if return_percentage < Decimal("6"):
        return "pos_4_to_6"
    if return_percentage < Decimal("8"):
        return "pos_6_to_8"
    return "ge_pos_8"

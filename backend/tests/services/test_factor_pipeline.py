from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from app.services.factor_pipeline import FactorPipeline, FactorPreprocessor, LinearFactorScorer


def test_rank_and_zscore_transforms():
    preprocessor = FactorPreprocessor()
    series = pd.Series([10.0, 20.0, 30.0], index=["A", "B", "C"])

    ranked = preprocessor.transform(series, "rank_pct")
    assert ranked.to_dict() == {"A": 1 / 3, "B": 2 / 3, "C": 1.0}

    zscore = preprocessor.transform(series, "zscore")
    assert np.isclose(float(zscore.mean()), 0.0)
    assert np.isclose(float(zscore.std(ddof=0)), 1.0)


def test_market_cap_neutralize_removes_linear_size_exposure():
    preprocessor = FactorPreprocessor()
    index = ["A", "B", "C", "D", "E"]
    log_size = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], index=index)
    factor = 2.0 * log_size + pd.Series([0.2, -0.1, 0.05, -0.05, 0.1], index=index)
    metadata = pd.DataFrame({"log_circ_mv": log_size}, index=index)

    residual = preprocessor.market_cap_neutralize(factor, metadata)
    corr = residual.corr(log_size)

    assert abs(float(corr)) < 1e-12
    assert residual.notna().sum() == len(index)


def test_industry_zscore_handles_constant_group_as_missing():
    preprocessor = FactorPreprocessor()
    series = pd.Series([1.0, 1.0, 2.0, 4.0], index=["A", "B", "C", "D"])
    metadata = pd.DataFrame(
        {"industry": ["Tech", "Tech", "Finance", "Finance"]},
        index=["A", "B", "C", "D"],
    )

    result = preprocessor.industry_zscore(series, metadata)

    assert pd.isna(result.loc["A"])
    assert pd.isna(result.loc["B"])
    assert np.isclose(float(result.loc["C"]), -1.0)
    assert np.isclose(float(result.loc["D"]), 1.0)


def test_pipeline_loads_filters_and_scores(monkeypatch):
    store = MagicMock()

    def _load_cross_section(name, trade_date, symbols=None, as_of_time=None, params=None):
        assert trade_date == date(2026, 5, 20)
        values = {
            "value": {"A": 1.0, "B": 2.0, "C": 3.0},
            "risk": {"A": 3.0, "B": 2.0, "C": 1.0},
            "is_st": {"A": 0.0, "B": 1.0, "C": 0.0},
        }
        body = values.get(name, {})
        if symbols:
            return {symbol: body[symbol] for symbol in symbols if symbol in body}
        return body

    store.load_cross_section.side_effect = _load_cross_section
    pipeline = FactorPipeline(store=store)
    monkeypatch.setattr(
        pipeline,
        "load_metadata",
        lambda symbols: pd.DataFrame(
            {
                "industry": ["Tech", "Tech", "Finance"],
                "log_circ_mv": [1.0, 2.0, 3.0],
            },
            index=symbols,
        ),
    )

    result = pipeline.build_cross_section(
        factor_specs=[
            {"name": "value", "weight": 1.0, "direction": "higher_better", "transform": "rank_pct"},
            {"name": "risk", "weight": 1.0, "direction": "lower_better", "transform": "rank_pct"},
        ],
        trade_date=date(2026, 5, 20),
        symbols=["A", "B", "C"],
        filters=[{"name": "is_st", "operator": ">=", "value": 0.5}],
        min_factor_coverage=0.5,
        scorer=LinearFactorScorer(),
    )

    assert "B" in result.excluded_symbols
    assert list(result.frame.index) == ["C", "A"]
    assert result.frame.loc["C", "score"] > result.frame.loc["A", "score"]


def test_pipeline_supports_effective_dates_per_factor_and_filter(monkeypatch):
    store = MagicMock()
    seen: list[tuple[str, date]] = []

    def _load_cross_section(name, trade_date, symbols=None, as_of_time=None, params=None):
        seen.append((name, trade_date))
        values = {
            "daily_value": {"A": 1.0, "B": 2.0},
            "timer_filter": {"A": 0.0, "B": 1.0},
        }
        body = values.get(name, {})
        if symbols:
            return {symbol: body[symbol] for symbol in symbols if symbol in body}
        return body

    store.load_cross_section.side_effect = _load_cross_section
    pipeline = FactorPipeline(store=store)
    monkeypatch.setattr(
        pipeline,
        "load_metadata",
        lambda symbols: pd.DataFrame({"industry": ["Tech"] * len(symbols)}, index=symbols),
    )

    result = pipeline.build_cross_section(
        factor_specs=[{"name": "daily_value", "weight": 1.0}],
        trade_date=date(2026, 6, 22),
        symbols=["A", "B"],
        filters=[{"name": "timer_filter", "operator": ">=", "value": 0.5}],
        min_factor_coverage=1.0,
        factor_date_map={"daily_value": date(2026, 6, 18)},
        filter_date_map={"timer_filter": date(2026, 6, 22)},
    )

    assert ("daily_value", date(2026, 6, 18)) in seen
    assert ("timer_filter", date(2026, 6, 22)) in seen
    assert "B" in result.excluded_symbols
    assert list(result.frame.index) == ["A"]

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app.services.cn_paper_ml_experiment import (
    build_factor_feature_snapshot,
    list_cn_paper_experiment_specs,
    summarize_feature_snapshot,
)


class DummyStore:
    def load(self, *, factor_names, start_date, end_date, symbols=None, as_of_time=None, params_hash=None):
        rows = [
            {"symbol": "000001.SZ", "trade_date": date(2024, 1, 2), "factor_name": "paper_composite_value", "value": 0.7},
            {"symbol": "000001.SZ", "trade_date": date(2024, 1, 2), "factor_name": "paper_reversal_20d", "value": -0.1},
            {"symbol": "000002.SZ", "trade_date": date(2024, 1, 2), "factor_name": "paper_composite_value", "value": 0.2},
            {"symbol": "000001.SZ", "trade_date": date(2024, 1, 3), "factor_name": "paper_composite_value", "value": 0.8},
        ]
        frame = pd.DataFrame(rows)
        frame = frame[frame["factor_name"].isin(factor_names)]
        frame = frame[(frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)]
        if symbols:
            frame = frame[frame["symbol"].isin(symbols)]
        return frame


def test_list_cn_paper_experiment_specs() -> None:
    specs = list_cn_paper_experiment_specs()
    assert specs
    assert any(32 in item["paper_ids"] for item in specs)
    assert all(item["status"] == "offline_only" for item in specs)


def test_build_factor_feature_snapshot_pivots_factor_cache() -> None:
    frame = build_factor_feature_snapshot(
        factor_names=["paper_composite_value", "paper_reversal_20d"],
        trade_dates=[date(2024, 1, 2), date(2024, 1, 3)],
        store=DummyStore(),
    )

    assert list(frame.columns) == [
        "trade_date",
        "symbol",
        "paper_composite_value",
        "paper_reversal_20d",
        "feature_coverage",
    ]
    assert len(frame) == 3
    first = frame[(frame["trade_date"] == date(2024, 1, 2)) & (frame["symbol"] == "000001.SZ")].iloc[0]
    assert first["paper_composite_value"] == 0.7
    assert first["feature_coverage"] == 1.0
    summary = summarize_feature_snapshot(frame, ["paper_composite_value", "paper_reversal_20d"])
    assert summary["rows"] == 3
    assert summary["symbols"] == 2


def test_build_factor_feature_snapshot_rejects_unknown_factor() -> None:
    with pytest.raises(ValueError, match="Unsupported experiment factor"):
        build_factor_feature_snapshot(
            factor_names=["unknown_factor"],
            trade_dates=[date(2024, 1, 2)],
            store=DummyStore(),
        )

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from math import exp, log

import numpy as np
import pandas as pd
import pytest

import app.scripts.research_intraday_t_v4 as v4
from app.scripts.research_intraday_t_v3 import (
    _implementation_fingerprint as v3_implementation_fingerprint,
)
from app.scripts.research_intraday_t_v4 import (
    BENCHMARK_MAP,
    DEFAULT_BASE_QUANTITIES,
    ResearchGateBacktester,
    _implementation_fingerprint,
    _implementation_manifest,
    apply_entry_gate,
    build_recommendation,
    build_stress_scenarios,
    build_variants,
    compute_causal_gate_features,
    frame_fingerprint,
    resolve_research_base_quantities,
    signal_ledger_fingerprint,
    validate_run_matrix,
    write_artifacts,
)
from app.services.intraday_t_backtest import BacktestConfig
from app.services.intraday_t_strategy import CostModel

CAUSAL_FEATURE_COLUMNS = (
    "residual_return_bps",
    "amount_location",
    "amount_scale",
    "amount_z",
    "price_volume_interaction",
    "volume_return_forecast_bps",
    "forecast_intercept_bps",
    "forecast_return_beta",
    "forecast_price_volume_beta",
    "forecast_training_days",
    "forecast_training_samples",
    "stock_jump_scale_bps",
    "benchmark_jump_scale_bps",
    "stock_jump_score",
    "benchmark_jump_score",
    "idiosyncratic_jump",
    "recent_idiosyncratic_jump",
    "amihud_impact",
    "amihud_location",
    "amihud_scale",
    "amihud_impact_z",
)


def _dates(count: int) -> list[date]:
    start = date(2025, 1, 2)
    return [start + timedelta(days=offset) for offset in range(count)]


def _variant(name: str):
    return next(item for item in build_variants() if item.name == name)


def _bar(
    *,
    timestamp: pd.Timestamp,
    symbol: str,
    open_price: float,
    close: float,
    amount: float,
) -> dict:
    return {
        "datetime": timestamp,
        "symbol": symbol,
        "open": open_price,
        "high": max(open_price, close) * 1.0001,
        "low": min(open_price, close) * 0.9999,
        "close": close,
        "volume": amount / close,
        "amount": amount,
    }


def _causal_frames(
    day_count: int = 10,
    minutes: tuple[str, ...] = (
        "09:31",
        "09:32",
        "09:33",
        "09:34",
        "13:01",
        "13:02",
        "13:03",
        "13:04",
    ),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    stock_rows: list[dict] = []
    benchmark_rows: list[dict] = []
    for day_offset, trade_date in enumerate(_dates(day_count)):
        stock_previous = 20.0 + day_offset * 0.1
        benchmark_previous = 100.0 + day_offset * 0.2
        for minute_offset, minute in enumerate(minutes):
            if minute == "13:01":
                stock_previous *= 1.01
                benchmark_previous *= 1.01
            timestamp = pd.Timestamp(f"{trade_date.isoformat()} {minute}")
            benchmark_return_bps = ((day_offset + minute_offset) % 3 - 1) * 2.0
            residual_return_bps = (((day_offset * 5 + minute_offset * 3) % 9) - 4) * 3.0
            stock_return_bps = benchmark_return_bps + residual_return_bps
            stock_close = stock_previous * exp(stock_return_bps / 10_000)
            benchmark_close = benchmark_previous * exp(benchmark_return_bps / 10_000)
            amount = exp(10.0 + day_offset * 0.12 + minute_offset * 0.04)
            stock_rows.append(
                _bar(
                    timestamp=timestamp,
                    symbol="603629.SH",
                    open_price=stock_previous,
                    close=stock_close,
                    amount=amount,
                )
            )
            benchmark_rows.append(
                _bar(
                    timestamp=timestamp,
                    symbol="000001.SH",
                    open_price=benchmark_previous,
                    close=benchmark_close,
                    amount=amount * 50,
                )
            )
            stock_previous = stock_close
            benchmark_previous = benchmark_close
    return (
        pd.DataFrame(stock_rows).set_index("datetime"),
        pd.DataFrame(benchmark_rows).set_index("datetime"),
    )


def _compute(stock: pd.DataFrame, benchmark: pd.DataFrame) -> pd.DataFrame:
    return compute_causal_gate_features(
        stock,
        benchmark,
        amount_history_days=3,
        min_amount_history_days=3,
        ols_history_days=4,
        min_ols_days=2,
        jump_window=5,
        min_jump_observations=3,
        jump_threshold=4.0,
        recent_jump_minutes=3,
        amihud_history_days=3,
        min_amihud_history_days=3,
    )


def test_v4_scope_is_fixed_to_the_two_deployment_stocks():
    assert BENCHMARK_MAP == {
        "603629.SH": "000001.SH",
        "688008.SH": "000688.SH",
    }
    assert DEFAULT_BASE_QUANTITIES == {
        "603629.SH": 2_000,
        "688008.SH": 1_000,
    }


def test_explicit_empty_base_quantities_fail_instead_of_falling_back_to_defaults():
    assert resolve_research_base_quantities(None) == DEFAULT_BASE_QUANTITIES
    with pytest.raises(ValueError, match="exactly"):
        resolve_research_base_quantities({})


def test_injected_index_data_still_passes_through_the_physical_schema_normalizer(
    monkeypatch,
    tmp_path,
):
    injected = pd.DataFrame(
        {
            "time": ["2026-07-14 09:31:00"],
            "symbol": ["000001.SH"],
            "open": [3_500.0],
            "high": [3_501.0],
            "low": [3_499.0],
            "close": [3_500.5],
            "volume": [100_000.0],
            "money": [350_050_000.0],
        }
    )
    calls: list[pd.DataFrame] = []

    class EmptyStore:
        def load_minute(self, *args, **kwargs):
            return pd.DataFrame(
                columns=("symbol", "open", "high", "low", "close", "volume", "amount"),
                index=pd.DatetimeIndex([], name="datetime"),
            )

    def stop_after_normalization(raw: pd.DataFrame) -> pd.DataFrame:
        calls.append(raw)
        raise RuntimeError("injected index normalized")

    monkeypatch.setattr(v4, "normalize_index_minute_data", stop_after_normalization)

    with pytest.raises(RuntimeError, match="injected index normalized"):
        v4.run_research(
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            output_dir=tmp_path,
            store=EmptyStore(),
            index_data=injected,
        )
    assert calls == [injected]


def test_injected_normalized_index_data_is_validated_without_requiring_physical_names(
    monkeypatch,
    tmp_path,
):
    timestamp = pd.Timestamp("2026-07-14 09:31:00")
    injected = pd.DataFrame(
        {
            "symbol": ["000001.SH"],
            "open": [3_500.0],
            "high": [3_501.0],
            "low": [3_499.0],
            "close": [3_500.5],
            "volume": [100_000.0],
            "amount": [350_050_000.0],
        },
        index=pd.DatetimeIndex([timestamp], name="datetime"),
    )
    observed: list[pd.DataFrame] = []

    class EmptyStore:
        def load_minute(self, *args, **kwargs):
            return pd.DataFrame(
                columns=("symbol", "open", "high", "low", "close", "volume", "amount"),
                index=pd.DatetimeIndex([], name="datetime"),
            )

    def stop_after_validation(stocks, indexes, **kwargs):
        observed.append(indexes)
        raise RuntimeError("normalized index accepted")

    monkeypatch.setattr(v4, "validate_complete_panel", stop_after_validation)

    with pytest.raises(RuntimeError, match="normalized index accepted"):
        v4.run_research(
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
            output_dir=tmp_path,
            store=EmptyStore(),
            index_data=injected,
        )
    pd.testing.assert_frame_equal(observed[0], injected, check_dtype=False)


def test_v4_variants_keep_directional_move_as_anchor_and_add_three_independent_gates():
    variants = build_variants()

    assert [item.name for item in variants] == [
        "directional_move_0_100",
        "volume_return_forecast",
        "idiosyncratic_jump_veto",
        "amihud_impact",
    ]
    assert all(item.params.max_entry_z == 2.4 for item in variants)
    assert all(item.params.morning_entry_start.isoformat() == "10:00:00" for item in variants)
    assert all(item.params.morning_entry_end.isoformat() == "10:30:00" for item in variants)
    assert all(item.params.allow_afternoon_entries is False for item in variants)


def test_future_market_changes_do_not_rewrite_past_causal_features():
    stock, benchmark = _causal_frames()
    original = _compute(stock, benchmark)
    final_day = _dates(10)[-1]
    changed_stock = stock.copy()
    changed_benchmark = benchmark.copy()
    stock_future = changed_stock.index.date == final_day
    benchmark_future = changed_benchmark.index.date == final_day
    changed_stock.loc[stock_future, "close"] *= np.linspace(0.95, 1.15, stock_future.sum())
    changed_stock.loc[stock_future, "amount"] *= 50
    changed_stock.loc[stock_future, "high"] = changed_stock.loc[
        stock_future, ["open", "high", "close"]
    ].max(axis=1)
    changed_stock.loc[stock_future, "low"] = changed_stock.loc[
        stock_future, ["open", "low", "close"]
    ].min(axis=1)
    changed_benchmark.loc[benchmark_future, "close"] *= np.linspace(
        1.10, 0.90, benchmark_future.sum()
    )
    changed_benchmark.loc[benchmark_future, "high"] = changed_benchmark.loc[
        benchmark_future, ["open", "high", "close"]
    ].max(axis=1)
    changed_benchmark.loc[benchmark_future, "low"] = changed_benchmark.loc[
        benchmark_future, ["open", "low", "close"]
    ].min(axis=1)

    changed = _compute(changed_stock, changed_benchmark)

    past = original.index.date < final_day
    pd.testing.assert_frame_equal(
        original.loc[past, list(CAUSAL_FEATURE_COLUMNS)],
        changed.loc[past, list(CAUSAL_FEATURE_COLUMNS)],
        check_dtype=False,
    )


def test_amount_robust_z_uses_same_minute_history_shifted_by_one_day():
    stock_rows: list[dict] = []
    benchmark_rows: list[dict] = []
    log_amounts = (10.0, 12.0, 14.0, 40.0)
    for trade_date, log_amount in zip(_dates(4), log_amounts, strict=True):
        timestamp = pd.Timestamp(f"{trade_date.isoformat()} 09:31")
        stock_rows.append(
            _bar(
                timestamp=timestamp,
                symbol="603629.SH",
                open_price=20.0,
                close=20.02,
                amount=exp(log_amount) - 1.0,
            )
        )
        benchmark_rows.append(
            _bar(
                timestamp=timestamp,
                symbol="000001.SH",
                open_price=100.0,
                close=100.0,
                amount=1_000_000.0,
            )
        )

    featured = _compute(
        pd.DataFrame(stock_rows).set_index("datetime"),
        pd.DataFrame(benchmark_rows).set_index("datetime"),
    )
    latest = featured.iloc[-1]

    assert featured.iloc[:3]["amount_location"].isna().all()
    assert latest["amount_location"] == pytest.approx(12.0)
    assert latest["amount_scale"] == pytest.approx(1.4826 * 2.0)
    assert latest["amount_z"] == pytest.approx((40.0 - 12.0) / (1.4826 * 2.0))
    assert latest["price_volume_interaction"] == pytest.approx(
        latest["amount_z"] * latest["residual_return_bps"]
    )


def test_returns_and_forecast_targets_do_not_cross_day_or_lunch_boundaries():
    minutes = ("09:31", "09:32", "13:01", "13:02")
    stock, benchmark = _causal_frames(day_count=6, minutes=minutes)
    final_day = _dates(6)[-1]
    afternoon_open = pd.Timestamp(f"{final_day.isoformat()} 13:01")
    morning_open = pd.Timestamp(f"{final_day.isoformat()} 09:31")

    featured = _compute(stock, benchmark)

    expected_morning = 10_000 * (
        log(stock.loc[morning_open, "close"] / stock.loc[morning_open, "open"])
        - log(benchmark.loc[morning_open, "close"] / benchmark.loc[morning_open, "open"])
    )
    expected_afternoon = 10_000 * (
        log(stock.loc[afternoon_open, "close"] / stock.loc[afternoon_open, "open"])
        - log(benchmark.loc[afternoon_open, "close"] / benchmark.loc[afternoon_open, "open"])
    )
    assert featured.loc[morning_open, "residual_return_bps"] == pytest.approx(expected_morning)
    assert featured.loc[afternoon_open, "residual_return_bps"] == pytest.approx(expected_afternoon)
    # Days 4 and 5 are the first two completed days with causal amount z values.
    assert featured.loc[morning_open, "forecast_training_days"] == 2
    assert featured.loc[morning_open, "forecast_training_samples"] == 4


def test_online_ols_coefficients_use_only_previous_completed_days():
    stock, benchmark = _causal_frames(day_count=10)
    featured = _compute(stock, benchmark)
    evaluation_day = _dates(10)[-1]
    evaluation = featured.loc[featured.index.date == evaluation_day]
    coefficient_columns = [
        "forecast_intercept_bps",
        "forecast_return_beta",
        "forecast_price_volume_beta",
        "forecast_training_days",
        "forecast_training_samples",
    ]
    assert evaluation[coefficient_columns].notna().all().all()
    assert all(evaluation[column].nunique() == 1 for column in coefficient_columns)

    changed_stock = stock.copy()
    current_day = changed_stock.index.date == evaluation_day
    changed_stock.loc[current_day, "amount"] *= np.linspace(1.0, 100.0, current_day.sum())
    changed_stock.loc[current_day, "close"] *= np.linspace(1.0, 1.2, current_day.sum())
    changed_stock.loc[current_day, "high"] = changed_stock.loc[
        current_day, ["open", "high", "close"]
    ].max(axis=1)
    changed_stock.loc[current_day, "low"] = changed_stock.loc[
        current_day, ["open", "low", "close"]
    ].min(axis=1)
    changed = _compute(changed_stock, benchmark)
    changed_evaluation = changed.loc[changed.index.date == evaluation_day]

    pd.testing.assert_frame_equal(
        evaluation[coefficient_columns],
        changed_evaluation[coefficient_columns],
        check_dtype=False,
    )


def _jump_frames(
    *,
    current_stock_bps: float,
    current_benchmark_bps: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    trade_date = _dates(1)[0]
    timestamps = pd.date_range(f"{trade_date.isoformat()} 09:31", periods=10, freq="1min")
    stock_rows: list[dict] = []
    benchmark_rows: list[dict] = []
    stock_previous = 20.0
    benchmark_previous = 100.0
    jump_at = timestamps[7]
    for offset, timestamp in enumerate(timestamps):
        ordinary_bps = (8.0, 10.0, 12.0)[offset % 3]
        stock_bps = current_stock_bps if timestamp == jump_at else ordinary_bps
        benchmark_bps = current_benchmark_bps if timestamp == jump_at else ordinary_bps
        stock_close = stock_previous * exp(stock_bps / 10_000)
        benchmark_close = benchmark_previous * exp(benchmark_bps / 10_000)
        stock_rows.append(
            _bar(
                timestamp=timestamp,
                symbol="603629.SH",
                open_price=stock_previous,
                close=stock_close,
                amount=500_000.0 + offset * 10_000,
            )
        )
        benchmark_rows.append(
            _bar(
                timestamp=timestamp,
                symbol="000001.SH",
                open_price=benchmark_previous,
                close=benchmark_close,
                amount=50_000_000.0 + offset * 100_000,
            )
        )
        stock_previous = stock_close
        benchmark_previous = benchmark_close
    return (
        pd.DataFrame(stock_rows).set_index("datetime"),
        pd.DataFrame(benchmark_rows).set_index("datetime"),
        jump_at,
    )


def test_jump_scale_excludes_the_current_bar():
    first_stock, first_benchmark, jump_at = _jump_frames(
        current_stock_bps=60.0,
        current_benchmark_bps=10.0,
    )
    second_stock, second_benchmark, _ = _jump_frames(
        current_stock_bps=120.0,
        current_benchmark_bps=10.0,
    )

    first = _compute(first_stock, first_benchmark)
    second = _compute(second_stock, second_benchmark)

    assert first.loc[jump_at, "stock_jump_scale_bps"] == pytest.approx(
        second.loc[jump_at, "stock_jump_scale_bps"]
    )
    assert second.loc[jump_at, "stock_jump_score"] > first.loc[jump_at, "stock_jump_score"]


def test_benchmark_cojump_is_not_labeled_as_idiosyncratic():
    common_stock, common_benchmark, jump_at = _jump_frames(
        current_stock_bps=80.0,
        current_benchmark_bps=80.0,
    )
    idio_stock, idio_benchmark, _ = _jump_frames(
        current_stock_bps=80.0,
        current_benchmark_bps=10.0,
    )

    common = _compute(common_stock, common_benchmark)
    idiosyncratic = _compute(idio_stock, idio_benchmark)

    assert common.loc[jump_at, "stock_jump_score"] >= 4.0
    assert common.loc[jump_at, "benchmark_jump_score"] >= 4.0
    assert bool(common.loc[jump_at, "idiosyncratic_jump"]) is False
    assert bool(common.loc[jump_at, "recent_idiosyncratic_jump"]) is False
    assert idiosyncratic.loc[jump_at, "stock_jump_score"] >= 4.0
    assert idiosyncratic.loc[jump_at, "benchmark_jump_score"] < 4.0
    assert bool(idiosyncratic.loc[jump_at, "idiosyncratic_jump"]) is True
    assert idiosyncratic.loc[
        jump_at : jump_at + pd.Timedelta(minutes=2), "recent_idiosyncratic_jump"
    ].all()


def test_amihud_impact_z_uses_five_bars_and_only_prior_same_minute_observations():
    minutes = ("09:31", "09:32", "09:33", "09:34", "09:35")
    stock, benchmark = _causal_frames(day_count=4, minutes=minutes)
    featured = _compute(stock, benchmark)
    evaluation_times = [pd.Timestamp(f"{trade_date.isoformat()} 09:35") for trade_date in _dates(4)]
    prior = featured.loc[evaluation_times[:3], "amihud_impact"].to_numpy(dtype=float)
    expected_location = float(np.median(prior))
    expected_mad = float(np.median(np.abs(prior - expected_location)))
    latest = featured.loc[evaluation_times[-1]]

    assert featured.loc[evaluation_times[:3], "amihud_location"].isna().all()
    assert featured.groupby(featured.index.date).head(4)["amihud_impact"].isna().all()
    assert latest["amihud_location"] == pytest.approx(expected_location)
    assert latest["amihud_scale"] == pytest.approx(1.4826 * expected_mad)
    assert latest["amihud_impact_z"] == pytest.approx(
        (latest["amihud_impact"] - expected_location) / (1.4826 * expected_mad)
    )


def test_amihud_window_resets_at_lunch():
    minutes = (
        "09:31",
        "09:32",
        "09:33",
        "09:34",
        "09:35",
        "13:01",
        "13:02",
        "13:03",
        "13:04",
        "13:05",
    )
    stock, benchmark = _causal_frames(day_count=4, minutes=minutes)
    featured = _compute(stock, benchmark)
    final_day = _dates(4)[-1]

    start = f"{final_day.isoformat()} 13:01"
    end = f"{final_day.isoformat()} 13:04"
    assert featured.loc[start:end, "amihud_impact"].isna().all()
    assert pd.notna(featured.loc[pd.Timestamp(f"{final_day.isoformat()} 13:05"), "amihud_impact"])


def test_directional_anchor_is_inclusive_at_zero_and_exclusive_at_100bps():
    frame = pd.DataFrame(
        {
            "ready": [True, True, True, True, False],
            "zscore": [2.0, -2.0, 2.0, -2.0, 2.0],
            "session_return_bps": [0.0, -99.999, 100.0, 0.001, 0.0],
        }
    )

    gate = apply_entry_gate(frame, _variant("directional_move_0_100"))

    assert gate.tolist() == [True, True, False, False, False]


def test_volume_return_gate_requires_a_strict_opposite_forecast_and_fails_closed():
    frame = pd.DataFrame(
        {
            "ready": [True] * 5,
            "zscore": [-2.0, -2.0, 2.0, 2.0, -2.0],
            "session_return_bps": [0.0] * 5,
            "volume_return_forecast_bps": [1.0, 0.0, -1.0, 1.0, np.nan],
        }
    )

    gate = apply_entry_gate(frame, _variant("volume_return_forecast"))

    assert gate.tolist() == [True, False, True, False, False]


def test_recent_idiosyncratic_jump_gate_vetoes_true_and_fails_closed():
    frame = pd.DataFrame(
        {
            "ready": [True, True, True, False],
            "zscore": [-2.0] * 4,
            "session_return_bps": [0.0] * 4,
            "recent_idiosyncratic_jump": [False, True, pd.NA, False],
        }
    )

    gate = apply_entry_gate(frame, _variant("idiosyncratic_jump_veto"))

    assert gate.tolist() == [True, False, False, False]


def test_amihud_gate_includes_one_and_a_half_sigma_boundary_and_fails_closed():
    frame = pd.DataFrame(
        {
            "ready": [True] * 5,
            "zscore": [-2.0] * 5,
            "session_return_bps": [0.0] * 5,
            "amihud_impact_z": [1.5, 1.500001, -4.0, np.nan, 0.0],
        }
    )
    frame.loc[4, "session_return_bps"] = -100.0

    gate = apply_entry_gate(frame, _variant("amihud_impact"))

    assert gate.tolist() == [True, False, True, False, False]


def test_each_new_gate_also_enforces_the_directional_anchor():
    frame = pd.DataFrame(
        {
            "ready": [True],
            "zscore": [2.0],
            "session_return_bps": [100.0],
            "volume_return_forecast_bps": [-10.0],
            "recent_idiosyncratic_jump": [False],
            "amihud_impact_z": [0.0],
        }
    )

    for name in (
        "volume_return_forecast",
        "idiosyncratic_jump_veto",
        "amihud_impact",
    ):
        assert apply_entry_gate(frame, _variant(name)).tolist() == [False]


def _featured_backtest_frame() -> pd.DataFrame:
    rows = []
    times = ("09:59", "10:00", "10:01", "10:02", "15:00")
    for offset, minute in enumerate(times):
        close = 20.0 + offset * 0.01
        rows.append(
            {
                "datetime": pd.Timestamp(f"2026-07-14 {minute}"),
                "symbol": "603629.SH",
                "open": close,
                "high": close + 0.05,
                "low": close - 0.05,
                "close": close,
                "volume": 20_000,
                "amount": close * 20_000,
                "vwap": 20.0,
                "zscore": 0.0,
                "previous_zscore": 0.0,
                "fast_ema": 20.0,
                "slow_ema": 20.0,
                "vwap_slope": 0.0,
                "volume_ratio": 1.0,
                "estimated_edge_bps": 35.0,
                "previous_price": close - 0.01,
                "realized_vol_bps": 30.0,
                "session_return_bps": 0.0,
                "ready": True,
                "volume_return_forecast_bps": np.nan,
            }
        )
    frame = pd.DataFrame(rows).set_index("datetime")
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [
        -2.0,
        -2.3,
    ]
    frame.loc[pd.Timestamp("2026-07-14 10:00"), "volume_return_forecast_bps"] = 1.0
    frame.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [
        -0.1,
        -2.0,
    ]
    return frame


def test_entry_gate_never_blocks_an_active_pair_restoration():
    frame = _featured_backtest_frame()
    variant = _variant("volume_return_forecast")
    config = BacktestConfig(
        initial_capital=100_000.0,
        base_quantities={"603629.SH": 2_000},
        params=variant.params,
        max_bar_volume_fraction=0.1,
    )

    result = ResearchGateBacktester(variant).run(frame, config)

    assert result["metrics"]["entry_count"] == 1
    assert result["metrics"]["completed_pairs"] == 1
    assert [trade["leg"] for trade in result["trades"]] == ["entry", "restore"]


def test_stress_scenarios_are_fixed_at_two_five_ten_bps_and_capacity():
    scenarios = build_stress_scenarios()

    assert [(item.name, item.slippage_bps) for item in scenarios] == [
        ("nominal", 2.0),
        ("slippage_5bp", 5.0),
        ("slippage_10bp", 10.0),
        ("participation_2_5pct", 2.0),
    ]
    assert scenarios[-1].max_bar_volume_fraction == 0.025


def test_cost_stress_freezes_decision_cost_and_preserves_the_signal_ledger():
    frame = _featured_backtest_frame()
    variant = _variant("volume_return_forecast")
    results = []
    for scenario in build_stress_scenarios():
        config = BacktestConfig(
            initial_capital=100_000.0,
            base_quantities={"603629.SH": 2_000},
            params=variant.params,
            cost=CostModel(slippage_bps=scenario.slippage_bps),
            decision_cost=CostModel(slippage_bps=2.0),
            max_bar_volume_fraction=scenario.max_bar_volume_fraction,
        )
        results.append(ResearchGateBacktester(variant).run(frame, config))

    assert [result["metrics"]["entry_count"] for result in results] == [1, 1, 1, 1]
    assert len({signal_ledger_fingerprint(result["trades"]) for result in results[:3]}) == 1
    entry_fill_prices = [result["trades"][0]["fill_price"] for result in results[:3]]
    assert entry_fill_prices == sorted(entry_fill_prices)
    assert len(set(entry_fill_prices)) == 3


def test_signal_ledger_hash_ignores_prices_and_pnl_but_covers_trade_identity():
    trade = {
        "pair_id": "pair-1",
        "symbol": "603629.SH",
        "direction": "POSITIVE",
        "leg": "entry",
        "signal_at": "2026-07-14T10:00:00",
        "fill_at": "2026-07-14T10:01:00",
        "quantity": 500,
        "reason": "positive_t_entry",
        "fill_price": 20.01,
        "net_pnl": -1.0,
    }
    original = signal_ledger_fingerprint([trade])
    economic_change = {**trade, "fill_price": 20.20, "net_pnl": -100.0}
    assert signal_ledger_fingerprint([economic_change]) == original

    for key, changed_value in {
        "pair_id": "pair-2",
        "symbol": "688008.SH",
        "direction": "REVERSE",
        "leg": "restore",
        "signal_at": "2026-07-14T10:01:00",
        "fill_at": "2026-07-14T10:02:00",
        "quantity": 600,
        "reason": "mean_reversion_exit",
    }.items():
        assert signal_ledger_fingerprint([{**trade, key: changed_value}]) != original


def test_recommendation_is_always_research_only_even_for_perfect_metrics():
    recommendation = build_recommendation(
        [
            {
                "fold": "fold_01",
                "variant": "volume_return_forecast",
                "scenario": "nominal",
                "metrics": {
                    "net_t_pnl": 1_000_000.0,
                    "completed_pairs": 10_000,
                    "open_pairs_at_end": 0,
                    "restoration_failures": 0,
                    "restoration_rate": 1.0,
                },
            }
        ]
    )

    assert recommendation["decision"] == "research_only"
    assert recommendation["auto_promoted"] is False


def test_run_matrix_is_a_complete_unique_cartesian_product():
    folds = ("fold_01", "fold_02")
    variants = tuple(item.name for item in build_variants())
    scenarios = tuple(item.name for item in build_stress_scenarios())
    runs = [
        {"fold": fold, "variant": variant, "scenario": scenario}
        for fold in folds
        for variant in variants
        for scenario in scenarios
    ]

    validate_run_matrix(
        runs,
        fold_names=folds,
        variant_names=variants,
        scenario_names=scenarios,
    )

    bad_matrices = (
        runs[:-1],
        [*runs, runs[0].copy()],
        [*runs, {"fold": "fold_99", "variant": variants[0], "scenario": scenarios[0]}],
    )
    for invalid in bad_matrices:
        with pytest.raises(ValueError, match="run matrix"):
            validate_run_matrix(
                invalid,
                fold_names=folds,
                variant_names=variants,
                scenario_names=scenarios,
            )


def test_artifact_json_commits_csv_hash_and_implementation_fingerprint(tmp_path):
    fingerprint = _implementation_fingerprint()
    ledger = [
        {
            "fold": "fold_01",
            "variant": "directional_move_0_100",
            "scenario": "nominal",
            "pair_id": "pair-1",
            "symbol": "603629.SH",
            "direction": "POSITIVE",
            "leg": "entry",
            "signal_at": "2026-07-14T10:00:00",
            "fill_at": "2026-07-14T10:01:00",
            "quantity": 500,
            "reason": "positive_t_entry",
        }
    ]
    report = {
        "data_quality": {"implementation_fingerprint": fingerprint},
        "signal_ledger": ledger,
        "runs": [
            {
                "fold": "fold_01",
                "sample": "retrospective_test",
                "variant": "directional_move_0_100",
                "scenario": "nominal",
                "period_start": "2025-01-01",
                "period_end": "2025-01-02",
                "trade_days": 2,
                "bars": 960,
                "metrics": {"net_t_pnl": -1.0},
            }
        ],
    }

    paths = write_artifacts(report, tmp_path)

    persisted = json.loads((tmp_path / "research.json").read_text(encoding="utf-8"))
    runs_hash = hashlib.sha256((tmp_path / "runs.csv").read_bytes()).hexdigest()
    ledger_hash = hashlib.sha256((tmp_path / "signal_ledger.csv").read_bytes()).hexdigest()
    assert persisted["artifact_integrity"] == {
        "commit_marker": "research.json",
        "runs_csv_sha256": runs_hash,
        "signal_ledger_csv_sha256": ledger_hash,
    }
    assert persisted["data_quality"]["implementation_fingerprint"] == fingerprint
    assert len(fingerprint) == 64
    int(fingerprint, 16)
    assert fingerprint == _implementation_fingerprint()
    assert fingerprint != v3_implementation_fingerprint()
    assert paths["json"].endswith("research.json")
    assert paths["signal_ledger"].endswith("signal_ledger.csv")


def test_implementation_manifest_covers_every_strategy_layer():
    manifest = {
        path.replace("\\", "/"): digest for path, digest in _implementation_manifest().items()
    }
    required_suffixes = (
        "backend/app/scripts/research_intraday_t_v4.py",
        "backend/app/scripts/research_intraday_t_v3.py",
        "backend/app/scripts/research_intraday_t_v2.py",
        "backend/app/services/intraday_t_strategy.py",
        "backend/app/services/intraday_t_backtest.py",
    )

    for suffix in required_suffixes:
        matches = [digest for path, digest in manifest.items() if path.endswith(suffix)]
        assert len(matches) == 1
        assert len(matches[0]) == 64
        int(matches[0], 16)


def test_market_fingerprint_changes_when_amount_changes():
    frame = _featured_backtest_frame()
    original = frame_fingerprint(frame)
    changed = frame.copy()
    changed.iloc[0, changed.columns.get_loc("amount")] += 1.0

    assert frame_fingerprint(changed) != original

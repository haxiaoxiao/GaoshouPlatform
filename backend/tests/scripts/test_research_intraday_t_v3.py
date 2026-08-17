from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from math import log

import pandas as pd
import pytest

from app.scripts.research_intraday_t_v3 import (
    BENCHMARK_MAP,
    DEFAULT_BASE_QUANTITIES,
    ResearchGateBacktester,
    apply_entry_gate,
    build_recommendation,
    build_retrospective_folds,
    build_variants,
    compute_causal_market_features,
    frame_fingerprint,
    limit_price_fingerprint,
    normalize_index_minute_data,
    validate_calendar_coverage,
    validate_complete_panel,
    validate_research_base_quantities,
    write_artifacts,
)
from app.services.intraday_t_backtest import BacktestConfig
from app.services.intraday_t_strategy import CostModel

FEATURE_COLUMNS = (
    "market_residual_bps",
    "residual_location_bps",
    "residual_scale_bps",
    "residual_z",
    "residual_path_efficiency",
    "relative_jump_score",
)


def _dates(count: int) -> list[date]:
    start = date(2024, 1, 2)
    return [start + timedelta(days=offset) for offset in range(count)]


def _variant(name: str):
    return next(item for item in build_variants() if item.name == name)


def _market_frames(day_count: int = 5) -> tuple[pd.DataFrame, pd.DataFrame]:
    second_minute_residuals = (-30.0, -10.0, 10.0, 30.0, 50.0)
    third_minute_residuals = (10.0, -20.0, 30.0, -40.0, 60.0)
    stock_rows: list[dict] = []
    index_rows: list[dict] = []
    for offset, trade_date in enumerate(_dates(day_count)):
        for minute, residual_bps in zip(
            ("09:31", "09:32", "09:33"),
            (0.0, second_minute_residuals[offset], third_minute_residuals[offset]),
            strict=True,
        ):
            timestamp = pd.Timestamp(f"{trade_date.isoformat()} {minute}")
            stock_close = 20.0 * (1.0 + residual_bps / 10_000)
            stock_rows.append(
                {
                    "datetime": timestamp,
                    "symbol": "603629.SH",
                    "open": stock_close,
                    "high": stock_close,
                    "low": stock_close,
                    "close": stock_close,
                    "volume": 10_000,
                    "amount": stock_close * 10_000,
                }
            )
            index_rows.append(
                {
                    "datetime": timestamp,
                    "symbol": "000001.SH",
                    "open": 100.0,
                    "high": 100.0,
                    "low": 100.0,
                    "close": 100.0,
                    "volume": 100_000,
                    "amount": 10_000_000.0,
                }
            )
    return (
        pd.DataFrame(stock_rows).set_index("datetime"),
        pd.DataFrame(index_rows).set_index("datetime"),
    )


def test_benchmark_map_is_fixed_for_the_two_supported_stocks():
    assert BENCHMARK_MAP == {
        "603629.SH": "000001.SH",
        "688008.SH": "000688.SH",
    }


def test_retrospective_folds_use_expanding_history_and_contiguous_test_blocks():
    trade_dates = _dates(398)

    folds = build_retrospective_folds(
        trade_dates,
        warmup_days=252,
        test_days=42,
    )

    assert [len(fold.test_dates) for fold in folds] == [42, 42, 62]
    assert [len(fold.train_dates) for fold in folds] == [252, 294, 336]
    assert folds[0].test_dates == tuple(trade_dates[252:294])
    assert folds[1].test_dates == tuple(trade_dates[294:336])
    assert folds[2].test_dates == tuple(trade_dates[336:398])
    for fold in folds:
        assert set(fold.train_dates).isdisjoint(fold.test_dates)
        assert max(fold.train_dates) < min(fold.test_dates)
    for left, right in zip(folds, folds[1:], strict=False):
        assert left.test_dates[-1] < right.test_dates[0]
        assert right.train_dates == left.train_dates + left.test_dates


def test_causal_market_features_require_three_prior_same_clock_observations():
    stock, index = _market_frames()

    featured = compute_causal_market_features(
        stock,
        index,
        history_days=20,
        min_history_days=3,
    )

    assert set(FEATURE_COLUMNS).issubset(featured.columns)
    same_clock = featured.loc[featured.index.strftime("%H:%M") == "09:32"]
    assert same_clock.iloc[:3]["residual_z"].isna().all()
    assert same_clock.iloc[:3]["residual_location_bps"].isna().all()
    assert same_clock.iloc[:3]["residual_scale_bps"].isna().all()
    assert pd.notna(same_clock.iloc[3]["residual_z"])
    assert same_clock.iloc[3]["residual_scale_bps"] > 0


def test_causal_market_features_do_not_rewrite_the_past_when_future_prices_change():
    stock, index = _market_frames()
    original = compute_causal_market_features(
        stock,
        index,
        history_days=20,
        min_history_days=3,
    )
    final_day = _dates(5)[-1]
    changed_stock = stock.copy()
    changed_index = index.copy()
    stock_future = changed_stock.index.date == final_day
    index_future = changed_index.index.date == final_day
    changed_stock.loc[stock_future, "close"] *= 1.25
    changed_stock.loc[stock_future, "high"] = changed_stock.loc[stock_future, ["high", "close"]].max(
        axis=1
    )
    changed_index.loc[index_future, "close"] *= 0.75
    changed_index.loc[index_future, "low"] = changed_index.loc[index_future, ["low", "close"]].min(
        axis=1
    )

    changed = compute_causal_market_features(
        changed_stock,
        changed_index,
        history_days=20,
        min_history_days=3,
    )

    past = original.index.date < final_day
    pd.testing.assert_frame_equal(
        original.loc[past, list(FEATURE_COLUMNS)],
        changed.loc[past, list(FEATURE_COLUMNS)],
        check_dtype=False,
    )


def test_market_residual_includes_the_first_minute_open_to_close_move():
    timestamp = pd.Timestamp("2024-01-02 09:31")
    stock = pd.DataFrame(
        {
            "symbol": ["603629.SH"],
            "open": [20.0],
            "high": [20.02],
            "low": [20.0],
            "close": [20.02],
            "volume": [10_000],
            "amount": [200_200.0],
        },
        index=pd.DatetimeIndex([timestamp], name="datetime"),
    )
    index = pd.DataFrame(
        {
            "symbol": ["000001.SH"],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [100_000],
            "amount": [10_000_000.0],
        },
        index=pd.DatetimeIndex([timestamp], name="datetime"),
    )

    featured = compute_causal_market_features(
        stock,
        index,
        history_days=3,
        min_history_days=1,
    )

    assert featured.iloc[0]["market_residual_bps"] == pytest.approx(
        10_000 * log(20.02 / 20.0)
    )


def test_variants_have_a_fixed_order_and_preregistered_thresholds():
    variants = build_variants()

    assert [item.name for item in variants] == [
        "baseline_time_window",
        "rv_15_25",
        "directional_move_0_100",
        "max_z_2_25",
        "market_residual",
        "residual_regime",
    ]
    assert variants[0].params.morning_entry_start.isoformat() == "10:00:00"
    assert variants[0].params.morning_entry_end.isoformat() == "10:30:00"
    assert variants[0].params.allow_afternoon_entries is False
    assert variants[3].params.max_entry_z == 2.25
    summaries = {item.name: item.summary for item in variants}
    assert "15" in summaries["rv_15_25"] and "25" in summaries["rv_15_25"]
    assert "0" in summaries["directional_move_0_100"]
    assert "100" in summaries["directional_move_0_100"]
    assert "2.25" in summaries["max_z_2_25"]
    assert "1" in summaries["market_residual"]
    assert "0.65" in summaries["residual_regime"]
    assert "4" in summaries["residual_regime"]


def test_baseline_gate_preserves_the_original_ready_mask():
    frame = pd.DataFrame({"ready": [True, False, True]})

    gate = apply_entry_gate(frame, _variant("baseline_time_window"))

    pd.testing.assert_series_equal(
        gate,
        pd.Series([True, False, True]),
        check_names=False,
    )


def test_realized_vol_gate_uses_an_inclusive_lower_and_exclusive_upper_bound():
    frame = pd.DataFrame(
        {
            "ready": [True, True, True, True, False],
            "realized_vol_bps": [14.999, 15.0, 24.999, 25.0, 20.0],
        }
    )

    gate = apply_entry_gate(frame, _variant("rv_15_25"))

    assert gate.tolist() == [False, True, True, False, False]


def test_directional_move_gate_uses_sign_z_times_session_return():
    frame = pd.DataFrame(
        {
            "ready": [True, True, True, True, False],
            "zscore": [-2.0, -2.0, 2.0, 2.0, -2.0],
            "session_return_bps": [0.0, 50.0, 99.999, 100.0, -50.0],
        }
    )

    gate = apply_entry_gate(frame, _variant("directional_move_0_100"))

    assert gate.tolist() == [True, False, True, False, False]


def test_max_z_gate_rejects_the_upper_boundary_and_preserves_ready():
    frame = pd.DataFrame(
        {
            "ready": [True, True, True, False],
            "zscore": [-2.249, 2.25, -2.5, 2.0],
        }
    )

    gate = apply_entry_gate(frame, _variant("max_z_2_25"))

    assert gate.tolist() == [True, False, False, False]


def test_market_residual_gate_requires_same_sign_and_one_sigma():
    frame = pd.DataFrame(
        {
            "ready": [True, True, True, True, False],
            "zscore": [-2.0, 2.0, -2.0, 2.0, -2.0],
            "residual_z": [-1.0, 1.0, 1.2, -0.999, -1.5],
        }
    )

    gate = apply_entry_gate(frame, _variant("market_residual"))

    assert gate.tolist() == [True, True, False, False, False]


def test_residual_regime_gate_only_uses_efficiency_and_jump_limits():
    frame = pd.DataFrame(
        {
            "ready": [True, True, True, False, True],
            "residual_path_efficiency": [0.65, 0.651, 0.5, 0.5, float("nan")],
            "relative_jump_score": [4.0, 1.0, 4.001, 2.0, 2.0],
        }
    )

    gate = apply_entry_gate(frame, _variant("residual_regime"))

    assert gate.tolist() == [True, False, False, False, False]


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
                "estimated_edge_bps": 100.0,
                "previous_price": close - 0.01,
                "realized_vol_bps": 30.0,
                "session_return_bps": 0.0,
                "ready": True,
            }
        )
    frame = pd.DataFrame(rows).set_index("datetime")
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [
        -2.0,
        -2.3,
    ]
    frame.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [
        -0.1,
        -2.0,
    ]
    return frame


def test_research_backtester_blocks_an_entry_when_the_variant_gate_is_false():
    frame = _featured_backtest_frame()
    baseline = _variant("baseline_time_window")
    config = BacktestConfig(
        initial_capital=100_000.0,
        base_quantities={"603629.SH": 2_000},
        params=baseline.params,
        max_bar_volume_fraction=0.1,
    )

    baseline_result = ResearchGateBacktester(baseline).run(frame, config)
    blocked_result = ResearchGateBacktester(_variant("rv_15_25")).run(frame, config)

    assert baseline_result["metrics"]["entry_count"] == 1
    assert blocked_result["metrics"]["entry_count"] == 0
    assert blocked_result["trades"] == []


def test_research_gate_does_not_block_an_active_pair_restoration():
    frame = _featured_backtest_frame()
    frame["residual_z"] = 0.0
    frame.loc[pd.Timestamp("2026-07-14 10:00"), "residual_z"] = -1.5
    variant = _variant("market_residual")
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


def test_cost_stress_keeps_the_nominal_decision_cost_and_changes_execution_only():
    frame = _featured_backtest_frame()
    frame["estimated_edge_bps"] = 35.0
    variant = _variant("baseline_time_window")
    execution_cost = CostModel(slippage_bps=5.0)
    adaptive = BacktestConfig(
        initial_capital=100_000.0,
        base_quantities={"603629.SH": 2_000},
        params=variant.params,
        cost=execution_cost,
        max_bar_volume_fraction=0.1,
    )
    frozen = BacktestConfig(
        initial_capital=100_000.0,
        base_quantities={"603629.SH": 2_000},
        params=variant.params,
        cost=execution_cost,
        decision_cost=CostModel(slippage_bps=2.0),
        max_bar_volume_fraction=0.1,
    )

    adaptive_result = ResearchGateBacktester(variant).run(frame, adaptive)
    frozen_result = ResearchGateBacktester(variant).run(frame, frozen)

    assert adaptive_result["metrics"]["entry_count"] == 0
    assert frozen_result["metrics"]["entry_count"] == 1
    assert frozen_result["trades"][0]["fill_price"] > frame.iloc[2]["open"]


def test_index_minute_normalizer_maps_physical_columns_to_market_contract():
    raw = pd.DataFrame(
        {
            "time": ["2026-07-14 09:31:00", "2026-07-14 09:32:00"],
            "symbol": ["000001.SH", "000001.SH"],
            "open": [3500.0, 3501.0],
            "high": [3501.0, 3502.0],
            "low": [3499.0, 3500.0],
            "close": [3500.5, 3501.5],
            "volume": [100_000, 120_000],
            "money": [350_050_000.0, 420_180_000.0],
        }
    )

    normalized = normalize_index_minute_data(raw)

    assert isinstance(normalized.index, pd.DatetimeIndex)
    assert normalized.index.name == "datetime"
    assert normalized.index.tolist() == list(pd.to_datetime(raw["time"]))
    assert "money" not in normalized.columns
    assert normalized["amount"].tolist() == raw["money"].tolist()
    assert normalized["symbol"].tolist() == ["000001.SH", "000001.SH"]


def test_index_minute_normalizer_rejects_conflicting_duplicate_symbol_minutes():
    raw = pd.DataFrame(
        {
            "time": ["2026-07-14 09:31:00", "2026-07-14 09:31:00"],
            "symbol": ["000001.SH", "000001.SH"],
            "open": [3500.0, 3500.0],
            "high": [3501.0, 3501.0],
            "low": [3499.0, 3499.0],
            "close": [3500.5, 3509.5],
            "volume": [100_000, 100_000],
            "money": [350_050_000.0, 350_950_000.0],
        }
    )

    with pytest.raises(ValueError, match="duplicate"):
        normalize_index_minute_data(raw)


def test_complete_panel_rejects_240_rows_with_a_wrong_session_minute():
    trade_date = date(2026, 7, 14)
    minutes = list(pd.date_range(f"{trade_date} 09:31", f"{trade_date} 11:30", freq="1min"))
    minutes += list(pd.date_range(f"{trade_date} 13:01", f"{trade_date} 15:00", freq="1min"))

    def panel(symbols: tuple[str, ...]) -> pd.DataFrame:
        rows = []
        for symbol in symbols:
            for timestamp in minutes:
                rows.append(
                    {
                        "datetime": timestamp,
                        "symbol": symbol,
                        "open": 10.0,
                        "high": 10.1,
                        "low": 9.9,
                        "close": 10.0,
                        "volume": 10_000,
                        "amount": 100_000.0,
                    }
                )
        return pd.DataFrame(rows).set_index("datetime")

    stocks = panel(("603629.SH", "688008.SH"))
    indexes = panel(("000001.SH", "000688.SH"))
    bad_timestamp = pd.Timestamp(f"{trade_date} 12:00")
    stock_rows = stocks.reset_index()
    stock_rows.loc[0, "datetime"] = bad_timestamp
    stocks = stock_rows.set_index("datetime")

    with pytest.raises(ValueError, match="session minute grid"):
        validate_complete_panel(
            stocks,
            indexes,
            stock_symbols=("603629.SH", "688008.SH"),
            benchmark_symbols=("000001.SH", "000688.SH"),
        )


def test_calendar_coverage_rejects_a_jointly_missing_trade_date():
    with pytest.raises(ValueError, match="calendar"):
        validate_calendar_coverage(
            observed_dates=_dates(3),
            expected_dates=_dates(4),
        )


def test_research_fingerprints_change_with_open_amount_and_limit_prices():
    frame = _featured_backtest_frame()
    original = frame_fingerprint(frame)
    changed = frame.copy()
    changed.iloc[0, changed.columns.get_loc("open")] += 0.01
    changed.iloc[0, changed.columns.get_loc("amount")] += 100.0

    assert frame_fingerprint(changed) != original
    first_limits = {"603629.SH|2026-07-14": {"up": 22.0, "down": 18.0}}
    second_limits = {"603629.SH|2026-07-14": {"up": 22.01, "down": 18.0}}
    assert limit_price_fingerprint(first_limits) != limit_price_fingerprint(second_limits)


def test_research_base_quantities_must_fix_both_symbols():
    with pytest.raises(ValueError, match="exactly"):
        validate_research_base_quantities({"603629.SH": 2_000})
    with pytest.raises(ValueError, match="positive"):
        validate_research_base_quantities({"603629.SH": 2_000, "688008.SH": 0})

    assert validate_research_base_quantities(DEFAULT_BASE_QUANTITIES) == DEFAULT_BASE_QUANTITIES


def test_artifact_json_commits_the_runs_csv_hash(tmp_path):
    report = {
        "runs": [
            {
                "fold": "fold_01",
                "sample": "retrospective_test",
                "variant": "baseline_time_window",
                "scenario": "nominal",
                "period_start": "2025-01-01",
                "period_end": "2025-01-02",
                "trade_days": 2,
                "bars": 960,
                "metrics": {"net_t_pnl": -1.0},
            }
        ]
    }

    paths = write_artifacts(report, tmp_path)

    csv_hash = hashlib.sha256((tmp_path / "runs.csv").read_bytes()).hexdigest()
    persisted = json.loads((tmp_path / "research.json").read_text(encoding="utf-8"))
    assert persisted["artifact_integrity"] == {
        "commit_marker": "research.json",
        "runs_csv_sha256": csv_hash,
    }
    assert paths["json"].endswith("research.json")


def test_v3_recommendation_is_always_research_only_and_never_auto_promotes():
    recommendation = build_recommendation(
        [
            {
                "fold": "fold_01",
                "variant": "residual_regime",
                "scenario": "nominal",
                "metrics": {
                    "net_t_pnl": 1_000_000.0,
                    "completed_pairs": 10_000,
                    "open_pairs_at_end": 0,
                },
            }
        ]
    )

    assert recommendation["decision"] == "research_only"
    assert recommendation["auto_promoted"] is False


def test_retrospective_screen_rejects_too_few_pairs_and_single_symbol_profit():
    def run(fold: str, variant: str, scenario: str, pnl: float) -> dict:
        return {
            "fold": fold,
            "variant": variant,
            "scenario": scenario,
                "metrics": {
                    "net_t_pnl": pnl,
                    "net_pnl_without_best_pair": pnl - 10.0,
                    "completed_pairs": 25,
                    "open_pairs_at_end": 0,
                    "restoration_failures": 0,
                    "restoration_rate": 1.0,
            },
            "symbol_summaries": [
                {"symbol": "603629.SH", "completed_pairs": 13, "net_pnl": -1.0},
                {"symbol": "688008.SH", "completed_pairs": 12, "net_pnl": pnl + 1.0},
            ],
            "direction_metrics": {
                "POSITIVE": {"completed_pairs": 12, "net_pnl": pnl / 2},
                "REVERSE": {"completed_pairs": 13, "net_pnl": pnl / 2},
            },
            "exit_reasons": {
                "mean_reversion_exit": {"count": 12, "net_pnl": 2 * pnl},
                "risk_restore": {"count": 13, "net_pnl": -pnl},
            },
        }

    runs = []
    for fold in ("fold_01", "fold_02", "fold_03"):
        runs.extend(
            [
                run(fold, "baseline_time_window", "nominal", -100.0),
                run(fold, "directional_move_0_100", "nominal", 100.0),
                run(fold, "directional_move_0_100", "slippage_5bp", 50.0),
                run(fold, "directional_move_0_100", "slippage_10bp", -10.0),
                run(fold, "directional_move_0_100", "participation_2_5pct", 25.0),
            ]
        )

    screen = build_recommendation(runs)["screens"]["directional_move_0_100"]

    assert screen["completed_pairs"] == 75
    assert screen["minimum_pairs_required"] == 80
    assert screen["all_symbols_positive"] is False
    assert screen["slippage_10bp_net_pnl"] == -30.0
    assert screen["participation_2_5pct_net_pnl"] == 75.0
    assert screen["participation_pair_retention"] == 1.0
    assert screen["minimum_participation_pair_retention"] == 0.8
    assert screen["restoration_safety_passed"] is True
    assert screen["retrospective_screen_passed"] is False

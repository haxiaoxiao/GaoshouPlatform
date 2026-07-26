from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from app.scripts.research_intraday_t_v5 import (
    BENCHMARK_MAP,
    DEFAULT_BASE_QUANTITIES,
    ResearchGateBacktester,
    apply_entry_gate,
    attach_sentiment_features,
    build_recommendation,
    build_stress_scenarios,
    build_variants,
    compute_causal_sentiment_features,
    load_market_sentiment_daily,
    market_sentiment_fingerprint,
    validate_sentiment_panel,
    write_artifacts,
)
from app.services.intraday_t_backtest import BacktestConfig


def _dates(count: int) -> list[date]:
    start = date(2025, 1, 2)
    return [start + timedelta(days=offset) for offset in range(count)]


def _variant(name: str):
    return next(item for item in build_variants() if item.name == name)


def _daily_sentiment(count: int = 48) -> pd.DataFrame:
    rows = []
    for offset, trade_date in enumerate(_dates(count)):
        market_locked_up = 20 + offset % 9
        market_locked_down = 3 + (offset * 2) % 7
        market_touched_up = market_locked_up + 4 + offset % 5
        main_locked_up = 14 + offset % 6
        main_locked_down = 1 + offset % 4
        main_touched_up = main_locked_up + 2 + offset % 3
        twenty_locked_up = 2 + offset % 4
        twenty_locked_down = offset % 2
        twenty_touched_up = twenty_locked_up + 1 + offset % 2
        promotion_eligible = 45 + offset % 13
        promotion_touched = 8 + offset % 7
        promotion_at_limit = 4 + offset % 5
        rows.append(
            {
                "trade_date": trade_date,
                "snapshot_time": "10:00:00",
                "market_covered": 5_000 + offset % 10,
                "market_locked_up": market_locked_up,
                "market_locked_down": market_locked_down,
                "market_touched_up": market_touched_up,
                "market_broken_up": market_touched_up - market_locked_up,
                "main_covered": 3_000,
                "main_locked_up": main_locked_up,
                "main_locked_down": main_locked_down,
                "main_touched_up": main_touched_up,
                "main_broken_up": main_touched_up - main_locked_up,
                "twenty_covered": 1_900,
                "twenty_locked_up": twenty_locked_up,
                "twenty_locked_down": twenty_locked_down,
                "twenty_touched_up": twenty_touched_up,
                "twenty_broken_up": twenty_touched_up - twenty_locked_up,
                "board_source_conflicts": 0,
                "promotion_eligible": promotion_eligible,
                "promotion_observed": promotion_eligible,
                "promotion_touched": promotion_touched,
                "promotion_at_limit": promotion_at_limit,
                "p12_eligible": 32 + offset % 8,
                "p12_observed": 32 + offset % 8,
                "p12_touched": 5 + offset % 4,
                "p12_at_limit": 3 + offset % 3,
                "p23_eligible": 8 + offset % 4,
                "p23_observed": 8 + offset % 4,
                "p23_touched": 2 + offset % 3,
                "p23_at_limit": 1 + offset % 2,
                "p3plus_eligible": 5 + offset % 3,
                "p3plus_observed": 5 + offset % 3,
                "p3plus_touched": 1 + offset % 2,
                "p3plus_at_limit": offset % 2,
            }
        )
    return pd.DataFrame(rows).set_index("trade_date")


def test_v5_scope_and_variants_are_fixed_to_two_stocks_and_three_sentiment_gates():
    assert BENCHMARK_MAP == {
        "603629.SH": "000001.SH",
        "688008.SH": "000688.SH",
    }
    assert DEFAULT_BASE_QUANTITIES == {"603629.SH": 2_000, "688008.SH": 1_000}
    assert [item.name for item in build_variants()] == [
        "directional_move_0_100",
        "volume_return_forecast",
        "volume_limit_breadth_alignment",
        "volume_board_promotion_alignment",
        "volume_composite_market_sentiment",
    ]
    assert all(item.params.morning_entry_start == time(10, 0) for item in build_variants())
    assert all(item.params.morning_entry_end == time(10, 30) for item in build_variants())


def test_stress_scenarios_reuse_the_v4_cost_and_capacity_matrix():
    scenarios = build_stress_scenarios()

    assert [(item.name, item.slippage_bps) for item in scenarios] == [
        ("nominal", 2.0),
        ("slippage_5bp", 5.0),
        ("slippage_10bp", 10.0),
        ("participation_2_5pct", 2.0),
    ]
    assert scenarios[-1].max_bar_volume_fraction == 0.025


def test_validate_sentiment_panel_checks_point_in_time_accounting_and_full_dates():
    frame = _daily_sentiment(3)
    quality = validate_sentiment_panel(frame, expected_dates=_dates(3))

    assert quality["trade_days"] == 3
    assert quality["snapshot_time"] == "10:00:00"

    broken = frame.copy()
    broken.iloc[0, broken.columns.get_loc("market_broken_up")] += 1
    with pytest.raises(ValueError, match="broken-up accounting"):
        validate_sentiment_panel(broken, expected_dates=_dates(3))

    with pytest.raises(ValueError, match="missing sentiment dates"):
        validate_sentiment_panel(frame.iloc[:-1], expected_dates=_dates(3))

    collapsed = _daily_sentiment(10)
    collapsed.iloc[-1, collapsed.columns.get_loc("market_covered")] = 100
    with pytest.raises(ValueError, match="coverage collapsed"):
        validate_sentiment_panel(collapsed)


def test_causal_sentiment_features_use_smoothed_rates_and_past_only_history():
    raw = _daily_sentiment()
    featured = compute_causal_sentiment_features(
        raw,
        history_days=20,
        min_history_days=15,
    )
    final_date = raw.index[-1]

    assert featured.loc[final_date, "promotion_at_limit_rate"] == pytest.approx(
        (raw.loc[final_date, "promotion_at_limit"] + 0.5)
        / (raw.loc[final_date, "promotion_eligible"] + 1.0)
    )
    assert featured.loc[final_date, "p12_at_limit_rate"] == pytest.approx(
        (raw.loc[final_date, "p12_at_limit"] + 0.5)
        / (raw.loc[final_date, "p12_eligible"] + 1.0)
    )
    assert np.isfinite(featured.loc[final_date, "market_limit_breadth_z"])
    assert np.isfinite(featured.loc[final_date, "promotion_at_limit_rate_z"])
    assert np.isfinite(featured.loc[final_date, "main_composite_sentiment_z"])
    assert np.isfinite(featured.loc[final_date, "twenty_composite_sentiment_z"])

    changed = raw.copy()
    changed.loc[final_date, "market_locked_up"] = 500
    changed.loc[final_date, "market_touched_up"] = 510
    changed.loc[final_date, "market_broken_up"] = 10
    changed_featured = compute_causal_sentiment_features(
        changed,
        history_days=20,
        min_history_days=15,
    )
    pd.testing.assert_frame_equal(
        featured.iloc[:-1],
        changed_featured.iloc[:-1],
        check_dtype=False,
    )
    assert featured.loc[final_date, "market_limit_breadth_location"] == pytest.approx(
        changed_featured.loc[final_date, "market_limit_breadth_location"]
    )


def test_zero_mad_and_insufficient_history_fail_closed():
    raw = _daily_sentiment(45)
    for column in (
        "market_locked_up",
        "market_locked_down",
        "market_touched_up",
        "market_broken_up",
        "main_locked_up",
        "main_locked_down",
        "main_touched_up",
        "main_broken_up",
        "twenty_locked_up",
        "twenty_locked_down",
        "twenty_touched_up",
        "twenty_broken_up",
        "promotion_eligible",
        "promotion_observed",
        "promotion_touched",
        "promotion_at_limit",
    ):
        raw[column] = raw[column].iloc[0]
    featured = compute_causal_sentiment_features(raw, history_days=40, min_history_days=40)

    assert featured.iloc[:40]["market_limit_breadth_z"].isna().all()
    assert pd.isna(featured.iloc[-1]["market_limit_breadth_z"])
    assert pd.isna(featured.iloc[-1]["promotion_at_limit_rate_z"])
    assert pd.isna(featured.iloc[-1]["main_composite_sentiment_z"])
    assert pd.isna(featured.iloc[-1]["twenty_composite_sentiment_z"])


def test_incomplete_board_cohort_makes_promotion_rates_unavailable():
    raw = _daily_sentiment(48)
    final_date = raw.index[-1]
    raw.loc[final_date, "promotion_observed"] -= 1
    raw.loc[final_date, "p12_observed"] -= 1

    featured = compute_causal_sentiment_features(raw, history_days=20, min_history_days=15)

    assert pd.isna(featured.loc[final_date, "promotion_at_limit_rate"])
    assert pd.isna(featured.loc[final_date, "p12_at_limit_rate"])


def test_attach_sentiment_features_respects_10am_availability_and_symbol_segment():
    raw = _daily_sentiment(48)
    featured = compute_causal_sentiment_features(raw, history_days=20, min_history_days=15)
    trade_date = raw.index[-1]
    rows = []
    for symbol in BENCHMARK_MAP:
        for minute in ("09:59", "10:00", "10:01"):
            rows.append(
                {
                    "datetime": pd.Timestamp(f"{trade_date.isoformat()} {minute}"),
                    "symbol": symbol,
                    "ready": True,
                    "zscore": -2.0,
                    "session_return_bps": -20.0,
                }
            )
    minute = pd.DataFrame(rows).set_index("datetime")

    attached = attach_sentiment_features(minute, featured)

    assert attached.loc[attached.index.time < time(10, 0), "sentiment_ready"].eq(False).all()
    assert attached.loc[attached.index.time >= time(10, 0), "sentiment_ready"].eq(True).all()
    main = attached.loc[
        (attached["symbol"] == "603629.SH") & (attached.index.time == time(10, 0))
    ].iloc[0]
    twenty = attached.loc[
        (attached["symbol"] == "688008.SH") & (attached.index.time == time(10, 0))
    ].iloc[0]
    assert main["segment_limit_breadth_z"] == pytest.approx(
        featured.loc[trade_date, "main_limit_breadth_z"]
    )
    assert twenty["segment_limit_breadth_z"] == pytest.approx(
        featured.loc[trade_date, "twenty_limit_breadth_z"]
    )


@pytest.mark.parametrize(
    ("variant_name", "feature_name"),
    [
        ("volume_limit_breadth_alignment", "market_limit_breadth_z"),
        ("volume_board_promotion_alignment", "promotion_at_limit_rate_z"),
        ("volume_composite_market_sentiment", "composite_sentiment_z"),
    ],
)
def test_directional_sentiment_gates_block_cold_positive_t_and_hot_reverse_t(
    variant_name: str,
    feature_name: str,
):
    frame = pd.DataFrame(
        {
            "ready": [True] * 6,
            "sentiment_ready": [True, True, True, True, True, False],
            "zscore": [-2.0, -2.0, 2.0, 2.0, -2.0, -2.0],
            "session_return_bps": [-50.0, -50.0, 50.0, 50.0, -50.0, -50.0],
            "volume_return_forecast_bps": [1.0, 1.0, -1.0, -1.0, 1.0, 1.0],
            feature_name: [-1.5, -1.500001, 1.5, 1.500001, np.nan, -1.0],
        }
    )

    gate = apply_entry_gate(frame, _variant(variant_name))

    assert gate.tolist() == [True, False, True, False, False, False]


def test_anchor_is_unchanged_when_sentiment_is_missing_and_new_gates_keep_the_anchor():
    frame = pd.DataFrame(
        {
            "ready": [True, True],
            "sentiment_ready": [False, True],
            "zscore": [-2.0, 2.0],
            "session_return_bps": [-50.0, 100.0],
            "volume_return_forecast_bps": [1.0, -1.0],
            "market_limit_breadth_z": [np.nan, -5.0],
            "promotion_at_limit_rate_z": [np.nan, -5.0],
            "composite_sentiment_z": [np.nan, -5.0],
        }
    )

    assert apply_entry_gate(frame, _variant("directional_move_0_100")).tolist() == [True, False]
    assert apply_entry_gate(frame, _variant("volume_return_forecast")).tolist() == [True, False]
    for name in (
        "volume_limit_breadth_alignment",
        "volume_board_promotion_alignment",
        "volume_composite_market_sentiment",
    ):
        assert apply_entry_gate(frame, _variant(name)).tolist() == [False, False]


def _featured_backtest_frame() -> pd.DataFrame:
    rows = []
    for offset, minute in enumerate(("09:59", "10:00", "10:01", "10:02", "15:00")):
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
                "sentiment_ready": True,
                "market_limit_breadth_z": 0.0,
                "volume_return_forecast_bps": np.nan,
            }
        )
    frame = pd.DataFrame(rows).set_index("datetime")
    frame.loc[pd.Timestamp("2026-07-14 10:00"), ["zscore", "previous_zscore"]] = [-2.0, -2.3]
    frame.loc[pd.Timestamp("2026-07-14 10:00"), "volume_return_forecast_bps"] = 1.0
    frame.loc[pd.Timestamp("2026-07-14 10:01"), ["zscore", "previous_zscore"]] = [-0.1, -2.0]
    frame.loc[pd.Timestamp("2026-07-14 10:01"), "market_limit_breadth_z"] = np.nan
    return frame


def test_sentiment_gate_never_blocks_an_active_pair_restoration():
    variant = _variant("volume_limit_breadth_alignment")
    result = ResearchGateBacktester(variant).run(
        _featured_backtest_frame(),
        BacktestConfig(
            initial_capital=100_000.0,
            base_quantities={"603629.SH": 2_000},
            params=variant.params,
            max_bar_volume_fraction=0.1,
        ),
    )

    assert result["metrics"]["entry_count"] == 1
    assert result["metrics"]["completed_pairs"] == 1
    assert [trade["leg"] for trade in result["trades"]] == ["entry", "restore"]


def _write_parquet(root: Path, name: str, frame: pd.DataFrame) -> None:
    target = root / name / "year=2025" / "month=01"
    target.mkdir(parents=True)
    frame.to_parquet(target / "part-test.parquet", index=False)


def _minute_rows(symbol: str, trade_date: date, *, up: float, last: float, high: float) -> list[dict]:
    rows = []
    for offset, timestamp in enumerate(
        pd.date_range(f"{trade_date.isoformat()} 09:31", periods=30, freq="min")
    ):
        close = last if offset == 29 else min(last, high - 0.1)
        rows.append(
            {
                "symbol": symbol,
                "datetime": timestamp,
                "open": close,
                "high": high if offset == 20 else close,
                "low": close,
                "close": close,
                "volume": 1_000.0,
                "amount": close * 1_000.0,
                "source": "test",
            }
        )
    rows.append(
        {
            "symbol": symbol,
            "datetime": pd.Timestamp(f"{trade_date.isoformat()} 10:01"),
            "open": up - 1.0,
            "high": up - 1.0,
            "low": up - 1.0,
            "close": up - 1.0,
            "volume": 1_000.0,
            "amount": (up - 1.0) * 1_000.0,
            "source": "test",
        }
    )
    return rows


def test_loader_builds_a_strict_10am_snapshot_and_board_advancement_from_prior_day(
    tmp_path: Path,
):
    prior = date(2025, 1, 3)
    current = date(2025, 1, 6)
    parquet_root = tmp_path / "parquet"
    rows = []
    rows += _minute_rows("603629.SH", current, up=11.0, last=9.5, high=9.8)
    rows += _minute_rows("688008.SH", current, up=12.0, last=8.0, high=8.5)
    rows += _minute_rows("600001.SH", current, up=11.0, last=11.0, high=11.0)
    rows += _minute_rows("300001.SZ", current, up=12.0, last=11.5, high=12.0)
    _write_parquet(parquet_root, "klines_minute", pd.DataFrame(rows))
    _write_parquet(
        parquet_root,
        "tushare_limit_list_d",
        pd.DataFrame(
            {
                "trade_date_dt": [prior, prior],
                "symbol": ["600001.SH", "603629.SH"],
                "limit": ["U", "U"],
                "limit_times": [1, 1],
            }
        ),
    )
    _write_parquet(
        parquet_root,
        "tushare_limit_step",
        pd.DataFrame(
            {
                "trade_date_dt": [prior, prior],
                "symbol": ["600001.SH", "300001.SZ"],
                "name": ["冲突样本", "二板样本"],
                "nums": ["2", "2"],
            }
        ),
    )
    database = tmp_path / "gaoshou.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE stock_limit_prices ("
            "symbol TEXT NOT NULL, trade_date DATE NOT NULL, "
            "up_limit REAL, down_limit REAL, PRIMARY KEY(symbol, trade_date))"
        )
        connection.executemany(
            "INSERT INTO stock_limit_prices VALUES (?, ?, ?, ?)",
            [
                ("603629.SH", prior.isoformat(), 11.0, 9.0),
                ("603629.SH", current.isoformat(), 11.0, 9.0),
                ("688008.SH", current.isoformat(), 12.0, 8.0),
                ("600001.SH", current.isoformat(), 11.0, 9.0),
                ("300001.SZ", current.isoformat(), 12.0, 8.0),
            ],
        )

    result = load_market_sentiment_daily(
        parquet_root=parquet_root,
        db_path=database,
        start_date=current,
        end_date=current,
        minute_source="test",
    )
    row = result.loc[current]

    assert row["market_covered"] == 4
    assert row["market_locked_up"] == 1
    assert row["market_locked_down"] == 1
    assert row["market_touched_up"] == 2
    assert row["market_broken_up"] == 1
    assert row["main_locked_up"] == 1
    assert row["twenty_locked_down"] == 1
    assert row["board_source_conflicts"] == 1
    assert row["promotion_eligible"] == 3
    assert row["promotion_observed"] == 3
    assert row["promotion_touched"] == 2
    assert row["promotion_at_limit"] == 1
    assert row["p12_eligible"] == 1
    assert row["p12_at_limit"] == 0
    assert row["p23_eligible"] == 2
    assert row["p23_at_limit"] == 1


def test_sentiment_fingerprint_is_order_stable_and_value_sensitive():
    frame = _daily_sentiment(3)
    expected = market_sentiment_fingerprint(frame)

    assert market_sentiment_fingerprint(frame.sample(frac=1, random_state=7)) == expected
    changed = frame.copy()
    changed.iloc[-1, changed.columns.get_loc("market_locked_up")] += 1
    assert market_sentiment_fingerprint(changed) != expected


def test_artifacts_include_the_daily_sentiment_csv_hash_and_replace_commit_marker_last(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    sentiment = compute_causal_sentiment_features(
        _daily_sentiment(48), history_days=20, min_history_days=15
    )
    report = {
        "runs": [
            {
                "fold": "fold_1",
                "sample": "retrospective_test",
                "variant": "directional_move_0_100",
                "scenario": "nominal",
                "period_start": "2025-01-01",
                "period_end": "2025-01-31",
                "trade_days": 20,
                "bars": 9_600,
                "signal_ledger_sha256": "abc",
                "metrics": {"net_t_pnl": 1.0},
            }
        ],
        "recommendation": {"decision": "research_only"},
    }

    replacements: list[str] = []
    original_replace = Path.replace

    def tracking_replace(source: Path, target: Path | str):
        replacements.append(Path(target).name)
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", tracking_replace)
    paths = write_artifacts(report, tmp_path, sentiment_daily=sentiment, signal_ledger=[])

    sentiment_path = Path(paths["market_sentiment_daily"])
    persisted = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
    assert sentiment_path.is_file()
    assert persisted["artifact_integrity"]["market_sentiment_daily_csv_sha256"] == hashlib.sha256(
        sentiment_path.read_bytes()
    ).hexdigest()
    assert persisted["recommendation"]["decision"] == "research_only"
    assert replacements[-1] == "research.json"
    assert replacements.count("research.json") == 1


def test_recommendation_can_never_promote_a_sentiment_variant():
    runs = []
    for variant in build_variants():
        for scenario in build_stress_scenarios():
            runs.append(
                {
                    "fold": "fold_1",
                    "variant": variant.name,
                    "scenario": scenario.name,
                    "metrics": {
                        "net_t_pnl": 1_000.0,
                        "completed_pairs": 100,
                        "open_pairs_at_end": 0,
                        "restoration_failures": 0,
                        "restoration_rate": 1.0,
                    },
                    "symbol_summaries": [],
                }
            )

    recommendation = build_recommendation(runs)

    assert recommendation["decision"] == "research_only"
    assert recommendation["auto_promoted"] is False
    assert recommendation["formal_forward_required"] is True

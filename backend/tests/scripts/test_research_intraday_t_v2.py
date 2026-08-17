from __future__ import annotations

import csv
import json
import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from app.scripts.research_intraday_t_v2 import (
    build_ablation_variants,
    build_chronological_folds,
    build_recommendation,
    build_stress_scenarios,
    load_limit_prices_sqlite,
    run_research,
    summarize_sample_coverage,
    write_research_artifacts,
)


def _dates(count: int) -> list[date]:
    start = date(2024, 1, 2)
    return [start + timedelta(days=offset) for offset in range(count)]


def test_chronological_folds_expand_training_without_train_test_overlap():
    folds = build_chronological_folds(
        _dates(14),
        min_train_days=6,
        test_days=3,
        holdout_days=2,
    )

    assert [fold.name for fold in folds] == ["fold_01", "fold_02", "holdout"]
    assert [len(fold.test_dates) for fold in folds] == [3, 3, 2]
    assert [fold.is_holdout for fold in folds] == [False, False, True]
    assert folds[0].train_dates == tuple(_dates(6))
    assert folds[1].train_dates == tuple(_dates(9))
    assert folds[-1].train_dates == tuple(_dates(12))
    assert all(set(fold.train_dates).isdisjoint(fold.test_dates) for fold in folds)
    assert all(max(fold.train_dates) < min(fold.test_dates) for fold in folds)
    assert set(folds[-1].test_dates).isdisjoint(
        date_value for fold in folds[:-1] for date_value in fold.test_dates
    )


def test_chronological_folds_reject_an_undersized_sample():
    with pytest.raises(ValueError, match="not enough trade dates"):
        build_chronological_folds(
            _dates(9),
            min_train_days=6,
            test_days=2,
            holdout_days=2,
        )


def test_chronological_folds_keep_a_short_final_development_test_block():
    folds = build_chronological_folds(
        _dates(13),
        min_train_days=6,
        test_days=3,
        holdout_days=2,
    )

    assert [len(fold.test_dates) for fold in folds] == [3, 2, 2]
    assert set(folds[0].test_dates + folds[1].test_dates + folds[2].test_dates) == set(
        _dates(13)[6:]
    )


def test_fixed_ablation_order_is_cumulative():
    variants = build_ablation_variants()

    assert [variant.name for variant in variants] == [
        "v1_compatible",
        "extreme_z_gate",
        "time_window_gate",
        "realized_vol_gate",
        "adverse_day_gate",
    ]
    assert variants[0].params.max_pairs_per_day == 2
    assert variants[0].params.cooldown_minutes == 10
    assert variants[0].params.min_realized_vol_bps == 0
    assert variants[1].params.max_entry_z == 2.4
    assert variants[1].params.min_realized_vol_bps == 0
    assert variants[2].params.morning_entry_start.isoformat() == "10:00:00"
    assert variants[2].params.morning_entry_end.isoformat() == "10:30:00"
    assert variants[2].params.allow_afternoon_entries is False
    assert variants[3].params.min_realized_vol_bps == 20.0
    assert variants[3].params.max_adverse_day_move_bps is None
    assert variants[4].params.max_adverse_day_move_bps == 50.0


def test_stress_scenarios_cover_cost_and_participation_cases():
    scenarios = build_stress_scenarios()

    assert [(item.name, item.slippage_bps, item.max_bar_volume_fraction) for item in scenarios] == [
        ("nominal", 2.0, 0.05),
        ("slippage_5bp", 5.0, 0.05),
        ("slippage_10bp", 10.0, 0.05),
        ("participation_2_5pct", 2.0, 0.025),
        ("participation_5pct", 2.0, 0.05),
    ]


def test_limit_price_loader_reads_exact_prices_from_sqlite(tmp_path):
    db_path = tmp_path / "limits.sqlite"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE stock_limit_prices ("
            "symbol TEXT, trade_date DATE, up_limit REAL, down_limit REAL)"
        )
        connection.executemany(
            "INSERT INTO stock_limit_prices VALUES (?, ?, ?, ?)",
            [
                ("603629.SH", "2024-01-02", 11.0, 9.0),
                ("688008.SH", "2024-01-02", 24.0, 16.0),
                ("603629.SH", "2024-02-01", 12.0, 10.0),
                ("000001.SZ", "2024-01-02", 11.0, 9.0),
            ],
        )

    prices = load_limit_prices_sqlite(
        db_path,
        symbols=("603629.SH", "688008.SH"),
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
    )

    assert prices == {
        "603629.SH|2024-01-02": {"up": 11.0, "down": 9.0},
        "688008.SH|2024-01-02": {"up": 24.0, "down": 16.0},
    }


def test_artifact_writer_emits_json_and_flat_run_csv(tmp_path):
    report = {
        "schema_version": 1,
        "coverage": {"bars": 200, "trade_days": 10},
        "recommendation": {"decision": "do_not_promote", "auto_promoted": False},
        "runs": [
            {
                "kind": "ablation",
                "fold": "fold_01",
                "sample": "test",
                "variant": "realized_vol_gate",
                "scenario": "nominal",
                "period_start": "2024-01-02",
                "period_end": "2024-01-12",
                "bars": 100,
                "trade_days": 5,
                "metrics": {
                    "net_t_pnl": 123.4,
                    "completed_pairs": 4,
                    "restoration_failures": 0,
                },
            }
        ],
    }

    paths = write_research_artifacts(report, tmp_path)

    assert json.loads(paths.json_path.read_text(encoding="utf-8")) == report
    with paths.csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["variant"] == "realized_vol_gate"
    assert rows[0]["net_t_pnl"] == "123.4"
    assert rows[0]["restoration_failures"] == "0"


def test_sample_coverage_reports_symbol_gaps_and_common_days():
    values = []
    for day_offset in range(4):
        day = date(2024, 1, 2) + timedelta(days=day_offset)
        values.append(
            {"datetime": datetime.combine(day, datetime.min.time()), "symbol": "603629.SH"}
        )
        if day_offset != 2:
            values.append(
                {"datetime": datetime.combine(day, datetime.min.time()), "symbol": "688008.SH"}
            )
    frame = pd.DataFrame(values).set_index("datetime")

    coverage = summarize_sample_coverage(
        frame,
        symbols=("603629.SH", "688008.SH"),
        requested_start=date(2024, 1, 2),
        requested_end=date(2024, 1, 5),
    )

    assert coverage["bars"] == 7
    assert coverage["trade_days"] == 4
    assert coverage["common_trade_days"] == 3
    assert coverage["symbols"]["688008.SH"]["missing_trade_days"] == ["2024-01-04"]
    assert coverage["symbols"]["688008.SH"]["longest_missing_streak"] == 1
    assert coverage["warnings"]


def test_sample_coverage_reports_calendar_wide_and_intraday_gaps():
    rows = []
    first = date(2024, 1, 2)
    third = date(2024, 1, 4)
    for day in (first, third):
        for symbol in ("603629.SH", "688008.SH"):
            for offset in range(3 if day == first else 240):
                rows.append(
                    {
                        "datetime": datetime.combine(day, datetime.min.time())
                        + timedelta(minutes=offset),
                        "symbol": symbol,
                    }
                )
    frame = pd.DataFrame(rows).set_index("datetime")

    coverage = summarize_sample_coverage(
        frame,
        symbols=("603629.SH", "688008.SH"),
        requested_start=first,
        requested_end=date(2024, 1, 5),
        expected_trade_dates=(first, date(2024, 1, 3), third),
    )

    assert coverage["missing_expected_trade_days"] == ["2024-01-03"]
    assert coverage["incomplete_symbol_days"] == 2
    assert coverage["observed_end_lag_calendar_days"] == 1
    assert any("calendar" in warning for warning in coverage["warnings"])
    assert any("intraday" in warning for warning in coverage["warnings"])


def _nominal_run(fold: str, variant: str, pnl: float, unresolved: int = 0) -> dict:
    return {
        "kind": "ablation",
        "fold": fold,
        "sample": "test",
        "variant": variant,
        "scenario": "nominal",
        "metrics": {
            "net_t_pnl": pnl,
            "restoration_failures": unresolved,
            "open_pairs_at_end": unresolved,
        },
    }


def test_recommendation_requires_fold_consistency_holdout_and_zero_open_pairs():
    runs = [
        _nominal_run("fold_01", "v1_compatible", -10),
        _nominal_run("fold_01", "realized_vol_gate", 5),
        _nominal_run("fold_02", "v1_compatible", -8),
        _nominal_run("fold_02", "realized_vol_gate", 4),
        _nominal_run("holdout", "v1_compatible", -3),
        _nominal_run("holdout", "realized_vol_gate", 2),
    ]

    recommendation = build_recommendation(runs)

    assert recommendation["decision"] == "eligible_for_manual_review"
    assert recommendation["auto_promoted"] is False
    assert recommendation["fold_improvements"] == "2/2"
    assert recommendation["holdout_improved"] is True
    assert recommendation["zero_unresolved_final_pairs"] is True

    runs[-1]["metrics"]["open_pairs_at_end"] = 1
    rejected = build_recommendation(runs)
    assert rejected["decision"] == "do_not_promote"
    assert rejected["zero_unresolved_final_pairs"] is False

    coverage_rejected = build_recommendation(
        runs,
        coverage={
            "missing_expected_trade_days": ["2024-01-03"],
            "incomplete_symbol_days": 0,
            "limit_prices": {"missing_symbol_days": 0},
        },
    )
    assert coverage_rejected["decision"] == "do_not_promote"
    assert coverage_rejected["coverage_ready"] is False


class _FakeStore:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame
        self.loads = []

    def load_minute(self, symbols, start, end, columns=None, timer_times=None):
        self.loads.append((tuple(symbols), start, end, columns, timer_times))
        return self.frame.copy()

    def load_trading_dates(self, symbols, start_date, end_date):
        return sorted(set(self.frame.index.date))


class _FakeBacktester:
    def __init__(self):
        self.calls = []

    def run(self, frame, config):
        self.calls.append((frame.copy(), config))
        days = sorted(set(frame.index.date))
        return {
            "period": {
                "start": frame.index.min().isoformat(),
                "end": frame.index.max().isoformat(),
                "trade_days": len(days),
                "bars": len(frame),
            },
            "metrics": {
                "net_t_pnl": float(config.params.min_realized_vol_bps),
                "completed_pairs": 1,
                "restoration_failures": 0,
                "open_pairs_at_end": 0,
            },
        }


def test_research_runner_loads_market_data_once_and_writes_fold_artifacts(tmp_path):
    rows = []
    for day in _dates(10):
        for symbol in ("603629.SH", "688008.SH"):
            rows.append(
                {
                    "datetime": datetime.combine(day, datetime.min.time()),
                    "symbol": symbol,
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.0,
                    "volume": 10_000,
                    "amount": 100_000.0,
                }
            )
    frame = pd.DataFrame(rows).set_index("datetime")
    store = _FakeStore(frame)
    backtester = _FakeBacktester()

    report = run_research(
        start_date=_dates(10)[0],
        end_date=_dates(10)[-1],
        min_train_days=4,
        test_days=2,
        holdout_days=2,
        output_dir=tmp_path,
        store=store,
        backtester=backtester,
    )

    assert len(store.loads) == 1
    assert store.loads[0][1] == datetime.combine(_dates(10)[0], datetime.min.time())
    assert store.loads[0][2] == datetime.combine(
        _dates(10)[-1] + timedelta(days=1), datetime.min.time()
    )
    assert store.loads[0][3] == [
        "symbol",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]
    assert report["coverage"]["trade_days"] == 10
    assert [fold["name"] for fold in report["folds"]] == ["fold_01", "fold_02", "holdout"]
    assert len(report["runs"]) == 45
    assert report["recommendation"]["auto_promoted"] is False
    assert all(config.require_exact_limit_prices for _, config in backtester.calls)
    assert (tmp_path / "research.json").exists()
    assert (tmp_path / "runs.csv").exists()

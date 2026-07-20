"""Read-only walk-forward research runner for the intraday-T v2 strategy."""

from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from math import isfinite
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.data_stores.base import MarketDataStore
from app.services.intraday_t_backtest import BacktestConfig, IntradayTBacktester
from app.services.intraday_t_strategy import SUPPORTED_SYMBOLS, CostModel, StrategyParams

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = ROOT / ".runtime" / "intraday-t-v2-research"


@dataclass(frozen=True)
class ChronologicalFold:
    name: str
    train_dates: tuple[date, ...]
    test_dates: tuple[date, ...]
    is_holdout: bool = False


@dataclass(frozen=True)
class AblationVariant:
    name: str
    params: StrategyParams
    changes: tuple[str, ...]


@dataclass(frozen=True)
class StressScenario:
    name: str
    slippage_bps: float
    max_bar_volume_fraction: float


@dataclass(frozen=True)
class ArtifactPaths:
    json_path: Path
    csv_path: Path


def build_chronological_folds(
    trade_dates: Sequence[date],
    *,
    min_train_days: int,
    test_days: int,
    holdout_days: int,
) -> list[ChronologicalFold]:
    """Build expanding folds and a final holdout from sorted unique trade dates."""
    if min_train_days <= 0 or test_days <= 0 or holdout_days <= 0:
        raise ValueError("fold day counts must be positive")
    dates = tuple(sorted(set(trade_dates)))
    required = min_train_days + test_days + holdout_days
    if len(dates) < required:
        raise ValueError(
            f"not enough trade dates: received {len(dates)}, require at least {required}"
        )

    development_end = len(dates) - holdout_days
    folds: list[ChronologicalFold] = []
    cursor = min_train_days
    sequence = 1
    while cursor < development_end:
        test_end = min(cursor + test_days, development_end)
        folds.append(
            ChronologicalFold(
                name=f"fold_{sequence:02d}",
                train_dates=dates[:cursor],
                test_dates=dates[cursor:test_end],
            )
        )
        cursor = test_end
        sequence += 1

    folds.append(
        ChronologicalFold(
            name="holdout",
            train_dates=dates[:development_end],
            test_dates=dates[development_end:],
            is_holdout=True,
        )
    )
    return folds


def build_ablation_variants() -> list[AblationVariant]:
    """Return the fixed, cumulative v1-to-v2 ablation sequence."""
    v1 = StrategyParams(
        max_entry_z=2.999999,
        min_realized_vol_bps=0.0,
        max_pairs_per_day=2,
        cooldown_minutes=10,
        morning_entry_start=time(9, 45),
        morning_entry_end=time(11, 21),
        allow_afternoon_entries=True,
        afternoon_entry_start=time(13, 5),
        afternoon_entry_end=time(14, 41),
    )
    extreme_z = replace(
        v1,
        max_entry_z=2.4,
        max_pairs_per_day=1,
        cooldown_minutes=20,
    )
    time_window = replace(
        extreme_z,
        morning_entry_start=time(10, 0),
        morning_entry_end=time(10, 30),
        allow_afternoon_entries=False,
    )
    realized_vol = replace(time_window, min_realized_vol_bps=20.0)
    adverse_day = replace(realized_vol, max_adverse_day_move_bps=50.0)
    return [
        AblationVariant("v1_compatible", v1, ()),
        AblationVariant(
            "extreme_z_gate",
            extreme_z,
            ("abs(zscore) < 2.4", "max one pair", "20 minute cooldown"),
        ),
        AblationVariant(
            "time_window_gate",
            time_window,
            ("10:00 <= entry time < 10:30", "no afternoon entries"),
        ),
        AblationVariant(
            "realized_vol_gate",
            realized_vol,
            ("realized volatility >= 20 bps",),
        ),
        AblationVariant(
            "adverse_day_gate",
            adverse_day,
            ("absolute session move < 50 bps",),
        ),
    ]


def build_stress_scenarios() -> list[StressScenario]:
    return [
        StressScenario("nominal", slippage_bps=2.0, max_bar_volume_fraction=0.05),
        StressScenario("slippage_5bp", slippage_bps=5.0, max_bar_volume_fraction=0.05),
        StressScenario("slippage_10bp", slippage_bps=10.0, max_bar_volume_fraction=0.05),
        StressScenario(
            "participation_2_5pct",
            slippage_bps=2.0,
            max_bar_volume_fraction=0.025,
        ),
        StressScenario("participation_5pct", slippage_bps=2.0, max_bar_volume_fraction=0.05),
    ]


def load_limit_prices_sqlite(
    db_path: str | Path,
    *,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
) -> dict[str, dict[str, float]]:
    """Load exact daily limits through a read-only SQLite connection."""
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"limit-price database does not exist: {path}")
    requested_symbols = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    if not requested_symbols:
        return {}
    placeholders = ", ".join("?" for _ in requested_symbols)
    query = (
        "SELECT symbol, trade_date, up_limit, down_limit "
        "FROM stock_limit_prices "
        f"WHERE symbol IN ({placeholders}) "
        "AND trade_date >= ? AND trade_date <= ? "
        "ORDER BY symbol, trade_date"
    )
    uri = f"{path.as_uri()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        rows = connection.execute(
            query,
            (*requested_symbols, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

    prices: dict[str, dict[str, float]] = {}
    for symbol, raw_trade_date, raw_up, raw_down in rows:
        if raw_up is None or raw_down is None:
            continue
        up = float(raw_up)
        down = float(raw_down)
        if not isfinite(up) or not isfinite(down) or up <= 0 or down <= 0:
            continue
        trade_date = date.fromisoformat(str(raw_trade_date)[:10])
        prices[f"{symbol}|{trade_date.isoformat()}"] = {"up": up, "down": down}
    return prices


def summarize_sample_coverage(
    minute_data: pd.DataFrame,
    *,
    symbols: Sequence[str],
    requested_start: date,
    requested_end: date,
    expected_trade_dates: Sequence[date] | None = None,
) -> dict[str, Any]:
    if minute_data.empty:
        raise ValueError("minute data is empty")
    if not isinstance(minute_data.index, pd.DatetimeIndex):
        raise ValueError("minute data requires a DatetimeIndex")
    if "symbol" not in minute_data.columns:
        raise ValueError("minute data requires symbol column")

    observed_dates = tuple(sorted(set(minute_data.index.date)))
    observed_set = set(observed_dates)
    expected_dates = tuple(sorted(set(expected_trade_dates or observed_dates)))
    missing_expected = [value for value in expected_dates if value not in observed_set]
    warnings: list[str] = []
    symbol_coverage: dict[str, dict[str, Any]] = {}
    date_sets: list[set[date]] = []
    for symbol in symbols:
        group = minute_data.loc[minute_data["symbol"].astype(str) == symbol]
        symbol_dates = set(group.index.date)
        date_sets.append(symbol_dates)
        missing = [value for value in observed_dates if value not in symbol_dates]
        if missing:
            warnings.append(f"{symbol} is missing {len(missing)} observed market days")
        symbol_coverage[symbol] = {
            "bars": len(group),
            "trade_days": len(symbol_dates),
            "start": group.index.min().isoformat() if not group.empty else None,
            "end": group.index.max().isoformat() if not group.empty else None,
            "missing_trade_days": [value.isoformat() for value in missing],
            "longest_missing_streak": _longest_missing_streak(observed_dates, symbol_dates),
        }

    common_dates = set.intersection(*date_sets) if date_sets else set()
    intraday_counts = (
        minute_data.assign(_trade_date=minute_data.index.date)
        .groupby(["symbol", "_trade_date"])
        .size()
    )
    incomplete_records = [
        {
            "symbol": str(symbol),
            "trade_date": trade_date.isoformat(),
            "bars": int(count),
        }
        for (symbol, trade_date), count in intraday_counts.items()
        if int(count) < 230
    ]
    end_lag = max(0, (requested_end - max(observed_dates)).days)
    if requested_end - requested_start < timedelta(days=730):
        warnings.append("requested window is shorter than two calendar years")
    if missing_expected:
        warnings.append(
            f"authoritative calendar has {len(missing_expected)} days with no minute data"
        )
    if incomplete_records:
        warnings.append(
            f"intraday coverage is incomplete for {len(incomplete_records)} symbol-days"
        )
    if end_lag:
        warnings.append(f"minute coverage ends {end_lag} calendar days before the requested end")
    return {
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "observed_start": min(observed_set).isoformat(),
        "observed_end": max(observed_set).isoformat(),
        "bars": len(minute_data),
        "trade_days": len(observed_dates),
        "common_trade_days": len(common_dates),
        "expected_trade_days": len(expected_dates),
        "missing_expected_trade_days": [value.isoformat() for value in missing_expected],
        "incomplete_symbol_days": len(incomplete_records),
        "incomplete_symbol_day_details": incomplete_records,
        "observed_end_lag_calendar_days": end_lag,
        "symbols": symbol_coverage,
        "warnings": warnings,
    }


def build_recommendation(
    runs: Sequence[dict[str, Any]],
    *,
    baseline: str = "v1_compatible",
    candidate: str = "realized_vol_gate",
    coverage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    nominal = {
        (str(run["fold"]), str(run["variant"])): run
        for run in runs
        if run.get("kind") == "ablation"
        and run.get("sample") == "test"
        and run.get("scenario") == "nominal"
        and run.get("variant") in {baseline, candidate}
    }
    fold_names = sorted(
        fold
        for fold, variant in nominal
        if fold != "holdout" and variant == candidate and (fold, baseline) in nominal
    )
    improvements = [
        _net_pnl(nominal[(fold, candidate)]) > _net_pnl(nominal[(fold, baseline)])
        for fold in fold_names
    ]
    holdout_available = ("holdout", baseline) in nominal and ("holdout", candidate) in nominal
    holdout_improved = holdout_available and (
        _net_pnl(nominal[("holdout", candidate)]) > _net_pnl(nominal[("holdout", baseline)])
    )
    candidate_runs = [
        run for run in runs if run.get("sample") == "test" and run.get("variant") == candidate
    ]
    zero_unresolved = bool(candidate_runs) and all(
        _unresolved_pairs(run.get("metrics", {})) == 0 for run in candidate_runs
    )
    nominal_candidate_runs = [
        run
        for run in candidate_runs
        if run.get("kind") == "ablation" and run.get("scenario") == "nominal"
    ]
    non_negative_nominal = bool(nominal_candidate_runs) and all(
        _net_pnl(run) >= 0 for run in nominal_candidate_runs
    )
    stress_runs = [run for run in candidate_runs if run.get("kind") == "stress"]
    stress_non_negative = not stress_runs or all(_net_pnl(run) >= 0 for run in stress_runs)
    fold_consistent = bool(fold_names) and all(improvements)
    coverage_ready = coverage is None or (
        not coverage.get("missing_expected_trade_days")
        and int(coverage.get("incomplete_symbol_days", 0) or 0) == 0
        and int((coverage.get("limit_prices") or {}).get("missing_symbol_days", 0) or 0) == 0
    )
    eligible = (
        fold_consistent
        and holdout_improved
        and zero_unresolved
        and non_negative_nominal
        and stress_non_negative
        and coverage_ready
    )

    reasons = [
        f"candidate improved on {sum(improvements)} of {len(improvements)} non-holdout folds",
        "holdout improved" if holdout_improved else "holdout did not improve",
        (
            "zero unresolved final pairs"
            if zero_unresolved
            else "one or more candidate runs ended with unresolved pairs"
        ),
        (
            "all nominal candidate folds were non-negative"
            if non_negative_nominal
            else "one or more nominal candidate folds were negative"
        ),
        (
            "all stress runs were non-negative"
            if stress_non_negative
            else "one or more stress runs were negative"
        ),
        (
            "coverage checks passed"
            if coverage_ready
            else "minute calendar, intraday, or limit-price coverage is incomplete"
        ),
    ]
    return {
        "decision": "eligible_for_manual_review" if eligible else "do_not_promote",
        "auto_promoted": False,
        "candidate": candidate,
        "baseline": baseline,
        "fold_improvements": f"{sum(improvements)}/{len(improvements)}",
        "holdout_improved": holdout_improved,
        "zero_unresolved_final_pairs": zero_unresolved,
        "non_negative_nominal_folds": non_negative_nominal,
        "stress_non_negative": stress_non_negative,
        "coverage_ready": coverage_ready,
        "reasons": reasons,
    }


def run_research(
    *,
    start_date: date,
    end_date: date,
    symbols: Sequence[str] = tuple(SUPPORTED_SYMBOLS),
    min_train_days: int = 252,
    test_days: int = 63,
    holdout_days: int = 63,
    initial_capital: float = 1_000_000.0,
    base_quantities: dict[str, int] | None = None,
    limit_prices: dict[str, dict[str, float]] | None = None,
    limit_price_db_path: str | Path | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    store: MarketDataStore | None = None,
    backtester: IntradayTBacktester | None = None,
) -> dict[str, Any]:
    """Execute fixed read-only ablations and stress tests, then write artifacts."""
    if start_date > end_date:
        raise ValueError("start_date must not exceed end_date")
    requested_symbols = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    unknown = set(requested_symbols) - set(SUPPORTED_SYMBOLS)
    if not requested_symbols or unknown:
        raise ValueError(f"unsupported symbols: {sorted(unknown)}")

    market_store = store or get_market_data_store()
    research_backtester = backtester or IntradayTBacktester()
    columns = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    minute_data = market_store.load_minute(
        requested_symbols,
        datetime.combine(start_date, time.min),
        datetime.combine(end_date + timedelta(days=1), time.min),
        columns=columns,
    )
    if minute_data.empty:
        raise ValueError("minute data is empty for requested research window")
    if not isinstance(minute_data.index, pd.DatetimeIndex):
        if "datetime" not in minute_data.columns:
            raise ValueError("minute data requires a DatetimeIndex or datetime column")
        minute_data = minute_data.set_index(pd.to_datetime(minute_data.pop("datetime")))
    minute_data = minute_data.sort_index(kind="stable")

    calendar_loader = getattr(market_store, "load_trading_dates", None)
    expected_trade_dates = (
        calendar_loader(requested_symbols, start_date, end_date)
        if callable(calendar_loader)
        else []
    )

    trade_dates = sorted(set(minute_data.index.date))
    folds = build_chronological_folds(
        trade_dates,
        min_train_days=min_train_days,
        test_days=test_days,
        holdout_days=holdout_days,
    )
    variants = build_ablation_variants()
    scenarios = build_stress_scenarios()
    if limit_prices is not None and limit_price_db_path is not None:
        raise ValueError("supply limit_prices or limit_price_db_path, not both")
    prices = (
        load_limit_prices_sqlite(
            limit_price_db_path,
            symbols=requested_symbols,
            start_date=start_date,
            end_date=end_date,
        )
        if limit_price_db_path is not None
        else (limit_prices or {})
    )
    runs: list[dict[str, Any]] = []
    nominal = scenarios[0]
    for fold in folds:
        samples = (("train", fold.train_dates), ("test", fold.test_dates))
        for sample_name, sample_dates in samples:
            sample = _slice_dates(minute_data, sample_dates)
            for variant in variants:
                runs.append(
                    _execute_run(
                        research_backtester,
                        sample,
                        fold=fold.name,
                        sample_name=sample_name,
                        kind="ablation",
                        variant=variant,
                        scenario=nominal,
                        initial_capital=initial_capital,
                        base_quantities=base_quantities or {},
                        limit_prices=prices,
                    )
                )

        test_sample = _slice_dates(minute_data, fold.test_dates)
        candidate = next(item for item in variants if item.name == "realized_vol_gate")
        for scenario in scenarios:
            runs.append(
                _execute_run(
                    research_backtester,
                    test_sample,
                    fold=fold.name,
                    sample_name="test",
                    kind="stress",
                    variant=candidate,
                    scenario=scenario,
                    initial_capital=initial_capital,
                    base_quantities=base_quantities or {},
                    limit_prices=prices,
                )
            )

    coverage = summarize_sample_coverage(
        minute_data,
        symbols=requested_symbols,
        requested_start=start_date,
        requested_end=end_date,
        expected_trade_dates=expected_trade_dates or None,
    )
    coverage["limit_prices"] = _summarize_limit_price_coverage(
        minute_data,
        symbols=requested_symbols,
        prices=prices,
    )
    if not prices:
        coverage["warnings"].append(
            "exact daily limit prices were not supplied; limit-price fill rejection was not active"
        )
    elif coverage["limit_prices"]["missing_symbol_days"]:
        missing = coverage["limit_prices"]["missing_symbol_days"]
        coverage["warnings"].append(
            f"exact daily limit prices are missing for {missing} observed symbol-days"
        )
    output = Path(output_dir)
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "protocol": {
            "fixed_before_run": True,
            "automatic_parameter_selection": False,
            "auto_promotion_enabled": False,
            "fold_method": "expanding_train_contiguous_test_final_holdout",
        },
        "symbols": list(requested_symbols),
        "coverage": coverage,
        "folds": [_fold_record(fold) for fold in folds],
        "ablations": [
            {"name": item.name, "changes": list(item.changes), "params": asdict(item.params)}
            for item in variants
        ],
        "stress_scenarios": [asdict(item) for item in scenarios],
        "runs": runs,
        "recommendation": build_recommendation(runs, coverage=coverage),
        "artifacts": {
            "json": str((output / "research.json").resolve()),
            "csv": str((output / "runs.csv").resolve()),
        },
    }
    write_research_artifacts(report, output)
    return report


def write_research_artifacts(
    report: dict[str, Any], output_dir: str | Path = DEFAULT_OUTPUT_DIR
) -> ArtifactPaths:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "research.json"
    csv_path = output / "runs.csv"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )

    metric_names = sorted(
        {str(name) for run in report.get("runs", []) for name in run.get("metrics", {})}
    )
    run_fields = [
        "kind",
        "fold",
        "sample",
        "variant",
        "scenario",
        "period_start",
        "period_end",
        "bars",
        "trade_days",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[*run_fields, *metric_names])
        writer.writeheader()
        for run in report.get("runs", []):
            row = {name: run.get(name) for name in run_fields}
            row.update({name: run.get("metrics", {}).get(name) for name in metric_names})
            writer.writerow(row)
    return ArtifactPaths(json_path=json_path, csv_path=csv_path)


def _json_default(value: Any) -> str:
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _longest_missing_streak(observed_dates: Sequence[date], symbol_dates: set[date]) -> int:
    longest = 0
    current = 0
    for value in observed_dates:
        if value in symbol_dates:
            current = 0
        else:
            current += 1
            longest = max(longest, current)
    return longest


def _summarize_limit_price_coverage(
    minute_data: pd.DataFrame,
    *,
    symbols: Sequence[str],
    prices: dict[str, dict[str, float]],
) -> dict[str, Any]:
    by_symbol: dict[str, dict[str, int]] = {}
    total_expected = 0
    total_available = 0
    for symbol in symbols:
        group = minute_data.loc[minute_data["symbol"].astype(str) == symbol]
        observed_dates = sorted(set(group.index.date))
        available = sum(
            f"{symbol}|{trade_date.isoformat()}" in prices for trade_date in observed_dates
        )
        expected = len(observed_dates)
        by_symbol[symbol] = {
            "observed_days": expected,
            "available_days": available,
            "missing_days": expected - available,
        }
        total_expected += expected
        total_available += available
    return {
        "expected_symbol_days": total_expected,
        "available_symbol_days": total_available,
        "missing_symbol_days": total_expected - total_available,
        "symbols": by_symbol,
    }


def _net_pnl(run: dict[str, Any]) -> float:
    metrics = run.get("metrics", {})
    return float(metrics.get("net_t_pnl", metrics.get("incremental_pnl", 0.0)) or 0.0)


def _unresolved_pairs(metrics: dict[str, Any]) -> int:
    value = metrics.get("open_pairs_at_end")
    if isinstance(value, list):
        return len(value)
    if value is not None:
        return int(value)
    return int(metrics.get("restoration_failures", 0) or 0)


def _slice_dates(frame: pd.DataFrame, dates: Sequence[date]) -> pd.DataFrame:
    selected = set(dates)
    result = frame.loc[[value in selected for value in frame.index.date]]
    if result.empty:
        raise ValueError("research fold contains no minute bars")
    return result


def _execute_run(
    backtester: IntradayTBacktester,
    frame: pd.DataFrame,
    *,
    fold: str,
    sample_name: str,
    kind: str,
    variant: AblationVariant,
    scenario: StressScenario,
    initial_capital: float,
    base_quantities: dict[str, int],
    limit_prices: dict[str, dict[str, float]],
) -> dict[str, Any]:
    cost = replace(CostModel(), slippage_bps=scenario.slippage_bps)
    result = backtester.run(
        frame,
        BacktestConfig(
            initial_capital=initial_capital,
            base_quantities=base_quantities,
            params=variant.params,
            cost=cost,
            max_bar_volume_fraction=scenario.max_bar_volume_fraction,
            limit_prices=limit_prices,
            require_exact_limit_prices=True,
        ),
    )
    trades = result.get("trades", [])
    restore_trades = [trade for trade in trades if trade.get("leg") == "restore"]
    exit_counts = Counter(str(trade.get("reason", "unknown")) for trade in restore_trades)
    exit_net_pnl: dict[str, float] = {}
    for trade in restore_trades:
        reason = str(trade.get("reason", "unknown"))
        exit_net_pnl[reason] = exit_net_pnl.get(reason, 0.0) + float(
            trade.get("net_pnl", 0.0) or 0.0
        )
    rejection_counts = Counter(
        str(rejection.get("reason", "unknown")) for rejection in result.get("rejections", [])
    )
    period = result["period"]
    return {
        "kind": kind,
        "fold": fold,
        "sample": sample_name,
        "variant": variant.name,
        "scenario": scenario.name,
        "period_start": period["start"],
        "period_end": period["end"],
        "bars": period["bars"],
        "trade_days": period["trade_days"],
        "params": asdict(variant.params),
        "cost": asdict(cost),
        "max_bar_volume_fraction": scenario.max_bar_volume_fraction,
        "metrics": result["metrics"],
        "direction_metrics": result.get("direction_metrics", {}),
        "symbol_summaries": result.get("symbol_summaries", []),
        "exit_reasons": {
            reason: {"count": count, "net_pnl": round(exit_net_pnl.get(reason, 0.0), 4)}
            for reason, count in sorted(exit_counts.items())
        },
        "rejections_by_reason": dict(sorted(rejection_counts.items())),
    }


def _fold_record(fold: ChronologicalFold) -> dict[str, Any]:
    return {
        "name": fold.name,
        "is_holdout": fold.is_holdout,
        "train_start": fold.train_dates[0].isoformat(),
        "train_end": fold.train_dates[-1].isoformat(),
        "train_days": len(fold.train_dates),
        "test_start": fold.test_dates[0].isoformat(),
        "test_end": fold.test_dates[-1].isoformat(),
        "test_days": len(fold.test_dates),
    }


def main() -> int:
    today = date.today()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-date", type=date.fromisoformat, default=today - timedelta(days=730)
    )
    parser.add_argument("--end-date", type=date.fromisoformat, default=today)
    parser.add_argument("--symbols", nargs="+", default=list(SUPPORTED_SYMBOLS))
    parser.add_argument("--min-train-days", type=int, default=252)
    parser.add_argument("--test-days", type=int, default=63)
    parser.add_argument("--holdout-days", type=int, default=63)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--limit-price-db",
        type=Path,
        default=settings.sqlite_db_path,
        help="read-only SQLite database containing stock_limit_prices",
    )
    args = parser.parse_args()
    report = run_research(
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
        min_train_days=args.min_train_days,
        test_days=args.test_days,
        holdout_days=args.holdout_days,
        limit_price_db_path=args.limit_price_db,
        output_dir=args.output_dir,
    )
    print(
        json.dumps(
            {
                "artifacts": report["artifacts"],
                "coverage": report["coverage"],
                "recommendation": report["recommendation"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

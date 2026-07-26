"""Causal two-stock intraday-T v4 research runner.

The runner is deliberately read-only and retrospective. It evaluates three
independent microstructure-inspired entry gates against the v3 directional anchor,
persists reproducible artifacts, and can never promote a strategy automatically.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
from collections import Counter
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, time, timedelta
from importlib.metadata import PackageNotFoundError, version
from math import pi
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

import numpy as np
import pandas as pd

from app.data_stores import get_market_data_store
from app.data_stores.base import MarketDataStore
from app.scripts.research_intraday_t_v2 import load_limit_prices_sqlite
from app.scripts.research_intraday_t_v3 import (
    MARKET_VALUE_COLUMNS,
    ResearchVariant,
    RetrospectiveFold,
    StressScenario,
    _json_default,
    _json_fingerprint,
    _mark_reference_breaks,
    _normalize_stock_minute_data,
    _reference_break_keys,
    _rolling_prior_median_mad,
    _sqlite_path_from_settings,
    _validate_market_values,
    build_retrospective_folds,
    compute_causal_market_features,
    frame_fingerprint,
    limit_price_fingerprint,
    load_index_minute_data,
    normalize_index_minute_data,
    validate_calendar_coverage,
    validate_complete_panel,
    validate_research_base_quantities,
)
from app.services.intraday_t_backtest import BacktestConfig, IntradayTBacktester
from app.services.intraday_t_strategy import CostModel, StrategyParams

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_START_DATE = date(2024, 7, 19)
DEFAULT_END_DATE = date(2026, 3, 13)
DEFAULT_OUTPUT_DIR = (
    ROOT
    / ".runtime"
    / "intraday-t-v4-research"
    / f"{DEFAULT_START_DATE.isoformat()}_{DEFAULT_END_DATE.isoformat()}"
)
DEFAULT_BASE_QUANTITIES = {"603629.SH": 2_000, "688008.SH": 1_000}
BENCHMARK_MAP = {
    "603629.SH": "000001.SH",
    "688008.SH": "000688.SH",
}
SIGNAL_LEDGER_FIELDS = (
    "pair_id",
    "symbol",
    "direction",
    "leg",
    "signal_at",
    "fill_at",
    "quantity",
    "reason",
)


def build_variants() -> list[ResearchVariant]:
    """Return the fixed anchor and three independent v4 gates."""
    baseline = StrategyParams(
        max_entry_z=2.4,
        min_realized_vol_bps=0.0,
        max_pairs_per_day=1,
        cooldown_minutes=20,
        morning_entry_start=time(10, 0),
        morning_entry_end=time(10, 30),
        allow_afternoon_entries=False,
    )
    return [
        ResearchVariant(
            "directional_move_0_100",
            "v3 anchor: 0 <= sign(z) * session return < 100 bps",
            baseline,
        ),
        ResearchVariant(
            "volume_return_forecast",
            "anchor plus past-only price-volume next-residual forecast",
            baseline,
        ),
        ResearchVariant(
            "idiosyncratic_jump_veto",
            "anchor excluding recent stock-only jumps",
            baseline,
        ),
        ResearchVariant(
            "amihud_impact",
            "anchor excluding unusually high residual price impact",
            baseline,
        ),
    ]


def build_stress_scenarios() -> list[StressScenario]:
    """Keep execution stresses fixed while decision costs remain nominal."""
    return [
        StressScenario("nominal", slippage_bps=2.0),
        StressScenario("slippage_5bp", slippage_bps=5.0),
        StressScenario("slippage_10bp", slippage_bps=10.0),
        StressScenario(
            "participation_2_5pct",
            slippage_bps=2.0,
            max_bar_volume_fraction=0.025,
        ),
    ]


def resolve_research_base_quantities(
    values: dict[str, int] | None,
) -> dict[str, int]:
    """Use defaults only when omitted; an explicit partial mapping fails closed."""
    candidate = dict(DEFAULT_BASE_QUANTITIES) if values is None else dict(values)
    return validate_research_base_quantities(candidate)


def _normalize_index_frame(frame: pd.DataFrame) -> pd.DataFrame:
    indexes = frame.copy()
    if not isinstance(indexes.index, pd.DatetimeIndex):
        if "datetime" not in indexes.columns:
            raise ValueError("index minute data requires a DatetimeIndex or datetime column")
        indexes.index = pd.to_datetime(indexes.pop("datetime"), errors="coerce")
    indexes.index.name = "datetime"
    required = {"symbol", *MARKET_VALUE_COLUMNS}
    missing = required - set(indexes.columns)
    if missing:
        raise ValueError(f"index minute data missing columns: {sorted(missing)}")
    if indexes.index.isna().any():
        raise ValueError("index minute data contains invalid timestamps")
    for column in MARKET_VALUE_COLUMNS:
        indexes[column] = pd.to_numeric(indexes[column], errors="coerce")
    _validate_market_values(indexes, label="index minute data")
    duplicate = pd.DataFrame(
        {"symbol": indexes["symbol"].astype(str).to_numpy(), "datetime": indexes.index}
    ).duplicated(keep=False)
    if duplicate.any():
        raise ValueError("index minute data contains duplicate symbol-minute bars")
    return indexes.sort_index(kind="stable")


def _segment_keys(index: pd.DatetimeIndex) -> tuple[pd.Series, pd.Series]:
    clock = pd.Series(index.time, index=index)
    morning = clock.between(time(9, 31), time(11, 30), inclusive="both")
    afternoon = clock.between(time(13, 1), time(15, 0), inclusive="both")
    if not bool((morning | afternoon).all()):
        raise ValueError("minute data contains bars outside continuous trading sessions")
    segment = pd.Series(np.where(morning, "AM", "PM"), index=index)
    day = pd.Series(index.date, index=index)
    return day, segment


def _segmented_log_returns_bps(
    close: pd.Series,
    open_price: pd.Series,
    day: pd.Series,
    segment: pd.Series,
) -> pd.Series:
    log_close = np.log(close)
    returns = log_close.groupby([day, segment]).diff()
    first = close.groupby([day, segment]).cumcount().eq(0)
    returns.loc[first] = log_close.loc[first] - np.log(open_price.loc[first])
    return returns * 10_000.0


def _past_bipower_scale(
    returns_bps: pd.Series,
    day: pd.Series,
    segment: pd.Series,
    *,
    window: int,
    min_observations: int,
) -> pd.Series:
    scale = pd.Series(np.nan, index=returns_bps.index, dtype=float)
    group_keys = pd.MultiIndex.from_arrays([day.to_numpy(), segment.to_numpy()])
    for _, positions in pd.Series(np.arange(len(returns_bps)), index=group_keys).groupby(
        level=[0, 1], sort=False
    ):
        labels = returns_bps.index[positions.to_numpy()]
        values = returns_bps.loc[labels].abs()
        prior_products = values.shift(1) * values.shift(2)
        bipower = prior_products.rolling(
            window,
            min_periods=min_observations,
        ).mean()
        local_scale = np.sqrt((pi / 2.0) * bipower)
        scale.loc[labels] = local_scale.where(local_scale > 1e-12)
    return scale


def _recent_boolean_state(
    values: pd.Series,
    day: pd.Series,
    segment: pd.Series,
    *,
    window: int,
) -> pd.Series:
    result = pd.Series(pd.NA, index=values.index, dtype="boolean")
    group_keys = pd.MultiIndex.from_arrays([day.to_numpy(), segment.to_numpy()])
    for _, positions in pd.Series(np.arange(len(values)), index=group_keys).groupby(
        level=[0, 1], sort=False
    ):
        labels = values.index[positions.to_numpy()]
        numeric = values.loc[labels].astype("Float64")
        recent = numeric.rolling(window, min_periods=window).max()
        valid = recent.notna()
        result.loc[labels[valid.to_numpy()]] = recent.loc[valid].gt(0).to_numpy()
    return result


def _segmented_rolling_median(
    values: pd.Series,
    day: pd.Series,
    segment: pd.Series,
    *,
    window: int,
) -> pd.Series:
    result = pd.Series(np.nan, index=values.index, dtype=float)
    group_keys = pd.MultiIndex.from_arrays([day.to_numpy(), segment.to_numpy()])
    for _, positions in pd.Series(np.arange(len(values)), index=group_keys).groupby(
        level=[0, 1], sort=False
    ):
        labels = values.index[positions.to_numpy()]
        result.loc[labels] = values.loc[labels].rolling(
            window,
            min_periods=window,
        ).median()
    return result


def _online_price_volume_forecast(
    residual_bps: pd.Series,
    interaction: pd.Series,
    day: pd.Series,
    segment: pd.Series,
    *,
    history_days: int,
    min_days: int,
) -> pd.DataFrame:
    target = residual_bps.groupby([day, segment]).shift(-1)
    result = pd.DataFrame(
        np.nan,
        index=residual_bps.index,
        columns=(
            "volume_return_forecast_bps",
            "forecast_intercept_bps",
            "forecast_return_beta",
            "forecast_price_volume_beta",
            "forecast_training_days",
            "forecast_training_samples",
        ),
        dtype=float,
    )
    trade_days = tuple(sorted(set(day)))
    finite = (
        np.isfinite(residual_bps)
        & np.isfinite(interaction)
        & np.isfinite(target)
    )
    for position, evaluation_day in enumerate(trade_days):
        prior_days = set(trade_days[max(0, position - history_days) : position])
        if not prior_days:
            continue
        training = day.isin(prior_days) & finite
        training_days = int(day.loc[training].nunique())
        training_samples = int(training.sum())
        if training_days < min_days or training_samples < 3:
            continue
        x = np.column_stack(
            (
                np.ones(training_samples),
                residual_bps.loc[training].to_numpy(dtype=float),
                interaction.loc[training].to_numpy(dtype=float),
            )
        )
        y = target.loc[training].to_numpy(dtype=float)
        coefficients, _, rank, _ = np.linalg.lstsq(x, y, rcond=None)
        if rank < 3 or not np.isfinite(coefficients).all():
            continue
        current = day.eq(evaluation_day)
        current_x = np.column_stack(
            (
                np.ones(int(current.sum())),
                residual_bps.loc[current].to_numpy(dtype=float),
                interaction.loc[current].to_numpy(dtype=float),
            )
        )
        forecast = current_x @ coefficients
        result.loc[current, "volume_return_forecast_bps"] = forecast
        result.loc[current, "forecast_intercept_bps"] = coefficients[0]
        result.loc[current, "forecast_return_beta"] = coefficients[1]
        result.loc[current, "forecast_price_volume_beta"] = coefficients[2]
        result.loc[current, "forecast_training_days"] = training_days
        result.loc[current, "forecast_training_samples"] = training_samples
    return result


def compute_causal_gate_features(
    stock_frame: pd.DataFrame,
    index_frame: pd.DataFrame,
    *,
    amount_history_days: int = 20,
    min_amount_history_days: int = 15,
    ols_history_days: int = 60,
    min_ols_days: int = 60,
    jump_window: int = 20,
    min_jump_observations: int = 10,
    jump_threshold: float = 4.0,
    recent_jump_minutes: int = 10,
    amihud_window: int = 5,
    amihud_history_days: int = 20,
    min_amihud_history_days: int = 15,
    params: StrategyParams | None = None,
) -> pd.DataFrame:
    """Add past-only v4 gate features to the production-equivalent strategy frame."""
    history_pairs = (
        (amount_history_days, min_amount_history_days, "amount"),
        (ols_history_days, min_ols_days, "OLS"),
        (amihud_history_days, min_amihud_history_days, "Amihud"),
    )
    for history, minimum, label in history_pairs:
        if not 1 <= minimum <= history:
            raise ValueError(f"minimum {label} history must be in [1, history_days]")
    if jump_window < 2 or not 1 <= min_jump_observations <= jump_window:
        raise ValueError("jump history settings are invalid")
    if jump_threshold <= 0 or recent_jump_minutes <= 0 or amihud_window <= 0:
        raise ValueError("jump thresholds must be positive")

    stocks = _normalize_stock_minute_data(stock_frame)
    indexes = _normalize_index_frame(index_frame)
    feature_params = params or build_variants()[0].params
    base = compute_causal_market_features(stocks, indexes, params=feature_params)
    groups: list[pd.DataFrame] = []
    for symbol, raw_group in base.groupby("symbol", sort=False):
        benchmark_symbol = BENCHMARK_MAP.get(str(symbol))
        if benchmark_symbol is None:
            raise ValueError(f"no fixed benchmark for {symbol}")
        group = raw_group.sort_index(kind="stable").copy()
        benchmark = indexes.loc[indexes["symbol"].astype(str).eq(benchmark_symbol)].reindex(
            group.index
        )
        if benchmark[list(MARKET_VALUE_COLUMNS)].isna().any().any():
            raise ValueError(f"benchmark alignment is incomplete for {symbol}")
        day, segment = _segment_keys(group.index)
        stock_close = pd.to_numeric(group["close"], errors="coerce")
        stock_open = pd.to_numeric(group["open"], errors="coerce")
        benchmark_close = pd.to_numeric(benchmark["close"], errors="coerce")
        benchmark_open = pd.to_numeric(benchmark["open"], errors="coerce")
        stock_return_bps = _segmented_log_returns_bps(
            stock_close, stock_open, day, segment
        )
        benchmark_return_bps = _segmented_log_returns_bps(
            benchmark_close, benchmark_open, day, segment
        )
        residual_bps = stock_return_bps - benchmark_return_bps
        slots = pd.Series(group.index.strftime("%H:%M"), index=group.index)

        log_amount = np.log1p(pd.to_numeric(group["amount"], errors="coerce"))
        amount_location, amount_mad = _rolling_prior_median_mad(
            log_amount,
            slots,
            history_days=amount_history_days,
            min_history_days=min_amount_history_days,
        )
        amount_scale = (1.4826 * amount_mad).where(amount_mad > 1e-12)
        amount_z = (log_amount - amount_location) / amount_scale
        interaction = amount_z * residual_bps
        forecasts = _online_price_volume_forecast(
            residual_bps,
            interaction,
            day,
            segment,
            history_days=ols_history_days,
            min_days=min_ols_days,
        )

        stock_jump_scale = _past_bipower_scale(
            stock_return_bps,
            day,
            segment,
            window=jump_window,
            min_observations=min_jump_observations,
        )
        benchmark_jump_scale = _past_bipower_scale(
            benchmark_return_bps,
            day,
            segment,
            window=jump_window,
            min_observations=min_jump_observations,
        )
        stock_jump_score = stock_return_bps.abs() / stock_jump_scale
        benchmark_jump_score = benchmark_return_bps.abs() / benchmark_jump_scale
        valid_jump = np.isfinite(stock_jump_score) & np.isfinite(benchmark_jump_score)
        idiosyncratic_jump = pd.Series(pd.NA, index=group.index, dtype="boolean")
        idiosyncratic_jump.loc[valid_jump] = (
            stock_jump_score.loc[valid_jump].ge(jump_threshold)
            & benchmark_jump_score.loc[valid_jump].lt(jump_threshold)
        )
        recent_jump = _recent_boolean_state(
            idiosyncratic_jump,
            day,
            segment,
            window=recent_jump_minutes,
        )

        amount_millions = pd.to_numeric(group["amount"], errors="coerce") / 1_000_000.0
        raw_amihud = residual_bps.abs() / amount_millions.where(amount_millions > 0)
        amihud = np.log1p(
            _segmented_rolling_median(
                raw_amihud,
                day,
                segment,
                window=amihud_window,
            )
        )
        amihud_location, amihud_mad = _rolling_prior_median_mad(
            amihud,
            slots,
            history_days=amihud_history_days,
            min_history_days=min_amihud_history_days,
        )
        amihud_scale = (1.4826 * amihud_mad).where(amihud_mad > 1e-12)

        group["residual_return_bps"] = residual_bps
        group["amount_location"] = amount_location
        group["amount_scale"] = amount_scale
        group["amount_z"] = amount_z
        group["price_volume_interaction"] = interaction
        for column in forecasts:
            group[column] = forecasts[column]
        group["stock_jump_scale_bps"] = stock_jump_scale
        group["benchmark_jump_scale_bps"] = benchmark_jump_scale
        group["stock_jump_score"] = stock_jump_score
        group["benchmark_jump_score"] = benchmark_jump_score
        group["idiosyncratic_jump"] = idiosyncratic_jump
        group["recent_idiosyncratic_jump"] = recent_jump
        group["amihud_impact"] = amihud
        group["amihud_location"] = amihud_location
        group["amihud_scale"] = amihud_scale
        group["amihud_impact_z"] = (amihud - amihud_location) / amihud_scale
        groups.append(group)
    if not groups:
        raise ValueError("stock minute data is empty")
    return pd.concat(groups).sort_index(kind="stable")


def _required_columns(frame: pd.DataFrame, names: Sequence[str], variant: str) -> None:
    missing = set(names) - set(frame.columns)
    if missing:
        raise ValueError(f"{variant} gate requires columns: {sorted(missing)}")


def apply_entry_gate(frame: pd.DataFrame, variant: ResearchVariant) -> pd.Series:
    """Apply the v3 anchor and exactly one optional v4 entry-only gate."""
    _required_columns(frame, ("ready", "zscore", "session_return_bps"), variant.name)
    ready = frame["ready"].fillna(False).astype(bool)
    if "reference_break" in frame:
        ready &= ~frame["reference_break"].fillna(True).astype(bool)
    zscore = pd.to_numeric(frame["zscore"], errors="coerce")
    session_move = pd.to_numeric(frame["session_return_bps"], errors="coerce")
    directional = np.sign(zscore) * session_move
    anchor = ready & directional.ge(0.0) & directional.lt(100.0)
    if variant.name == "directional_move_0_100":
        return anchor
    if variant.name == "volume_return_forecast":
        _required_columns(frame, ("volume_return_forecast_bps",), variant.name)
        forecast = pd.to_numeric(frame["volume_return_forecast_bps"], errors="coerce")
        return anchor & forecast.notna() & (zscore * forecast).lt(0.0)
    if variant.name == "idiosyncratic_jump_veto":
        _required_columns(frame, ("recent_idiosyncratic_jump",), variant.name)
        recent = frame["recent_idiosyncratic_jump"].astype("boolean")
        return anchor & recent.notna() & ~recent.fillna(True).astype(bool)
    if variant.name == "amihud_impact":
        _required_columns(frame, ("amihud_impact_z",), variant.name)
        impact = pd.to_numeric(frame["amihud_impact_z"], errors="coerce")
        return anchor & impact.notna() & impact.le(1.5)
    raise ValueError(f"unknown research variant: {variant.name}")


class ResearchGateBacktester(IntradayTBacktester):
    """Gate new entries while leaving an active pair's restoration path untouched."""

    def __init__(self, variant: ResearchVariant) -> None:
        self.variant = variant

    def _prepare_frame(self, minute_data: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
        frame = super()._prepare_frame(minute_data, params)
        frame["ready"] = apply_entry_gate(frame, self.variant)
        return frame


def signal_ledger_fingerprint(trades: Sequence[dict[str, Any]]) -> str:
    normalized = [
        {field: trade.get(field) for field in SIGNAL_LEDGER_FIELDS}
        for trade in trades
    ]
    payload = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_run_matrix(
    runs: Sequence[dict[str, Any]],
    *,
    fold_names: Sequence[str],
    variant_names: Sequence[str],
    scenario_names: Sequence[str],
) -> None:
    expected = {
        (fold, variant, scenario)
        for fold in fold_names
        for variant in variant_names
        for scenario in scenario_names
    }
    actual = [
        (str(run.get("fold")), str(run.get("variant")), str(run.get("scenario")))
        for run in runs
    ]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise ValueError("run matrix is not the complete unique Cartesian product")


def _slice_dates(frame: pd.DataFrame, dates: Sequence[date]) -> pd.DataFrame:
    selected = set(dates)
    sample = frame.loc[[value in selected for value in frame.index.date]]
    if sample.empty:
        raise ValueError("research fold contains no minute bars")
    return sample


def _execute_run(
    frame: pd.DataFrame,
    *,
    fold: RetrospectiveFold,
    variant: ResearchVariant,
    scenario: StressScenario,
    initial_capital: float,
    base_quantities: dict[str, int],
    limit_prices: dict[str, dict[str, float]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    sample = _slice_dates(frame, fold.test_dates)
    cost = replace(CostModel(), slippage_bps=scenario.slippage_bps)
    result = ResearchGateBacktester(variant).run(
        sample,
        BacktestConfig(
            initial_capital=initial_capital,
            base_quantities=base_quantities,
            params=variant.params,
            cost=cost,
            decision_cost=CostModel(slippage_bps=2.0),
            max_bar_volume_fraction=scenario.max_bar_volume_fraction,
            limit_prices=limit_prices,
            require_exact_limit_prices=True,
        ),
    )
    restores = [item for item in result["trades"] if item["leg"] == "restore"]
    exit_counts = Counter(str(item["reason"]) for item in restores)
    exit_pnl: dict[str, float] = {}
    for item in restores:
        reason = str(item["reason"])
        exit_pnl[reason] = exit_pnl.get(reason, 0.0) + float(item["net_pnl"])
    metrics = dict(result["metrics"])
    best_pair = max((float(item["net_pnl"]) for item in restores), default=0.0)
    metrics["average_net_pnl_per_pair"] = round(
        float(metrics["net_t_pnl"]) / len(restores) if restores else 0.0,
        4,
    )
    metrics["best_pair_net_pnl"] = round(best_pair, 4)
    metrics["net_pnl_without_best_pair"] = round(
        float(metrics["net_t_pnl"]) - max(0.0, best_pair),
        4,
    )
    risk_count = exit_counts.get("risk_restore", 0)
    metrics["risk_restore_rate"] = round(risk_count / len(restores), 6) if restores else 0.0
    ledger = [
        {
            "fold": fold.name,
            "variant": variant.name,
            "scenario": scenario.name,
            **{field: trade.get(field) for field in SIGNAL_LEDGER_FIELDS},
        }
        for trade in result["trades"]
    ]
    run = {
        "fold": fold.name,
        "sample": "retrospective_test",
        "variant": variant.name,
        "scenario": scenario.name,
        "period_start": result["period"]["start"],
        "period_end": result["period"]["end"],
        "trade_days": result["period"]["trade_days"],
        "bars": result["period"]["bars"],
        "metrics": metrics,
        "direction_metrics": result["direction_metrics"],
        "symbol_summaries": result["symbol_summaries"],
        "exit_reasons": {
            reason: {"count": count, "net_pnl": round(exit_pnl.get(reason, 0.0), 4)}
            for reason, count in sorted(exit_counts.items())
        },
        "rejections_by_reason": dict(
            sorted(Counter(str(item["reason"]) for item in result["rejections"]).items())
        ),
        "signal_ledger_sha256": signal_ledger_fingerprint(result["trades"]),
    }
    return run, ledger


def _validate_signal_ledgers(runs: Sequence[dict[str, Any]]) -> None:
    grouped: dict[tuple[str, str], set[str]] = {}
    for run in runs:
        if run.get("scenario") == "participation_2_5pct":
            continue
        key = (str(run["fold"]), str(run["variant"]))
        grouped.setdefault(key, set()).add(str(run["signal_ledger_sha256"]))
    drift = [key for key, fingerprints in grouped.items() if len(fingerprints) != 1]
    if drift:
        raise RuntimeError(f"execution stress changed the signal ledger for {drift}")


def build_recommendation(runs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Summarize the screens while permanently retaining research-only status."""
    nominal = [run for run in runs if run.get("scenario") == "nominal"]
    anchor_by_fold = {
        str(run["fold"]): float(run.get("metrics", {}).get("net_t_pnl", 0.0))
        for run in nominal
        if run.get("variant") == "directional_move_0_100"
    }
    screens: dict[str, dict[str, Any]] = {}
    for name in dict.fromkeys(str(run.get("variant")) for run in runs):
        selected = [run for run in nominal if run.get("variant") == name]
        all_variant = [run for run in runs if run.get("variant") == name]
        if not selected:
            continue
        nominal_pnl = sum(float(run["metrics"].get("net_t_pnl", 0.0)) for run in selected)
        pairs = sum(int(run["metrics"].get("completed_pairs", 0)) for run in selected)
        symbol_pnl: dict[str, float] = {}
        symbol_pairs: dict[str, int] = {}
        for run in selected:
            for summary in run.get("symbol_summaries", []):
                symbol = str(summary.get("symbol"))
                symbol_pnl[symbol] = symbol_pnl.get(symbol, 0.0) + float(
                    summary.get("net_pnl", 0.0)
                )
                symbol_pairs[symbol] = symbol_pairs.get(symbol, 0) + int(
                    summary.get("completed_pairs", 0)
                )
        screens[name] = {
            "nominal_net_pnl": round(nominal_pnl, 4),
            "completed_pairs": pairs,
            "positive_folds": sum(
                float(run["metrics"].get("net_t_pnl", 0.0)) > 0 for run in selected
            ),
            "folds_improved_vs_anchor": sum(
                float(run["metrics"].get("net_t_pnl", 0.0))
                > anchor_by_fold.get(str(run["fold"]), float("inf"))
                for run in selected
            ),
            "slippage_5bp_net_pnl": round(
                sum(
                    float(run["metrics"].get("net_t_pnl", 0.0))
                    for run in all_variant
                    if run.get("scenario") == "slippage_5bp"
                ),
                4,
            ),
            "slippage_10bp_net_pnl": round(
                sum(
                    float(run["metrics"].get("net_t_pnl", 0.0))
                    for run in all_variant
                    if run.get("scenario") == "slippage_10bp"
                ),
                4,
            ),
            "participation_2_5pct_net_pnl": round(
                sum(
                    float(run["metrics"].get("net_t_pnl", 0.0))
                    for run in all_variant
                    if run.get("scenario") == "participation_2_5pct"
                ),
                4,
            ),
            "participation_completed_pairs": sum(
                int(run["metrics"].get("completed_pairs", 0))
                for run in all_variant
                if run.get("scenario") == "participation_2_5pct"
            ),
            "symbol_net_pnl": {key: round(value, 4) for key, value in sorted(symbol_pnl.items())},
            "symbol_pairs": dict(sorted(symbol_pairs.items())),
            "restoration_safety_passed": bool(all_variant)
            and all(
                int(run["metrics"].get("open_pairs_at_end", 0)) == 0
                and int(run["metrics"].get("restoration_failures", 0)) == 0
                and float(run["metrics"].get("restoration_rate", 0.0)) == 1.0
                for run in all_variant
            ),
            "historical_screen_only": True,
        }
    return {
        "decision": "research_only",
        "auto_promoted": False,
        "formal_forward_required": True,
        "scope": list(BENCHMARK_MAP),
        "screens": screens,
        "reason": (
            "all available history influenced hypothesis formation; these two-stock "
            "results are diagnostic and cannot change paper or live defaults"
        ),
    }


def _implementation_manifest() -> dict[str, str]:
    paths = (
        Path(__file__),
        ROOT / "backend" / "app" / "scripts" / "research_intraday_t_v3.py",
        ROOT / "backend" / "app" / "scripts" / "research_intraday_t_v2.py",
        ROOT / "backend" / "app" / "services" / "intraday_t_strategy.py",
        ROOT / "backend" / "app" / "services" / "intraday_t_backtest.py",
    )
    return {
        str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths
    }


def _runtime_versions() -> dict[str, str]:
    packages = {}
    for package in ("pandas", "numpy", "duckdb"):
        try:
            packages[package] = version(package)
        except PackageNotFoundError:
            packages[package] = "unavailable"
    return {"python": platform.python_version(), **packages}


def _implementation_fingerprint() -> str:
    payload = {
        "files": _implementation_manifest(),
        "runtime": _runtime_versions(),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def write_artifacts(
    report: dict[str, Any],
    output_dir: str | Path,
    *,
    signal_ledger: Sequence[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Atomically write both CSVs before replacing research.json as commit marker."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "research.json"
    runs_path = output / "runs.csv"
    ledger_path = output / "signal_ledger.csv"
    token = uuid4().hex
    json_temp = output / f".research-{token}.json.tmp"
    runs_temp = output / f".runs-{token}.csv.tmp"
    ledger_temp = output / f".signal-ledger-{token}.csv.tmp"
    ledger_rows = list(signal_ledger if signal_ledger is not None else report.get("signal_ledger", []))

    metric_names = sorted(
        {str(name) for run in report["runs"] for name in run.get("metrics", {})}
    )
    run_fields = [
        "fold",
        "sample",
        "variant",
        "scenario",
        "period_start",
        "period_end",
        "trade_days",
        "bars",
        "signal_ledger_sha256",
    ]
    with runs_temp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[*run_fields, *metric_names])
        writer.writeheader()
        for run in report["runs"]:
            row = {name: run.get(name) for name in run_fields}
            row.update({name: run.get("metrics", {}).get(name) for name in metric_names})
            writer.writerow(row)

    ledger_fields = ["fold", "variant", "scenario", *SIGNAL_LEDGER_FIELDS]
    with ledger_temp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ledger_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ledger_rows)

    report["artifact_integrity"] = {
        "commit_marker": "research.json",
        "runs_csv_sha256": hashlib.sha256(runs_temp.read_bytes()).hexdigest(),
        "signal_ledger_csv_sha256": hashlib.sha256(ledger_temp.read_bytes()).hexdigest(),
    }
    persisted = dict(report)
    persisted.pop("signal_ledger", None)
    json_temp.write_text(
        json.dumps(
            persisted,
            ensure_ascii=False,
            indent=2,
            default=_json_default,
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    runs_temp.replace(runs_path)
    ledger_temp.replace(ledger_path)
    json_temp.replace(json_path)
    return {
        "json": str(json_path.resolve()),
        "csv": str(runs_path.resolve()),
        "signal_ledger": str(ledger_path.resolve()),
    }


def run_research(
    *,
    start_date: date = DEFAULT_START_DATE,
    end_date: date = DEFAULT_END_DATE,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    initial_capital: float = 1_000_000.0,
    base_quantities: dict[str, int] | None = None,
    limit_price_db_path: str | Path | None = None,
    store: MarketDataStore | None = None,
    index_data: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Run the frozen two-stock v4 retrospective diagnostics."""
    implementation_at_start = _implementation_fingerprint()
    if start_date > end_date:
        raise ValueError("start_date must not exceed end_date")
    stock_symbols = tuple(BENCHMARK_MAP)
    benchmark_symbols = tuple(dict.fromkeys(BENCHMARK_MAP.values()))
    market_store = store or get_market_data_store()
    columns = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
    raw_stocks = market_store.load_minute(
        stock_symbols,
        datetime.combine(start_date, time.min),
        datetime.combine(end_date + timedelta(days=1), time.min),
        columns=columns,
    )
    stocks = _normalize_stock_minute_data(raw_stocks)
    if index_data is None:
        indexes = load_index_minute_data(
            symbols=benchmark_symbols,
            start_date=start_date,
            end_date=end_date,
        )
    elif {"time", "money"}.issubset(index_data.columns):
        indexes = normalize_index_minute_data(index_data)
    else:
        indexes = _normalize_index_frame(index_data)
    trade_dates = validate_complete_panel(
        stocks,
        indexes,
        stock_symbols=stock_symbols,
        benchmark_symbols=benchmark_symbols,
    )
    calendar_loader = getattr(market_store, "load_trading_dates", None)
    if not callable(calendar_loader):
        raise ValueError("daily reference calendar loader is unavailable")
    expected_trade_dates = calendar_loader(stock_symbols, start_date, end_date)
    calendar_quality = validate_calendar_coverage(
        observed_dates=trade_dates,
        expected_dates=expected_trade_dates,
    )
    folds = build_retrospective_folds(trade_dates)
    prices = load_limit_prices_sqlite(
        limit_price_db_path or _sqlite_path_from_settings(),
        symbols=stock_symbols,
        start_date=start_date,
        end_date=end_date,
    )
    expected_limit_keys = {
        f"{symbol}|{trade_date.isoformat()}"
        for symbol in stock_symbols
        for trade_date in trade_dates
    }
    missing_limits = sorted(expected_limit_keys - set(prices))
    if missing_limits:
        raise ValueError(f"exact limit prices missing for {len(missing_limits)} symbol-days")

    featured = compute_causal_gate_features(stocks, indexes)
    reference_breaks = _reference_break_keys(stocks, prices)
    featured = _mark_reference_breaks(featured, reference_breaks)
    variants = build_variants()
    scenarios = build_stress_scenarios()
    quantities = resolve_research_base_quantities(base_quantities)
    configuration = {
        "research_period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "initial_capital": initial_capital,
        "benchmark_map": BENCHMARK_MAP,
        "base_quantities": quantities,
        "feature_settings": {
            "amount_history_days": 20,
            "min_amount_history_days": 15,
            "ols_history_days": 60,
            "min_ols_days": 60,
            "jump_window": 20,
            "min_jump_observations": 10,
            "jump_threshold": 4.0,
            "recent_jump_minutes": 10,
            "amihud_window": 5,
            "amihud_history_days": 20,
            "min_amihud_history_days": 15,
            "amihud_max_z": 1.5,
        },
        "variants": [
            {"name": item.name, "summary": item.summary, "params": asdict(item.params)}
            for item in variants
        ],
        "stress_scenarios": [asdict(item) for item in scenarios],
        "nominal_decision_cost": asdict(CostModel(slippage_bps=2.0)),
    }

    runs: list[dict[str, Any]] = []
    signal_ledger: list[dict[str, Any]] = []
    for fold in folds:
        for variant in variants:
            for scenario in scenarios:
                run, ledger = _execute_run(
                    featured,
                    fold=fold,
                    variant=variant,
                    scenario=scenario,
                    initial_capital=initial_capital,
                    base_quantities=quantities,
                    limit_prices=prices,
                )
                runs.append(run)
                signal_ledger.extend(ledger)
    validate_run_matrix(
        runs,
        fold_names=[fold.name for fold in folds],
        variant_names=[variant.name for variant in variants],
        scenario_names=[scenario.name for scenario in scenarios],
    )
    _validate_signal_ledgers(runs)
    implementation_at_end = _implementation_fingerprint()
    if implementation_at_end != implementation_at_start:
        raise RuntimeError("intraday-T research implementation changed during the run")

    report: dict[str, Any] = {
        "schema_version": 2,
        "generated_at": datetime.now(UTC).isoformat(),
        "research_status": "retrospective_exploration",
        "protocol": {
            "fixed_before_run": True,
            "automatic_parameter_selection": False,
            "auto_promotion_enabled": False,
            "holdout_is_unseen": False,
            "fold_method": "252-day warmup then contiguous 42/42/remainder blocks",
            "feature_causality": "same-clock statistics and daily OLS use completed history only",
            "execution": "signal at minute t; fill at next minute open",
            "stress_method": (
                "freeze decision cost at 2bp; require identical 2/5/10bp signal ledgers; "
                "audit 2.5% participation separately"
            ),
        },
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "trade_days": len(trade_dates),
            "stock_bars": len(stocks),
            "benchmark_bars": len(indexes),
        },
        "symbols": list(stock_symbols),
        "benchmark_map": BENCHMARK_MAP,
        "base_quantities": quantities,
        "data_quality": {
            "expected_bars_per_symbol_day": 240,
            "internally_aligned": True,
            "session_minute_grid_complete": True,
            **calendar_quality,
            "limit_price_symbol_days": len(prices),
            "missing_limit_price_symbol_days": 0,
            "reference_break_symbol_days_excluded": sorted(reference_breaks),
            "stock_fingerprint": frame_fingerprint(stocks),
            "benchmark_fingerprint": frame_fingerprint(indexes),
            "limit_price_fingerprint": limit_price_fingerprint(prices),
            "implementation_fingerprint": implementation_at_start,
            "implementation_manifest": _implementation_manifest(),
            "runtime_versions": _runtime_versions(),
            "configuration_fingerprint": _json_fingerprint(configuration),
            "signal_ledger_consistent_across_costs": True,
        },
        "folds": [
            {
                "name": fold.name,
                "train_start": fold.train_dates[0].isoformat(),
                "train_end": fold.train_dates[-1].isoformat(),
                "train_days": len(fold.train_dates),
                "test_start": fold.test_dates[0].isoformat(),
                "test_end": fold.test_dates[-1].isoformat(),
                "test_days": len(fold.test_dates),
            }
            for fold in folds
        ],
        "features": {
            "residual_return_bps": "stock minus fixed-index segmented one-minute log return",
            "volume_return_forecast": "prior-day same-clock amount z and prior-60-day OLS",
            "idiosyncratic_jump_veto": "stock jump without simultaneous benchmark jump",
            "amihud_impact": (
                "within-session 5-bar median residual impact, log1p, same-clock robust z"
            ),
        },
        "variants": configuration["variants"],
        "stress_scenarios": configuration["stress_scenarios"],
        "runs": runs,
        "recommendation": build_recommendation(runs),
    }
    report["artifacts"] = {
        "json": str((Path(output_dir) / "research.json").resolve()),
        "csv": str((Path(output_dir) / "runs.csv").resolve()),
        "signal_ledger": str((Path(output_dir) / "signal_ledger.csv").resolve()),
    }
    write_artifacts(report, output_dir, signal_ledger=signal_ledger)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=date.fromisoformat, default=DEFAULT_END_DATE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit-price-db", type=Path, default=None)
    args = parser.parse_args()
    report = run_research(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        limit_price_db_path=args.limit_price_db,
    )
    print(json.dumps(report["recommendation"], ensure_ascii=False, indent=2))
    print(json.dumps(report["artifacts"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

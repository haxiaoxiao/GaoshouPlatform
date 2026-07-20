"""Causal, read-only research runner for intraday-T v3 hypotheses.

This module deliberately does not change production strategy defaults. It evaluates a
small preregistered set of entry gates on a clean historical panel and always returns a
research-only recommendation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from math import pi
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

import numpy as np
import pandas as pd

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.data_stores.base import MarketDataStore
from app.db.duckdb import get_duckdb
from app.scripts.research_intraday_t_v2 import load_limit_prices_sqlite
from app.services.intraday_t_backtest import BacktestConfig, IntradayTBacktester
from app.services.intraday_t_strategy import CostModel, StrategyParams, compute_intraday_features

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_START_DATE = date(2024, 7, 19)
DEFAULT_END_DATE = date(2026, 3, 13)
DEFAULT_OUTPUT_DIR = (
    ROOT
    / ".runtime"
    / "intraday-t-v3-research"
    / f"{DEFAULT_START_DATE.isoformat()}_{DEFAULT_END_DATE.isoformat()}"
)
DEFAULT_BASE_QUANTITIES = {"603629.SH": 2_000, "688008.SH": 1_000}
BENCHMARK_MAP = {
    "603629.SH": "000001.SH",
    "688008.SH": "000688.SH",
}
MARKET_VALUE_COLUMNS = ("open", "high", "low", "close", "volume", "amount")
EXPECTED_SESSION_MINUTES = frozenset(
    [
        *pd.date_range("2000-01-03 09:31", "2000-01-03 11:30", freq="1min").time,
        *pd.date_range("2000-01-03 13:01", "2000-01-03 15:00", freq="1min").time,
    ]
)


@dataclass(frozen=True)
class RetrospectiveFold:
    name: str
    train_dates: tuple[date, ...]
    test_dates: tuple[date, ...]


@dataclass(frozen=True)
class ResearchVariant:
    name: str
    summary: str
    params: StrategyParams


@dataclass(frozen=True)
class StressScenario:
    name: str
    slippage_bps: float
    max_bar_volume_fraction: float = 0.05


def build_retrospective_folds(
    trade_dates: Sequence[date],
    *,
    warmup_days: int = 252,
    test_days: int = 42,
) -> list[RetrospectiveFold]:
    """Build three expanding-history blocks, merging any tail into the last block."""
    if warmup_days <= 0 or test_days <= 0:
        raise ValueError("warmup_days and test_days must be positive")
    dates = tuple(sorted(set(trade_dates)))
    remaining = len(dates) - warmup_days
    if remaining < test_days * 2 + 1:
        raise ValueError("not enough trade dates for two tests and a final block")

    folds: list[RetrospectiveFold] = []
    cursor = warmup_days
    for sequence in range(1, 4):
        if sequence < 3:
            end = cursor + test_days
        else:
            end = len(dates)
        folds.append(
            RetrospectiveFold(
                name=f"fold_{sequence:02d}",
                train_dates=dates[:cursor],
                test_dates=dates[cursor:end],
            )
        )
        cursor = end
    return folds


def build_variants() -> list[ResearchVariant]:
    """Return independent, fixed v3 hypotheses in their preregistered order."""
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
            "baseline_time_window",
            "v2 baseline: 10:00 <= entry < 10:30",
            baseline,
        ),
        ResearchVariant(
            "rv_15_25",
            "15 <= causal RV10 < 25 bps",
            baseline,
        ),
        ResearchVariant(
            "directional_move_0_100",
            "0 <= sign(z) * session return < 100 bps",
            baseline,
        ),
        ResearchVariant(
            "max_z_2_25",
            "entry tail guard: abs(z) < 2.25",
            replace(baseline, max_entry_z=2.25),
        ),
        ResearchVariant(
            "market_residual",
            "raw z and causal market-residual z agree with abs(residual z) >= 1",
            baseline,
        ),
        ResearchVariant(
            "residual_regime",
            "residual path efficiency <= 0.65 and relative jump score <= 4",
            baseline,
        ),
    ]


def build_stress_scenarios() -> list[StressScenario]:
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


def validate_research_base_quantities(values: dict[str, int]) -> dict[str, int]:
    expected = set(BENCHMARK_MAP)
    if set(values) != expected:
        raise ValueError(f"base_quantities must contain exactly {sorted(expected)}")
    normalized = {symbol: int(values[symbol]) for symbol in BENCHMARK_MAP}
    if any(quantity <= 0 for quantity in normalized.values()):
        raise ValueError("base_quantities must be positive")
    return normalized


def normalize_index_minute_data(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize the JQ physical index schema and fail closed on duplicate bars."""
    required = ("time", "symbol", "open", "high", "low", "close", "volume", "money")
    missing = set(required) - set(raw.columns)
    if missing:
        raise ValueError(f"index minute data missing columns: {sorted(missing)}")
    frame = raw.loc[:, list(required)].rename(columns={"time": "datetime", "money": "amount"})
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    if frame["datetime"].isna().any():
        raise ValueError("index minute data contains invalid timestamps")
    for column in MARKET_VALUE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    duplicate = frame.duplicated(["symbol", "datetime"], keep=False)
    if duplicate.any():
        raise ValueError("index minute data contains duplicate symbol-minute bars")
    _validate_market_values(frame, label="index minute data")
    return frame.sort_values(["datetime", "symbol"], kind="stable").set_index("datetime")


def load_index_minute_data(
    *,
    symbols: Sequence[str],
    start_date: date,
    end_date: date,
    connection: Any | None = None,
    parquet_data_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Read the fixed benchmark whitelist from the local JQ index dataset."""
    requested = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols))
    allowed = set(BENCHMARK_MAP.values())
    unknown = set(requested) - allowed
    if not requested or unknown:
        raise ValueError(f"unsupported benchmark symbols: {sorted(unknown)}")
    root = Path(parquet_data_dir or settings.parquet_data_dir)
    pattern = str(root / "jq_index_minute_bars" / "year=*" / "month=*" / "*.parquet")
    pattern = pattern.replace("\\", "/")
    placeholders = ", ".join("?" for _ in requested)
    query = f"""
        SELECT time, symbol, open, high, low, close, volume, money
        FROM read_parquet(?, hive_partitioning=true)
        WHERE symbol IN ({placeholders})
          AND time >= ?
          AND time < ?
        ORDER BY time, symbol
    """
    start = datetime.combine(start_date, time.min)
    end = datetime.combine(end_date + timedelta(days=1), time.min)
    db = connection or get_duckdb()
    raw = db.execute(query, [pattern, *requested, start, end]).df()
    if raw.empty:
        raise ValueError("index minute data is empty for the requested window")
    return normalize_index_minute_data(raw)


def _normalize_stock_minute_data(raw: pd.DataFrame) -> pd.DataFrame:
    frame = raw.copy()
    if not isinstance(frame.index, pd.DatetimeIndex):
        if "datetime" not in frame.columns:
            raise ValueError("stock minute data requires datetime")
        frame.index = pd.to_datetime(frame.pop("datetime"), errors="coerce")
    frame.index.name = "datetime"
    required = {"symbol", "open", "high", "low", "close", "volume", "amount"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"stock minute data missing columns: {sorted(missing)}")
    if frame.index.isna().any():
        raise ValueError("stock minute data contains invalid timestamps")
    for column in MARKET_VALUE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    _validate_market_values(frame, label="stock minute data")
    duplicate_keys = pd.DataFrame(
        {"symbol": frame["symbol"].astype(str).to_numpy(), "datetime": frame.index}
    ).duplicated(keep=False)
    if duplicate_keys.any():
        raise ValueError("stock minute data contains duplicate symbol-minute bars")
    return frame.sort_index(kind="stable")


def _validate_market_values(frame: pd.DataFrame, *, label: str) -> None:
    values = frame.loc[:, MARKET_VALUE_COLUMNS]
    if values.isna().any().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError(f"{label} contains non-finite market values")
    if (values[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError(f"{label} contains non-positive prices")
    if (values[["volume", "amount"]] < 0).any().any():
        raise ValueError(f"{label} contains negative volume or amount")
    upper = values[["open", "low", "close"]].max(axis=1)
    lower = values[["open", "high", "close"]].min(axis=1)
    if values["high"].lt(upper).any() or values["low"].gt(lower).any():
        raise ValueError(f"{label} contains invalid OHLC relationships")


def _rolling_prior_median_mad(
    values: pd.Series,
    slots: pd.Series,
    *,
    history_days: int,
    min_history_days: int,
) -> tuple[pd.Series, pd.Series]:
    location = pd.Series(np.nan, index=values.index, dtype=float)
    mad = pd.Series(np.nan, index=values.index, dtype=float)
    for _, slot_values in values.groupby(slots, sort=False):
        ordered = slot_values.sort_index(kind="stable")
        prior = ordered.shift(1)
        rolling = prior.rolling(history_days, min_periods=min_history_days)
        slot_location = rolling.median()
        slot_mad = rolling.apply(
            lambda sample: _finite_mad(sample),
            raw=True,
        )
        location.loc[ordered.index] = slot_location
        mad.loc[ordered.index] = slot_mad
    return location, mad


def _finite_mad(sample: np.ndarray) -> float:
    finite = sample[np.isfinite(sample)]
    if finite.size == 0:
        return float("nan")
    center = np.median(finite)
    return float(np.median(np.abs(finite - center)))


def _intraday_residual_regime(
    relative_returns: pd.Series,
    day_keys: pd.Series,
    *,
    path_window: int,
    jump_window: int,
) -> tuple[pd.Series, pd.Series]:
    efficiency = pd.Series(np.nan, index=relative_returns.index, dtype=float)
    jump_score = pd.Series(np.nan, index=relative_returns.index, dtype=float)
    for _, day_returns in relative_returns.groupby(day_keys, sort=False):
        ordered = day_returns.sort_index(kind="stable")
        net_move = ordered.rolling(path_window, min_periods=path_window).sum().abs()
        travelled = ordered.abs().rolling(path_window, min_periods=path_window).sum()
        efficiency.loc[ordered.index] = net_move / travelled.where(travelled > 1e-12)

        absolute = ordered.abs()
        prior_products = absolute.shift(1) * absolute.shift(2)
        min_jump_history = max(5, jump_window // 2)
        bipower = prior_products.rolling(
            jump_window,
            min_periods=min_jump_history,
        ).mean()
        scale = np.sqrt((pi / 2.0) * bipower)
        jump_score.loc[ordered.index] = absolute / scale.where(scale > 1e-12)
    return efficiency, jump_score


def compute_causal_market_features(
    stock_frame: pd.DataFrame,
    index_frame: pd.DataFrame,
    *,
    history_days: int = 20,
    min_history_days: int = 15,
    path_window: int = 15,
    jump_window: int = 20,
    params: StrategyParams | None = None,
) -> pd.DataFrame:
    """Add market residual and regime features using observations available at time t."""
    if not 1 <= min_history_days <= history_days:
        raise ValueError("min_history_days must be in [1, history_days]")
    if path_window < 2 or jump_window < 2:
        raise ValueError("path_window and jump_window must be at least 2")
    stocks = _normalize_stock_minute_data(stock_frame)
    indexes = index_frame.copy()
    if not isinstance(indexes.index, pd.DatetimeIndex):
        if "datetime" not in indexes.columns:
            raise ValueError("index minute data requires a DatetimeIndex")
        indexes.index = pd.to_datetime(indexes.pop("datetime"), errors="coerce")
    if "symbol" not in indexes or "close" not in indexes:
        raise ValueError("index minute data requires symbol and close")
    index_keys = pd.DataFrame(
        {"symbol": indexes["symbol"].astype(str).to_numpy(), "datetime": indexes.index}
    )
    if index_keys.duplicated().any():
        raise ValueError("index minute data contains duplicate symbol-minute bars")

    feature_params = params or build_variants()[0].params
    groups: list[pd.DataFrame] = []
    for symbol, raw_group in stocks.groupby("symbol", sort=False):
        benchmark = BENCHMARK_MAP.get(str(symbol))
        if benchmark is None:
            raise ValueError(f"no fixed benchmark for {symbol}")
        group = raw_group.sort_index(kind="stable")
        featured_days = [
            compute_intraday_features(day_group, feature_params)
            for _, day_group in group.groupby(group.index.date, sort=False)
        ]
        featured = pd.concat(featured_days).sort_index(kind="stable")
        featured["vwap"] = featured["session_vwap"]

        benchmark_rows = indexes.loc[indexes["symbol"].astype(str) == benchmark]
        benchmark_close = pd.to_numeric(
            benchmark_rows["close"], errors="coerce"
        ).sort_index(kind="stable")
        benchmark_open = pd.to_numeric(
            benchmark_rows["open"], errors="coerce"
        ).sort_index(kind="stable")
        aligned_benchmark = benchmark_close.reindex(featured.index)
        aligned_benchmark_open = benchmark_open.reindex(featured.index)
        missing_benchmark = aligned_benchmark.isna() | aligned_benchmark_open.isna()
        if missing_benchmark.any():
            missing_count = int(missing_benchmark.sum())
            raise ValueError(f"benchmark alignment missing {missing_count} bars for {symbol}")
        close = pd.to_numeric(featured["close"], errors="coerce")
        stock_open = pd.to_numeric(featured["open"], errors="coerce")
        if (
            (close <= 0).any()
            or (stock_open <= 0).any()
            or (aligned_benchmark <= 0).any()
            or (aligned_benchmark_open <= 0).any()
        ):
            raise ValueError("stock and benchmark prices must be positive")
        day_keys = pd.Series(featured.index.date, index=featured.index)
        stock_log = np.log(close)
        benchmark_log = np.log(aligned_benchmark)
        stock_session_open = np.log(stock_open).groupby(day_keys).transform("first")
        benchmark_session_open = np.log(aligned_benchmark_open).groupby(day_keys).transform(
            "first"
        )
        stock_session = stock_log - stock_session_open
        benchmark_session = benchmark_log - benchmark_session_open
        residual_bps = (stock_session - benchmark_session) * 10_000
        slots = pd.Series(featured.index.strftime("%H:%M"), index=featured.index)
        location, raw_mad = _rolling_prior_median_mad(
            residual_bps,
            slots,
            history_days=history_days,
            min_history_days=min_history_days,
        )
        scale = (1.4826 * raw_mad).where(raw_mad > 1e-9)
        featured["market_residual_bps"] = residual_bps
        featured["residual_location_bps"] = location
        featured["residual_scale_bps"] = scale
        featured["residual_z"] = (residual_bps - location) / scale

        stock_return = stock_log.groupby(day_keys).diff()
        benchmark_return = benchmark_log.groupby(day_keys).diff()
        first_bar = featured.groupby(day_keys).cumcount().eq(0)
        stock_return.loc[first_bar] = stock_log.loc[first_bar] - stock_session_open.loc[first_bar]
        benchmark_return.loc[first_bar] = (
            benchmark_log.loc[first_bar] - benchmark_session_open.loc[first_bar]
        )
        relative_return = stock_return - benchmark_return
        efficiency, jump_score = _intraday_residual_regime(
            relative_return,
            day_keys,
            path_window=path_window,
            jump_window=jump_window,
        )
        featured["residual_path_efficiency"] = efficiency
        featured["relative_jump_score"] = jump_score
        featured["benchmark_symbol"] = benchmark
        groups.append(featured)
    return pd.concat(groups).sort_index(kind="stable")


def _required_columns(frame: pd.DataFrame, names: Sequence[str], variant: str) -> None:
    missing = set(names) - set(frame.columns)
    if missing:
        raise ValueError(f"{variant} gate requires columns: {sorted(missing)}")


def apply_entry_gate(frame: pd.DataFrame, variant: ResearchVariant) -> pd.Series:
    """Return base-ready AND the selected entry-only research gate."""
    _required_columns(frame, ("ready",), variant.name)
    ready = frame["ready"].fillna(False).astype(bool)
    if "reference_break" in frame:
        ready &= ~frame["reference_break"].fillna(True).astype(bool)
    if variant.name == "baseline_time_window":
        return ready
    if variant.name == "rv_15_25":
        _required_columns(frame, ("realized_vol_bps",), variant.name)
        values = pd.to_numeric(frame["realized_vol_bps"], errors="coerce")
        return ready & values.ge(15.0) & values.lt(25.0)
    if variant.name == "directional_move_0_100":
        _required_columns(frame, ("zscore", "session_return_bps"), variant.name)
        directional = np.sign(pd.to_numeric(frame["zscore"], errors="coerce")) * pd.to_numeric(
            frame["session_return_bps"], errors="coerce"
        )
        return ready & directional.ge(0.0) & directional.lt(100.0)
    if variant.name == "max_z_2_25":
        _required_columns(frame, ("zscore",), variant.name)
        return ready & pd.to_numeric(frame["zscore"], errors="coerce").abs().lt(2.25)
    if variant.name == "market_residual":
        _required_columns(frame, ("zscore", "residual_z"), variant.name)
        raw_z = pd.to_numeric(frame["zscore"], errors="coerce")
        residual_z = pd.to_numeric(frame["residual_z"], errors="coerce")
        return ready & residual_z.abs().ge(1.0) & (raw_z * residual_z).gt(0.0)
    if variant.name == "residual_regime":
        _required_columns(
            frame,
            ("residual_path_efficiency", "relative_jump_score"),
            variant.name,
        )
        efficiency = pd.to_numeric(frame["residual_path_efficiency"], errors="coerce")
        jump = pd.to_numeric(frame["relative_jump_score"], errors="coerce")
        return ready & efficiency.le(0.65) & jump.le(4.0)
    raise ValueError(f"unknown research variant: {variant.name}")


class ResearchGateBacktester(IntradayTBacktester):
    """Apply a research entry gate while allowing every active pair to restore."""

    def __init__(self, variant: ResearchVariant) -> None:
        self.variant = variant

    def _prepare_frame(self, minute_data: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
        frame = super()._prepare_frame(minute_data, params)
        frame["ready"] = apply_entry_gate(frame, self.variant)
        return frame


def _slice_dates(frame: pd.DataFrame, dates: Sequence[date]) -> pd.DataFrame:
    selected = set(dates)
    sample = frame.loc[[value in selected for value in frame.index.date]]
    if sample.empty:
        raise ValueError("research fold contains no minute bars")
    return sample


def validate_complete_panel(
    stock_frame: pd.DataFrame,
    index_frame: pd.DataFrame,
    *,
    stock_symbols: Sequence[str],
    benchmark_symbols: Sequence[str],
    expected_bars_per_day: int = 240,
) -> list[date]:
    dates_by_series: dict[str, set[date]] = {}
    for label, frame, symbols in (
        ("stock", stock_frame, stock_symbols),
        ("benchmark", index_frame, benchmark_symbols),
    ):
        for symbol in symbols:
            group = frame.loc[frame["symbol"].astype(str) == symbol]
            if group.empty:
                raise ValueError(f"{label} panel is empty for {symbol}")
            counts = pd.Series(group.index.date).value_counts()
            incomplete = counts.loc[counts != expected_bars_per_day]
            if not incomplete.empty:
                raise ValueError(
                    f"{label} panel has incomplete {symbol} days: "
                    f"{sorted(value.isoformat() for value in incomplete.index)}"
                )
            for trade_date, day_group in group.groupby(group.index.date, sort=False):
                actual_minutes = frozenset(day_group.index.time)
                if actual_minutes != EXPECTED_SESSION_MINUTES:
                    missing = len(EXPECTED_SESSION_MINUTES - actual_minutes)
                    unexpected = len(actual_minutes - EXPECTED_SESSION_MINUTES)
                    raise ValueError(
                        f"{label} panel session minute grid is invalid for "
                        f"{symbol} on {trade_date}: missing={missing}, unexpected={unexpected}"
                    )
            dates_by_series[f"{label}:{symbol}"] = set(group.index.date)
    first_dates = next(iter(dates_by_series.values()))
    for key, values in dates_by_series.items():
        if values != first_dates:
            raise ValueError(f"research panel trade dates do not align for {key}")
    return sorted(first_dates)


def validate_calendar_coverage(
    *,
    observed_dates: Sequence[date],
    expected_dates: Sequence[date],
) -> dict[str, Any]:
    observed = set(observed_dates)
    expected = set(expected_dates)
    if not expected:
        raise ValueError("daily reference calendar is empty")
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    if missing or unexpected:
        raise ValueError(
            "research panel does not match the daily reference calendar: "
            f"missing={len(missing)}, unexpected={len(unexpected)}"
        )
    return {
        "daily_reference_complete": True,
        "daily_reference_trade_days": len(expected),
        "missing_daily_reference_days": [],
        "unexpected_minute_days": [],
    }


def _reference_break_keys(
    stock_frame: pd.DataFrame,
    limit_prices: dict[str, dict[str, float]],
    *,
    threshold: float = 0.02,
) -> set[str]:
    breaks: set[str] = set()
    for symbol, group in stock_frame.groupby("symbol", sort=False):
        closes = group.groupby(group.index.date)["close"].last().sort_index()
        prior_close = closes.shift(1)
        for trade_date, previous in prior_close.items():
            if pd.isna(previous):
                continue
            key = f"{symbol}|{trade_date.isoformat()}"
            limits = limit_prices.get(key)
            if not limits:
                continue
            reference = (float(limits["up"]) + float(limits["down"])) / 2.0
            if abs(reference / float(previous) - 1.0) > threshold:
                breaks.add(key)
    return breaks


def _mark_reference_breaks(frame: pd.DataFrame, keys: set[str]) -> pd.DataFrame:
    result = frame.copy()
    row_keys = pd.Series(
        [f"{symbol}|{timestamp.date().isoformat()}" for timestamp, symbol in zip(
            result.index, result["symbol"], strict=True
        )],
        index=result.index,
    )
    result["reference_break"] = row_keys.isin(keys).to_numpy()
    return result


def _execute_run(
    frame: pd.DataFrame,
    *,
    fold: RetrospectiveFold,
    variant: ResearchVariant,
    scenario: StressScenario,
    initial_capital: float,
    base_quantities: dict[str, int],
    limit_prices: dict[str, dict[str, float]],
) -> dict[str, Any]:
    sample = _slice_dates(frame, fold.test_dates)
    cost = replace(CostModel(), slippage_bps=scenario.slippage_bps)
    result = ResearchGateBacktester(variant).run(
        sample,
        BacktestConfig(
            initial_capital=initial_capital,
            base_quantities=base_quantities,
            params=variant.params,
            cost=cost,
            decision_cost=CostModel(),
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
    best_pair = max((float(item["net_pnl"]) for item in restores), default=0.0)
    monthly: dict[str, dict[str, float | int]] = {}
    for item in restores:
        month = str(item["fill_at"])[:7]
        summary = monthly.setdefault(month, {"completed_pairs": 0, "net_pnl": 0.0})
        summary["completed_pairs"] = int(summary["completed_pairs"]) + 1
        summary["net_pnl"] = float(summary["net_pnl"]) + float(item["net_pnl"])
    rejection_counts = Counter(str(item["reason"]) for item in result["rejections"])
    metrics = dict(result["metrics"])
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
    return {
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
        "monthly_results": {
            month: {
                "completed_pairs": int(summary["completed_pairs"]),
                "net_pnl": round(float(summary["net_pnl"]), 4),
            }
            for month, summary in sorted(monthly.items())
        },
        "rejections_by_reason": dict(sorted(rejection_counts.items())),
    }


def build_recommendation(runs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Summarize retrospective screens without ever promoting a strategy."""
    minimum_pairs = 80
    minimum_pairs_per_symbol = 20
    minimum_participation_pair_retention = 0.80
    nominal = [run for run in runs if run.get("scenario") == "nominal"]
    baseline = {
        str(run["fold"]): float(run["metrics"].get("net_t_pnl", 0.0))
        for run in nominal
        if run.get("variant") == "baseline_time_window"
    }
    screens: dict[str, dict[str, Any]] = {}
    variant_names = [item.name for item in build_variants()]
    for name in variant_names:
        selected = [run for run in nominal if run.get("variant") == name]
        if not selected:
            continue
        pnl = sum(float(run["metrics"].get("net_t_pnl", 0.0)) for run in selected)
        pairs = sum(int(run["metrics"].get("completed_pairs", 0)) for run in selected)
        best_pair = max(
            (
                float(
                    run["metrics"].get(
                        "best_pair_net_pnl",
                        float(run["metrics"].get("net_t_pnl", 0.0))
                        - float(run["metrics"].get("net_pnl_without_best_pair", 0.0)),
                    )
                )
                for run in selected
            ),
            default=0.0,
        )
        net_without_best = pnl - max(0.0, best_pair)
        improvements = sum(
            float(run["metrics"].get("net_t_pnl", 0.0)) > baseline.get(str(run["fold"]), 0.0)
            for run in selected
            if str(run["fold"]) in baseline
        )
        stress_5 = sum(
            float(run["metrics"].get("net_t_pnl", 0.0))
            for run in runs
            if run.get("variant") == name and run.get("scenario") == "slippage_5bp"
        )
        stress_10 = sum(
            float(run["metrics"].get("net_t_pnl", 0.0))
            for run in runs
            if run.get("variant") == name and run.get("scenario") == "slippage_10bp"
        )
        participation = sum(
            float(run["metrics"].get("net_t_pnl", 0.0))
            for run in runs
            if run.get("variant") == name
            and run.get("scenario") == "participation_2_5pct"
        )
        participation_pairs = sum(
            int(run["metrics"].get("completed_pairs", 0))
            for run in runs
            if run.get("variant") == name
            and run.get("scenario") == "participation_2_5pct"
        )
        participation_retention = participation_pairs / pairs if pairs else 0.0
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
        direction_pnl: dict[str, float] = {}
        for run in selected:
            for direction, summary in run.get("direction_metrics", {}).items():
                direction_pnl[str(direction)] = direction_pnl.get(str(direction), 0.0) + float(
                    summary.get("net_pnl", 0.0)
                )
        exit_pnl: dict[str, float] = {}
        for run in selected:
            for reason, summary in run.get("exit_reasons", {}).items():
                exit_pnl[str(reason)] = exit_pnl.get(str(reason), 0.0) + float(
                    summary.get("net_pnl", 0.0)
                )
        all_symbols_positive = bool(symbol_pnl) and all(value > 0 for value in symbol_pnl.values())
        all_directions_positive = bool(direction_pnl) and all(
            value > 0 for value in direction_pnl.values()
        )
        symbol_sample_ready = bool(symbol_pairs) and all(
            value >= minimum_pairs_per_symbol for value in symbol_pairs.values()
        )
        zero_open_pairs = all(
            int(run["metrics"].get("open_pairs_at_end", 0)) == 0 for run in selected
        )
        variant_runs = [run for run in runs if run.get("variant") == name]
        restoration_safety_passed = bool(variant_runs) and all(
            int(run["metrics"].get("open_pairs_at_end", 0)) == 0
            and int(run["metrics"].get("restoration_failures", 0)) == 0
            and float(run["metrics"].get("restoration_rate", 0.0)) == 1.0
            for run in variant_runs
        )
        risk_restore_contained = (
            exit_pnl.get("mean_reversion_exit", 0.0) + exit_pnl.get("risk_restore", 0.0) > 0
        )
        positive_folds = sum(
            float(run["metrics"].get("net_t_pnl", 0.0)) > 0 for run in selected
        )
        screens[name] = {
            "nominal_net_pnl": round(pnl, 4),
            "net_pnl_without_best_pair": round(net_without_best, 4),
            "completed_pairs": pairs,
            "minimum_pairs_required": minimum_pairs,
            "minimum_pairs_per_symbol": minimum_pairs_per_symbol,
            "positive_folds": positive_folds,
            "folds_improved_vs_baseline": improvements,
            "slippage_5bp_net_pnl": round(stress_5, 4),
            "slippage_10bp_net_pnl": round(stress_10, 4),
            "participation_2_5pct_net_pnl": round(participation, 4),
            "participation_completed_pairs": participation_pairs,
            "participation_pair_retention": round(participation_retention, 6),
            "minimum_participation_pair_retention": minimum_participation_pair_retention,
            "symbol_net_pnl": {key: round(value, 4) for key, value in sorted(symbol_pnl.items())},
            "symbol_pairs": dict(sorted(symbol_pairs.items())),
            "direction_net_pnl": {
                key: round(value, 4) for key, value in sorted(direction_pnl.items())
            },
            "all_symbols_positive": all_symbols_positive,
            "all_directions_positive": all_directions_positive,
            "risk_restore_contained": risk_restore_contained,
            "zero_open_pairs": zero_open_pairs,
            "restoration_safety_passed": restoration_safety_passed,
            "retrospective_screen_passed": bool(
                name != "baseline_time_window"
                and pnl > 0
                and stress_5 > 0
                and participation > 0
                and participation_retention >= minimum_participation_pair_retention
                and improvements >= 2
                and positive_folds >= 2
                and pairs >= minimum_pairs
                and net_without_best > 0
                and all_symbols_positive
                and all_directions_positive
                and symbol_sample_ready
                and risk_restore_contained
                and zero_open_pairs
                and restoration_safety_passed
            ),
        }
    return {
        "decision": "research_only",
        "auto_promoted": False,
        "formal_forward_required": True,
        "forward_sample_starts_after": "2026-07-20",
        "screens": screens,
        "reason": (
            "all historical periods have been observed or influenced hypothesis formation; "
            "only a newly frozen forward paper sample can support manual review"
        ),
    }


def frame_fingerprint(frame: pd.DataFrame) -> str:
    required = {"symbol", *MARKET_VALUE_COLUMNS}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"cannot fingerprint frame missing columns: {sorted(missing)}")
    payload = frame.loc[:, ["symbol", *MARKET_VALUE_COLUMNS]].copy()
    payload.insert(0, "datetime", pd.to_datetime(frame.index).astype("int64"))
    payload = payload.reset_index(drop=True)
    payload = payload.sort_values(["symbol", "datetime"], kind="stable")
    hashed = pd.util.hash_pandas_object(payload, index=False).to_numpy()
    return hashlib.sha256(hashed.tobytes()).hexdigest()


def limit_price_fingerprint(prices: dict[str, dict[str, float]]) -> str:
    payload = json.dumps(prices, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _implementation_fingerprint() -> str:
    paths = (
        Path(__file__),
        ROOT / "backend" / "app" / "services" / "intraday_t_strategy.py",
        ROOT / "backend" / "app" / "services" / "intraday_t_backtest.py",
    )
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.name.encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _json_fingerprint(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
        allow_nan=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sqlite_path_from_settings() -> Path:
    value = settings.database_url
    for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if value.startswith(prefix):
            return Path(value[len(prefix) :])
    raise ValueError("v3 research requires a SQLite database URL")


def write_artifacts(report: dict[str, Any], output_dir: str | Path) -> dict[str, str]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "research.json"
    csv_path = output / "runs.csv"
    token = uuid4().hex
    json_temp = output / f".research-{token}.json.tmp"
    csv_temp = output / f".runs-{token}.csv.tmp"
    metric_names = sorted(
        {str(name) for run in report["runs"] for name in run.get("metrics", {})}
    )
    fields = [
        "fold",
        "sample",
        "variant",
        "scenario",
        "period_start",
        "period_end",
        "trade_days",
        "bars",
    ]
    with csv_temp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[*fields, *metric_names])
        writer.writeheader()
        for run in report["runs"]:
            row = {name: run.get(name) for name in fields}
            row.update({name: run["metrics"].get(name) for name in metric_names})
            writer.writerow(row)
    report["artifact_integrity"] = {
        "commit_marker": "research.json",
        "runs_csv_sha256": hashlib.sha256(csv_temp.read_bytes()).hexdigest(),
    }
    json_temp.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    csv_temp.replace(csv_path)
    json_temp.replace(json_path)
    return {"json": str(json_path.resolve()), "csv": str(csv_path.resolve())}


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime, time, Path)):
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"cannot serialize {type(value).__name__}")


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
    """Run independent v3 gates on the frozen, complete historical panel."""
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
    indexes = index_data if index_data is not None else load_index_minute_data(
        symbols=benchmark_symbols,
        start_date=start_date,
        end_date=end_date,
    )
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

    featured = compute_causal_market_features(stocks, indexes)
    reference_breaks = _reference_break_keys(stocks, prices)
    featured = _mark_reference_breaks(featured, reference_breaks)
    variants = build_variants()
    scenarios = build_stress_scenarios()
    quantities = validate_research_base_quantities(
        dict(base_quantities or DEFAULT_BASE_QUANTITIES)
    )
    configuration = {
        "research_period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        },
        "initial_capital": initial_capital,
        "benchmark_map": BENCHMARK_MAP,
        "base_quantities": quantities,
        "folds": [
            {
                "name": fold.name,
                "train_dates": [value.isoformat() for value in fold.train_dates],
                "test_dates": [value.isoformat() for value in fold.test_dates],
            }
            for fold in folds
        ],
        "feature_settings": {
            "history_days": 20,
            "min_history_days": 15,
            "path_window": 15,
            "jump_window": 20,
        },
        "variants": [
            {"name": item.name, "summary": item.summary, "params": asdict(item.params)}
            for item in variants
        ],
        "stress_scenarios": [asdict(item) for item in scenarios],
        "nominal_decision_cost": asdict(CostModel()),
    }
    runs = [
        _execute_run(
            featured,
            fold=fold,
            variant=variant,
            scenario=scenario,
            initial_capital=initial_capital,
            base_quantities=quantities,
            limit_prices=prices,
        )
        for fold in folds
        for variant in variants
        for scenario in scenarios
    ]
    implementation_at_end = _implementation_fingerprint()
    if implementation_at_end != implementation_at_start:
        raise RuntimeError("intraday-T research implementation changed during the run")
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "research_status": "retrospective_exploration",
        "protocol": {
            "fixed_before_run": True,
            "automatic_parameter_selection": False,
            "auto_promotion_enabled": False,
            "holdout_is_unseen": False,
            "fold_method": "252-day warmup then contiguous 42/42/remainder blocks",
            "feature_causality": "same-clock statistics use shift(1) and past 20 days only",
            "stress_method": "freeze decision cost at nominal 2bp; vary execution only",
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
            "configuration_fingerprint": _json_fingerprint(configuration),
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
            "market_residual": "stock session log return minus fixed benchmark session log return",
            "residual_z": "same-minute prior-20-day median and 1.4826*MAD",
            "residual_path_efficiency": "15-minute abs net residual move / residual path length",
            "relative_jump_score": "current residual return / past-only bipower scale",
        },
        "variants": configuration["variants"],
        "stress_scenarios": configuration["stress_scenarios"],
        "runs": runs,
        "recommendation": build_recommendation(runs),
    }
    report["artifacts"] = {
        "json": str((Path(output_dir) / "research.json").resolve()),
        "csv": str((Path(output_dir) / "runs.csv").resolve()),
    }
    write_artifacts(report, output_dir)
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

"""Point-in-time market-sentiment research for the two intraday-T stocks.

The runner keeps the v4 trading and execution contract unchanged. It adds a
10:00 market snapshot, causal sentiment normalisation, and three independent
entry-only gates. Results are retrospective and can never promote a strategy.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import sqlite3
from collections import Counter
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, time, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4

import numpy as np
import pandas as pd

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.data_stores.base import MarketDataStore
from app.scripts import research_intraday_t_v4 as v4
from app.scripts.research_intraday_t_v2 import load_limit_prices_sqlite
from app.scripts.research_intraday_t_v3 import (
    ResearchVariant,
    RetrospectiveFold,
    StressScenario,
    _json_default,
    _json_fingerprint,
    _mark_reference_breaks,
    _reference_break_keys,
    _rolling_prior_median_mad,
    _sqlite_path_from_settings,
    build_retrospective_folds,
    frame_fingerprint,
    limit_price_fingerprint,
    validate_calendar_coverage,
    validate_complete_panel,
)
from app.services.intraday_t_backtest import BacktestConfig, IntradayTBacktester
from app.services.intraday_t_strategy import CostModel, StrategyParams

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_START_DATE = date(2024, 7, 19)
DEFAULT_END_DATE = date(2026, 3, 13)
DEFAULT_OUTPUT_DIR = (
    ROOT
    / ".runtime"
    / "intraday-t-v5-sentiment-research"
    / f"{DEFAULT_START_DATE.isoformat()}_{DEFAULT_END_DATE.isoformat()}"
)
DEFAULT_BASE_QUANTITIES = dict(v4.DEFAULT_BASE_QUANTITIES)
BENCHMARK_MAP = dict(v4.BENCHMARK_MAP)
SYMBOL_LIMIT_SEGMENT = {
    "603629.SH": "main",
    "688008.SH": "twenty",
}
SENTIMENT_SNAPSHOT_TIME = time(10, 0)
SENTIMENT_GATE_THRESHOLD = 1.5
SENTIMENT_HISTORY_DAYS = 60
SENTIMENT_MIN_HISTORY_DAYS = 40

SENTIMENT_RAW_COLUMNS = (
    "snapshot_time",
    "market_covered",
    "market_locked_up",
    "market_locked_down",
    "market_touched_up",
    "market_broken_up",
    "main_covered",
    "main_locked_up",
    "main_locked_down",
    "main_touched_up",
    "main_broken_up",
    "twenty_covered",
    "twenty_locked_up",
    "twenty_locked_down",
    "twenty_touched_up",
    "twenty_broken_up",
    "board_source_conflicts",
    "promotion_eligible",
    "promotion_observed",
    "promotion_touched",
    "promotion_at_limit",
    "p12_eligible",
    "p12_observed",
    "p12_touched",
    "p12_at_limit",
    "p23_eligible",
    "p23_observed",
    "p23_touched",
    "p23_at_limit",
    "p3plus_eligible",
    "p3plus_observed",
    "p3plus_touched",
    "p3plus_at_limit",
)
_COUNT_COLUMNS = tuple(column for column in SENTIMENT_RAW_COLUMNS if column != "snapshot_time")


def build_variants() -> list[ResearchVariant]:
    """Return both v4 controls and three incremental sentiment gates."""
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
            "v4 anchor: 0 <= sign(z) * session return < 100 bps",
            baseline,
        ),
        ResearchVariant(
            "volume_return_forecast",
            "v4 best candidate: anchor plus past-only price-volume forecast",
            baseline,
        ),
        ResearchVariant(
            "volume_limit_breadth_alignment",
            "volume candidate excluding adverse 10:00 limit breadth extremes",
            baseline,
        ),
        ResearchVariant(
            "volume_board_promotion_alignment",
            "volume candidate excluding adverse prior-board at-limit promotion extremes",
            baseline,
        ),
        ResearchVariant(
            "volume_composite_market_sentiment",
            "volume candidate plus global, segment, at-limit and promotion alignment",
            baseline,
        ),
    ]


def build_stress_scenarios() -> list[StressScenario]:
    """Reuse the frozen v4 execution stress matrix."""
    return v4.build_stress_scenarios()


def _normalize_sentiment_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "trade_date" in result.columns:
        raw_dates = result.pop("trade_date")
    else:
        raw_dates = pd.Series(result.index, index=result.index)
    parsed = pd.to_datetime(raw_dates, errors="coerce")
    if parsed.isna().any():
        raise ValueError("market sentiment contains invalid trade dates")
    result.index = pd.Index(parsed.dt.date, name="trade_date")
    if result.index.duplicated().any():
        raise ValueError("market sentiment contains duplicate trade dates")
    missing = set(SENTIMENT_RAW_COLUMNS) - set(result.columns)
    if missing:
        raise ValueError(f"market sentiment missing columns: {sorted(missing)}")
    result["snapshot_time"] = result["snapshot_time"].astype(str).str[:8]
    for column in _COUNT_COLUMNS:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    return result.sort_index(kind="stable")


def validate_sentiment_panel(
    frame: pd.DataFrame,
    *,
    expected_dates: Sequence[date] | None = None,
    minimum_market_coverage: int | None = None,
    minimum_prior_coverage_ratio: float = 0.9,
) -> dict[str, Any]:
    """Validate completeness and all point-in-time count identities."""
    values = _normalize_sentiment_frame(frame)
    if values.empty:
        raise ValueError("market sentiment is empty")
    snapshot_values = set(values["snapshot_time"])
    if snapshot_values != {SENTIMENT_SNAPSHOT_TIME.isoformat()}:
        raise ValueError("market sentiment snapshot time must be exactly 10:00:00")
    numeric = values.loc[:, _COUNT_COLUMNS]
    if numeric.isna().any().any() or not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise ValueError("market sentiment contains non-finite counts")
    if (numeric < 0).any().any():
        raise ValueError("market sentiment contains negative counts")
    if not np.allclose(numeric.to_numpy(dtype=float), np.rint(numeric.to_numpy(dtype=float))):
        raise ValueError("market sentiment counts must be integers")
    if minimum_market_coverage is not None and (
        values["market_covered"] < minimum_market_coverage
    ).any():
        raise ValueError("market sentiment coverage is below the frozen absolute floor")
    if not 0 < minimum_prior_coverage_ratio <= 1:
        raise ValueError("minimum_prior_coverage_ratio must be in (0, 1]")
    prior_coverage = values["market_covered"].shift(1).rolling(
        20,
        min_periods=5,
    ).median()
    collapsed = values["market_covered"] < prior_coverage * minimum_prior_coverage_ratio
    if collapsed.fillna(False).any():
        raise ValueError("market sentiment coverage collapsed versus prior days")

    for prefix in ("market", "main", "twenty"):
        locked = values[f"{prefix}_locked_up"]
        touched = values[f"{prefix}_touched_up"]
        broken = values[f"{prefix}_broken_up"]
        if (locked > touched).any() or not np.array_equal(
            broken.to_numpy(dtype=int),
            (touched - locked).to_numpy(dtype=int),
        ):
            raise ValueError(f"{prefix} broken-up accounting is inconsistent")
        if (values[f"{prefix}_locked_down"] > values[f"{prefix}_covered"]).any():
            raise ValueError(f"{prefix} locked-down count exceeds coverage")
        if (touched > values[f"{prefix}_covered"]).any():
            raise ValueError(f"{prefix} touched-up count exceeds coverage")

    for prefix in ("promotion", "p12", "p23", "p3plus"):
        eligible = values[f"{prefix}_eligible"]
        observed = values[f"{prefix}_observed"]
        touched = values[f"{prefix}_touched"]
        at_limit = values[f"{prefix}_at_limit"]
        if (
            (at_limit > touched).any()
            or (touched > observed).any()
            or (observed > eligible).any()
        ):
            raise ValueError(f"{prefix} advancement accounting is inconsistent")

    if expected_dates is not None:
        expected = set(expected_dates)
        actual = set(values.index)
        missing_dates = sorted(expected - actual)
        extra_dates = sorted(actual - expected)
        if missing_dates:
            raise ValueError(f"missing sentiment dates: {[item.isoformat() for item in missing_dates]}")
        if extra_dates:
            raise ValueError(f"unexpected sentiment dates: {[item.isoformat() for item in extra_dates]}")
    return {
        "trade_days": len(values),
        "snapshot_time": SENTIMENT_SNAPSHOT_TIME.isoformat(),
        "first_trade_date": values.index[0].isoformat(),
        "last_trade_date": values.index[-1].isoformat(),
        "minimum_market_coverage": int(values["market_covered"].min()),
        "maximum_market_coverage": int(values["market_covered"].max()),
        "board_source_conflicts": int(values["board_source_conflicts"].sum()),
        "incomplete_promotion_days": int(
            (values["promotion_observed"] < values["promotion_eligible"]).sum()
        ),
        "point_in_time_accounting_valid": True,
    }


def _smoothed_rate(success: pd.Series, eligible: pd.Series) -> pd.Series:
    rate = (success.astype(float) + 0.5) / (eligible.astype(float) + 1.0)
    return rate.where(eligible > 0)


def _add_prior_robust_z(
    frame: pd.DataFrame,
    column: str,
    *,
    history_days: int,
    min_history_days: int,
) -> None:
    slots = pd.Series("daily", index=frame.index)
    location, mad = _rolling_prior_median_mad(
        pd.to_numeric(frame[column], errors="coerce"),
        slots,
        history_days=history_days,
        min_history_days=min_history_days,
    )
    scale = (1.4826 * mad).where(mad > 1e-12)
    frame[f"{column}_location"] = location
    frame[f"{column}_scale"] = scale
    frame[f"{column}_z"] = (frame[column] - location) / scale


def compute_causal_sentiment_features(
    frame: pd.DataFrame,
    *,
    history_days: int = SENTIMENT_HISTORY_DAYS,
    min_history_days: int = SENTIMENT_MIN_HISTORY_DAYS,
) -> pd.DataFrame:
    """Derive smoothed sentiment metrics using strictly prior-day baselines."""
    if not 1 <= min_history_days <= history_days:
        raise ValueError("min_history_days must be in [1, history_days]")
    validate_sentiment_panel(frame)
    result = _normalize_sentiment_frame(frame)

    for prefix in ("market", "main", "twenty"):
        result[f"{prefix}_limit_breadth"] = np.log(
            (result[f"{prefix}_locked_up"] + 0.5)
            / (result[f"{prefix}_locked_down"] + 0.5)
        )
    result["market_at_limit_quality"] = _smoothed_rate(
        result["market_locked_up"], result["market_touched_up"]
    )
    promotion_complete = result["promotion_observed"].eq(result["promotion_eligible"])
    result["promotion_at_limit_rate"] = _smoothed_rate(
        result["promotion_at_limit"], result["promotion_eligible"]
    ).where(promotion_complete)
    for prefix in ("p12", "p23", "p3plus"):
        complete = result[f"{prefix}_observed"].eq(result[f"{prefix}_eligible"])
        result[f"{prefix}_touch_rate"] = _smoothed_rate(
            result[f"{prefix}_touched"], result[f"{prefix}_eligible"]
        ).where(complete)
        result[f"{prefix}_at_limit_rate"] = _smoothed_rate(
            result[f"{prefix}_at_limit"], result[f"{prefix}_eligible"]
        ).where(complete)

    for column in (
        "market_limit_breadth",
        "main_limit_breadth",
        "twenty_limit_breadth",
        "market_at_limit_quality",
        "promotion_at_limit_rate",
    ):
        _add_prior_robust_z(
            result,
            column,
            history_days=history_days,
            min_history_days=min_history_days,
        )

    common = [
        result["market_limit_breadth_z"].clip(-3.0, 3.0),
        result["market_at_limit_quality_z"].clip(-3.0, 3.0),
        result["promotion_at_limit_rate_z"].clip(-3.0, 3.0),
    ]
    for segment in ("main", "twenty"):
        components = pd.concat(
            [*common, result[f"{segment}_limit_breadth_z"].clip(-3.0, 3.0)],
            axis=1,
        )
        raw_column = f"{segment}_composite_sentiment"
        result[raw_column] = components.mean(axis=1, skipna=False)
        _add_prior_robust_z(
            result,
            raw_column,
            history_days=history_days,
            min_history_days=min_history_days,
        )
    return result


def attach_sentiment_features(
    minute_frame: pd.DataFrame,
    sentiment_daily: pd.DataFrame,
) -> pd.DataFrame:
    """Attach the daily snapshot at and after 10:00 with symbol-specific segments."""
    if not isinstance(minute_frame.index, pd.DatetimeIndex):
        raise ValueError("minute data requires a DatetimeIndex")
    if "symbol" not in minute_frame:
        raise ValueError("minute data requires symbol")
    daily = sentiment_daily.copy()
    if "market_limit_breadth_z" not in daily:
        daily = compute_causal_sentiment_features(daily)
    daily = daily.sort_index(kind="stable")
    result = minute_frame.copy()
    day_keys = pd.Series(result.index.date, index=result.index)
    available = day_keys.isin(daily.index) & pd.Series(
        result.index.time >= SENTIMENT_SNAPSHOT_TIME,
        index=result.index,
    )

    attached_columns = [column for column in daily.columns if column != "snapshot_time"]
    for column in attached_columns:
        mapping = daily[column].to_dict()
        result[column] = day_keys.map(mapping)
        result.loc[~available, column] = np.nan

    segment = result["symbol"].map(SYMBOL_LIMIT_SEGMENT)
    result["segment_limit_breadth_z"] = np.where(
        segment.eq("main"),
        result.get("main_limit_breadth_z"),
        np.where(segment.eq("twenty"), result.get("twenty_limit_breadth_z"), np.nan),
    )
    result["composite_sentiment_z"] = np.where(
        segment.eq("main"),
        result.get("main_composite_sentiment_z"),
        np.where(
            segment.eq("twenty"),
            result.get("twenty_composite_sentiment_z"),
            np.nan,
        ),
    )
    result["sentiment_ready"] = available & segment.notna()
    return result


def _required_columns(frame: pd.DataFrame, names: Sequence[str], variant: str) -> None:
    missing = set(names) - set(frame.columns)
    if missing:
        raise ValueError(f"{variant} gate requires columns: {sorted(missing)}")


def apply_entry_gate(frame: pd.DataFrame, variant: ResearchVariant) -> pd.Series:
    """Apply the v4 controls and at most one incremental sentiment gate."""
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

    _required_columns(frame, ("volume_return_forecast_bps",), variant.name)
    forecast = pd.to_numeric(frame["volume_return_forecast_bps"], errors="coerce")
    volume_anchor = anchor & forecast.notna() & (zscore * forecast).lt(0.0)
    if variant.name == "volume_return_forecast":
        return volume_anchor

    feature_by_variant = {
        "volume_limit_breadth_alignment": "market_limit_breadth_z",
        "volume_board_promotion_alignment": "promotion_at_limit_rate_z",
        "volume_composite_market_sentiment": "composite_sentiment_z",
    }
    feature_name = feature_by_variant.get(variant.name)
    if feature_name is None:
        raise ValueError(f"unknown research variant: {variant.name}")
    _required_columns(frame, ("sentiment_ready", feature_name), variant.name)
    sentiment_ready = frame["sentiment_ready"].fillna(False).astype(bool)
    feature = pd.to_numeric(frame[feature_name], errors="coerce")
    adverse_alignment = np.sign(zscore) * feature
    return (
        volume_anchor
        & sentiment_ready
        & feature.notna()
        & adverse_alignment.le(SENTIMENT_GATE_THRESHOLD)
    )


class ResearchGateBacktester(IntradayTBacktester):
    """Gate entries while preserving the inherited active-pair restore path."""

    def __init__(self, variant: ResearchVariant) -> None:
        self.variant = variant

    def _prepare_frame(self, minute_data: pd.DataFrame, params: StrategyParams) -> pd.DataFrame:
        frame = super()._prepare_frame(minute_data, params)
        frame["ready"] = apply_entry_gate(frame, self.variant)
        return frame


def _sql_literal(value: str | Path) -> str:
    return str(value).replace("\\", "/").replace("'", "''")


def _dataset_glob(parquet_root: Path, dataset: str) -> str:
    root = parquet_root / dataset
    if not root.is_dir() or not next(root.rglob("*.parquet"), None):
        raise FileNotFoundError(f"required Parquet dataset is unavailable: {root}")
    return _sql_literal(root / "**" / "*.parquet")


def _expected_trade_dates_sqlite(
    db_path: Path,
    *,
    start_date: date,
    end_date: date,
) -> list[date]:
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        rows = connection.execute(
            "SELECT DISTINCT trade_date FROM stock_limit_prices "
            "WHERE symbol=? AND trade_date>=? AND trade_date<=? ORDER BY trade_date",
            ("603629.SH", start_date.isoformat(), end_date.isoformat()),
        ).fetchall()
    return [date.fromisoformat(str(row[0])[:10]) for row in rows]


def load_market_sentiment_daily(
    *,
    parquet_root: str | Path,
    db_path: str | Path,
    start_date: date,
    end_date: date,
    minute_source: str = "jq",
) -> pd.DataFrame:
    """Build causal 10:00 limit breadth and prior-board advancement snapshots."""
    if start_date > end_date:
        raise ValueError("start_date must not exceed end_date")
    if not minute_source or not all(character.isalnum() or character in "_-" for character in minute_source):
        raise ValueError("minute_source contains unsupported characters")
    root = Path(parquet_root).expanduser().resolve()
    database = Path(db_path).expanduser().resolve()
    if not database.is_file():
        raise FileNotFoundError(f"limit-price database does not exist: {database}")
    minute_glob = _dataset_glob(root, "klines_minute")
    detail_glob = _dataset_glob(root, "tushare_limit_list_d")
    step_glob = _dataset_glob(root, "tushare_limit_step")
    prior_start = start_date - timedelta(days=40)

    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("LOAD sqlite")
        connection.execute(
            f"ATTACH '{_sql_literal(database)}' AS sentiment_sqlite "
            "(TYPE SQLITE, READ_ONLY)"
        )
        query = f"""
        WITH daily_limits AS (
            SELECT symbol,
                   CAST(trade_date AS DATE) AS trade_date,
                   up_limit,
                   down_limit,
                   (up_limit - down_limit) / (up_limit + down_limit) AS limit_band
            FROM sentiment_sqlite.stock_limit_prices
            WHERE trade_date >= '{prior_start.isoformat()}'
              AND trade_date <= '{end_date.isoformat()}'
              AND up_limit > 0
              AND down_limit > 0
        ),
        calendar_history AS (
            SELECT DISTINCT trade_date
            FROM daily_limits
            WHERE symbol = '603629.SH'
        ),
        calendar AS (
            SELECT trade_date,
                   lag(trade_date) OVER (ORDER BY trade_date) AS prior_date
            FROM calendar_history
        ),
        bars AS (
            SELECT symbol,
                   CAST(datetime AS DATE) AS trade_date,
                   max(high) AS high_so_far,
                   min(low) AS low_so_far,
                   arg_max(close, datetime) AS last_price,
                   count(*) AS bar_count
            FROM parquet_scan('{minute_glob}', hive_partitioning=1)
            WHERE datetime >= TIMESTAMP '{start_date.isoformat()} 09:31:00'
              AND datetime <= TIMESTAMP '{end_date.isoformat()} 10:00:00'
              AND CAST(datetime AS TIME) BETWEEN TIME '09:31:00' AND TIME '10:00:00'
              AND source = '{minute_source}'
            GROUP BY symbol, CAST(datetime AS DATE)
        ),
        complete_snapshots AS (
            SELECT b.symbol,
                   b.trade_date,
                   b.high_so_far,
                   b.low_so_far,
                   b.last_price,
                   limits.up_limit,
                   limits.down_limit,
                   limits.limit_band,
                   b.high_so_far >= limits.up_limit - 0.005 AS touched_up,
                   b.low_so_far <= limits.down_limit + 0.005 AS touched_down,
                   abs(b.last_price - limits.up_limit) <= 0.005 AS locked_up,
                   abs(b.last_price - limits.down_limit) <= 0.005 AS locked_down
            FROM bars AS b
            JOIN daily_limits AS limits
              ON limits.symbol = b.symbol
             AND limits.trade_date = b.trade_date
            WHERE b.bar_count = 30
        ),
        standard_snapshots AS (
            SELECT *
            FROM complete_snapshots
            WHERE limit_band BETWEEN 0.08 AND 0.12
               OR limit_band BETWEEN 0.17 AND 0.22
        ),
        first_boards AS (
            SELECT DISTINCT trade_date_dt AS prior_date,
                            symbol,
                            1 AS prior_board,
                            1 AS source_priority
            FROM parquet_scan('{detail_glob}', hive_partitioning=1)
            WHERE trade_date_dt BETWEEN DATE '{prior_start.isoformat()}'
                                    AND DATE '{end_date.isoformat()}'
              AND "limit" = 'U'
              AND TRY_CAST(limit_times AS INTEGER) = 1
        ),
        higher_boards AS (
            SELECT DISTINCT trade_date_dt AS prior_date,
                            symbol,
                            TRY_CAST(nums AS INTEGER) AS prior_board,
                            2 AS source_priority
            FROM parquet_scan('{step_glob}', hive_partitioning=1)
            WHERE trade_date_dt BETWEEN DATE '{prior_start.isoformat()}'
                                    AND DATE '{end_date.isoformat()}'
              AND TRY_CAST(nums AS INTEGER) >= 2
              AND upper(coalesce(name, '')) NOT LIKE '%ST%'
        ),
        board_candidates AS (
            SELECT * FROM first_boards
            UNION ALL
            SELECT * FROM higher_boards
        ),
        board_conflicts AS (
            SELECT prior_date, symbol
            FROM board_candidates
            GROUP BY prior_date, symbol
            HAVING count(DISTINCT prior_board) > 1
        ),
        prior_boards AS (
            SELECT candidates.prior_date,
                   candidates.symbol,
                   arg_max(
                       candidates.prior_board,
                       candidates.source_priority * 1_000 + candidates.prior_board
                   ) AS prior_board
            FROM board_candidates AS candidates
            GROUP BY candidates.prior_date, candidates.symbol
        ),
        conflict_daily AS (
            SELECT calendar.trade_date,
                   count(*) AS board_source_conflicts
            FROM calendar
            JOIN board_conflicts AS conflicts
              ON conflicts.prior_date = calendar.prior_date
            WHERE calendar.trade_date BETWEEN DATE '{start_date.isoformat()}'
                                          AND DATE '{end_date.isoformat()}'
            GROUP BY calendar.trade_date
        ),
        eligible_promotion_rows AS (
            SELECT calendar.trade_date,
                   boards.symbol,
                   boards.prior_board,
                   limits.limit_band
            FROM calendar
            JOIN prior_boards AS boards
              ON boards.prior_date = calendar.prior_date
            JOIN daily_limits AS limits
              ON limits.symbol = boards.symbol
             AND limits.trade_date = calendar.trade_date
            WHERE calendar.trade_date BETWEEN DATE '{start_date.isoformat()}'
                                          AND DATE '{end_date.isoformat()}'
              AND (limits.limit_band BETWEEN 0.08 AND 0.12
                   OR limits.limit_band BETWEEN 0.17 AND 0.22)
        ),
        promotion_rows AS (
            SELECT eligible.trade_date,
                   eligible.symbol,
                   eligible.prior_board,
                   snapshots.symbol IS NOT NULL AS observed,
                   snapshots.touched_up,
                   snapshots.locked_up
            FROM eligible_promotion_rows AS eligible
            LEFT JOIN standard_snapshots AS snapshots
              ON snapshots.symbol = eligible.symbol
             AND snapshots.trade_date = eligible.trade_date
        ),
        promotion_daily AS (
            SELECT trade_date,
                   count(*) AS promotion_eligible,
                   count_if(observed) AS promotion_observed,
                   count_if(observed AND touched_up) AS promotion_touched,
                   count_if(observed AND locked_up) AS promotion_at_limit,
                   count(*) FILTER (WHERE prior_board = 1) AS p12_eligible,
                   count_if(observed) FILTER (WHERE prior_board = 1) AS p12_observed,
                   count_if(observed AND touched_up)
                       FILTER (WHERE prior_board = 1) AS p12_touched,
                   count_if(observed AND locked_up)
                       FILTER (WHERE prior_board = 1) AS p12_at_limit,
                   count(*) FILTER (WHERE prior_board = 2) AS p23_eligible,
                   count_if(observed) FILTER (WHERE prior_board = 2) AS p23_observed,
                   count_if(observed AND touched_up)
                       FILTER (WHERE prior_board = 2) AS p23_touched,
                   count_if(observed AND locked_up)
                       FILTER (WHERE prior_board = 2) AS p23_at_limit,
                   count(*) FILTER (WHERE prior_board >= 3) AS p3plus_eligible,
                   count_if(observed) FILTER (WHERE prior_board >= 3) AS p3plus_observed,
                   count_if(observed AND touched_up)
                       FILTER (WHERE prior_board >= 3) AS p3plus_touched,
                   count_if(observed AND locked_up)
                       FILTER (WHERE prior_board >= 3) AS p3plus_at_limit
            FROM promotion_rows
            GROUP BY trade_date
        ),
        snapshot_daily AS (
            SELECT trade_date,
                   count(*) AS market_covered,
                   count_if(locked_up) AS market_locked_up,
                   count_if(locked_down) AS market_locked_down,
                   count_if(touched_up) AS market_touched_up,
                   count_if(touched_up AND NOT locked_up) AS market_broken_up,
                   count_if(limit_band BETWEEN 0.08 AND 0.12) AS main_covered,
                   count_if(limit_band BETWEEN 0.08 AND 0.12 AND locked_up)
                       AS main_locked_up,
                   count_if(limit_band BETWEEN 0.08 AND 0.12 AND locked_down)
                       AS main_locked_down,
                   count_if(limit_band BETWEEN 0.08 AND 0.12 AND touched_up)
                       AS main_touched_up,
                   count_if(limit_band BETWEEN 0.08 AND 0.12 AND touched_up AND NOT locked_up)
                       AS main_broken_up,
                   count_if(limit_band BETWEEN 0.17 AND 0.22) AS twenty_covered,
                   count_if(limit_band BETWEEN 0.17 AND 0.22 AND locked_up)
                       AS twenty_locked_up,
                   count_if(limit_band BETWEEN 0.17 AND 0.22 AND locked_down)
                       AS twenty_locked_down,
                   count_if(limit_band BETWEEN 0.17 AND 0.22 AND touched_up)
                       AS twenty_touched_up,
                   count_if(limit_band BETWEEN 0.17 AND 0.22 AND touched_up AND NOT locked_up)
                       AS twenty_broken_up
            FROM standard_snapshots
            GROUP BY trade_date
        )
        SELECT snapshots.trade_date,
               '10:00:00' AS snapshot_time,
               snapshots.* EXCLUDE (trade_date),
               coalesce(conflicts.board_source_conflicts, 0) AS board_source_conflicts,
               coalesce(promotion.promotion_eligible, 0) AS promotion_eligible,
               coalesce(promotion.promotion_observed, 0) AS promotion_observed,
               coalesce(promotion.promotion_touched, 0) AS promotion_touched,
               coalesce(promotion.promotion_at_limit, 0) AS promotion_at_limit,
               coalesce(promotion.p12_eligible, 0) AS p12_eligible,
               coalesce(promotion.p12_observed, 0) AS p12_observed,
               coalesce(promotion.p12_touched, 0) AS p12_touched,
               coalesce(promotion.p12_at_limit, 0) AS p12_at_limit,
               coalesce(promotion.p23_eligible, 0) AS p23_eligible,
               coalesce(promotion.p23_observed, 0) AS p23_observed,
               coalesce(promotion.p23_touched, 0) AS p23_touched,
               coalesce(promotion.p23_at_limit, 0) AS p23_at_limit,
               coalesce(promotion.p3plus_eligible, 0) AS p3plus_eligible,
               coalesce(promotion.p3plus_observed, 0) AS p3plus_observed,
               coalesce(promotion.p3plus_touched, 0) AS p3plus_touched,
               coalesce(promotion.p3plus_at_limit, 0) AS p3plus_at_limit
        FROM snapshot_daily AS snapshots
        LEFT JOIN promotion_daily AS promotion USING (trade_date)
        LEFT JOIN conflict_daily AS conflicts USING (trade_date)
        WHERE snapshots.trade_date BETWEEN DATE '{start_date.isoformat()}'
                                       AND DATE '{end_date.isoformat()}'
        ORDER BY snapshots.trade_date
        """
        result = connection.execute(query).fetchdf()
    finally:
        connection.close()

    normalized = _normalize_sentiment_frame(result)
    expected_dates = _expected_trade_dates_sqlite(
        database,
        start_date=start_date,
        end_date=end_date,
    )
    if not expected_dates:
        raise ValueError("target-stock trading calendar is empty")
    validate_sentiment_panel(normalized, expected_dates=expected_dates)
    return normalized


def market_sentiment_fingerprint(frame: pd.DataFrame) -> str:
    """Return an order-stable fingerprint for raw or featured sentiment days."""
    values = frame.copy().sort_index(kind="stable")
    values.index = pd.Index(
        [pd.Timestamp(item).date().isoformat() for item in values.index],
        name="trade_date",
    )
    values = values.reset_index()
    ordered = values.loc[:, sorted(values.columns)]
    payload = ordered.to_csv(
        index=False,
        na_rep="<NA>",
        float_format="%.12g",
        lineterminator="\n",
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
    result = ResearchGateBacktester(variant).run(
        sample,
        BacktestConfig(
            initial_capital=initial_capital,
            base_quantities=base_quantities,
            params=variant.params,
            cost=replace(CostModel(), slippage_bps=scenario.slippage_bps),
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
            **{field: trade.get(field) for field in v4.SIGNAL_LEDGER_FIELDS},
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
        "signal_ledger_sha256": v4.signal_ledger_fingerprint(result["trades"]),
    }
    return run, ledger


def build_recommendation(runs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Summarise screens while permanently retaining research-only status."""
    recommendation = v4.build_recommendation(runs)
    volume_runs = {
        (str(run.get("fold")), str(run.get("scenario"))): float(
            run.get("metrics", {}).get("net_t_pnl", 0.0)
        )
        for run in runs
        if run.get("variant") == "volume_return_forecast"
    }
    for name, screen in recommendation.get("screens", {}).items():
        if not str(name).startswith("volume_") or name == "volume_return_forecast":
            continue
        selected = [run for run in runs if run.get("variant") == name]
        scenario_increment: dict[str, float] = {}
        for scenario in dict.fromkeys(str(run.get("scenario")) for run in selected):
            candidate = sum(
                float(run.get("metrics", {}).get("net_t_pnl", 0.0))
                for run in selected
                if run.get("scenario") == scenario
            )
            baseline = sum(
                value
                for (fold_name, scenario_name), value in volume_runs.items()
                if scenario_name == scenario and fold_name
            )
            scenario_increment[scenario] = round(candidate - baseline, 4)
        screen["incremental_vs_volume_by_scenario"] = scenario_increment
        screen["nominal_folds_improved_vs_volume"] = sum(
            float(run.get("metrics", {}).get("net_t_pnl", 0.0))
            > volume_runs.get((str(run.get("fold")), "nominal"), float("inf"))
            for run in selected
            if run.get("scenario") == "nominal"
        )
    recommendation["sentiment_comparison_baseline"] = "volume_return_forecast"
    recommendation["reason"] = (
        "market-sentiment hypotheses and all available history are contaminated by "
        "research; these two-stock diagnostics cannot change paper or live defaults"
    )
    return recommendation


def _implementation_manifest() -> dict[str, str]:
    paths = (
        Path(__file__),
        ROOT / "backend" / "app" / "scripts" / "research_intraday_t_v4.py",
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
    payload = {"files": _implementation_manifest(), "runtime": _runtime_versions()}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def write_artifacts(
    report: dict[str, Any],
    output_dir: str | Path,
    *,
    sentiment_daily: pd.DataFrame,
    signal_ledger: Sequence[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Stage every artifact and replace research.json exactly once, last."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "research.json"
    runs_path = output / "runs.csv"
    ledger_path = output / "signal_ledger.csv"
    sentiment_path = output / "market_sentiment_daily.csv"
    token = uuid4().hex
    json_temp = output / f".research-{token}.json.tmp"
    runs_temp = output / f".runs-{token}.csv.tmp"
    ledger_temp = output / f".signal-ledger-{token}.csv.tmp"
    sentiment_temp = output / f".market-sentiment-{token}.csv.tmp"

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

    ledger_rows = list(
        signal_ledger if signal_ledger is not None else report.get("signal_ledger", [])
    )
    ledger_fields = ["fold", "variant", "scenario", *v4.SIGNAL_LEDGER_FIELDS]
    with ledger_temp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ledger_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ledger_rows)

    persisted_sentiment = sentiment_daily.copy().sort_index(kind="stable").reset_index()
    persisted_sentiment["trade_date"] = persisted_sentiment["trade_date"].map(
        lambda value: pd.Timestamp(value).date().isoformat()
    )
    persisted_sentiment.to_csv(
        sentiment_temp,
        index=False,
        na_rep="",
        float_format="%.12g",
        lineterminator="\n",
    )
    report["artifact_integrity"] = {
        "commit_marker": "research.json",
        "runs_csv_sha256": hashlib.sha256(runs_temp.read_bytes()).hexdigest(),
        "signal_ledger_csv_sha256": hashlib.sha256(ledger_temp.read_bytes()).hexdigest(),
        "market_sentiment_daily_csv_sha256": hashlib.sha256(
            sentiment_temp.read_bytes()
        ).hexdigest(),
    }
    persisted_report = dict(report)
    persisted_report.pop("signal_ledger", None)
    json_temp.write_text(
        json.dumps(
            persisted_report,
            ensure_ascii=False,
            indent=2,
            default=_json_default,
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    runs_temp.replace(runs_path)
    ledger_temp.replace(ledger_path)
    sentiment_temp.replace(sentiment_path)
    json_temp.replace(json_path)
    return {
        "json": str(json_path.resolve()),
        "csv": str(runs_path.resolve()),
        "signal_ledger": str(ledger_path.resolve()),
        "market_sentiment_daily": str(sentiment_path.resolve()),
    }


def run_research(
    *,
    start_date: date = DEFAULT_START_DATE,
    end_date: date = DEFAULT_END_DATE,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    initial_capital: float = 1_000_000.0,
    base_quantities: dict[str, int] | None = None,
    limit_price_db_path: str | Path | None = None,
    parquet_root: str | Path | None = None,
    store: MarketDataStore | None = None,
    index_data: pd.DataFrame | None = None,
    sentiment_data: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Run the frozen two-stock v5 retrospective sentiment diagnostics."""
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
    stocks = v4._normalize_stock_minute_data(raw_stocks)
    if index_data is None:
        indexes = v4.load_index_minute_data(
            symbols=benchmark_symbols,
            start_date=start_date,
            end_date=end_date,
        )
    elif {"time", "money"}.issubset(index_data.columns):
        indexes = v4.normalize_index_minute_data(index_data)
    else:
        indexes = v4._normalize_index_frame(index_data)
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
    database = Path(limit_price_db_path or _sqlite_path_from_settings()).resolve()
    prices = load_limit_prices_sqlite(
        database,
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

    raw_sentiment = (
        load_market_sentiment_daily(
            parquet_root=parquet_root or settings.parquet_data_dir,
            db_path=database,
            start_date=start_date,
            end_date=end_date,
        )
        if sentiment_data is None
        else _normalize_sentiment_frame(sentiment_data)
    )
    sentiment_quality = validate_sentiment_panel(
        raw_sentiment,
        expected_dates=trade_dates,
        minimum_market_coverage=4_500 if sentiment_data is None else None,
    )
    sentiment_features = compute_causal_sentiment_features(raw_sentiment)

    featured = v4.compute_causal_gate_features(stocks, indexes)
    reference_breaks = _reference_break_keys(stocks, prices)
    featured = _mark_reference_breaks(featured, reference_breaks)
    featured = attach_sentiment_features(featured, sentiment_features)
    variants = build_variants()
    scenarios = build_stress_scenarios()
    quantities = v4.resolve_research_base_quantities(base_quantities)
    configuration = {
        "research_period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "initial_capital": initial_capital,
        "benchmark_map": BENCHMARK_MAP,
        "base_quantities": quantities,
        "sentiment_snapshot_time": SENTIMENT_SNAPSHOT_TIME.isoformat(),
        "sentiment_history_days": SENTIMENT_HISTORY_DAYS,
        "sentiment_min_history_days": SENTIMENT_MIN_HISTORY_DAYS,
        "sentiment_gate_threshold": SENTIMENT_GATE_THRESHOLD,
        "canonical_minute_source": "jq",
        "limit_segments": {"main": [0.08, 0.12], "twenty": [0.17, 0.22]},
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
    v4.validate_run_matrix(
        runs,
        fold_names=[fold.name for fold in folds],
        variant_names=[variant.name for variant in variants],
        scenario_names=[scenario.name for scenario in scenarios],
    )
    v4._validate_signal_ledgers(runs)
    implementation_at_end = _implementation_fingerprint()
    if implementation_at_end != implementation_at_start:
        raise RuntimeError("intraday-T research implementation changed during the run")

    report: dict[str, Any] = {
        "schema_version": 3,
        "generated_at": datetime.now(UTC).isoformat(),
        "research_status": "retrospective_exploration",
        "protocol": {
            "fixed_before_run": True,
            "automatic_parameter_selection": False,
            "auto_promotion_enabled": False,
            "holdout_is_unseen": False,
            "fold_method": "252-day warmup then contiguous 42/42/remainder blocks",
            "sentiment_causality": (
                "09:31-10:00 completed bars plus prior-day board cohorts; "
                "all robust baselines use strictly prior days"
            ),
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
            "market_sentiment": sentiment_quality,
            "limit_price_symbol_days": len(prices),
            "missing_limit_price_symbol_days": 0,
            "reference_break_symbol_days_excluded": sorted(reference_breaks),
            "stock_fingerprint": frame_fingerprint(stocks),
            "benchmark_fingerprint": frame_fingerprint(indexes),
            "limit_price_fingerprint": limit_price_fingerprint(prices),
            "market_sentiment_raw_fingerprint": market_sentiment_fingerprint(raw_sentiment),
            "market_sentiment_feature_fingerprint": market_sentiment_fingerprint(
                sentiment_features
            ),
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
            "limit_breadth": "log smoothed 10:00 locked-up to locked-down ratio",
            "at_limit_quality": (
                "smoothed share whose 10:00 last price remains at the exact upper limit"
            ),
            "promotion_at_limit_rate": (
                "prior-day board cohort whose 10:00 last price remains at upper limit"
            ),
            "board_ladders": "1-to-2, 2-to-3 and 3-plus touch/at-limit proxy rates",
            "composite": (
                "equal-weight clipped global, matching-segment, at-limit and promotion z"
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
        "market_sentiment_daily": str(
            (Path(output_dir) / "market_sentiment_daily.csv").resolve()
        ),
    }
    write_artifacts(
        report,
        output_dir,
        sentiment_daily=sentiment_features,
        signal_ledger=signal_ledger,
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", type=date.fromisoformat, default=DEFAULT_END_DATE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit-price-db", type=Path, default=None)
    parser.add_argument("--parquet-root", type=Path, default=None)
    args = parser.parse_args()
    report = run_research(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        limit_price_db_path=args.limit_price_db,
        parquet_root=args.parquet_root,
    )
    print(json.dumps(report["recommendation"], ensure_ascii=False, indent=2))
    print(json.dumps(report["artifacts"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

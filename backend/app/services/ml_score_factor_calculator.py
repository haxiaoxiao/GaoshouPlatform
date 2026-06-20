"""Precompute external QuantaAlpha ML score factors into factor_values."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd

from app.services.factor_value_store import factor_params_hash, get_factor_value_store
from app.db.duckdb import get_duckdb


QUANTA_RESULTS = Path(r"E:\Projects\QuantaAlpha\data\results")
ML_DIR = QUANTA_RESULTS / "ml_multifactor_strategies_20260618"
ALGO_DIR = QUANTA_RESULTS / "ml_algorithm_benchmark_20260619"


ML_SCORE_FACTOR_SPECS: dict[str, dict[str, Any]] = {
    "ml_ltr_core_blend_score": {
        "source_path": ML_DIR / "ml_multifactor_predictions.parquet",
        "expand_to_daily": True,
        "models": {
            "all_factor_post2024_lgbm:lgbm_lambdarank": 0.55,
            "long_technical_lgbm:lgbm_lambdarank": 0.45,
        },
    },
    "ml_ltr_long_technical_score": {
        "source_path": ML_DIR / "ml_multifactor_predictions.parquet",
        "expand_to_daily": True,
        "models": {
            "long_technical_lgbm:lgbm_lambdarank": 1.0,
        },
    },
    "ml_smallcap_diverse_stack_score": {
        "source_path": ALGO_DIR / "ml_algorithm_ensemble_predictions.parquet",
        "expand_to_daily": True,
        "models": {
            "algorithm_rank_ensemble:diverse_lgbm_xgb_tree_stack": 1.0,
        },
    },
}


def precompute_ml_score_factors(
    *,
    factor_names: Sequence[str],
    start_date: str | date,
    end_date: str | date,
    symbols: Sequence[str] | None = None,
    progress_callback: Callable[[float, str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    names = [str(name) for name in factor_names if str(name)]
    unknown = [name for name in names if name not in ML_SCORE_FACTOR_SPECS]
    if unknown:
        raise ValueError(f"Unsupported ML score factors: {unknown}")
    start = pd.Timestamp(start_date).date()
    end = pd.Timestamp(end_date).date()
    symbol_set = {str(symbol) for symbol in symbols or [] if str(symbol).strip()}

    frames: list[pd.DataFrame] = []
    rows: dict[str, int] = {}
    errors: dict[str, str] = {}
    total = max(1, len(names))
    for idx, factor_name in enumerate(names, start=1):
        _report(progress_callback, (idx - 1) / total, "计算ML分数因子", {"factor_name": factor_name, "current": idx, "total": total})
        try:
            frame = build_ml_score_frame(factor_name, start_date=start, end_date=end, symbols=symbol_set or None)
        except Exception as exc:
            rows[factor_name] = 0
            errors[factor_name] = str(exc)
            continue
        rows[factor_name] = int(len(frame))
        if not frame.empty:
            frames.append(frame)

    written = 0
    if frames:
        body = pd.concat(frames, ignore_index=True)
        writer = get_factor_value_store().batch_writer(batch_size=200_000)
        written = writer.write_frame(body)
    _report(progress_callback, 1.0, "完成", {"rows_written": written})
    failed = [name for name, message in errors.items() if message]
    return {
        "symbols": len(symbol_set),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "factor_names": names,
        "rows": rows,
        "rows_written": int(written),
        "requested_factor_count": len(names),
        "written_factor_count": sum(1 for count in rows.values() if count > 0),
        "zero_row_factor_count": sum(1 for count in rows.values() if count <= 0),
        "zero_row_factor_names": [name for name, count in rows.items() if count <= 0],
        "failed_factor_names": failed,
        "errors": errors,
    }


def build_ml_score_frame(
    factor_name: str,
    *,
    start_date: date,
    end_date: date,
    symbols: set[str] | None = None,
) -> pd.DataFrame:
    spec = ML_SCORE_FACTOR_SPECS[factor_name]
    source = _read_predictions(Path(spec["source_path"]))
    pieces: list[pd.DataFrame] = []
    for key, weight in spec["models"].items():
        experiment, model = key.split(":", 1)
        part = source[source["experiment"].eq(experiment) & source["model"].eq(model)].copy()
        if part.empty:
            raise RuntimeError(f"No ML prediction rows for {key}")
        column = key.replace(":", "__")
        part[column] = part.groupby("trade_date", sort=False)["score"].rank(pct=True)
        pieces.append(part[["symbol", "trade_date", column]].copy())

    merged = pieces[0]
    for part in pieces[1:]:
        merged = merged.merge(part, on=["symbol", "trade_date"], how="inner")
    if merged.empty:
        return _empty_frame()
    # Keep the latest signal before start_date so the monthly ML signal can be
    # carried forward onto daily AKQuant rebalance dates inside the request.
    signal_dates = sorted(pd.Timestamp(item).date() for item in merged["trade_date"].drop_duplicates())
    prior_dates = [item for item in signal_dates if item < start_date]
    earliest_needed = prior_dates[-1] if prior_dates else start_date
    merged = merged[(merged["trade_date"] >= earliest_needed) & (merged["trade_date"] <= end_date)].copy()
    if symbols:
        merged = merged[merged["symbol"].isin(symbols)].copy()
    if merged.empty:
        return _empty_frame()

    total_weight = float(sum(spec["models"].values()))
    score = pd.Series(0.0, index=merged.index, dtype="float64")
    for key, weight in spec["models"].items():
        column = key.replace(":", "__")
        score += (float(weight) / total_weight) * pd.to_numeric(merged[column], errors="coerce")

    out = merged[["symbol", "trade_date"]].copy()
    out["factor_name"] = factor_name
    out["value"] = score.clip(0.0, 1.0)
    out["as_of_time"] = ""
    out["params_hash"] = factor_params_hash({})
    out["source"] = "quantaalpha.ml_walk_forward"
    out["created_at"] = datetime.now()
    out = out.replace([np.inf, -np.inf], np.nan).dropna(subset=["value"])
    if bool(spec.get("expand_to_daily", True)):
        out = _expand_signal_to_daily(out, start_date=start_date, end_date=end_date)
    return out


def _expand_signal_to_daily(frame: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    if frame.empty:
        return frame
    trading_dates = _trading_dates(start_date, end_date)
    if not trading_dates:
        return frame
    signal_dates = sorted(pd.Timestamp(item).date() for item in frame["trade_date"].drop_duplicates())
    pieces: list[pd.DataFrame] = []
    for idx, signal_date in enumerate(signal_dates):
        next_signal = signal_dates[idx + 1] if idx + 1 < len(signal_dates) else None
        effective_dates = [
            item for item in trading_dates
            if item >= signal_date and (next_signal is None or item < next_signal)
        ]
        if not effective_dates:
            continue
        base = frame[frame["trade_date"].eq(signal_date)].drop(columns=["trade_date"]).copy()
        for trade_date in effective_dates:
            part = base.copy()
            part["trade_date"] = trade_date
            pieces.append(part)
    if not pieces:
        return frame
    out = pd.concat(pieces, ignore_index=True)
    return out[frame.columns].copy()


def _trading_dates(start_date: date, end_date: date) -> list[date]:
    data_glob = str(Path(r"E:\Projects\Data\parquet\klines_daily\**\*.parquet")).replace("\\", "/")
    rows = get_duckdb().execute(
        f"""
        SELECT DISTINCT trade_date
        FROM read_parquet('{data_glob}', hive_partitioning=true, union_by_name=true)
        WHERE trade_date >= DATE '{start_date.isoformat()}'
          AND trade_date <= DATE '{end_date.isoformat()}'
        ORDER BY trade_date
        """
    ).fetchall()
    return [pd.Timestamp(row[0]).date() for row in rows]


def _read_predictions(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path)
    required = {"symbol", "trade_date", "score", "experiment", "model"}
    missing = required - set(frame.columns)
    if missing:
        raise KeyError(f"Missing prediction columns in {path}: {sorted(missing)}")
    out = frame[list(required)].copy()
    out["symbol"] = out["symbol"].astype(str)
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.date
    out["score"] = pd.to_numeric(out["score"], errors="coerce")
    return out.dropna(subset=["score"])


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol", "trade_date", "factor_name", "value", "as_of_time", "params_hash", "source", "created_at"])


def _report(
    progress_callback: Callable[[float, str, dict[str, Any]], None] | None,
    progress: float,
    stage: str,
    meta: dict[str, Any],
) -> None:
    if progress_callback is not None:
        progress_callback(progress, stage, meta)

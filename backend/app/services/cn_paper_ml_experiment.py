"""Offline experiment helpers for AI/deep-learning paper ideas."""

from __future__ import annotations

from datetime import date
from typing import Any, Sequence

import pandas as pd

from app.services.factor_catalog import CN_PAPER_FACTOR_SPECS
from app.services.factor_value_store import FactorValueStore, get_factor_value_store


CN_PAPER_EXPERIMENT_SPECS: list[dict[str, Any]] = [
    {
        "paper_ids": [32],
        "name": "price_fundamental_mixed_features",
        "model_family": ["lasso", "mlp"],
        "feature_groups": ["cn_paper_fundamental", "cn_paper_daily_events"],
        "default_factor_names": [
            "paper_composite_value",
            "paper_growth_quality_score",
            "paper_financial_health_score",
            "paper_reversal_20d",
            "paper_rsi_reversal_score",
        ],
        "status": "offline_only",
        "target_policy": "Use forward returns generated inside the experiment window; do not store labels in factor cache.",
    },
    {
        "paper_ids": [33, 41],
        "name": "multi_model_ensemble_features",
        "model_family": ["lasso", "mlp", "stacking"],
        "feature_groups": ["cn_paper_implemented"],
        "default_factor_names": list(CN_PAPER_FACTOR_SPECS),
        "status": "offline_only",
        "target_policy": "Train/validation/test splits must be time ordered and persisted with model version metadata.",
    },
    {
        "paper_ids": [34],
        "name": "deepseek_factor_review",
        "model_family": ["llm_review"],
        "feature_groups": ["cn_paper_daily_events", "alpha101"],
        "default_factor_names": [
            "paper_overnight_turnover_corr",
            "paper_high_low_volume_event",
            "paper_new_high_anchor",
            "paper_reversal_20d",
        ],
        "status": "offline_only",
        "target_policy": "LLM output is treated as candidate factor metadata, not as executable trading code.",
    },
    {
        "paper_ids": [36],
        "name": "ai_factor_discovery_snapshot",
        "model_family": ["feature_search", "gru"],
        "feature_groups": ["cn_paper_implemented", "research_factor_ideas", "alpha101"],
        "default_factor_names": [
            "paper_size_rotation_score",
            "paper_value_growth_rotation_score",
            "paper_asset_allocation_proxy",
            "paper_trend_fund_support",
        ],
        "status": "offline_only",
        "target_policy": "Sequence labels must be created from strictly future returns after feature timestamps.",
    },
]


def list_cn_paper_experiment_specs() -> list[dict[str, Any]]:
    return [dict(item) for item in CN_PAPER_EXPERIMENT_SPECS]


def build_factor_feature_snapshot(
    *,
    factor_names: Sequence[str],
    trade_dates: Sequence[date],
    symbols: Sequence[str] | None = None,
    store: FactorValueStore | None = None,
) -> pd.DataFrame:
    names = [str(name) for name in factor_names if str(name).strip()]
    dates = sorted({item for item in trade_dates})
    if not names:
        raise ValueError("factor_names must not be empty")
    if not dates:
        raise ValueError("trade_dates must not be empty")
    unknown = [name for name in names if name not in CN_PAPER_FACTOR_SPECS and not name.startswith("alpha101_") and not name.startswith("research_")]
    if unknown:
        raise ValueError(f"Unsupported experiment factor names: {unknown}")

    factor_store = store or get_factor_value_store()
    values = factor_store.load(
        factor_names=names,
        start_date=dates[0],
        end_date=dates[-1],
        symbols=symbols,
    )
    if values.empty:
        columns = ["trade_date", "symbol", *names, "feature_coverage"]
        return pd.DataFrame(columns=columns)

    values = values[values["trade_date"].isin(dates)].copy()
    if values.empty:
        columns = ["trade_date", "symbol", *names, "feature_coverage"]
        return pd.DataFrame(columns=columns)
    values["symbol"] = values["symbol"].astype(str)
    values["factor_name"] = values["factor_name"].astype(str)
    values["value"] = pd.to_numeric(values["value"], errors="coerce")
    matrix = values.pivot_table(
        index=["trade_date", "symbol"],
        columns="factor_name",
        values="value",
        aggfunc="last",
    )
    for name in names:
        if name not in matrix.columns:
            matrix[name] = pd.NA
    matrix = matrix[names].reset_index()
    matrix["feature_coverage"] = matrix[names].notna().sum(axis=1) / float(len(names))
    return matrix.sort_values(["trade_date", "symbol"]).reset_index(drop=True)


def summarize_feature_snapshot(frame: pd.DataFrame, factor_names: Sequence[str]) -> dict[str, Any]:
    names = [str(name) for name in factor_names if str(name).strip()]
    if frame.empty:
        return {
            "rows": 0,
            "symbols": 0,
            "trade_dates": 0,
            "factor_names": names,
            "mean_feature_coverage": 0.0,
        }
    return {
        "rows": int(len(frame)),
        "symbols": int(frame["symbol"].nunique()),
        "trade_dates": int(frame["trade_date"].nunique()),
        "factor_names": names,
        "mean_feature_coverage": float(pd.to_numeric(frame["feature_coverage"], errors="coerce").mean()),
    }

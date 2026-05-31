from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.services.factor_value_store import (
    FactorValueStore,
    factor_params_hash,
    get_factor_value_store,
)


def _sqlite_db_path() -> Path:
    return Path(settings.data_dir) / "gaoshou.db"


def _load_daily(symbols: Sequence[str], start_date: date, end_date: date, lookback_days: int = 370) -> pd.DataFrame:
    store = get_market_data_store()
    daily = store.load_daily(
        list(symbols),
        start_date - timedelta(days=lookback_days),
        end_date,
        columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"],
    )
    if daily.empty:
        return pd.DataFrame()
    frame = daily.reset_index() if "trade_date" not in daily.columns else daily.reset_index(drop=True)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    for column in ("open", "high", "low", "close", "volume", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["symbol", "trade_date", "close"])
    return frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def _load_financial(symbols: Sequence[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in symbols)
    sql = f"""
        SELECT symbol, report_date, revenue, net_profit, gross_margin, total_assets, total_liability, total_equity
        FROM financial_data
        WHERE symbol IN ({placeholders})
        ORDER BY symbol, report_date
    """
    with sqlite3.connect(_sqlite_db_path()) as conn:
        return pd.read_sql_query(sql, conn, params=list(symbols))


def _align_financial_asof(daily: pd.DataFrame, financial: pd.DataFrame) -> pd.DataFrame:
    if daily.empty or financial.empty:
        return daily.copy()
    financial = financial.copy()
    financial["report_date"] = pd.to_datetime(financial["report_date"]).astype("datetime64[ns]")
    daily_frame = daily.copy()
    daily_frame["trade_date_ts"] = pd.to_datetime(daily_frame["trade_date"]).astype("datetime64[ns]")
    parts: list[pd.DataFrame] = []
    for symbol, trade_group in daily_frame.groupby("symbol", sort=False):
        fin_group = financial[financial["symbol"] == symbol].sort_values("report_date")
        if fin_group.empty:
            parts.append(trade_group)
            continue
        merged = pd.merge_asof(
            trade_group.sort_values("trade_date_ts"),
            fin_group,
            left_on="trade_date_ts",
            right_on="report_date",
            direction="backward",
        )
        if "symbol_x" in merged.columns:
            merged["symbol"] = merged["symbol_x"]
            merged = merged.drop(columns=["symbol_x"], errors="ignore")
        merged = merged.drop(columns=["symbol_y"], errors="ignore")
        parts.append(merged)
    aligned = pd.concat(parts, ignore_index=True) if parts else daily_frame
    return aligned.drop(columns=["trade_date_ts"], errors="ignore")


def _cross_sectional_market_returns(daily: pd.DataFrame) -> pd.Series:
    returns = daily.groupby("symbol")["close"].pct_change()
    trade_dates = pd.to_datetime(daily["trade_date"])
    return returns.groupby(trade_dates).transform("mean")


def _rolling_beta(asset_return: pd.Series, market_return: pd.Series, window: int) -> pd.Series:
    cov = asset_return.groupby(level=0).rolling(window).cov(market_return.groupby(level=0).rolling(window).mean().droplevel(0)).droplevel(0)
    var = market_return.groupby(level=0).rolling(window).var().droplevel(0)
    return cov / var.replace(0, np.nan)


def _compute_research_factor(frame: pd.DataFrame, factor_name: str, market_return: pd.Series) -> pd.Series:
    close_return = frame.groupby("symbol")["close"].pct_change()
    if factor_name == "research_short_reversal":
        return -(frame.groupby("symbol")["close"].pct_change(5))
    if factor_name == "research_turnover_liquidity":
        avg_amount = frame.groupby("symbol")["amount"].transform(lambda series: series.rolling(20).mean())
        avg_volume = frame.groupby("symbol")["volume"].transform(lambda series: series.rolling(20).mean())
        return avg_amount / avg_volume.replace(0, np.nan)
    if factor_name == "research_low_beta":
        rolling_cov = close_return.groupby(frame["symbol"]).rolling(252).cov(market_return).droplevel(0)
        rolling_var = market_return.groupby(frame["symbol"]).rolling(252).var().droplevel(0)
        return -(rolling_cov / rolling_var.replace(0, np.nan))
    if factor_name == "research_idiosyncratic_volatility":
        rolling_cov = close_return.groupby(frame["symbol"]).rolling(60).cov(market_return).droplevel(0)
        rolling_var = market_return.groupby(frame["symbol"]).rolling(60).var().droplevel(0)
        beta = rolling_cov / rolling_var.replace(0, np.nan)
        residual = close_return - beta * market_return
        return -residual.groupby(frame["symbol"]).transform(lambda series: series.rolling(60).std())
    if factor_name == "research_residual_momentum":
        rolling_cov = close_return.groupby(frame["symbol"]).rolling(60).cov(market_return).droplevel(0)
        rolling_var = market_return.groupby(frame["symbol"]).rolling(60).var().droplevel(0)
        beta = rolling_cov / rolling_var.replace(0, np.nan)
        residual = close_return - beta * market_return
        long_term = residual.groupby(frame["symbol"]).transform(lambda series: series.rolling(252).sum())
        short_term = residual.groupby(frame["symbol"]).transform(lambda series: series.rolling(21).sum())
        return long_term - short_term
    if factor_name == "research_gross_profitability":
        gross_margin = pd.to_numeric(frame.get("gross_margin"), errors="coerce") / 100.0
        revenue = pd.to_numeric(frame.get("revenue"), errors="coerce")
        total_assets = pd.to_numeric(frame.get("total_assets"), errors="coerce")
        return (revenue * gross_margin) / total_assets.replace(0, np.nan)
    if factor_name == "research_asset_growth":
        total_assets = pd.to_numeric(frame.get("total_assets"), errors="coerce")
        previous = total_assets.groupby(frame["symbol"]).shift(4)
        return total_assets / previous.replace(0, np.nan) - 1.0
    if factor_name == "research_accruals":
        net_profit = pd.to_numeric(frame.get("net_profit"), errors="coerce")
        total_assets = pd.to_numeric(frame.get("total_assets"), errors="coerce")
        total_liability = pd.to_numeric(frame.get("total_liability"), errors="coerce")
        delta_assets = total_assets.groupby(frame["symbol"]).diff(4)
        delta_liability = total_liability.groupby(frame["symbol"]).diff(4)
        return (delta_assets - delta_liability - net_profit) / total_assets.replace(0, np.nan)
    raise ValueError(f"Unsupported research factor: {factor_name}")


def precompute_research_factors(
    *,
    factor_names: Sequence[str],
    start_date: date,
    end_date: date,
    symbols: Sequence[str],
    store: FactorValueStore | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    def report(progress: float, stage: str, **meta: Any) -> None:
        if progress_callback is not None:
            progress_callback(max(0.0, min(1.0, progress)), stage, meta)

    report(0.02, "parse")
    daily = _load_daily(symbols, start_date, end_date)
    if daily.empty:
        raise RuntimeError("No daily data available for research factor precompute")
    financial = _load_financial(symbols)
    frame = _align_financial_asof(daily, financial)
    market_return = _cross_sectional_market_returns(frame)
    rows: list[dict[str, Any]] = []
    created_at = datetime.now()
    empty_hash = factor_params_hash({})
    total = max(len(factor_names), 1)
    for index, factor_name in enumerate(factor_names, start=1):
        report(0.10 + 0.80 * (index / total), "research", current=index, total=total, factor_name=factor_name)
        values = pd.to_numeric(_compute_research_factor(frame, factor_name, market_return), errors="coerce")
        factor_frame = pd.DataFrame({
            "symbol": frame["symbol"].astype(str),
            "trade_date": frame["trade_date"],
            "value": values,
        })
        factor_frame = factor_frame[(factor_frame["trade_date"] >= start_date) & (factor_frame["trade_date"] <= end_date)]
        factor_frame = factor_frame.dropna(subset=["value"])
        if factor_frame.empty:
            continue
        for item in factor_frame.itertuples(index=False):
            rows.append({
                "symbol": str(item.symbol),
                "trade_date": item.trade_date,
                "as_of_time": "",
                "factor_name": factor_name,
                "params_hash": empty_hash,
                "value": float(item.value),
                "source": "precompute.research",
                "created_at": created_at,
            })
    report(0.94, "write", rows_buffered=len(rows))
    factor_store = store or get_factor_value_store()
    written = factor_store.write(pd.DataFrame(rows)) if rows else 0
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["factor_name"]] = counts.get(row["factor_name"], 0) + 1
    report(1.0, "done", rows_written=written)
    return {
        "symbols": len(symbols),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rows": counts,
        "rows_written": written,
    }

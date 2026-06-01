"""Precompute domestic research-report factors.

The implemented factors intentionally use only data the platform already
stores. Reports that require free cash flow, analyst forecasts, Tick trades, or
order-book identifiers stay in the paper manifest backlog instead of receiving
synthetic proxy values.
"""

from __future__ import annotations

import gc
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
from loguru import logger

from app.core.config import settings
from app.data_stores import get_market_data_store
from app.services.factor_catalog import CN_PAPER_FACTOR_SPECS
from app.services.factor_precompute_runtime import (
    precompute_memory_policy,
    release_precompute_memory,
    symbol_date_chunks,
)
from app.services.factor_value_store import (
    FactorValueStore,
    factor_params_hash,
    get_factor_value_store,
)


def _sqlite_db_path() -> Path:
    return Path(settings.data_dir) / "gaoshou.db"


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _to_datetime64_ns(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").astype("datetime64[ns]")


def _load_daily(symbols: Sequence[str], start_date: date, end_date: date, lookback_days: int = 370) -> pd.DataFrame:
    columns = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount", "turnover_rate"]
    store = get_market_data_store()
    try:
        daily = store.load_daily(
            list(symbols),
            start_date - timedelta(days=lookback_days),
            end_date,
            columns=columns,
        )
    except Exception:
        daily = store.load_daily(
            list(symbols),
            start_date - timedelta(days=lookback_days),
            end_date,
            columns=[item for item in columns if item != "turnover_rate"],
        )
    if daily.empty:
        return pd.DataFrame()
    frame = daily.reset_index() if "trade_date" not in daily.columns else daily.reset_index(drop=True)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    for column in ("open", "high", "low", "close", "volume", "amount", "turnover_rate"):
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["symbol", "trade_date", "close"]).sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def _load_daily_basic(symbols: Sequence[str], start_date: date, end_date: date, lookback_days: int = 370) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    with sqlite3.connect(_sqlite_db_path()) as conn:
        if not _table_exists(conn, "stock_daily_basic"):
            return pd.DataFrame()
        columns = _existing_columns(conn, "stock_daily_basic")
        wanted = ["symbol", "trade_date", "total_mv", "circ_mv", "turnover_rate", "pe_ttm", "pb"]
        selected = [column for column in wanted if column in columns]
        if "symbol" not in selected or "trade_date" not in selected:
            return pd.DataFrame()
        placeholders = ",".join("?" for _ in symbols)
        sql = f"""
            SELECT {", ".join(selected)}
            FROM stock_daily_basic
            WHERE symbol IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
        """
        frame = pd.read_sql_query(
            sql,
            conn,
            params=[*symbols, (start_date - timedelta(days=lookback_days)).isoformat(), end_date.isoformat()],
        )
    if frame.empty:
        return frame
    frame["symbol"] = frame["symbol"].astype(str)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    for column in ("total_mv", "circ_mv", "turnover_rate", "pe_ttm", "pb"):
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def _load_financial(symbols: Sequence[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    wanted = [
        "symbol",
        "report_date",
        "roe",
        "revenue",
        "net_profit",
        "revenue_yoy",
        "profit_yoy",
        "gross_margin",
        "total_assets",
        "total_liability",
        "total_equity",
    ]
    with sqlite3.connect(_sqlite_db_path()) as conn:
        if not _table_exists(conn, "financial_data"):
            return pd.DataFrame()
        columns = _existing_columns(conn, "financial_data")
        selected = [column for column in wanted if column in columns]
        if "symbol" not in selected or "report_date" not in selected:
            return pd.DataFrame()
        placeholders = ",".join("?" for _ in symbols)
        frame = pd.read_sql_query(
            f"""
            SELECT {", ".join(selected)}
            FROM financial_data
            WHERE symbol IN ({placeholders})
            ORDER BY symbol, report_date
            """,
            conn,
            params=list(symbols),
        )
    for column in wanted:
        if column not in frame.columns:
            frame[column] = np.nan
    if frame.empty:
        return frame
    frame["symbol"] = frame["symbol"].astype(str)
    frame["report_date"] = _to_datetime64_ns(frame["report_date"])
    for column in wanted:
        if column not in {"symbol", "report_date"}:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["symbol", "report_date"]).sort_values(["symbol", "report_date"]).reset_index(drop=True)


def _load_stock_metadata(symbols: Sequence[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    db_path = _sqlite_db_path()
    if not db_path.exists():
        return pd.DataFrame()
    with sqlite3.connect(db_path) as conn:
        if not _table_exists(conn, "stocks"):
            return pd.DataFrame()
        columns = _existing_columns(conn, "stocks")
        wanted = ["symbol", "industry", "industry2", "industry3", "sector"]
        selected = [column for column in wanted if column in columns]
        if "symbol" not in selected:
            return pd.DataFrame()
        placeholders = ",".join("?" for _ in symbols)
        frame = pd.read_sql_query(
            f"""
            SELECT {", ".join(selected)}
            FROM stocks
            WHERE symbol IN ({placeholders})
            """,
            conn,
            params=list(symbols),
        )
    if frame.empty:
        return frame
    frame["symbol"] = frame["symbol"].astype(str)
    for column in ("industry", "industry2", "industry3", "sector"):
        if column not in frame.columns:
            frame[column] = ""
        frame[column] = frame[column].fillna("").astype(str)
    return frame.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)


def _merge_daily_basic_asof(daily: pd.DataFrame, daily_basic: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    if daily_basic.empty:
        for column in ("total_mv", "circ_mv", "turnover_rate_basic", "pe_ttm", "pb"):
            frame[column] = np.nan
        return frame
    basic = daily_basic.rename(columns={"turnover_rate": "turnover_rate_basic"}).copy()
    merged = frame.merge(basic, on=["symbol", "trade_date"], how="left")
    for column in ("total_mv", "circ_mv", "turnover_rate_basic", "pe_ttm", "pb"):
        if column not in merged.columns:
            merged[column] = np.nan
        merged[column] = merged.groupby("symbol", sort=False)[column].ffill()
    merged["turnover_rate"] = pd.to_numeric(merged["turnover_rate"], errors="coerce").fillna(merged["turnover_rate_basic"])
    return merged


def _align_financial_asof(daily: pd.DataFrame, financial: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return daily.copy()
    if financial.empty:
        frame = daily.copy()
        for column in (
            "roe",
            "revenue",
            "net_profit",
            "revenue_yoy",
            "profit_yoy",
            "gross_margin",
            "total_assets",
            "total_liability",
            "total_equity",
        ):
            frame[column] = np.nan
        return frame

    daily_frame = daily.copy()
    daily_frame["trade_date_ts"] = _to_datetime64_ns(daily_frame["trade_date"])
    financial_frame = financial.copy()
    financial_frame["report_date"] = _to_datetime64_ns(financial_frame["report_date"])
    financial_frame = financial_frame.dropna(subset=["symbol", "report_date"])
    parts: list[pd.DataFrame] = []
    for symbol, trade_group in daily_frame.groupby("symbol", sort=False):
        fin_group = financial_frame[financial_frame["symbol"] == symbol].sort_values("report_date")
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
        merged = merged.drop(columns=["symbol_x", "symbol_y"], errors="ignore")
        parts.append(merged)
    aligned = pd.concat(parts, ignore_index=True) if parts else daily_frame
    return aligned.drop(columns=["trade_date_ts"], errors="ignore")


def _merge_stock_metadata(daily: pd.DataFrame, metadata: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    if metadata.empty:
        for column in ("industry", "industry2", "industry3", "sector"):
            frame[column] = ""
        return frame
    merged = frame.merge(metadata, on="symbol", how="left")
    for column in ("industry", "industry2", "industry3", "sector"):
        if column not in merged.columns:
            merged[column] = ""
        merged[column] = merged[column].fillna("").astype(str)
    return merged


def _cs_rank(series: pd.Series, dates: pd.Series, *, ascending: bool = True) -> pd.Series:
    return series.groupby(dates).rank(pct=True, ascending=ascending)


def _cs_zscore(series: pd.Series, dates: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce").astype("float64")
    grouped = values.groupby(dates)
    mean = grouped.transform("mean")
    std = grouped.transform(lambda item: item.std(ddof=0))
    return (values - mean) / std.replace(0, np.nan)


def _cs_residual(y: pd.Series, x: pd.Series, dates: pd.Series) -> pd.Series:
    result = pd.Series(np.nan, index=y.index, dtype="float64")
    for _, idx in y.groupby(dates).groups.items():
        y_part = pd.to_numeric(y.loc[idx], errors="coerce")
        x_part = pd.to_numeric(x.loc[idx], errors="coerce")
        valid = y_part.notna() & x_part.notna() & np.isfinite(y_part) & np.isfinite(x_part)
        if int(valid.sum()) < 3 or x_part.loc[valid].nunique(dropna=True) < 2:
            if int(y_part.notna().sum()) >= 2:
                result.loc[idx] = y_part - y_part.mean()
            continue
        design = np.column_stack([np.ones(int(valid.sum())), x_part.loc[valid].to_numpy(dtype=float)])
        target = y_part.loc[valid].to_numpy(dtype=float)
        beta, *_ = np.linalg.lstsq(design, target, rcond=None)
        result.loc[y_part.loc[valid].index] = target - design @ beta
    return result


def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(window, min_periods=max(3, window // 2)).mean()
    loss = (-delta.clip(upper=0)).rolling(window, min_periods=max(3, window // 2)).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, np.nan)


def _trailing_return(close: pd.Series, symbols: pd.Series, window: int) -> pd.Series:
    return close.groupby(symbols).pct_change(window)


def _realized_volatility(close: pd.Series, symbols: pd.Series, window: int) -> pd.Series:
    daily_return = close.groupby(symbols).pct_change()
    return daily_return.groupby(symbols).transform(lambda item: item.rolling(window, min_periods=max(10, window // 3)).std())


def _style_leg_spread(
    leg_a: pd.Series,
    leg_b: pd.Series,
    returns: pd.Series,
    dates: pd.Series,
    *,
    quantile: float = 0.80,
) -> pd.Series:
    spread = pd.Series(np.nan, index=returns.index, dtype="float64")
    for _, idx in returns.groupby(dates).groups.items():
        a = pd.to_numeric(leg_a.loc[idx], errors="coerce")
        b = pd.to_numeric(leg_b.loc[idx], errors="coerce")
        r = pd.to_numeric(returns.loc[idx], errors="coerce")
        valid_a = a.notna() & r.notna()
        valid_b = b.notna() & r.notna()
        if int(valid_a.sum()) < 3 or int(valid_b.sum()) < 3:
            continue
        top_a = r.loc[valid_a & (a >= a.loc[valid_a].quantile(quantile))]
        top_b = r.loc[valid_b & (b >= b.loc[valid_b].quantile(quantile))]
        if len(top_a) and len(top_b):
            spread.loc[idx] = float(top_a.mean() - top_b.mean())
    return spread


def _compute_daily_factor(frame: pd.DataFrame, factor_name: str) -> pd.Series:
    dates = frame["trade_date"]
    symbols = frame["symbol"]
    close = pd.to_numeric(frame["close"], errors="coerce")
    open_price = pd.to_numeric(frame["open"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce")

    if factor_name == "paper_pb_roe_residual":
        bp = _safe_ratio(pd.Series(1.0, index=frame.index), pd.to_numeric(frame["pb"], errors="coerce"))
        roe = pd.to_numeric(frame["roe"], errors="coerce")
        return _cs_residual(bp, roe, dates)

    if factor_name == "paper_composite_value":
        bp = _safe_ratio(pd.Series(1.0, index=frame.index), pd.to_numeric(frame["pb"], errors="coerce"))
        ep = _safe_ratio(pd.Series(1.0, index=frame.index), pd.to_numeric(frame["pe_ttm"], errors="coerce"))
        bp_rank = _cs_rank(bp.where(bp > 0), dates)
        ep_rank = _cs_rank(ep.where(ep > 0), dates)
        return pd.concat([bp_rank, ep_rank], axis=1).mean(axis=1, skipna=True)

    if factor_name == "paper_growth_quality_score":
        components = [
            _cs_zscore(frame[column], dates)
            for column in ("revenue_yoy", "profit_yoy", "roe", "gross_margin")
            if column in frame.columns
        ]
        return pd.concat(components, axis=1).mean(axis=1, skipna=True) if components else pd.Series(np.nan, index=frame.index)

    if factor_name == "paper_financial_health_score":
        assets = pd.to_numeric(frame["total_assets"], errors="coerce")
        liability = pd.to_numeric(frame["total_liability"], errors="coerce")
        net_profit = pd.to_numeric(frame["net_profit"], errors="coerce")
        gross_margin = pd.to_numeric(frame["gross_margin"], errors="coerce")
        profitability = _safe_ratio(net_profit, assets)
        debt_ratio = _safe_ratio(liability, assets)
        asset_growth = assets.groupby(symbols).pct_change(252)
        parts = [
            _cs_zscore(profitability, dates),
            _cs_zscore(gross_margin, dates),
            -_cs_zscore(debt_ratio, dates),
            -_cs_zscore(asset_growth, dates),
        ]
        return pd.concat(parts, axis=1).mean(axis=1, skipna=True)

    if factor_name == "paper_overnight_turnover_corr":
        prev_close = close.groupby(symbols).shift(1)
        overnight = (open_price / prev_close.replace(0, np.nan) - 1.0).abs()
        turnover = pd.to_numeric(frame["turnover_rate"], errors="coerce")
        values = pd.Series(np.nan, index=frame.index, dtype="float64")
        for _, idx in frame.groupby("symbol", sort=False).groups.items():
            values.loc[idx] = overnight.loc[idx].rolling(20, min_periods=10).corr(turnover.loc[idx])
        return values

    if factor_name == "paper_rsi_reversal_score":
        values = pd.Series(np.nan, index=frame.index, dtype="float64")
        for _, idx in frame.groupby("symbol", sort=False).groups.items():
            rsi = _rsi(close.loc[idx], 14)
            vol_ratio = volume.loc[idx] / volume.loc[idx].rolling(20, min_periods=5).mean().replace(0, np.nan)
            values.loc[idx] = -rsi * vol_ratio.clip(lower=0.25, upper=4.0)
        return values

    if factor_name == "paper_new_high_anchor":
        rolling_high = close.groupby(symbols).transform(lambda item: item.rolling(240, min_periods=120).max())
        return (close >= rolling_high * 0.999).astype("float64").where(rolling_high.notna())

    if factor_name == "paper_high_low_volume_event":
        rolling_high = close.groupby(symbols).transform(lambda item: item.rolling(120, min_periods=60).max())
        rolling_low = close.groupby(symbols).transform(lambda item: item.rolling(120, min_periods=60).min())
        rolling_vol = volume.groupby(symbols).transform(lambda item: item.rolling(120, min_periods=60).max())
        position = (close - rolling_low) / (rolling_high - rolling_low).replace(0, np.nan)
        volume_ratio = volume / rolling_vol.replace(0, np.nan)
        return ((1.0 - 2.0 * position.clip(0.0, 1.0)) * volume_ratio).where(volume_ratio >= 0.9, 0.0)

    if factor_name == "paper_reversal_20d":
        return -close.groupby(symbols).pct_change(20)

    if factor_name == "paper_size_rotation_score":
        market_cap = pd.to_numeric(frame["circ_mv"], errors="coerce").fillna(pd.to_numeric(frame["total_mv"], errors="coerce"))
        size_rank = _cs_rank(market_cap, dates)
        returns_20d = _trailing_return(close, symbols, 20)
        style_spread = pd.Series(np.nan, index=frame.index, dtype="float64")
        for _, idx in frame.groupby("trade_date", sort=False).groups.items():
            rank_part = pd.to_numeric(size_rank.loc[idx], errors="coerce")
            ret_part = pd.to_numeric(returns_20d.loc[idx], errors="coerce")
            valid = rank_part.notna() & ret_part.notna()
            if int(valid.sum()) < 6:
                continue
            small_return = ret_part.loc[valid & (rank_part <= 0.33)].mean()
            large_return = ret_part.loc[valid & (rank_part >= 0.67)].mean()
            if np.isfinite(small_return) and np.isfinite(large_return):
                style_spread.loc[idx] = float(small_return - large_return)
        small_score = 1.0 - size_rank
        large_score = size_rank
        return pd.Series(
            np.where(style_spread >= 0, small_score, large_score),
            index=frame.index,
            dtype="float64",
        ).where(style_spread.notna() & size_rank.notna())

    if factor_name == "paper_value_growth_rotation_score":
        value_score = pd.to_numeric(_compute_daily_factor(frame, "paper_composite_value"), errors="coerce")
        growth_score = pd.to_numeric(_compute_daily_factor(frame, "paper_growth_quality_score"), errors="coerce")
        returns_20d = _trailing_return(close, symbols, 20)
        growth_minus_value = _style_leg_spread(growth_score, value_score, returns_20d, dates)
        return pd.Series(
            np.where(growth_minus_value >= 0, growth_score, value_score),
            index=frame.index,
            dtype="float64",
        ).where(growth_minus_value.notna())

    if factor_name == "paper_industry_momentum_20d":
        industry = frame.get("industry")
        if industry is None:
            return pd.Series(np.nan, index=frame.index)
        industry = industry.replace("", np.nan)
        returns_20d = _trailing_return(close, symbols, 20)
        industry_return = returns_20d.groupby([dates, industry], dropna=True).transform("mean")
        return _cs_rank(industry_return, dates)

    if factor_name == "paper_defensive_quality_lowvol":
        health = pd.to_numeric(_compute_daily_factor(frame, "paper_financial_health_score"), errors="coerce")
        volatility = _realized_volatility(close, symbols, 60)
        parts = [
            _cs_zscore(health, dates),
            -_cs_zscore(volatility, dates),
        ]
        return pd.concat(parts, axis=1).mean(axis=1, skipna=True)

    if factor_name == "paper_asset_allocation_proxy":
        returns_60d = _trailing_return(close, symbols, 60)
        volatility = _realized_volatility(close, symbols, 60)
        rolling_high = close.groupby(symbols).transform(lambda item: item.rolling(60, min_periods=20).max())
        drawdown = close / rolling_high.replace(0, np.nan) - 1.0
        parts = [
            _cs_zscore(returns_60d, dates),
            -_cs_zscore(volatility, dates),
            _cs_zscore(drawdown, dates),
        ]
        return pd.concat(parts, axis=1).mean(axis=1, skipna=True)

    raise ValueError(f"Unsupported CN paper daily factor: {factor_name}")


def _load_minute(symbols: Sequence[str], start_date: date, end_date: date, lookback_days: int = 10) -> pd.DataFrame:
    store = get_market_data_store()
    start_dt = datetime.combine(start_date - timedelta(days=lookback_days), datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.min.time()) + timedelta(days=1)
    minute = store.load_minute(
        list(symbols),
        start_dt,
        end_dt,
        columns=["symbol", "datetime", "open", "close", "volume"],
    )
    if minute.empty:
        return pd.DataFrame()
    frame = minute.reset_index() if "datetime" not in minute.columns else minute.reset_index(drop=True)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
    frame["trade_date"] = frame["datetime"].dt.date
    for column in ("open", "close", "volume"):
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.dropna(subset=["symbol", "datetime", "trade_date"]).sort_values(["symbol", "datetime"]).reset_index(drop=True)


def _minute_vwap(frame: pd.DataFrame) -> float:
    volume = pd.to_numeric(frame["volume"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    denom = float(volume.sum())
    if denom <= 0:
        return float("nan")
    return float((close * volume).sum() / denom)


def _compute_minute_factor_frame(
    minute: pd.DataFrame,
    factor_names: Sequence[str],
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    wanted = set(factor_names)
    for symbol, symbol_frame in minute.groupby("symbol", sort=False):
        by_day = {day: body.sort_values("datetime") for day, body in symbol_frame.groupby("trade_date", sort=True)}
        days = sorted(by_day)
        for index, trade_day in enumerate(days):
            if trade_day < start_date or trade_day > end_date:
                continue
            previous_days = days[max(0, index - 5):index]
            if not previous_days:
                continue
            previous = pd.concat([by_day[day] for day in previous_days], ignore_index=True)
            prev_volume = pd.to_numeric(previous["volume"], errors="coerce").dropna()
            if len(prev_volume) < 20:
                continue
            threshold = float(prev_volume.quantile(0.90))
            current = by_day[trade_day].copy()
            trend = current[pd.to_numeric(current["volume"], errors="coerce") > threshold]
            if trend.empty:
                continue
            day_vwap = _minute_vwap(current)
            trend_vwap = _minute_vwap(trend)
            if "paper_trend_fund_vwap_ratio" in wanted and np.isfinite(day_vwap) and day_vwap > 0 and np.isfinite(trend_vwap):
                rows.append({
                    "symbol": str(symbol),
                    "trade_date": trade_day,
                    "factor_name": "paper_trend_fund_vwap_ratio",
                    "value": (trend_vwap - day_vwap) / day_vwap,
                })
            if "paper_trend_fund_support" in wanted:
                direction = np.sign(pd.to_numeric(trend["close"], errors="coerce") - pd.to_numeric(trend["open"], errors="coerce"))
                volume = pd.to_numeric(trend["volume"], errors="coerce")
                denom = float(volume.sum())
                if denom > 0:
                    rows.append({
                        "symbol": str(symbol),
                        "trade_date": trade_day,
                        "factor_name": "paper_trend_fund_support",
                        "value": float((direction * volume).sum() / denom),
                    })
    return pd.DataFrame(rows)


def _factor_categories(factor_names: Sequence[str]) -> dict[str, list[str]]:
    daily: list[str] = []
    minute: list[str] = []
    for name in factor_names:
        category = str(CN_PAPER_FACTOR_SPECS.get(str(name), {}).get("category") or "")
        if category == "paper_minute":
            minute.append(str(name))
        else:
            daily.append(str(name))
    return {"daily": daily, "minute": minute}


def precompute_cn_paper_factors(
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

    names = [str(name) for name in factor_names if str(name)]
    unknown = [name for name in names if name not in CN_PAPER_FACTOR_SPECS]
    if unknown:
        raise ValueError(f"Unsupported CN paper factors: {unknown}")

    categories = _factor_categories(names)
    created_at = datetime.now()
    empty_hash = factor_params_hash({})
    errors: dict[str, str] = {}
    factor_store = store or get_factor_value_store()
    writer = factor_store.batch_writer()

    def add_feature_row(row: dict[str, Any]) -> None:
        writer.add(row)

    if categories["daily"]:
        report(0.05, "cn_paper_load_daily", factor_count=len(categories["daily"]))
        max_lookback = max(int(CN_PAPER_FACTOR_SPECS[name].get("lookback") or 0) for name in categories["daily"])
        daily = _load_daily(symbols, start_date, end_date, lookback_days=max(max_lookback, 60))
        if daily.empty:
            raise RuntimeError("No daily data available for CN paper factor precompute")
        daily_basic = _load_daily_basic(symbols, start_date, end_date, lookback_days=max(max_lookback, 370))
        frame = _merge_daily_basic_asof(daily, daily_basic)
        financial = _load_financial(symbols)
        frame = _align_financial_asof(frame, financial).sort_values(["symbol", "trade_date"]).reset_index(drop=True)
        frame = _merge_stock_metadata(frame, _load_stock_metadata(symbols)).sort_values(["symbol", "trade_date"]).reset_index(drop=True)
        total = max(len(categories["daily"]), 1)
        for index, factor_name in enumerate(categories["daily"], start=1):
            try:
                report(0.10 + 0.45 * (index / total), "cn_paper_daily", current=index, total=total, factor_name=factor_name)
                values = pd.to_numeric(_compute_daily_factor(frame, factor_name), errors="coerce")
                factor_frame = pd.DataFrame({
                    "symbol": frame["symbol"].astype(str),
                    "trade_date": frame["trade_date"],
                    "factor_name": factor_name,
                    "value": values,
                })
                factor_frame = factor_frame[(factor_frame["trade_date"] >= start_date) & (factor_frame["trade_date"] <= end_date)]
                factor_frame = factor_frame.replace([np.inf, -np.inf], np.nan).dropna(subset=["value"])
                factor_frame["as_of_time"] = ""
                factor_frame["params_hash"] = empty_hash
                factor_frame["source"] = "precompute.cn_paper"
                factor_frame["created_at"] = created_at
                writer.write_frame(factor_frame[[
                    "symbol",
                    "trade_date",
                    "as_of_time",
                    "factor_name",
                    "params_hash",
                    "value",
                    "source",
                    "created_at",
                ]])
                del factor_frame, values
                gc.collect()
            except Exception as exc:
                errors[factor_name] = str(exc)

    if categories["minute"]:
        report(0.58, "cn_paper_load_minute", factor_count=len(categories["minute"]))
        chunks = symbol_date_chunks(symbols, start_date, end_date, policy=precompute_memory_policy())
        loaded_any_minute = False
        written_any_minute = False
        for chunk in chunks:
            report(
                0.58 + 0.30 * (chunk.index / chunk.total),
                "cn_paper_load_minute",
                current=chunk.index,
                total=chunk.total,
                current_date=chunk.start_date.isoformat(),
                end_date=chunk.end_date.isoformat(),
                symbols=len(chunk.symbols),
                rows_written=writer.written,
            )
            # Minute factors do not require cross-sectional state, so keep
            # each read bounded by symbol and date chunks. Loading the full
            # pool/range can materialize hundreds of millions of 1m rows.
            minute = _load_minute(chunk.symbols, chunk.start_date, chunk.end_date, lookback_days=10)
            if minute.empty:
                continue
            loaded_any_minute = True
            minute_frame = _compute_minute_factor_frame(minute, categories["minute"], chunk.start_date, chunk.end_date)
            if minute_frame.empty:
                del minute, minute_frame
                release_precompute_memory()
                continue
            minute_frame["as_of_time"] = ""
            minute_frame["params_hash"] = empty_hash
            minute_frame["source"] = "precompute.cn_paper"
            minute_frame["created_at"] = created_at
            writer.write_frame(minute_frame[[
                "symbol",
                "trade_date",
                "as_of_time",
                "factor_name",
                "params_hash",
                "value",
                "source",
                "created_at",
            ]])
            writer.flush()
            written_any_minute = True
            del minute, minute_frame
            release_precompute_memory()
        if not loaded_any_minute:
            errors.update({name: "No minute data available for CN paper factor precompute" for name in categories["minute"]})
        elif not written_any_minute:
            logger.warning("CN paper minute data loaded but produced no factor rows for {}", categories["minute"])

    report(0.92, "cn_paper_write", rows_buffered=writer.rows_buffered)
    writer.flush()
    failed = [name for name in names if name in errors]
    report(1.0, "cn_paper_done", rows_written=writer.written)
    return {
        "symbols": len(symbols),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rows": {name: writer.counts.get(name, 0) for name in names},
        "rows_written": writer.written,
        "failed_factor_names": failed,
        "errors": errors,
    }

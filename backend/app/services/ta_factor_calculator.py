from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Sequence

import numpy as np
import pandas as pd

from app.compute.operators.ta_ops import _call_ta
from app.data_stores import get_market_data_store
from app.services.factor_catalog import TA_FACTOR_SPECS
from app.services.factor_value_store import FactorValueStore, factor_params_hash, get_factor_value_store


def _build_daily_frame(symbols: Sequence[str], start_date: date, end_date: date, lookback_days: int) -> pd.DataFrame:
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
    frame = frame.dropna(subset=["symbol", "trade_date", "open", "high", "low", "close"])
    return frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def _natr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    atr = np.asarray(_call_ta("ATR", high, low, close, timeperiod=period), dtype=np.float64)
    return atr / np.where(np.abs(close) > 1e-8, close, np.nan) * 100.0


def _ad(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    denom = np.where(np.abs(high - low) > 1e-8, high - low, np.nan)
    clv = ((close - low) - (high - close)) / denom
    return np.nan_to_num(clv, nan=0.0) * volume


def _mfi(high: np.ndarray, low: np.ndarray, close: np.ndarray, volume: np.ndarray, period: int) -> np.ndarray:
    tp = (high + low + close) / 3.0
    money_flow = tp * volume
    delta = np.diff(tp, prepend=np.nan)
    pos = np.where(delta > 0, money_flow, 0.0)
    neg = np.where(delta < 0, money_flow, 0.0)
    pos_sum = pd.Series(pos).rolling(period).sum()
    neg_sum = pd.Series(neg).rolling(period).sum()
    ratio = pos_sum / neg_sum.replace(0, np.nan)
    return (100 - (100 / (1 + ratio))).to_numpy(dtype=np.float64)


def _adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    up_move = np.diff(high, prepend=np.nan)
    down_move = -np.diff(low, prepend=np.nan)
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = pd.concat([
        pd.Series(high - low),
        pd.Series(np.abs(high - np.roll(close, 1))),
        pd.Series(np.abs(low - np.roll(close, 1))),
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    plus_di = 100 * pd.Series(plus_dm).rolling(period).mean() / atr.replace(0, np.nan)
    minus_di = 100 * pd.Series(minus_dm).rolling(period).mean() / atr.replace(0, np.nan)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    return dx.rolling(period).mean().to_numpy(dtype=np.float64)


def _aroonosc(high: np.ndarray, low: np.ndarray, period: int) -> np.ndarray:
    high_series = pd.Series(high, dtype=np.float64)
    low_series = pd.Series(low, dtype=np.float64)
    up = high_series.rolling(period).apply(lambda values: (period - 1 - np.argmax(values)) / period * 100.0, raw=True)
    down = low_series.rolling(period).apply(lambda values: (period - 1 - np.argmin(values)) / period * 100.0, raw=True)
    return (down - up).to_numpy(dtype=np.float64)


def _compute_factor(frame: pd.DataFrame, factor_name: str) -> np.ndarray:
    close = frame["close"].to_numpy(dtype=np.float64)
    high = frame["high"].to_numpy(dtype=np.float64)
    low = frame["low"].to_numpy(dtype=np.float64)
    open_ = frame["open"].to_numpy(dtype=np.float64)
    volume = frame["volume"].fillna(0.0).to_numpy(dtype=np.float64)
    amount = frame["amount"].fillna(0.0).to_numpy(dtype=np.float64)

    if factor_name == "ta_sma_20":
        return np.asarray(_call_ta("SMA", close, timeperiod=20), dtype=np.float64)
    if factor_name == "ta_ema_20":
        return np.asarray(_call_ta("EMA", close, timeperiod=20), dtype=np.float64)
    if factor_name == "ta_rsi_14":
        return np.asarray(_call_ta("RSI", close, timeperiod=14), dtype=np.float64)
    if factor_name == "ta_macd_dif_12_26_9":
        return np.asarray(_call_ta("MACD", close, fastperiod=12, slowperiod=26, signalperiod=9)[0], dtype=np.float64)
    if factor_name == "ta_macd_dea_12_26_9":
        return np.asarray(_call_ta("MACD", close, fastperiod=12, slowperiod=26, signalperiod=9)[1], dtype=np.float64)
    if factor_name == "ta_macd_hist_12_26_9":
        return np.asarray(_call_ta("MACD", close, fastperiod=12, slowperiod=26, signalperiod=9)[2], dtype=np.float64)
    if factor_name == "ta_bbands_upper_20":
        return np.asarray(_call_ta("BBANDS", close, timeperiod=20, nbdevup=2, nbdevdn=2)[0], dtype=np.float64)
    if factor_name == "ta_bbands_middle_20":
        return np.asarray(_call_ta("BBANDS", close, timeperiod=20, nbdevup=2, nbdevdn=2)[1], dtype=np.float64)
    if factor_name == "ta_bbands_lower_20":
        return np.asarray(_call_ta("BBANDS", close, timeperiod=20, nbdevup=2, nbdevdn=2)[2], dtype=np.float64)
    if factor_name == "ta_atr_14":
        return np.asarray(_call_ta("ATR", high, low, close, timeperiod=14), dtype=np.float64)
    if factor_name == "ta_natr_14":
        return _natr(high, low, close, 14)
    if factor_name == "ta_obv":
        return np.asarray(_call_ta("OBV", close, volume), dtype=np.float64)
    if factor_name == "ta_ad":
        return np.cumsum(_ad(high, low, close, volume))
    if factor_name == "ta_mfi_14":
        return _mfi(high, low, close, volume, 14)
    if factor_name == "ta_cci_14":
        return np.asarray(_call_ta("CCI", high, low, close, timeperiod=14), dtype=np.float64)
    if factor_name == "ta_willr_14":
        return np.asarray(_call_ta("WILLR", high, low, close, timeperiod=14), dtype=np.float64)
    if factor_name == "ta_roc_10":
        return pd.Series(close).pct_change(10).mul(100.0).to_numpy(dtype=np.float64)
    if factor_name == "ta_adx_14":
        return _adx(high, low, close, 14)
    if factor_name == "ta_aroonosc_14":
        return _aroonosc(high, low, 14)
    if factor_name == "ta_typprice":
        return ((high + low + close) / 3.0).astype(np.float64)
    raise ValueError(f"Unsupported TA factor: {factor_name}")


def precompute_ta_factors(
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

    lookback = max(int(TA_FACTOR_SPECS.get(name, {}).get("lookback") or 0) for name in factor_names) if factor_names else 60
    report(0.02, "parse")
    daily = _build_daily_frame(symbols, start_date, end_date, max(lookback, 60))
    if daily.empty:
        raise RuntimeError("No daily data available for TA factor precompute")
    rows: list[dict[str, Any]] = []
    created_at = datetime.now()
    empty_hash = factor_params_hash({})
    grouped = list(daily.groupby("symbol", sort=False))
    total = max(len(grouped) * max(len(factor_names), 1), 1)
    current = 0
    for symbol, frame in grouped:
        frame = frame.sort_values("trade_date").reset_index(drop=True)
        for factor_name in factor_names:
            current += 1
            report(0.08 + 0.82 * (current / total), "ta_lib", current=current, total=total, factor_name=factor_name, symbol=symbol)
            values = _compute_factor(frame, factor_name)
            factor_frame = pd.DataFrame({
                "trade_date": frame["trade_date"],
                "value": pd.to_numeric(values, errors="coerce"),
            })
            factor_frame = factor_frame[(factor_frame["trade_date"] >= start_date) & (factor_frame["trade_date"] <= end_date)]
            factor_frame = factor_frame.dropna(subset=["value"])
            if factor_frame.empty:
                continue
            for item in factor_frame.itertuples(index=False):
                rows.append({
                    "symbol": str(symbol),
                    "trade_date": item.trade_date,
                    "as_of_time": "",
                    "factor_name": factor_name,
                    "params_hash": empty_hash,
                    "value": float(item.value),
                    "source": "precompute.ta_lib",
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

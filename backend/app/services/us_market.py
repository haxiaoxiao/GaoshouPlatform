"""US market proxy data used by TSMF entry filters."""

from __future__ import annotations

import json
import math
import re
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from app.core.config import settings

NASDAQ_URL = "https://api.nasdaq.com/api/quote/{symbol}/historical"
DEFAULT_US_MARKET_SPECS = ("QQQ:etf", "SMH:etf", "SOXX:etf", "NVDA:stocks")


def default_us_market_dir() -> Path:
    return Path(settings.data_dir) / "external" / "us_market"


def default_us_market_file() -> Path:
    return default_us_market_dir() / "us_market_daily.csv"


def parse_symbol_spec(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if ":" in text:
        symbol, assetclass = text.split(":", 1)
        return symbol.strip().upper(), assetclass.strip().lower()
    return text.upper(), "etf"


def fetch_us_symbol_history(symbol: str, assetclass: str, start: date, end: date) -> pd.DataFrame:
    headers = {
        "accept": "application/json, text/plain, */*",
        "origin": "https://www.nasdaq.com",
        "referer": "https://www.nasdaq.com/",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
    }
    params = {
        "assetclass": assetclass,
        "fromdate": start.isoformat(),
        "todate": end.isoformat(),
        "limit": "9999",
    }
    response = requests.get(NASDAQ_URL.format(symbol=symbol), params=params, headers=headers, timeout=60)
    response.raise_for_status()
    payload = response.json()
    rows = (((payload.get("data") or {}).get("tradesTable") or {}).get("rows") or [])
    if not rows:
        raise RuntimeError(f"No Nasdaq rows returned for {symbol}")

    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"], format="%m/%d/%Y", errors="coerce").dt.date
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = frame[column].map(_clean_number)
    frame["symbol"] = symbol.upper()
    frame["assetclass"] = assetclass
    frame = frame[["symbol", "assetclass", "date", "open", "high", "low", "close", "volume"]]
    frame = frame.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    frame["ret_1d"] = frame["close"].pct_change()
    frame["source"] = "nasdaq_api_historical"
    return frame


def sync_us_market_history(
    *,
    start_date: date,
    end_date: date,
    symbol_specs: list[str] | tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
    sleep_seconds: float = 0.5,
) -> dict[str, Any]:
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")

    out_dir = Path(output_dir) if output_dir else default_us_market_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    specs = tuple(symbol_specs or DEFAULT_US_MARKET_SPECS)
    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    metadata: dict[str, Any] = {
        "source": NASDAQ_URL,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "output_dir": str(out_dir),
        "symbols": [],
    }

    for spec in specs:
        symbol, assetclass = parse_symbol_spec(spec)
        try:
            frame = fetch_us_symbol_history(symbol, assetclass, start_date, end_date)
        except Exception as exc:
            fallback = out_dir / f"{symbol}_daily.csv"
            if not fallback.exists():
                failures.append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
                continue
            frame = pd.read_csv(fallback)
            if "date" in frame.columns:
                frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
            frame["source"] = frame.get("source", "local_fallback")
            failures.append({"symbol": symbol, "error": f"network_fallback:{type(exc).__name__}: {exc}"})

        frame.to_csv(out_dir / f"{symbol}_daily.csv", index=False)
        _try_write_parquet(frame, out_dir / f"{symbol}_daily.parquet")
        frames.append(frame)
        metadata["symbols"].append(
            {
                "symbol": symbol,
                "assetclass": assetclass,
                "rows": int(len(frame)),
                "min_date": str(frame["date"].min()) if "date" in frame.columns and not frame.empty else None,
                "max_date": str(frame["date"].max()) if "date" in frame.columns and not frame.empty else None,
            }
        )
        time.sleep(max(0.0, float(sleep_seconds or 0.0)))

    if not frames:
        existing = out_dir / "us_market_daily.csv"
        if existing.exists():
            combined = pd.read_csv(existing)
        else:
            raise RuntimeError(f"No US market data synced: {failures}")
    else:
        combined = pd.concat(frames, ignore_index=True)

    combined.to_csv(out_dir / "us_market_daily.csv", index=False)
    _try_write_parquet(combined, out_dir / "us_market_daily.parquet")
    metadata["rows"] = int(len(combined))
    metadata["failed_symbols"] = failures
    (out_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata


def load_us_market_frame(path: str | Path | None = None) -> pd.DataFrame:
    data_path = Path(path) if path else default_us_market_file()
    if data_path.is_dir():
        data_path = data_path / "us_market_daily.csv"
    if not data_path.exists():
        return pd.DataFrame()

    if data_path.suffix.lower() == ".parquet":
        raw = pd.read_parquet(data_path)
    else:
        raw = pd.read_csv(data_path)
    if raw.empty:
        return pd.DataFrame()

    if {"date", "symbol", "ret_1d"}.issubset(raw.columns):
        frame = raw.copy()
        frame["us_date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["ret_1d"] = pd.to_numeric(frame["ret_1d"], errors="coerce")
        pivot = frame.pivot_table(index="us_date", columns="symbol", values="ret_1d", aggfunc="last")
        pivot.columns = [f"{str(column).lower()}_ret_1d" for column in pivot.columns]
        result = pivot.sort_index()
    else:
        date_col = "us_date" if "us_date" in raw.columns else "date"
        if date_col not in raw.columns:
            return pd.DataFrame()
        result = raw.copy()
        result[date_col] = pd.to_datetime(result[date_col], errors="coerce")
        result = result.set_index(date_col).sort_index()
    result = result.loc[~result.index.isna()]
    return result


def us_overnight_entry_filter_state(
    trade_date: date,
    *,
    mode: str = "none",
    data_path: str | Path | None = None,
    max_lag_days: int = 5,
    caution_exposure: float = 0.85,
    defensive_exposure: float = 0.70,
    qqq_caution_ret: float = -0.01,
    qqq_defensive_ret: float = -0.02,
    semi_caution_ret: float = -0.02,
    semi_defensive_ret: float = -0.03,
    nvda_caution_ret: float = -0.03,
    nvda_defensive_ret: float = -0.04,
) -> dict[str, Any]:
    overlay = str(mode or "none").strip().lower()
    if overlay in {"", "none", "off", "false", "0"}:
        return {"entry_filter_enabled": False}

    frame = load_us_market_frame(data_path)
    base_state: dict[str, Any] = {
        "entry_filter_enabled": True,
        "us_overnight_overlay": overlay,
    }
    if frame.empty:
        return {
            **base_state,
            "entry_filter_block_buys": False,
            "us_overnight_exposure": 1.0,
            "us_overnight_reason": "missing_us_data",
        }

    current = pd.Timestamp(trade_date)
    pos = frame.index.searchsorted(current, side="left") - 1
    if pos < 0:
        return {
            **base_state,
            "entry_filter_block_buys": False,
            "us_overnight_exposure": 1.0,
            "us_overnight_reason": "no_prior_us_day",
        }

    row = frame.iloc[int(pos)]
    us_date = frame.index[int(pos)]
    lag_days = int((current.date() - us_date.date()).days)
    if lag_days > max(1, int(max_lag_days or 5)):
        return {
            **base_state,
            "entry_filter_block_buys": False,
            "us_overnight_exposure": 1.0,
            "us_overnight_reason": "stale_us_day",
            "us_date": us_date.strftime("%Y-%m-%d"),
            "us_lag_calendar_days": lag_days,
        }

    exposure, reason = _us_overnight_exposure(
        row,
        overlay=overlay,
        caution_exposure=caution_exposure,
        defensive_exposure=defensive_exposure,
        qqq_caution_ret=qqq_caution_ret,
        qqq_defensive_ret=qqq_defensive_ret,
        semi_caution_ret=semi_caution_ret,
        semi_defensive_ret=semi_defensive_ret,
        nvda_caution_ret=nvda_caution_ret,
        nvda_defensive_ret=nvda_defensive_ret,
    )
    state = {
        **base_state,
        "entry_filter_block_buys": float(exposure or 1.0) < 0.999,
        "us_overnight_exposure": exposure,
        "us_overnight_reason": reason,
        "us_date": us_date.strftime("%Y-%m-%d"),
        "us_lag_calendar_days": lag_days,
    }
    for symbol in ("qqq", "smh", "soxx", "nvda"):
        state[f"{symbol}_ret_1d"] = _row_float(row, f"{symbol}_ret_1d")
    return state


def apply_entry_filter_to_target_weights(
    target_weights: dict[str, float],
    *,
    current_positions: dict[str, float],
    price_map: dict[str, float],
    portfolio_value: float,
    entry_filter_state: dict[str, Any] | None = None,
) -> tuple[dict[str, float], dict[str, Any]]:
    state = dict(entry_filter_state or {})
    if not state.get("entry_filter_block_buys") or portfolio_value <= 0:
        return dict(target_weights), state

    filtered: dict[str, float] = {}
    blocked_new = 0
    blocked_add = 0
    for symbol, raw_target in target_weights.items():
        target = max(0.0, float(raw_target or 0.0))
        quantity = float(current_positions.get(symbol, 0.0) or 0.0)
        price = float(price_map.get(symbol, 0.0) or 0.0)
        current_weight = quantity * price / portfolio_value if quantity > 0 and price > 0 else 0.0
        if current_weight <= 0 and target > 0:
            blocked_new += 1
            continue
        if target > current_weight:
            blocked_add += 1
            target = current_weight
        if target > 0:
            filtered[str(symbol)] = target

    state["entry_filter_blocked_new"] = blocked_new
    state["entry_filter_blocked_add"] = blocked_add
    return filtered, state


def _us_overnight_exposure(row: Any, **kwargs: Any) -> tuple[float, str]:
    overlay = str(kwargs["overlay"])
    qqq = _row_float(row, "qqq_ret_1d")
    smh = _row_float(row, "smh_ret_1d")
    soxx = _row_float(row, "soxx_ret_1d")
    nvda = _row_float(row, "nvda_ret_1d")
    caution = _clamp(kwargs["caution_exposure"])
    defensive = _clamp(kwargs["defensive_exposure"])
    semi_values = [value for value in (smh, soxx) if _finite(value)]

    if overlay == "qqq_downside":
        if _finite(qqq) and qqq <= float(kwargs["qqq_defensive_ret"]):
            return defensive, "qqq_defensive"
        if _finite(qqq) and qqq <= float(kwargs["qqq_caution_ret"]):
            return caution, "qqq_caution"
        return 1.0, "clear"
    if overlay == "semi_downside":
        if any(value <= float(kwargs["semi_defensive_ret"]) for value in semi_values):
            return defensive, "semi_defensive"
        if any(value <= float(kwargs["semi_caution_ret"]) for value in semi_values):
            return caution, "semi_caution"
        return 1.0, "clear"
    if overlay == "combined_downside":
        if (
            (_finite(qqq) and qqq <= float(kwargs["qqq_defensive_ret"]))
            or any(value <= float(kwargs["semi_defensive_ret"]) for value in semi_values)
            or (_finite(nvda) and nvda <= float(kwargs["nvda_defensive_ret"]))
        ):
            return defensive, "combined_defensive"
        if (
            (_finite(qqq) and qqq <= float(kwargs["qqq_caution_ret"]))
            or any(value <= float(kwargs["semi_caution_ret"]) for value in semi_values)
            or (_finite(nvda) and nvda <= float(kwargs["nvda_caution_ret"]))
        ):
            return caution, "combined_caution"
        return 1.0, "clear"
    return 1.0, "unknown_overlay"


def _row_float(row: Any, name: str) -> float:
    try:
        value = row.get(name)
    except Exception:
        value = None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result if _finite(result) else math.nan


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _clamp(value: Any) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return 1.0


def _clean_number(value: Any) -> float | None:
    text = str(value or "").replace(",", "").replace("$", "").strip()
    if not text or text in {"N/A", "--"}:
        return None
    text = re.sub(r"[^0-9.\-]", "", text)
    return float(text) if text else None


def _try_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    try:
        frame.to_parquet(path, index=False)
    except Exception:
        pass

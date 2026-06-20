"""MarketDataStore-backed akquant DataFeedAdapter."""
from __future__ import annotations

import os
import sqlite3
import threading
from collections import OrderedDict
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from app.backtest.engine.akquant import AKQUANT_AVAILABLE
from app.backtest.engine.interface import IDataProvider

if AKQUANT_AVAILABLE:
    from akquant.feed_adapter import BasePandasFeedAdapter, FeedSlice
else:
    BasePandasFeedAdapter = object
    FeedSlice = object


class MarketDataStoreFeedAdapter(BasePandasFeedAdapter):
    """IDataProvider-backed feed adapter.

    Daily data is small enough to preload. Minute data for a broad universe is
    loaded per symbol on demand and retained in a bounded LRU cache. The AKQuant
    engine may still materialize one full adapter request before running, so the
    caller is responsible for slicing large minute backtests into time chunks.
    """

    name = "market_data_store"

    def __init__(
        self,
        data_provider: IDataProvider,
        symbols: list[str],
        start_date: date,
        end_date: date,
        bar_type: str = "daily",
        max_cached_symbols: int | None = None,
        timer_times: tuple[str, ...] | None = None,
        smart_candidate_top_n: int = 0,
        smart_full_universe_times: tuple[str, ...] | None = None,
        smart_keep_index_symbols: tuple[str, ...] | None = None,
        timer_price_adjustment_mode: str = "none",
    ):
        self._data_provider = data_provider
        self._symbols = symbols
        self._start_date = start_date
        self._end_date = end_date
        self._bar_type = bar_type
        self._timer_times = timer_times or self._parse_timer_times()
        self._smart_candidate_top_n = max(0, int(smart_candidate_top_n or 0))
        self._smart_full_universe_times = self._normalize_time_set(
            smart_full_universe_times or ()
        )
        self._smart_keep_index_symbols = set(smart_keep_index_symbols or ())
        self._timer_price_adjustment_mode = str(timer_price_adjustment_mode or "none").lower()
        self._smart_clock_symbol = str(symbols[0]) if symbols else ""
        self._smart_candidate_cache: dict[date, set[str]] = {}
        self._minute_adjustment_ratios: dict[date, pd.DataFrame | None] = {}
        default_cache_limit = int(os.getenv("AKQUANT_FEED_CACHE_SYMBOLS", "64" if bar_type == "minute" else "512"))
        self._max_cached_symbols = max_cached_symbols or default_cache_limit
        self._cache: OrderedDict[str, pd.DataFrame] = OrderedDict()
        self._has_any_data = False
        self._loaded = False
        self._bulk_minute_timer_loaded = False
        self._progress_lock = threading.Lock()
        self._total_rows = 0
        self._served_rows = 0
        self._served_requests = 0
        self._served_symbols: set[str] = set()

    async def preload(self) -> None:
        if self._loaded:
            return

        if self._bar_type in {"minute", "minute_timer"}:
            if self._bar_type == "minute_timer" and self._use_bulk_minute_timer():
                await self._preload_minute_timer_bulk()
                self._loaded = True
                logger.info(
                    "MarketDataFeedAdapter: bulk minute_timer mode, symbols={}, loaded={}, rows={}",
                    len(self._symbols),
                    len(self._cache),
                    sum(len(df) for df in self._cache.values()),
                )
                return
            has_data = getattr(self._data_provider, "has_data", None)
            if has_data is not None:
                self._has_any_data = await has_data(
                    self._symbols,
                    self._start_date,
                    self._end_date,
                    self._bar_type,
                )
            self._loaded = True
            logger.info(
                "MarketDataFeedAdapter: lazy minute mode, symbols={}, cache_limit={}",
                len(self._symbols),
                self._max_cached_symbols,
            )
            return

        df_all = await self._data_provider.load_daily(
            self._symbols,
            self._start_date,
            self._end_date,
        )

        if df_all.empty:
            logger.warning("MarketDataFeedAdapter: no data loaded")
            self._loaded = True
            return

        for sym, grp in df_all.groupby("symbol"):
            sym_str = str(sym)
            grp = grp.drop(columns=["symbol"], errors="ignore")
            self._cache[sym_str] = grp.copy()

        self._has_any_data = bool(self._cache)
        self._total_rows = int(len(df_all))
        self._loaded = True
        logger.info(
            "MarketDataFeedAdapter: loaded {} symbols, {} total rows",
            len(self._cache),
            len(df_all),
        )

    def load(self, request: FeedSlice) -> pd.DataFrame:
        if not self._loaded:
            return pd.DataFrame()

        sym = str(request.symbol)
        df = self._cache.get(sym)
        if df is None:
            if self._bar_type == "minute_timer" and self._bulk_minute_timer_loaded:
                return pd.DataFrame()
            if self._bar_type not in {"minute", "minute_timer"}:
                return pd.DataFrame()
            df = self._load_symbol(sym)
            if df.empty:
                return pd.DataFrame()
        else:
            self._cache.move_to_end(sym)

        normalized = self.normalize(df.copy(), sym)
        clipped = self._clip_time_range(normalized, request.start_time, request.end_time)
        self._record_served(sym, len(clipped))
        return clipped

    @property
    def progress_snapshot(self) -> dict[str, int | float | str]:
        with self._progress_lock:
            total_rows = int(self._total_rows or 0)
            served_rows = int(self._served_rows or 0)
            served_requests = int(self._served_requests or 0)
            served_symbols = len(self._served_symbols)
        row_ratio = min(1.0, served_rows / total_rows) if total_rows > 0 else 0.0
        symbol_ratio = min(1.0, served_symbols / len(self._symbols)) if self._symbols else 0.0
        return {
            "bar_type": self._bar_type,
            "total_bars": total_rows,
            "served_bars": served_rows,
            "served_requests": served_requests,
            "served_symbols": served_symbols,
            "total_symbols": len(self._symbols),
            "bar_progress_ratio": round(row_ratio, 4),
            "symbol_progress_ratio": round(symbol_ratio, 4),
        }

    def clear_cache(self) -> None:
        self._cache.clear()

    def _use_bulk_minute_timer(self) -> bool:
        value = os.getenv("AKQUANT_BULK_MINUTE_TIMER", "1").lower()
        return value not in {"0", "false", "no"}

    async def _preload_minute_timer_bulk(self) -> None:
        batch_size = max(1, int(os.getenv("AKQUANT_BULK_MINUTE_TIMER_BATCH", "300")))
        batches = [
            self._symbols[i : i + batch_size]
            for i in range(0, len(self._symbols), batch_size)
        ]
        for batch in batches:
            df_all = await self._data_provider.load_minute(
                batch,
                self._start_date,
                self._end_date,
                timer_times=self._timer_times,
            )
            df_all = self._adjust_minute_timer_price_space(df_all)
            df_all = self._shrink_frame(df_all)
            df_all = self._filter_smart_minute_timer(df_all)
            if df_all.empty:
                continue
            for sym, grp in df_all.groupby("symbol", sort=False):
                sym_str = str(sym)
                frame = grp.drop(columns=["symbol"], errors="ignore").copy()
                self._cache[sym_str] = frame
        self._has_any_data = bool(self._cache)
        self._bulk_minute_timer_loaded = True

    def _load_symbol(self, symbol: str) -> pd.DataFrame:
        # AKQuant BasePandasFeedAdapter.load() is synchronous. The lazy path
        # therefore must use the store synchronously if StoreDataProvider exposes
        # it. Bulk minute_timer is the default path for large timer backtests.
        store = getattr(self._data_provider, "_store", None)
        if store is not None:
            if self._bar_type in {"minute", "minute_timer"}:
                start = datetime.combine(self._start_date, datetime.min.time())
                end = datetime.combine(self._end_date, datetime.min.time()) + pd.Timedelta(days=1)
                timer_times = self._timer_times if self._bar_type == "minute_timer" else None
                df_all = store.load_minute([symbol], start, end, timer_times=timer_times)
            else:
                df_all = store.load_daily([symbol], self._start_date, self._end_date)
            df_all = self._adjust_minute_timer_price_space(df_all)
            df_all = self._shrink_frame(df_all)
            df_all = self._filter_smart_minute_timer(df_all)
        else:
            logger.warning(
                "MarketDataFeedAdapter lazy load requires StoreDataProvider; "
                "enable bulk preload or pass StoreDataProvider"
            )
            return pd.DataFrame()
        if df_all.empty:
            return pd.DataFrame()

        df = df_all.drop(columns=["symbol"], errors="ignore").copy()
        self._cache[symbol] = df
        self._add_total_rows(len(df))
        self._cache.move_to_end(symbol)
        while len(self._cache) > self._max_cached_symbols:
            self._cache.popitem(last=False)
        return df

    def _add_total_rows(self, rows: int) -> None:
        if rows <= 0:
            return
        with self._progress_lock:
            self._total_rows += int(rows)

    def _record_served(self, symbol: str, rows: int) -> None:
        with self._progress_lock:
            self._served_requests += 1
            self._served_symbols.add(str(symbol))
            self._served_rows += max(0, int(rows or 0))

    def _filter_smart_minute_timer(self, df: pd.DataFrame) -> pd.DataFrame:
        if (
            self._bar_type != "minute_timer"
            or df.empty
            or "symbol" not in df.columns
        ):
            return df
        if (
            self._smart_candidate_top_n <= 0
            and not self._smart_full_universe_times
            and not self._smart_keep_index_symbols
            and not self._smart_clock_symbol
        ):
            return df
        frame = df.copy()
        if not isinstance(frame.index, pd.DatetimeIndex):
            if "datetime" not in frame.columns:
                return df
            frame["datetime"] = pd.to_datetime(frame["datetime"])
            frame = frame.set_index("datetime")
        time_keys = frame.index.strftime("%H:%M")
        full_mask = np.asarray(time_keys.isin(self._smart_full_universe_times), dtype=bool)
        if full_mask.all():
            return frame
        keep = full_mask.copy()
        if (~full_mask).any():
            symbols = frame["symbol"].astype(str)
            if self._smart_clock_symbol:
                keep |= np.asarray(symbols == self._smart_clock_symbol, dtype=bool)
            if self._smart_keep_index_symbols:
                keep |= np.asarray(symbols.isin(self._smart_keep_index_symbols), dtype=bool)
            remaining = ~keep
            if remaining.any() and self._smart_candidate_top_n > 0:
                remaining_days = pd.Index(frame.index[remaining].date).unique()
                for trade_day in remaining_days:
                    day_mask = remaining & (frame.index.date == trade_day)
                    candidates = self._candidate_symbols_for_date(trade_day)
                    if candidates:
                        keep |= day_mask & np.asarray(symbols.isin(candidates), dtype=bool)
        return frame.iloc[keep]

    def _candidate_symbols_for_date(self, trade_day: date) -> set[str]:
        cached = self._smart_candidate_cache.get(trade_day)
        if cached is not None:
            return cached
        if self._smart_candidate_top_n <= 0:
            self._smart_candidate_cache[trade_day] = set()
            return set()
        symbols = self._load_market_cap_candidates(trade_day)
        if not symbols:
            symbols = set(self._symbols)
        self._smart_candidate_cache[trade_day] = symbols
        return symbols

    def _load_market_cap_candidates(self, trade_day: date) -> set[str]:
        db_path = self._project_root() / "data" / "gaoshou.db"
        if not db_path.exists():
            db_path = self._project_root() / "backend" / "data" / "gaoshou.db"
        if not db_path.exists() or not self._symbols:
            return set()
        as_of = trade_day - timedelta(days=1)
        placeholders = ",".join("?" for _ in self._symbols)
        try:
            with sqlite3.connect(db_path) as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_daily_basic'"
                ).fetchone()
                if not exists:
                    return set()
                rows = conn.execute(
                    f"""
                    SELECT symbol, total_mv
                    FROM stock_daily_basic
                    WHERE symbol IN ({placeholders})
                      AND trade_date <= ?
                      AND total_mv > 0
                    ORDER BY trade_date DESC, total_mv ASC
                    """,
                    (*self._symbols, as_of),
                ).fetchall()
        except Exception:
            return set()
        latest: dict[str, float] = {}
        for symbol, total_mv in rows:
            symbol = str(symbol)
            if symbol not in latest:
                latest[symbol] = float(total_mv or 0)
        return {
            symbol
            for symbol, _ in sorted(latest.items(), key=lambda item: item[1])[
                : self._smart_candidate_top_n
            ]
        }

    def _adjust_minute_timer_price_space(self, df: pd.DataFrame) -> pd.DataFrame:
        if (
            self._bar_type != "minute_timer"
            or self._timer_price_adjustment_mode
            not in {"previous_close_ratio", "prev_close_ratio", "daily"}
            or df.empty
            or "symbol" not in df.columns
        ):
            return df
        price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
        if not price_cols:
            return df
        frame = df.copy()
        if isinstance(frame.index, pd.DatetimeIndex):
            dt = pd.Series(frame.index, index=frame.index)
        elif "datetime" in frame.columns:
            dt = pd.to_datetime(frame["datetime"])
        else:
            return frame
        frame["_adjust_trade_date"] = pd.to_datetime(dt).dt.date
        for trade_day, idx in frame.groupby("_adjust_trade_date", sort=False).groups.items():
            ratio_frame = self._minute_adjustment_ratio_frame(trade_day)
            if ratio_frame is None or ratio_frame.empty:
                continue
            loc = list(idx)
            merged = frame.loc[loc, ["symbol"]].merge(
                ratio_frame,
                on="symbol",
                how="left",
                sort=False,
            )
            ratios = pd.to_numeric(merged["ratio"], errors="coerce").fillna(1.0).to_numpy()
            if np.all(np.isclose(ratios, 1.0)):
                continue
            for col in price_cols:
                values = pd.to_numeric(frame.loc[loc, col], errors="coerce").to_numpy()
                frame.loc[loc, col] = np.where(values > 0, values * ratios, values)
        return frame.drop(columns=["_adjust_trade_date"], errors="ignore")

    def _minute_adjustment_ratio_frame(self, trade_day: date) -> pd.DataFrame | None:
        cached = self._minute_adjustment_ratios.get(trade_day)
        if trade_day in self._minute_adjustment_ratios:
            return cached
        store = getattr(self._data_provider, "_store", None)
        previous = self._previous_trade_date(trade_day)
        if store is None or previous is None:
            self._minute_adjustment_ratios[trade_day] = None
            return None
        try:
            daily = store.load_daily(
                self._symbols,
                previous,
                previous,
                columns=["symbol", "trade_date", "close"],
            )
            minute = store.load_minute(
                self._symbols,
                datetime.combine(previous, datetime.min.time()),
                datetime.combine(previous, datetime.min.time()) + timedelta(days=1),
                columns=["symbol", "datetime", "close"],
                timer_times=["15:00"],
            )
        except Exception:
            self._minute_adjustment_ratios[trade_day] = None
            return None
        if daily.empty or minute.empty:
            self._minute_adjustment_ratios[trade_day] = None
            return None
        daily_close = daily.reset_index(drop=True)[["symbol", "close"]].rename(columns={"close": "daily_close"})
        minute_close = minute.copy()
        if "datetime" not in minute_close.columns:
            minute_close = minute_close.reset_index()
            if "datetime" not in minute_close.columns and "index" in minute_close.columns:
                minute_close = minute_close.rename(columns={"index": "datetime"})
        else:
            minute_close = minute_close.reset_index(drop=True)
        if "datetime" not in minute_close.columns:
            self._minute_adjustment_ratios[trade_day] = None
            return None
        minute_close["datetime"] = pd.to_datetime(minute_close["datetime"])
        minute_close = (
            minute_close.sort_values("datetime")
            .groupby("symbol", as_index=False)
            .tail(1)[["symbol", "close"]]
            .rename(columns={"close": "minute_close"})
        )
        ratios = daily_close.merge(minute_close, on="symbol", how="inner")
        ratios["daily_close"] = pd.to_numeric(ratios["daily_close"], errors="coerce")
        ratios["minute_close"] = pd.to_numeric(ratios["minute_close"], errors="coerce")
        ratios["ratio"] = np.where(
            (ratios["daily_close"] > 0) & (ratios["minute_close"] > 0),
            ratios["daily_close"] / ratios["minute_close"],
            1.0,
        )
        ratios = ratios[["symbol", "ratio"]]
        self._minute_adjustment_ratios[trade_day] = ratios
        return ratios

    def _previous_trade_date(self, trade_day: date) -> date | None:
        store = getattr(self._data_provider, "_store", None)
        if store is None:
            return None
        try:
            df = store.load_daily(
                self._symbols[:1],
                trade_day - timedelta(days=14),
                trade_day - timedelta(days=1),
                columns=["symbol", "trade_date", "close"],
            )
        except Exception:
            return None
        if df.empty:
            return None
        values = pd.to_datetime(df.reset_index()["trade_date"]).dt.date
        values = values[values < trade_day]
        if values.empty:
            return None
        return values.max()

    @staticmethod
    def _project_root() -> Path:
        return Path(__file__).resolve().parents[4]

    @staticmethod
    def _normalize_time_set(values: tuple[str, ...]) -> set[str]:
        result = set()
        for value in values:
            text = str(value).strip()
            if not text:
                continue
            parts = text.split(":")
            if len(parts) >= 2:
                try:
                    result.add(f"{int(parts[0]):02d}:{int(parts[1]):02d}")
                except ValueError:
                    continue
        return result

    @staticmethod
    def _shrink_frame(df: pd.DataFrame) -> pd.DataFrame:
        for col in ["open", "high", "low", "close", "amount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
        return df

    @property
    def has_any_data(self) -> bool:
        return self._has_any_data or bool(self._cache)

    @staticmethod
    def _parse_timer_times() -> tuple[str, ...]:
        raw = os.getenv(
            "AKQUANT_TIMER_MINUTE_TIMES",
            "10:00:00,10:30:00,14:30:00,14:50:00",
        )
        values = []
        for item in raw.split(","):
            text = item.strip()
            if not text:
                continue
            if len(text) == 5:
                text = f"{text}:00"
            values.append(text)
        return tuple(values)

    @property
    def _timer_minutes(self) -> tuple[int, ...]:
        values = []
        for text in self._timer_times:
            try:
                hour, minute, *_ = text.split(":")
                values.append(int(hour) * 60 + int(minute))
            except Exception:
                continue
        return tuple(values)

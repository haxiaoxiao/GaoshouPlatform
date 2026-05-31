"""ClickHouse 数据提供者 — 实现 IDataProvider"""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Any

import pandas as pd

from app.backtest.engine.interface import IDataProvider
from app.data_stores import get_market_data_store
from app.data_stores.base import MarketDataStore
from app.db.clickhouse import get_ch_client


class StoreDataProvider(IDataProvider):
    """通过 MarketDataStore 抽象层加载回测数据

    支持 Parquet 和 ClickHouse 后端，由 market_data_backend 配置决定。
    所有 store 方法均为同步调用，通过 asyncio.to_thread 桥接。
    """

    def __init__(self, store: MarketDataStore | None = None):
        self._store = store or get_market_data_store()

    async def load_daily(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        return await asyncio.to_thread(
            self._store.load_daily, symbols, start_date, end_date
        )

    async def load_minute(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        timer_times: tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        dt_start = datetime.combine(start_date, datetime.min.time())
        dt_end = datetime.combine(end_date, datetime.min.time()) + pd.Timedelta(days=1)
        return await asyncio.to_thread(
            self._store.load_minute,
            symbols, dt_start, dt_end, None, timer_times,
        )

    async def has_data(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        bar_type: str = "daily",
    ) -> bool:
        return await asyncio.to_thread(
            self._store.has_data, symbols, start_date, end_date, bar_type
        )

    async def load_trading_dates(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> list[date]:
        return await asyncio.to_thread(
            self._store.load_trading_dates, symbols, start_date, end_date
        )

    async def load_benchmark(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.Series:
        return await asyncio.to_thread(
            self._store.load_benchmark, symbol, start_date, end_date
        )


class ClickHouseDataProvider(IDataProvider):
    """从 ClickHouse 加载回测数据（兼容旧路径，直接使用 CH 客户端）"""

    @staticmethod
    def _close_client(ch: Any) -> None:
        try:
            ch.disconnect()
        except Exception:
            pass

    def _execute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        ch = get_ch_client()
        try:
            return ch.execute(query, params or {})
        finally:
            self._close_client(ch)

    def _detect_daily_columns(self) -> list[str]:
        """探测 klines_daily 实际列名（兼容有无 turnover_rate 的情况）"""
        cols = self._execute("SELECT name FROM system.columns WHERE table = 'klines_daily'")
        return [r[0] for r in cols]

    async def load_daily(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        def _query():
            available = self._detect_daily_columns()
            wanted = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]
            extras = [c for c in ["turnover_rate"] if c in available]
            all_cols = wanted + extras
            col_str = ", ".join(all_cols)
            rows = self._execute(
                f"""
                SELECT {col_str}
                FROM klines_daily
                WHERE symbol IN %(syms)s
                  AND trade_date >= %(start)s
                  AND trade_date <= %(end)s
                ORDER BY symbol, trade_date
                """,
                {"syms": symbols, "start": start_date, "end": end_date},
            )
            if not rows:
                return pd.DataFrame(columns=all_cols)
            df = pd.DataFrame(rows, columns=all_cols)
            numeric_cols = [c for c in ["open", "high", "low", "close", "amount"] + extras]
            for col in numeric_cols:
                df[col] = df[col].astype(float)
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.drop_duplicates(subset=["symbol", "trade_date"], keep="first")
            df = df.set_index("trade_date").sort_index()
            return df

        return await asyncio.to_thread(_query)

    async def has_data(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        bar_type: str = "daily",
    ) -> bool:
        """Fast existence check used by lazy feed adapters."""
        if not symbols:
            return False

        def _query():
            if bar_type in {"minute", "minute_timer"}:
                rows = self._execute(
                    """
                    SELECT 1
                    FROM klines_minute
                    WHERE symbol IN %(syms)s
                      AND datetime >= %(start)s
                      AND datetime < %(end_plus)s
                    LIMIT 1
                    """,
                    {
                        "syms": symbols,
                        "start": datetime.combine(start_date, datetime.min.time()),
                        "end_plus": datetime.combine(end_date, datetime.min.time())
                        + pd.Timedelta(days=1),
                    },
                )
            else:
                rows = self._execute(
                    """
                    SELECT 1
                    FROM klines_daily
                    WHERE symbol IN %(syms)s
                      AND trade_date >= %(start)s
                      AND trade_date <= %(end)s
                    LIMIT 1
                    """,
                    {"syms": symbols, "start": start_date, "end": end_date},
                )
            return bool(rows)

        return await asyncio.to_thread(_query)

    async def load_minute(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        timer_times: tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        def _query():
            time_filter = ""
            params: dict[str, Any] = {
                "syms": symbols,
                "start": datetime.combine(start_date, datetime.min.time()),
                "end_plus": datetime.combine(end_date, datetime.min.time()) + pd.Timedelta(days=1),
            }
            if timer_times:
                timer_minutes = []
                for text in timer_times:
                    try:
                        hour, minute, *_ = str(text).split(":")
                        timer_minutes.append(int(hour) * 60 + int(minute))
                    except Exception:
                        continue
                if timer_minutes:
                    time_filter = "AND (toHour(datetime) * 60 + toMinute(datetime)) IN %(timer_minutes)s"
                    params["timer_minutes"] = tuple(timer_minutes)

            rows = self._execute(
                f"""
                SELECT symbol, datetime, open, high, low, close, volume, amount
                FROM klines_minute
                WHERE symbol IN %(syms)s
                  AND datetime >= %(start)s
                  AND datetime < %(end_plus)s
                  {time_filter}
                ORDER BY symbol, datetime
                """,
                params,
            )
            if not rows:
                return pd.DataFrame(
                    columns=["symbol", "datetime", "open", "high", "low",
                             "close", "volume", "amount"]
                )
            df = pd.DataFrame(
                rows,
                columns=["symbol", "datetime", "open", "high", "low",
                         "close", "volume", "amount"],
            )
            for col in ["open", "high", "low", "close", "amount"]:
                df[col] = df[col].astype(float)
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.drop_duplicates(subset=["symbol", "datetime"], keep="first")
            df = df.set_index("datetime").sort_index()
            return df

        return await asyncio.to_thread(_query)

    async def load_trading_dates(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> list[date]:
        def _query():
            rows = self._execute(
                """
                SELECT DISTINCT trade_date
                FROM klines_daily
                WHERE symbol IN %(syms)s
                  AND trade_date >= %(start)s
                  AND trade_date <= %(end)s
                ORDER BY trade_date
                """,
                {"syms": symbols, "start": start_date, "end": end_date},
            )
            return [row[0] for row in rows]

        return await asyncio.to_thread(_query)

    async def load_benchmark(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.Series:
        def _query():
            rows = self._execute(
                """
                SELECT trade_date, close
                FROM klines_daily
                WHERE symbol = %(sym)s
                  AND trade_date >= %(start)s
                  AND trade_date <= %(end)s
                ORDER BY trade_date
                """,
                {"sym": symbol, "start": start_date, "end": end_date},
            )
            if not rows:
                return pd.Series(dtype=float, name="benchmark")
            df = pd.DataFrame(rows, columns=["trade_date", "close"])
            df["close"] = df["close"].astype(float)
            df["trade_date"] = pd.to_datetime(df["trade_date"])
            df = df.set_index("trade_date")
            returns = df["close"].pct_change().fillna(0.0)
            returns.name = symbol
            return returns

        return await asyncio.to_thread(_query)

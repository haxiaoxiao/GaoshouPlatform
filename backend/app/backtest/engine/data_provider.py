"""ClickHouse 数据提供者 — 实现 IDataProvider"""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import Any

import pandas as pd
from loguru import logger

from app.backtest.engine.interface import IDataProvider
from app.db.clickhouse import get_ch_client


class ClickHouseDataProvider(IDataProvider):
    """从 ClickHouse 加载回测数据"""

    def _detect_daily_columns(self) -> list[str]:
        """探测 klines_daily 实际列名（兼容有无 turnover_rate 的情况）"""
        ch = get_ch_client()
        cols = ch.execute("SELECT name FROM system.columns WHERE table = 'klines_daily'")
        return [r[0] for r in cols]

    async def load_daily(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        def _query():
            ch = get_ch_client()
            available = self._detect_daily_columns()
            wanted = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]
            extras = [c for c in ["turnover_rate"] if c in available]
            all_cols = wanted + extras
            col_str = ", ".join(all_cols)
            rows = ch.execute(
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

    async def load_minute(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> pd.DataFrame:
        def _query():
            ch = get_ch_client()
            rows = ch.execute(
                """
                SELECT symbol, datetime, open, high, low, close, volume, amount
                FROM klines_minute
                WHERE symbol IN %(syms)s
                  AND datetime >= %(start)s
                  AND datetime < %(end_plus)s
                ORDER BY symbol, datetime
                """,
                {
                    "syms": symbols,
                    "start": datetime.combine(start_date, datetime.min.time()),
                    "end_plus": datetime.combine(end_date, datetime.min.time()) + pd.Timedelta(days=1),
                },
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
            ch = get_ch_client()
            rows = ch.execute(
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
            ch = get_ch_client()
            rows = ch.execute(
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

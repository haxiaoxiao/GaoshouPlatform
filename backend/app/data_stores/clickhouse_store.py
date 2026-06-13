"""ClickHouse 行情数据存储 — 包装现有 ClickHouse 查询实现 MarketDataStore"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Sequence

import pandas as pd
from loguru import logger

from app.data_stores.base import MarketDataStore
from app.db.clickhouse import get_ch_client


class ClickHouseMarketDataStore(MarketDataStore):
    """将现有 ClickHouse 查询包装为 MarketDataStore 接口

    保持与 ClickHouseDataProvider 完全一致的查询语义，便于回退。
    """

    def __init__(self):
        self._ch = None

    def _client(self):
        """每次调用创建新连接，避免共享状态冲突"""
        return get_ch_client()

    def _execute(self, query: str, params: dict | None = None):
        ch = self._client()
        try:
            return ch.execute(query, params or {})
        finally:
            try:
                ch.disconnect()
            except Exception:
                pass

    def _detect_daily_columns(self) -> list[str]:
        cols = self._execute("SELECT name FROM system.columns WHERE table = 'klines_daily'")
        return [r[0] for r in cols]

    # ------------------------------------------------------------------
    # 读取
    # ------------------------------------------------------------------

    def load_daily(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        columns: Sequence[str] | None = None,
    ) -> pd.DataFrame:
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
            {"syms": tuple(symbols), "start": start_date, "end": end_date},
        )
        if not rows:
            return pd.DataFrame(columns=all_cols)
        df = pd.DataFrame(rows, columns=all_cols)
        for col in ["open", "high", "low", "close", "amount"] + extras:
            if col in df.columns:
                df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.drop_duplicates(subset=["symbol", "trade_date"], keep="first")
        return df.set_index("trade_date").sort_index()

    def load_minute(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
        columns: Sequence[str] | None = None,
        timer_times: Sequence[str] | None = None,
    ) -> pd.DataFrame:
        time_filter = ""
        params: dict = {"syms": tuple(symbols), "start": start, "end_plus": end}
        if timer_times:
            timer_minutes = []
            for text in timer_times:
                try:
                    h, m, *_ = str(text).split(":")
                    timer_minutes.append(int(h) * 60 + int(m))
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
        wanted = ["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
        if not rows:
            return pd.DataFrame(columns=wanted)
        df = pd.DataFrame(rows, columns=wanted)
        for col in ["open", "high", "low", "close", "amount"]:
            df[col] = df[col].astype(float)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.drop_duplicates(subset=["symbol", "datetime"], keep="first")
        return df.set_index("datetime").sort_index()

    def load_minute_volume_sum(
        self,
        symbols: Sequence[str],
        start: datetime,
        end: datetime,
    ) -> dict[str, float]:
        if not symbols:
            return {}
        rows = self._execute(
            """
            SELECT symbol, sum(volume) AS volume
            FROM klines_minute
            WHERE symbol IN %(syms)s
              AND datetime >= %(start)s
              AND datetime < %(end_plus)s
            GROUP BY symbol
            """,
            {"syms": tuple(symbols), "start": start, "end_plus": end},
        )
        return {str(symbol): float(volume or 0.0) for symbol, volume in rows}

    def has_data(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        bar_type: str = "daily",
    ) -> bool:
        if not symbols:
            return False
        if bar_type in {"minute", "minute_timer"}:
            rows = self._execute(
                """
                SELECT 1 FROM klines_minute
                WHERE symbol IN %(syms)s
                  AND datetime >= %(start)s AND datetime < %(end_plus)s
                LIMIT 1
                """,
                {
                    "syms": tuple(symbols),
                    "start": datetime.combine(start_date, datetime.min.time()),
                    "end_plus": datetime.combine(end_date, datetime.min.time()) + pd.Timedelta(days=1),
                },
            )
        else:
            rows = self._execute(
                """SELECT 1 FROM klines_daily WHERE symbol IN %(syms)s
                   AND trade_date >= %(start)s AND trade_date <= %(end)s LIMIT 1""",
                {"syms": tuple(symbols), "start": start_date, "end": end_date},
            )
        return bool(rows)

    def load_trading_dates(
        self, symbols: Sequence[str], start_date: date, end_date: date
    ) -> list[date]:
        rows = self._execute(
            """SELECT DISTINCT trade_date FROM klines_daily
               WHERE symbol IN %(syms)s AND trade_date >= %(start)s AND trade_date <= %(end)s
               ORDER BY trade_date""",
            {"syms": tuple(symbols), "start": start_date, "end": end_date},
        )
        return [r[0] for r in rows]

    def load_benchmark(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.Series:
        rows = self._execute(
            """SELECT trade_date, close FROM klines_daily
               WHERE symbol = %(sym)s AND trade_date >= %(start)s AND trade_date <= %(end)s
               ORDER BY trade_date""",
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

    def top_by_avg_amount(
        self, start_date: date, end_date: date, limit: int
    ) -> list[str]:
        rows = self._execute(
            """
            SELECT symbol, avg(amount) as avg_amount
            FROM klines_daily
            WHERE trade_date >= %(start)s AND trade_date <= %(end)s
              AND amount > 0 AND close > 0
            GROUP BY symbol
            ORDER BY avg_amount DESC
            LIMIT %(limit)s
            """,
            {"start": start_date, "end": end_date, "limit": limit},
        )
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # 写入 — 暂不实现（ClickHouse 写入继续走现有路径）
    # ------------------------------------------------------------------

    def write_daily(self, df: pd.DataFrame) -> int:
        logger.warning("ClickHouseMarketDataStore.write_daily 未实现，请使用现有同步服务写入")
        return 0

    def write_minute(self, df: pd.DataFrame, *, dataset: str = "klines_minute") -> int:
        logger.warning("ClickHouseMarketDataStore.write_minute 未实现，请使用现有同步服务写入")
        return 0

    # ------------------------------------------------------------------
    # 覆盖摘要
    # ------------------------------------------------------------------

    def coverage(
        self,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        *,
        dataset: str,
        timer_times: Sequence[str] | None = None,
    ) -> dict:
        table = "klines_minute" if "minute" in dataset else "klines_daily"
        col = "datetime" if "minute" in dataset else "trade_date"
        start_value: object = start_date
        end_value: object = end_date
        timer_clause = ""
        params = {"syms": tuple(symbols), "start": start_value, "end": end_value}
        if "minute" in dataset:
            start_value = datetime.combine(start_date, time.min)
            end_value = datetime.combine(end_date, time.max)
            params["start"] = start_value
            params["end"] = end_value
            timer_minutes = self._timer_minutes(timer_times)
            if timer_minutes:
                timer_clause = "AND (toHour(datetime) * 60 + toMinute(datetime)) IN %(timer_minutes)s"
                params["timer_minutes"] = tuple(timer_minutes)
        rows = self._execute(
            f"""SELECT symbol, MIN({col}), MAX({col}), COUNT(*)
                FROM {table}
                WHERE symbol IN %(syms)s AND {col} >= %(start)s AND {col} <= %(end)s
                {timer_clause}
                GROUP BY symbol""",
            params,
        )
        syms = [r[0] for r in rows]
        total = sum(r[3] for r in rows)
        return {"total_rows": total, "symbols_covered": syms, "date_range": f"{start_date} ~ {end_date}"}

    @staticmethod
    def _timer_minutes(timer_times: Sequence[str] | None) -> list[int]:
        minutes: list[int] = []
        for value in timer_times or []:
            parts = str(value).strip().split(":")
            if len(parts) < 2:
                continue
            try:
                minutes.append(int(parts[0]) * 60 + int(parts[1]))
            except ValueError:
                continue
        return sorted(set(minutes))

"""Bar 数据源 — 从 ClickHouse 加载日线数据"""
from dataclasses import dataclass
from datetime import date
from typing import Iterator

import pandas as pd
from loguru import logger

from app.backtest.event.calendar import TradingCalendar


@dataclass
class Bar:
    """单日 Bar 数据"""

    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    turnover_rate: float

    @classmethod
    def from_row(cls, symbol: str, trade_date: date, row: dict) -> "Bar":
        return cls(
            symbol=symbol,
            trade_date=trade_date,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=int(row["volume"]),
            amount=float(row["amount"]),
            turnover_rate=float(row.get("turnover_rate", 0)),
        )


class BarEventSource:
    """日线 Bar 数据源 — 一次性加载，按交易日产出 Bar 序列"""

    def __init__(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        calendar: TradingCalendar,
    ):
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.calendar = calendar
        # {symbol: DataFrame(index=trade_date)}
        self._data: dict[str, pd.DataFrame] = {}
        self._loaded = False

    @classmethod
    async def from_clickhouse(
        cls,
        symbols: list[str],
        start_date: date,
        end_date: date,
        calendar: TradingCalendar,
    ) -> "BarEventSource":
        """从 ClickHouse 加载日线数据"""
        from app.db.clickhouse import get_ch_client

        source = cls(symbols, start_date, end_date, calendar)
        ch = get_ch_client()

        rows = ch.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, volume, amount
            FROM klines_daily
            WHERE symbol IN %(syms)s
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY symbol, trade_date
            """,
            {"syms": symbols, "start": start_date, "end": end_date},
        )

        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low",
                     "close", "volume", "amount"],
        )
        if df.empty:
            raise ValueError(f"No kline data for symbols {symbols} in [{start_date}, {end_date}]")

        for col in ["open", "high", "low", "close", "amount"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        # ClickHouse klines_daily may have multiple rows per (symbol, date).
        # Drop duplicates keeping the first row.
        df = df.drop_duplicates(subset=["symbol", "trade_date"], keep="first")

        for sym, grp in df.groupby("symbol"):
            source._data[sym] = grp.set_index("trade_date")

        source._loaded = True
        logger.info("BarEventSource: loaded {} symbols, {} rows", len(source._data), len(df))
        return source

    @classmethod
    def from_dataframe(
        cls,
        data: dict[str, pd.DataFrame],
        calendar: TradingCalendar,
    ) -> "BarEventSource":
        """从已有 DataFrame 创建（测试用）"""
        source = cls(symbols=list(data.keys()), start_date=date(2000, 1, 1),
                     end_date=date(2099, 12, 31), calendar=calendar)
        source._data = data
        source._loaded = True
        return source

    def get_bars(self, trade_date: date) -> dict[str, Bar]:
        """获取指定交易日所有标的的 Bar 数据"""
        result: dict[str, Bar] = {}
        ts = pd.Timestamp(trade_date)
        for sym in self.symbols:
            df = self._data.get(sym)
            if df is None or ts not in df.index:
                continue
            row = df.loc[ts]
            result[sym] = Bar(
                symbol=sym,
                trade_date=trade_date,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                amount=float(row["amount"]),
                turnover_rate=float(row.get("turnover_rate", 0)),
            )
        return result

    def get_history(self, symbol: str, end_date: date, n_days: int = 60) -> pd.DataFrame:
        """获取某标的截止某日的历史数据"""
        df = self._data.get(symbol)
        if df is None:
            return pd.DataFrame()
        ts = pd.Timestamp(end_date)
        mask = df.index <= ts
        return df.loc[mask].tail(n_days)

    def iter_trading_dates(self) -> Iterator[date]:
        """按交易日历迭代日期"""
        for d in self.calendar.get_date_range(self.start_date, self.end_date):
            yield d

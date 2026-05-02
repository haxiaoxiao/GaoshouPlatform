"""Bar 数据源 — 从 ClickHouse 加载日线数据"""
from datetime import date, datetime
from typing import Iterator, TYPE_CHECKING

import numpy as np
import pandas as pd
from loguru import logger

from app.backtest.event.calendar import TradingCalendar

if TYPE_CHECKING:
    from app.backtest.event.event_source import BarEventSource


class Bar:
    """日线 Bar — 兼容 RQAlpha BarObject 属性

    实例属性（直接访问）:
        symbol, order_book_id, datetime, trade_date
        open, high, low, close, last
        volume, total_turnover
        limit_up, limit_down, prev_close
        suspended, is_trading, isnan

    方法:
        mavg(intervals)  — 移动均线
        vwap(intervals)  — 成交量加权均价
    """

    __slots__ = ("_data", "_source", "_symbol", "_trade_date")

    def __init__(self, symbol: str, trade_date: date, data: dict, source: "BarEventSource | None" = None):
        self._symbol = symbol
        self._trade_date = trade_date
        self._data = data
        self._source = source

    # ── 标识 ──
    @property
    def symbol(self) -> str:
        """合约简称"""
        return self._symbol

    @property
    def order_book_id(self) -> str:
        """合约代码（与 symbol 相同）"""
        return self._symbol

    @property
    def datetime(self) -> datetime:
        """时间戳"""
        return datetime.combine(self._trade_date, datetime.min.time())

    @property
    def trade_date(self) -> date:
        """交易日期"""
        return self._trade_date

    # ── OHLCV ──
    @property
    def open(self) -> float:
        return float(self._data["open"])

    @property
    def high(self) -> float:
        return float(self._data["high"])

    @property
    def low(self) -> float:
        return float(self._data["low"])

    @property
    def close(self) -> float:
        return float(self._data["close"])

    @property
    def last(self) -> float:
        """当前最新价（日线等于收盘价）"""
        return self.close

    @property
    def volume(self) -> int:
        return int(self._data["volume"])

    @property
    def total_turnover(self) -> float:
        """成交额"""
        return float(self._data.get("amount", 0))

    # ── 涨跌停 ──
    @property
    def prev_close(self) -> float:
        """昨日收盘价"""
        return float(self._data.get("prev_close", np.nan))

    @property
    def limit_up(self) -> float:
        """涨停价（A股 ±10%）"""
        pc = self.prev_close
        if np.isnan(pc) or pc <= 0:
            return np.nan
        return round(pc * 1.1, 2)

    @property
    def limit_down(self) -> float:
        """跌停价（A股 ±10%）"""
        pc = self.prev_close
        if np.isnan(pc) or pc <= 0:
            return np.nan
        return round(pc * 0.9, 2)

    # ── 状态 ──
    @property
    def is_trading(self) -> bool:
        """是否有成交量"""
        return self.volume > 0

    @property
    def isnan(self) -> bool:
        """数据是否缺失"""
        return np.isnan(self.close)

    @property
    def suspended(self) -> bool:
        """是否停牌"""
        return self.isnan or self.volume == 0

    # ── 方法 ──
    def mavg(self, intervals: int, frequency: str = "1d") -> float:
        """移动均线 — 返回最近 intervals 根 Bar 收盘价均值"""
        if self._source is None:
            return np.nan
        df = self._source.get_history(self._symbol, self._trade_date, intervals)
        if df.empty:
            return np.nan
        closes = df["close"].astype(float)
        return float(closes.tail(intervals).mean())

    def vwap(self, intervals: int, frequency: str = "1d") -> float:
        """成交量加权均价"""
        if self._source is None:
            return np.nan
        df = self._source.get_history(self._symbol, self._trade_date, intervals)
        if df.empty:
            return np.nan
        closes = df["close"].astype(float)
        volumes = df["volume"].astype(float)
        total_vol = volumes.tail(intervals).sum()
        if total_vol == 0:
            return 0.0
        return float((closes.tail(intervals) * volumes.tail(intervals)).sum() / total_vol)

    def __repr__(self) -> str:
        return (
            f"Bar(symbol={self._symbol}, datetime={self._trade_date}, "
            f"open={self.open}, close={self.close}, volume={self.volume})"
        )

    def __getitem__(self, key: str):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    @classmethod
    def from_row(cls, symbol: str, trade_date: date, row: dict, source: "BarEventSource | None" = None) -> "Bar":
        return cls(symbol=symbol, trade_date=trade_date, data=dict(row), source=source)


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
        prev_ts = self._find_prev_trade_date(trade_date)
        for sym in self.symbols:
            df = self._data.get(sym)
            if df is None or ts not in df.index:
                continue
            row = df.loc[ts].to_dict()
            # Inject prev_close from previous trading day
            if prev_ts and sym in self._data:
                pdf = self._data[sym]
                if prev_ts in pdf.index:
                    row["prev_close"] = float(pdf.loc[prev_ts, "close"])
            result[sym] = Bar.from_row(sym, trade_date, row, source=self)
        return result

    def _find_prev_trade_date(self, trade_date: date) -> pd.Timestamp | None:
        """查找前一个可用的交易日（从数据中）"""
        all_dates: set[pd.Timestamp] = set()
        for df in self._data.values():
            all_dates.update(df.index.tolist())
        sorted_dates = sorted(d for d in all_dates if d < pd.Timestamp(trade_date))
        return sorted_dates[-1] if sorted_dates else None

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

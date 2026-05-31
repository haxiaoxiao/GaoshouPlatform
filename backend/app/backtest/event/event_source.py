"""Bar 数据源 — 从 ClickHouse 加载日线数据"""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from typing import TYPE_CHECKING, Iterator

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

    def __init__(self, symbol: str, trade_date: date | datetime, data: dict, source: BarEventSource | None = None):
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
        """时间戳 — 日线返回日期零点，分钟线返回实际时间"""
        if isinstance(self._trade_date, datetime):
            return self._trade_date
        return datetime.combine(self._trade_date, datetime.min.time())

    @property
    def trade_date(self) -> date:
        """交易日期"""
        if isinstance(self._trade_date, datetime):
            return self._trade_date.date()
        return self._trade_date

    @property
    def minute_time(self) -> datetime | None:
        """分钟时间戳 — 仅分钟 Bar 有效，日线 Bar 返回 None"""
        if isinstance(self._trade_date, datetime):
            return self._trade_date
        return None

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
    def from_row(cls, symbol: str, trade_date: date | datetime, row: dict, source: BarEventSource | None = None) -> Bar:
        return cls(symbol=symbol, trade_date=trade_date, data=dict(row), source=source)


class BarDict:
    """bar_dict — 兼容 RQAlpha BarMap，提供 dict-like 访问所有标的当前 Bar"""

    def __init__(self, bars: dict[str, Bar], trade_date: date):
        self._bars = bars
        self.dt = trade_date

    def __getitem__(self, key: str) -> Bar:
        if key not in self._bars:
            raise KeyError(f"{key} not in bar_dict (available: {list(self._bars.keys())})")
        return self._bars[key]

    def __contains__(self, key: str) -> bool:
        return key in self._bars

    def __len__(self) -> int:
        return len(self._bars)

    def __iter__(self):
        return iter(self._bars)

    def items(self):
        return self._bars.items()

    def keys(self):
        return self._bars.keys()

    def values(self):
        return self._bars.values()

    def __repr__(self) -> str:
        syms = list(self._bars.keys())[:5]
        more = f" +{len(self._bars) - 5}" if len(self._bars) > 5 else ""
        return f"BarDict(dt={self.dt}, symbols={syms}{more})"


class BarEventSource:
    """Bar 数据源 — 一次性加载日线/分钟数据，按交易日产出 Bar 序列"""

    def __init__(
        self,
        symbols: list[str],
        start_date: date,
        end_date: date,
        calendar: TradingCalendar,
        bar_type: str = "daily",
    ):
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.calendar = calendar
        self.bar_type = bar_type
        # {symbol: DataFrame(index=trade_date)}
        self._data: dict[str, pd.DataFrame] = {}
        # {symbol: DataFrame(index=datetime)} — minute data
        self._minute_data: dict[str, pd.DataFrame] = {}
        self._loaded = False
        self._prev_date_cache: dict = {}
        self._all_dates_sorted: list = []

    @classmethod
    async def from_clickhouse(
        cls,
        symbols: list[str],
        start_date: date,
        end_date: date,
        calendar: TradingCalendar,
        bar_type: str = "daily",
    ) -> BarEventSource:
        """从行情存储加载日线/分钟数据 (线程池中执行, 不阻塞事件循环)"""
        from app.data_stores import get_market_data_store

        source = cls(symbols, start_date, end_date, calendar, bar_type=bar_type)

        def _load():
            store = get_market_data_store()
            df = store.load_daily(symbols, start_date, end_date)
            if df.empty:
                raise ValueError("No kline data")

            for sym, grp in df.groupby("symbol"):
                source._data[sym] = grp

            logger.info("BarEventSource: loaded {} symbols, {} rows", len(source._data), len(df))

        await asyncio.to_thread(_load)
        source._loaded = True
        return source

    @classmethod
    def from_dataframe(
        cls,
        data: dict[str, pd.DataFrame],
        calendar: TradingCalendar,
        minute_data: dict[str, pd.DataFrame] | None = None,
    ) -> BarEventSource:
        """从已有 DataFrame 创建（测试用）"""
        source = cls(symbols=list(data.keys()), start_date=date(2000, 1, 1),
                     end_date=date(2099, 12, 31), calendar=calendar)
        source._data = data
        if minute_data:
            source._minute_data = minute_data
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
            if prev_ts is not None and prev_ts in df.index:
                row["prev_close"] = float(df.loc[prev_ts, "close"])
            result[sym] = Bar.from_row(sym, trade_date, row, source=self)
        return result

    def _find_prev_trade_date(self, trade_date: date) -> pd.Timestamp | None:
        """查找前一个可用的交易日（缓存优化）"""
        ts = pd.Timestamp(trade_date)
        if ts in self._prev_date_cache:
            return self._prev_date_cache[ts]

        if not self._all_dates_sorted:
            all_dates: set[pd.Timestamp] = set()
            for df in self._data.values():
                all_dates.update(df.index.tolist())
            self._all_dates_sorted = sorted(all_dates)

        result = None
        for d in self._all_dates_sorted:
            if d >= ts:
                break
            result = d
        self._prev_date_cache[ts] = result
        return result

    def get_history(self, symbol: str, end_date: date, n_days: int = 60) -> pd.DataFrame:
        """获取某标的截止某日的历史数据"""
        df = self._data.get(symbol)
        if df is None:
            return pd.DataFrame()
        ts = pd.Timestamp(end_date)
        mask = df.index <= ts
        return df.loc[mask].tail(n_days)

    def get_intraday(self, symbol: str, trade_date: date) -> list[Bar]:
        """获取某标的某日的所有分钟 Bar"""
        df = self._minute_data.get(symbol)
        if df is None:
            return []
        day_start = pd.Timestamp(datetime.combine(trade_date, datetime.min.time()))
        day_end = day_start + pd.Timedelta(days=1)
        mask = (df.index >= day_start) & (df.index < day_end)
        rows = df.loc[mask]
        return [Bar(symbol, idx.to_pydatetime(), row.to_dict(), source=self)
                for idx, row in rows.iterrows()]

    def get_intraday_history(self, symbol: str, end_date: date, n_days: int = 5) -> pd.DataFrame:
        """获取某标的截止某日的 N 日分钟 OHLCV 数据"""
        df = self._minute_data.get(symbol)
        if df is None:
            return pd.DataFrame()
        end_ts = pd.Timestamp(datetime.combine(end_date, datetime.max.time()))
        start_ts = end_ts - pd.Timedelta(days=n_days * 2)
        mask = (df.index <= end_ts) & (df.index >= start_ts)
        return df.loc[mask].copy()

    def iter_trading_dates(self) -> Iterator[date]:
        """按交易日历迭代日期"""
        yield from self.calendar.get_date_range(self.start_date, self.end_date)

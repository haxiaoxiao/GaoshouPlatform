"""交易日历 — 基于 MarketDataStore klines_daily"""
from datetime import date

from loguru import logger


class TradingCalendar:
    """交易日历 — 从行情存储获取有效交易日列表"""

    def __init__(self, trading_dates: list[date] | None = None):
        """
        Args:
            trading_dates: 预加载的交易日列表；若为 None 则延迟加载
        """
        self._dates: list[date] | None = None
        self._dates_set: set[date] | None = None
        if trading_dates is not None:
            self._dates = sorted(trading_dates)
            self._dates_set = set(self._dates)

    @classmethod
    async def from_clickhouse(
        cls,
        symbols: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> "TradingCalendar":
        """从行情存储加载唯一交易日 (线程池中执行)"""
        import asyncio

        from app.data_stores import get_market_data_store

        def _load():
            store = get_market_data_store()
            sd = start_date or date(2000, 1, 1)
            ed = end_date or date.today()
            return store.load_trading_dates(symbols, sd, ed)

        dates = await asyncio.to_thread(_load)
        logger.info("TradingCalendar: {} trading days", len(dates))
        return cls(dates)

    @classmethod
    def from_list(cls, dates: list[date]) -> "TradingCalendar":
        """从已有日期列表创建（测试用）"""
        return cls(dates)

    @property
    def dates(self) -> list[date]:
        if self._dates is None:
            self._ensure_loaded()
        return self._dates  # type: ignore[return-value]

    def _ensure_loaded(self) -> None:
        if self._dates is None:
            raise RuntimeError("TradingCalendar not loaded — call from_clickhouse() or from_list() first")

    def is_trading_date(self, d: date) -> bool:
        if self._dates_set is None:
            self._ensure_loaded()
        return d in self._dates_set  # type: ignore[operator]

    def get_next_trading_date(self, d: date) -> date | None:
        dates = self.dates
        for td in dates:
            if td > d:
                return td
        return None

    def get_previous_trading_date(self, d: date) -> date | None:
        dates = self.dates
        prev = None
        for td in dates:
            if td >= d:
                return prev
            prev = td
        return prev

    def get_date_range(self, start: date, end: date) -> list[date]:
        """返回 [start, end] 范围内的交易日"""
        dates = self.dates
        return [d for d in dates if start <= d <= end]

    def __len__(self) -> int:
        return len(self.dates)

    def __contains__(self, d: date) -> bool:
        return self.is_trading_date(d)

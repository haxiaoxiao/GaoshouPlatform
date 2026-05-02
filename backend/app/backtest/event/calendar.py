"""交易日历 — 基于 ClickHouse klines_daily"""
from datetime import date, timedelta

from loguru import logger


class TradingCalendar:
    """交易日历 — 从 ClickHouse 获取有效交易日列表"""

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
        """从 ClickHouse klines_daily 加载唯一交易日"""
        from app.db.clickhouse import get_ch_client

        ch = get_ch_client()
        query = "SELECT DISTINCT trade_date FROM klines_daily WHERE symbol IN %(syms)s"
        params: dict = {"syms": symbols}
        if start_date:
            query += " AND trade_date >= %(start)s"
            params["start"] = start_date
        if end_date:
            query += " AND trade_date <= %(end)s"
            params["end"] = end_date
        query += " ORDER BY trade_date"

        rows = ch.execute(query, params)
        dates = sorted({r[0] for r in rows})
        logger.info("TradingCalendar: {} trading days from {}-{}", len(dates),
                     dates[0] if dates else "N/A", dates[-1] if dates else "N/A")
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

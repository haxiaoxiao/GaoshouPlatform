"""ClickHouse → akquant DataFeedAdapter"""
from __future__ import annotations

from datetime import date

import pandas as pd
from loguru import logger

from app.backtest.engine.akquant import AKQUANT_AVAILABLE
from app.backtest.engine.interface import IDataProvider

if AKQUANT_AVAILABLE:
    from akquant.feed_adapter import BasePandasFeedAdapter, FeedSlice
else:
    BasePandasFeedAdapter = object
    FeedSlice = object


class ClickHouseFeedAdapter(BasePandasFeedAdapter):
    """从 ClickHouse 预加载数据，适配为 akquant DataFeedAdapter"""

    name = "clickhouse"

    def __init__(
        self,
        data_provider: IDataProvider,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ):
        self._data_provider = data_provider
        self._symbols = symbols
        self._start_date = start_date
        self._end_date = end_date
        self._cache: dict[str, pd.DataFrame] = {}
        self._loaded = False

    async def preload(self):
        """预加载所有 symbol 的日线数据到内存"""
        if self._loaded:
            return

        df_all = await self._data_provider.load_daily(
            self._symbols, self._start_date, self._end_date
        )

        if df_all.empty:
            logger.warning("ClickHouseFeedAdapter: no data loaded")
            self._loaded = True
            return

        for sym, grp in df_all.groupby("symbol"):
            sym_str = str(sym)
            grp = grp.drop(columns=["symbol"], errors="ignore")
            self._cache[sym_str] = grp.copy()

        self._loaded = True
        logger.info(
            "ClickHouseFeedAdapter: loaded {} symbols, {} total rows",
            len(self._cache), len(df_all),
        )

    def load(self, request: "FeedSlice") -> pd.DataFrame:
        """akquant 按 symbol + 时间窗口按需调用"""
        if not self._loaded:
            return pd.DataFrame()

        sym = str(request.symbol)
        df = self._cache.get(sym)
        if df is None:
            return pd.DataFrame()

        df = df.copy()
        df = self.normalize(df, sym)

        return self._clip_time_range(df, request.start_time, request.end_time)

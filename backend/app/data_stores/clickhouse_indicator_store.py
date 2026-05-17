"""ClickHouse 指标数据存储 — 包装现有 CH 查询实现 IndicatorStore"""
from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from loguru import logger

from app.data_stores.indicator_base import IndicatorStore
from app.db.clickhouse import get_ch_client


class ClickHouseIndicatorStore(IndicatorStore):
    """将现有 ClickHouse stock_indicators/indicator_timeseries 包装为 IndicatorStore"""

    def _execute(self, query: str, params: dict | None = None):
        ch = get_ch_client()
        try:
            return ch.execute(query, params or {})
        finally:
            try:
                ch.disconnect()
            except Exception:
                pass

    def load_cross_section(
        self,
        names: list[str],
        trade_date: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        conditions = ["trade_date = %(dt)s"]
        params: dict = {"dt": trade_date}
        if names:
            conditions.append("indicator_name IN %(names)s")
            params["names"] = tuple(names)
        if symbols:
            conditions.append("symbol IN %(syms)s")
            params["syms"] = tuple(symbols)
        where = " AND ".join(conditions)

        rows = self._execute(
            f"SELECT symbol, indicator_name, trade_date, value, updated_at "
            f"FROM stock_indicators WHERE {where} "
            f"ORDER BY symbol, indicator_name",
            params,
        )
        if not rows:
            return pd.DataFrame(columns=["symbol", "indicator_name", "trade_date", "value", "updated_at"])
        return pd.DataFrame(rows, columns=["symbol", "indicator_name", "trade_date", "value", "updated_at"])

    def load_timeseries(
        self,
        names: list[str],
        start: date,
        end: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        conditions = [
            "indicator_name IN %(names)s",
            "datetime >= %(start)s",
            "datetime < %(end_plus)s",
        ]
        params: dict = {
            "names": tuple(names),
            "start": datetime.combine(start, datetime.min.time()),
            "end_plus": datetime.combine(end, datetime.min.time()) + pd.Timedelta(days=1),
        }
        if symbols:
            conditions.append("symbol IN %(syms)s")
            params["syms"] = tuple(symbols)
        where = " AND ".join(conditions)

        rows = self._execute(
            f"SELECT symbol, indicator_name, datetime, value, updated_at "
            f"FROM indicator_timeseries WHERE {where} "
            f"ORDER BY symbol, indicator_name, datetime",
            params,
        )
        if not rows:
            return pd.DataFrame(columns=["symbol", "indicator_name", "datetime", "value", "updated_at"])
        return pd.DataFrame(rows, columns=["symbol", "indicator_name", "datetime", "value", "updated_at"])

    def write_cross_section(self, df: pd.DataFrame) -> int:
        logger.warning("ClickHouseIndicatorStore.write_cross_section 未实现，请使用现有调度器")
        return 0

    def write_timeseries(self, df: pd.DataFrame) -> int:
        logger.warning("ClickHouseIndicatorStore.write_timeseries 未实现，请使用现有调度器")
        return 0

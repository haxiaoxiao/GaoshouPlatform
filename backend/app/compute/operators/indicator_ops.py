"""L1 指标算子 — 从 stock_indicators 表获取因子数据"""
from datetime import date, datetime, timedelta

import pandas as pd
from clickhouse_driver import Client

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class IndicatorOp(Operator):
    """指标算子 — 获取 stock_indicators 表中的因子值"""

    name = "indicator"
    level = 1
    category = "indicator"
    signature: str = "indicator(indicator_name, symbol)"

    def __init__(self):
        super().__init__()
        self._cache: dict[str, pd.Series] = {}
        self._cache_date: date | None = None
        self._ch_client: Client | None = None

    def _get_ch_client(self) -> Client:
        if self._ch_client is None:
            from app.db.clickhouse import get_ch_client

            self._ch_client = get_ch_client()
        return self._ch_client

    def _load_indicators(self, indicator_name: str, trade_date: date) -> pd.Series:
        """从 ClickHouse 加载指标数据"""
        cache_key = f"{indicator_name}:{trade_date}"
        today = date.today()

        # 每天只加载一次
        if self._cache_date != today or cache_key not in self._cache:
            self._cache.clear()
            self._cache_date = today

            ch = self._get_ch_client()

            # 获取指定指标的当日数据
            result = ch.execute(
                """
                SELECT symbol, value FROM stock_indicators
                WHERE indicator_name = %(name)s AND trade_date = %(date)s
                """,
                {"name": indicator_name, "date": today},
            )

            if result:
                symbols, values = zip(*result)
                self._cache[cache_key] = pd.Series(values, index=symbols)
            else:
                self._cache[cache_key] = pd.Series(dtype=float)

        return self._cache.get(cache_key, pd.Series(dtype=float))

    def apply(self, *args, **kwargs) -> pd.Series:
        if len(args) < 1:
            raise TypeError(f"{self.name} requires at least indicator_name argument")

        # 获取指标名称
        indicator_name = args[0]

        # 如果是 series，取最后一天的值
        if "series" in kwargs:
            series = kwargs["series"]
            if isinstance(series, pd.Series) and len(series) > 0:
                trade_date = series.index[-1]
                if isinstance(trade_date, (datetime, pd.Timestamp)):
                    trade_date = trade_date.date()
            else:
                trade_date = date.today()
        else:
            trade_date = date.today()

        # 加载指标数据
        indicator_series = self._load_indicators(indicator_name, trade_date)

        # 返回 series 形式（与价格数据对应）
        if isinstance(series, pd.Series) and len(series) > 0:
            # 返回与输入 series 相同长度的指标值
            result = pd.Series(index=series.index, dtype=float)
            for idx in series.index:
                sym = idx  # 这里需要从index提取symbol，暂时简化处理
                if sym in indicator_series.index:
                    result.loc[idx] = indicator_series.loc[sym]
            return result

        return indicator_series


# 注册指标算子
OperatorRegistry.register(IndicatorOp())


class PeTtmOp(Operator):
    """PE TTM 算子"""

    name = "pe_ttm"
    level = 1
    category = "indicator"
    signature: str = "pe_ttm(series)"

    def apply(self, *args, **kwargs) -> pd.Series:
        if "series" not in kwargs:
            raise TypeError(f"{self.name} requires 'series' argument")

        series = kwargs["series"]
        # PE TTM 需要从指标表获取
        # 这里暂时返回一个placeholder，实际计算需要因子引擎支持
        return pd.Series(0, index=series.index)


class DividendYieldOp(Operator):
    """股息率算子"""

    name = "dividend_yield"
    level = 1
    category = "indicator"
    signature: str = "dividend_yield(series)"

    def apply(self, *args, **kwargs) -> pd.Series:
        if "series" not in kwargs:
            raise TypeError(f"{self.name} requires 'series' argument")

        series = kwargs["series"]
        # 股息率需要从指标表获取
        return pd.Series(0, index=series.index)


for op in [PeTtmOp(), DividendYieldOp()]:
    OperatorRegistry.register(op)
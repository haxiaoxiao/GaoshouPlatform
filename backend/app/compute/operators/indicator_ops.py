"""L1 indicator operators backed by the local indicator store."""
from datetime import date, datetime

import pandas as pd

from app.compute.operators.base import Operator
from app.compute.operators.registry import OperatorRegistry


class IndicatorOp(Operator):
    """Load cross-sectional indicator values from the Parquet indicator store."""

    name = "indicator"
    level = 1
    category = "indicator"
    signature: str = "indicator(indicator_name, symbol)"

    def __init__(self):
        super().__init__()
        self._cache: dict[str, pd.Series] = {}
        self._cache_date: date | None = None

    def _load_indicators(self, indicator_name: str, trade_date: date) -> pd.Series:
        cache_key = f"{indicator_name}:{trade_date}"
        today = date.today()
        if self._cache_date != today or cache_key not in self._cache:
            self._cache.clear()
            self._cache_date = today

            from app.data_stores import get_indicator_store

            df = get_indicator_store().load_cross_section([indicator_name], trade_date)
            if df.empty:
                self._cache[cache_key] = pd.Series(dtype=float)
            else:
                self._cache[cache_key] = pd.Series(
                    df["value"].astype(float).tolist(),
                    index=df["symbol"].astype(str).tolist(),
                )
        return self._cache.get(cache_key, pd.Series(dtype=float))

    def apply(self, *args, **kwargs) -> pd.Series:
        if len(args) < 1:
            raise TypeError(f"{self.name} requires at least indicator_name argument")

        indicator_name = str(args[0])
        series = kwargs.get("series")
        if isinstance(series, pd.Series) and len(series) > 0:
            trade_date = series.index[-1]
            if isinstance(trade_date, (datetime, pd.Timestamp)):
                trade_date = trade_date.date()
        else:
            trade_date = date.today()

        indicator_series = self._load_indicators(indicator_name, trade_date)
        if isinstance(series, pd.Series) and len(series) > 0:
            result = pd.Series(index=series.index, dtype=float)
            for idx in series.index:
                symbol = str(idx)
                if symbol in indicator_series.index:
                    result.loc[idx] = indicator_series.loc[symbol]
            return result
        return indicator_series


class PeTtmOp(Operator):
    name = "pe_ttm"
    level = 1
    category = "indicator"
    signature: str = "pe_ttm(series)"

    def apply(self, *args, **kwargs) -> pd.Series:
        if "series" not in kwargs:
            raise TypeError(f"{self.name} requires 'series' argument")
        return pd.Series(0, index=kwargs["series"].index)


class DividendYieldOp(Operator):
    name = "dividend_yield"
    level = 1
    category = "indicator"
    signature: str = "dividend_yield(series)"

    def apply(self, *args, **kwargs) -> pd.Series:
        if "series" not in kwargs:
            raise TypeError(f"{self.name} requires 'series' argument")
        return pd.Series(0, index=kwargs["series"].index)


for op in [IndicatorOp(), PeTtmOp(), DividendYieldOp()]:
    OperatorRegistry.register(op)

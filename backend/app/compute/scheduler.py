"""Daily precompute scheduler for factor expressions."""
from datetime import date

import pandas as pd
from loguru import logger

from app.compute.cache import ComputeCache, get_compute_cache


class ComputeScheduler:
    def __init__(self, cache: ComputeCache | None = None):
        self.cache = cache or get_compute_cache()
        self._precomputed_expressions: list[str] = []

    def register_precompute(self, expression: str) -> None:
        if expression not in self._precomputed_expressions:
            self._precomputed_expressions.append(expression)

    async def run_daily_jobs(
        self,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> None:
        if not self._precomputed_expressions:
            logger.info("No precomputed expressions registered, skipping")
            return

        trade_date = trade_date or date.today()
        if symbols is None:
            logger.warning("No symbols provided for compute precompute")
            return
        if not symbols:
            logger.warning("No symbols found for precompute")
            return

        for expression in self._precomputed_expressions:
            try:
                self._precompute_one(expression, symbols, trade_date)
            except Exception as exc:
                logger.error("Failed to precompute '{}': {}", expression, exc)

    def _precompute_one(
        self,
        expression: str,
        symbols: list[str],
        trade_date: date,
    ) -> None:
        from app.compute.expression import evaluate_expression
        from app.data_stores import get_market_data_store

        columns = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount"]
        df = get_market_data_store().load_daily(symbols, date(2020, 1, 1), trade_date, columns=columns)
        if df.empty:
            return
        df = df.reset_index(drop=False)
        if "trade_date" not in df.columns and "index" in df.columns:
            df = df.rename(columns={"index": "trade_date"})
        for col in ["open", "high", "low", "close", "amount"]:
            if col in df.columns:
                df[col] = df[col].astype(float)
        if "turnover_rate" not in df.columns:
            df["turnover_rate"] = pd.NA
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        data = {symbol: group.set_index("trade_date") for symbol, group in df.groupby("symbol")}
        result = evaluate_expression(expression, data)
        if isinstance(result, pd.DataFrame):
            result = result.iloc[:, 0]

        expr_hash = self.cache.make_key(expression)
        if isinstance(result, dict):
            for symbol, series in result.items():
                ts_date = pd.Timestamp(trade_date)
                if ts_date in series.index:
                    value = series.loc[ts_date]
                    if pd.notna(value):
                        self.cache.save_to_parquet(
                            expr_hash,
                            trade_date,
                            pd.Series({symbol: float(value)}),
                            expression=expression,
                        )
        elif isinstance(result, pd.Series):
            self.cache.save_to_parquet(expr_hash, trade_date, result, expression=expression)


_scheduler: ComputeScheduler | None = None


def get_compute_scheduler() -> ComputeScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = ComputeScheduler()
    return _scheduler

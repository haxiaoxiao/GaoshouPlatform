"""Compute service — single and batch factor computation.

Extracted from compute/api.py; adds batch_compute for factor board use.
"""

from datetime import date, datetime

import pandas as pd
from loguru import logger

from app.compute.expression import evaluate_expression
from app.models.factor import FactorConfig, StockPool


class ComputeService:
    """Factor computation: single evaluate + batch compute."""

    async def evaluate(self, config: FactorConfig) -> dict:
        """Compute a single factor and return FactorMatrix-like dict."""
        symbols = await self._resolve_stock_pool(config.stock_pool)
        data = await self._load_market_data(symbols, config.start_date, config.end_date)
        result = evaluate_expression(config.expression, data)
        return self._to_matrix(result, config)

    async def batch_compute(self, configs: list[FactorConfig]) -> list[dict]:
        """Batch compute multiple factors."""
        results = []
        for cfg in configs:
            try:
                result = await self.evaluate(cfg)
                results.append({
                    "config": cfg.expression,
                    "status": "ok",
                    "data": result,
                })
            except Exception as e:
                logger.opt(exception=True).error(
                    "Batch compute failed: {}", cfg.expression
                )
                results.append({
                    "config": cfg.expression,
                    "status": "error",
                    "error": str(e),
                })
        return results

    async def _resolve_stock_pool(self, stock_pool: StockPool) -> list[str]:
        """Resolve stock pool name to list of symbols.

        Queries ClickHouse for all available symbols. In a production system,
        this would filter by actual index constituents (hs300, zz500, etc.).
        """
        from app.db.clickhouse import get_ch_client

        ch = get_ch_client()
        # Look back 2 years to capture symbols active during the evaluation window
        rows = ch.execute(
            "SELECT DISTINCT symbol FROM klines_daily "
            "WHERE trade_date >= today() - 730 "
            "ORDER BY symbol"
        )
        return [r[0] for r in rows]

    async def _load_market_data(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> dict:
        """Load market data from ClickHouse for the given symbols and date range.

        Returns dict[symbol -> DataFrame] keyed by trade_date.
        """
        from app.db.clickhouse import get_ch_client

        ch = get_ch_client()
        rows = ch.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s
              AND trade_date >= %(start)s
              AND trade_date <= %(end)s
            ORDER BY symbol, trade_date
            """,
            {"syms": symbols, "start": start_date, "end": end_date},
        )

        if not rows:
            return {}

        df = pd.DataFrame(
            rows,
            columns=[
                "symbol", "trade_date", "open", "high", "low", "close",
                "volume", "amount", "turnover_rate",
            ],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        return data

    def _to_matrix(self, result, config: FactorConfig) -> dict:
        """Convert expression result to dict format.

        Handles pd.DataFrame, pd.Series, and dict[str, pd.Series] results
        from the expression engine.
        """
        if isinstance(result, pd.DataFrame):
            return {
                "expression": config.expression,
                "stock_pool": config.stock_pool.value,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "data": result.to_dict(orient="records"),
                "shape": list(result.shape),
            }
        if isinstance(result, pd.Series):
            return {
                "expression": config.expression,
                "stock_pool": config.stock_pool.value,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "data": result.to_dict(),
                "length": len(result),
            }
        if isinstance(result, dict):
            # dict[str, pd.Series] format used by the expression engine
            out: dict[str, list[dict]] = {}
            for sym, series in result.items():
                if not isinstance(series, pd.Series):
                    continue
                out[sym] = [
                    {
                        "trade_date": str(idx.date()),
                        "value": float(v) if pd.notna(v) else None,
                    }
                    for idx, v in series.items()
                    if config.start_date <= idx.date() <= config.end_date
                ]
            return {
                "expression": config.expression,
                "stock_pool": config.stock_pool.value,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "data": out,
            }
        return {"expression": config.expression, "data": result}


compute_service = ComputeService()

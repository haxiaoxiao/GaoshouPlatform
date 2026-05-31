"""Compute service — single and batch factor computation.

Extracted from compute/api.py; adds batch_compute for factor board use.
"""

from datetime import date

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
        """Resolve stock pool name to list of symbols via index components."""
        from datetime import date as dt_date

        pool_name = stock_pool.value if hasattr(stock_pool, "value") else str(stock_pool)
        today = dt_date.today()

        # 指数池：走 index_components 表
        try:
            from app.services.index_catalog import get_index_item
            from app.services.index_components import load_index_symbols
        except Exception:
            get_index_item = None
            load_index_symbols = None

        item = get_index_item(pool_name) if get_index_item else None
        if item is not None:
            if not item.pool_enabled:
                raise ValueError(
                    f"Stock pool {pool_name} is unavailable because strict historical constituents are missing"
                )
            if load_index_symbols is None:
                raise RuntimeError("Index component loader is unavailable")
            symbols = await load_index_symbols(item.symbol, today, today)
            if symbols:
                return sorted(symbols)
            raise ValueError(f"Historical constituents for {item.symbol} are unavailable in the requested window")

        # Fallback: 从 SQLite stocks 表读取
        import sqlite3
        from pathlib import Path

        from app.core.config import settings
        db_path = Path(settings.data_dir) / "gaoshou.db"
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT symbol FROM stocks WHERE is_delist=0 AND is_st=0 ORDER BY symbol"
                ).fetchall()
                return [r[0] for r in rows]
        return []

    async def _load_market_data(
        self, symbols: list[str], start_date: date, end_date: date
    ) -> dict:
        """Load market data from store for the given symbols and date range.

        Returns dict[symbol -> DataFrame] keyed by trade_date.
        """
        import numpy as np

        from app.data_stores import get_market_data_store

        store = get_market_data_store()
        df = store.load_daily(symbols, start_date, end_date)
        if df.empty:
            return {}
        if "turnover_rate" not in df.columns:
            df["turnover_rate"] = np.nan

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp

        return data

    def _to_matrix(self, result, config: FactorConfig) -> dict:
        """Convert expression result to dict format.

        Handles pd.DataFrame, pd.Series, and dict[str, pd.Series] results
        from the expression engine.
        """
        if isinstance(result, pd.DataFrame):
            return {
                "expression": config.expression,
                "stock_pool": str(config.stock_pool),
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "data": result.to_dict(orient="records"),
                "shape": list(result.shape),
            }
        if isinstance(result, pd.Series):
            return {
                "expression": config.expression,
                "stock_pool": str(config.stock_pool),
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
                "stock_pool": str(config.stock_pool),
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "data": out,
            }
        return {"expression": config.expression, "data": result}


compute_service = ComputeService()

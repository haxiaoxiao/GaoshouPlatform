"""每日盘后预计算调度器"""
import logging
from datetime import date

import pandas as pd

from app.compute.cache import ComputeCache, get_compute_cache

logger = logging.getLogger(__name__)


class ComputeScheduler:
    """每日盘后批量预计算常用因子"""

    def __init__(self, cache: ComputeCache | None = None):
        self.cache = cache or get_compute_cache()
        self._precomputed_expressions: list[str] = []

    def register_precompute(self, expression: str) -> None:
        """注册一个需要每日预计算的表达式"""
        if expression not in self._precomputed_expressions:
            self._precomputed_expressions.append(expression)

    async def run_daily_jobs(
        self,
        symbols: list[str] | None = None,
        trade_date: date | None = None,
    ) -> None:
        """执行每日预计算任务"""
        if not self._precomputed_expressions:
            logger.info("No precomputed expressions registered, skipping")
            return

        if trade_date is None:
            trade_date = date.today()

        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()

        if symbols is None:
            rows = ch.execute("SELECT DISTINCT symbol FROM klines_daily")
            symbols = [r[0] for r in rows]

        if not symbols:
            logger.warning("No symbols found for precompute")
            return

        for expr in self._precomputed_expressions:
            try:
                self._precompute_one(expr, symbols, trade_date)
            except Exception as e:
                logger.error(f"Failed to precompute '{expr}': {e}")

    def _precompute_one(
        self,
        expression: str,
        symbols: list[str],
        trade_date: date,
    ) -> None:
        """预计算单个表达式并写入 L2 缓存"""
        from app.compute.expression import evaluate_expression
        from app.db.clickhouse import get_ch_client
        ch = get_ch_client()
        rows = ch.execute(
            """
            SELECT symbol, trade_date, open, high, low, close, volume, amount, turnover_rate
            FROM klines_daily
            WHERE symbol IN %(syms)s AND trade_date >= %(start)s
            ORDER BY symbol, trade_date
            """,
            {"syms": symbols, "start": date(2020, 1, 1)},
        )

        if not rows:
            return

        df = pd.DataFrame(
            rows,
            columns=["symbol", "trade_date", "open", "high", "low", "close", "volume", "amount", "turnover_rate"],
        )
        for col in ["open", "high", "low", "close", "amount", "turnover_rate"]:
            df[col] = df[col].astype(float)
        df["trade_date"] = pd.to_datetime(df["trade_date"])

        data = {}
        for sym, grp in df.groupby("symbol"):
            data[sym] = grp.set_index("trade_date")

        result = evaluate_expression(expression, data)
        if isinstance(result, pd.DataFrame):
            result = result.iloc[:, 0]

        expr_hash = self.cache.make_key(expression)
        if isinstance(result, dict):
            for sym, series in result.items():
                if trade_date in series.index:
                    val = series.loc[trade_date]
                    if pd.notna(val):
                        self.cache.save_to_ch(
                            expr_hash, trade_date,
                            pd.Series({sym: float(val)}),
                        )
        elif isinstance(result, pd.Series):
            self.cache.save_to_ch(expr_hash, trade_date, result)


# 全局单例
_scheduler: ComputeScheduler | None = None


def get_compute_scheduler() -> ComputeScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = ComputeScheduler()
    return _scheduler

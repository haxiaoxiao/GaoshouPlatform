"""深度价值策略，使用统一市场数据接口。

这是一条年度再平衡的低估值高股息策略：先在全市场里筛选“价格显著低于
长期均值、PE 为正且不过热、股息率足够高”的标的，再按折价程度和股息
收益综合排序，形成可长期持有的价值篮子。

它的定位不是短线交易，而是价值代理组合：用比较稳定的价格锚、盈利约束
和现金分红来筛掉大量噪声样本，只保留更接近“可长期拿住”的标的。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np

from app.data_stores import get_market_data_store
from app.data_stores.factory import get_indicator_store


@dataclass
class DVBasket:
    entry_date: date
    stocks: list[dict]  # [{symbol, entry_price, shares}]


class DeepValueStrategy:
    """筛选深度折价且高股息的股票，并在每年 5 月再平衡。

    这条策略的核心不是追涨，而是尽量在估值压缩、现金分红明确的前提下
    配置权益资产，所以它更像一个“价值代理”而不是短线交易器。

    参数含义:
        PRICE_TO_MA_MAX: 当前价格相对 250 日均线的上限，越低越强调折价。
        DIVIDEND_YIELD_MIN: 最低股息率门槛，用来过滤低分红或无分红样本。
        PE_MIN / PE_MAX: 盈利约束区间，避免把亏损或过热标的纳入组合。
        MAX_POSITIONS: 单次再平衡最多保留多少只股票。
        SINGLE_PCT: 单只股票的名义仓位占比，用于控制组合分散度。
    """

    PRICE_TO_MA_MAX = 0.9
    DIVIDEND_YIELD_MIN = 3.5
    PE_MIN = 0
    PE_MAX = 40
    MAX_POSITIONS = 10
    SINGLE_PCT = 0.10

    def __init__(self, pool_symbols: list[str] | None = None):
        # 市场数据和指标数据都通过抽象 store 读取，策略不关心底层到底是
        # Parquet 还是其它后端。
        self._store = get_market_data_store()
        self._indicator_store = get_indicator_store()
        self.pool_symbols = pool_symbols

    def _get_pool(self) -> list[str]:
        if self.pool_symbols:
            return self.pool_symbols
        # 默认股票池取本地 SQLite 快照中的正常交易标的：排除 ST 和退市股，
        # 避免把不可用样本混进价值筛选。
        from sqlalchemy import create_engine, select

        from app.core.config import settings
        from app.db.models.stock import Stock

        url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    select(Stock.symbol).where(Stock.is_st == 0, Stock.is_delist == 0)
                ).all()
            return [r[0] for r in rows]
        finally:
            engine.dispose()

    def _get_may_dates(self, start: date, end: date) -> list[date]:
        """返回每一年 5 月第一个可交易日。

        这是策略的年度调仓节拍：用 5 月第一个交易日作为统一再平衡点，
        便于和“年度价值篮子”假设对齐。
        """
        all_dates = self._store.load_trading_dates([], start, end)
        may_dates = []
        for trade_date in all_dates:
            if trade_date.month == 5:
                # 每年只在 5 月第一个可交易日调仓一次，用季节性窗口保持
                # 节奏一致，减少不必要的换仓噪声。
                year = trade_date.year
                if not may_dates or may_dates[-1].year != year:
                    may_dates.append(trade_date)
        return may_dates

    def screen(self, as_of_date: date, pool: list[str]) -> list[dict]:
        """按折价深度和股息率对候选股票排序。

        返回字段包括 close、ma250、ratio、pe、dividend_yield 和 score，
        其中 score 只是对已满足硬门槛的候选做相对排序。
        """
        lookback = as_of_date - timedelta(days=400)
        df = self._store.load_daily(pool, lookback, as_of_date)
        if df.empty:
            return []

        # 取每个标的最新可用收盘价，再和 250 日均线比较，衡量折价深度。
        df_sorted = df.sort_index()
        latest = df_sorted.groupby("symbol").last()
        close_map = {sym: float(row["close"]) for sym, row in latest.iterrows()}

        ma_map = {}
        for sym in pool:
            sym_data = df[df["symbol"] == sym]
            if len(sym_data) >= 250:
                ma_map[sym] = float(sym_data["close"].tail(250).mean())

        ind_df = self._indicator_store.load_cross_section(
            ["pe_ttm", "dividend_yield"], as_of_date, pool
        )
        pe_map = {}
        dy_map = {}
        if not ind_df.empty:
            for _, row in ind_df.iterrows():
                name = row["indicator_name"]
                val = float(row["value"]) if row["value"] is not None else None
                if val is None:
                    continue
                # 要求 PE 和股息率都来自与价格快照同一时点的截面数据，
                # 避免把不同日期的财务值混在一起。
                if name == "pe_ttm" and val > 0:
                    pe_map[row["symbol"]] = val
                elif name == "dividend_yield":
                    dy_map[row["symbol"]] = val

        candidates = []
        for sym, close in close_map.items():
            if close <= 0:
                continue
            ma250 = ma_map.get(sym)
            if not ma250 or ma250 <= 0:
                continue
            # 折价和股息率都要先过门槛，得分只负责给“已经合格”的股票做
            # 相对排序。
            ratio = close / ma250
            if ratio >= self.PRICE_TO_MA_MAX:
                continue
            pe = pe_map.get(sym)
            if pe is None or pe <= self.PE_MIN or pe >= self.PE_MAX:
                continue
            dividend_yield = dy_map.get(sym)
            if dividend_yield is None or dividend_yield <= self.DIVIDEND_YIELD_MIN:
                continue

            discount_score = min((1 - ratio) / 0.5, 1.0) * 50
            yield_score = min(dividend_yield / 15.0, 1.0) * 50
            candidates.append(
                {
                    "symbol": sym,
                    "close": close,
                    "ma250": round(ma250, 2),
                    "ratio": round(ratio, 4),
                    "pe": pe,
                    "dividend_yield": dividend_yield,
                    "score": round(discount_score + yield_score, 4),
                }
            )

        candidates.sort(key=lambda item: -item["score"])
        return candidates[: self.MAX_POSITIONS]

    def run(
        self,
        start_date: date,
        end_date: date,
        initial_capital: float = 1_000_000,
    ) -> dict[str, Any]:
        """运行年度再平衡回测。

        返回值包含交易、收益、分红和组合统计指标，便于前端直接展示。
        """
        pool = self._get_pool()
        rebalance_dates = self._get_may_dates(start_date, end_date)
        if len(rebalance_dates) < 2:
            return {
                "error": "not enough rebalance dates",
                "rebalance_dates": len(rebalance_dates),
            }

        baskets: list[DVBasket] = []
        all_trades: list[dict] = []

        for rebalance_date in rebalance_dates:
            for basket in list(baskets):
                for item in basket.stocks:
                    price = self._get_price(item["symbol"], rebalance_date)
                    if price and price > 0:
                        # 出场收益要把持有期内现金分红一起算进去，才能和
                        # “总回报”型价值假设对齐。
                        div_sum = self._get_dividends_between(
                            item["symbol"], basket.entry_date, rebalance_date
                        )
                        total_return = (price + div_sum) / item["entry_price"] - 1
                        pnl = total_return * 100
                        all_trades.append(
                            {
                                "symbol": item["symbol"],
                                "direction": "sell",
                                "trade_date": rebalance_date.isoformat(),
                                "entry_date": basket.entry_date.isoformat(),
                                "entry_price": round(item["entry_price"], 2),
                                "price": round(price, 2),
                                "dividend": round(div_sum, 4),
                                "pnl_pct": round(pnl, 2),
                            }
                        )
                baskets.remove(basket)

            candidates = self.screen(rebalance_date, pool)
            if not candidates:
                continue

            # 仓位采用等名义金额分配：这样策略更容易解释，也不会让单一
            # 标的在组合里过度主导。
            invest_per_stock = initial_capital * self.SINGLE_PCT
            basket_stocks = []
            for candidate in candidates:
                price = candidate["close"]
                shares = int(invest_per_stock / price / 100) * 100
                if shares < 100:
                    continue
                basket_stocks.append(
                    {
                        "symbol": candidate["symbol"],
                        "entry_price": price,
                        "shares": shares,
                        "ratio": candidate["ratio"],
                        "pe": candidate["pe"],
                        "dividend_yield": candidate["dividend_yield"],
                        "score": candidate["score"],
                    }
                )
                all_trades.append(
                    {
                        "symbol": candidate["symbol"],
                        "direction": "buy",
                        "trade_date": rebalance_date.isoformat(),
                        "price": round(price, 2),
                        "shares": shares,
                        "price_to_ma": candidate["ratio"],
                        "pe": candidate["pe"],
                        "dividend_yield": candidate["dividend_yield"],
                    }
                )

            if basket_stocks:
                baskets.append(DVBasket(rebalance_date, basket_stocks))

        final_date = end_date
        for basket in baskets:
            for item in basket.stocks:
                price = self._get_price(item["symbol"], final_date) or item["entry_price"]
                div_sum = self._get_dividends_between(
                    item["symbol"], basket.entry_date, final_date
                )
                total_return = (price + div_sum) / item["entry_price"] - 1
                pnl = total_return * 100
                all_trades.append(
                    {
                        "symbol": item["symbol"],
                        "direction": "sell",
                        "trade_date": final_date.isoformat(),
                        "entry_date": basket.entry_date.isoformat(),
                        "entry_price": round(item["entry_price"], 2),
                        "price": round(price, 2),
                        "dividend": round(div_sum, 4),
                        "pnl_pct": round(pnl, 2),
                    }
                )

        sell_trades = [
            trade
            for trade in all_trades
            if "pnl_pct" in trade and trade["pnl_pct"] is not None
        ]
        pnls = [trade["pnl_pct"] for trade in sell_trades]
        winning = [pnl for pnl in pnls if pnl > 0]

        groups: dict[str, list[float]] = {}
        for trade in sell_trades:
            groups.setdefault(trade.get("entry_date", "unknown"), []).append(
                trade["pnl_pct"]
            )
        basket_returns = [float(np.mean(values)) for values in groups.values()]

        total_return = (
            float(np.prod([1 + value / 100 for value in basket_returns]) - 1) * 100
            if basket_returns
            else 0.0
        )

        total_dividends = sum(trade.get("dividend", 0) for trade in sell_trades)
        return {
            "total_return": round(total_return / 100, 4),
            "annual_return": round(
                total_return / max((end_date - start_date).days / 365, 1) / 100, 4
            ),
            "total_trades": len(sell_trades),
            "win_trades": len(winning),
            "loss_trades": len(pnls) - len(winning),
            "win_rate": round(len(winning) / len(pnls), 4) if pnls else 0,
            "avg_return": round(float(np.mean(pnls)) / 100, 4) if pnls else 0,
            "total_dividends": round(total_dividends, 2),
            "sharpe_ratio": round(
                float(np.mean(pnls)) / (float(np.std(pnls)) + 0.001) / 100, 4
            )
            if pnls and len(pnls) > 1
            else 0,
            "max_drawdown": 0,
            "initial_capital": initial_capital,
            "final_capital": round(initial_capital * (1 + total_return / 100), 2),
            "basket_count": len(basket_returns),
            "basket_returns": [round(value, 2) for value in basket_returns],
            "trades": sorted(all_trades, key=lambda item: item.get("trade_date", "")),
        }

    def _get_price(self, symbol: str, as_of: date) -> float | None:
        lookback = as_of - timedelta(days=60)
        df = self._store.load_daily([symbol], lookback, as_of, columns=["close"])
        if df.empty:
            return None
        val = float(df["close"].iloc[-1])
        return val if val > 0 else None

    def _get_dividends_between(self, symbol: str, start: date, end: date) -> float:
        """返回区间内的现金分红总额（若可用）。

        现金分红来自指标库，因为它是点时公司行为序列，不是价格序列的自然属性。
        """
        df = self._indicator_store.load_cross_section(["dividend_cash"], end, [symbol])
        if df.empty:
            return 0.0
        vals = df[(df["trade_date"] > str(start)) & (df["trade_date"] <= str(end))]
        total = vals["value"].sum() if not vals.empty else 0.0
        return float(total) if total else 0.0

"""深度价值策略 — 年度调仓版本

每年5月第一个交易日后筛选，等权买入，持有到次年5月。
直接查询 ClickHouse，绕过事件引擎，10年回测 < 30秒。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np

from app.db.clickhouse import get_ch_client


@dataclass
class DVBasket:
    entry_date: date
    stocks: list[dict]  # [{symbol, entry_price, shares}]


class DeepValueStrategy:
    """深度价值策略 — 低PE + 高股息 + 深度折价"""

    PRICE_TO_MA_MAX = 0.9
    DIVIDEND_YIELD_MIN = 3.5
    PE_MIN = 0
    PE_MAX = 40
    MAX_POSITIONS = 10
    SINGLE_PCT = 0.10

    def __init__(self, pool_symbols: list[str] | None = None):
        self.ch = get_ch_client()
        self.pool_symbols = pool_symbols

    def _get_pool(self) -> list[str]:
        if self.pool_symbols:
            return self.pool_symbols
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings
        url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(
                select(Stock.symbol).where(Stock.is_st == 0, Stock.is_delist == 0)
            ).all()
        engine.dispose()
        return [r[0] for r in rows]

    def _get_may_dates(self, start: date, end: date) -> list[date]:
        """获取每个5月的第一个可用交易日"""
        rows = self.ch.execute(
            "SELECT DISTINCT trade_date FROM klines_daily "
            "WHERE trade_date >= %(s)s AND trade_date <= %(e)s "
            "ORDER BY trade_date",
            {"s": start, "e": end},
        )
        all_dates = [r[0] for r in rows]
        may_dates = []
        for d in all_dates:
            if d.month == 5:
                year = d.year
                if not may_dates or may_dates[-1].year != year:
                    may_dates.append(d)
        return may_dates

    def screen(self, as_of_date: date, pool: list[str]) -> list[dict]:
        """批量筛选候选股票 — 一次查询获取全量数据，按 score 排序"""
        # ── 批量获取收盘价 ──
        close_rows = self.ch.execute(
            "SELECT symbol, close FROM klines_daily "
            "WHERE symbol IN %(syms)s AND trade_date=("
            "SELECT max(trade_date) FROM klines_daily "
            "WHERE trade_date<=%(d)s AND symbol=klines_daily.symbol)",
            {"syms": tuple(pool), "d": as_of_date},
        )
        close_map = {r[0]: float(r[1]) for r in close_rows}

        # ── 批量获取250周均线 ──
        ma_rows = self.ch.execute(
            "SELECT symbol, avg(close) FROM ("
            "SELECT symbol, close, row_number() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn "
            "FROM klines_weekly WHERE symbol IN %(syms)s AND trade_date<=%(d)s"
            ") WHERE rn <= 250 GROUP BY symbol",
            {"syms": tuple(pool), "d": as_of_date},
        )
        ma_map = {r[0]: float(r[1]) for r in ma_rows if r[1]}

        # ── 批量获取PE ──
        pe_rows = self.ch.execute(
            "SELECT symbol, value FROM ("
            "SELECT symbol, value, row_number() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn "
            "FROM stock_indicators WHERE symbol IN %(syms)s "
            "AND indicator_name='pe_ttm' AND trade_date<=%(d)s"
            ") WHERE rn=1",
            {"syms": tuple(pool), "d": as_of_date},
        )
        pe_map = {r[0]: float(r[1]) for r in pe_rows if r[1] and float(r[1]) > 0}

        # ── 批量获取股息率 ──
        dy_rows = self.ch.execute(
            "SELECT symbol, value FROM ("
            "SELECT symbol, value, row_number() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn "
            "FROM stock_indicators WHERE symbol IN %(syms)s "
            "AND indicator_name='dividend_yield' AND trade_date<=%(d)s"
            ") WHERE rn=1",
            {"syms": tuple(pool), "d": as_of_date},
        )
        dy_map = {r[0]: float(r[1]) for r in dy_rows if r[1] and float(r[1]) > 0}

        # ── 计算评分 ──
        candidates = []
        for sym, close in close_map.items():
            if close <= 0:
                continue
            ma250 = ma_map.get(sym)
            if not ma250 or ma250 <= 0:
                continue
            ratio = close / ma250
            if ratio >= self.PRICE_TO_MA_MAX:
                continue
            pe = pe_map.get(sym)
            if pe is None or pe <= self.PE_MIN or pe >= self.PE_MAX:
                continue
            dy = dy_map.get(sym)
            if dy is None or dy <= self.DIVIDEND_YIELD_MIN:
                continue

            discount_score = min((1 - ratio) / 0.5, 1.0) * 50
            yield_score = min(dy / 15.0, 1.0) * 50
            candidates.append({
                "symbol": sym, "close": close,
                "ma250": round(ma250, 2), "ratio": round(ratio, 4),
                "pe": pe, "dividend_yield": dy,
                "score": round(discount_score + yield_score, 4),
            })

        candidates.sort(key=lambda x: -x["score"])
        return candidates[:self.MAX_POSITIONS]

    def run(
        self, start_date: date, end_date: date,
        initial_capital: float = 1_000_000,
    ) -> dict[str, Any]:
        """运行回测"""
        pool = self._get_pool()
        rebalance_dates = self._get_may_dates(start_date, end_date)
        if len(rebalance_dates) < 2:
            return {"error": "调仓日不足", "rebalance_dates": len(rebalance_dates)}

        # ── 批量加载每日收盘价（用于跟踪持仓） ──
        daily_prices: dict[str, dict[date, float]] = {}
        all_held: set[str] = set()

        baskets: list[DVBasket] = []
        all_trades: list[dict] = []

        for i, rebalance_date in enumerate(rebalance_dates):
            # ── 平仓：上一期持有的所有股票按当日价格卖出 ──
            for basket in list(baskets):
                for item in basket.stocks:
                    price = self._get_price(item["symbol"], rebalance_date)
                    if price and price > 0:
                        div_sum = self._get_dividends_between(
                            item["symbol"], basket.entry_date, rebalance_date
                        )
                        total_return = (price + div_sum) / item["entry_price"] - 1
                        pnl = total_return * 100
                        all_trades.append({
                            "symbol": item["symbol"],
                            "direction": "sell",
                            "trade_date": rebalance_date.isoformat(),
                            "entry_date": basket.entry_date.isoformat(),
                            "entry_price": round(item["entry_price"], 2),
                            "price": round(price, 2),
                            "dividend": round(div_sum, 4),
                            "pnl_pct": round(pnl, 2),
                        })
                baskets.remove(basket)

            # ── 筛选 & 买入 ──
            candidates = self.screen(rebalance_date, pool)
            if not candidates:
                continue

            invest_per_stock = initial_capital * self.SINGLE_PCT
            basket_stocks = []
            for c in candidates:
                price = c["close"]
                shares = int(invest_per_stock / price / 100) * 100
                if shares < 100:
                    continue
                basket_stocks.append({
                    "symbol": c["symbol"],
                    "entry_price": price,
                    "shares": shares,
                    "ratio": c["ratio"], "pe": c["pe"],
                    "dividend_yield": c["dividend_yield"], "score": c["score"],
                })
                all_trades.append({
                    "symbol": c["symbol"],
                    "direction": "buy",
                    "trade_date": rebalance_date.isoformat(),
                    "price": round(price, 2),
                    "shares": shares,
                    "price_to_ma": c["ratio"],
                    "pe": c["pe"],
                    "dividend_yield": c["dividend_yield"],
                })

            if basket_stocks:
                baskets.append(DVBasket(rebalance_date, basket_stocks))

        # ── 强制平仓（用 end_date 而非最后一期调仓日）──
        final_date = end_date
        for basket in baskets:
            for item in basket.stocks:
                price = self._get_price(item["symbol"], final_date) or item["entry_price"]
                div_sum = self._get_dividends_between(
                    item["symbol"], basket.entry_date, final_date
                )
                total_return = (price + div_sum) / item["entry_price"] - 1
                pnl = total_return * 100
                all_trades.append({
                    "symbol": item["symbol"],
                    "direction": "sell",
                    "trade_date": final_date.isoformat(),
                    "entry_date": basket.entry_date.isoformat(),
                    "entry_price": round(item["entry_price"], 2),
                    "price": round(price, 2),
                    "dividend": round(div_sum, 4),
                    "pnl_pct": round(pnl, 2),
                })

        # ── 统计 ──
        sell_trades = [t for t in all_trades if "pnl_pct" in t and t["pnl_pct"] is not None]
        pnls = [t["pnl_pct"] for t in sell_trades]
        winning = [p for p in pnls if p > 0]

        # 分组收益
        groups: dict[str, list[float]] = {}
        for t in sell_trades:
            groups.setdefault(t.get("entry_date", "unknown"), []).append(t["pnl_pct"])
        basket_returns = [float(np.mean(v)) for v in groups.values()]

        total_return = float(np.prod([1 + r/100 for r in basket_returns]) - 1) * 100 if basket_returns else 0.0

        total_dividends = sum(t.get("dividend", 0) for t in sell_trades)
        return {
            "total_return": round(total_return / 100, 4),  # 转为小数，与 v1 格式一致
            "annual_return": round(total_return / max((end_date - start_date).days / 365, 1) / 100, 4),
            "total_trades": len(sell_trades),
            "win_trades": len(winning),
            "loss_trades": len(pnls) - len(winning),
            "win_rate": round(len(winning) / len(pnls), 4) if pnls else 0,
            "avg_return": round(float(np.mean(pnls)) / 100, 4) if pnls else 0,
            "total_dividends": round(total_dividends, 2),
            "sharpe_ratio": round(float(np.mean(pnls)) / (float(np.std(pnls)) + 0.001) / 100, 4) if pnls and len(pnls) > 1 else 0,
            "max_drawdown": 0,
            "initial_capital": initial_capital,
            "final_capital": round(initial_capital * (1 + total_return / 100), 2),
            "basket_count": len(basket_returns),
            "basket_returns": [round(r, 2) for r in basket_returns],
            "trades": sorted(all_trades, key=lambda x: x.get("trade_date", "")),
        }

    def _get_price(self, symbol: str, as_of: date) -> float | None:
        r = self.ch.execute(
            "SELECT close FROM klines_daily "
            "WHERE symbol=%(s)s AND trade_date<=%(d)s "
            "ORDER BY trade_date DESC LIMIT 1",
            {"s": symbol, "d": as_of},
        )
        return float(r[0][0]) if r and r[0] and r[0][0] else None

    def _get_dividends_between(self, symbol: str, start: date, end: date) -> float:
        """获取两个日期之间的累计现金分红（每股）"""
        r = self.ch.execute(
            "SELECT sum(value) FROM stock_indicators "
            "WHERE symbol=%(s)s AND indicator_name='dividend_cash' "
            "AND trade_date > %(start)s AND trade_date <= %(end)s",
            {"s": symbol, "start": start, "end": end},
        )
        return float(r[0][0] or 0) if r and r[0] else 0.0

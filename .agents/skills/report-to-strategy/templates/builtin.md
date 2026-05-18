# 内置策略模板

适用于全市场批量筛选 + 高复杂度。独立 Python 类，绕过事件引擎，直接批量查 ClickHouse。

## 模板骨架

```python
"""策略名称 — 年度/季度调仓
描述策略逻辑
"""
from app.db.clickhouse import get_ch_client
import numpy as np
from datetime import date
from dataclasses import dataclass

@dataclass
class Basket:
    entry_date: date
    stocks: list[dict]

class MyStrategy:
    # 可调参数
    PARAM1 = default_value
    MAX_POSITIONS = 10
    SINGLE_PCT = 0.10

    def __init__(self, pool_symbols=None):
        self.ch = get_ch_client()
        self.pool_symbols = pool_symbols

    def _get_pool(self) -> list[str]:
        """获取股票池"""
        if self.pool_symbols: return self.pool_symbols
        from sqlalchemy import create_engine, select
        from app.db.models.stock import Stock
        from app.core.config import settings
        url = settings.database_url.replace("+aiosqlite", "")
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(select(Stock.symbol)
                .where(Stock.is_st==0, Stock.is_delist==0)).all()
        engine.dispose()
        return [r[0] for r in rows]

    def _get_rebalance_dates(self, start, end) -> list[date]:
        """获取调仓日期"""
        rows = self.ch.execute(
            "SELECT DISTINCT trade_date FROM klines_daily "
            "WHERE trade_date>=%(s)s AND trade_date<=%(e)s ORDER BY trade_date",
            {"s": start, "e": end})
        # 根据需要筛选（如只取每月第一个交易日）
        return [r[0] for r in rows]

    def screen(self, as_of_date, pool) -> list[dict]:
        """批量筛选 — 一次查询获取全量数据"""
        # 批量查收盘价
        close_rows = self.ch.execute(
            "SELECT symbol, close FROM klines_daily "
            "WHERE symbol IN %(s)s AND trade_date=("
            "SELECT max(trade_date) FROM klines_daily "
            "WHERE trade_date<=%(d)s AND symbol=klines_daily.symbol)",
            {"s": tuple(pool), "d": as_of_date})
        close_map = {r[0]: float(r[1]) for r in close_rows}

        # 批量查指标
        indicator_rows = self.ch.execute(
            "SELECT symbol, value FROM ("
            "SELECT symbol, value, row_number() OVER "
            "(PARTITION BY symbol ORDER BY trade_date DESC) AS rn "
            "FROM stock_indicators WHERE symbol IN %(s)s "
            "AND indicator_name=%(n)s AND trade_date<=%(d)s"
            ") WHERE rn=1",
            {"s": tuple(pool), "d": as_of_date, "n": "YOUR_INDICATOR"})
        indicator_map = {r[0]: float(r[1]) for r in indicator_rows if r[1]}

        # 评分筛选
        candidates = []
        for sym, close in close_map.items():
            value = indicator_map.get(sym)
            if value is None: continue
            # 条件判断 + 评分
            score = value
            candidates.append({"symbol": sym, "close": close, "score": score})
        candidates.sort(key=lambda x: -x["score"])
        return candidates[:self.MAX_POSITIONS]

    def run(self, start_date, end_date, initial_capital=1_000_000):
        pool = self._get_pool()
        rebalance_dates = self._get_rebalance_dates(start_date, end_date)
        baskets, all_trades = [], []

        for d in rebalance_dates:
            # 平仓
            for b in list(baskets):
                for item in b.stocks:
                    price = self._get_price(item["symbol"], d)
                    if price:
                        all_trades.append({
                            "symbol": item["symbol"], "direction": "sell",
                            "trade_date": d.isoformat(), "price": round(price,2),
                            "pnl_pct": round((price/item["entry_price"]-1)*100,2)
                        })
                baskets.remove(b)

            # 买入
            candidates = self.screen(d, pool)
            invest = initial_capital * self.SINGLE_PCT
            stocks = []
            for c in candidates:
                shares = int(invest / c["close"] / 100) * 100
                if shares < 100: continue
                stocks.append({"symbol": c["symbol"], "entry_price": c["close"], "shares": shares})
                all_trades.append({
                    "symbol": c["symbol"], "direction": "buy",
                    "trade_date": d.isoformat(), "price": round(c["close"],2), "shares": shares
                })
            if stocks: baskets.append(Basket(d, stocks))

        # 强制平仓
        for b in baskets:
            for item in b.stocks:
                price = self._get_price(item["symbol"], end_date) or item["entry_price"]
                all_trades.append({
                    "symbol": item["symbol"], "direction": "sell",
                    "trade_date": end_date.isoformat(), "price": round(price,2),
                    "pnl_pct": round((price/item["entry_price"]-1)*100,2)
                })

        sells = [t for t in all_trades if "pnl_pct" in t]
        pnls = [t["pnl_pct"] for t in sells]
        win = [p for p in pnls if p > 0]
        return {
            "total_return": ...,
            "total_trades": len(sells), "win_trades": len(win),
            "loss_trades": len(pnls)-len(win),
            "win_rate": round(len(win)/len(pnls),4) if pnls else 0,
            "trades": sorted(all_trades, key=lambda x: x.get("trade_date",""))
        }

    def _get_price(self, symbol, as_of):
        r = self.ch.execute(
            "SELECT close FROM klines_daily "
            "WHERE symbol=%(s)s AND trade_date<=%(d)s ORDER BY trade_date DESC LIMIT 1",
            {"s": symbol, "d": as_of})
        return float(r[0][0]) if r and r[0] and r[0][0] else None
```

## 关键设计原则

1. **批量查询代替逐股查询** — `WHERE symbol IN (tuple)` 一次拉全量
2. **row_number() OVER 窗口函数取最新值** — 避免 N+1 查询
3. **参数用类属性** — 允许 API 注入覆盖
4. **返回格式与 v1 回测一致** — total_return 为小数(0.15=15%)

## API 注册

新策略在 `backend/app/strategies/` 创建文件，在 `backend/app/api/strategy.py` 添加端点。

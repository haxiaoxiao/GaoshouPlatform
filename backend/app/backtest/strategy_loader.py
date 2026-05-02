"""策略加载器 — 表达式信号模式 + 订单 API"""
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd
from loguru import logger

from app.backtest.event.events import Event, EventType
from app.backtest.event.event_source import Bar, BarEventSource
from app.backtest.portfolio.account import Account
from app.backtest.portfolio.position import PositionManager


@dataclass
class StrategyContext:
    """策略上下文 — 注入给策略的回测环境"""

    account: Account
    position_manager: PositionManager
    current_date: date | None = None
    current_bar: Bar | None = None
    event_source: BarEventSource | None = None

    # 订单生成
    pending_orders: list[dict] = field(default_factory=list)
    _order_counter: int = 0

    def order_shares(
        self,
        symbol: str,
        shares: int,
        price: float | None = None,
        direction: str | None = None,
    ) -> str | None:
        """按股数下单

        Args:
            symbol: 标的代码
            shares: 正数为买，负数为卖
            price: 限价；None 则用当前 bar 收盘价
        """
        if shares == 0:
            return None

        if price is None and self.current_bar:
            price = self.current_bar.close
        if price is None or price <= 0:
            logger.warning("Cannot place order for {} — invalid price", symbol)
            return None

        if direction is None:
            direction = "buy" if shares > 0 else "sell"
        quantity = abs(shares)

        amount = quantity * price
        self._order_counter += 1
        order_id = f"ord_{self._order_counter:06d}"

        self.pending_orders.append({
            "order_id": order_id,
            "symbol": symbol,
            "direction": direction,
            "price": price,
            "quantity": quantity,
            "amount": amount,
            "date": self.current_date,
        })
        return order_id

    def order_value(
        self,
        symbol: str,
        value: float,
        price: float | None = None,
        direction: str = "buy",
    ) -> str | None:
        """按金额下单

        Args:
            symbol: 标的代码
            value: 买入金额（正数）
            price: 限价
            direction: "buy" or "sell"
        """
        if price is None and self.current_bar:
            price = self.current_bar.close
        if price is None or price <= 0 or value <= 0:
            return None

        shares = int(value / price / 100) * 100  # 整手
        if shares == 0:
            return None

        return self.order_shares(symbol, shares, price, direction)

    def clear_orders(self) -> None:
        self.pending_orders.clear()


class ExpressionSignalStrategy:
    """表达式信号策略 — 将因子表达式转换为买卖信号"""

    def __init__(
        self,
        expression: str,
        symbols: list[str],
        buy_threshold: float = 0.0,
        sell_threshold: float = 0.0,
        position_pct: float = 0.2,
    ):
        self.expression = expression
        self.symbols = symbols
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.position_pct = position_pct
        self._signal_cache: dict[str, pd.Series] = {}

    def register(self, event_bus, ctx: StrategyContext) -> None:
        """注册 BAR 监听器"""
        self._ctx = ctx
        event_bus.add_listener(EventType.BAR, self._on_bar)

    def _on_bar(self, event: Event) -> None:
        bar: Bar = event.data["bar"]
        date_val = event.data.get("date")
        source = self._ctx.event_source

        if source is None:
            return

        # 计算信号值
        hist = source.get_history(bar.symbol, bar.trade_date, n_days=252)
        if hist.empty:
            return

        try:
            sig = self._compute_signal(bar.symbol, {bar.symbol: hist})
            if sig is None:
                return
        except Exception as exc:
            logger.debug("Signal compute failed for {}: {}", bar.symbol, exc)
            return

        # 信号 → 订单
        if sig > self.buy_threshold:
            pos = self._ctx.position_manager.get_or_create(bar.symbol)
            if pos.total_shares == 0:
                available = self._ctx.account.available_cash * self.position_pct
                self._ctx.order_value(bar.symbol, available, bar.close, "buy")
        elif sig < self.sell_threshold:
            pos = self._ctx.position_manager.get(bar.symbol)
            if pos and pos.total_shares > 0:
                try:
                    self._ctx.order_shares(bar.symbol, -pos.total_shares, bar.close)
                except ValueError:
                    pass  # T+1 locked

    def _compute_signal(self, symbol: str, data: dict[str, pd.DataFrame]) -> float | None:
        from app.compute.expression import evaluate_expression

        cache_key = f"{symbol}:{data[symbol].index[-1]}"
        if cache_key in self._signal_cache:
            cached = self._signal_cache[cache_key]
            return float(cached.iloc[-1]) if len(cached) > 0 else None

        result = evaluate_expression(self.expression, data)
        if isinstance(result, dict):
            series = result.get(symbol)
        else:
            series = result

        if series is not None and len(series) > 0:
            self._signal_cache[cache_key] = series
            return float(series.iloc[-1])
        return None

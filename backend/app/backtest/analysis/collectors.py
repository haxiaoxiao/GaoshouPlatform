"""数据收集器 — 监听事件，汇聚交易/订单记录"""
from dataclasses import dataclass, field
from datetime import date, datetime

from app.backtest.event.events import Event, EventType


@dataclass
class TradeRecord:
    trade_id: str
    order_id: str
    symbol: str
    direction: str
    price: float
    quantity: int
    commission: float = 0.0
    pnl: float | None = None
    trade_date: date | None = None


@dataclass
class OrderRecord:
    order_id: str
    symbol: str
    direction: str
    price: float
    quantity: int
    status: str = "pending"
    filled_quantity: int = 0
    message: str = ""


class TradeCollector:
    """成交记录收集器"""

    def __init__(self):
        self.trades: list[TradeRecord] = []

    def register(self, event_bus) -> None:
        event_bus.add_listener(EventType.TRADE, self._on_trade, system=True)

    def _on_trade(self, event: Event) -> None:
        trade = event.data.get("trade")
        if trade is None:
            return
        self.trades.append(TradeRecord(
            trade_id=trade.get("trade_id", ""),
            order_id=trade.get("order_id", ""),
            symbol=trade.get("symbol", ""),
            direction=trade.get("direction", "buy"),
            price=trade.get("price", 0),
            quantity=trade.get("quantity", 0),
            commission=trade.get("commission", 0),
            pnl=trade.get("pnl"),
            trade_date=event.data.get("date"),
        ))

    def to_dicts(self) -> list[dict]:
        return [
            {
                "trade_id": t.trade_id,
                "order_id": t.order_id,
                "symbol": t.symbol,
                "direction": t.direction,
                "price": t.price,
                "quantity": t.quantity,
                "commission": t.commission,
                "pnl": t.pnl,
                "trade_date": str(t.trade_date) if t.trade_date else None,
            }
            for t in self.trades
        ]


class OrderCollector:
    """订单记录收集器"""

    def __init__(self):
        self.orders: list[OrderRecord] = []

    def register(self, event_bus) -> None:
        event_bus.add_listener(EventType.ORDER_CREATION_PASS, self._on_pass, system=True)
        event_bus.add_listener(EventType.ORDER_CREATION_REJECT, self._on_reject, system=True)

    def _on_pass(self, event: Event) -> None:
        order = event.data.get("order", {})
        self.orders.append(OrderRecord(
            order_id=order.get("order_id", ""),
            symbol=order.get("symbol", ""),
            direction=order.get("direction", "buy"),
            price=order.get("price", 0),
            quantity=order.get("quantity", 0),
            status="filled",
            filled_quantity=order.get("quantity", 0),
        ))

    def _on_reject(self, event: Event) -> None:
        order = event.data.get("order", {})
        self.orders.append(OrderRecord(
            order_id=order.get("order_id", ""),
            symbol=order.get("symbol", ""),
            direction=order.get("direction", "buy"),
            price=order.get("price", 0),
            quantity=order.get("quantity", 0),
            status="rejected",
            message=order.get("reason", ""),
        ))

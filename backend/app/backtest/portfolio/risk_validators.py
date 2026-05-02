"""风控校验器 — 注册为 system listener，校验失败阻断传播"""
from loguru import logger

from app.backtest.event.events import Event, EventType
from app.backtest.portfolio.account import Account
from app.backtest.portfolio.position import PositionManager


class CashValidator:
    """资金校验器 — 检查可用资金是否足够"""

    def __init__(self, account: Account):
        self.account = account

    def register(self, event_bus) -> None:
        event_bus.add_listener(EventType.ORDER_PENDING_NEW, self.validate, system=True)

    def validate(self, event: Event) -> bool:
        order = event.data.get("order", {})
        amount = order.get("amount", 0)
        direction = order.get("direction", "buy")

        if direction == "buy" and amount > self.account.available_cash:
            logger.warning(
                "CashValidator rejected: order={} available={:.2f} needed={:.2f}",
                order.get("order_id", "?"), self.account.available_cash, amount,
            )
            event.stop_propagation()
            return True
        return False


class PriceValidator:
    """价格校验器 — 检查委托价是否合法"""

    def __init__(self, limit_pct: float = 0.1):
        self.limit_pct = limit_pct

    def register(self, event_bus) -> None:
        event_bus.add_listener(EventType.ORDER_PENDING_NEW, self.validate, system=True)

    def validate(self, event: Event) -> bool:
        order = event.data.get("order", {})
        price = order.get("price", 0)
        ref_price = order.get("ref_price", 0)

        if price <= 0:
            logger.warning("PriceValidator rejected: invalid price {}", price)
            event.stop_propagation()
            return True

        if ref_price > 0 and abs(price / ref_price - 1) > self.limit_pct:
            logger.warning(
                "PriceValidator rejected: price {} deviates > {:.0%} from ref {}",
                price, self.limit_pct, ref_price,
            )
            event.stop_propagation()
            return True

        return False


class PositionLimitValidator:
    """持仓数量限制"""

    def __init__(self, position_manager: PositionManager, max_positions: int | None = None):
        self.position_manager = position_manager
        self.max_positions = max_positions

    def register(self, event_bus) -> None:
        if self.max_positions is not None and self.max_positions > 0:
            event_bus.add_listener(EventType.ORDER_PENDING_NEW, self.validate, system=True)

    def validate(self, event: Event) -> bool:
        order = event.data.get("order", {})
        direction = order.get("direction", "buy")

        if direction != "buy":
            return False

        symbol = order.get("symbol", "")
        current_count = sum(
            1 for s, p in self.position_manager.positions.items()
            if p.total_shares > 0
        )
        if symbol not in self.position_manager.positions or \
           self.position_manager.positions[symbol].total_shares == 0:
            current_count += 1

        if self.max_positions and current_count > self.max_positions:
            logger.warning(
                "PositionLimitValidator rejected: {} positions exceeds max {}",
                current_count, self.max_positions,
            )
            event.stop_propagation()
            return True

        return False

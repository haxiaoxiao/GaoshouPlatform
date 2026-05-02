"""事件类型与事件数据结构"""
from dataclasses import dataclass, field
from enum import auto, Enum
from datetime import datetime
from typing import Any


class EventType(str, Enum):
    """回测事件类型 — 按生命周期排序"""

    # 引擎生命周期
    ENGINE_START = "engine_start"
    ENGINE_END = "engine_end"

    # 日级生命周期
    BEFORE_TRADING = "before_trading"
    AFTER_TRADING = "after_trading"
    SETTLEMENT = "settlement"

    # Bar 生命周期
    PRE_BAR = "pre_bar"
    BAR = "bar"
    POST_BAR = "post_bar"

    # 订单生命周期
    ORDER_PENDING_NEW = "order_pending_new"
    ORDER_CREATION_PASS = "order_creation_pass"
    ORDER_CREATION_REJECT = "order_creation_reject"

    # 成交
    TRADE = "trade"


# 执行优先级: 数字越小越先执行
EVENT_PRIORITY: dict[EventType, int] = {
    EventType.ENGINE_START: 0,
    EventType.BEFORE_TRADING: 1,
    EventType.PRE_BAR: 2,
    EventType.BAR: 3,
    EventType.POST_BAR: 4,
    EventType.ORDER_PENDING_NEW: 5,
    EventType.ORDER_CREATION_PASS: 6,
    EventType.ORDER_CREATION_REJECT: 6,
    EventType.TRADE: 7,
    EventType.AFTER_TRADING: 8,
    EventType.SETTLEMENT: 9,
    EventType.ENGINE_END: 10,
}


@dataclass
class Event:
    """回测事件 — 在 EventBus 上发布的消息"""

    event_type: EventType
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime | None = None
    _propagate: bool = True

    @property
    def propagate(self) -> bool:
        return self._propagate

    def stop_propagation(self) -> None:
        """阻断事件传播 — 后续 listener 不会收到此事件"""
        self._propagate = False

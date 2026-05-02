"""事件总线 — 有序发布/订阅"""
from collections import defaultdict
from typing import Any, Callable

from loguru import logger

from app.backtest.event.events import Event, EventType

Listener = Callable[[Event], Any]


class EventBus:
    """同步事件总线 — system listeners 优先执行，返回 True 可阻断传播"""

    def __init__(self):
        self._listeners: dict[EventType, list[tuple[Listener, bool]]] = defaultdict(list)
        self._event_count: int = 0

    def add_listener(self, event_type: EventType, fn: Listener, *, system: bool = False) -> None:
        """注册事件监听器

        Args:
            event_type: 监听的事件类型
            fn: 回调函数 fn(event: Event) -> Any
            system: 是否为系统级监听器 (优先执行)
        """
        self._listeners[event_type].append((fn, system))

    def remove_listener(self, event_type: EventType, fn: Listener) -> None:
        """移除事件监听器"""
        self._listeners[event_type] = [
            (l, s) for l, s in self._listeners[event_type] if l != fn
        ]

    def publish_event(self, event: Event) -> bool:
        """发布事件 — 按 system-first 顺序通知监听器

        Returns:
            True 如果事件未被阻断，False 如果被系统监听器阻断
        """
        self._event_count += 1
        listeners = self._listeners.get(event.event_type, [])

        if not listeners:
            return True

        # system listeners first, then user listeners
        ordered = sorted(listeners, key=lambda x: (not x[1], 0))

        log_fn = logger.trace if self._event_count <= 50 else logger.debug

        for fn, is_system in ordered:
            try:
                result = fn(event)
                # System listener 返回 True 阻断传播
                if is_system and result is True and not event.propagate:
                    # Already stopped — don't dispatch to remaining
                    pass
                if not event.propagate:
                    log_fn(
                        "Event {} blocked by {} listener",
                        event.event_type.value,
                        "system" if is_system else "user",
                    )
                    return False
            except Exception as exc:
                logger.error(
                    "Error in {} listener {} for event {}: {}",
                    "system" if is_system else "user",
                    getattr(fn, "__name__", fn),
                    event.event_type.value,
                    exc,
                )

        return True

    def clear(self) -> None:
        """清除所有监听器"""
        self._listeners.clear()
        self._event_count = 0

    @property
    def listener_count(self) -> int:
        return sum(len(v) for v in self._listeners.values())

"""EventBus 单元测试"""
import pytest
from app.backtest.event.events import Event, EventType
from app.backtest.event.event_bus import EventBus


class TestEventBus:
    def test_add_and_publish_listener(self):
        bus = EventBus()
        received = []

        bus.add_listener(EventType.BAR, lambda e: received.append(e))
        event = Event(EventType.BAR, data={"symbol": "000300.SH"})
        bus.publish_event(event)

        assert len(received) == 1
        assert received[0].data["symbol"] == "000300.SH"

    def test_system_listener_runs_first(self):
        bus = EventBus()
        order = []

        bus.add_listener(EventType.BAR, lambda e: order.append("user"))
        bus.add_listener(EventType.BAR, lambda e: order.append("system"), system=True)

        bus.publish_event(Event(EventType.BAR))
        assert order == ["system", "user"]

    def test_stop_propagation_blocks_subsequent(self):
        bus = EventBus()
        received = []

        def blocker(e):
            e.stop_propagation()

        bus.add_listener(EventType.BAR, blocker, system=True)
        bus.add_listener(EventType.BAR, lambda e: received.append(e))

        result = bus.publish_event(Event(EventType.BAR))
        assert result is False
        assert len(received) == 0

    def test_multiple_event_types_independent(self):
        bus = EventBus()
        bars = []
        trades = []

        bus.add_listener(EventType.BAR, lambda e: bars.append(e))
        bus.add_listener(EventType.TRADE, lambda e: trades.append(e))

        bus.publish_event(Event(EventType.BAR))
        bus.publish_event(Event(EventType.BAR))
        bus.publish_event(Event(EventType.TRADE))

        assert len(bars) == 2
        assert len(trades) == 1

    def test_remove_listener(self):
        bus = EventBus()
        received = []

        def handler(e):
            received.append(e)

        bus.add_listener(EventType.BAR, handler)
        bus.remove_listener(EventType.BAR, handler)
        bus.publish_event(Event(EventType.BAR))

        assert len(received) == 0

    def test_listener_error_does_not_crash_bus(self):
        bus = EventBus()
        received = []

        def bad(e):
            raise RuntimeError("boom")

        bus.add_listener(EventType.BAR, bad)
        bus.add_listener(EventType.BAR, lambda e: received.append(e))

        bus.publish_event(Event(EventType.BAR))
        assert len(received) == 1

    def test_clear_removes_all(self):
        bus = EventBus()
        bus.add_listener(EventType.BAR, lambda e: None)
        bus.add_listener(EventType.TRADE, lambda e: None)

        assert bus.listener_count == 2
        bus.clear()
        assert bus.listener_count == 0

    def test_engine_lifecycle_events(self):
        bus = EventBus()
        lifecycle = []

        bus.add_listener(EventType.ENGINE_START, lambda e: lifecycle.append("start"))
        bus.add_listener(EventType.BEFORE_TRADING, lambda e: lifecycle.append("before"))
        bus.add_listener(EventType.AFTER_TRADING, lambda e: lifecycle.append("after"))
        bus.add_listener(EventType.ENGINE_END, lambda e: lifecycle.append("end"))

        bus.publish_event(Event(EventType.ENGINE_START))
        bus.publish_event(Event(EventType.BEFORE_TRADING))
        bus.publish_event(Event(EventType.AFTER_TRADING))
        bus.publish_event(Event(EventType.ENGINE_END))

        assert lifecycle == ["start", "before", "after", "end"]

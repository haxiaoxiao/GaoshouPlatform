"""事件驱动执行器 — 主事件循环"""
from datetime import date, datetime

from loguru import logger

from app.backtest.event.events import Event, EventType
from app.backtest.event.event_bus import EventBus
from app.backtest.event.event_source import Bar, BarEventSource


class EventDrivenExecutor:
    """事件驱动执行器 — 按交易日历遍历 Bar，拆分为生命周期事件分发"""

    def __init__(self, event_bus: EventBus, calendar=None):
        self.event_bus = event_bus
        self.calendar = calendar
        self._current_date: date | None = None
        self._start_time: datetime | None = None

    def run(self, event_source: BarEventSource) -> dict:
        """主事件循环

        Returns:
            执行上下文字典，包含 daily_snapshots, trades, orders 等
        """
        ctx: dict = {
            "start_time": datetime.now(),
            "end_time": None,
            "n_bars": 0,
            "n_dates": 0,
        }

        # ENGINE_START
        if not self.event_bus.publish_event(Event(EventType.ENGINE_START, data={"ctx": ctx})):
            logger.warning("ENGINE_START was blocked — aborting")
            return ctx

        for i, trade_date in enumerate(event_source.iter_trading_dates()):
            self._current_date = trade_date
            ctx["n_dates"] = i + 1

            bars = event_source.get_bars(trade_date)
            if not bars:
                continue

            # Phase 1: BEFORE_TRADING
            self.event_bus.publish_event(Event(
                EventType.BEFORE_TRADING,
                data={"date": trade_date, "bars": bars, "ctx": ctx},
                timestamp=datetime.now(),
            ))

            # Phase 2: Per-instrument BAR lifecycle
            for symbol, bar in bars.items():
                ctx["n_bars"] += 1

                # PRE_BAR
                if not self.event_bus.publish_event(Event(
                    EventType.PRE_BAR,
                    data={"bar": bar, "date": trade_date, "ctx": ctx},
                    timestamp=datetime.now(),
                )):
                    continue

                # BAR (主事件 — 策略信号在此)
                if not self.event_bus.publish_event(Event(
                    EventType.BAR,
                    data={"bar": bar, "date": trade_date, "ctx": ctx},
                    timestamp=datetime.now(),
                )):
                    continue

                # POST_BAR
                self.event_bus.publish_event(Event(
                    EventType.POST_BAR,
                    data={"bar": bar, "date": trade_date, "ctx": ctx},
                    timestamp=datetime.now(),
                ))

            # Phase 3: AFTER_TRADING + SETTLEMENT
            self.event_bus.publish_event(Event(
                EventType.AFTER_TRADING,
                data={"date": trade_date, "bars": bars, "ctx": ctx},
                timestamp=datetime.now(),
            ))

            self.event_bus.publish_event(Event(
                EventType.SETTLEMENT,
                data={"date": trade_date, "ctx": ctx},
                timestamp=datetime.now(),
            ))

            if i % 50 == 0:
                logger.debug("Event loop: processed {} dates, {} bars", ctx["n_dates"], ctx["n_bars"])

        # ENGINE_END
        self.event_bus.publish_event(Event(
            EventType.ENGINE_END,
            data={"ctx": ctx},
            timestamp=datetime.now(),
        ))

        ctx["end_time"] = datetime.now()
        elapsed = (ctx["end_time"] - ctx["start_time"]).total_seconds()
        logger.info(
            "Event-driven backtest complete: {} dates, {} bars in {:.1f}s",
            ctx["n_dates"], ctx["n_bars"], elapsed,
        )
        return ctx

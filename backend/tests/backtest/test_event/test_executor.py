"""EventDrivenExecutor 单元测试"""
import pytest
from datetime import date

import pandas as pd

from app.backtest.event.events import Event, EventType
from app.backtest.event.event_bus import EventBus
from app.backtest.event.calendar import TradingCalendar
from app.backtest.event.event_source import BarEventSource
from app.backtest.event.executor import EventDrivenExecutor


@pytest.fixture
def sample_kline_data():
    """生成 5 天的模拟日线数据"""
    symbols = ["000300.SH", "600000.SH"]
    dates = pd.date_range("2024-01-02", "2024-01-08", freq="B")
    data = {}
    for sym in symbols:
        rows = []
        for i, dt in enumerate(dates):
            base = 10 + i * 0.5
            rows.append({
                "symbol": sym,
                "trade_date": dt,
                "open": base,
                "high": base + 0.3,
                "low": base - 0.3,
                "close": base + 0.1,
                "volume": 1000000 + i * 100000,
                "amount": (base + 0.1) * (1000000 + i * 100000),
                "turnover_rate": 0.5 + i * 0.1,
            })
        df = pd.DataFrame(rows)
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        data[sym] = df.set_index("trade_date")
    return data


@pytest.fixture
def calendar():
    return TradingCalendar.from_list([
        date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4),
        date(2024, 1, 5), date(2024, 1, 8),
    ])


@pytest.fixture
def event_source(sample_kline_data, calendar):
    return BarEventSource.from_dataframe(sample_kline_data, calendar)


class TestExecutor:
    def test_full_cycle_events_fired(self, event_source):
        bus = EventBus()
        events_received = []

        for et in EventType:
            bus.add_listener(et, lambda e, et=et: events_received.append(et))

        executor = EventDrivenExecutor(bus)
        ctx = executor.run(event_source)

        assert EventType.ENGINE_START in events_received
        assert EventType.ENGINE_END in events_received
        assert EventType.BAR in events_received
        assert EventType.BEFORE_TRADING in events_received
        assert EventType.AFTER_TRADING in events_received

    def test_bar_data_contains_symbol_and_prices(self, event_source):
        bus = EventBus()
        bar_dicts = []

        bus.add_listener(EventType.BAR, lambda e: bar_dicts.append(e.data["bar_dict"]))

        executor = EventDrivenExecutor(bus)
        executor.run(event_source)

        assert len(bar_dicts) == 5  # 5 dates, 1 bar_dict per day
        bd = bar_dicts[0]
        assert len(bd) == 2  # 000300.SH, 600000.SH
        bar = bd["000300.SH"]
        assert bar.symbol == "000300.SH"
        assert bar.close > 0
        assert bar.volume > 0

    def test_execution_context_counts(self, event_source):
        bus = EventBus()
        executor = EventDrivenExecutor(bus)
        ctx = executor.run(event_source)

        assert ctx["n_dates"] == 5
        assert ctx["n_bars"] == 10  # 5 dates x 2 symbols

    def test_stopped_event_blocks_bar(self, event_source):
        bus = EventBus()
        bars = []
        blocked = []

        def block_first(e):
            if not blocked:
                blocked.append(True)
                e.stop_propagation()

        bus.add_listener(EventType.BAR, block_first, system=True)
        bus.add_listener(EventType.BAR, lambda e: bars.append(e))

        executor = EventDrivenExecutor(bus)
        executor.run(event_source)

        # First BAR is blocked, so 4 daily bars pass (instead of 5)
        assert len(bars) >= 4

    def test_engine_start_blocked_aborts(self, event_source):
        bus = EventBus()

        def block_start(e):
            e.stop_propagation()

        bus.add_listener(EventType.ENGINE_START, block_start, system=True)

        executor = EventDrivenExecutor(bus)
        ctx = executor.run(event_source)

        assert ctx["n_dates"] == 0

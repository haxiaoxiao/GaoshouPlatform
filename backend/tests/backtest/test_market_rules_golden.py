from datetime import date

import pytest

from app.backtest.event.event_source import Bar
from app.backtest.event.events import Event, EventType
from app.backtest.portfolio.risk_validators import TradabilityValidator


def _order_event(
    *,
    direction: str,
    close: float,
    volume: int,
    prev_close: float = 10.0,
    limit_up: float | None = None,
    limit_down: float | None = None,
) -> Event:
    limit_fields = {}
    if limit_up is not None:
        limit_fields["limit_up"] = limit_up
    if limit_down is not None:
        limit_fields["limit_down"] = limit_down
    bar = Bar(
        "000001.SZ",
        date(2026, 1, 5),
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": volume,
            "amount": close * volume,
            "prev_close": prev_close,
            **limit_fields,
        },
    )
    return Event(
        EventType.ORDER_PENDING_NEW,
        data={"order": {"direction": direction, "symbol": "000001.SZ"}, "bar": bar},
    )


@pytest.mark.parametrize(
    ("direction", "close", "volume"),
    [
        ("buy", 10.0, 0),
        ("sell", 10.0, 0),
        ("buy", 11.0, 1000),
        ("sell", 9.0, 1000),
    ],
)
def test_tradability_golden_rejects_suspension_and_locked_limit_orders(
    direction: str,
    close: float,
    volume: int,
):
    event = _order_event(direction=direction, close=close, volume=volume)

    rejected = TradabilityValidator().validate(event)

    assert rejected is True
    assert event.propagate is False


@pytest.mark.parametrize(("direction", "close"), [("sell", 11.0), ("buy", 9.0), ("buy", 10.0)])
def test_tradability_golden_allows_executable_order_sides(direction: str, close: float):
    event = _order_event(direction=direction, close=close, volume=1000)

    rejected = TradabilityValidator().validate(event)

    assert rejected is False
    assert event.propagate is True


def test_tradability_golden_prefers_exchange_supplied_limit_price():
    event = _order_event(
        direction="buy",
        close=10.5,
        volume=1000,
        limit_up=10.5,
        limit_down=9.5,
    )

    rejected = TradabilityValidator().validate(event)

    assert rejected is True
    assert event.propagate is False

import asyncio
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.db.models.base import Base
from app.db.models.intraday_t import IntradayTSession
from app.services.intraday_t_paper import IntradayTPaperService, append_realtime_quote_bars
from app.services.intraday_t_strategy import StrategyParams


@pytest.fixture
async def paper_service():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    snapshots: dict[str, dict] = {}

    async def market_provider(now: datetime):
        values = snapshots[now.strftime("%H:%M")]
        return pd.DataFrame(
            [
                {
                    "symbol": "603629.SH",
                    "open": values["price"],
                    "high": values["price"],
                    "low": values["price"],
                    "close": values["price"],
                    "volume": 10_000,
                    "amount": values["price"] * 10_000,
                    "vwap": values.get("vwap", values["price"]),
                    "zscore": values["zscore"],
                    "previous_zscore": values["previous_zscore"],
                    "fast_ema": values["price"],
                    "slow_ema": values["price"],
                    "vwap_slope": 0.0,
                    "volume_ratio": 1.0,
                    "estimated_edge_bps": values.get("estimated_edge_bps", 100.0),
                    "previous_price": values.get("previous_price", values["price"] - 0.01),
                    "realized_vol_bps": values.get("realized_vol_bps", 30.0),
                    "session_return_bps": values.get("session_return_bps", 0.0),
                    "ready": True,
                }
            ],
            index=[pd.Timestamp(now)],
        )

    async def quote_provider(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.2,
                "askPrice": [20.21],
                "bidPrice": [20.19],
                "ask_volume": [10_000],
                "bid_volume": [10_000],
                "raw": {
                    "upStopPrice": 30.0,
                    "downStopPrice": 10.0,
                    "askVol": [10_000],
                    "bidVol": [10_000],
                },
            }
        ]

    service = IntradayTPaperService(
        session_factory=factory,
        market_provider=market_provider,
        quote_provider=quote_provider,
    )
    yield service, factory, snapshots
    await engine.dispose()


def _manual_account():
    return {
        "cash": 200_000.0,
        "positions": {
            "603629.SH": {"quantity": 2_000, "available": 2_000, "avg_cost": 20.0},
            "688008.SH": {"quantity": 1_000, "available": 1_000, "avg_cost": 70.0},
        },
    }


def test_realtime_quote_is_appended_after_local_warmup_without_using_cumulative_tick_volume():
    index = pd.date_range("2026-07-14 09:31", periods=14, freq="min")
    frame = pd.DataFrame(
        {
            "symbol": ["603629.SH"] * len(index),
            "open": [20.0] * len(index),
            "high": [20.1] * len(index),
            "low": [19.9] * len(index),
            "close": [20.0] * len(index),
            "volume": [1_000.0] * len(index),
            "amount": [2_000_000.0] * len(index),
        },
        index=index,
    )

    enriched = append_realtime_quote_bars(
        frame,
        {
            "603629.SH": {
                "lastPrice": 20.5,
                "high": 20.6,
                "low": 19.8,
                "volume": 9_999_999,
            }
        },
        datetime(2026, 7, 14, 9, 45),
    )

    latest = enriched.loc[pd.Timestamp("2026-07-14 09:45")]
    assert latest["close"] == 20.5
    assert latest["volume"] == 1_000
    assert latest["amount"] == 2_050_000


@pytest.mark.asyncio
async def test_paper_session_persists_and_can_be_loaded_by_a_new_service(paper_service):
    service, factory, _ = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )

    assert started["status"] == "RUNNING"
    assert started["account_source"] == "manual"
    assert started["states"]["603629.SH"]["opening_quantity"] == 2_000
    assert started["states"]["688008.SH"]["opening_sellable"] == 1_000
    assert started["real_order_submit_enabled"] is False

    recovered = await IntradayTPaperService(session_factory=factory).status(started["session_id"])
    assert recovered["session_id"] == started["session_id"]
    assert recovered["recoverable"] is True


@pytest.mark.asyncio
async def test_paper_evaluation_is_minute_idempotent_and_persists_complete_pair(paper_service):
    service, factory, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    session_id = started["session_id"]
    snapshots.update(
        {
            "10:00": {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 20.1, "zscore": -0.1, "previous_zscore": -2.0},
            "10:02": {"price": 20.2, "zscore": 0.0, "previous_zscore": -0.1},
        }
    )

    signal = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))
    assert signal["signals"][0]["side"] == "BUY"
    entry = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1))
    assert entry["fills"][0]["leg"] == "entry"
    duplicate = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1, 30))
    assert duplicate["duplicate"] is True
    restore = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 2))
    assert restore["fills"][0]["leg"] == "restore"

    trades = await IntradayTPaperService(session_factory=factory).trades(session_id)
    assert [item["side"] for item in trades] == ["BUY", "SELL"]
    status = await service.status(session_id)
    assert status["states"]["603629.SH"]["current_quantity"] == 2_000
    assert status["states"]["603629.SH"]["completed_pairs"] == 1


@pytest.mark.asyncio
async def test_paper_session_cannot_stop_with_an_unrestored_position(paper_service):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    session_id = started["session_id"]
    snapshots.update(
        {
            "10:00": {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 20.1, "zscore": -1.8, "previous_zscore": -2.0},
        }
    )
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1))

    with pytest.raises(ValueError, match="restore"):
        await service.stop(session_id, now=datetime(2026, 7, 14, 10, 2))


@pytest.mark.asyncio
async def test_paper_session_can_source_opening_positions_from_account_provider():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=StaticPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def account_provider():
        return {
            "cash": 50_000,
            "source": "qmt",
            "positions": _manual_account()["positions"],
        }

    service = IntradayTPaperService(session_factory=factory, account_provider=account_provider)
    started = await service.start(now=datetime(2026, 7, 14, 9, 35))
    assert started["account_source"] == "qmt"
    await engine.dispose()


@pytest.mark.asyncio
async def test_paper_runner_can_start_and_stop_without_losing_session_state(paper_service):
    service, _, _ = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )

    running = await service.start_runner(
        started["session_id"], interval_seconds=60, immediate=False
    )
    assert running["runner_active"] is True
    assert (await service.status(started["session_id"]))["runner_active"] is True

    stopped = await service.stop_runner(started["session_id"])
    assert stopped["runner_active"] is False
    assert stopped["status"] == "RUNNING"


@pytest.mark.asyncio
async def test_paper_session_carries_an_open_pair_and_restores_it_on_the_next_trade_date(
    paper_service,
):
    service, factory, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 20.1, "zscore": -1.8, "previous_zscore": -2.0},
            "10:02": {"price": 20.2, "zscore": -0.1, "previous_zscore": -1.8},
            "09:31": {"price": 20.2, "zscore": 0.0, "previous_zscore": 0.0},
            "09:32": {"price": 20.2, "zscore": 0.0, "previous_zscore": 0.0},
        }
    )
    await service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 0))
    await service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 1))
    await service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 2))

    async with factory() as db:
        row = await db.scalar(
            select(IntradayTSession).where(IntradayTSession.session_id == started["session_id"])
        )
        runtime = dict(row.runtime_state)
        states = dict(runtime["states"])
        state = dict(states["603629.SH"])
        state["completed_pairs"] = 1
        state["realized_net_pnl"] = 88.0
        states["603629.SH"] = state
        runtime["states"] = states
        row.runtime_state = runtime
        await db.commit()

    forced = await service.evaluate(started["session_id"], now=datetime(2026, 7, 15, 9, 31))
    assert forced["trade_date"] == "2026-07-15"
    assert forced["fills"] == []
    assert forced["signals"][0]["reason"] == "force_restore"
    assert forced["pending"]["603629.SH"]["signal_at"] == "2026-07-15T09:31:00"
    assert forced["states"]["603629.SH"]["completed_pairs"] == 0
    assert forced["states"]["603629.SH"]["realized_net_pnl"] == 0
    assert forced["states"]["603629.SH"]["sellable_remaining"] == 2_500

    restored = await service.evaluate(started["session_id"], now=datetime(2026, 7, 15, 9, 32))
    assert restored["fills"][0]["leg"] == "restore"
    assert restored["states"]["603629.SH"]["current_quantity"] == 2_000
    assert restored["states"]["603629.SH"]["active_direction"] is None


@pytest.mark.asyncio
async def test_same_session_evaluations_are_serialized_and_minute_idempotent(paper_service):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots["10:00"] = {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3}
    original_provider = service._market_provider
    active_calls = 0
    maximum_active_calls = 0

    async def slow_provider(now: datetime):
        nonlocal active_calls, maximum_active_calls
        active_calls += 1
        maximum_active_calls = max(maximum_active_calls, active_calls)
        try:
            await asyncio.sleep(0.05)
            return await original_provider(now)
        finally:
            active_calls -= 1

    service._market_provider = slow_provider
    results = await asyncio.gather(
        service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 0)),
        service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 0, 30)),
    )

    assert maximum_active_calls == 1
    assert sorted(result["duplicate"] for result in results) == [False, True]


@pytest.mark.asyncio
async def test_pending_paper_entry_is_cancelled_when_next_quote_reaches_a_price_limit(
    paper_service,
):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {"price": 19.9, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 20.0, "zscore": -0.1, "previous_zscore": -2.0},
        }
    )
    first = await service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 0))
    assert first["signals"]

    async def limit_quote(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.0,
                "askPrice": [20.0],
                "bidPrice": [19.99],
                "ask_volume": [10_000],
                "bid_volume": [10_000],
                "upStopPrice": 20.0,
                "downStopPrice": 16.0,
            }
        ]

    service._quote_provider = limit_quote
    blocked = await service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 1))

    assert blocked["fills"] == []
    assert blocked["pending"] == {}
    assert blocked["states"]["603629.SH"]["current_quantity"] == 2_000


@pytest.mark.asyncio
async def test_pending_entry_is_cancelled_when_only_its_fill_price_reaches_the_limit(
    paper_service,
):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {"price": 19.8, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 19.99, "zscore": -1.8, "previous_zscore": -2.0},
        }
    )
    session_id = started["session_id"]
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))

    async def quote_at_limit(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 19.99,
                "ask_price": [20.0],
                "bid_price": [19.98],
                "ask_volume": [10_000],
                "bid_volume": [10_000],
                "raw": {
                    "upStopPrice": 20.0,
                    "downStopPrice": 16.0,
                    "askVol": [10_000],
                    "bidVol": [10_000],
                },
            }
        ]

    service._quote_provider = quote_at_limit
    blocked = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1))

    assert blocked["fills"] == []
    assert blocked["pending"] == {}
    assert blocked["states"]["603629.SH"]["current_quantity"] == 2_000


@pytest.mark.asyncio
async def test_paper_does_not_open_a_new_pair_without_exact_limit_prices(paper_service):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots["10:00"] = {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3}

    async def quote_without_limits(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.0,
                "askPrice": [20.01],
                "bidPrice": [19.99],
            }
        ]

    service._quote_provider = quote_without_limits
    result = await service.evaluate(started["session_id"], now=datetime(2026, 7, 14, 10, 0))

    assert result["signals"] == []
    assert result["pending"] == {}
    assert result["states"]["603629.SH"]["current_quantity"] == 2_000


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entry_zscore", "entry_previous_zscore", "entry_side", "restore_side", "blocked_limit"),
    [
        (-2.0, -2.3, "BUY", "SELL", "downStopPrice"),
        (2.0, 2.3, "SELL", "BUY", "upStopPrice"),
    ],
)
async def test_restore_pending_retries_after_its_directional_price_limit_clears(
    paper_service,
    entry_zscore,
    entry_previous_zscore,
    entry_side,
    restore_side,
    blocked_limit,
):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {
                "price": 20.0,
                "zscore": entry_zscore,
                "previous_zscore": entry_previous_zscore,
            },
            "10:01": {
                "price": 20.0,
                "zscore": entry_zscore * 0.9,
                "previous_zscore": entry_zscore,
            },
            "10:02": {"price": 20.0, "zscore": 0.0, "previous_zscore": entry_zscore * 0.9},
            "10:03": {"price": 20.0, "zscore": 0.0, "previous_zscore": 0.0},
            "10:04": {"price": 20.1, "zscore": 0.0, "previous_zscore": 0.0},
        }
    )
    session_id = started["session_id"]
    signal = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))
    assert signal["signals"][0]["side"] == entry_side
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1))
    restore_signal = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 2))
    assert restore_signal["signals"][0]["side"] == restore_side

    async def blocked_quote(_symbols: list[str]):
        limits = {"upStopPrice": 22.0, "downStopPrice": 18.0}
        limits[blocked_limit] = 20.0
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.0,
                "askPrice": [20.0],
                "bidPrice": [20.0],
                "ask_volume": [10_000],
                "bid_volume": [10_000],
                "raw": {**limits, "askVol": [10_000], "bidVol": [10_000]},
            }
        ]

    service._quote_provider = blocked_quote
    blocked = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 3))
    assert blocked["fills"] == []
    assert blocked["pending"]["603629.SH"]["side"] == restore_side
    assert blocked["pending"]["603629.SH"]["signal_at"] == "2026-07-14T10:03:00"

    async def open_quote(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.1,
                "askPrice": [20.11],
                "bidPrice": [20.09],
                "ask_volume": [10_000],
                "bid_volume": [10_000],
                "raw": {
                    "upStopPrice": 22.0,
                    "downStopPrice": 18.0,
                    "askVol": [10_000],
                    "bidVol": [10_000],
                },
            }
        ]

    service._quote_provider = open_quote
    restored = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 4))
    assert restored["fills"][0]["leg"] == "restore"
    assert restored["pending"] == {}
    assert restored["states"]["603629.SH"]["current_quantity"] == 2_000


@pytest.mark.asyncio
async def test_pending_entry_is_cancelled_after_a_missing_natural_minute(paper_service):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3},
            "10:02": {"price": 20.1, "zscore": 0.0, "previous_zscore": -2.0},
        }
    )
    session_id = started["session_id"]
    first = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))
    assert first["pending"]

    skipped = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 2))

    assert skipped["fills"] == []
    assert skipped["pending"] == {}
    assert skipped["states"]["603629.SH"]["current_quantity"] == 2_000


@pytest.mark.asyncio
async def test_pending_entry_without_current_quote_depth_is_cancelled(paper_service):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 20.1, "zscore": -1.8, "previous_zscore": -2.0},
        }
    )
    session_id = started["session_id"]
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))

    async def quote_without_depth(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.1,
                "ask_price": [20.11],
                "bid_price": [20.09],
                "raw": {"upStopPrice": 22.0, "downStopPrice": 18.0},
            }
        ]

    service._quote_provider = quote_without_depth
    result = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1))

    assert result["fills"] == []
    assert result["pending"] == {}
    assert result["states"]["603629.SH"]["current_quantity"] == 2_000


@pytest.mark.asyncio
async def test_restore_pending_waits_for_sufficient_raw_qmt_depth(paper_service):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 20.1, "zscore": -1.8, "previous_zscore": -2.0},
            "10:02": {"price": 20.2, "zscore": 0.0, "previous_zscore": -1.8},
            "10:03": {"price": 20.2, "zscore": 0.0, "previous_zscore": 0.0},
            "10:04": {"price": 20.2, "zscore": 0.0, "previous_zscore": 0.0},
        }
    )
    session_id = started["session_id"]
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1))
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 2))

    async def shallow_quote(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.2,
                "bid_price": [20.19],
                "ask_price": [20.21],
                "bid_volume": [100],
                "ask_volume": [10_000],
                "raw": {
                    "upStopPrice": 22.0,
                    "downStopPrice": 18.0,
                    "bidVol": [100],
                    "askVol": [10_000],
                },
            }
        ]

    service._quote_provider = shallow_quote
    waiting = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 3))
    assert waiting["fills"] == []
    assert waiting["pending"]["603629.SH"]["side"] == "SELL"
    assert waiting["pending"]["603629.SH"]["signal_at"] == "2026-07-14T10:03:00"

    async def deep_quote(_symbols: list[str]):
        return [
            {
                "symbol": "603629.SH",
                "lastPrice": 20.2,
                "bid_price": [20.19],
                "ask_price": [20.21],
                "bid_volume": [500],
                "ask_volume": [10_000],
                "raw": {
                    "upStopPrice": 22.0,
                    "downStopPrice": 18.0,
                    "bidVol": [500],
                    "askVol": [10_000],
                },
            }
        ]

    service._quote_provider = deep_quote
    restored = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 4))
    assert restored["fills"][0]["leg"] == "restore"


@pytest.mark.asyncio
async def test_stale_restore_pending_is_regenerated_instead_of_filled(paper_service):
    service, _, snapshots = paper_service
    started = await service.start(
        manual_account=_manual_account(), now=datetime(2026, 7, 14, 9, 35)
    )
    snapshots.update(
        {
            "10:00": {"price": 20.0, "zscore": -2.0, "previous_zscore": -2.3},
            "10:01": {"price": 20.1, "zscore": -1.8, "previous_zscore": -2.0},
            "10:02": {"price": 20.2, "zscore": 0.0, "previous_zscore": -1.8},
            "10:04": {"price": 20.3, "zscore": 0.0, "previous_zscore": 0.0},
            "10:05": {"price": 20.3, "zscore": 0.0, "previous_zscore": 0.0},
        }
    )
    session_id = started["session_id"]
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 0))
    await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 1))
    signaled = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 2))
    assert signaled["pending"]["603629.SH"]["side"] == "SELL"

    regenerated = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 4))
    assert regenerated["fills"] == []
    assert regenerated["pending"]["603629.SH"]["signal_at"] == "2026-07-14T10:04:00"

    restored = await service.evaluate(session_id, now=datetime(2026, 7, 14, 10, 5))
    assert restored["fills"][0]["leg"] == "restore"


def test_paper_snapshot_builds_v2_fields_from_causal_minute_bars():
    index = pd.date_range("2026-07-14 09:55", periods=8, freq="min")
    frame = pd.DataFrame(
        {
            "symbol": ["603629.SH"] * len(index),
            "open": [20.0 + i * 0.01 for i in range(len(index))],
            "high": [20.02 + i * 0.01 for i in range(len(index))],
            "low": [19.98 + i * 0.01 for i in range(len(index))],
            "close": [20.0 + i * 0.01 for i in range(len(index))],
            "volume": [10_000] * len(index),
            "amount": [200_000] * len(index),
        },
        index=index,
    )
    service = IntradayTPaperService()

    snapshots = service._latest_snapshots(
        frame,
        service._strategy_params(
            {
                "warmup_bars": 5,
                "volatility_window": 5,
                "realized_vol_window": 5,
                "min_realized_vol_bps": 0,
            }
        ),
    )

    snapshot = snapshots["603629.SH"]
    assert snapshot.previous_price == pytest.approx(frame["close"].iloc[-2])
    assert snapshot.realized_vol_bps > 0
    assert snapshot.session_return_bps > 0
    assert snapshot.at_price_limit is False


def test_paper_snapshot_requires_a_bar_from_the_evaluation_minute():
    frame = pd.DataFrame(
        [
            {
                "symbol": "603629.SH",
                "open": 20.0,
                "high": 20.0,
                "low": 20.0,
                "close": 20.0,
                "volume": 10_000,
                "amount": 200_000,
                "vwap": 20.0,
                "zscore": -2.0,
                "previous_zscore": -2.3,
                "fast_ema": 20.0,
                "slow_ema": 20.0,
                "vwap_slope": 0.0,
                "volume_ratio": 1.0,
                "estimated_edge_bps": 100.0,
                "previous_price": 19.9,
                "realized_vol_bps": 30.0,
                "session_return_bps": 0.0,
                "ready": True,
            }
        ],
        index=[pd.Timestamp("2026-07-14 10:00")],
    )
    service = IntradayTPaperService()

    snapshots = service._latest_snapshots(
        frame,
        StrategyParams(),
        as_of=datetime(2026, 7, 14, 10, 1),
    )

    assert snapshots == {}


def test_stale_timestamped_qmt_quote_is_not_appended_as_a_current_bar():
    index = pd.date_range("2026-07-14 09:55", periods=6, freq="min")
    frame = pd.DataFrame(
        {
            "symbol": ["603629.SH"] * len(index),
            "open": [20.0] * len(index),
            "high": [20.0] * len(index),
            "low": [20.0] * len(index),
            "close": [20.0] * len(index),
            "volume": [1_000.0] * len(index),
            "amount": [2_000_000.0] * len(index),
        },
        index=index,
    )

    enriched = append_realtime_quote_bars(
        frame,
        {
            "603629.SH": {
                "lastPrice": 20.5,
                "quote_time": datetime(2026, 7, 14, 10, 0),
                "raw": {"time": int(datetime(2026, 7, 14, 10, 0).timestamp() * 1_000)},
            }
        },
        datetime(2026, 7, 14, 10, 1),
    )

    assert enriched.index.max() == pd.Timestamp("2026-07-14 10:00")
    assert pd.Timestamp("2026-07-14 10:01") not in enriched.index


def test_paper_snapshot_reads_exact_limits_from_the_raw_qmt_quote_shape():
    frame = pd.DataFrame(
        [
            {
                "symbol": "603629.SH",
                "open": 22.0,
                "high": 22.0,
                "low": 22.0,
                "close": 22.0,
                "volume": 10_000,
                "amount": 220_000,
                "vwap": 21.5,
                "zscore": 2.0,
                "previous_zscore": 2.3,
                "fast_ema": 21.9,
                "slow_ema": 21.8,
                "vwap_slope": 0.0,
                "volume_ratio": 1.0,
                "estimated_edge_bps": 100.0,
                "previous_price": 21.9,
                "realized_vol_bps": 30.0,
                "session_return_bps": 100.0,
                "ready": True,
            }
        ],
        index=[pd.Timestamp("2026-07-14 10:05")],
    )
    service = IntradayTPaperService()

    snapshot = service._latest_snapshots(
        frame,
        StrategyParams(),
        quote_map={
            "603629.SH": {
                "raw": {"upStopPrice": 22.0, "downStopPrice": 18.0},
            }
        },
    )["603629.SH"]

    assert snapshot.limit_price_available is True
    assert snapshot.at_price_limit is True

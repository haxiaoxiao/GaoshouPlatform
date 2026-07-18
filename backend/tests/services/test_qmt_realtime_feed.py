from __future__ import annotations

import asyncio
import json
from concurrent.futures import Future
from contextlib import suppress
from datetime import UTC, datetime, timedelta, timezone
from math import nan
from threading import Thread
from typing import Any, Callable

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.services.qmt_realtime_feed import QmtRealtimeFeed

NOW = datetime(2026, 7, 18, 10, 0, 0)
CORE_INDICES = ("000001.SH", "399001.SZ", "000985.SH")


class FakeClock:
    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeXtdataAdapter:
    def __init__(self) -> None:
        self.next_seq = 1
        self.subscribe_calls: list[tuple[str, ...]] = []
        self.unsubscribe_calls: list[int] = []
        self.full_tick_calls: list[tuple[str, ...]] = []
        self.callbacks: dict[tuple[str, ...], Callable[[dict[str, Any]], None]] = {}
        self.sequence_by_group: dict[tuple[str, ...], int] = {}
        self.failed_groups: set[tuple[str, ...]] = set()
        self.disconnected = False

    def subscribe_whole_quote(
        self,
        codes: list[str],
        callback: Callable[[dict[str, Any]], None],
    ) -> int:
        group = tuple(codes)
        self.subscribe_calls.append(group)
        if group in self.failed_groups:
            raise RuntimeError(f"subscription failed: {group}")
        seq = self.next_seq
        self.next_seq += 1
        self.callbacks[group] = callback
        self.sequence_by_group[group] = seq
        return seq

    def unsubscribe_quote(self, seq: int) -> None:
        self.unsubscribe_calls.append(seq)

    def get_full_tick(self, codes: list[str]) -> dict[str, dict[str, Any]]:
        self.full_tick_calls.append(tuple(codes))
        raise AssertionError("tests must complete the submitted polling future manually")

    def emit(
        self,
        group: tuple[str, ...],
        symbol: str,
        payload: dict[str, Any],
    ) -> None:
        self.callbacks[group]({symbol: payload})

    def disconnect(self) -> None:
        self.disconnected = True


class ManualPollSubmitter:
    def __init__(self) -> None:
        self.futures: list[Future[dict[str, dict[str, Any]]]] = []
        self.calls: list[tuple[str, ...]] = []

    def __call__(
        self,
        function: Callable[[list[str]], dict[str, dict[str, Any]]],
        codes: list[str],
    ) -> Future[dict[str, dict[str, Any]]]:
        del function
        future: Future[dict[str, dict[str, Any]]] = Future()
        self.futures.append(future)
        self.calls.append(tuple(codes))
        return future


class ManualBlockingCall:
    def __init__(self) -> None:
        self.functions: list[Callable[[], Any]] = []
        self.futures: list[asyncio.Future[Any]] = []

    async def __call__(self, function: Callable[[], Any]) -> Any:
        future = asyncio.get_running_loop().create_future()
        self.functions.append(function)
        self.futures.append(future)
        return await future


class SwitchingBlockingCall:
    def __init__(self) -> None:
        self.block = False
        self.functions: list[Callable[[], Any]] = []
        self.futures: list[asyncio.Future[Any]] = []

    async def __call__(self, function: Callable[[], Any]) -> Any:
        if not self.block:
            return function()
        future = asyncio.get_running_loop().create_future()
        self.functions.append(function)
        self.futures.append(future)
        return await future


class FirstControlTimeoutThenAwait:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, awaitable: Any, timeout: float) -> Any:
        del timeout
        self.calls += 1
        if self.calls == 1:
            await asyncio.sleep(0)
            raise TimeoutError
        return await awaitable


class FifthControlTimeoutThenAwait:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, awaitable: Any, timeout: float) -> Any:
        del timeout
        self.calls += 1
        if self.calls == 5:
            await asyncio.sleep(0)
            raise TimeoutError
        return await awaitable


async def control_timeout_immediately(awaitable: Any, timeout: float) -> Any:
    del awaitable, timeout
    await asyncio.sleep(0)
    raise TimeoutError


async def immediate_blocking_call(function: Callable[[], Any]) -> Any:
    return function()


async def await_manual_future(future: Future[Any], timeout: float) -> Any:
    del timeout
    return await asyncio.wrap_future(future)


async def timeout_immediately(future: Future[Any], timeout: float) -> Any:
    del future, timeout
    raise TimeoutError


async def shield_manual_future(future: Future[Any], timeout: float) -> Any:
    del timeout
    return await asyncio.shield(asyncio.wrap_future(future))


def epoch_ms(value: datetime) -> int:
    aware = (
        value.replace(tzinfo=timezone(timedelta(hours=8)))
        if value.tzinfo is None
        else value
    )
    return int(aware.timestamp() * 1000)


def raw_tick(
    clock: FakeClock,
    *,
    price: float = 10.2,
    previous_close: float = 10.0,
    status: int | None = 3,
    quote_time: datetime | None = None,
) -> dict[str, Any]:
    return {
        "time": epoch_ms(quote_time or clock()),
        "lastPrice": price,
        "lastClose": previous_close,
        "open": 10.1,
        "high": 10.3,
        "low": 9.9,
        "volume": 1234,
        "amount": 12340.0,
        "stockStatus": status,
        "speed1Min": 0.2,
        "speed5Min": -0.1,
    }


def make_feed(
    *,
    adapter: FakeXtdataAdapter | None = None,
    clock: FakeClock | None = None,
    submitter: ManualPollSubmitter | None = None,
    poll_waiter: Callable[[Future[Any], float], Any] = await_manual_future,
    universe: tuple[str, ...] = ("600000.SH", "000001.SZ", "430001.BJ"),
) -> tuple[QmtRealtimeFeed, FakeXtdataAdapter, FakeClock, ManualPollSubmitter]:
    fake = adapter or FakeXtdataAdapter()
    fake_clock = clock or FakeClock()
    manual_submitter = submitter or ManualPollSubmitter()
    feed = QmtRealtimeFeed(
        adapter=fake,
        clock=fake_clock,
        universe_loader=lambda: universe,
        blocking_call=immediate_blocking_call,
        poll_submitter=manual_submitter,
        poll_waiter=poll_waiter,
        market_session=lambda _now: True,
    )
    return feed, fake, fake_clock, manual_submitter


async def activate_push(
    adapter: FakeXtdataAdapter,
    clock: FakeClock,
    *,
    sh_price: float = 10.2,
) -> None:
    adapter.emit(("SH",), "600000.SH", raw_tick(clock, price=sh_price))
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)


async def wait_for_poll_submission(
    submitter: ManualPollSubmitter,
    count: int = 1,
) -> None:
    for _ in range(50):
        if len(submitter.futures) >= count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {count} poll submissions, got {len(submitter.futures)}")


@pytest.mark.asyncio
async def test_start_subscribes_four_groups_and_coalesces_whitelisted_push_ticks() -> None:
    feed, adapter, clock, _submitter = make_feed()

    await feed.start()

    assert adapter.subscribe_calls == [
        ("SH",),
        ("SZ",),
        ("BJ",),
        CORE_INDICES,
    ]
    assert feed.status.mode == "polling_30s"

    payload = raw_tick(clock)
    payload["askPrice"] = [10.3]
    callback_thread = Thread(
        target=adapter.emit,
        args=(("SH",), "600000.SH", payload),
    )
    callback_thread.start()
    callback_thread.join()
    payload["lastPrice"] = 99.0
    assert feed.latest_ticks() == {}
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)

    tick = feed.latest_ticks()["600000.SH"]
    assert tick.last_price == 10.2
    assert tick.stock_status == 0
    assert tick.quote_time == NOW
    assert tick.quote_time.tzinfo is None
    assert not hasattr(tick, "ask_price")
    assert feed.status.last_quote_at == NOW
    assert feed.status.mode == "push"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [3, 13])
async def test_active_qmt_statuses_are_normalized(status: int) -> None:
    feed, adapter, clock, _submitter = make_feed()
    await feed.start()

    await activate_push(adapter, clock)
    adapter.emit(("SH",), "600000.SH", raw_tick(clock, status=status))
    await asyncio.sleep(0)

    assert feed.latest_ticks()["600000.SH"].stock_status == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [1, 16, 17, 20])
async def test_suspended_qmt_statuses_are_normalized(status: int) -> None:
    feed, adapter, clock, _submitter = make_feed()
    await feed.start()

    await activate_push(adapter, clock)
    adapter.emit(("SH",), "600000.SH", raw_tick(clock, status=status))
    await asyncio.sleep(0)

    assert feed.latest_ticks()["600000.SH"].stock_status == 1


@pytest.mark.asyncio
async def test_invalid_status_prices_times_and_nonfinite_values_are_rejected() -> None:
    feed, adapter, clock, _submitter = make_feed()
    await feed.start()

    await activate_push(adapter, clock)
    baseline = dict(feed.latest_ticks())
    invalid_payloads = (
        raw_tick(clock, status=21),
        raw_tick(clock, price=0),
        raw_tick(clock, previous_close=0),
        raw_tick(clock, price=nan),
        {**raw_tick(clock), "time": nan},
        {**raw_tick(clock), "amount": nan},
    )
    for payload in invalid_payloads:
        adapter.emit(("SH",), "600000.SH", payload)
    await asyncio.sleep(0)

    assert feed.latest_ticks() == baseline


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [0, 10, None])
async def test_unknown_status_and_pvolume_are_supported(status: int | None) -> None:
    feed, adapter, clock, _submitter = make_feed()
    await feed.start()
    payload = {**raw_tick(clock, status=status), "volume": 12, "pvolume": 1200}
    adapter.emit(("SH",), "600000.SH", payload)
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)

    tick = feed.latest_ticks()["600000.SH"]
    assert tick.stock_status is None
    assert tick.volume == 1200


@pytest.mark.asyncio
async def test_aware_clock_uses_beijing_trade_date_and_rejects_any_future_tick() -> None:
    beijing = timezone(timedelta(hours=8))
    clock = FakeClock(datetime(2026, 7, 18, 2, 0, tzinfo=UTC))
    feed, adapter, _clock, _submitter = make_feed(clock=clock)
    await feed.start()

    current_quote = datetime(2026, 7, 18, 10, 0, tzinfo=beijing)
    adapter.emit(
        ("SH",),
        "600000.SH",
        {**raw_tick(clock), "time": epoch_ms(current_quote)},
    )
    adapter.emit(
        ("SZ",),
        "000001.SZ",
        {**raw_tick(clock), "time": epoch_ms(current_quote)},
    )
    await asyncio.sleep(0)
    assert feed.status.mode == "push"

    future_quote = current_quote + timedelta(milliseconds=1)
    adapter.emit(
        ("SH",),
        "600000.SH",
        {**raw_tick(clock, price=99.0), "time": epoch_ms(future_quote)},
    )
    await asyncio.sleep(0)

    assert feed.latest_ticks()["600000.SH"].last_price == 10.2


@pytest.mark.asyncio
async def test_previous_day_tick_cannot_recover_or_count_as_fresh() -> None:
    feed, adapter, clock, _submitter = make_feed()
    await feed.start()
    yesterday = clock() - timedelta(days=1)
    adapter.emit(("SH",), "600000.SH", raw_tick(clock, quote_time=yesterday))
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)

    assert "600000.SH" not in feed.latest_ticks()
    assert feed.status.mode == "polling_30s"
    assert feed.status.market_coverage["SH"] == 0.0


@pytest.mark.asyncio
async def test_latest_map_rejects_out_of_order_and_old_generation_callbacks() -> None:
    feed, adapter, clock, submitter = make_feed()
    await feed.start()
    old_callback = adapter.callbacks[("SH",)]

    await activate_push(adapter, clock, sh_price=10.5)
    older = clock() - timedelta(seconds=1)
    adapter.emit(("SH",), "600000.SH", raw_tick(clock, price=8.0, quote_time=older))
    await asyncio.sleep(0)
    assert feed.latest_ticks()["600000.SH"].last_price == 10.5

    clock.advance(60)
    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await health
    old_callback({"600000.SH": raw_tick(clock, price=99.0)})
    await asyncio.sleep(0)

    assert feed.status.connection_generation == 2
    assert feed.latest_ticks()["600000.SH"].last_price == 10.2


@pytest.mark.asyncio
async def test_push_stale_after_five_seconds_falls_back_and_poll_recovers() -> None:
    feed, adapter, clock, submitter = make_feed()
    await feed.start()
    await activate_push(adapter, clock)

    clock.advance(6)
    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)

    assert feed.status.mode == "polling_30s"
    assert submitter.calls[-1] == (
        "600000.SH",
        "000001.SZ",
        "430001.BJ",
        *CORE_INDICES,
    )
    submitter.futures[-1].set_result(
        {"600000.SH": raw_tick(clock, price=10.4)}
    )
    await health

    assert feed.status.mode == "polling_30s"
    assert feed.latest_ticks()["600000.SH"].last_price == 10.4
    assert feed.status.market_coverage["SH"] == 1.0


@pytest.mark.asyncio
async def test_continuing_sh_ticks_cannot_hide_stale_sz_push() -> None:
    feed, adapter, clock, submitter = make_feed()
    await feed.start()
    await activate_push(adapter, clock)
    clock.advance(4)
    adapter.emit(("SH",), "600000.SH", raw_tick(clock))
    await asyncio.sleep(0)
    clock.advance(1.001)

    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    assert feed.status.mode == "polling_30s"
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await health


@pytest.mark.asyncio
async def test_failed_poll_is_offline_and_push_recovers_immediately() -> None:
    feed, adapter, clock, submitter = make_feed()
    await feed.start()
    await activate_push(adapter, clock)
    clock.advance(6)

    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    submitter.futures[-1].set_exception(RuntimeError("miniQMT unavailable"))
    await health
    assert feed.status.mode == "offline"

    await activate_push(adapter, clock, sh_price=10.8)
    assert feed.status.mode == "push"
    assert feed.latest_ticks()["600000.SH"].last_price == 10.8


@pytest.mark.asyncio
async def test_resubscribe_after_sixty_seconds_requires_fresh_generation_tick() -> None:
    adapter = FakeXtdataAdapter()
    adapter.failed_groups.add(("SH",))
    feed, adapter, clock, submitter = make_feed(adapter=adapter)
    await feed.start()

    assert feed.status.mode == "polling_30s"
    first_poll = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await first_poll

    adapter.failed_groups.clear()
    clock.advance(60)
    second_poll = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter, 2)
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await second_poll

    assert feed.status.connection_generation == 2
    assert feed.status.mode == "polling_30s"
    await activate_push(adapter, clock, sh_price=11.0)
    assert feed.status.mode == "push"


@pytest.mark.asyncio
async def test_failed_old_unsubscribe_does_not_create_duplicate_subscription() -> None:
    class UnsubscribeFailAdapter(FakeXtdataAdapter):
        def unsubscribe_quote(self, seq: int) -> None:
            self.unsubscribe_calls.append(seq)
            if seq == 1:
                raise RuntimeError("unsubscribe failed")

    adapter = UnsubscribeFailAdapter()
    feed, adapter, clock, _submitter = make_feed(
        adapter=adapter,
        poll_waiter=timeout_immediately,
    )
    await feed.start()
    await activate_push(adapter, clock)
    clock.advance(60)

    await feed.run_health_cycle()

    assert adapter.subscribe_calls.count(("SH",)) == 1
    assert feed.market_details["SH"]["reason"] == (
        "push unavailable: previous subscription cleanup failed"
    )


@pytest.mark.asyncio
async def test_bj_subscription_failure_keeps_sh_sz_push_and_polls_only_bj() -> None:
    adapter = FakeXtdataAdapter()
    adapter.failed_groups.add(("BJ",))
    feed, adapter, clock, submitter = make_feed(adapter=adapter)

    await feed.start()

    assert feed.status.mode == "polling_30s"
    await activate_push(adapter, clock)
    assert feed.status.mode == "push"
    assert feed.status.market_coverage["BJ"] == 0.0
    assert "BJ" in (feed.status.reason or "")
    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    assert submitter.calls[-1] == ("430001.BJ",)
    submitter.futures[-1].set_result({"430001.BJ": raw_tick(clock)})
    await health
    assert feed.status.mode == "push"
    assert feed.status.market_coverage["BJ"] == 1.0
    assert feed.market_details["BJ"]["mode"] == "polling_30s"
    assert feed.market_details["SH"]["mode"] == "push"


@pytest.mark.asyncio
async def test_market_details_are_json_ready_and_report_bj_local_push() -> None:
    feed, adapter, clock, _submitter = make_feed()
    await feed.start()
    await activate_push(adapter, clock)
    adapter.emit(("BJ",), "430001.BJ", raw_tick(clock))
    await asyncio.sleep(0)

    details = feed.market_details

    assert details["BJ"] == {
        "mode": "push",
        "reason": None,
        "coverage": 1.0,
        "last_quote_at": NOW.isoformat(),
    }
    assert json.loads(json.dumps(details))["SH"]["mode"] == "push"


@pytest.mark.asyncio
async def test_timed_out_poll_is_retained_non_reentrant_and_late_result_is_discarded() -> None:
    feed, adapter, clock, submitter = make_feed(poll_waiter=timeout_immediately)
    await feed.start()
    await activate_push(adapter, clock)
    clock.advance(6)

    await feed.run_health_cycle()
    assert feed.status.mode == "offline"
    assert len(submitter.futures) == 1
    await feed.run_health_cycle()
    assert len(submitter.futures) == 1

    submitter.futures[0].set_result({"600000.SH": raw_tick(clock, price=99.0)})
    await feed.run_health_cycle()
    assert feed.latest_ticks()["600000.SH"].last_price != 99.0


@pytest.mark.asyncio
async def test_poll_result_is_discarded_when_push_recovers_while_poll_is_running() -> None:
    feed, adapter, clock, submitter = make_feed()
    await feed.start()
    await activate_push(adapter, clock)
    clock.advance(6)

    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    await activate_push(adapter, clock, sh_price=10.8)
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock, price=99.0)})
    await health

    assert feed.status.mode == "push"
    assert feed.latest_ticks()["600000.SH"].last_price == 10.8


@pytest.mark.asyncio
async def test_poll_batch_remains_fresh_for_forty_five_seconds() -> None:
    feed, adapter, clock, submitter = make_feed()
    adapter.failed_groups.update({("SH",), ("SZ",)})
    await feed.start()

    first = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await first
    assert feed.status.mode == "polling_30s"

    clock.advance(29)
    await feed.run_health_cycle()
    assert len(submitter.futures) == 1
    assert feed.status.mode == "polling_30s"
    assert feed.status.market_coverage["SH"] == 1.0

    clock.advance(16)
    assert feed.status.market_coverage["SH"] == 1.0
    clock.advance(0.001)
    assert feed.status.market_coverage["SH"] == 0.0


@pytest.mark.asyncio
async def test_stale_after_five_and_exact_thirty_second_poll_boundaries() -> None:
    feed, adapter, clock, submitter = make_feed()
    await feed.start()
    await activate_push(adapter, clock)

    clock.advance(5)
    await feed.run_health_cycle()
    assert feed.status.mode == "push"
    assert len(submitter.futures) == 0

    clock.advance(0.001)
    first = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    assert feed.status.mode == "polling_30s"
    assert len(submitter.futures) == 1
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await first

    clock.advance(29.999)
    await feed.run_health_cycle()
    assert len(submitter.futures) == 1
    clock.advance(0.001)
    second = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter, 2)
    assert len(submitter.futures) == 2
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await second


@pytest.mark.asyncio
async def test_market_coverage_reports_exact_eighty_percent() -> None:
    universe = (
        "600000.SH",
        "600001.SH",
        "600002.SH",
        "600003.SH",
        "600004.SH",
        "000001.SZ",
    )
    feed, adapter, clock, _submitter = make_feed(universe=universe)
    await feed.start()
    for symbol in universe[:4]:
        adapter.emit(("SH",), symbol, raw_tick(clock))
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)

    assert feed.status.mode == "push"
    assert feed.status.market_coverage["SH"] == pytest.approx(0.8)


@pytest.mark.asyncio
async def test_poll_coverage_cannot_promote_one_new_tick_to_push_coverage() -> None:
    universe = (
        "600000.SH",
        "600001.SH",
        "600002.SH",
        "600003.SH",
        "600004.SH",
        "000001.SZ",
    )
    feed, adapter, clock, submitter = make_feed(universe=universe)
    await feed.start()
    polling = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    submitter.futures[-1].set_result(
        {symbol: raw_tick(clock) for symbol in (*universe[:4], universe[-1])}
    )
    await polling

    adapter.emit(("SH",), universe[4], raw_tick(clock))
    adapter.emit(("SZ",), universe[-1], raw_tick(clock))
    await asyncio.sleep(0)

    assert feed.status.market_coverage["SH"] == 1.0
    assert feed.status.mode == "polling_30s"


@pytest.mark.asyncio
async def test_suspended_symbols_are_removed_from_coverage_denominator() -> None:
    universe = ("600000.SH", "600001.SH", "000001.SZ")
    feed, adapter, clock, _submitter = make_feed(universe=universe)
    await feed.start()
    adapter.emit(("SH",), "600000.SH", raw_tick(clock))
    adapter.emit(("SH",), "600001.SH", raw_tick(clock, status=16))
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)

    assert feed.status.market_coverage["SH"] == 1.0
    assert feed.status.mode == "push"


@pytest.mark.asyncio
async def test_stale_suspended_receipt_returns_to_missing_coverage_denominator() -> None:
    universe = ("600000.SH", "600001.SH", "000001.SZ")
    feed, adapter, clock, submitter = make_feed(universe=universe)
    await feed.start()
    adapter.emit(("SH",), "600000.SH", raw_tick(clock))
    adapter.emit(("SH",), "600001.SH", raw_tick(clock, status=16))
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)
    assert feed.status.market_coverage["SH"] == 1.0

    clock.advance(5.001)
    adapter.emit(("SH",), "600000.SH", raw_tick(clock))
    adapter.emit(("SZ",), "000001.SZ", raw_tick(clock))
    await asyncio.sleep(0)

    assert feed.status.market_coverage["SH"] == 0.5
    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    assert feed.status.mode == "polling_30s"
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await health


@pytest.mark.asyncio
async def test_core_index_failure_is_reported_and_polled_without_losing_market_push() -> None:
    adapter = FakeXtdataAdapter()
    adapter.failed_groups.add(CORE_INDICES)
    feed, adapter, clock, submitter = make_feed(adapter=adapter)
    await feed.start()
    await activate_push(adapter, clock)

    assert feed.status.mode == "push"
    assert "INDEX" in (feed.status.reason or "")
    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    assert submitter.calls[-1] == CORE_INDICES
    submitter.futures[-1].set_result({"000001.SH": raw_tick(clock)})
    await health
    assert feed.status.mode == "push"
    assert feed.status.market_coverage["INDEX"] == pytest.approx(1 / 3)


@pytest.mark.asyncio
async def test_closed_market_session_never_touches_sdk() -> None:
    adapter = FakeXtdataAdapter()
    clock = FakeClock()
    session_open = False
    feed = QmtRealtimeFeed(
        adapter=adapter,
        clock=clock,
        universe_loader=lambda: ("600000.SH", "000001.SZ"),
        blocking_call=immediate_blocking_call,
        poll_submitter=ManualPollSubmitter(),
        poll_waiter=await_manual_future,
        market_session=lambda _now: session_open,
    )

    await feed.start()
    await feed.run_health_cycle()

    assert feed.status.mode == "closed"
    assert adapter.subscribe_calls == []
    assert adapter.full_tick_calls == []


@pytest.mark.asyncio
async def test_hung_subscription_is_bounded_and_late_sequence_is_unsubscribed() -> None:
    adapter = FakeXtdataAdapter()
    blocker = ManualBlockingCall()
    control_waiter = FirstControlTimeoutThenAwait()
    feed = QmtRealtimeFeed(
        adapter=adapter,
        clock=FakeClock(),
        universe_loader=lambda: ("600000.SH", "000001.SZ"),
        blocking_call=blocker,
        poll_submitter=ManualPollSubmitter(),
        poll_waiter=await_manual_future,
        control_waiter=control_waiter,
        market_session=lambda _now: True,
    )

    await feed.start()
    assert feed.status.mode == "offline"
    assert len(blocker.futures) == 1

    blocker.futures[0].set_result(77)
    for _ in range(10):
        await asyncio.sleep(0)
        if len(blocker.futures) == 2:
            break
    assert len(blocker.futures) == 2
    blocker.functions[1]()
    blocker.futures[1].set_result(None)
    await asyncio.sleep(0)
    assert adapter.unsubscribe_calls == [77]


@pytest.mark.asyncio
async def test_pending_control_call_is_not_reentered_on_health_retry() -> None:
    adapter = FakeXtdataAdapter()
    blocker = ManualBlockingCall()
    clock = FakeClock()
    feed = QmtRealtimeFeed(
        adapter=adapter,
        clock=clock,
        universe_loader=lambda: ("600000.SH", "000001.SZ"),
        blocking_call=blocker,
        poll_submitter=ManualPollSubmitter(),
        poll_waiter=timeout_immediately,
        control_waiter=control_timeout_immediately,
        market_session=lambda _now: True,
    )

    await feed.start()
    assert len(blocker.futures) == 1
    clock.advance(60)
    await feed.run_health_cycle()

    assert len(blocker.futures) == 1
    blocker.futures[0].set_exception(RuntimeError("late failure"))
    await asyncio.sleep(0)
    await feed.stop()


@pytest.mark.asyncio
async def test_stop_during_subscribe_unsubscribes_late_sequence_without_registration() -> None:
    adapter = FakeXtdataAdapter()
    blocker = ManualBlockingCall()
    feed = QmtRealtimeFeed(
        adapter=adapter,
        clock=FakeClock(),
        universe_loader=lambda: ("600000.SH", "000001.SZ"),
        blocking_call=blocker,
        poll_submitter=ManualPollSubmitter(),
        poll_waiter=await_manual_future,
        market_session=lambda _now: True,
    )

    start_task = asyncio.create_task(feed.start())
    for _ in range(10):
        await asyncio.sleep(0)
        if blocker.futures:
            break
    assert len(blocker.futures) == 1

    await feed.stop()
    blocker.futures[0].set_result(77)
    for _ in range(20):
        await asyncio.sleep(0)
        if len(blocker.futures) >= 2:
            break
    assert len(blocker.futures) == 2
    blocker.functions[1]()
    blocker.futures[1].set_result(None)
    await start_task

    assert feed.status.mode == "closed"
    assert adapter.unsubscribe_calls == [77]
    assert feed.latest_ticks() == {}


@pytest.mark.asyncio
async def test_timed_out_stop_unsubscribe_drains_remaining_sequences_in_order() -> None:
    adapter = FakeXtdataAdapter()
    blocker = SwitchingBlockingCall()
    control_waiter = FifthControlTimeoutThenAwait()
    feed = QmtRealtimeFeed(
        adapter=adapter,
        clock=FakeClock(),
        universe_loader=lambda: ("600000.SH", "000001.SZ"),
        blocking_call=blocker,
        poll_submitter=ManualPollSubmitter(),
        poll_waiter=await_manual_future,
        control_waiter=control_waiter,
        market_session=lambda _now: True,
    )
    await feed.start()
    expected_ids = [adapter.sequence_by_group[group] for group in adapter.subscribe_calls]
    blocker.block = True

    await feed.stop()

    assert feed.status.mode == "closed"
    assert len(blocker.futures) == 1
    blocker.block = False
    blocker.functions[0]()
    blocker.futures[0].set_result(None)
    for _ in range(20):
        await asyncio.sleep(0)
        if adapter.unsubscribe_calls == expected_ids:
            break

    assert adapter.unsubscribe_calls == expected_ids


@pytest.mark.asyncio
async def test_bj_retry_keeps_critical_subscriptions_and_connection_generation() -> None:
    adapter = FakeXtdataAdapter()
    adapter.failed_groups.add(("BJ",))
    feed, adapter, clock, _submitter = make_feed(
        adapter=adapter,
        poll_waiter=timeout_immediately,
    )
    await feed.start()
    await activate_push(adapter, clock)
    original_generation = feed.status.connection_generation
    critical_sequences = {
        group: adapter.sequence_by_group[group] for group in (("SH",), ("SZ",))
    }

    adapter.failed_groups.clear()
    clock.advance(60)
    await activate_push(adapter, clock)
    await feed.run_health_cycle()

    assert feed.status.connection_generation == original_generation
    assert feed.status.mode == "push"
    assert {
        group: adapter.sequence_by_group[group] for group in (("SH",), ("SZ",))
    } == critical_sequences
    assert not set(critical_sequences.values()) & set(adapter.unsubscribe_calls)


@pytest.mark.asyncio
async def test_stop_returns_while_poll_sdk_future_is_still_running() -> None:
    feed, adapter, clock, submitter = make_feed(poll_waiter=shield_manual_future)
    await feed.start()
    await activate_push(adapter, clock)
    clock.advance(6)
    health = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)

    await feed.stop()

    assert feed.status.mode == "closed"
    assert not submitter.futures[-1].done()
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock, price=99)})
    with suppress(asyncio.CancelledError):
        await health
    assert feed.latest_ticks() == {}


@pytest.mark.asyncio
async def test_start_and_health_are_concurrently_idempotent() -> None:
    feed, adapter, clock, submitter = make_feed()
    await asyncio.gather(feed.start(), feed.start())
    assert len(adapter.subscribe_calls) == 4

    await activate_push(adapter, clock)
    clock.advance(6)
    health_one = asyncio.create_task(feed.run_health_cycle())
    health_two = asyncio.create_task(feed.run_health_cycle())
    await wait_for_poll_submission(submitter)
    assert len(submitter.futures) == 1
    submitter.futures[-1].set_result({"600000.SH": raw_tick(clock)})
    await asyncio.gather(health_one, health_two)
    assert len(submitter.futures) == 1


@pytest.mark.asyncio
async def test_missing_xtquant_does_not_block_start_or_escape() -> None:
    class MissingAdapter(FakeXtdataAdapter):
        def subscribe_whole_quote(self, codes, callback):  # type: ignore[no-untyped-def]
            del codes, callback
            raise ModuleNotFoundError("xtquant")

    feed, _adapter, _clock, _submitter = make_feed(adapter=MissingAdapter())

    await feed.start()

    assert feed.status.mode == "offline"
    assert "xtquant" in (feed.status.reason or "")


@pytest.mark.asyncio
async def test_subscription_errors_do_not_expose_sensitive_runtime_values() -> None:
    class SensitiveAdapter(FakeXtdataAdapter):
        def subscribe_whole_quote(self, codes, callback):  # type: ignore[no-untyped-def]
            del codes, callback
            raise RuntimeError(
                "account 622202123456 token=topsecret cookie: session123 "
                r"trader_path=C:\Users\Albert\QMT bearer abcdef"
            )

    feed, _adapter, _clock, _submitter = make_feed(adapter=SensitiveAdapter())

    await feed.start()

    reason = feed.status.reason or ""
    assert reason.startswith("SH push unavailable: RuntimeError")
    assert "622202123456" not in reason
    assert "topsecret" not in reason
    assert "session123" not in reason
    assert "Albert" not in reason
    assert "abcdef" not in reason
    assert len(reason) <= 240


@pytest.mark.asyncio
async def test_stop_disables_callbacks_then_unsubscribes_in_order_and_is_idempotent() -> None:
    feed, adapter, clock, _submitter = make_feed()
    await feed.start()
    callbacks = dict(adapter.callbacks)
    expected_ids = [adapter.sequence_by_group[group] for group in adapter.subscribe_calls]

    await asyncio.gather(feed.stop(), feed.stop())

    assert feed.status.mode == "closed"
    assert adapter.unsubscribe_calls == expected_ids
    assert adapter.disconnected is False
    for callback in callbacks.values():
        callback({"600000.SH": raw_tick(clock, price=99.0)})
    await asyncio.sleep(0)
    assert feed.latest_ticks() == {}
    await feed.run_health_cycle()
    assert adapter.unsubscribe_calls == expected_ids


def test_market_radar_realtime_settings_have_defaults_and_positive_bounds() -> None:
    fields = Settings.model_fields
    assert fields["market_radar_realtime_enabled"].default is True
    assert fields["market_radar_push_stale_seconds"].default == 5
    assert fields["market_radar_poll_interval_seconds"].default == 30
    assert fields["market_radar_resubscribe_seconds"].default == 60

    for field_name in (
        "market_radar_push_stale_seconds",
        "market_radar_poll_interval_seconds",
        "market_radar_resubscribe_seconds",
    ):
        with pytest.raises(ValidationError):
            Settings(**{field_name: 0})

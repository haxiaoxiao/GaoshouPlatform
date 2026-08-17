from __future__ import annotations

import asyncio
import threading

import pytest

from app.services.xueqiu_session import XueqiuSession


class FakeCrawler:
    def __init__(self) -> None:
        self.thread_ids: list[int] = []
        self.disconnected = False


def record(crawler: FakeCrawler) -> None:
    crawler.thread_ids.append(threading.get_ident())


@pytest.mark.asyncio
async def test_waits_without_collecting_then_resumes_on_same_thread():
    crawler = FakeCrawler()
    login_results = iter([False, False, True])
    events: list[dict] = []
    collected: list[str] = []

    def factory() -> FakeCrawler:
        record(crawler)
        return crawler

    def verify(value: FakeCrawler) -> dict:
        record(value)
        return {"server_verified": next(login_results)}

    def collect(value: FakeCrawler, symbol: str, **kwargs):
        record(value)
        collected.append(symbol)
        return [], {"raw_count": 0}

    def disconnect(value: FakeCrawler) -> None:
        record(value)
        value.disconnected = True

    session = XueqiuSession(
        crawler_factory=factory,
        login_verifier=verify,
        collector=collect,
        disconnector=disconnect,
        poll_interval=0,
        login_timeout=1,
        progress_callback=events.append,
    )

    await session.start()
    result = await session.wait_for_login()
    assert collected == []
    await session.collect("600519.SH", max_pages=1, min_reply=0)
    await session.disconnect()

    assert result.status == "authenticated"
    assert collected == ["600519.SH"]
    assert [event["stage"] for event in events] == [
        "xueqiu_spyder.waiting_for_login",
        "xueqiu_spyder.login_succeeded",
    ]
    assert len(set(crawler.thread_ids)) == 1
    assert crawler.disconnected is True


@pytest.mark.asyncio
async def test_login_timeout_does_not_collect_and_disconnects():
    crawler = FakeCrawler()
    events: list[dict] = []
    collected: list[str] = []

    def verify(value: FakeCrawler) -> dict:
        record(value)
        return {"server_verified": False}

    session = XueqiuSession(
        crawler_factory=lambda: crawler,
        login_verifier=verify,
        collector=lambda value, symbol, **kwargs: collected.append(symbol),
        disconnector=lambda value: setattr(value, "disconnected", True),
        poll_interval=0,
        login_timeout=0,
        progress_callback=events.append,
    )

    await session.start()
    result = await session.wait_for_login()
    await session.disconnect()

    assert result.status == "login_timeout"
    assert collected == []
    assert [event["stage"] for event in events] == ["xueqiu_spyder.waiting_for_login"]
    assert crawler.disconnected is True


@pytest.mark.asyncio
async def test_default_login_wait_pauses_until_manual_login():
    crawler = FakeCrawler()
    events: list[dict] = []

    session = XueqiuSession(
        crawler_factory=lambda: crawler,
        login_verifier=lambda value: {"server_verified": False},
        collector=lambda value, symbol, **kwargs: ([], {}),
        disconnector=lambda value: setattr(value, "disconnected", True),
        progress_callback=events.append,
    )

    await session.start()
    wait_task = asyncio.create_task(session.wait_for_login())
    while not events:
        await asyncio.sleep(0)

    assert not wait_task.done()
    assert events == [
        {
            **events[0],
            "stage": "xueqiu_spyder.waiting_for_login",
            "login_wait_timeout_seconds": None,
            "login_check_interval_seconds": 60.0,
        }
    ]

    wait_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait_task
    await session.disconnect()


@pytest.mark.asyncio
async def test_disconnect_is_idempotent():
    crawler = FakeCrawler()
    disconnect_calls = 0

    def disconnect(value: FakeCrawler) -> None:
        nonlocal disconnect_calls
        disconnect_calls += 1

    session = XueqiuSession(
        crawler_factory=lambda: crawler,
        login_verifier=lambda value: {"server_verified": True},
        collector=lambda value, symbol, **kwargs: ([], {}),
        disconnector=disconnect,
        poll_interval=0,
        login_timeout=1,
    )

    await session.start()
    await session.disconnect()
    await session.disconnect()

    assert disconnect_calls == 1

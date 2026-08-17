"""Bounded fan-out broker for aggregate market-radar events."""

from __future__ import annotations

import asyncio
import math
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.services.market_radar_contracts import StreamEvent, StreamEventType


class BrokerDisconnected(RuntimeError):
    """Raised when a stream client is closed or too slow to consume safely."""


@dataclass(slots=True)
class _SubscriberState:
    id: int
    prelude: deque[StreamEvent]
    queue: deque[StreamEvent]
    condition: asyncio.Condition
    overflow_count: int = 0
    disconnected: bool = False
    disconnect_reason: str | None = None


class MarketRadarSubscription:
    def __init__(self, broker: MarketRadarStreamBroker, state: _SubscriberState) -> None:
        self._broker = broker
        self._state = state

    @property
    def id(self) -> int:
        return self._state.id

    @property
    def pending(self) -> int:
        return len(self._state.prelude) + len(self._state.queue)

    @property
    def disconnect_reason(self) -> str | None:
        return self._state.disconnect_reason

    async def get(self) -> StreamEvent:
        state = self._state
        async with state.condition:
            await state.condition.wait_for(
                lambda: bool(state.prelude) or bool(state.queue) or state.disconnected
            )
            if state.disconnected:
                raise BrokerDisconnected(state.disconnect_reason or "subscription_closed")
            if state.prelude:
                return state.prelude.popleft()
            return state.queue.popleft()

    async def close(self) -> None:
        await self._broker.unsubscribe(self.id)

    def __aiter__(self) -> MarketRadarSubscription:
        return self

    async def __anext__(self) -> StreamEvent:
        try:
            return await self.get()
        except BrokerDisconnected as exc:
            raise StopAsyncIteration from exc


class MarketRadarStreamBroker:
    """Fan out aggregate events without allowing slow clients to block publishers."""

    def __init__(
        self,
        *,
        queue_size: int = 64,
        overflow_disconnect_threshold: int = 3,
        heartbeat_seconds: float = 15.0,
        clock: Callable[[], datetime] = datetime.now,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        if overflow_disconnect_threshold < 1:
            raise ValueError("overflow_disconnect_threshold must be positive")
        if not math.isfinite(heartbeat_seconds) or heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be finite and positive")
        self._queue_size = queue_size
        self._hard_limit = queue_size + overflow_disconnect_threshold - 1
        self._overflow_disconnect_threshold = overflow_disconnect_threshold
        self._heartbeat_seconds = heartbeat_seconds
        self._clock = clock
        self._subscribers: dict[int, _SubscriberState] = {}
        self._subscriber_lock = asyncio.Lock()
        self._next_subscriber_id = 1
        self._sequence = 0
        self._last_heartbeat_at: datetime | None = None

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def subscribe(
        self,
        *,
        initial_events: Iterable[tuple[StreamEventType, Mapping[str, Any]]] = (),
    ) -> MarketRadarSubscription:
        async with self._subscriber_lock:
            subscriber_id = self._next_subscriber_id
            self._next_subscriber_id += 1
            state = _SubscriberState(
                id=subscriber_id,
                prelude=deque(),
                queue=deque(maxlen=self._hard_limit),
                condition=asyncio.Condition(),
            )
            for event_type, data in initial_events:
                state.prelude.append(self._new_event(event_type, data, self._clock()))
            self._subscribers[subscriber_id] = state
        return MarketRadarSubscription(self, state)

    async def unsubscribe(self, subscriber_id: int) -> None:
        async with self._subscriber_lock:
            state = self._subscribers.pop(subscriber_id, None)
        if state is None:
            return
        async with state.condition:
            state.disconnected = True
            state.disconnect_reason = state.disconnect_reason or "subscription_closed"
            state.prelude.clear()
            state.queue.clear()
            state.condition.notify_all()

    async def publish(
        self,
        event: StreamEventType,
        data: Mapping[str, Any],
        *,
        created_at: datetime | None = None,
    ) -> StreamEvent:
        async with self._subscriber_lock:
            stream_event = self._new_event(event, data, created_at or self._clock())
            states = tuple(self._subscribers.values())
        for state in states:
            await self._offer(state, stream_event)
            if state.disconnected:
                async with self._subscriber_lock:
                    if self._subscribers.get(state.id) is state:
                        self._subscribers.pop(state.id, None)
        return stream_event

    def _new_event(
        self,
        event: StreamEventType,
        data: Mapping[str, Any],
        created_at: datetime,
    ) -> StreamEvent:
        self._sequence += 1
        return StreamEvent(
            sequence=self._sequence,
            event_id=str(self._sequence),
            event=event,
            data=data,
            created_at=created_at,
        )

    async def heartbeat(self, now: datetime | None = None) -> bool:
        current = now or self._clock()
        if (
            self._last_heartbeat_at is not None
            and (current - self._last_heartbeat_at).total_seconds() < self._heartbeat_seconds
        ):
            return False
        self._last_heartbeat_at = current
        await self.publish("heartbeat", {"at": current.isoformat()}, created_at=current)
        return True

    async def _offer(self, state: _SubscriberState, event: StreamEvent) -> None:
        async with state.condition:
            if state.disconnected:
                return
            if event.event == "snapshot":
                for index, pending in enumerate(state.queue):
                    if pending.event == "snapshot":
                        state.queue[index] = event
                        state.condition.notify()
                        return
            if len(state.queue) >= self._queue_size:
                dropped = self._drop_disposable(state.queue)
                if event.event in {"snapshot", "heartbeat"} and not dropped:
                    return
                if not dropped:
                    state.overflow_count += 1
                    if state.overflow_count >= self._overflow_disconnect_threshold:
                        state.disconnected = True
                        state.disconnect_reason = "slow_subscriber"
                        state.prelude.clear()
                        state.queue.clear()
                        state.condition.notify_all()
                        return
            if len(state.queue) >= self._hard_limit:
                state.disconnected = True
                state.disconnect_reason = "slow_subscriber"
                state.prelude.clear()
                state.queue.clear()
                state.condition.notify_all()
                return
            state.queue.append(event)
            state.condition.notify()

    @staticmethod
    def _drop_disposable(queue: deque[StreamEvent]) -> bool:
        for index, pending in enumerate(queue):
            if pending.event in {"snapshot", "heartbeat"}:
                del queue[index]
                return True
        return False

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from functools import partial
from math import isfinite
from threading import RLock, Thread
from typing import Any, Literal, Protocol, TypeVar, cast

from app.services.market_radar_calculator import QuoteTick

RealtimeMode = Literal["push", "polling_30s", "offline", "closed"]

CORE_INDICES = ("000001.SH", "399001.SZ", "000985.SH")
_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("SH", ("SH",)),
    ("SZ", ("SZ",)),
    ("BJ", ("BJ",)),
    ("INDEX", CORE_INDICES),
)
_CRITICAL_PUSH_GROUPS = frozenset({"SH", "SZ"})
_CALLBACK_FIELDS = (
    "time",
    "lastPrice",
    "lastClose",
    "open",
    "high",
    "low",
    "volume",
    "pvolume",
    "amount",
    "stockStatus",
    "speed1Min",
    "speed5Min",
)
_ACTIVE_STATUSES = frozenset({3, 13})
_UNKNOWN_STATUSES = frozenset({0, 10})
_SUSPENDED_STATUSES = frozenset({1, 16, 17, 20})
_INVALID_STATUSES = frozenset({21})
_BEIJING = timezone(timedelta(hours=8))
_POLL_TIMEOUT_SECONDS = 20.0
_POLL_FRESH_SECONDS = 45.0
_CONTROL_TIMEOUT_SECONDS = 5.0
_MIN_PUSH_COVERAGE = 0.8
_MIN_EPOCH_MS = 946_684_800_000
_A_SHARE_SYMBOL_RE = re.compile(
    r"^(?:(?:60|68)\d{4}\.SH|(?:00|30)\d{4}\.SZ|[489]\d{5}\.BJ)$"
)

T = TypeVar("T")
RawBatch = Mapping[str, Mapping[str, Any]]
BlockingCall = Callable[[Callable[[], T]], Awaitable[T]]
PollSubmitter = Callable[
    [Callable[[list[str]], dict[str, dict[str, Any]]], list[str]],
    Future[dict[str, dict[str, Any]]],
]
PollWaiter = Callable[[Future[Any], float], Awaitable[Any]]
ControlWaiter = Callable[[Awaitable[Any], float], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class RealtimeFeedStatus:
    mode: RealtimeMode
    changed_at: datetime
    last_quote_at: datetime | None
    connection_generation: int
    reason: str | None
    market_coverage: dict[str, float]


@dataclass(frozen=True, slots=True)
class _Receipt:
    received_at: datetime
    source: Literal["push", "poll"]
    connection_generation: int
    group_generation: int


class _ControlBusyError(TimeoutError):
    pass


class XtdataAdapter(Protocol):
    def subscribe_whole_quote(
        self,
        codes: list[str],
        callback: Callable[[dict[str, Any]], None],
    ) -> int: ...

    def unsubscribe_quote(self, seq: int) -> Any: ...

    def get_full_tick(self, codes: list[str]) -> dict[str, dict[str, Any]]: ...


class LazyXtdataAdapter:
    """Import xtquant only when the realtime feed is actually started."""

    def __init__(self) -> None:
        self._xtdata: Any | None = None

    def _module(self) -> Any:
        if self._xtdata is None:
            from xtquant import xtdata

            self._xtdata = xtdata
        return self._xtdata

    def subscribe_whole_quote(
        self,
        codes: list[str],
        callback: Callable[[dict[str, Any]], None],
    ) -> int:
        return cast(int, self._module().subscribe_whole_quote(codes, callback))

    def unsubscribe_quote(self, seq: int) -> Any:
        return self._module().unsubscribe_quote(seq)

    def get_full_tick(self, codes: list[str]) -> dict[str, dict[str, Any]]:
        return cast(dict[str, dict[str, Any]], self._module().get_full_tick(codes))


class QmtRealtimeFeed:
    def __init__(
        self,
        *,
        adapter: XtdataAdapter | None = None,
        clock: Callable[[], datetime] | None = None,
        universe_loader: Callable[[], Iterable[str] | Awaitable[Iterable[str]]],
        enabled: bool = True,
        push_stale_seconds: float = 5.0,
        poll_interval_seconds: float = 30.0,
        resubscribe_seconds: float = 60.0,
        market_session: Callable[[datetime], bool] | None = None,
        blocking_call: BlockingCall[Any] | None = None,
        poll_submitter: PollSubmitter | None = None,
        poll_waiter: PollWaiter | None = None,
        control_waiter: ControlWaiter | None = None,
    ) -> None:
        for name, value in (
            ("push_stale_seconds", push_stale_seconds),
            ("poll_interval_seconds", poll_interval_seconds),
            ("resubscribe_seconds", resubscribe_seconds),
        ):
            if not isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")

        self._adapter = adapter or LazyXtdataAdapter()
        self._clock = clock or datetime.now
        self._universe_loader = universe_loader
        self._enabled = enabled
        self._push_stale_seconds = float(push_stale_seconds)
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._resubscribe_seconds = float(resubscribe_seconds)
        self._market_session = market_session or _is_a_share_market_session

        self._blocking_call = blocking_call or self._run_blocking
        self._poll_submitter = poll_submitter or self._submit_poll
        self._poll_waiter = poll_waiter or _wait_for_poll
        self._control_waiter = control_waiter or _wait_for_control

        now = self._clock()
        self._mode: RealtimeMode = "closed"
        self._status_changed_at = now
        self._reason: str | None = None
        self._connection_generation = 0
        self._subscriptions: dict[str, int] = {}
        self._group_generations: dict[str, int] = {
            group: 0 for group, _codes in _GROUPS
        }
        self._subscription_errors: dict[str, str] = {}
        self._last_subscription_attempt_at: datetime | None = None
        self._last_universe_attempt_at: datetime | None = None
        self._universe_ready = False
        self._universe_error: str | None = None
        self._push_wait_started_at: datetime | None = None
        self._last_valid_push_by_group: dict[str, datetime] = {}
        self._last_poll_attempt_at: datetime | None = None
        self._poll_future: Future[dict[str, dict[str, Any]]] | None = None
        self._poll_attempt = 0

        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = False
        self._terminal_closed = False
        self._callbacks_enabled = False
        self._session_open = False
        self._session_recovery_pending = False
        self._allowed_symbols: frozenset[str] = frozenset(CORE_INDICES)
        self._symbols_by_group: dict[str, tuple[str, ...]] = {
            "SH": (),
            "SZ": (),
            "BJ": (),
            "INDEX": CORE_INDICES,
        }
        self._latest: dict[str, QuoteTick] = {}
        self._receipts: dict[str, _Receipt] = {}
        self._latest_lock = RLock()

        self._start_lock = asyncio.Lock()
        self._stop_lock = asyncio.Lock()
        self._health_guard = asyncio.Lock()
        self._control_lock = asyncio.Lock()
        self._control_task: asyncio.Future[Any] | None = None
        self._late_cleanup_tasks: set[asyncio.Task[None]] = set()
        self._deferred_unsubscribe_ids: list[int] = []
        self._health_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        async with self._start_lock:
            if self._started or self._terminal_closed:
                return
            self._started = True
            self._loop = asyncio.get_running_loop()
            if not self._enabled:
                self._set_mode("closed", "realtime feed disabled")
                return

            now = self._clock()
            self._session_open = self._market_session(now)
            self._session_recovery_pending = not self._session_open
            if not await self._load_universe_if_due(now, force=True):
                return

            if not self._session_open:
                self._set_mode("closed", "market session closed")
                return

            self._callbacks_enabled = True
            await self._replace_subscriptions(initial=True)

    async def stop(self) -> None:
        async with self._stop_lock:
            if self._terminal_closed:
                return
            self._terminal_closed = True
            self._callbacks_enabled = False
            self._connection_generation += 1

            health_task = self._health_task
            if health_task is not None and not health_task.done():
                health_task.cancel()
                try:
                    await health_task
                except asyncio.CancelledError:
                    pass

            subscription_ids = tuple(self._subscriptions.values())
            self._subscriptions.clear()
            self._subscription_errors.clear()
            for seq in subscription_ids:
                await self._unsubscribe_or_defer(seq)

            with self._latest_lock:
                self._latest.clear()
                self._receipts.clear()
            self._poll_future = None
            self._set_mode("closed", None)

    async def run_health_cycle(self) -> None:
        if self._terminal_closed or not self._started or not self._enabled:
            return
        async with self._health_guard:
            if self._health_task is None or self._health_task.done():
                self._health_task = asyncio.create_task(self._run_health_cycle())
            task = self._health_task
        await asyncio.shield(task)

    def latest_ticks(self) -> dict[str, QuoteTick]:
        with self._latest_lock:
            return dict(self._latest)

    @property
    def status(self) -> RealtimeFeedStatus:
        with self._latest_lock:
            latest_quote = max(
                (tick.quote_time for tick in self._latest.values()),
                default=None,
            )
            coverage = self._market_coverage(self._clock())
        return RealtimeFeedStatus(
            mode=self._mode,
            changed_at=self._status_changed_at,
            last_quote_at=latest_quote,
            connection_generation=self._connection_generation,
            reason=self._reason,
            market_coverage=coverage,
        )

    @property
    def market_details(self) -> dict[str, dict[str, object]]:
        now = self._clock()
        with self._latest_lock:
            coverage = self._market_coverage(now)
            details: dict[str, dict[str, object]] = {}
            for group in ("SH", "SZ", "BJ", "INDEX"):
                group_ticks = [
                    self._latest[symbol]
                    for symbol in self._symbols_by_group[group]
                    if symbol in self._latest
                ]
                last_quote = max((tick.quote_time for tick in group_ticks), default=None)
                error = self._subscription_errors.get(group)
                if group in self._subscriptions and self._group_push_fresh(group, now):
                    mode: RealtimeMode = "push"
                    reason = None
                elif coverage[group] > 0:
                    mode = "polling_30s"
                    reason = error or "push quotes stale"
                elif self._mode == "closed":
                    mode = "closed"
                    reason = self._reason
                else:
                    mode = "offline"
                    reason = error or "no fresh quotes"
                details[group] = {
                    "mode": mode,
                    "reason": reason,
                    "coverage": coverage[group],
                    "last_quote_at": last_quote.isoformat() if last_quote else None,
                }
            return details

    async def _run_health_cycle(self) -> None:
        if self._terminal_closed:
            return
        now = self._clock()
        if not self._market_session(now):
            self._session_open = False
            self._session_recovery_pending = True
            self._set_mode("closed", "market session closed")
            return
        self._session_open = True

        if not await self._load_universe_if_due(now):
            return

        if self._poll_call_is_still_running():
            if self._mode != "push":
                self._set_mode("offline", "QMT poll call still running")
            return

        subscription_attempted = False
        if self._session_recovery_pending:
            self._callbacks_enabled = True
            await self._replace_subscriptions(initial=False)
            self._session_recovery_pending = False
            subscription_attempted = True
            now = self._clock()
        elif not self._subscriptions and self._subscription_retry_due(now):
            self._callbacks_enabled = True
            await self._replace_subscriptions(initial=self._connection_generation == 0)
            subscription_attempted = True
            now = self._clock()

        if self._mode == "push" and not self._critical_push_ready(now):
            self._set_mode("polling_30s", "push quotes stale")

        local_retry_groups = self._local_retry_groups()
        needs_resubscribe = bool(self._subscription_errors) or bool(local_retry_groups) or self._mode in {
            "polling_30s",
            "offline",
        }
        if (
            not subscription_attempted
            and needs_resubscribe
            and self._subscription_retry_due(now)
        ):
            if self._mode == "push" and self._critical_push_ready(now):
                await self._retry_group_subscriptions(local_retry_groups)
            else:
                await self._replace_subscriptions(initial=False)
            now = self._clock()

        target_groups = self._poll_target_groups()
        if not target_groups:
            return
        await self._poll_if_due(target_groups, now)

    def _subscription_retry_due(self, now: datetime) -> bool:
        return self._last_subscription_attempt_at is None or (
            _elapsed(now, self._last_subscription_attempt_at)
            >= self._resubscribe_seconds
        )

    def _poll_call_is_still_running(self) -> bool:
        future = self._poll_future
        if future is None:
            return False
        if not future.done():
            return True
        try:
            future.result()
        except Exception:
            pass
        self._poll_future = None
        return False

    async def _replace_subscriptions(self, *, initial: bool) -> None:
        if self._terminal_closed:
            return
        self._connection_generation += 1
        generation = self._connection_generation
        self._last_valid_push_by_group.clear()
        old_subscriptions = dict(self._subscriptions)
        self._subscriptions.clear()
        self._subscription_errors.clear()
        self._last_subscription_attempt_at = self._clock()
        self._push_wait_started_at = self._clock()

        failed_unsubscribe_groups: set[str] = set()
        for group, seq in old_subscriptions.items():
            if not await self._unsubscribe_or_defer(seq):
                failed_unsubscribe_groups.add(group)
                self._subscriptions[group] = seq
                self._subscription_errors[group] = (
                    "push unavailable: previous subscription cleanup failed"
                )

        for index, (group, codes) in enumerate(_GROUPS):
            if self._terminal_closed or generation != self._connection_generation:
                break
            if group in failed_unsubscribe_groups:
                continue
            group_generation = self._next_group_generation(group)
            callback = self._make_callback(group, generation, group_generation)
            try:
                seq = await self._run_control(
                    partial(self._adapter.subscribe_whole_quote, list(codes), callback),
                    late_subscription=True,
                )
                if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
                    raise RuntimeError(f"invalid subscription id: {seq!r}")
                if (
                    self._terminal_closed
                    or generation != self._connection_generation
                    or group_generation != self._group_generations[group]
                ):
                    await self._drain_deferred_unsubscribes()
                    await self._unsubscribe_or_defer(seq)
                    break
                self._subscriptions[group] = seq
            except Exception as exc:
                self._subscription_errors[group] = _safe_error("push unavailable", exc)
                if isinstance(exc, TimeoutError):
                    for remaining_group, _remaining_codes in _GROUPS[index + 1 :]:
                        self._subscription_errors[remaining_group] = (
                            "push unavailable: control executor busy"
                        )
                    break

        if self._terminal_closed or generation != self._connection_generation:
            return
        if not self._subscriptions:
            self._set_mode("offline", self._subscription_reason())
        else:
            reason = self._subscription_reason() or "awaiting fresh SH/SZ push quotes"
            self._set_mode("polling_30s", reason)
        if not initial and not self._subscription_errors:
            self._reason = "awaiting fresh SH/SZ push quotes"

    async def _retry_group_subscriptions(self, groups: frozenset[str]) -> None:
        if not groups or self._terminal_closed:
            return
        generation = self._connection_generation
        self._last_subscription_attempt_at = self._clock()
        codes_by_group = dict(_GROUPS)
        for group in ("SH", "SZ", "BJ", "INDEX"):
            if group not in groups or self._terminal_closed:
                continue
            old_seq = self._subscriptions.pop(group, None)
            if old_seq is not None and not await self._unsubscribe_or_defer(old_seq):
                self._subscriptions[group] = old_seq
                self._subscription_errors[group] = "push unavailable: unsubscribe failed"
                continue

            group_generation = self._next_group_generation(group)
            self._last_valid_push_by_group.pop(group, None)
            callback = self._make_callback(group, generation, group_generation)
            try:
                seq = await self._run_control(
                    partial(
                        self._adapter.subscribe_whole_quote,
                        list(codes_by_group[group]),
                        callback,
                    ),
                    late_subscription=True,
                )
                if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
                    raise RuntimeError(f"invalid subscription id: {seq!r}")
                if (
                    self._terminal_closed
                    or generation != self._connection_generation
                    or group_generation != self._group_generations[group]
                ):
                    await self._drain_deferred_unsubscribes()
                    await self._unsubscribe_or_defer(seq)
                    return
                self._subscriptions[group] = seq
                self._subscription_errors.pop(group, None)
            except Exception as exc:
                self._subscription_errors[group] = _safe_error("push unavailable", exc)

        if self._terminal_closed:
            return
        self._set_mode("push", self._subscription_reason())

    def _make_callback(
        self,
        group: str,
        generation: int,
        group_generation: int,
    ) -> Callable[[dict[str, Any]], None]:
        loop = self._loop

        def callback(raw_batch: dict[str, Any]) -> None:
            if not self._callbacks_enabled or loop is None or not isinstance(raw_batch, Mapping):
                return
            copied: dict[str, dict[str, Any]] = {}
            for raw_symbol, raw_tick in raw_batch.items():
                if not isinstance(raw_symbol, str) or not isinstance(raw_tick, Mapping):
                    continue
                copied[raw_symbol] = {
                    field: raw_tick[field] for field in _CALLBACK_FIELDS if field in raw_tick
                }
            if copied:
                loop.call_soon_threadsafe(
                    self._accept_raw_batch,
                    generation,
                    group,
                    group_generation,
                    copied,
                    "push",
                )

        return callback

    def _accept_raw_batch(
        self,
        generation: int,
        group: str | None,
        group_generation: int | None,
        raw_batch: RawBatch,
        source: Literal["push", "poll"],
    ) -> int:
        if (
            self._terminal_closed
            or generation != self._connection_generation
            or (
                group is not None
                and group_generation != self._group_generations.get(group)
            )
            or (source == "push" and not self._callbacks_enabled)
        ):
            return 0
        now = self._clock()
        if source == "push" and not self._market_session(now):
            return 0

        accepted = 0
        fresh_tradeable_push = False
        with self._latest_lock:
            for raw_symbol, raw_tick in raw_batch.items():
                symbol = raw_symbol.strip().upper()
                if symbol not in self._allowed_symbols:
                    continue
                expected_group = self._symbol_group(symbol)
                if group is not None and expected_group != group:
                    continue
                tick = _normalize_tick(symbol, raw_tick, now)
                if tick is None:
                    continue
                existing = self._latest.get(symbol)
                if existing is not None and tick.quote_time < existing.quote_time:
                    continue
                self._latest[symbol] = tick
                symbol_group = self._symbol_group(symbol)
                self._receipts[symbol] = _Receipt(
                    received_at=now,
                    source=source,
                    connection_generation=generation,
                    group_generation=self._group_generations[symbol_group],
                )
                accepted += 1
                age = _elapsed(now, tick.quote_time)
                if (
                    source == "push"
                    and tick.stock_status != 1
                    and 0 <= age <= self._push_stale_seconds
                ):
                    fresh_tradeable_push = True

        if source == "push" and group is not None and fresh_tradeable_push:
            self._last_valid_push_by_group[group] = now
            if self._critical_push_ready(now):
                self._set_mode("push", self._subscription_reason())
        return accepted

    async def _poll_if_due(self, target_groups: frozenset[str], now: datetime) -> None:
        if self._poll_call_is_still_running():
            if self._mode != "push":
                self._set_mode("offline", "QMT poll call still running")
            return

        if (
            self._last_poll_attempt_at is not None
            and _elapsed(now, self._last_poll_attempt_at) < self._poll_interval_seconds
        ):
            return

        codes = self._codes_for_groups(target_groups)
        if not codes:
            return
        self._last_poll_attempt_at = now
        self._poll_attempt += 1
        token = (self._connection_generation, self._poll_attempt, target_groups)
        future = self._poll_submitter(self._adapter.get_full_tick, list(codes))
        self._poll_future = future
        try:
            raw_batch = await self._poll_waiter(future, _POLL_TIMEOUT_SECONDS)
        except TimeoutError:
            if self._mode != "push":
                self._set_mode("offline", "QMT poll timed out after 20 seconds")
            else:
                self._set_mode("push", self._degraded_poll_reason(target_groups, "poll timed out"))
            return
        except Exception as exc:
            self._poll_future = None
            if self._mode != "push":
                self._set_mode("offline", _safe_error("QMT poll failed", exc))
            else:
                self._set_mode(
                    "push",
                    self._degraded_poll_reason(target_groups, _safe_error("poll failed", exc)),
                )
            return

        self._poll_future = None
        if not self._poll_result_is_current(token):
            return
        if not isinstance(raw_batch, Mapping):
            if self._mode != "push":
                self._set_mode("offline", "QMT poll returned an invalid payload")
            return
        accepted = self._accept_raw_batch(
            token[0],
            None,
            None,
            cast(RawBatch, raw_batch),
            "poll",
        )
        coverage = self._market_coverage(self._clock())
        has_fresh_quote = any(coverage[group] > 0 for group in target_groups)
        if accepted and has_fresh_quote:
            if self._mode != "push":
                self._set_mode("polling_30s", self._subscription_reason())
        elif self._mode != "push":
            self._set_mode("offline", "QMT poll returned no valid quotes")

    def _poll_result_is_current(
        self,
        token: tuple[int, int, frozenset[str]],
    ) -> bool:
        if self._terminal_closed or token[0] != self._connection_generation:
            return False
        target_groups = token[2]
        if self._mode == "push" and target_groups & _CRITICAL_PUSH_GROUPS:
            return False
        if self._mode == "push" and not (target_groups & self._subscription_errors.keys()):
            return False
        return True

    def _poll_target_groups(self) -> frozenset[str]:
        if self._mode == "closed":
            return frozenset()
        if self._mode == "push":
            return frozenset((*self._subscription_errors, *self._local_retry_groups()))
        return frozenset(self._symbols_by_group)

    def _codes_for_groups(self, groups: Iterable[str]) -> tuple[str, ...]:
        values: list[str] = []
        for group in ("SH", "SZ", "BJ", "INDEX"):
            if group in groups:
                values.extend(self._symbols_by_group[group])
        return tuple(dict.fromkeys(values))

    async def _load_universe_if_due(
        self,
        now: datetime,
        *,
        force: bool = False,
    ) -> bool:
        if self._universe_ready:
            return True
        if (
            not force
            and self._last_universe_attempt_at is not None
            and _elapsed(now, self._last_universe_attempt_at)
            < self._resubscribe_seconds
        ):
            self._set_mode("offline", self._universe_error or "universe unavailable")
            return False

        self._last_universe_attempt_at = now
        try:
            loaded = self._universe_loader()
            symbols = await loaded if inspect.isawaitable(loaded) else loaded
            self._set_universe(symbols)
        except Exception as exc:
            self._universe_ready = False
            self._universe_error = _safe_error("universe unavailable", exc)
            self._set_mode("offline", self._universe_error)
            return False

        self._universe_ready = True
        self._universe_error = None
        return True

    def _set_universe(self, symbols: Iterable[str]) -> None:
        normalized = tuple(
            dict.fromkeys(
                symbol.strip().upper()
                for symbol in symbols
                if isinstance(symbol, str)
                and _A_SHARE_SYMBOL_RE.fullmatch(symbol.strip().upper()) is not None
                and symbol.strip().upper() not in CORE_INDICES
            )
        )
        self._allowed_symbols = frozenset((*normalized, *CORE_INDICES))
        self._symbols_by_group = {
            market: tuple(symbol for symbol in normalized if symbol.endswith(f".{market}"))
            for market in ("SH", "SZ", "BJ")
        }
        self._symbols_by_group["INDEX"] = CORE_INDICES
        with self._latest_lock:
            self._latest = {
                symbol: tick
                for symbol, tick in self._latest.items()
                if symbol in self._allowed_symbols
            }
            self._receipts = {
                symbol: receipt
                for symbol, receipt in self._receipts.items()
                if symbol in self._allowed_symbols
            }

    def _symbol_group(self, symbol: str) -> str:
        if symbol in CORE_INDICES:
            return "INDEX"
        return symbol.rsplit(".", 1)[-1]

    def _next_group_generation(self, group: str) -> int:
        value = self._group_generations[group] + 1
        self._group_generations[group] = value
        return value

    def _local_retry_groups(self) -> frozenset[str]:
        return frozenset(self._subscription_errors).difference(_CRITICAL_PUSH_GROUPS)

    def _subscription_reason(self) -> str | None:
        if not self._subscription_errors:
            return None
        reason = "; ".join(
            f"{group} {self._subscription_errors[group]}"
            for group in ("SH", "SZ", "BJ", "INDEX")
            if group in self._subscription_errors
        )
        return reason[:240]

    def _degraded_poll_reason(self, groups: Iterable[str], detail: str) -> str:
        return f"{','.join(sorted(groups))} {detail}"

    def _market_coverage(self, now: datetime) -> dict[str, float]:
        coverage: dict[str, float] = {}
        for group in ("SH", "SZ", "BJ", "INDEX"):
            expected = self._symbols_by_group[group]
            if not expected:
                coverage[group] = 0.0
                continue
            valid = 0
            suspended = 0
            for symbol in expected:
                tick = self._latest.get(symbol)
                receipt = self._receipts.get(symbol)
                if tick is None or receipt is None:
                    continue
                if not self._receipt_is_fresh(symbol, receipt, now):
                    continue
                if tick.stock_status == 1:
                    suspended += 1
                else:
                    valid += 1
            eligible = len(expected) - suspended
            coverage[group] = valid / eligible if eligible else 0.0
        return coverage

    def _group_push_fresh(self, group: str, now: datetime) -> bool:
        last_valid = self._last_valid_push_by_group.get(group)
        if last_valid is None or _elapsed(now, last_valid) > self._push_stale_seconds:
            return False
        return self._push_coverage(group, now) >= _MIN_PUSH_COVERAGE

    def _push_coverage(self, group: str, now: datetime) -> float:
        expected = self._symbols_by_group[group]
        valid = 0
        suspended = 0
        for symbol in expected:
            tick = self._latest.get(symbol)
            receipt = self._receipts.get(symbol)
            if (
                tick is None
                or receipt is None
                or receipt.source != "push"
                or not self._receipt_is_fresh(symbol, receipt, now)
            ):
                continue
            if tick.stock_status == 1:
                suspended += 1
            else:
                valid += 1
        eligible = len(expected) - suspended
        return valid / eligible if eligible else 0.0

    def _receipt_is_fresh(
        self,
        symbol: str,
        receipt: _Receipt,
        now: datetime,
    ) -> bool:
        group = self._symbol_group(symbol)
        if (
            receipt.connection_generation != self._connection_generation
            or receipt.group_generation != self._group_generations[group]
        ):
            return False
        max_age = (
            self._push_stale_seconds
            if receipt.source == "push"
            else _POLL_FRESH_SECONDS
        )
        tick = self._latest.get(symbol)
        if tick is None:
            return False
        receipt_age = _elapsed(now, receipt.received_at)
        quote_age = _elapsed(now, tick.quote_time)
        return 0 <= receipt_age <= max_age and 0 <= quote_age <= max_age

    def _critical_push_ready(self, now: datetime) -> bool:
        return _CRITICAL_PUSH_GROUPS.issubset(self._subscriptions) and all(
            self._group_push_fresh(group, now) for group in _CRITICAL_PUSH_GROUPS
        )

    def _set_mode(self, mode: RealtimeMode, reason: str | None) -> None:
        if self._mode != mode or self._reason != reason:
            self._status_changed_at = self._clock()
        self._mode = mode
        self._reason = reason

    async def _run_blocking(self, function: Callable[[], T]) -> T:
        future = _submit_daemon_call(
            function,
            thread_name="market-radar-qmt-control",
        )
        return cast(T, await asyncio.wrap_future(future))

    async def _run_control(
        self,
        function: Callable[[], T],
        *,
        late_subscription: bool = False,
        timeout_unsubscribe_seq: int | None = None,
    ) -> T:
        async with self._control_lock:
            pending = self._control_task
            if pending is not None:
                if not pending.done():
                    raise _ControlBusyError("QMT control call still running")
                self._control_task = None

            task = asyncio.ensure_future(self._blocking_call(function))
            self._control_task = task
            try:
                return cast(T, await self._control_waiter(task, _CONTROL_TIMEOUT_SECONDS))
            except TimeoutError:
                if late_subscription:
                    task.add_done_callback(self._cleanup_late_subscription)
                elif timeout_unsubscribe_seq is not None:
                    task.add_done_callback(
                        partial(
                            self._cleanup_timed_out_unsubscribe,
                            timeout_unsubscribe_seq,
                        )
                    )
                else:
                    task.add_done_callback(self._cleanup_timed_out_control)
                raise TimeoutError("QMT control call timed out") from None
            except asyncio.CancelledError:
                if late_subscription:
                    task.add_done_callback(self._cleanup_late_subscription)
                elif timeout_unsubscribe_seq is not None:
                    task.add_done_callback(
                        partial(
                            self._cleanup_timed_out_unsubscribe,
                            timeout_unsubscribe_seq,
                        )
                    )
                else:
                    task.add_done_callback(self._cleanup_timed_out_control)
                raise
            finally:
                if task.done() and self._control_task is task:
                    self._control_task = None

    def _cleanup_late_subscription(self, task: asyncio.Future[Any]) -> None:
        if self._control_task is task:
            self._control_task = None
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        cleanup = loop.create_task(self._finish_late_subscription(task))
        self._track_cleanup_task(cleanup)

    def _cleanup_timed_out_control(self, task: asyncio.Future[Any]) -> None:
        if self._control_task is task:
            self._control_task = None
        if not task.cancelled():
            try:
                task.result()
            except Exception:
                pass
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        cleanup = loop.create_task(self._drain_deferred_unsubscribes())
        self._track_cleanup_task(cleanup)

    def _cleanup_timed_out_unsubscribe(
        self,
        seq: int,
        task: asyncio.Future[Any],
    ) -> None:
        if self._control_task is task:
            self._control_task = None
        succeeded = False
        if not task.cancelled():
            try:
                task.result()
                succeeded = True
            except Exception:
                pass
        if succeeded:
            self._remove_deferred_unsubscribe(seq)
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        cleanup = loop.create_task(self._drain_deferred_unsubscribes())
        self._track_cleanup_task(cleanup)

    def _track_cleanup_task(self, cleanup: asyncio.Task[None]) -> None:
        self._late_cleanup_tasks.add(cleanup)
        cleanup.add_done_callback(self._late_cleanup_done)

    async def _finish_late_subscription(self, task: asyncio.Future[Any]) -> None:
        seq: Any = None
        if not task.cancelled():
            try:
                seq = task.result()
            except Exception:
                pass
        await self._drain_deferred_unsubscribes()
        if isinstance(seq, int) and not isinstance(seq, bool) and seq > 0:
            await self._unsubscribe_or_defer(seq)

    def _late_cleanup_done(self, task: asyncio.Task[None]) -> None:
        self._late_cleanup_tasks.discard(task)
        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            pass

    async def _unsubscribe_or_defer(self, seq: int, *, front: bool = False) -> bool:
        self._defer_unsubscribe(seq, front=front)
        try:
            await self._run_control(
                partial(self._adapter.unsubscribe_quote, seq),
                timeout_unsubscribe_seq=seq,
            )
        except Exception:
            return False
        self._remove_deferred_unsubscribe(seq)
        return True

    async def _drain_deferred_unsubscribes(self) -> None:
        while self._deferred_unsubscribe_ids:
            seq = self._deferred_unsubscribe_ids.pop(0)
            if not await self._unsubscribe_or_defer(seq, front=True):
                return

    def _defer_unsubscribe(self, seq: int, *, front: bool = False) -> None:
        if seq in self._deferred_unsubscribe_ids:
            return
        if front:
            self._deferred_unsubscribe_ids.insert(0, seq)
        else:
            self._deferred_unsubscribe_ids.append(seq)

    def _remove_deferred_unsubscribe(self, seq: int) -> None:
        self._deferred_unsubscribe_ids = [
            value for value in self._deferred_unsubscribe_ids if value != seq
        ]

    def _submit_poll(
        self,
        function: Callable[[list[str]], dict[str, dict[str, Any]]],
        codes: list[str],
    ) -> Future[dict[str, dict[str, Any]]]:
        return _submit_daemon_call(
            partial(function, codes),
            thread_name="market-radar-qmt-poll",
        )


def _submit_daemon_call[T](
    function: Callable[[], T],
    *,
    thread_name: str,
) -> Future[T]:
    future: Future[T] = Future()

    def run() -> None:
        if not future.set_running_or_notify_cancel():
            return
        try:
            result = function()
        except BaseException as exc:
            future.set_exception(exc)
        else:
            future.set_result(result)

    Thread(target=run, name=thread_name, daemon=True).start()
    return future


async def _wait_for_poll(future: Future[Any], timeout: float) -> Any:
    wrapped = asyncio.wrap_future(future)
    return await asyncio.wait_for(asyncio.shield(wrapped), timeout=timeout)


async def _wait_for_control(awaitable: Awaitable[Any], timeout: float) -> Any:
    return await asyncio.wait_for(asyncio.shield(awaitable), timeout=timeout)


def _normalize_tick(symbol: str, raw: Mapping[str, Any], now: datetime) -> QuoteTick | None:
    try:
        current = _beijing_naive(now)
        raw_time = _finite_number(raw.get("time"))
        if raw_time < _MIN_EPOCH_MS:
            return None
        quote_time = datetime.fromtimestamp(raw_time / 1000.0, tz=_BEIJING).replace(tzinfo=None)
        if quote_time.date() != current.date() or quote_time > current:
            return None

        last_price = _positive_number(raw.get("lastPrice"))
        previous_close = _positive_number(raw.get("lastClose"))
        status = _normalize_stock_status(raw.get("stockStatus"))
        return QuoteTick(
            symbol=symbol,
            quote_time=quote_time,
            last_price=last_price,
            previous_close=previous_close,
            open_price=_optional_number(raw, "open", minimum=0.0, zero_is_none=True),
            high_price=_optional_number(raw, "high", minimum=0.0, zero_is_none=True),
            low_price=_optional_number(raw, "low", minimum=0.0, zero_is_none=True),
            volume=_optional_volume(raw),
            amount=_optional_number(raw, "amount", minimum=0.0),
            stock_status=status,
            speed_1m=_optional_number(raw, "speed1Min"),
            speed_5m=_optional_number(raw, "speed5Min"),
        )
    except (OverflowError, OSError, TypeError, ValueError):
        return None


def _normalize_stock_status(raw: Any) -> int | None:
    if raw is None:
        return None
    value = _finite_number(raw)
    integer = int(value)
    if value != integer:
        return None
    if integer in _ACTIVE_STATUSES:
        return 0
    if integer in _SUSPENDED_STATUSES:
        return 1
    if integer in _UNKNOWN_STATUSES:
        return None
    if integer in _INVALID_STATUSES:
        raise ValueError("quote status is invalid")
    raise ValueError("quote status is unsupported")


def _positive_number(raw: Any) -> float:
    value = _finite_number(raw)
    if value <= 0:
        raise ValueError("price must be positive")
    return value


def _optional_number(
    raw: Mapping[str, Any],
    field: str,
    *,
    minimum: float | None = None,
    zero_is_none: bool = False,
) -> float | None:
    if field not in raw or raw[field] is None:
        return None
    value = _finite_number(raw[field])
    if minimum is not None and value < minimum:
        raise ValueError(f"{field} is below its minimum")
    if zero_is_none and value == 0:
        return None
    return value


def _optional_volume(raw: Mapping[str, Any]) -> float | None:
    field = "pvolume" if raw.get("pvolume") is not None else "volume"
    return _optional_number(raw, field, minimum=0.0)


def _finite_number(raw: Any) -> float:
    if isinstance(raw, bool):
        raise TypeError("booleans are not numeric quote values")
    value = float(raw)
    if not isfinite(value):
        raise ValueError("quote value must be finite")
    return value


def _elapsed(now: datetime, previous: datetime) -> float:
    return (_beijing_naive(now) - _beijing_naive(previous)).total_seconds()


def _beijing_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(_BEIJING).replace(tzinfo=None)


def _safe_error(prefix: str, exc: Exception) -> str:
    detail = str(exc).strip() or type(exc).__name__
    detail = re.sub(
        r"(?i)\b(account(?:_?id)?|api[_-]?key|token|secret|cookie|password|pwd|bearer)"
        r"\b\s*(?:[:=]\s*|\s+)[^\s,;]+",
        lambda match: f"{match.group(1)}=<redacted>",
        detail,
    )
    detail = re.sub(
        r"(?:账号|账户)\s*(?:[:=]\s*|\s+)[^\s,;]+",
        "账号=<redacted>",
        detail,
    )
    detail = re.sub(r"[A-Za-z]:[\\/][^\s,;]+", "<path>", detail)
    detail = re.sub(r"/(?:[^\s/]+/)+[^\s,;]+", "<path>", detail)
    detail = detail[:160]
    return f"{prefix}: {type(exc).__name__}: {detail}"


def _is_a_share_market_session(now: datetime) -> bool:
    now = _beijing_naive(now)
    if now.weekday() >= 5:
        return False
    current = now.time()
    return time(9, 30) <= current <= time(11, 30) or time(13, 0) <= current <= time(15, 0)

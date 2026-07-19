"""Sync-service-owned scheduling and execution for market-radar EOD snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from loguru import logger

from app.services.task_queue import (
    MARKET_RADAR_QUEUE_NAME,
    QueuedTask,
    get_task_queue,
)

MARKET_RADAR_EOD_JOB_ID = "market-radar-eod"
_HONG_KONG = ZoneInfo("Asia/Hong_Kong")
_RELATED_SYNC_TYPES = frozenset(
    {
        "datasync",
        "index_daily",
        "kline_daily",
        "sentiment",
        "sentiment_nga",
        "sentiment_xueqiu",
        "stock_full",
        "stock_info",
        "tushare_relay",
    }
)


@dataclass(frozen=True)
class RadarEodResult:
    status: Literal["fresh", "partial", "stale", "unavailable"]
    deleted_intraday: int


@dataclass
class RadarRuntimeTask:
    task_id: str
    status: Literal["queued", "running", "completed", "failed", "cancelled"]
    target_date: date
    reason: str
    source_run_id: str | None = None
    snapshot_status: str | None = None
    recompute_needed: bool = False
    deleted_intraday: int = 0
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "kind": "market_radar_refresh",
            "status": self.status,
            "refresh_kind": "eod",
            "trade_date": self.target_date.isoformat(),
            "reason": self.reason,
            "source_run_id": self.source_run_id,
            "snapshot_status": self.snapshot_status,
            "recompute_needed": self.recompute_needed,
            "deleted_intraday": self.deleted_intraday,
            "error": self.error,
        }


EodRunner = Callable[[date, datetime], Awaitable[RadarEodResult]]
RecomputeDatesLoader = Callable[[], Awaitable[Iterable[date]]]


class MarketRadarRuntime:
    """Own the radar queue, daily cron, retention, and partial recomputation."""

    def __init__(
        self,
        *,
        run_eod: EodRunner | None = None,
        load_recompute_dates: RecomputeDatesLoader | None = None,
        clock: Callable[[], datetime] = datetime.now,
    ) -> None:
        self._run_eod = run_eod or _run_eod_with_database
        self._load_recompute_dates = load_recompute_dates or (
            _load_recompute_dates if run_eod is None else _empty_recompute_dates
        )
        self._clock = clock
        self._scheduler: Any | None = None
        self._started = False
        self._tasks: dict[str, RadarRuntimeTask] = {}
        self._active_by_date: dict[date, str] = {}
        self._recompute_needed_dates: set[date] = set()
        self._handled_sync_run_ids: set[str] = set()
        self._lock = asyncio.Lock()

    async def start(self, scheduler: Any | None = None) -> None:
        if self._started:
            return
        restored_dates = set(await self._load_recompute_dates())
        if scheduler is not None:
            scheduler.add_job(
                self._scheduled_eod,
                trigger=CronTrigger(
                    day_of_week="mon-fri",
                    hour=15,
                    minute=20,
                    timezone=_HONG_KONG,
                ),
                id=MARKET_RADAR_EOD_JOB_ID,
                name="Market radar EOD refresh",
                replace_existing=True,
                coalesce=True,
                max_instances=1,
                misfire_grace_time=3600,
            )
        self._recompute_needed_dates.update(restored_dates)
        self._scheduler = scheduler
        self._started = True

    async def stop(self) -> None:
        scheduler = self._scheduler
        self._scheduler = None
        self._started = False
        try:
            if scheduler is not None:
                scheduler.remove_job(MARKET_RADAR_EOD_JOB_ID)
        except JobLookupError:
            pass
        finally:
            queue = get_task_queue(MARKET_RADAR_QUEUE_NAME)
            result = queue.cancel_all()
            for task_id in result["cancelled_task_ids"]:
                state = self._tasks.get(task_id)
                if state is not None and state.status in {"queued", "running"}:
                    state.status = "cancelled"
            await queue.shutdown()

    async def submit_eod(
        self,
        target_date: date,
        *,
        reason: str,
        source_run_id: str | None = None,
    ) -> RadarRuntimeTask:
        if not self._started:
            raise RuntimeError("market radar runtime is not started")
        async with self._lock:
            active_id = self._active_by_date.get(target_date)
            if active_id is not None:
                active = self._tasks[active_id]
                if active.status in {"queued", "running"}:
                    return active
            task_id = f"market-radar-eod-{uuid4().hex}"
            state = RadarRuntimeTask(
                task_id=task_id,
                status="queued",
                target_date=target_date,
                reason=reason,
                source_run_id=source_run_id,
            )
            self._tasks[task_id] = state
            self._active_by_date[target_date] = task_id
            await get_task_queue(MARKET_RADAR_QUEUE_NAME).submit(
                QueuedTask(
                    task_id=task_id,
                    title=f"market radar EOD {target_date.isoformat()}",
                    handler=lambda: self._execute(state),
                    metadata={
                        "refresh_kind": "eod",
                        "trade_date": target_date.isoformat(),
                        "reason": reason,
                        "source_run_id": source_run_id,
                    },
                )
            )
            return state

    async def notify_sync_completed(
        self,
        run_id: str,
        sync_type: str,
    ) -> RadarRuntimeTask | None:
        if sync_type not in _RELATED_SYNC_TYPES or not run_id:
            return None
        async with self._lock:
            if run_id in self._handled_sync_run_ids or not self._recompute_needed_dates:
                return None
            self._handled_sync_run_ids.add(run_id)
            target_date = max(self._recompute_needed_dates)
        return await self.submit_eod(
            target_date,
            reason="sync_completed",
            source_run_id=run_id,
        )

    def task_status(self, task_id: str) -> RadarRuntimeTask | None:
        return self._tasks.get(task_id)

    async def _scheduled_eod(self) -> None:
        await self.submit_eod(self._clock().date(), reason="scheduled")

    async def _execute(self, state: RadarRuntimeTask) -> None:
        state.status = "running"
        cutoff = self._clock() - timedelta(days=90)
        try:
            result = await self._run_eod(state.target_date, cutoff)
        except asyncio.CancelledError:
            state.status = "cancelled"
            raise
        except Exception as exc:
            state.status = "failed"
            state.error = type(exc).__name__
            logger.opt(exception=True).error(
                "Market radar EOD task {} failed: {}",
                state.task_id,
                type(exc).__name__,
            )
        else:
            state.status = "completed"
            state.snapshot_status = result.status
            state.deleted_intraday = result.deleted_intraday
            state.recompute_needed = result.status == "partial"
            if state.recompute_needed:
                newer_dates = {
                    value
                    for value in self._recompute_needed_dates
                    if value > state.target_date
                }
                if not newer_dates:
                    self._recompute_needed_dates = {state.target_date}
            else:
                self._recompute_needed_dates = {
                    value
                    for value in self._recompute_needed_dates
                    if value > state.target_date
                }
        finally:
            async with self._lock:
                if self._active_by_date.get(state.target_date) == state.task_id:
                    self._active_by_date.pop(state.target_date, None)


async def _run_eod_with_database(target_date: date, cutoff: datetime) -> RadarEodResult:
    from app.data_stores import get_market_data_store
    from app.db.sqlite import async_session_factory
    from app.services.market_radar import MarketRadarService
    from app.services.market_radar_data import MarketRadarDataService
    from app.services.market_radar_store import MarketRadarStore
    from app.services.qmt_realtime_feed import QmtRealtimeFeed

    async with async_session_factory() as session:
        market_store = get_market_data_store()
        radar_store = MarketRadarStore(session)
        service = MarketRadarService(
            feed=QmtRealtimeFeed(universe_loader=lambda: (), enabled=False),
            data_service=MarketRadarDataService(session, store=market_store),
            store=radar_store,
            eligible_universe_loader=_empty_universe,
            symbol_context_loader=_empty_symbol_context,
        )
        try:
            snapshot = await service.refresh_eod(target_date, commit=False)
            deleted = await radar_store.cleanup_intraday_snapshots(cutoff=cutoff)
            await session.commit()
        except BaseException:
            await session.rollback()
            raise
    return RadarEodResult(status=snapshot.status, deleted_intraday=deleted)


async def _load_recompute_dates() -> tuple[date, ...]:
    from app.db.sqlite import async_session_factory
    from app.services.market_radar_store import MarketRadarStore

    async with async_session_factory() as session:
        snapshot = await MarketRadarStore(session).get_latest_snapshot(snapshot_type="eod")
    if snapshot is None or snapshot.status != "partial":
        return ()
    return (snapshot.as_of.date(),)


async def _empty_recompute_dates() -> tuple[date, ...]:
    return ()


async def _empty_universe() -> tuple[str, ...]:
    return ()


async def _empty_symbol_context(*_args: object) -> dict[str, object]:
    return {}


_runtime: MarketRadarRuntime | None = None


def start_market_radar_runtime() -> MarketRadarRuntime:
    global _runtime
    if _runtime is None:
        _runtime = MarketRadarRuntime()
    return _runtime


def get_market_radar_runtime() -> MarketRadarRuntime | None:
    return _runtime


async def notify_market_radar_sync_completed(
    run_id: str,
    sync_type: str,
) -> RadarRuntimeTask | None:
    runtime = get_market_radar_runtime()
    if runtime is None:
        return None
    return await runtime.notify_sync_completed(run_id, sync_type)

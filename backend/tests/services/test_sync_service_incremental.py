from __future__ import annotations

import asyncio
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from app.services import sync_service as sync_service_module
from app.services.sentiment import XueqiuCrawlBlockedError
from app.services.sentiment_focus_pool import FocusTarget, ResolvedFocusPool
from app.services.sync_service import SyncService


class FakeSession:
    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class FakeAsyncSession(FakeSession):
    pass


class FakeSessionContext:
    async def __aenter__(self):
        return FakeAsyncSession()

    async def __aexit__(self, exc_type, exc, traceback):
        return False


async def noop_persist_sync_progress(self, *args, **kwargs) -> None:
    return None


async def noop_create_sync_log(self, *args, **kwargs) -> object:
    return object()


@pytest.fixture(autouse=True)
def no_incremental_coverage_issues(monkeypatch):
    def fake_coverage_details(dataset, start_date, end_date, symbols=None):
        return {
            "lookback_start": start_date.isoformat(),
            "lookback_end": end_date.isoformat(),
            "reference_date_count": 0,
            "observed_date_count": 0,
            "missing_dates": [],
            "missing_ranges": [],
            "low_coverage_dates": [],
            "earliest_issue_date": None,
            "expected_symbol_count": 0,
            "symbol_threshold": None,
            "typical_minute_bars_per_symbol": None,
            "minute_bar_threshold": None,
        }

    monkeypatch.setattr(sync_service_module, "_market_incremental_coverage_details", fake_coverage_details)


@pytest.mark.asyncio
async def test_sync_sentiment_nga_tracks_flocktrader_progress(monkeypatch):
    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.progress_callback = progress_callback

        async def run(self, source, symbol, **kwargs):
            assert source == "flocktrader"
            assert symbol is None
            assert kwargs["force_refresh"] is True
            if self.progress_callback:
                self.progress_callback(
                    {
                        "stage": "nga.cache.write",
                        "source": "flocktrader",
                        "current_step": "cache_write",
                        "current_date": "2026-06-27",
                        "cache_posts": 140,
                        "scan_time_basis": "last_reply_time",
                        "cache_partition": "last_reply_time",
                    }
                )
            return {"source": "flocktrader", "mode": "daily_cache", "upserted": 3}

    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeAsyncSession()).sync_sentiment_nga(
        start_date=date(2026, 6, 27),
        end_date=date(2026, 6, 27),
    )

    assert progress.status == "completed"
    assert progress.details["source"] == "flocktrader"
    assert progress.details["nga_progress"]["source"] == "flocktrader"
    assert progress.details["current_date"] == "2026-06-27"
    assert progress.details["cache_posts"] == 140
    assert progress.details["scan_time_basis"] == "last_reply_time"
    assert progress.details["cache_partition"] == "last_reply_time"
    assert progress.details["result"]["upserted"] == 3


@pytest.mark.asyncio
async def test_sync_sentiment_gates_xueqiu_once_for_all_symbols(monkeypatch):
    instances = []

    class FakeXueqiuSession:
        def __init__(self, **kwargs):
            self.symbols = []
            self.wait_calls = 0
            self.disconnect_calls = 0
            instances.append(self)

        async def start(self):
            return None

        async def wait_for_login(self):
            self.wait_calls += 1
            return SimpleNamespace(status="authenticated", auth={"server_verified": True})

        async def collect(self, symbol, **kwargs):
            self.symbols.append(symbol)
            return [], {"raw_count": 0, "auth": {"server_verified": True}}

        async def disconnect(self):
            self.disconnect_calls += 1

    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.service = self

        def _collect_xueqiu(self, *args, **kwargs):
            raise AssertionError("fake session handles collection")

        async def upsert_posts(self, posts):
            return len(posts)

        async def run(self, *args, **kwargs):
            raise AssertionError("scheduled Xueqiu must use the shared session")

    monkeypatch.setattr(sync_service_module, "XueqiuSession", FakeXueqiuSession, raising=False)
    monkeypatch.setattr(sync_service_module, "async_session_factory", lambda: FakeSessionContext())
    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeAsyncSession()).sync_sentiment(
        sources=["xueqiu_spyder"],
        symbols=["600519.SH", "000001.SZ"],
        max_pages=1,
        min_reply=0,
        sync_mode="full",
    )

    assert len(instances) == 1
    assert instances[0].wait_calls == 1
    assert instances[0].symbols == ["600519.SH", "000001.SZ"]
    assert instances[0].disconnect_calls == 1
    assert progress.success_count == 2


@pytest.mark.asyncio
async def test_sync_sentiment_xueqiu_waits_for_manual_login_with_configured_cadence(monkeypatch):
    constructor_kwargs = []

    class FakeXueqiuSession:
        def __init__(self, **kwargs):
            constructor_kwargs.append(kwargs)

        async def start(self):
            return None

        async def wait_for_login(self):
            return SimpleNamespace(status="login_timeout", auth={"server_verified": False})

        async def disconnect(self):
            return None

    monkeypatch.setattr(sync_service_module, "XueqiuSession", FakeXueqiuSession, raising=False)
    monkeypatch.setattr(sync_service_module, "async_session_factory", lambda: FakeSessionContext())
    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", lambda *args, **kwargs: SimpleNamespace())
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)
    monkeypatch.setattr(sync_service_module.app_settings, "xueqiu_login_poll_interval_seconds", 75.0, raising=False)
    monkeypatch.setattr(sync_service_module.app_settings, "xueqiu_login_timeout_seconds", 0, raising=False)

    await SyncService(FakeAsyncSession()).sync_sentiment(
        sources=["xueqiu_spyder"],
        symbols=["600519.SH"],
        max_pages=1,
        min_reply=0,
        sync_mode="full",
    )

    assert constructor_kwargs[0]["poll_interval"] == 75.0
    assert constructor_kwargs[0]["login_timeout"] is None


@pytest.mark.asyncio
async def test_sync_sentiment_login_timeout_skips_all_xueqiu_symbols(monkeypatch):
    instances = []

    class FakeXueqiuSession:
        def __init__(self, **kwargs):
            self.symbols = []
            self.disconnect_calls = 0
            instances.append(self)

        async def start(self):
            return None

        async def wait_for_login(self):
            return SimpleNamespace(status="login_timeout", auth={"server_verified": False})

        async def collect(self, symbol, **kwargs):
            self.symbols.append(symbol)
            raise AssertionError("timed-out session must not collect")

        async def disconnect(self):
            self.disconnect_calls += 1

    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.service = self

        def _collect_xueqiu(self, *args, **kwargs):
            raise AssertionError("timed-out session must not collect")

        async def run(self, *args, **kwargs):
            raise AssertionError("scheduled Xueqiu must use the shared session")

    monkeypatch.setattr(sync_service_module, "XueqiuSession", FakeXueqiuSession, raising=False)
    monkeypatch.setattr(sync_service_module, "async_session_factory", lambda: FakeSessionContext())
    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeAsyncSession()).sync_sentiment(
        sources=["xueqiu_spyder"],
        symbols=["600519.SH", "000001.SZ"],
        max_pages=1,
        min_reply=0,
        sync_mode="full",
    )

    assert instances[0].symbols == []
    assert instances[0].disconnect_calls == 1
    assert progress.failed_count == 1
    assert progress.details["results"] == [
        {
            "ok": False,
            "source": "xueqiu_spyder",
            "symbol": None,
            "error_code": "xueqiu_login_timeout",
            "error": "Xueqiu login timed out; Chrome remains open for manual login",
        }
    ]


@pytest.mark.asyncio
async def test_sync_sentiment_cancellation_disconnects_xueqiu_session(monkeypatch):
    collect_started = asyncio.Event()
    release_collect = asyncio.Event()
    instance = None

    class FakeXueqiuSession:
        def __init__(self, **kwargs):
            nonlocal instance
            self.symbols = []
            self.disconnect_calls = 0
            instance = self

        async def start(self):
            return None

        async def wait_for_login(self):
            return SimpleNamespace(status="authenticated", auth={"server_verified": True})

        async def collect(self, symbol, **kwargs):
            self.symbols.append(symbol)
            collect_started.set()
            await release_collect.wait()
            return [], {"raw_count": 0}

        async def disconnect(self):
            self.disconnect_calls += 1

    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.service = self

        def _collect_xueqiu(self, *args, **kwargs):
            raise AssertionError("fake session handles collection")

        async def upsert_posts(self, posts):
            return 0

    monkeypatch.setattr(sync_service_module, "XueqiuSession", FakeXueqiuSession)
    monkeypatch.setattr(sync_service_module, "async_session_factory", lambda: FakeSessionContext())
    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    task = asyncio.create_task(
        SyncService(FakeAsyncSession()).sync_sentiment(
            sources=["xueqiu_spyder"],
            symbols=["600519.SH", "000001.SZ"],
            max_pages=1,
            min_reply=0,
            sync_mode="full",
        )
    )
    await collect_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert instance is not None
    assert instance.symbols == ["600519.SH"]
    assert instance.disconnect_calls == 1


@pytest.mark.asyncio
async def test_sync_sentiment_uses_focus_pool_page_limits_and_pacing(monkeypatch):
    instances = []
    delays = []

    class FakeResolver:
        def __init__(self, session):
            assert isinstance(session, FakeAsyncSession)

        async def resolve(self):
            return ResolvedFocusPool(
                targets=(
                    FocusTarget("002313.SZ", ("qmt_holding",)),
                    FocusTarget("600114.SH", ("qmt_holding", "vault_active")),
                    FocusTarget("002138.SZ", ("vault_active",), "valid.md"),
                ),
                qmt_status="fresh",
                vault_count=2,
                overlap_count=1,
                snapshot_captured_at=datetime(2026, 7, 17, 10, 0),
            )

    class FakeXueqiuSession:
        def __init__(self, **kwargs):
            self.calls = []
            instances.append(self)

        async def start(self):
            return None

        async def wait_for_login(self):
            return SimpleNamespace(status="authenticated", auth={"server_verified": True})

        async def collect(self, symbol, **kwargs):
            self.calls.append((symbol, kwargs["max_pages"]))
            return [], {"raw_count": 0}

        async def disconnect(self):
            return None

    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.service = self

        def _collect_xueqiu(self, *args, **kwargs):
            raise AssertionError("fake session handles collection")

        async def upsert_posts(self, posts):
            return 0

    async def fake_sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(sync_service_module, "SentimentFocusPoolResolver", FakeResolver, raising=False)
    monkeypatch.setattr(sync_service_module, "XueqiuSession", FakeXueqiuSession)
    monkeypatch.setattr(sync_service_module, "async_session_factory", lambda: FakeSessionContext())
    monkeypatch.setattr(sync_service_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(sync_service_module.app_settings, "xueqiu_stock_delay_seconds", 2.0)
    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeAsyncSession()).sync_sentiment(
        sources=["xueqiu_spyder"],
        max_pages=3,
        min_reply=0,
        sync_mode="full",
    )

    assert instances[0].calls == [
        ("002313.SZ", 3),
        ("600114.SH", 3),
        ("002138.SZ", 2),
    ]
    assert delays == [2.0, 2.0]
    assert progress.details["target_pool"]["symbol_count"] == 3
    assert progress.details["target_pool"]["qmt_status"] == "fresh"
    assert progress.details["target_pool"]["overlap_count"] == 1


@pytest.mark.asyncio
async def test_sync_sentiment_explicit_xueqiu_symbol_bypasses_focus_pool(monkeypatch):
    calls = []

    class FailResolver:
        def __init__(self, session):
            raise AssertionError("explicit symbols must bypass focus pool resolution")

    class FakeXueqiuSession:
        def __init__(self, **kwargs):
            return None

        async def start(self):
            return None

        async def wait_for_login(self):
            return SimpleNamespace(status="authenticated", auth={"server_verified": True})

        async def collect(self, symbol, **kwargs):
            calls.append((symbol, kwargs["max_pages"]))
            return [], {"raw_count": 0}

        async def disconnect(self):
            return None

    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.service = self

        def _collect_xueqiu(self, *args, **kwargs):
            raise AssertionError("fake session handles collection")

        async def upsert_posts(self, posts):
            return 0

    monkeypatch.setattr(sync_service_module, "SentimentFocusPoolResolver", FailResolver, raising=False)
    monkeypatch.setattr(sync_service_module, "XueqiuSession", FakeXueqiuSession)
    monkeypatch.setattr(sync_service_module, "async_session_factory", lambda: FakeSessionContext())
    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeAsyncSession()).sync_sentiment(
        sources=["xueqiu_spyder"],
        symbols=["600519.SH"],
        max_pages=4,
        min_reply=0,
        sync_mode="full",
    )

    assert calls == [("600519.SH", 4)]
    assert "target_pool" not in progress.details


@pytest.mark.asyncio
async def test_sync_sentiment_circuit_breaker_stops_remaining_xueqiu_targets(monkeypatch):
    attempted = []

    class FakeResolver:
        def __init__(self, session):
            return None

        async def resolve(self):
            return ResolvedFocusPool(
                targets=(
                    FocusTarget("002313.SZ", ("qmt_holding",)),
                    FocusTarget("600114.SH", ("qmt_holding",)),
                    FocusTarget("002138.SZ", ("vault_active",)),
                ),
                qmt_status="fresh",
                vault_count=1,
                overlap_count=0,
                snapshot_captured_at=datetime(2026, 7, 17, 10, 0),
            )

    class FakeXueqiuSession:
        def __init__(self, **kwargs):
            return None

        async def start(self):
            return None

        async def wait_for_login(self):
            return SimpleNamespace(status="authenticated", auth={"server_verified": True})

        async def collect(self, symbol, **kwargs):
            attempted.append(symbol)
            if symbol == "600114.SH":
                raise XueqiuCrawlBlockedError(reason="http_405", status_code=405)
            return [], {"raw_count": 0}

        async def disconnect(self):
            return None

    class FakeSentimentIngestService:
        def __init__(self, session, progress_callback=None):
            self.service = self

        def _collect_xueqiu(self, *args, **kwargs):
            raise AssertionError("fake session handles collection")

        async def upsert_posts(self, posts):
            return 0

    monkeypatch.setattr(sync_service_module, "SentimentFocusPoolResolver", FakeResolver)
    monkeypatch.setattr(sync_service_module, "XueqiuSession", FakeXueqiuSession)
    monkeypatch.setattr(sync_service_module, "async_session_factory", lambda: FakeSessionContext())
    monkeypatch.setattr(sync_service_module.app_settings, "xueqiu_stock_delay_seconds", 0.0)
    monkeypatch.setattr("app.services.sentiment.SentimentIngestService", FakeSentimentIngestService)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeAsyncSession()).sync_sentiment(
        sources=["xueqiu_spyder"],
        max_pages=3,
        min_reply=0,
        sync_mode="full",
    )

    assert attempted == ["002313.SZ", "600114.SH"]
    assert progress.status == "completed"
    assert progress.details["outcome"] == "partial"
    assert progress.details["target_pool"]["crawl_limited_reason"] == "http_405"
    assert [result.get("error_code") for result in progress.details["results"]] == [
        None,
        "xueqiu_circuit_breaker",
        "xueqiu_circuit_open",
    ]


@pytest.mark.asyncio
async def test_sync_kline_daily_incremental_backfills_middle_gap(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_daily_batch(symbols, start_date, end_date):
        captured["symbols"] = symbols
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    def fake_coverage_details(dataset, start_date, end_date, symbols=None):
        return {
            "lookback_start": start_date.isoformat(),
            "lookback_end": end_date.isoformat(),
            "reference_date_count": 4,
            "observed_date_count": 3,
            "missing_dates": ["2026-06-03"],
            "missing_ranges": [{"start": "2026-06-03", "end": "2026-06-03"}],
            "low_coverage_dates": [],
            "earliest_issue_date": "2026-06-03",
            "expected_symbol_count": 1,
            "symbol_threshold": None,
            "typical_minute_bars_per_symbol": None,
            "minute_bar_threshold": None,
        }

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 6),
    )
    monkeypatch.setattr(sync_service_module, "_market_incremental_coverage_details", fake_coverage_details)
    monkeypatch.setattr(sync_service_module.qmt_gateway, "get_kline_daily_batch", fake_get_kline_daily_batch)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_daily(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["start_date"] == date(2026, 6, 3)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.details["start_date"] == "2026-06-03"
    assert progress.details["latest_local_date"] == "2026-06-06"
    assert progress.details["incremental"]["incremental_start_reason"] == "coverage_gap_or_low_coverage"


@pytest.mark.asyncio
async def test_sync_kline_daily_incremental_refreshes_low_coverage_latest_date(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_daily_batch(symbols, start_date, end_date):
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    def fake_coverage_details(dataset, start_date, end_date, symbols=None):
        return {
            "lookback_start": start_date.isoformat(),
            "lookback_end": end_date.isoformat(),
            "reference_date_count": 1,
            "observed_date_count": 1,
            "missing_dates": [],
            "missing_ranges": [],
            "low_coverage_dates": [
                {
                    "date": "2026-06-06",
                    "row_count": 15,
                    "symbol_count": 15,
                    "reasons": ["low_symbol_coverage"],
                }
            ],
            "earliest_issue_date": "2026-06-06",
            "expected_symbol_count": 5000,
            "symbol_threshold": 4250,
            "typical_minute_bars_per_symbol": None,
            "minute_bar_threshold": None,
        }

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 6),
    )
    monkeypatch.setattr(sync_service_module, "_market_incremental_coverage_details", fake_coverage_details)
    monkeypatch.setattr(sync_service_module.qmt_gateway, "get_kline_daily_batch", fake_get_kline_daily_batch)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_daily(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["start_date"] == date(2026, 6, 6)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.status == "completed"
    assert "skipped" not in progress.details


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_overlaps_latest_local_date(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_minute_batch(symbols, start_date, end_date):
        captured["symbols"] = symbols
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 4),
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fake_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["symbols"] == ["000001.SZ"]
    assert captured["start_date"] == date(2026, 6, 3)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.details["start_date"] == "2026-06-03"
    assert progress.details["latest_local_date"] == "2026-06-04"
    assert progress.details["auto_incremental"] is True
    assert progress.details["incremental_overlap_days"] == 1


@pytest.mark.asyncio
async def test_build_datasync_plan_reports_exact_minute_watermark(monkeypatch):
    def fake_plan(dataset, **kwargs):
        latest = date(2026, 7, 10) if dataset == "klines_minute" else date(2026, 7, 9)
        return date(2026, 7, 9), latest, {"coverage": {}}

    monkeypatch.setattr(sync_service_module, "_market_incremental_sync_plan", fake_plan)
    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_datetime",
        lambda dataset, symbols=None: datetime(2026, 7, 10, 14, 37),
    )
    monkeypatch.setattr(
        sync_service_module,
        "_daily_trading_dependency_start",
        lambda end_date: date(2026, 7, 9),
        raising=False,
    )

    plan = await SyncService(FakeSession()).build_datasync_plan(end_date=date(2026, 7, 10))

    assert plan["latest"]["kline_minute"] == "2026-07-10T14:37:00"
    assert plan["ranges"]["kline_minute"]["watermark"] == "2026-07-10T14:37:00"
    assert "daily_trading_dependencies" in plan["steps"]
    assert plan["ranges"]["daily_trading_dependencies"] == {
        "start_date": "2026-07-09",
        "end_date": "2026-07-10",
        "will_sync": True,
    }


def test_sync_daily_trading_dependencies_requests_only_p0_datasets(monkeypatch):
    captured: dict[str, object] = {}

    def fake_sync(step):
        captured.update(step)
        return {"daily_basic_rows": 10, "limit_rows": 10, "adj_factor_rows": 10}

    monkeypatch.setattr("app.services.factor_dependency_sync._sync_tushare_daily_step", fake_sync)

    result = sync_service_module._sync_daily_trading_dependencies(
        date(2026, 7, 9),
        date(2026, 7, 10),
    )

    assert captured["datasets"] == ["stock_daily_basic", "stock_limit_prices", "adj_factors"]
    assert result["adj_factor_rows"] == 10


def test_minute_sync_completeness_reports_coverage_and_missing_symbols(monkeypatch):
    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_datetime",
        lambda dataset, symbols=None: datetime(2026, 7, 10, 14, 37),
    )
    monkeypatch.setattr(
        sync_service_module,
        "_market_incremental_coverage_details",
        lambda dataset, start_date, end_date, symbols=None: {
            "missing_dates": ["2026-07-09"],
            "missing_ranges": [{"start": "2026-07-09", "end": "2026-07-09"}],
            "low_coverage_dates": [],
        },
    )
    monkeypatch.setattr(
        sync_service_module,
        "_minute_latest_date_symbols",
        lambda trade_date, symbols=None: {"000001.SZ"},
    )

    result = sync_service_module._minute_sync_completeness(
        start_date=date(2026, 7, 9),
        end_date=date(2026, 7, 10),
        symbols=["000001.SZ", "000002.SZ"],
    )

    assert result["latest_datetime"] == "2026-07-10T14:37:00"
    assert result["symbol_coverage_ratio"] == 0.5
    assert result["missing_symbol_count"] == 1
    assert result["missing_symbols"] == ["000002.SZ"]
    assert result["missing_ranges"] == [{"start": "2026-07-09", "end": "2026-07-09"}]


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_refreshes_overlap_when_latest_is_end(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_minute_batch(symbols, start_date, end_date):
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 6),
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fake_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["start_date"] == date(2026, 6, 5)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.status == "completed"
    assert "skipped" not in progress.details


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_skips_when_local_data_after_target_end(monkeypatch):
    async def fail_get_kline_minute_batch(*args, **kwargs):
        raise AssertionError("sync should skip when local overlap start is after target end")

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: date(2026, 6, 8),
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fail_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert progress.status == "completed"
    assert progress.details["skipped"] is True
    assert progress.details["skip_reason"] == "already up to date"
    assert progress.details["start_date"] == "2026-06-07"


@pytest.mark.asyncio
async def test_sync_kline_minute_incremental_preserves_explicit_start_without_local_data(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_get_kline_minute_batch(symbols, start_date, end_date):
        captured["start_date"] = start_date
        captured["end_date"] = end_date
        return {}

    monkeypatch.setattr(
        sync_service_module,
        "_latest_market_date_for_symbols",
        lambda dataset, symbols: None,
    )
    monkeypatch.setattr(
        sync_service_module.qmt_gateway,
        "get_kline_minute_batch",
        fake_get_kline_minute_batch,
    )
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        start_date=date(2026, 5, 1),
        end_date=date(2026, 6, 6),
        auto_incremental=True,
    )

    assert captured["start_date"] == date(2026, 5, 1)
    assert captured["end_date"] == date(2026, 6, 6)
    assert progress.details["start_date"] == "2026-05-01"


@pytest.mark.asyncio
async def test_sync_kline_minute_cleans_cache_when_cancelled(monkeypatch):
    cleaned: dict[str, object] = {}

    monkeypatch.setattr(sync_service_module.app_settings, "qmt_minute_clean_cache_after_sync", True)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)
    monkeypatch.setattr(sync_service_module, "_sync_cancelled", lambda progress: True)

    def fake_clean_local_cache(**kwargs):
        cleaned.update(kwargs)
        return {"deleted": 1, "freed_mb": 0.1}

    monkeypatch.setattr(sync_service_module.qmt_gateway, "clean_local_cache", fake_clean_local_cache)

    progress = await SyncService(FakeSession()).sync_kline_minute(
        symbols=["000001.SZ"],
        start_date=date(2026, 6, 6),
        end_date=date(2026, 6, 6),
    )

    assert progress.status == "cancelled"
    assert cleaned == {"symbols": ["000001.SZ"], "data_type": "kline"}
    assert progress.details["cache_cleaned"] == {"deleted": 1, "freed_mb": 0.1}


@pytest.mark.asyncio
async def test_sync_kline_minute_skips_cache_cleanup_when_no_symbols(monkeypatch):
    cleaned = []

    monkeypatch.setattr(sync_service_module.app_settings, "qmt_minute_clean_cache_after_sync", True)
    monkeypatch.setattr(SyncService, "persist_sync_progress", noop_persist_sync_progress)
    monkeypatch.setattr(SyncService, "create_sync_log", noop_create_sync_log)

    def fake_clean_local_cache(**kwargs):
        cleaned.append(kwargs)
        return {"deleted": 0, "freed_mb": 0}

    monkeypatch.setattr(sync_service_module.qmt_gateway, "clean_local_cache", fake_clean_local_cache)

    progress = type("P", (), {"details": {}})()
    sync_service_module._clean_qmt_kline_cache_after_download(progress, [])

    assert cleaned == []
    assert progress.details["cache_cleaned"] == "skipped"
    assert progress.details["cache_clean_skipped_reason"] == "no_symbols"

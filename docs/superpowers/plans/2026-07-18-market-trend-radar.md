# Market Trend Radar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an explainable A-share market radar with QMT push quotes, 30-second polling fallback, SSE delivery, persisted alerts, daily breadth/limit/crowding/sentiment analytics, and a responsive Vue workbench.

**Architecture:** The API process owns one `QmtRealtimeFeed`, coalesces raw callbacks into a one-second market snapshot, persists a throttled snapshot, evaluates alert rules, and broadcasts aggregate SSE events. The sync service owns daily analytics jobs; both processes share SQLite snapshots/events while all market history remains in Parquet/DuckDB. Frontend state consumes REST for initial/recovery snapshots and SSE for changes, falling back to 30-second REST polling.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy async, Alembic, DuckDB/Parquet, xtquant, pytest, Vue 3, TypeScript, Pinia, ECharts 6, Vitest, Playwright

---

## File Map

- `backend/app/db/models/market_radar.py`: snapshot, rule, and event ORM models only.
- `backend/app/services/market_radar_calculator.py`: pure breadth, crowding, emotion, and severity calculations.
- `backend/app/services/market_radar_data.py`: SQLite/Parquet/sentiment reads and source freshness.
- `backend/app/services/qmt_realtime_feed.py`: sole QMT quote owner and push/poll/offline state machine.
- `backend/app/services/market_radar.py`: orchestration, persistence, rule lifecycle, and stream broker.
- `backend/app/api/market_radar.py`: REST and SSE contracts.
- `frontend/src/api/marketRadar.ts`: typed REST/SSE payload contracts.
- `frontend/src/stores/marketRadar.ts`: snapshot loading, stream recovery, alert actions, and 30-second fallback.
- `frontend/src/views/MarketRadar/`: page and focused chart/table components.
- `docs/market-radar.md`: user-facing metric, freshness, runtime, and troubleshooting guide.

## Locked Interfaces

The backend modules must use these public shapes; implementation-only fields may be private:

```python
@dataclass(frozen=True)
class QuoteTick:
    symbol: str
    quote_time: datetime
    last_price: float
    previous_close: float
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    volume: float | None = None
    amount: float | None = None
    stock_status: int | None = None
    speed_1m: float | None = None
    speed_5m: float | None = None

@dataclass(frozen=True)
class RealtimeFeedStatus:
    mode: Literal["push", "polling_30s", "offline", "closed"]
    changed_at: datetime
    last_quote_at: datetime | None
    connection_generation: int
    reason: str | None
    market_coverage: dict[str, float]

class QmtRealtimeFeed:
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def run_health_cycle(self) -> None: ...
    def latest_ticks(self) -> dict[str, QuoteTick]: ...
    @property
    def status(self) -> RealtimeFeedStatus: ...
```

REST responses and SSE `snapshot` data share one envelope:

```typescript
export interface MarketRadarEnvelope<T> {
  as_of: string | null
  computed_at: string
  status: 'fresh' | 'partial' | 'stale' | 'unavailable'
  confidence: number
  realtime_mode: 'push' | 'polling_30s' | 'offline' | 'closed'
  sources: Array<{ name: string; as_of: string | null; status: string; reason?: string | null }>
  data: T
}
```

SSE event data adds `schema_version: 1`, `event_id`, `sequence`, and `occurred_at`; event names remain `mode`, `snapshot`, `alert`, and `heartbeat`.

## Task 1: Pure Market Calculations

**Files:**
- Create: `backend/app/services/market_radar_calculator.py`
- Create: `backend/tests/services/test_market_radar_calculator.py`

- [ ] **Step 1: Write failing bucket and coverage tests**

Define synthetic ticks around all ten boundaries plus a flat, suspended, stale, and invalid quote. Assert `calculate_breadth()` returns exact bucket counts, separate flat count, valid/eligible counts, and `partial` below 80% coverage.

```python
result = calculate_breadth(ticks, eligible_symbols, now=NOW, max_age_seconds=5)
assert result.buckets["le_neg_8"].count == 1
assert result.buckets["ge_pos_8"].count == 1
assert result.flat_count == 1
assert result.coverage.valid == 11
```

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_market_radar_calculator.py -q`

Expected: import fails because the calculator does not exist.

- [ ] **Step 3: Implement immutable contracts and breadth calculation**

Add `QuoteTick`, `Coverage`, `BreadthBucket`, `BreadthResult`, `ScoreComponent`, and `CompositeScore` dataclasses. Normalize percent returns as percentage points and use the exact interval definitions in the design spec. Invalid/stale/suspended ticks never enter the numerator.

- [ ] **Step 4: Add failing crowding/emotion tests**

Assert fixed weights, 120-day percentile conversion, missing component reweighting, the 70% minimum effective weight, and all emotion labels at exact boundaries.

```python
score = composite_score(components, minimum_weight=0.70)
assert score.status == "fresh"
assert sum(item.contribution for item in score.components) == pytest.approx(score.value)
assert emotion_label(15) == "恐慌"
```

- [ ] **Step 5: Implement score helpers and run GREEN**

Implement robust percentile rank with deterministic ties, direction inversion, contribution detail, crowding labels, and emotion labels. Run the focused test; expected all pass.

## Task 2: Persistence And Migration

**Files:**
- Create: `backend/app/db/models/market_radar.py`
- Modify: `backend/app/db/models/__init__.py`
- Create: `backend/migrations/versions/20260718_0001_market_radar.py`
- Modify: `backend/tests/test_migrations.py`
- Create: `backend/tests/services/test_market_radar_store.py`

- [ ] **Step 1: Write failing model and migration tests**

Assert the three tables and required indexes/columns exist after upgrade, downgrade removes only radar tables, and no account ID, position quantity, cost, cookie, or token column exists.

- [ ] **Step 2: Run migration tests and confirm RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/test_migrations.py -k market_radar -q`

- [ ] **Step 3: Add models and migration**

Create `MarketRadarSnapshot`, `MarketAlertRule`, and `MarketAlertEvent`. Store JSON as text consistently with existing models; use timezone-naive local datetimes consistently with the project. Set migration `down_revision = "20260714_0001"` after rechecking `alembic heads` immediately before creation.

- [ ] **Step 4: Implement store tests and repository helpers**

Test idempotent snapshot upsert, one unresolved event per dedupe key, new cycle after resolution, acknowledge/dismiss transitions, and 90-day intraday snapshot cleanup.

- [ ] **Step 5: Run model/store/migration tests and confirm GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/test_migrations.py tests/services/test_market_radar_store.py -q`

## Task 3: QMT Push With 30-Second Fallback

**Files:**
- Create: `backend/app/services/qmt_realtime_feed.py`
- Modify: `backend/app/core/config.py`
- Modify: `.env.example`
- Create: `backend/tests/services/test_qmt_realtime_feed.py`

- [ ] **Step 1: Write a fake xtdata adapter and failing state-machine tests**

The fake must capture callbacks and subscription IDs without connecting to QMT. Cover `push`, stale-after-5-seconds, `polling_30s`, `offline`, recovery after 60 seconds, generation changes, late callback rejection, BJ-only failure, and ordered shutdown.

```python
feed = QmtRealtimeFeed(adapter=fake, clock=clock, universe_loader=load_symbols)
await feed.start()
fake.emit("600000.SH", valid_tick())
assert feed.status.mode == "push"
clock.advance(6)
await feed.run_health_cycle()
assert feed.status.mode == "polling_30s"
```

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_qmt_realtime_feed.py -q`

- [ ] **Step 3: Implement adapter, owner lifecycle, and coalescing**

Lazy-import xtquant, subscribe to SH/SZ/BJ plus explicit core indices, normalize only approved fields, guard callbacks with `connection_generation`, keep latest values by symbol, and signal the asyncio loop thread-safely. Do not call databases or calculators in callbacks.

- [ ] **Step 4: Implement polling and recovery**

Directly call `xtdata.get_full_tick(active_symbols + core_indices)` under a non-reentrant owner guard with a 20-second timeout. Skip a cycle while a timed-out SDK call is still active. Retry push every 60 seconds and preserve per-market coverage/reason details.

- [ ] **Step 5: Add settings and run GREEN**

Add the four documented `MARKET_RADAR_*` settings with strict positive bounds and sample env values. Run focused tests and `ruff check` for the new module.

## Task 4: Data Aggregation And Freshness

**Files:**
- Create: `backend/app/services/market_radar_data.py`
- Create: `backend/tests/services/test_market_radar_data.py`

- [ ] **Step 1: Write failing daily breadth tests with temporary Parquet**

Create two-day bars covering first listing day, suspended/zero-volume, ST, BJ, and valid SH/SZ rows. Assert ten bins, flat count, market breakdown, exclusions, and latest expected trading day.

- [ ] **Step 2: Implement structured DuckDB queries**

Read through `ParquetMarketDataStore` path helpers and parameterized DuckDB queries. Join the valid stock universe from SQLite in memory; never accept arbitrary SQL/WHERE input.

- [ ] **Step 3: Write and implement limit-ladder freshness tests**

Use temporary `tushare_limit_list_d` and `tushare_limit_step` Parquet. Exact-date data returns rows; old data returns `stale` with its real date; missing step data derives consecutive limit-ups only from fresh detail rows.

- [ ] **Step 4: Implement crowding, sector, and sentiment inputs**

Compute top-1/top-5 amount shares, top-three industry share, volume percentile, top-300 20-day mean correlation, and optional margin change. Read sentiment summaries from existing analyzed rows and return component-level source dates and statuses.

- [ ] **Step 5: Run data tests and confirm GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_market_radar_data.py -q`

## Task 5: Radar Orchestration And Alert Lifecycle

**Files:**
- Create: `backend/app/services/market_radar.py`
- Create: `backend/tests/services/test_market_radar_service.py`

- [ ] **Step 1: Write failing orchestration tests**

Inject fake feed/data/store dependencies. Assert one-second coalescing, 30-second snapshot persistence, immediate high alert persistence, no tick persistence, and `fresh/partial/stale/unavailable` propagation.

- [ ] **Step 2: Implement the stream broker**

Use a bounded subscriber queue per client. Coalesce pending `snapshot` events, never drop `mode` or `alert`, disconnect chronically slow clients, and emit a 15-second heartbeat.

- [ ] **Step 3: Write failing default-rule tests**

Cover every fixed threshold from the design, holding/watchlist sensitivity, data freshness gating, 15-minute cooldown, medium persistence for two snapshots, worsening escalation, and resolution after two clear snapshots.

- [ ] **Step 4: Implement the rule engine and focus universe**

Union positive QMT holdings, watchlist symbols, and active focus-pool targets. Reuse `SentimentFocusPoolResolver.resolve()` from `backend/app/services/sentiment_focus_pool.py`; do not duplicate account parsing or persist position amounts. Evaluate only typed rule kinds and build evidence with value, threshold, baseline, source time, and formula/rule version.

- [ ] **Step 5: Run service tests and confirm GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_market_radar_service.py -q`

## Task 6: REST, SSE, And Application Lifecycle

**Files:**
- Create: `backend/app/api/market_radar.py`
- Modify: `backend/app/api/router.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/sync_main.py`
- Create: `backend/tests/api/test_market_radar_routes.py`

- [ ] **Step 1: Write failing API contract tests**

Cover overview, breadth, limit ladder, crowding, sectors, paged alerts, rule CRUD, acknowledge/dismiss, refresh task submission, and SSE headers/first events. Invalid rule parameters must return 422; stale components must keep dates and reasons.

- [ ] **Step 2: Implement typed request/response models and routes**

Register `/api/market-radar`. SSE frames include `id`, `event`, `retry: 5000`, JSON data, `no-cache, no-transform`, and `X-Accel-Buffering: no`. Catch client cancellation without logging an error.

- [ ] **Step 3: Wire lifecycles exactly once**

Start/stop the singleton real-time feed in API lifespan when enabled. Register daily radar jobs only in sync-service scheduler; do not create a second scheduler or QMT subscription. miniQMT absence logs a bounded status message and never aborts startup.

- [ ] **Step 4: Run API tests and confirm GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/api/test_market_radar_routes.py -q`

## Task 7: Frontend Transport And Persistent Notifications

**Files:**
- Create: `frontend/src/api/marketRadar.ts`
- Create: `frontend/src/stores/marketRadar.ts`
- Create: `frontend/src/stores/marketRadar.test.ts`
- Modify: `frontend/src/stores/notification.ts`
- Modify: `frontend/src/components/NotificationPanel.vue`

- [ ] **Step 1: Write failing store tests**

Mock EventSource and timers. Verify initial REST load, SSE mode/snapshot/alert updates, ID dedupe, 20-second heartbeat timeout, 30-second REST fallback, 5/10/20/40/60 reconnect delays, visibility recovery, and cleanup.

- [ ] **Step 2: Implement typed API and store**

Use native EventSource only for stream transport and the existing request client for REST. Use a request generation token so stale fallback responses cannot overwrite a recovered live snapshot.

- [ ] **Step 3: Extend notifications**

Merge task and market events under separate ID namespaces. Fetch unread high alerts from the backend, persist acknowledge state server-side, and route clicks to `/market-radar?alert=<id>`.

- [ ] **Step 4: Run frontend unit tests and confirm GREEN**

Run: `cd frontend; npm run test -- --run src/stores/marketRadar.test.ts`

## Task 8: Market Radar Workbench

**Files:**
- Create: `frontend/src/views/MarketRadar/index.vue`
- Create: `frontend/src/views/MarketRadar/MarketBreadthChart.vue`
- Create: `frontend/src/views/MarketRadar/IndexTrendChart.vue`
- Create: `frontend/src/views/MarketRadar/LimitLadderTable.vue`
- Create: `frontend/src/views/MarketRadar/CrowdingBreakdown.vue`
- Create: `frontend/src/views/MarketRadar/SectorTemperature.vue`
- Create: `frontend/src/views/MarketRadar/AlertFeed.vue`
- Modify: `frontend/src/lib/echarts.ts`
- Modify: `frontend/src/router/index.ts`
- Modify: `frontend/src/app/navigation.ts`
- Create: `frontend/src/views/MarketRadar/marketRadarView.test.ts`

- [ ] **Step 1: Write failing source-contract tests**

Assert the route/nav item exists, charts expose nonzero stable containers, stale components render dates/reasons, alert actions are buttons, and mobile tables have an accessible compact mode.

- [ ] **Step 2: Build the dense workbench layout**

Use full-width bands: status, pulse strip, breadth/index charts, limit/crowding, sectors, alerts. Reuse existing CSS tokens and ECharts wrapper. Add no nested cards, marketing hero, decorative SVG, or second chart library.

- [ ] **Step 3: Implement chart updates and accessibility**

Update only ECharts series/options on one-second snapshots, throttle resize, dispose on unmount, provide tabular fallback and stale/empty states, and preserve red-up/green-down semantics.

- [ ] **Step 4: Run view tests and build**

Run: `cd frontend; npm run test -- --run src/views/MarketRadar/marketRadarView.test.ts src/app/navigation.test.ts src/router/index.test.ts`

Run: `cd frontend; npm run build`

## Task 9: Daily Scheduling, Documentation, And Desktop Scripts

**Files:**
- Modify: `backend/app/core/scheduler.py`
- Modify: `backend/tests/services/test_sync_service_incremental.py`
- Create: `docs/market-radar.md`
- Modify: `README.md`
- Modify: `docs/user-manual.md`
- Modify: `docs/data-source-cheatsheet.md`
- Modify: `docs/frontend-information-architecture.md`
- Modify: `C:/Users/Albert/Desktop/启动GaoshouPlatform.bat`
- Modify: `C:/Users/Albert/Desktop/关闭GaoshouPlatform.bat`

- [ ] **Step 1: Write failing scheduler tests**

Assert one daily job at 15:20, no API-owned scheduler, same-date partial recomputation, and no overlap with the sync FIFO writer.

- [ ] **Step 2: Implement scheduler registration and cleanup**

Run daily analytics through the dedicated single-worker radar queue, retain daily snapshots indefinitely, delete intraday snapshots older than 90 days, and expose runtime task status.

- [ ] **Step 3: Update user and operator documentation**

Document formulas, labels, default thresholds, freshness, push/poll/offline modes, SSE recovery, QMT optionality, API endpoints, data source dates, and troubleshooting without secrets.

- [ ] **Step 4: Update desktop scripts**

Keep existing ports. Add API radar status to startup health output and orderly API shutdown coverage; missing miniQMT prints a non-blocking realtime-offline notice.

## Task 10: Verification And Runtime Rollout

**Files:**
- Verify all files above; do not stage unrelated dirty worktree changes.

- [ ] **Step 1: Run focused backend suites**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_market_radar_calculator.py tests/services/test_market_radar_store.py tests/services/test_qmt_realtime_feed.py tests/services/test_market_radar_data.py tests/services/test_market_radar_service.py tests/api/test_market_radar_routes.py tests/test_migrations.py -q`

- [ ] **Step 2: Run static checks**

Run: `cd backend; .\.venv\Scripts\python.exe -m ruff check app/services/market_radar_calculator.py app/services/market_radar_data.py app/services/qmt_realtime_feed.py app/services/market_radar.py app/api/market_radar.py app/db/models/market_radar.py tests/services/test_market_radar*.py tests/services/test_qmt_realtime_feed.py tests/api/test_market_radar_routes.py`

Run: `cd backend; .\.venv\Scripts\python.exe -m compileall -q app/services app/api/market_radar.py app/db/models/market_radar.py`

- [ ] **Step 3: Run frontend verification**

Run: `cd frontend; npm run test -- --run`

Run: `cd frontend; npm run build`

- [ ] **Step 4: Start affected services and run smoke tests**

Apply Alembic migration, restart API `8800`, sync service `8810`, and frontend `3511`. Verify health, overview, stream headers/heartbeat, offline daily snapshot, alert actions, and no real-order endpoint calls.

- [ ] **Step 5: Perform visual QA**

Use Playwright screenshots at 1440x900, 1280x720, and 390x844. Confirm charts are nonblank, no overlaps/overflow, stale data is explicit, and alert details/actions work. When miniQMT is available during trading hours, verify end-to-end push latency <=2 seconds and forced disconnect changes to `polling_30s` within 5 seconds; otherwise record push runtime verification as blocked while keeping fake-adapter coverage green.

- [ ] **Step 6: Audit the final diff**

Run `git diff --check` and inspect every touched dirty file against its pre-task diff. Stage only market-radar hunks/files; do not revert or commit unrelated work from other sessions.

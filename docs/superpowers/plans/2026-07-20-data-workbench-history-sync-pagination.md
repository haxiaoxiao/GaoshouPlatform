# Data Workbench History, Sync Scope, and Kline Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the ten genuinely most recently viewed stocks, make daily sync market-wide by default, and lazily load older K-line pages when the chart is dragged left.

**Architecture:** Keep browser-local history in a focused versioned storage module, preserve the existing sync payload contract while changing the UI default scope, and extend the existing paginated K-line endpoint rather than creating a second data path. The chart emits a boundary event and the parent owns request sequencing, deduplication, and pagination state.

**Tech Stack:** Vue 3, TypeScript, Vitest, lightweight-charts, FastAPI, pytest, Parquet/DuckDB.

---

### Task 1: Persist genuine recent stocks

**Files:**
- Create: `frontend/src/views/DataManage/recentStocks.ts`
- Create: `frontend/src/views/DataManage/recentStocks.test.ts`
- Modify: `frontend/src/views/DataManage/index.vue`

- [ ] **Step 1: Write failing storage tests**

Test `loadRecentStocks()` with empty/corrupt storage and `rememberRecentStock()` with duplicates and 11 distinct stocks. Assert no seeded values, most-recent-first ordering, metadata preservation, and a 10-item maximum.

- [ ] **Step 2: Verify RED**

Run: `npm run test -- --run src/views/DataManage/recentStocks.test.ts`

Expected: FAIL because `recentStocks.ts` does not exist.

- [ ] **Step 3: Implement the storage module**

Export:

```ts
export const RECENT_STOCKS_STORAGE_KEY = 'gaoshou:data-workbench:recent-stocks:v1'
export const MAX_RECENT_STOCKS = 10
export function loadRecentStocks(storage: Pick<Storage, 'getItem'> = localStorage): StockOption[]
export function rememberRecentStock(stock: StockOption, current: StockOption[], storage: Pick<Storage, 'setItem'> = localStorage): StockOption[]
```

Validate `symbol` and `name`, normalize optional metadata, deduplicate by symbol, and catch storage exceptions.

- [ ] **Step 4: Replace the hard-coded array**

Initialize `recentStocks` from `loadRecentStocks()`. After explicit selection resolves a stock option, call `rememberRecentStock()` and update the ref. Do not write during `onMounted()`.

- [ ] **Step 5: Verify GREEN**

Run: `npm run test -- --run src/views/DataManage/recentStocks.test.ts`

Expected: PASS.

### Task 2: Default daily updates to the full market

**Files:**
- Modify: `frontend/src/views/DataManage/SyncPanel.vue`
- Modify: `frontend/src/views/platformUpgrade.test.ts`

- [ ] **Step 1: Add a failing source-contract test**

Assert that `stockScope` initializes to `all` and that `basePayload()` only includes parsed symbols when the user explicitly selects `custom`.

- [ ] **Step 2: Verify RED**

Run: `npm run test -- --run src/views/platformUpgrade.test.ts`

Expected: FAIL because the current default is `custom` with `000001.SZ`.

- [ ] **Step 3: Change the default**

Set:

```ts
const stockScope = ref<'custom' | 'all'>('all')
```

Keep the custom input value as a convenience only after the user chooses custom scope.

- [ ] **Step 4: Verify GREEN**

Run the focused test and confirm the default full-market contract passes.

### Task 3: Expose complete K-line pagination metadata

**Files:**
- Modify: `backend/app/api/data.py`
- Create: `backend/tests/api/test_data_kline_pagination.py`
- Modify: `frontend/src/api/kline.ts`

- [ ] **Step 1: Write a failing API test**

Inject a fake `DataService.get_klines()` result with `page=2`, `page_size=250`, `total=479`, and `total_pages=2`. Assert the JSON response exposes all four fields alongside `items`.

- [ ] **Step 2: Verify RED**

Run: `.\.venv\Scripts\python.exe -m pytest tests\api\test_data_kline_pagination.py -q`

Expected: FAIL because the route currently returns only `items` and `total`.

- [ ] **Step 3: Return pagination metadata**

Add `page`, `page_size`, and `total_pages` from the service result without changing data serialization.

- [ ] **Step 4: Update the TypeScript contract**

Add pagination parameters to `KlineParams` and define a `KlinePage` response interface with `items`, `total`, `page`, `page_size`, and `total_pages`.

- [ ] **Step 5: Verify GREEN**

Run the focused backend test and frontend typecheck through the later build step.

### Task 4: Lazily load older bars from the chart boundary

**Files:**
- Modify: `frontend/src/views/DataManage/KlineChart.vue`
- Modify: `frontend/src/views/DataManage/index.vue`
- Create: `frontend/src/views/DataManage/klinePagination.ts`
- Create: `frontend/src/views/DataManage/klinePagination.test.ts`

- [ ] **Step 1: Write failing pagination helper tests**

Test deterministic merge/deduplication of overlapping pages and state transitions for reset, next-page availability, in-flight suppression, and stale request versions.

- [ ] **Step 2: Verify RED**

Run: `npm run test -- --run src/views/DataManage/klinePagination.test.ts`

Expected: FAIL because the helper does not exist.

- [ ] **Step 3: Implement the pure pagination helper**

Export helpers that merge rows by `datetime`, sort newest-first for the page, and compute whether another page exists from `page < total_pages`.

- [ ] **Step 4: Emit the chart boundary event**

Declare `request-older`, subscribe with `chart.timeScale().subscribeVisibleLogicalRangeChange()`, and emit when `range.from <= 10`. Unsubscribe on unmount and suppress repeated emissions until the visible range moves away or new data arrives.

- [ ] **Step 5: Add parent pagination state**

On a fresh query, reset page/version and request page 1 with `page_size=250` for daily or `1000` for minute. On `request-older`, request `page + 1`, ignore stale responses, merge overlaps, and preserve current rows on failure.

- [ ] **Step 6: Verify GREEN**

Run both new frontend test files plus `src/views/platformUpgrade.test.ts`.

### Task 5: Runtime verification and data repair

**Files:**
- Modify if needed: `docs/user-manual.md`

- [ ] **Step 1: Run focused suites**

Run the new backend pagination test, synchronization tests affected by the default contract, and all new frontend tests.

- [ ] **Step 2: Run complete validation**

Run backend pytest, Ruff, compileall, frontend Vitest, and `npm run build`.

- [ ] **Step 3: Restart affected services**

Restart backend and frontend using the prod desktop scripts, then verify ports 8800 and 3511.

- [ ] **Step 4: Repair current Moutai data**

Submit an all-market incremental `kline_daily` sync through the existing FIFO sync API. Wait for terminal status, then verify `600519.SH` latest Parquet and API dates match QMT's latest available trading day.

- [ ] **Step 5: Browser smoke**

Open `/data`, search two different stocks, reload and verify order persists. Select a two-year Moutai range, drag the chart left, and verify row count and earliest loaded date expand without duplicates or console errors.

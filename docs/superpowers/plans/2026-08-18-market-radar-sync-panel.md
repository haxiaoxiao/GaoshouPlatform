# Market Radar Sync Panel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose and execute the three Parquet datasets required by Market Radar from the `/data/sync` panel.

**Architecture:** Register `tushare_limit_list_d`, `tushare_limit_step`, and `tushare_margin` as date-scoped Tushare Relay datasets, group them in a dedicated preset, and route each trading date through a shared Relay request helper. Normalize source dates to both the public `trade_date` field and Market Radar's `trade_date_dt` field before writing through the existing Parquet store.

**Tech Stack:** FastAPI/Python, pandas, DuckDB/Parquet, pytest, Vue 3 + TypeScript, Vitest/Vite.

---

### Task 1: Add failing backend coverage for the Market Radar catalog and normalization

**Files:**
- Modify: `backend/tests/services/test_tushare_relay.py`

- [ ] **Step 1: Write tests for dataset registration and preset membership**

Assert that all three dataset names are in the catalog, use the expected storage names and date columns, and that the `market_radar` preset contains exactly the three datasets.

- [ ] **Step 2: Write a normalization test for Market Radar date aliases**

Pass representative limit and margin rows to `_normalize_dataset_rows()` and assert `trade_date` and `trade_date_dt` are parsed timestamps, symbols are preserved, and numeric values such as `rzye`/`nums` remain numeric.

- [ ] **Step 3: Run the focused tests and verify the expected failure**

Run `backend\\.venv\\Scripts\\python.exe -m pytest tests/services/test_tushare_relay.py -q`.
Expected: failures because the new dataset constants, preset, and normalization branch do not exist yet.

### Task 2: Add failing backend coverage for per-date Relay requests and progress estimates

**Files:**
- Modify: `backend/tests/services/test_tushare_relay.py`

- [ ] **Step 1: Add a fake Relay client and progress object test**

Exercise the new date-scoped helper with one date and assert it calls the correct API name and `trade_date=YYYYMMDD`, returns rows with the requested date, and advances progress once per date.

- [ ] **Step 2: Add progress estimate assertions**

Assert `_estimate_total()` returns one unit per date for each of the three date-scoped Market Radar datasets.

- [ ] **Step 3: Run only the new tests and verify they fail for missing behavior**

Run `backend\\.venv\\Scripts\\python.exe -m pytest tests/services/test_tushare_relay.py -q` and confirm the failures point to missing handler/estimate behavior rather than test setup errors.

### Task 3: Implement Relay specs, preset, handlers, and normalization

**Files:**
- Modify: `backend/app/services/tushare_relay_specs.py`
- Modify: `backend/app/services/tushare_relay_sync.py`
- Test: `backend/tests/services/test_tushare_relay.py`

- [ ] **Step 1: Register the three specs and a `market_radar` dataset tuple**

Use API names `limit_list_d`, `limit_step`, and `margin`; store to the existing Parquet dataset names; set `trade_date` as the source date column; mark them date-scoped, Relay-key required, and medium risk.

- [ ] **Step 2: Add the Market Radar preset to `build_sync_catalog()`**

Expose display name `市场雷达数据`, description covering limit detail, limit ladder, and margin balance, and the three Relay dataset names in deterministic order.

- [ ] **Step 3: Add a shared date-scoped handler**

For each date, call the spec API with `trade_date=YYYYMMDD`; attach `trade_date` to rows when the relay response omits it; collect request metadata and increment progress once per date. Route the three dataset names through this handler.

- [ ] **Step 4: Update `_estimate_total()`**

Count one unit per requested date for each Market Radar dataset.

- [ ] **Step 5: Normalize Market Radar rows**

Parse `trade_date`, create `trade_date_dt` for Market Radar readers, preserve `ts_code`/`symbol` aliases, and run common numeric coercion. For margin rows preserve `exchange_id` and `rzye`; for limit rows preserve `limit`, `limit_times`, and `nums`.

- [ ] **Step 6: Run focused backend tests until green**

Run `backend\\.venv\\Scripts\\python.exe -m pytest tests/services/test_tushare_relay.py -q`.

### Task 4: Wire the frontend catalog filter and documentation

**Files:**
- Modify: `frontend/src/views/DataManage/SyncPanel.vue`
- Modify: `docs/market-radar.md`

- [ ] **Step 1: Add a visible catalog category option for Market Radar**

Add `relay_market_radar` to the category filter and map it to a readable label so users can find the three datasets even when the keyword filter is empty.

- [ ] **Step 2: Document the sync path**

Add a short section explaining that users should open `/data/sync`, choose the `市场雷达数据` preset, select the date range, run the Relay sync, and then use `/market-radar` refresh to recompute snapshots.

- [ ] **Step 3: Run frontend tests and type/build checks**

Run `cd frontend; npm run test -- --run; npm run build`.

### Task 5: Run regression checks and inspect the final diff

**Files:**
- Modify: none beyond the files above.

- [ ] **Step 1: Run Relay and Market Radar backend tests**

Run `backend\\.venv\\Scripts\\python.exe -m pytest tests/services/test_tushare_relay.py tests/services/test_market_radar_data.py tests/api/test_market_radar_routes.py -q`.

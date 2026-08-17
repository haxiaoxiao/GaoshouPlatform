# Factor Coverage Memory Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prevent opening `/factor` from launching an unbounded Parquet coverage scan that grows the backend Python process to multiple gigabytes.

**Architecture:** Keep bounded date-range coverage as the normal UI path. Add a backend guard so filtered `full_range=true` requests reuse the bounded `coverage()` query instead of calling the unbounded `coverage_many()` path. Preserve manifest-backed full-range behavior for unfiltered requests.

**Tech Stack:** Vue 3 + TypeScript, FastAPI, DuckDB/Parquet, pytest.

---

### Task 1: Add the backend guard regression test

**Files:**
- Modify: `backend/tests/api/test_factor_values_paper.py` or create a focused test in `backend/tests/api/test_factor_values_coverage.py`
- Test: backend API coverage behavior

- [x] **Step 1: Write a failing test**

Add a test that calls the coverage route with `full_range=True`, an index-resolved symbol list, and a parameterized factor, then asserts the store's bounded `coverage()` method is used and `coverage_many()` is not used.

- [x] **Step 2: Run the focused test**

Run `backend\\.venv\\Scripts\\python.exe -m pytest backend\\tests\\api\\test_factor_values_coverage.py -q` and confirm it fails because the route currently calls `coverage_many()` for every `full_range` request.

### Task 2: Implement bounded fallback in the API

**Files:**
- Modify: `backend/app/api/factor_values.py:283-328`

- [x] **Step 1: Add the decision in the route**

Use the bounded `store.coverage(...)` call whenever `full_range` is requested together with a resolved symbol list or non-`None` factor parameters. Keep `coverage_many(...)` only for unfiltered full-range calls so a valid manifest can still answer them cheaply.

- [x] **Step 2: Run the focused test**

Run the same focused pytest command and confirm it passes.

### Task 3: Stop the frontend from requesting unbounded coverage on page load

**Files:**
- Modify: `frontend/src/views/FactorResearch/FactorValueStore.vue:838-850`

- [x] **Step 1: Change automatic coverage refresh**

Change the automatic `loadCoverage()` request from `full_range: true` to `full_range: false`, leaving explicit precompute recovery behavior unchanged.

- [x] **Step 2: Run the frontend build**

Run `cd frontend; npm run build` and confirm the build completes successfully.

### Task 4: Verify runtime behavior

**Files:**
- No source changes.

- [x] **Step 1: Restart prod services**

Run `C:\Users\Albert\\Desktop\\关闭GaoshouPlatform.bat --no-pause`, then `C:\Users\Albert\\Desktop\\启动GaoshouPlatform.bat --no-pause` with optional checks/browser disabled.

- [x] **Step 2: Verify health endpoints**

Check `http://127.0.0.1:8800/health`, `http://127.0.0.1:8810/health`, and `http://127.0.0.1:3511/` all return HTTP 200.

- [x] **Step 3: Verify memory behavior**

Open `/factor`, sample the backend process working set for at least 30 seconds, and confirm it does not continue the previous unbounded growth pattern.

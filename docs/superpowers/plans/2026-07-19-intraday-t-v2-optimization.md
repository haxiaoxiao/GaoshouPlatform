# Intraday T v2 Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the loss-prone v1 entry and recovery behavior with a conservative, evidence-backed v2 gate set, exact limit-price integration, persistent cross-day restoration, and walk-forward verification.

**Architecture:** Keep the pure signal additions in `intraday_t_strategy.py`, keep fill/state continuity in `intraday_t_backtest.py`, and load point-in-time limit prices at the API boundary. Reuse the same `StrategyParams` and feature snapshot in paper trading, then expose only the stable v2 controls through the typed frontend.

**Tech Stack:** Python 3.12, pandas, FastAPI, SQLAlchemy async, SQLite, Vue 3, TypeScript, pytest, Vitest.

---

### Task 1: v2 Pure Signal Gates

**Files:**
- Modify: `backend/tests/services/test_intraday_t_strategy.py`
- Modify: `backend/app/services/intraday_t_strategy.py`

- [x] Add failing tests proving that entries reject `abs(zscore) >= max_entry_z`, reject realized volatility below the configured minimum, accept only the right-open 10:00-10:30 window by default, and validate `entry_z < max_entry_z < stop_z`.
- [x] Run the focused test and confirm assertion failures are caused by the missing v2 fields and gates.
- [x] Add `max_entry_z`, `realized_vol_window`, `min_realized_vol_bps`, entry-window parameters, optional adverse-day move, and the corresponding `MarketSnapshot` fields.
- [x] Compute causal `realized_vol_bps`, `session_return_bps`, and `previous_price`; apply common gates before direction selection without changing restoration priority.
- [x] Re-run the focused tests and keep the existing T+1, fee, cooldown, stop, lunch, and force-restore cases green.

### Task 2: Exact Limit Prices and Cross-Day Recovery

**Files:**
- Modify: `backend/tests/services/test_intraday_t_backtest.py`
- Modify: `backend/app/services/intraday_t_backtest.py`

- [x] Add a failing regression fixture for the 603629-style limit-up reverse-T signal and assert no entry is created at either daily price limit.
- [x] Add a failing two-day fixture where restoration is rejected on day one and assert the active pair is restored before any new entry on day two.
- [x] Attach exact daily limits to snapshots, reject new entries at either boundary, and preserve active state plus pair tracker across grouped trade days.
- [x] Track same-day restoration failures as unique pair IDs and report final unresolved pairs separately.
- [x] Re-run focused strategy and backtest tests.

### Task 3: API Limit-Price Loader and Contract

**Files:**
- Modify: `backend/tests/api/test_intraday_t.py`
- Modify: `backend/app/api/intraday_t.py`

- [x] Add a dependency-backed fake limit-price loader to API tests and assert its symbol/date map reaches `BacktestConfig`.
- [x] Add validation tests for the new v2 parameters and capability defaults.
- [x] Implement a batched, parameterized `stock_limit_prices` query and map rows to `symbol|trade_date -> {up, down}` without free-form SQL input.
- [x] Pass the map into the backtest and expose v2 defaults/risk-control metadata.
- [x] Re-run API tests and inspect OpenAPI for the absence of any submit route.

### Task 4: Paper Snapshot Compatibility

**Files:**
- Modify: `backend/tests/services/test_intraday_t_paper.py`
- Modify: `backend/app/services/intraday_t_paper.py`

- [x] Add a failing test that paper evaluation constructs the new v2 snapshot fields from causal minute bars and persists the expanded parameter set.
- [x] Update feature-to-snapshot conversion and parameter restoration without adding any real-order method.
- [x] Re-run paper tests, including restart recovery and same-minute idempotency.

### Task 5: Typed Frontend Controls

**Files:**
- Modify: `frontend/src/api/intradayT.ts`
- Modify: `frontend/src/api/intradayT.test.ts`
- Modify: `frontend/src/views/IntradayT/index.vue`
- Modify: `frontend/src/views/IntradayT/IntradayTPage.test.ts`

- [x] Add failing type/client and component assertions for `max_entry_z`, `realized_vol_window`, and `min_realized_vol_bps` payloads plus the v2 risk summary.
- [x] Extend strict TypeScript contracts and the strategy payload without `any` casts.
- [x] Add compact numeric controls and a non-promotional v2 status summary; preserve responsive layout and simulated-only behavior.
- [x] Re-run focused Vitest suites and production build.

### Task 6: Walk-Forward and Stress Verification

**Files:**
- Create: `backend/app/scripts/research_intraday_t_v2.py`
- Create: `backend/tests/scripts/test_research_intraday_t_v2.py`

- [x] Add failing tests for chronological fold creation, no train/test overlap, ablation ordering, and cost/participation scenarios.
- [x] Implement a read-only research runner that loads local minute data through `get_market_data_store()`, executes fixed ablations, and writes JSON/CSV artifacts under `.runtime/intraday-t-v2-research/`.
- [x] Run at least a two-year window with chronological folds and retain a final holdout; report sample coverage explicitly.
- [x] Run nominal, 5bp, and 10bp per-side slippage plus 2.5%/5% participation scenarios.
- [x] Do not auto-promote any parameter set; emit a recommendation based on fold consistency and zero unresolved final pairs.

### Task 7: Documentation, Research Memory, and Final Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/user-manual.md`
- Create or update: `E:/Projects/data/BaiduSyncdisk/TheLandsBetween/wiki/04 主题/策略研究/2026-07-19 - 日内做T v2优化研究.md`

- [x] Document v2 defaults, current evidence, data-gap warning, simulation-only boundary, and how to run the research script.
- [x] Record promoted, experimental, and rejected indicators plus exact artifact paths in the vault note.
- [x] Run all intraday-T backend tests, relevant API tests, Ruff, all intraday-T frontend tests, and `npm run build`.
- [x] Run the research script fresh and compare v1/v2, walk-forward folds, symbols, directions, exit reasons, and stress cases.
- [x] Restart only affected prod backend/frontend modules, verify health/capabilities/OpenAPI, and perform desktop/mobile browser QA with no real-submit control.
- [x] Reconcile every checkbox and report negative or inconclusive results without promoting an overfit configuration.

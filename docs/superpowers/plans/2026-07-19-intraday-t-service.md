# Intraday T Strategy Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a conservative A-share intraday T workbench for `603629.SH` and `688008.SH`, including minute backtesting, persistent simulated trading, APIs, and a production-quality frontend without exposing real-order submission.

**Architecture:** Keep signal and state-machine logic pure in `intraday_t_strategy.py`, run it over local Parquet minute bars in `intraday_t_backtest.py`, and persist simulated sessions/trades through a separate `intraday_t_paper.py` service. A dedicated FastAPI router owns the contract, while the Vue page consumes strict TypeScript models and remains isolated from the existing live-trading order path.

**Tech Stack:** Python 3.12, pandas, FastAPI, Pydantic, SQLAlchemy async, SQLite, DuckDB/Parquet market store, Vue 3, TypeScript, Element Plus, ECharts, Vitest, pytest.

---

## Task 1: Pure Strategy Domain

**Files:**
- Create: `backend/app/services/intraday_t_strategy.py`
- Create: `backend/tests/services/test_intraday_t_strategy.py`

- [x] Write failing tests for supported symbols, session windows, lot normalization, positive-T and reverse-T state transitions, T+1 sellable inventory, cooldown, trend rejection, fee-aware edge threshold, and forced restoration.
- [x] Run `backend\.venv\Scripts\python.exe -m pytest backend\tests\services\test_intraday_t_strategy.py -q` and confirm collection or assertion failures.
- [x] Implement enums/dataclasses, feature calculation, exchange lot rules, cost estimator, entry/exit decisions, and fill-driven state transitions.
- [x] Re-run the focused test and confirm all cases pass.

## Task 2: Minute Backtest

**Files:**
- Create: `backend/app/services/intraday_t_backtest.py`
- Create: `backend/tests/services/test_intraday_t_backtest.py`

- [x] Write failing fixture-driven tests for next-bar execution, slippage/commission/stamp/transfer fees, volume caps, price-limit rejection, passive-holding baseline, incremental P&L, pair completion, and end-of-day restoration.
- [x] Run the focused test and confirm it fails for the missing backtester.
- [x] Implement symbol/day replay, position sizing, fill accounting, metrics, equity curve, daily summaries, trade ledger, and JSON-safe serialization.
- [x] Re-run unit tests and run a local-data smoke backtest for both target symbols over a bounded period.

## Task 3: Persistent Simulated Trading

**Files:**
- Create: `backend/app/db/models/intraday_t.py`
- Modify: `backend/app/db/models/__init__.py`
- Create: `backend/app/services/intraday_t_paper.py`
- Create: `backend/tests/services/test_intraday_t_paper.py`

- [x] Write failing async tests for session creation, manual and QMT-derived opening positions, minute idempotency, simulated fills, restart recovery, stop/reset, and persisted trade history.
- [x] Run the focused test and confirm it fails before implementation.
- [x] Add isolated session/trade tables and export them through the model package.
- [x] Implement the paper service using async SQLAlchemy and the existing QMT gateway through non-blocking wrappers; never call a real submit method.
- [x] Re-run the focused test and database initialization smoke check.

## Task 4: FastAPI Contract

**Files:**
- Create: `backend/app/api/intraday_t.py`
- Modify: `backend/app/api/router.py`
- Create: `backend/tests/api/test_intraday_t.py`

- [x] Write failing API tests for capabilities, data coverage, backtest validation/results, paper start/status/evaluate/stop/reset, trade listing, fixed-symbol enforcement, and absence of order-submission routes.
- [x] Run `backend\.venv\Scripts\python.exe -m pytest backend\tests\api\test_intraday_t.py -q` and confirm failures.
- [x] Implement Pydantic request/response contracts and `/api/intraday-t` routes, wrapping CPU/blocking work with the project helper.
- [x] Register the router after re-reading the dirty shared router file.
- [x] Re-run API tests and OpenAPI smoke checks.

## Task 5: Typed Frontend Client and View Model

**Files:**
- Create: `frontend/src/api/intradayT.ts`
- Create: `frontend/src/api/intradayT.test.ts`
- Create: `frontend/src/views/IntradayT/model.ts`
- Create: `frontend/src/views/IntradayT/model.test.ts`

- [x] Write failing Vitest cases for request payloads, response normalization, metrics formatting, trade-side labels, and equity-series conversion.
- [x] Run `npm run test -- src/api/intradayT.test.ts src/views/IntradayT/model.test.ts` from `frontend` and confirm failures.
- [x] Implement strict API types/client functions and pure presentation helpers without `any` casts.
- [x] Re-run the focused Vitest suite.

## Task 6: Intraday T Workbench Page

**Files:**
- Create: `frontend/src/views/IntradayT/index.vue`
- Modify: `frontend/src/router/index.ts`
- Modify: `frontend/src/app/navigation.ts`
- Create: `frontend/src/views/IntradayT/IntradayTPage.test.ts`

- [x] Write a failing component test covering initial capabilities load, backtest submission, result rendering, paper controls, loading/error states, and disabled real-order behavior.
- [x] Implement a responsive three-band workbench: strategy controls, backtest analytics/ledger, and persistent simulated-session console.
- [x] Use existing Ivory & Pine tokens, restrained charts, accessible controls, stable responsive dimensions, and no nested decorative cards.
- [x] Re-read and minimally append the shared router/navigation files, then run component tests and `npm run build`.

## Task 7: Documentation and Full Verification

**Files:**
- Modify: `docs/user-manual.md`
- Modify: `README.md`

- [x] Add a concise operating guide, risk boundaries, two-symbol scope, simulated-only guarantee, parameter meanings, and recovery workflow.
- [x] Run all new backend tests together, all new frontend tests together, backend syntax/import checks, and the frontend production build.
- [x] Start or restart only the prod backend and frontend through the existing desktop scripts; do not change sync/feed process ownership.
- [x] Check `/api/system/health`, `/api/intraday-t/capabilities`, `/api/intraday-t/coverage`, and the frontend route on prod ports `8800` and `3511`.
- [x] Run desktop and mobile browser QA, inspect console/network errors, exercise a bounded backtest and paper-session lifecycle, and retain screenshots as verification artifacts.
- [x] Reconcile every checklist item and report exact commands/results plus any residual data/QMT limitations.

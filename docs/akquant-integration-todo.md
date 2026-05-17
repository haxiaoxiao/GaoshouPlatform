# AKQuant Integration Todo

This note tracks AKQuant integration work that is independent from the current
ID=43 data calibration.

Last updated: 2026-05-15.

## Current Capability Snapshot

Local AKQuant version: `0.2.26`.

Verified available capabilities:

| Capability | Status | Platform entry |
|---|---:|---|
| Event-driven backtest | Done | `POST /api/v2/backtest/run` with `engine="akquant"` |
| Runtime capability detection | Done | `GET /api/v2/backtest/capabilities` |
| Explicit slippage policy mapping | Done | numeric slippage -> `{type: "percent", value}` |
| ClickHouse daily feed | Done | `ClickHouseFeedAdapter` / `ClickHouseDataProvider` |
| ClickHouse full minute feed | Done | lazy symbol loading |
| ClickHouse sparse timer minute feed | Done | `bar_type="minute_timer"` with timer time filtering |
| AKQuant Grid Search | Done | `POST /api/v2/backtest/optimize/grid` |
| AKQuant Walk-forward Validation | Done | `POST /api/v2/backtest/optimize/walk-forward` |
| AKQuant Polars factor engine | Done | `POST /api/v2/compute/evaluate`, `engine="akquant"` |
| Strategy id resolution for optimization | Done | Grid/Walk-forward accept `strategy_id` |
| Index universe resolution for optimization | Done | Grid/Walk-forward accept `index_symbol` |
| Strategy parameter schema | Done | `POST /api/v2/backtest/strategy-params/schema` |
| Strategy parameter validation | Done | `POST /api/v2/backtest/strategy-params/validate` |
| Frontend API typing/build alignment | Done | `frontend/src/api/backtest.ts` and backtest views |

## Completed in the Latest Pass

- Added AKQuant strategy parameter discovery and validation endpoints:
  - `POST /api/v2/backtest/strategy-params/schema`
  - `POST /api/v2/backtest/strategy-params/validate`
- Added AKQuant optimization service coverage for:
  - Grid Search
  - Walk-forward Validation
  - `strategy_id` resolution
  - `index_symbol` universe resolution
  - `minute_timer` sparse minute data loading
- Routed `minute_timer` reads through ClickHouse with explicit timer-time filtering instead of loading full minute bars when the strategy only needs fixed intraday timestamps.
- Kept strategy runtime parameters controlled by the backtest request/control panel. Dates, universe, initial capital, fees, slippage, bar type, and timer times should come from the request payload and `strategy_params`, not from hardcoded strategy constants.
- Fixed frontend TypeScript/build drift around backtest result types, report views, stock detail K-line display, explorer API unwrapping, duplicate API exports, and unused symbols.
- Added focused backend tests in `backend/tests/backtest/test_akquant_integration.py`.

Validation commands:

```powershell
cd E:\Projects\GaoshouPlatform\frontend
npm run build
```

Result: build passed. Remaining warning: Vite reports several large chunks over 500 KB; this is a packaging/performance warning, not a build failure.

```powershell
cd E:\Projects\GaoshouPlatform\backend
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\.venv\Scripts\python.exe -m pytest tests\backtest\test_akquant_integration.py -q
```

Result: `7 passed`. Remaining warning: Pydantic class-based config deprecation.

## P0

- Keep `engine="akquant"` as the primary event-driven path for strategy backtests.
- Keep all backtest parameters controlled by the right-side control panel and API request payload. Strategy scripts should read `strategy_params`; do not hardcode dates, universe, capital, timer times, or fees.
- Make sparse timer minute data the default for strategies that only need fixed intraday timestamps. Avoid full minute data unless the strategy needs continuous intraday state.
- For optimization, use the same data mode as the backtest: `daily`, `minute`, or `minute_timer`.
- Add smoke tests around:
  - `GET /api/v2/backtest/capabilities` - done in `tests/backtest/test_akquant_integration.py`
  - `POST /api/v2/backtest/optimize/grid` route registration - done
  - `POST /api/v2/backtest/optimize/walk-forward` route registration - done
  - `minute_timer` optimization data loading - done
  - `POST /api/v2/compute/evaluate` with `engine="akquant"` - pending API-level test

P0 status: core API and backend wiring are complete. Remaining P0 item is API-level coverage for AKQuant compute evaluation.

## P1

- Expose Grid Search and Walk-forward controls in the frontend backtest panel:
  - parameter grid editor
  - metric selector
  - train/test period inputs
  - task progress and result table
- Build frontend controls on top of AKQuant's strategy parameter schema APIs.
- Add TA-Lib backend selection to factor/indicator UI:
  - builtin Python backend
  - AKQuant/Rust-compatible backend
  - availability and function count from `/capabilities`
- Persist optimization results to SQLite with task id, strategy id, universe, date range, metric, rows, and best parameter set.
- Add frontend controls that consume `strategy-params/schema` and validate before submission:
  - dynamic form generation for strategy parameters
  - typed timer-time input for `minute_timer`
  - validation error display from `strategy-params/validate`

## P2

- Feature Store baseline is now implemented for factor research:
  - generic Parquet dataset: `feature_values`
  - API: `GET /api/v2/features/definitions`, `GET /api/v2/features/groups`, `GET /api/v2/features/coverage`, `GET /api/v2/features/preview`, `POST /api/v2/features/precompute`, `POST /api/v2/features/groups/precompute`, `POST /api/v2/features/query`
  - frontend: Factor Research -> `Feature Store`, with feature group selection, coverage, precompute, and cross-section preview
  - initial built-ins: `cum_volume_at_time`, `max_volume_nd`, `high_volume_ratio`, `high_volume_signal`
  - ID=43 feature group: `small_cap_v4_core`
    - precomputed today: market-cap/rank, ST flag, pause flag, limit-up/down flags, yesterday-limit-up flag, and 14:30 high-volume features
    - registered for follow-up calculator: V4GV, V4GV21, MACD positive signal, combined buy signal, V4GV dead-cross sell signal
  - ID=43 reads `high_volume_signal` when `high_volume_mode="minute_to_now"` and falls back to in-strategy computation when values are missing.
- Remaining: integrate Redis cache for high-frequency repeated reads:
  - index constituents by `(index_symbol, as_of_date)`
  - timer coverage summary by `(index_symbol/symbol set, start, end, timer_times)`
  - factor expression results by `(engine, expression_hash, symbols_hash, start, end)`
  - strategy indicator arrays for ID=43-style repeated yearly debugging
- Add cache invalidation hooks after ClickHouse data sync.
- Add batch factor precompute jobs for AKQuant Polars expressions and store results in `factor_cache`.
- Add a unified comparison report for AKQuant vs JQ/RQAlpha semantics:
  - fill timing
  - limit-up/limit-down handling
  - suspension handling
  - ST and delist filtering
  - industry concentration rules
- Add one-click yearly debug mode for ID=43:
  - run each calendar year independently
  - export holdings/orders/log summary
  - compare with JQ logs by rebalance date and selected universe

## P3

- Add ML workflow entry points:
  - Walk-forward training adapter
  - sklearn/PyTorch model registry
  - prediction cache in ClickHouse
- Add live-trading bridge planning for AKQuant live runner while keeping miniQMT as the execution gateway.
- Add multi-asset instrument templates with AKQuant `InstrumentConfig` for stocks, ETFs, futures, and options.
- Add richer risk configuration UI for AKQuant `RiskConfig` and portfolio-level constraints.

## Operational Notes

- `minute_timer` optimization now loads only requested timer minutes from ClickHouse via `ClickHouseDataProvider.load_minute(..., timer_times=...)`.
- Backtest, Grid Search, and Walk-forward now share config preparation, so `strategy_id` and `index_symbol` behave consistently.
- Data calibration for ID=43 remains separate: it depends on miniQMT/ClickHouse minute coverage and should resume after manual downloads finish.
- For ID=43-style strategies, prefer:
  - `bar_type="minute_timer"` when intraday execution/reference prices are needed only at fixed timestamps.
  - `bar_type="daily"` when the strategy does not depend on intraday state.
  - full `bar_type="minute"` only when continuous intraday logic is required.
- AKQuant daily bars can only fill on the next bar under the current integration semantics. Do not use current-day close as a proxy execution price for this strategy family.

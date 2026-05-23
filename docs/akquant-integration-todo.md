# AKQuant Integration Todo

This note tracks AKQuant integration work that is independent from the current
ID=43 data calibration.

Last updated: 2026-05-19.

## Current Capability Snapshot

Local AKQuant version: `0.2.26`.

Verified available capabilities:

| Capability | Status | Platform entry |
|---|---:|---|
| Event-driven backtest | Done | `POST /api/backtest/run` with `engine="akquant"` |
| Runtime capability detection | Done | `GET /api/backtest/capabilities` |
| Explicit slippage policy mapping | Done | numeric slippage -> `{type: "percent", value}` |
| ClickHouse daily feed | Done | `ClickHouseFeedAdapter` / `ClickHouseDataProvider` |
| ClickHouse full minute feed | Done | lazy symbol loading |
| ClickHouse sparse timer minute feed | Done | `bar_type="minute_timer"` with timer time filtering |
| AKQuant Grid Search | Done | `POST /api/backtest/optimize/grid` |
| AKQuant Walk-forward Validation | Done | `POST /api/backtest/optimize/walk-forward` |
| AKQuant Polars factor engine | Done | `POST /api/compute/evaluate`, `engine="akquant"` |
| Strategy id resolution for optimization | Done | Grid/Walk-forward accept `strategy_id` |
| Index universe resolution for optimization | Done | Grid/Walk-forward accept `index_symbol` |
| Strategy parameter schema | Done | `POST /api/backtest/strategy-params/schema` |
| Strategy parameter validation | Done | `POST /api/backtest/strategy-params/validate` |
| Frontend API typing/build alignment | Done | `frontend/src/api/backtest.ts` and backtest views |
| Optimization persistence | Done | Grid/Walk-forward task summaries persisted in `backtests` as `record_type="optimization"` |
| Factor expression precompute | Done | `POST /api/compute/precompute`, supports `engine="builtin"` and `engine="akquant"` |
| Redis cache status | Done | `GET /api/system/cache` |

## Completed in the Latest Pass

- Added AKQuant strategy parameter discovery and validation endpoints:
  - `POST /api/backtest/strategy-params/schema`
  - `POST /api/backtest/strategy-params/validate`
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
- Added API-level AKQuant compute coverage for `POST /api/compute/evaluate`.
- Added a compact AKQuant optimization panel in the backtest runner:
  - JSON parameter grid
  - metric selector
  - Grid Search button
  - Walk-forward button
  - first-result table
- Persisted AKQuant Grid Search and Walk-forward summaries into the existing `backtests` table.
- Added expression precompute API for persistent `factor_cache` writes.
- Added cache status API for Redis/backtest/compute cache diagnostics.

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
  - `GET /api/backtest/capabilities` - done in `tests/backtest/test_akquant_integration.py`
  - `POST /api/backtest/optimize/grid` route registration - done
  - `POST /api/backtest/optimize/walk-forward` route registration - done
  - `minute_timer` optimization data loading - done
  - `POST /api/compute/evaluate` with `engine="akquant"` - done in `tests/api/test_factor_research_routes.py`

P0 status: complete for the currently scoped AKQuant integration.

## P1

- Expose Grid Search and Walk-forward controls in the frontend backtest panel - done:
  - parameter grid JSON editor
  - metric selector
  - train/test period inputs
  - shared task progress polling
  - result table preview
- Build frontend controls on top of AKQuant's strategy parameter schema APIs - partial:
  - schema/validate APIs are available
  - generic dynamic form remains future UI polish
- Add TA-Lib backend selection to factor/indicator UI:
  - builtin Python backend
  - AKQuant/Rust-compatible backend
  - availability and function count from `/capabilities`
- Persist optimization results to SQLite with task id, strategy id, universe, date range, metric, rows, and best parameter set - done as `backtests.parameters/result` with `record_type="optimization"`.
- Add frontend controls that consume `strategy-params/schema` and validate before submission:
  - dynamic form generation for strategy parameters
  - typed timer-time input for `minute_timer`
  - validation error display from `strategy-params/validate`

## P2

- Factor Value Store baseline is implemented for factor research:
  - generic Parquet dataset: `factor_values`
  - API: `GET /api/factor-values/definitions`, `GET /api/factor-values/groups`, `GET /api/factor-values/coverage`, `GET /api/factor-values/preview`, `POST /api/factor-values/precompute`, `POST /api/factor-values/groups/precompute`, `POST /api/factor-values/query`
  - frontend: 因子值缓存 with factor group selection, coverage, precompute, and cross-section preview
  - initial built-ins: `cum_volume_at_time`, `rolling_max_volume`, `high_volume_ratio`, `high_volume_signal`
  - ID=43 factor group: `small_cap_v4_core`
    - precomputed today: market-cap/rank, ST flag, pause flag, limit-up/down flags, yesterday-limit-up flag, and 14:30 high-volume features
    - registered for follow-up calculator: V4GV, V4GV21, MACD positive signal, combined buy signal, V4GV dead-cross sell signal
  - ID=43 reads `high_volume_signal` when `high_volume_mode="minute_to_now"` and falls back to in-strategy computation when values are missing.
- Remaining: integrate Redis cache for high-frequency repeated reads:
  - index constituents by `(index_symbol, as_of_date)` - done in ID=43 runtime path
  - timer coverage summary by `(index_symbol/symbol set, start, end, timer_times)` - done via Redis-backed `find_earliest_timer_coverage_date`
  - factor expression results by `(engine, expression_hash, symbols_hash, start, end)` - L1/L1.5/L2 cache path exists; explicit range-level Redis key pending
  - strategy indicator arrays for ID=43-style repeated yearly debugging - partial via daily-window/daily-basic Redis cache
- Add cache invalidation hooks after ClickHouse data sync - done for data sync background tasks.
- Add batch factor precompute jobs for AKQuant Polars expressions and store results in `factor_cache` - done via `POST /api/compute/precompute`.
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
  - API trigger/status - done via `POST /api/v2/small-cap-debug/yearly` and `GET /api/v2/small-cap-debug/yearly/{task_id}`

P2 status: baseline factor store, ID=43 reuse, Redis diagnostics, expression precompute, sync invalidation hooks, timer-coverage Redis cache, and yearly debug API are complete. Remaining P2 polish is explicit range-level Redis keys for arbitrary factor expression results and a frontend screen for yearly debug comparison.

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


# Feature Store

This document tracks the generic factor-research feature layer. The goal is to
make reusable point-in-time data available to factor analysis, stock screening,
and strategy backtests without embedding one strategy's intermediate variables
inside strategy code.

## Storage

Feature values are stored in Parquet:

```text
data/parquet/feature_values/year=YYYY/month=MM/part-*.parquet
```

Logical key:

```text
symbol, trade_date, as_of_time, feature_name, params_hash
```

Columns:

```text
symbol, trade_date, as_of_time, feature_name, params_hash, value, source, created_at
```

## Built-In Features

| Feature | Type | Frequency | Description |
|---|---|---|---|
| `smallcap_market_cap` | indicator | daily | point-in-time market cap used by ID=43 ranking |
| `smallcap_market_cap_rank` | factor | daily | ascending market-cap rank inside the selected universe |
| `smallcap_is_st` | state | daily | ST/delist/name-change exclusion flag |
| `smallcap_is_paused` | state | timer 10:30 | no usable timer bar at selection time |
| `smallcap_is_limit_up` | state | timer 10:30 | timer price is at or above up-limit |
| `smallcap_is_limit_down` | state | timer 10:30 | timer price is at or below down-limit |
| `smallcap_yesterday_limit_up` | state | daily | previous trading day closed at up-limit |
| `smallcap_v4gv` | indicator | daily | ID=43 V4GV technical value definition |
| `smallcap_v4gv21` | indicator | daily | ID=43 V4GV signal-line definition |
| `smallcap_macd_signal` | signal | daily | ID=43 MACD positive signal definition |
| `smallcap_indicator_buy_signal` | signal | daily | ID=43 combined technical buy-signal definition |
| `smallcap_v4gv_dead_cross` | signal | daily | ID=43 technical sell-signal definition |
| `cum_volume_at_time` | indicator | timer | cumulative intraday volume up to a configured timer point |
| `max_volume_nd` | indicator | daily/timer | rolling max volume using historical daily volume plus timer-day partial volume |
| `high_volume_ratio` | factor | timer | timer-day cumulative volume divided by rolling max volume |
| `high_volume_signal` | signal | timer | binary high-volume exit signal |

The `small_cap_v4_core` feature group collects the ID=43 small-cap feature
surface. Status, market-cap, and high-volume features can be precomputed today.
The V4GV/MACD items are registered as first-class feature definitions so the UI
and API can reason about them; their batch calculator is still a follow-up.

ID=43 currently consumes `high_volume_signal` only when
`high_volume_mode="minute_to_now"` and `enable_feature_store=true`; if no value
exists, the strategy falls back to its original calculation. The JQ-aligned
default remains `high_volume_mode="daily_include_now"`, which intentionally uses
the strategy-side calculation to match `get_bars(..., include_now=True)`.

## API

| Endpoint | Description |
|---|---|
| `GET /api/features/definitions` | list feature definitions, schemas, and dependencies |
| `GET /api/features/groups` | list feature groups such as `small_cap_v4_core` |
| `GET /api/features/coverage` | inspect stored coverage for a feature/date range |
| `GET /api/features/preview` | preview one date's cross-section values |
| `POST /api/features/precompute` | precompute supported features into Parquet |
| `POST /api/features/groups/precompute` | precompute a supported feature group |
| `POST /api/features/query` | query a feature cross-section |

## CLI

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\backend\.venv\Scripts\python.exe backend/app/scripts/precompute_features.py `
  --feature high_volume_signal `
  --index-symbol 399101.SZ `
  --start 2020-01-01 `
  --end 2026-05-06 `
  --time 14:30 `
  --window 120 `
  --threshold 0.9
```

This command requires `klines_daily` and `klines_minute_cum_timer` in the
configured Parquet market data directory.

Precompute the ID=43 small-cap core feature group:

```powershell
$env:PYTHONPATH='E:\Projects\GaoshouPlatform\backend'
.\backend\.venv\Scripts\python.exe backend/app/scripts/precompute_features.py `
  --group small_cap_v4_core `
  --index-symbol 399101.SZ `
  --start 2020-01-01 `
  --end 2022-12-31 `
  --time 10:30
```

This group writes 10:30 status flags, daily market-cap/rank flags, yesterday
limit-up flags, and the 14:30 high-volume feature set. It requires:

- `stock_daily_basic` and `stock_limit_prices` in SQLite
- `index_components` for `399101.SZ`
- Parquet `klines_daily`
- Parquet `klines_minute_timer` or `klines_minute` with 10:30 bars
- Parquet `klines_minute_cum_timer` with 14:30 cumulative volume for
  high-volume features

## Frontend

Open Factor Research and switch to the `Feature Store` tab. The tab provides:

- feature catalog and dependencies
- coverage query for an index pool or explicit symbols
- precompute trigger for supported features
- feature group selection and one-click group precompute
- cross-section preview for one trade date

## Next Extensions

- Register AKQuant/Polars expression outputs as feature definitions.
- Add redis caching for repeated coverage and cross-section reads.
- Add the batch calculator for `smallcap_v4gv`, `smallcap_v4gv21`,
  `smallcap_macd_signal`, `smallcap_indicator_buy_signal`, and
  `smallcap_v4gv_dead_cross`.
- Add lineage fields for data source version, adjustment mode, and compute code
  version.
- Add factor analysis workflows that use `feature_values` directly as input.

## ID=43 Strategy Integration Notes

- `cum_volume_at_time` is now treated as an external intraday cumulative-volume
  indicator. The strategy consumes it through an adapter that prefers
  Feature Store values and falls back to Parquet minute cumulative volume.
- V4GV/V4GV21/MACD are exposed through a strategy-side technical-signal adapter.
  It preserves the existing formula and Redis cache path, while keeping the
  strategy body independent from the indicator source.
- `execution_plan_mode=timer` remains the default. `execution_plan_mode=pre_open`
  is experimental: it requires a feed that includes a real open/pre-open bar
  (for example 09:30/09:31). With the current sparse timer feed, staged pre-open
  orders are submitted at the next available timer event and can expire instead
  of filling on the open.

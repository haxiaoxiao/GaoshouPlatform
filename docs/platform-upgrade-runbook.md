# Platform Upgrade Runbook

Last updated: 2026-07-17.

This runbook applies the research-lineage and live-safety upgrade without modifying the
active database or Factor Value Store in place.

## Preconditions

- Use a declared maintenance window. Stop backend and sync services before migrating.
- Restart with real order submission and automatic execution disabled.
- Keep `LIVE_TRADING_CONTROL_SECRET` outside source control. Without it, V1 live unlock
  remains intentionally unavailable.
- Confirm the startup summary shows the expected masked account and data roots.

## Database

The launcher runs this before either backend process:

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m alembic -c alembic.ini upgrade head
```

Expected head revision: `20260714_0001`. Rehearse `upgrade head`, `downgrade base`, and a
second `upgrade head` on a SQLite online-backup copy before changing production.

## Dataset Manifests

Build exact manifests only for stable datasets:

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m app.scripts.build_dataset_manifests `
  klines_daily klines_minute_timer stock_indicators `
  --data-dir E:/Projects/data/BaiduSyncdisk/parquet
```

`GET /api/v1/readiness` must report the manifest source and expose stale, missing, or
invalid datasets. Do not treat a missing manifest as validated data.

## Factor Store Cutover

Compaction always targets a separate directory. An interrupted run can be resumed; every
existing month is revalidated before reuse.

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m app.scripts.compact_factor_value_store `
  --source E:/Projects/data/BaiduSyncdisk/parquet/factor_values `
  --output E:/Projects/data/BaiduSyncdisk/parquet/factor_values_compacted_YYYYMMDD `
  --report E:/Projects/data/BaiduSyncdisk/parquet/factor_values_compacted_YYYYMMDD-report.json `
  --resume
```

Do not cut over unless the report says `validated: true`, all source months are present,
the output has fewer than 400 Parquet files, `_manifest.json` exists, and
`details.factor_coverage` contains every cached factor. For a validated compacted store
created before the coverage index was introduced, add only the metadata index with:

```powershell
cd E:\Projects\GaoshouPlatform-prod\backend
.\.venv\Scripts\python.exe -m app.scripts.compact_factor_value_store `
  --output E:/Projects/data/BaiduSyncdisk/parquet/factor_values_compacted_YYYYMMDD `
  --index-only
```

The full-range `FactorValueStore.coverage_many` P95 must be below one second before
cutover. During the maintenance window set:

```text
FACTOR_VALUE_STORE_DIR=E:/Projects/data/BaiduSyncdisk/parquet/factor_values_compacted_YYYYMMDD
```

The launcher refuses an override without a manifest. Rollback removes the override and
restarts the affected services. Retain the original directory for at least one release
cycle.

The following is a 2026-07-10 audit snapshot, not a live capacity report. The candidate is
`E:/Projects/data/BaiduSyncdisk/parquet/factor_values_compacted_20260710`: 79 Parquet
files, 223,259,144 effective rows, 35 indexed factors. It is a candidate only; production
continues to use the original path until a declared maintenance window.

## Historical Universes

Index-backed research and execution are point-in-time only. A requested range loads the
latest snapshot on or before its start plus constituent changes inside the range. A
single-day live decision loads the latest snapshot on or before that trading day. A
future/current snapshot must never fill a missing historical range; sync the missing
history and keep the release blocked instead.

## Release Flow

1. Create a ready data snapshot with `POST /api/v1/data-snapshots`.
2. Create a strategy release with code hash, Git commit, engine version, universe, factor
   hashes, parameters, and cost model.
3. Attach validation, backtest, walk-forward, and paper-shadow artifacts. A valid
   validation artifact must set `data_integrity`, `point_in_time`, `no_lookahead`, and
   `execution_consistency` to `true`. A backtest artifact must use
   `result_schema_version: 2`.
4. Promote only through `validated -> paper_approved -> live_approved`.
5. Run at least five consecutive trading days in paper with daily reconciliation, no
   duplicate orders, and no guardrail bypass.
6. Start one approved strategy as the live canary. Roll back on reconciliation or
   execution mismatch.

V1 live order submission requires a `live_approved` release, stable idempotency key,
expected masked account, and a non-expired in-memory control session. The request is
durably reserved before the external broker call; the internal capability is single-use
and bound to release, strategy, profile, account, idempotency key, reservation, and
control session. The legacy `/api/live-trading/orders/submit` endpoint returns 410,
and runner/cancel-resubmit paths cannot submit real orders. Mobile trading is read-only.

## TSMF Revalidation

Before creating a paper-approved TSMF release, rerun from `2020-01-02` through the latest
complete trading day. Use a three-year training window, six-month rolling validation,
and reserve the final twelve months as an untouched out-of-sample interval. Persist the
walk-forward and out-of-sample reports as checksummed research artifacts; do not promote
from an unversioned historical backtest.

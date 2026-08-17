# Sentiment Focus Pool Design

## Goal

Replace the scheduled Xueqiu target selection based on an arbitrary first 100 watchlist rows with a small, explicit focus pool composed of current miniQMT holdings and actively maintained TheLandsBetween company notes.

## Scope

This design covers target resolution, QMT availability and fallback behavior, Vault extraction, source provenance, deduplication, crawl pacing, and observability. It does not change order submission, trading strategy universes, or the existing manual watchlist UI.

## Target Sources

The normal pool contains the union of two sources:

1. `qmt_holding`: positive-quantity positions returned by the read-only `qmt_trading_service.account_snapshot()` interface.
2. `vault_active`: Markdown notes under `E:\Projects\data\BaiduSyncdisk\TheLandsBetween\wiki\03 实体\待观察` whose YAML frontmatter contains:
   - `type: company-research`
   - `status: active`
   - a canonical `symbol` matching `^\d{6}\.(SH|SZ|BJ)$`

The resolver must normalize symbols through the existing security-symbol normalizer and deduplicate by canonical symbol. It must not include `watchlist_stocks`, `A股精选`, broad index members, historical mentions, or sentiment mentions as fallback sources.

Each resolved symbol carries provenance such as `qmt_holding`, `vault_active`, or both. Provenance is recorded in the sync details, while position quantities, cost basis, account identifiers, and other financial fields are not persisted in the sentiment target snapshot or emitted to logs.

## QMT Snapshot And Fallback

On each scheduled run:

1. Query the broker account through the existing read-only `qmt_trading_service.account_snapshot()` method.
2. Keep symbols with `quantity > 0`.
3. Persist an ordered symbol snapshot and capture timestamp only after a successful query.
4. If QMT is unavailable, use the most recent successful snapshot younger than 24 hours and mark the source `qmt_holding_stale`.
5. If no usable snapshot exists, continue with Vault symbols and emit a high-signal `qmt_holdings_unavailable` warning. Never interpret a query failure as zero holdings.

The resolver must distinguish `fresh`, `stale`, and `unavailable` in progress details. A failed QMT query must not invoke any order, cancellation, or account-mutating operation.

## Vault Recency And Membership

The entity-note metadata and active path are the authoritative membership gate. Historical company mentions in synthesis documents are not promoted into the pool. The resolver records the note path and the latest dated tracking heading when available, but uses the frontmatter symbol as the canonical key.

The initial Vault set is expected to contain the 10 active company notes identified in the July 2026 audit. Future notes outside the designated directory require explicit metadata before they can enter the pool.

## Crawl Policy

The scheduled Xueqiu batch processes the resolved pool sequentially through one shared browser session. To reduce anti-bot failures:

- QMT-held symbols receive up to 3 pages.
- Vault-only symbols receive up to 2 pages.
- A bounded delay is inserted between symbols; the delay is configurable and defaults to a conservative 2 seconds.
- A 405/non-JSON response triggers a cooldown and stops the remaining Xueqiu batch for that run rather than continuing to hit the endpoint.
- The existing login gate remains in force. Manual login can be completed in the retained Chrome profile.

The existing per-symbol date-window filtering and post upsert behavior remain unchanged in this focused change. “Incremental” continues to use the latest stored publication date as the lower bound, but the resolved target pool and crawl outcome are made explicit in logs.

## Snapshot Storage

Store the last successful target snapshot in the shared Gaoshou SQLite database using a small dedicated table or equivalent structured persistence with:

- snapshot key/version
- captured timestamp
- ordered canonical symbols
- source/provenance metadata
- status
- error summary, if the current refresh was stale or unavailable

Do not store credentials, cookies, account IDs, position quantities, costs, or market values in this snapshot.

## Progress And Observability

Every scheduled run exposes:

- `target_pool.sources`
- `target_pool.symbol_count`
- `target_pool.qmt_status` (`fresh`, `stale`, or `unavailable`)
- `target_pool.vault_count`
- `target_pool.overlap_count`
- `target_pool.snapshot_captured_at`
- `target_pool.crawl_limited_reason` when a 405/cooldown stops the batch

The run summary distinguishes target-resolution failures from per-symbol crawl failures. A partial run is not reported as a fully successful run when the circuit breaker stops remaining symbols.

## Testing

Focused tests will cover:

- Fresh QMT holdings plus Vault symbols are unioned and deduplicated.
- QMT holdings are ordered before Vault-only symbols.
- A QMT failure uses a snapshot younger than 24 hours.
- An expired/missing snapshot does not fabricate an empty holdings set.
- Vault extraction rejects inactive, malformed, or out-of-directory notes.
- Position and Vault provenance is preserved without sensitive fields.
- QMT query code is read-only and does not call order APIs.
- Page limits differ for holdings and Vault-only symbols.
- A 405 response stops the remaining batch and records a circuit-breaker reason.
- The existing login gate and shared Playwright session continue to work.

## Expected Result

The scheduled task will normally crawl roughly 29 symbols: the 19 currently held positions plus the 10 active Vault company notes, less any future overlap. If QMT is temporarily unavailable, it will use the last good holdings snapshot for up to 24 hours and still include the Vault set. The old arbitrary 100-row watchlist scan will no longer be part of scheduled Xueqiu targeting.

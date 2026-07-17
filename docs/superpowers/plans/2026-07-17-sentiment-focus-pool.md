# Sentiment Focus Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make scheduled Xueqiu ingestion crawl only positive miniQMT holdings and active Vault company notes, with a 24-hour holdings fallback, source-specific page limits, pacing, and a batch circuit breaker.

**Architecture:** Add a focused resolver service that reads miniQMT through the existing read-only account snapshot API, parses a narrowly scoped Vault directory, and persists sanitized resolution snapshots in SQLite. `SyncService.sync_sentiment()` will invoke it only when Xueqiu has no explicit symbols, preserve the shared Playwright login session, and apply per-target crawl policy plus a typed anti-bot circuit breaker.

**Tech Stack:** Python 3.12, asyncio, SQLAlchemy async ORM, PyYAML, Playwright, pytest/pytest-asyncio

---

### Task 1: Sanitized Focus Snapshot Model

**Files:**
- Modify: `backend/app/db/models/sentiment.py`
- Modify: `backend/app/db/models/__init__.py`
- Test: `backend/tests/services/test_sentiment_focus_pool.py`

- [ ] **Step 1: Write the failing model contract test**

Create a test that constructs `SentimentFocusSnapshot` with only `snapshot_key`, `status`, `captured_at`, `symbols_json`, `provenance_json`, and `error_summary`, then asserts the table has no account, quantity, cost, or market-value columns:

```python
def test_focus_snapshot_schema_excludes_sensitive_position_fields() -> None:
    columns = set(SentimentFocusSnapshot.__table__.columns.keys())
    assert {"snapshot_key", "status", "captured_at", "symbols_json", "provenance_json"} <= columns
    assert columns.isdisjoint({"account_id", "quantity", "avg_cost", "market_value"})
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_focus_pool.py::test_focus_snapshot_schema_excludes_sensitive_position_fields -q`

Expected: collection fails because `SentimentFocusSnapshot` does not exist.

- [ ] **Step 3: Add and register the model**

Add a `SentimentFocusSnapshot` ORM model to `backend/app/db/models/sentiment.py`:

```python
class SentimentFocusSnapshot(Base, TimestampMixin):
    __tablename__ = "sentiment_focus_snapshots"
    __table_args__ = (
        Index("ix_sentiment_focus_snapshot_key_captured", "snapshot_key", "captured_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_key: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    captured_at: Mapped[datetime | None] = mapped_column(DateTime)
    symbols_json: Mapped[str] = mapped_column(Text, nullable=False)
    provenance_json: Mapped[str] = mapped_column(Text, nullable=False)
    error_summary: Mapped[str | None] = mapped_column(String(500))
```

Export it from `backend/app/db/models/__init__.py` so `Base.metadata.create_all()` creates the table.

- [ ] **Step 4: Run the model test and verify GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_focus_pool.py::test_focus_snapshot_schema_excludes_sensitive_position_fields -q`

Expected: `1 passed`.

- [ ] **Step 5: Commit the model slice**

```powershell
git add backend/app/db/models/sentiment.py backend/app/db/models/__init__.py backend/tests/services/test_sentiment_focus_pool.py
git commit -m "feat: add sanitized sentiment focus snapshots"
```

### Task 2: Vault And miniQMT Focus Resolver

**Files:**
- Create: `backend/app/services/sentiment_focus_pool.py`
- Modify: `backend/app/core/config.py`
- Modify: `backend/pyproject.toml`
- Modify: `.env.example`
- Test: `backend/tests/services/test_sentiment_focus_pool.py`

- [ ] **Step 1: Write failing Vault membership tests**

Add tests that create Markdown notes under a temporary `待观察` directory and exercise the wished-for `SentimentFocusPoolResolver`. Cover one valid `type: company-research` / `status: active` / canonical symbol note plus inactive, malformed-symbol, wrong-type, and sibling-directory notes. Assert only the valid canonical symbol is returned and its provenance contains the relative note path.

```python
notes = resolver.read_vault_targets()
assert [note.symbol for note in notes] == ["002138.SZ"]
assert notes[0].note_path.endswith("valid.md")
```

- [ ] **Step 2: Run the Vault tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_focus_pool.py -k vault -q`

Expected: fails because `sentiment_focus_pool` and its resolver do not exist.

- [ ] **Step 3: Implement strict Vault extraction**

Create `backend/app/services/sentiment_focus_pool.py` with immutable `VaultFocusTarget`, `FocusTarget`, and `ResolvedFocusPool` dataclasses. Parse only `vault_root.rglob("*.md")` using `yaml.safe_load()` on a leading `---` frontmatter block. Require exact metadata values and `^\d{6}\.(SH|SZ|BJ)$`; normalize accepted symbols through `normalize_sentiment_symbol()`. Extract the latest heading matching `YYYY-MM-DD` when present, but never scan other Vault directories.

Add settings:

```python
sentiment_focus_vault_dir: str = str(
    _DATA_DIR / "TheLandsBetween" / "wiki" / "03 实体" / "待观察"
)
xueqiu_stock_delay_seconds: float = 2.0
```

Declare `PyYAML>=6.0.0` in `backend/pyproject.toml`, and document `SENTIMENT_FOCUS_VAULT_DIR` plus `XUEQIU_STOCK_DELAY_SECONDS` in `.env.example` without changing local secrets.

- [ ] **Step 4: Run Vault tests and verify GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_focus_pool.py -k vault -q`

Expected: all selected tests pass.

- [ ] **Step 5: Write failing fresh/stale/unavailable resolver tests**

Use dependency injection for `account_snapshot`, `now`, and the async session. Assert:

```python
pool = await resolver.resolve()
assert [target.symbol for target in pool.targets] == ["002313.SZ", "600114.SH", "002138.SZ"]
assert pool.qmt_status == "fresh"
assert pool.overlap_count == 1
assert pool.targets[1].sources == ("qmt_holding", "vault_active")
```

Also assert a QMT exception uses a successful snapshot captured 23 hours ago with source `qmt_holding_stale`, rejects one captured 25 hours ago, continues with Vault-only targets, and never persists or logs position quantity, account, cost, cash, or market value.

- [ ] **Step 6: Run resolver tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_focus_pool.py -k "fresh or stale or unavailable" -q`

Expected: fails because `resolve()` fallback and persistence are not implemented.

- [ ] **Step 7: Implement resolution and snapshot fallback**

Implement `resolve()` to:

1. Read current Vault targets.
2. Await only the injected account snapshot callable, defaulting to `qmt_trading_service.account_snapshot`.
3. Keep positive-quantity positions, normalize and sort them, then order QMT holdings before sorted Vault-only names.
4. On success, persist a `fresh` sanitized snapshot with ordered symbols and source/note provenance.
5. On failure, select the newest `fresh` snapshot for `sentiment_xueqiu_focus_v1`; use only QMT-sourced symbols when `now - captured_at < 24 hours` and label them `qmt_holding_stale`.
6. Persist the current `stale` or `unavailable` resolution with a bounded error summary and no financial fields.
7. Return `ResolvedFocusPool.as_details()` with sources, symbol/vault/overlap counts, QMT status, and snapshot time.

- [ ] **Step 8: Run all resolver tests and verify GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_focus_pool.py -q`

Expected: all tests pass.

- [ ] **Step 9: Commit the resolver slice**

```powershell
git add backend/app/services/sentiment_focus_pool.py backend/app/core/config.py backend/pyproject.toml .env.example backend/tests/services/test_sentiment_focus_pool.py
git commit -m "feat: resolve sentiment targets from qmt and vault"
```

### Task 3: Scheduled Xueqiu Target Policy

**Files:**
- Modify: `backend/app/services/sync_service.py`
- Modify: `backend/tests/services/test_sync_service_incremental.py`

- [ ] **Step 1: Write failing scheduled-pool integration tests**

Inject a fake resolver returning QMT, overlap, and Vault-only targets. Call `sync_sentiment(sources=["xueqiu_spyder"], symbols=None)` and assert the old watchlist query is never used, QMT/overlap targets receive `max_pages=3`, Vault-only targets receive `max_pages=2`, and `progress.details["target_pool"]` contains resolver observability.

Add a separate explicit-symbol test asserting `symbols=["600519.SH"]` bypasses the resolver and continues to honor the caller's `max_pages`.

- [ ] **Step 2: Run integration tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sync_service_incremental.py -k "focus_pool or explicit_xueqiu" -q`

Expected: the no-symbol path still queries `watchlist_stocks`, or the resolver/page policy is absent.

- [ ] **Step 3: Replace watchlist selection and apply page policy**

In `SyncService.sync_sentiment()`:

- Resolve the focus pool only when Xueqiu is requested and no explicit symbol exists.
- Set `xueqiu_targets` from the ordered `FocusTarget` list.
- Keep non-Xueqiu global-source behavior unchanged.
- For focused targets pass `min(max_pages, 3)` when provenance includes QMT and `min(max_pages, 2)` for Vault-only targets.
- Put `target_pool` details in progress and preserve the existing shared `XueqiuSession` login gate.
- Sleep `settings.xueqiu_stock_delay_seconds` only between attempted Xueqiu symbols, never after the last target and never for an explicit one-symbol run.

- [ ] **Step 4: Run integration tests and verify GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sync_service_incremental.py -k "sentiment" -q`

Expected: all sentiment sync tests pass, including existing login timeout and cancellation tests.

- [ ] **Step 5: Commit the scheduler integration slice**

```powershell
git add backend/app/services/sync_service.py backend/tests/services/test_sync_service_incremental.py
git commit -m "feat: focus scheduled xueqiu sentiment crawl"
```

### Task 4: Typed Xueqiu Circuit Breaker

**Files:**
- Modify: `backend/app/services/sentiment.py`
- Modify: `backend/app/services/sync_service.py`
- Modify: `backend/tests/services/test_sentiment_service.py`
- Modify: `backend/tests/services/test_sync_service_incremental.py`

- [ ] **Step 1: Write failing crawler response tests**

Test `_BuiltinXueqiuCrawler.get_stock_posts()` with a fake Playwright page returning `{ok: false, error: "non_json_response", status: 405}`. Assert it raises `XueqiuCrawlBlockedError` with reason `non_json_response` and status code `405`; assert the exception message does not include the returned HTML snippet.

- [ ] **Step 2: Run crawler response tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_service.py -k "xueqiu_crawl_blocked" -q`

Expected: fails because the typed exception and HTTP status propagation do not exist.

- [ ] **Step 3: Implement the typed blocked response**

Add:

```python
class XueqiuCrawlBlockedError(RuntimeError):
    def __init__(self, *, reason: str, status_code: int | None = None) -> None:
        self.reason = reason
        self.status_code = status_code
        suffix = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(f"Xueqiu request blocked: {reason}{suffix}")
```

Return `status: resp.status` from the browser fetch result and raise this exception for any non-JSON response. Preserve ordinary parse/runtime errors as ordinary per-symbol failures.

- [ ] **Step 4: Write failing batch circuit-breaker test**

Make the second of three fake session collections raise `XueqiuCrawlBlockedError`. Assert only the first two symbols are attempted, the remaining target is recorded as skipped, `target_pool.crawl_limited_reason` is set, and the run outcome is partial rather than success.

- [ ] **Step 5: Run batch test and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sync_service_incremental.py -k "circuit_breaker" -q`

Expected: the third symbol is still attempted and no limited reason is reported.

- [ ] **Step 6: Implement batch short-circuiting**

Catch `XueqiuCrawlBlockedError` before the existing broad per-symbol handler, roll back the active ingest session, append a failed result for the blocked symbol, append `xueqiu_circuit_open` skipped results for remaining targets, set a stable `crawl_limited_reason`, and break the batch. Do not close Chrome itself; retain the existing crawler disconnect behavior and profile.

- [ ] **Step 7: Run circuit-breaker and sentiment regression tests**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_service.py -k "xueqiu" -q`

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sync_service_incremental.py -k "sentiment" -q`

Expected: all selected tests pass.

- [ ] **Step 8: Commit the circuit-breaker slice**

```powershell
git add backend/app/services/sentiment.py backend/app/services/sync_service.py backend/tests/services/test_sentiment_service.py backend/tests/services/test_sync_service_incremental.py
git commit -m "fix: stop xueqiu batch on blocked responses"
```

### Task 5: Verification And Runtime Rollout

**Files:**
- Verify: `backend/app/services/sentiment_focus_pool.py`
- Verify: `backend/app/services/sync_service.py`
- Verify: `backend/app/services/sentiment.py`
- Verify: `backend/app/db/models/sentiment.py`

- [ ] **Step 1: Run focused tests**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_focus_pool.py tests/services/test_sync_service_incremental.py tests/services/test_xueqiu_session.py -q`

Expected: all tests pass.

- [ ] **Step 2: Run broader sentiment tests**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/services/test_sentiment_service.py tests/api/test_sentiment_routes.py -q`

Expected: all tests pass.

- [ ] **Step 3: Run static validation**

Run: `cd backend; .\.venv\Scripts\python.exe -m compileall -q app/services/sentiment_focus_pool.py app/services/sentiment.py app/services/sync_service.py app/db/models/sentiment.py`

Run: `cd backend; .\.venv\Scripts\python.exe -m ruff check app/services/sentiment_focus_pool.py app/services/sentiment.py app/services/sync_service.py app/db/models/sentiment.py tests/services/test_sentiment_focus_pool.py tests/services/test_sync_service_incremental.py`

Expected: both commands exit `0`.

- [ ] **Step 4: Audit the diff and requirements**

Run: `git diff --check`

Run: `git status --short`

Confirm the feature diff contains no account identifiers, quantities, costs, market values, cookies, or credentials; confirm existing unrelated dirty files were not reverted.

- [ ] **Step 5: Restart and health-check the sync service**

Use the repository's Windows service scripts or the existing process command line to restart only the sync service on port `8810`, preserving `.env.local` injection. Verify `http://127.0.0.1:8810/health` and confirm the scheduler still reports `每日舆情增量` at `30 22 * * *`. Do not manually trigger a live Xueqiu batch unless requested.

- [ ] **Step 6: Leave a feature-only handoff diff**

Because the production worktree already contains unrelated edits in several touched files, do not stage whole dirty files. Record the exact feature paths and hunks in the completion summary, and commit only through a reviewed feature patch that excludes pre-existing changes.

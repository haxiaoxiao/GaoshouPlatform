# Xueqiu Login-Gated Crawler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make scheduled Xueqiu ingestion open one persistent login page, wait quietly for manual login for up to 15 minutes, then reuse one browser session for the whole symbol batch.

**Architecture:** Add an async session wrapper backed by a dedicated one-worker executor so every synchronous Playwright call stays on the same thread. Gate the Xueqiu batch before creating symbol tasks, report a stable waiting state, and separate Playwright disconnection from ownership of the externally launched Chrome process.

**Tech Stack:** Python 3.12, asyncio, concurrent.futures, Playwright sync API, FastAPI sync progress, pytest/pytest-asyncio

---

## File Map

- Create `backend/app/services/xueqiu_session.py`: thread-affine async session, login wait state, timeout and cleanup.
- Modify `backend/app/services/sentiment.py`: allow collection with an injected crawler and make `close()` disconnect without closing Chrome.
- Modify `backend/app/services/sentiment_adapters/xueqiu.py`: accept the batch-owned session path without creating per-symbol crawlers.
- Modify `backend/app/services/sync_service.py`: create one Xueqiu session, gate login once, process symbols sequentially, and expose progress.
- Create `backend/tests/services/test_xueqiu_session.py`: focused state-machine and thread-affinity tests.
- Modify `backend/tests/services/test_sentiment_service.py`: injected-crawler and Chrome-retention regression tests.
- Modify `backend/tests/services/test_sync_service_incremental.py`: batch orchestration, timeout and cancellation tests.

### Task 1: Thread-Affine Xueqiu Session

**Files:**
- Create: `backend/app/services/xueqiu_session.py`
- Create: `backend/tests/services/test_xueqiu_session.py`

- [ ] **Step 1: Write failing tests for login success, quiet waiting, timeout, and cleanup**

Define a fake crawler whose `verify_login()` outcomes are controlled by the test and whose method calls record `threading.get_ident()`. Assert that `XueqiuSession.start()`, `wait_for_login()`, `collect()`, and `disconnect()` all run crawler operations on one thread; assert the progress callback receives `xueqiu_spyder.waiting_for_login` only once and no collect call occurs before authentication.

```python
@pytest.mark.asyncio
async def test_waits_without_navigation_then_resumes_on_same_thread():
    crawler = FakeCrawler(login_results=[False, False, True])
    events = []
    session = XueqiuSession(lambda: crawler, poll_interval=0, login_timeout=1, progress_callback=events.append)
    await session.start()
    result = await session.wait_for_login()
    await session.collect("600519.SH", max_pages=1, min_reply=0)
    await session.disconnect()
    assert result.status == "authenticated"
    assert [event["stage"] for event in events] == ["xueqiu_spyder.waiting_for_login", "xueqiu_spyder.login_succeeded"]
    assert len(set(crawler.thread_ids)) == 1
```

- [ ] **Step 2: Run the new tests and verify RED**

Run: `backend\.venv\Scripts\python.exe -m pytest backend/tests/services/test_xueqiu_session.py -q`

Expected: collection error because `app.services.xueqiu_session` does not exist.

- [ ] **Step 3: Implement the minimal session state machine**

Create `XueqiuLoginResult(status, auth)` and `XueqiuSession`. Use `ThreadPoolExecutor(max_workers=1, thread_name_prefix="xueqiu")`; route every crawler operation through `loop.run_in_executor(self._executor, ...)`. Poll `_verify_xueqiu_login` without reload, use `time.monotonic()` for the deadline, emit waiting once, and return `login_timeout` after the deadline. `disconnect()` calls crawler cleanup in the executor and then shuts down the executor.

- [ ] **Step 4: Run the session tests and verify GREEN**

Run: `backend\.venv\Scripts\python.exe -m pytest backend/tests/services/test_xueqiu_session.py -q`

Expected: all tests pass.

- [ ] **Step 5: Commit the session unit**

```powershell
git add backend/app/services/xueqiu_session.py backend/tests/services/test_xueqiu_session.py
git commit -m "feat: add login-gated xueqiu session"
```

### Task 2: Separate Playwright Disconnect From Chrome Lifetime

**Files:**
- Modify: `backend/app/services/sentiment.py:1455-1540`
- Modify: `backend/tests/services/test_sentiment_service.py`

- [ ] **Step 1: Write failing crawler lifecycle tests**

Add tests asserting that disconnecting a CDP-attached crawler calls `playwright.stop()` but does not call `browser.close()`, and that an injected crawler passed to `_collect_xueqiu` is neither constructed nor closed per symbol.

```python
def test_disconnect_leaves_external_chrome_open():
    crawler = object.__new__(_BuiltinXueqiuCrawler)
    crawler._browser = FakeBrowser()
    crawler._playwright = FakePlaywright()
    crawler.disconnect()
    assert crawler._browser.close_calls == 0
    assert crawler._playwright.stop_calls == 1
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `backend\.venv\Scripts\python.exe -m pytest backend/tests/services/test_sentiment_service.py -k "xueqiu and (disconnect or injected)" -q`

Expected: fail because `disconnect()` and injected crawler support do not exist.

- [ ] **Step 3: Implement lifecycle separation and injected collection**

Rename the cleanup operation to `disconnect()` and stop Playwright without invoking `browser.close()` for the CDP browser. Change `_collect_xueqiu(..., crawler: Any | None = None)` so it owns and disconnects a crawler only when it constructed that crawler itself. Keep the adapter-compatible return structure unchanged.

- [ ] **Step 4: Run focused sentiment tests and verify GREEN**

Run: `backend\.venv\Scripts\python.exe -m pytest backend/tests/services/test_sentiment_service.py -k xueqiu -q`

Expected: all Xueqiu sentiment tests pass.

- [ ] **Step 5: Commit lifecycle changes**

```powershell
git add backend/app/services/sentiment.py backend/tests/services/test_sentiment_service.py
git commit -m "fix: retain chrome during xueqiu login"
```

### Task 3: Gate And Reuse The Session Across The Batch

**Files:**
- Modify: `backend/app/services/sync_service.py:3202-3360`
- Modify: `backend/app/services/sentiment_adapters/xueqiu.py`
- Modify: `backend/tests/services/test_sync_service_incremental.py`

- [ ] **Step 1: Write failing batch orchestration tests**

Build a fake `XueqiuSession` and assert: one session is created for multiple symbols; login is awaited once; no Xueqiu symbol work starts on timeout; successful login processes symbols sequentially; cancellation always calls `disconnect()` and prevents remaining symbols.

```python
@pytest.mark.asyncio
async def test_sync_sentiment_gates_xueqiu_once_for_all_symbols(monkeypatch):
    session = FakeXueqiuSession(login_status="authenticated")
    monkeypatch.setattr(sync_service_module, "XueqiuSession", lambda **_: session)
    progress = await SyncService(FakeAsyncSession()).sync_sentiment(
        sources=["xueqiu_spyder"], symbols=["600519.SH", "000001.SZ"], max_pages=1, min_reply=0
    )
    assert session.wait_calls == 1
    assert session.symbols == ["600519.SH", "000001.SZ"]
    assert session.disconnect_calls == 1
    assert progress.success_count == 2
```

- [ ] **Step 2: Run orchestration tests and verify RED**

Run: `backend\.venv\Scripts\python.exe -m pytest backend/tests/services/test_sync_service_incremental.py -k xueqiu -q`

Expected: fail because the batch does not own a shared Xueqiu session.

- [ ] **Step 3: Implement specialized Xueqiu batch orchestration**

Before building generic sentiment work items, split Xueqiu from other sources. Create one `XueqiuSession`, await its login gate, then collect Xueqiu symbols sequentially through that session. On timeout append one stable failure result with `error_code="xueqiu_login_timeout"`, not one failure per symbol. Emit `login_wait_started_at`, `login_wait_timeout_seconds=900`, and `login_url`; disconnect in `finally`. Leave other source concurrency unchanged.

- [ ] **Step 4: Adapt the Xueqiu adapter boundary**

Keep manual single-symbol ingestion working by retaining the adapter's standalone fallback. The scheduled batch passes its owned session explicitly and does not call `asyncio.to_thread` per symbol.

- [ ] **Step 5: Run orchestration and sentiment suites**

Run: `backend\.venv\Scripts\python.exe -m pytest backend/tests/services/test_sync_service_incremental.py backend/tests/services/test_sentiment_service.py backend/tests/services/test_xueqiu_session.py -q`

Expected: all tests pass.

- [ ] **Step 6: Validate syntax required by repository rules**

Run: `backend\.venv\Scripts\python.exe -c "import ast, pathlib; ast.parse(pathlib.Path('backend/app/services/sync_service.py').read_text(encoding='utf-8'))"`

Expected: exit code 0.

- [ ] **Step 7: Commit batch integration**

```powershell
git add backend/app/services/sync_service.py backend/app/services/sentiment_adapters/xueqiu.py backend/tests/services/test_sync_service_incremental.py
git commit -m "feat: gate scheduled xueqiu ingestion on login"
```

### Task 4: Full Verification And Production Restart

**Files:**
- Modify only if a test reveals an in-scope defect.

- [ ] **Step 1: Run all backend sentiment and scheduler tests**

Run: `backend\.venv\Scripts\python.exe -m pytest backend/tests/services/test_sentiment_service.py backend/tests/services/test_sync_service_incremental.py backend/tests/services/test_xueqiu_session.py backend/tests/api/test_sentiment_routes.py -q`

Expected: all tests pass with no warnings introduced by this change.

- [ ] **Step 2: Confirm no dedicated Xueqiu processes are running before restart**

Run a process query for `xueqiu-profile`, Playwright drivers owned by the sync service, and `uvicorn app.sync_main:app`. Stop only stale Xueqiu children if present.

- [ ] **Step 3: Restart the production sync service through the repository launcher**

Use the existing production start script or its exact sync-service command for port `8810`; do not start the dev `18810` service.

- [ ] **Step 4: Verify health and scheduler state**

Run: `Invoke-RestMethod http://127.0.0.1:8810/health` and `Invoke-RestMethod http://127.0.0.1:8810/api/data/sync/scheduler`.

Expected: health is successful; scheduler is running; `每日舆情增量` remains scheduled for 22:30.

- [ ] **Step 5: Run a controlled login-gate smoke test**

Trigger a single-symbol Xueqiu sync only with user authorization if it would perform an external write. Verify one Chrome login page remains stable, the progress stage becomes `waiting_for_login`, and no repeated navigation occurs. Cancel or allow timeout according to the test window.

- [ ] **Step 6: Review the final diff and commit any verification-only correction**

Run: `git diff --check` and `git status --short`.

Expected: no whitespace errors and no unrelated files staged.

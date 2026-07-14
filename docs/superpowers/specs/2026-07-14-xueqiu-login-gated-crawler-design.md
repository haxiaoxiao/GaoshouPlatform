# Xueqiu Login-Gated Crawler Design

## Goal

Prevent the scheduled Xueqiu sentiment job from repeatedly navigating or refreshing the login page while preserving a visible Chrome window for manual login. After login succeeds, the same scheduled run should continue automatically.

## Scope

This change covers the built-in Xueqiu crawler, sentiment batch orchestration, sync progress reporting, cancellation, and focused backend tests. It does not automate accepting Xueqiu's user agreement, entering credentials, clicking Login, or bypassing verification and anti-bot controls.

## Selected Behavior

- One scheduled sentiment run owns one shared Xueqiu crawler session.
- The crawler opens or attaches to the configured persistent Chrome profile once.
- Before any symbol navigation, it checks whether the session is authenticated.
- If unauthenticated, it shows the Xueqiu login page once and reports `waiting_for_login`.
- While waiting, it does not navigate symbols, refresh the login page, or create additional crawler instances.
- It polls authentication without page reload for up to 15 minutes.
- Successful manual login resumes the same batch automatically.
- Timeout ends only the Xueqiu portion with a clear login-timeout result. Chrome remains open for later manual login.
- Explicit cancellation stops polling promptly and closes Playwright resources, but leaves the externally launched Chrome process open.

## Architecture

### Session ownership

Move Xueqiu crawler construction out of the per-symbol collection path. The sentiment batch creates one `_BuiltinXueqiuCrawler`, passes it to each sequential Xueqiu work item, and closes its Playwright connection in a batch-level `finally` block.

Chrome and Playwright have separate lifetimes:

- Playwright is always disconnected when the batch completes, fails, times out, or is cancelled.
- A Chrome process launched with the persistent Xueqiu profile is intentionally retained after login timeout.
- A normal successful batch may also retain Chrome, matching the manual-login workflow and avoiding surprising window closure.

### Authentication gate

Add an authentication result with these outcomes:

- `authenticated`: proceed immediately.
- `waiting_for_login`: expose progress and poll.
- `login_timeout`: stop Xueqiu work without processing symbols.
- `cancelled`: stop promptly when the sync service shuts down or the run is cancelled.

Authentication is checked through session state and a lightweight authenticated endpoint or page-state probe. Polling must not call `page.reload()` or navigate away from the login page. The default poll interval is 2 seconds and the timeout is 900 seconds; both are injectable for tests.

### Batch execution

Xueqiu symbols remain sequential because they share a page and browser session. Other sentiment sources retain their existing concurrency behavior.

The Xueqiu flow is:

1. Resolve the watchlist symbols.
2. Create or attach one crawler.
3. Run the authentication gate once.
4. If authenticated, process each symbol with the shared crawler.
5. If login times out, return one source-level failure instead of one failure per symbol.
6. Disconnect Playwright in `finally` while retaining Chrome.

No queued Xueqiu symbol task may create its own crawler. This prevents a backlog from reopening Chrome after a stop request.

## Progress And Errors

The sync progress details expose:

- `stage: xueqiu_spyder.waiting_for_login`
- `current_source: xueqiu_spyder`
- `login_wait_started_at`
- `login_wait_timeout_seconds: 900`
- `login_url`

On successful login, the stage changes to `xueqiu_spyder.login_succeeded`, followed by normal page-fetch progress. On timeout, the result uses a stable error code such as `xueqiu_login_timeout` and a user-facing message explaining that Chrome was left open and the run can be retried after login.

Authentication failures, verification challenges, and expired sessions are source-level failures. Cookie values and credentials must never appear in logs or API responses.

## Existing Profiles

The configured persistent profile remains the source of truth. A successful manual login persists cookies for later scheduled runs. Environment-provided cookies, when configured, are injected before the authentication gate and then verified using the same mechanism.

## Shutdown And Cancellation

The sync service must track the active Xueqiu login wait. Service shutdown or task cancellation signals the wait loop, prevents later symbol work from starting, and disconnects Playwright. The implementation must not depend on force-killing Chrome to stop a run.

## Testing

Focused tests will prove:

- An authenticated session does not enter the wait loop.
- An unauthenticated session emits `waiting_for_login` once and performs no symbol navigation while waiting.
- Manual login during the window resumes the same batch.
- Timeout produces one source-level failure and leaves Chrome ownership unchanged.
- Multiple symbols reuse one crawler instance.
- Cancellation exits the wait and prevents pending symbols from starting.
- Playwright cleanup runs on success, timeout, failure, and cancellation.
- Progress and errors do not expose cookie values.

Tests use short injectable polling intervals and timeouts; production defaults remain 2 seconds and 15 minutes.

## Operational Outcome

At 22:30, a logged-in profile runs without visible login interaction. An expired profile opens one login page and waits quietly. The user can accept the agreement and log in manually; the job then resumes without another trigger. If no login occurs within 15 minutes, Xueqiu stops for that run, the other sentiment sources may complete, and Chrome stays available for login.

# Daily Sentiment Crawler Toggle Design

## Goal

Add a persistent switch in the GaoshouPlatform prod System Monitor that lets the operator enable or disable the scheduled `每日舆情增量` crawler without restarting services.

The switch controls only future scheduled executions. It does not cancel a run that has already started and does not disable manual sentiment ingestion.

## Existing Foundation

The scheduler already stores each recurring task in the shared SQLite `sync_tasks` table. `SyncTask.enabled` is authoritative at service startup, and `load_enabled_tasks()` registers only enabled rows with APScheduler. The daily sentiment task already has a stable name, `每日舆情增量`, and cron expression `30 22 * * *`.

The implementation will reuse this model rather than introduce an environment variable or a second feature-flag table.

## Backend API

The sync service will expose a narrowly scoped control contract for the daily sentiment task:

- `GET /api/data/sync/scheduler/daily-sentiment`
- `PUT /api/data/sync/scheduler/daily-sentiment`

The read response contains:

- `task_id`
- `name`
- `enabled`
- `cron_expression`
- `last_run_at`
- `next_run_at`
- `scheduler_job_present`

The update request accepts only:

```json
{"enabled": false}
```

The endpoint resolves the exact `每日舆情增量` row. It does not accept an arbitrary task ID or task name, preventing this control from changing unrelated market-data or trading schedules.

After persisting the requested state, the service reloads scheduled sync jobs through the existing scheduler lifecycle. Disabling removes the APScheduler job and clears `next_run_at`. Enabling registers the existing task with its stored cron expression and refreshes `next_run_at`.

If scheduler reload fails after the database update, the API returns an explicit error and logs the failure. The database remains authoritative, so a later successful reload or service restart converges to the saved state.

## Frontend Control

The System Monitor will add one compact operational row named `每日自动爬取` in the existing runtime-control area. It uses an Element Plus switch, with adjacent status text and the next scheduled execution time when enabled.

Behavior:

- Load the authoritative state from the sync service when the System Monitor loads.
- Disable the switch while a read or update is in flight.
- On change, send the desired `enabled` value to the update endpoint.
- Replace local state only with the server response.
- If the update fails, restore the last server-confirmed value and show an error message.
- When disabled, display `已关闭`; when enabled, display the formatted next-run time.

The control will not call the existing cancel endpoint. An already running daily sentiment job continues to completion, and the existing `停止全部同步` action remains the explicit way to cancel active work.

## Persistence And Runtime Semantics

`sync_tasks.enabled` is the single source of truth. No new table or environment variable is introduced.

State transitions:

1. `enabled -> disabled`: persist `enabled=false`, clear `next_run_at`, reload scheduler, and verify the job is absent.
2. `disabled -> enabled`: persist `enabled=true`, reload scheduler, and report the newly calculated `next_run_at`.
3. Repeating the current value is idempotent and returns the current authoritative state.

Manual API-triggered sentiment runs remain available in both states. The disabled state affects only the APScheduler registration for `每日舆情增量`.

## Error Handling

- Missing task row: return `404` with a stable message rather than silently creating another task.
- Invalid request payload: rely on Pydantic validation and return `422`.
- Scheduler reload failure: return `500`, log the exception type, and keep the persisted database state visible on the next read.
- Sync service unavailable: the frontend disables the switch and shows the existing service-unavailable treatment.

No crawler credentials, cookies, account identifiers, or position data are exposed by this API.

## Testing

Backend tests will verify:

- GET returns the persisted state and whether the APScheduler job exists.
- PUT false persists the disabled state, clears `next_run_at`, and removes the job.
- PUT true persists the enabled state and registers the job.
- Repeating the current value is idempotent.
- Missing task returns `404`.
- Manual sentiment sync remains callable while the scheduled task is disabled.

Frontend tests will verify:

- API request and response typing.
- The System Monitor renders the server-confirmed state.
- The switch is disabled while saving.
- A failed update restores the prior value and reports an error.

## Rollout

The current production value remains unchanged during deployment. After the sync service and frontend restart, the operator can change the state from System Monitor. The final rollout check will toggle the control in prod, verify the scheduler job disappears and reappears, and then restore the persisted state that existed before verification.

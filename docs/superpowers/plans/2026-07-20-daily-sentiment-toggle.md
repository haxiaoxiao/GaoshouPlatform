# Daily Sentiment Crawler Toggle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a persistent System Monitor switch that enables or disables only the scheduled `每日舆情增量` crawler while preserving manual ingestion and active runs.

**Architecture:** Reuse `SyncTask.enabled` as the single source of truth. Add a narrowly scoped sync-service GET/PUT API that persists the flag and reloads APScheduler, then expose it through the existing frontend sync API and a small Vue composable used by System Monitor.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy async ORM, APScheduler, pytest, Vue 3, TypeScript, Element Plus, Vitest

---

### Task 1: Daily Sentiment Scheduler Control API

**Files:**
- Modify: `backend/app/api/sync.py`
- Create: `backend/tests/api/test_daily_sentiment_schedule.py`

- [ ] **Step 1: Write failing GET and PUT API tests**

Create a focused API test module that seeds a `SyncTask` named `每日舆情增量`, overrides scheduler access with a fake scheduler, and exercises the sync-service app:

```python
@pytest.mark.asyncio
async def test_get_daily_sentiment_schedule_returns_persisted_and_runtime_state(...):
    task = await seed_daily_sentiment_task(enabled=True, next_run_at=NEXT_RUN)
    fake_scheduler.jobs[f"sync_task_{task.id}"] = object()

    response = await client.get("/api/data/sync/scheduler/daily-sentiment")

    assert response.status_code == 200
    assert response.json()["data"] == {
        "task_id": task.id,
        "name": "每日舆情增量",
        "enabled": True,
        "cron_expression": "30 22 * * *",
        "last_run_at": None,
        "next_run_at": NEXT_RUN.isoformat(),
        "scheduler_job_present": True,
    }


@pytest.mark.asyncio
async def test_put_daily_sentiment_schedule_disables_future_runs(...):
    task = await seed_daily_sentiment_task(enabled=True, next_run_at=NEXT_RUN)
    fake_scheduler.jobs[f"sync_task_{task.id}"] = object()

    response = await client.put(
        "/api/data/sync/scheduler/daily-sentiment",
        json={"enabled": False},
    )

    assert response.status_code == 200
    assert response.json()["data"]["enabled"] is False
    assert response.json()["data"]["next_run_at"] is None
    assert response.json()["data"]["scheduler_job_present"] is False
```

Add tests for enabling, an idempotent repeated value, a missing task returning 404, and scheduler reload failure returning 500 after the database value is persisted.

- [ ] **Step 2: Run tests and verify RED**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/api/test_daily_sentiment_schedule.py -q --basetemp=$env:TEMP\gaoshou-daily-crawler-api-red`

Expected: `404` for the new routes because the API contract is not implemented.

- [ ] **Step 3: Implement the request model and serializers**

In `backend/app/api/sync.py`, add:

```python
DAILY_SENTIMENT_TASK_NAME = "每日舆情增量"


class DailySentimentScheduleUpdate(BaseModel):
    enabled: bool


async def _get_daily_sentiment_task(session: AsyncSession) -> SyncTask:
    result = await session.execute(
        select(SyncTask).where(SyncTask.name == DAILY_SENTIMENT_TASK_NAME).limit(1)
    )
    task = result.scalar_one_or_none()
    if task is None:
        raise HTTPException(status_code=404, detail="daily sentiment schedule not found")
    return task


def _daily_sentiment_schedule_payload(task: SyncTask) -> dict[str, Any]:
    scheduler = get_scheduler()
    return {
        "task_id": task.id,
        "name": task.name,
        "enabled": bool(task.enabled),
        "cron_expression": task.cron_expression,
        "last_run_at": task.last_run_at.isoformat() if task.last_run_at else None,
        "next_run_at": task.next_run_at.isoformat() if task.next_run_at else None,
        "scheduler_job_present": scheduler.get_job(f"sync_task_{task.id}") is not None,
    }
```

Import `select`, `SyncTask`, `get_scheduler`, and `reload_scheduler_tasks` through existing backend modules.

- [ ] **Step 4: Implement GET and PUT routes**

Add:

```python
@router.get("/sync/scheduler/daily-sentiment")
async def get_daily_sentiment_schedule(
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    task = await _get_daily_sentiment_task(session)
    return {"code": 0, "message": "success", "data": _daily_sentiment_schedule_payload(task)}


@router.put("/sync/scheduler/daily-sentiment")
async def update_daily_sentiment_schedule(
    payload: DailySentimentScheduleUpdate,
    session: AsyncSession = Depends(get_async_session),
) -> dict[str, Any]:
    task = await _get_daily_sentiment_task(session)
    task.enabled = payload.enabled
    if not payload.enabled:
        task.next_run_at = None
    await session.commit()
    try:
        await reload_scheduler_tasks()
    except Exception as exc:
        logger.opt(exception=True).error(
            "Failed to reload scheduler after daily sentiment update: {}",
            type(exc).__name__,
        )
        raise HTTPException(status_code=500, detail="daily sentiment scheduler reload failed") from exc
    await session.refresh(task)
    return {"code": 0, "message": "success", "data": _daily_sentiment_schedule_payload(task)}
```

- [ ] **Step 5: Run API tests and verify GREEN**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/api/test_daily_sentiment_schedule.py tests/api/test_sync_queue.py -q --basetemp=$env:TEMP\gaoshou-daily-crawler-api-green`

Expected: all selected tests pass.

### Task 2: Frontend Sync API Contract

**Files:**
- Modify: `frontend/src/api/sync.ts`
- Create: `frontend/src/api/sync.test.ts`

- [ ] **Step 1: Write failing API client tests**

Mock `./request` and assert the wished-for calls:

```typescript
it('reads and updates the daily sentiment schedule', async () => {
  vi.mocked(request.get).mockResolvedValue(schedule)
  vi.mocked(request.put).mockResolvedValue({ ...schedule, enabled: false })

  await expect(syncApi.getDailySentimentSchedule()).resolves.toBe(schedule)
  await expect(syncApi.updateDailySentimentSchedule(false)).resolves.toMatchObject({ enabled: false })

  expect(request.get).toHaveBeenCalledWith('/data/sync/scheduler/daily-sentiment', { notifyError: false })
  expect(request.put).toHaveBeenCalledWith('/data/sync/scheduler/daily-sentiment', { enabled: false })
})
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cd frontend; npm test -- --run src/api/sync.test.ts`

Expected: fails because the two API client methods do not exist.

- [ ] **Step 3: Add the typed API methods**

Add:

```typescript
export interface DailySentimentSchedule {
  task_id: number
  name: string
  enabled: boolean
  cron_expression: string
  last_run_at: string | null
  next_run_at: string | null
  scheduler_job_present: boolean
}
```

Add to `syncApi`:

```typescript
getDailySentimentSchedule: () =>
  request.get<DailySentimentSchedule>('/data/sync/scheduler/daily-sentiment', { notifyError: false }),

updateDailySentimentSchedule: (enabled: boolean) =>
  request.put<DailySentimentSchedule>('/data/sync/scheduler/daily-sentiment', { enabled }),
```

- [ ] **Step 4: Run the API test and verify GREEN**

Run: `cd frontend; npm test -- --run src/api/sync.test.ts`

Expected: all selected tests pass.

### Task 3: Testable Daily Schedule State

**Files:**
- Create: `frontend/src/views/SystemMonitor/useDailySentimentSchedule.ts`
- Create: `frontend/src/views/SystemMonitor/useDailySentimentSchedule.test.ts`

- [ ] **Step 1: Write failing composable tests**

Test initial loading, server-confirmed update, and failure retention with an injected API:

```typescript
it('keeps the last confirmed state when an update fails', async () => {
  const api = {
    getDailySentimentSchedule: vi.fn().mockResolvedValue(enabledSchedule),
    updateDailySentimentSchedule: vi.fn().mockRejectedValue(new Error('offline')),
  }
  const control = useDailySentimentSchedule(api)
  await control.load()

  await expect(control.setEnabled(false)).rejects.toThrow('offline')

  expect(control.schedule.value).toEqual(enabledSchedule)
  expect(control.saving.value).toBe(false)
})
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cd frontend; npm test -- --run src/views/SystemMonitor/useDailySentimentSchedule.test.ts`

Expected: fails because the composable does not exist.

- [ ] **Step 3: Implement the composable**

Create a composable that accepts the two-method API shape, exposes `schedule`, `loading`, `saving`, `load()`, and `setEnabled(enabled)`, and assigns state only after a successful server response:

```typescript
export function useDailySentimentSchedule(api: DailySentimentScheduleApi = syncApi) {
  const schedule = ref<DailySentimentSchedule | null>(null)
  const loading = ref(false)
  const saving = ref(false)

  async function load() {
    loading.value = true
    try {
      schedule.value = await api.getDailySentimentSchedule()
    } finally {
      loading.value = false
    }
  }

  async function setEnabled(enabled: boolean) {
    if (saving.value) return schedule.value
    saving.value = true
    try {
      const confirmed = await api.updateDailySentimentSchedule(enabled)
      schedule.value = confirmed
      return confirmed
    } finally {
      saving.value = false
    }
  }

  return { schedule, loading, saving, load, setEnabled }
}
```

- [ ] **Step 4: Run composable tests and verify GREEN**

Run: `cd frontend; npm test -- --run src/views/SystemMonitor/useDailySentimentSchedule.test.ts`

Expected: all selected tests pass.

### Task 4: System Monitor Switch

**Files:**
- Modify: `frontend/src/views/SystemMonitor/index.vue`
- Verify: `frontend/src/views/SystemMonitor/useDailySentimentSchedule.test.ts`

- [ ] **Step 1: Wire schedule loading and update behavior**

Import and initialize the composable:

```typescript
const {
  schedule: dailySentimentSchedule,
  loading: loadingDailySentimentSchedule,
  saving: savingDailySentimentSchedule,
  load: loadDailySentimentSchedule,
  setEnabled: setDailySentimentScheduleEnabled,
} = useDailySentimentSchedule()
```

Include `loadDailySentimentSchedule()` in `loadOps()` and add:

```typescript
async function handleDailySentimentScheduleChange(value: string | number | boolean) {
  try {
    const updated = await setDailySentimentScheduleEnabled(Boolean(value))
    ElMessage.success(updated?.enabled ? '每日自动爬取已开启' : '每日自动爬取已关闭')
  } catch {
    ElMessage.error('每日自动爬取设置失败，已保留原状态')
  }
}
```

- [ ] **Step 2: Add the compact System Monitor control**

Add a `dock-group` before the live-trading guardrails:

```vue
<section class="dock-group">
  <div class="dock-row">
    <strong>每日自动爬取</strong>
    <el-tag
      :type="dailySentimentSchedule?.enabled ? 'success' : 'info'"
      effect="plain"
      size="small"
    >
      {{ dailySentimentSchedule?.enabled ? 'ON' : 'OFF' }}
    </el-tag>
  </div>
  <p>控制每日 22:30 舆情增量任务；关闭后手动抓取仍可使用。</p>
  <label class="guardrail-row" :class="{ 'guardrail-row--on': dailySentimentSchedule?.enabled }">
    <span>
      <strong>每日舆情增量</strong>
      <small>
        {{ dailySentimentSchedule?.enabled
          ? `下次 ${formatDateTime(dailySentimentSchedule.next_run_at)}`
          : '已关闭' }}
      </small>
    </span>
    <el-switch
      :model-value="dailySentimentSchedule?.enabled ?? false"
      :loading="savingDailySentimentSchedule"
      :disabled="loadingDailySentimentSchedule || savingDailySentimentSchedule || !dailySentimentSchedule"
      active-text="ON"
      inactive-text="OFF"
      inline-prompt
      @change="handleDailySentimentScheduleChange"
    />
  </label>
</section>
```

Using `model-value` rather than `v-model` prevents an optimistic UI state from surviving a failed request.

- [ ] **Step 3: Run frontend unit tests and build**

Run: `cd frontend; npm test -- --run src/api/sync.test.ts src/views/SystemMonitor/useDailySentimentSchedule.test.ts`

Run: `cd frontend; npm run build`

Expected: tests pass and Vite build exits `0`.

### Task 5: Regression And Prod Rollout

**Files:**
- Verify: `backend/app/api/sync.py`
- Verify: `frontend/src/api/sync.ts`
- Verify: `frontend/src/views/SystemMonitor/index.vue`

- [ ] **Step 1: Run focused backend and frontend checks**

Run: `cd backend; .\.venv\Scripts\python.exe -m pytest tests/api/test_daily_sentiment_schedule.py tests/api/test_sync_queue.py -q --basetemp=$env:TEMP\gaoshou-daily-crawler-final`

Run: `cd backend; .\.venv\Scripts\python.exe -m ruff check app/api/sync.py tests/api/test_daily_sentiment_schedule.py`

Run: `cd frontend; npm test -- --run src/api/sync.test.ts src/views/SystemMonitor/useDailySentimentSchedule.test.ts`

Run: `cd frontend; npm run build`

Expected: every command exits `0`.

- [ ] **Step 2: Audit the feature diff**

Run: `git diff --check`

Confirm no unrelated prod changes were reverted or staged, and confirm the endpoint cannot accept arbitrary task IDs or names.

- [ ] **Step 3: Restart prod services**

Restart the prod backend/sync/frontend through the repository Windows launcher or their existing process commands, preserving `.env.local`. Verify:

- `http://127.0.0.1:8810/health`
- scheduler status endpoint reports the daily task
- System Monitor loads at the active frontend port

- [ ] **Step 4: Verify the switch and restore original state**

Read and record the current `enabled` value. Toggle to the opposite state through the new API, verify `scheduler_job_present` and `next_run_at`, toggle back, and verify the original state is restored. Do not start a crawler or cancel an active run during this check.

- [ ] **Step 5: Leave a feature-only handoff**

Because prod may contain unrelated user changes, do not stage entire dirty files. Report the exact files and verification evidence; create a feature commit only from reviewed feature hunks if the worktree state permits it.

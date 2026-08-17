import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./request', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
  },
}))

import request from './request'
import { syncApi, type DailySentimentSchedule } from './sync'

const enabledSchedule: DailySentimentSchedule = {
  task_id: 1,
  name: '每日舆情增量',
  enabled: true,
  cron_expression: '30 22 * * *',
  last_run_at: null,
  next_run_at: '2026-07-21T22:30:00',
  scheduler_job_present: true,
}

beforeEach(() => {
  vi.mocked(request.get).mockReset()
  vi.mocked(request.put).mockReset()
})

describe('daily sentiment schedule API', () => {
  it('reads and updates the daily sentiment schedule', async () => {
    const disabledSchedule = {
      ...enabledSchedule,
      enabled: false,
      next_run_at: null,
      scheduler_job_present: false,
    }
    vi.mocked(request.get).mockResolvedValue(enabledSchedule)
    vi.mocked(request.put).mockResolvedValue(disabledSchedule)

    await expect(syncApi.getDailySentimentSchedule()).resolves.toBe(enabledSchedule)
    await expect(syncApi.updateDailySentimentSchedule(false)).resolves.toBe(disabledSchedule)

    expect(request.get).toHaveBeenCalledWith(
      '/data/sync/scheduler/daily-sentiment',
      { notifyError: false },
    )
    expect(request.put).toHaveBeenCalledWith(
      '/data/sync/scheduler/daily-sentiment',
      { enabled: false },
    )
  })
})

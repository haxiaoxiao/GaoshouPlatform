import { describe, expect, it, vi } from 'vitest'

import type { DailySentimentSchedule } from '@/api/sync'
import { useDailySentimentSchedule } from './useDailySentimentSchedule'

const enabledSchedule: DailySentimentSchedule = {
  task_id: 1,
  name: '每日舆情增量',
  enabled: true,
  cron_expression: '30 22 * * *',
  last_run_at: null,
  next_run_at: '2026-07-21T22:30:00',
  scheduler_job_present: true,
}

describe('useDailySentimentSchedule', () => {
  it('loads the authoritative schedule state', async () => {
    const api = {
      getDailySentimentSchedule: vi.fn().mockResolvedValue(enabledSchedule),
      updateDailySentimentSchedule: vi.fn(),
    }
    const control = useDailySentimentSchedule(api)

    const pending = control.load()
    expect(control.loading.value).toBe(true)
    await pending

    expect(control.schedule.value).toEqual(enabledSchedule)
    expect(control.loading.value).toBe(false)
  })

  it('replaces local state only with the confirmed update response', async () => {
    const disabledSchedule = {
      ...enabledSchedule,
      enabled: false,
      next_run_at: null,
      scheduler_job_present: false,
    }
    const api = {
      getDailySentimentSchedule: vi.fn().mockResolvedValue(enabledSchedule),
      updateDailySentimentSchedule: vi.fn().mockResolvedValue(disabledSchedule),
    }
    const control = useDailySentimentSchedule(api)
    await control.load()

    const pending = control.setEnabled(false)
    expect(control.saving.value).toBe(true)
    await expect(pending).resolves.toEqual(disabledSchedule)

    expect(api.updateDailySentimentSchedule).toHaveBeenCalledWith(false)
    expect(control.schedule.value).toEqual(disabledSchedule)
    expect(control.saving.value).toBe(false)
  })

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
})

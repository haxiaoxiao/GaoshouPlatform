// @vitest-environment jsdom

import { flushPromises, shallowMount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import SystemMonitor from './index.vue'

const { push, getDailySentimentSchedule, updateDailySentimentSchedule } = vi.hoisted(() => ({
  push: vi.fn(),
  getDailySentimentSchedule: vi.fn(),
  updateDailySentimentSchedule: vi.fn(),
}))

vi.mock('vue-router', () => ({
  useRouter: () => ({ push }),
}))

vi.mock('@/app/pageContext', () => ({
  usePageContext: vi.fn(),
}))

vi.mock('@/api/system', () => ({
  getLlmGatewayView: () => ({
    readiness: 'ready',
    enabled: 0,
    total: 0,
    primary: null,
    recentError: null,
  }),
  systemApi: {
    getStatus: vi.fn().mockResolvedValue({ status: 'ok', database: 'ok' }),
    dataSummary: vi.fn().mockResolvedValue({
      overall_status: 'good',
      by_key: {},
      market_data_backend: 'parquet',
    }),
    getDevDataMode: vi.fn().mockResolvedValue(null),
    getLiveTradingGuardrails: vi.fn().mockResolvedValue(null),
    listLlmEndpoints: vi.fn().mockResolvedValue([]),
  },
}))

vi.mock('@/api/sync', () => ({
  syncApi: {
    getStatus: vi.fn().mockResolvedValue({
      status: 'idle',
      sync_type: null,
      details: {},
      sync_service_available: true,
    }),
    getLogs: vi.fn().mockResolvedValue([]),
    cancelAll: vi.fn(),
    getDailySentimentSchedule,
    updateDailySentimentSchedule,
  },
}))

vi.mock('@/api/runtimeTasks', () => ({
  runtimeTaskApi: { list: vi.fn().mockResolvedValue([]) },
}))

vi.mock('@/api/liveTrading', () => ({
  liveTradingApi: { status: vi.fn().mockResolvedValue({}) },
}))

describe('System Monitor daily sentiment schedule', () => {
  beforeEach(() => {
    push.mockReset()
    getDailySentimentSchedule.mockReset().mockResolvedValue({
      task_id: 1,
      name: '每日舆情增量',
      enabled: true,
      cron_expression: '30 22 * * *',
      last_run_at: null,
      next_run_at: '2026-07-21T22:30:00',
      scheduler_job_present: true,
    })
    updateDailySentimentSchedule.mockReset().mockResolvedValue({
      task_id: 1,
      name: '每日舆情增量',
      enabled: false,
      cron_expression: '30 22 * * *',
      last_run_at: null,
      next_run_at: null,
      scheduler_job_present: false,
    })
  })

  it('renders server state and updates from the switch', async () => {
    const wrapper = shallowMount(SystemMonitor)
    await flushPromises()

    expect(wrapper.text()).toContain('每日自动爬取')
    expect(wrapper.text()).toContain('每日舆情增量')
    expect(wrapper.text()).toContain('下次 2026-07-21 22:30')

    const scheduleSwitch = wrapper
      .findAllComponents({ name: 'ElSwitch' })
      .find(item => item.props('modelValue') === true)
    expect(scheduleSwitch).toBeDefined()

    scheduleSwitch!.vm.$emit('change', false)
    await flushPromises()

    expect(updateDailySentimentSchedule).toHaveBeenCalledWith(false)
    expect(wrapper.text()).toContain('已关闭')
  })
})

import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./request', () => ({
  default: {
    get: vi.fn(),
    put: vi.fn(),
  },
}))

import * as systemModule from './system'
import request from './request'

beforeEach(() => {
  vi.mocked(request.get).mockReset()
})

describe('readiness context', () => {
  it('uses the daily dataset date and exposes live order risk', () => {
    const formatReadinessContext = (
      systemModule as typeof systemModule & {
        formatReadinessContext?: (payload: systemModule.PlatformReadiness) => unknown
      }
    ).formatReadinessContext

    expect(formatReadinessContext).toBeTypeOf('function')
    expect(formatReadinessContext!({
      as_of: '2026-07-10',
      environment: 'live',
      overall_status: 'degraded',
      datasets: {
        klines_daily: {
          dataset: 'klines_daily',
          status: 'ready',
          max_date: '2026-07-09',
        },
      },
      trading: {
        order_submit_enabled: true,
        auto_execute_enabled: false,
        control_secret_configured: false,
      },
    })).toEqual({
      environmentLabel: 'LIVE',
      dataDate: '2026-07-09',
      readinessLabel: '数据降级',
      orderSubmitLabel: '真实下单开启',
      orderSubmitRisk: true,
    })
  })

  it('shares readiness requests within the cache window', async () => {
    const payload = {
      as_of: '2026-07-10',
      environment: 'paper' as const,
      overall_status: 'ready' as const,
      datasets: {},
      trading: {
        order_submit_enabled: false,
        auto_execute_enabled: false,
        control_secret_configured: false,
      },
    }
    vi.mocked(request.get).mockResolvedValue(payload)

    const [first, second] = await Promise.all([
      systemModule.systemApi.readiness(),
      systemModule.systemApi.readiness(),
    ])

    expect(first).toBe(payload)
    expect(second).toBe(payload)
    expect(request.get).toHaveBeenCalledOnce()
    expect(request.get).toHaveBeenCalledWith('/v1/readiness')
  })
})

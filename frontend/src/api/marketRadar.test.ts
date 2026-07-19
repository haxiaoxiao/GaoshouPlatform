import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./request', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
    patch: vi.fn(),
    delete: vi.fn(),
  },
}))

import request from './request'
import { marketRadarApi } from './marketRadar'

describe('market radar API client', () => {
  beforeEach(() => {
    vi.mocked(request.get).mockReset()
    vi.mocked(request.post).mockReset()
    vi.mocked(request.patch).mockReset()
    vi.mocked(request.delete).mockReset()
  })

  it('loads the dashboard resources with typed query parameters', async () => {
    vi.mocked(request.get).mockResolvedValue({})

    await marketRadarApi.overview()
    await marketRadarApi.breadth({ days: 15, mode: 'percent' })
    await marketRadarApi.limitLadder('2026-07-18')
    await marketRadarApi.crowding({ scope: 'sector', subject: '电子' })
    await marketRadarApi.sectors('2026-07-18')

    expect(request.get).toHaveBeenNthCalledWith(1, '/market-radar/overview', undefined)
    expect(request.get).toHaveBeenNthCalledWith(2, '/market-radar/breadth', {
      params: { days: 15, mode: 'percent' },
    })
    expect(request.get).toHaveBeenNthCalledWith(3, '/market-radar/limit-ladder', {
      params: { trade_date: '2026-07-18' },
    })
    expect(request.get).toHaveBeenNthCalledWith(4, '/market-radar/crowding', {
      params: { scope: 'sector', subject: '电子' },
    })
    expect(request.get).toHaveBeenNthCalledWith(5, '/market-radar/sectors', {
      params: { trade_date: '2026-07-18' },
    })
  })

  it('loads only active high alerts for persistent notifications', async () => {
    vi.mocked(request.get).mockResolvedValue({})

    await marketRadarApi.activeHighAlerts({ signal: new AbortController().signal })

    expect(request.get).toHaveBeenCalledWith('/market-radar/alerts', expect.objectContaining({
      params: { status: 'active', severity: 'high', page: 1, page_size: 100 },
      notifyError: false,
      signal: expect.any(AbortSignal),
    }))
  })

  it('acknowledges an alert and exposes the same-origin stream URL', async () => {
    vi.mocked(request.post).mockResolvedValue({})

    await marketRadarApi.acknowledgeAlert(42)

    expect(request.post).toHaveBeenCalledWith('/market-radar/alerts/42/acknowledge')
    expect(marketRadarApi.streamUrl()).toBe('/api/market-radar/stream')
  })
})

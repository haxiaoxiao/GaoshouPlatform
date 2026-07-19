/** @vitest-environment jsdom */

import { createPinia, setActivePinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const marketApi = vi.hoisted(() => ({
  acknowledgeAlert: vi.fn(),
  activeHighAlerts: vi.fn(),
}))
vi.mock('@/api/marketRadar', async importOriginal => {
  const actual = await importOriginal<typeof import('@/api/marketRadar')>()
  return { ...actual, marketRadarApi: { ...actual.marketRadarApi, ...marketApi } }
})

import type { MarketRadarAlert } from '@/api/marketRadar'
import { useNotificationStore } from './notification'

const marketAlert = (overrides: Partial<MarketRadarAlert> = {}): MarketRadarAlert => ({
  id: 17,
  rule_id: 1,
  snapshot_id: 2,
  scope: 'market',
  subject: '*',
  direction: 'down',
  severity: 'high',
  status: 'active',
  title: '市场跌速预警',
  explanation: '核心指数五分钟跌幅超过阈值',
  dedupe_key: 'market-drop',
  evidence: {},
  triggered_at: '2026-07-18T10:00:00',
  last_seen_at: '2026-07-18T10:00:00',
  acknowledged_at: null,
  dismissed_at: null,
  resolved_at: null,
  last_notified_at: '2026-07-18T10:00:00',
  occurrence_count: 1,
  clear_streak: 0,
  ...overrides,
})

describe('persistent market alert notifications', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    marketApi.acknowledgeAlert.mockReset()
    marketApi.activeHighAlerts.mockReset()
  })

  it('adds only active high alerts under a separate stable ID namespace', () => {
    const store = useNotificationStore()

    store.addMarketAlert(marketAlert())
    store.addMarketAlert(marketAlert())
    store.addMarketAlert(marketAlert({ id: 18, severity: 'medium' }))
    store.addMarketAlert(marketAlert({ id: 19, status: 'resolved' }))

    expect(store.notifications).toHaveLength(1)
    expect(store.notifications[0]).toMatchObject({
      id: 'market-alert:17',
      marketAlertId: 17,
      source: 'market',
      read: false,
      route: '/market-radar?alert=17',
    })
  })

  it('synchronizes removals with active backend state without touching task notifications', () => {
    const store = useNotificationStore()
    store.addNotification({ id: 'task:1:done', type: 'success', title: '任务', message: '完成' })
    store.addMarketAlert(marketAlert())

    store.syncMarketAlerts([])

    expect(store.notifications.map(item => item.id)).toEqual(['task:1:done'])
  })

  it('marks a market alert read only after backend acknowledgement succeeds', async () => {
    const store = useNotificationStore()
    store.addMarketAlert(marketAlert())
    marketApi.acknowledgeAlert.mockRejectedValueOnce(new Error('offline'))

    await expect(store.acknowledgeMarketAlert('market-alert:17')).rejects.toThrow('offline')
    expect(store.notifications[0].read).toBe(false)

    marketApi.acknowledgeAlert.mockResolvedValueOnce({
      data: marketAlert({ status: 'acknowledged' }),
    })
    await store.acknowledgeMarketAlert('market-alert:17')
    expect(store.notifications[0].read).toBe(true)

    await store.acknowledgeMarketAlert('market-alert:17')
    expect(marketApi.acknowledgeAlert).toHaveBeenCalledTimes(2)
  })

  it('restores unread high alerts directly from the backend for the global panel', async () => {
    const store = useNotificationStore()
    marketApi.activeHighAlerts.mockResolvedValueOnce({
      data: { items: [marketAlert()], total: 1, page: 1, page_size: 100 },
    })

    await store.pollMarketAlerts()

    expect(marketApi.activeHighAlerts).toHaveBeenCalledOnce()
    expect(store.notifications[0].id).toBe('market-alert:17')
  })

  it('does not let an old active poll restore unread after acknowledgement', async () => {
    const store = useNotificationStore()
    store.addMarketAlert(marketAlert())
    const oldPoll = Promise.withResolvers<{
      data: { items: MarketRadarAlert[]; total: number; page: number; page_size: number }
    }>()
    marketApi.activeHighAlerts.mockImplementationOnce(() => oldPoll.promise)
    const polling = store.pollMarketAlerts()
    marketApi.acknowledgeAlert.mockResolvedValueOnce({
      data: marketAlert({ status: 'acknowledged' }),
    })

    await store.acknowledgeMarketAlert('market-alert:17')
    oldPoll.resolve({
      data: { items: [marketAlert()], total: 1, page: 1, page_size: 100 },
    })
    await polling

    expect(store.notifications[0]).toMatchObject({
      id: 'market-alert:17',
      read: true,
    })
  })
})

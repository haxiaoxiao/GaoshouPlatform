/** @vitest-environment jsdom */

import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const push = vi.hoisted(() => vi.fn())
const acknowledgeAlert = vi.hoisted(() => vi.fn())
vi.mock('vue-router', () => ({ useRouter: () => ({ push }) }))
vi.mock('@/api/marketRadar', async importOriginal => {
  const actual = await importOriginal<typeof import('@/api/marketRadar')>()
  return { ...actual, marketRadarApi: { ...actual.marketRadarApi, acknowledgeAlert } }
})

import type { MarketRadarAlert } from '@/api/marketRadar'
import { useNotificationStore } from '@/stores/notification'
import NotificationPanel from './NotificationPanel.vue'

const alert: MarketRadarAlert = {
  id: 23,
  rule_id: 1,
  snapshot_id: null,
  scope: 'market',
  subject: '*',
  direction: 'down',
  severity: 'high',
  status: 'active',
  title: '高风险预警',
  explanation: '市场跌幅扩大',
  dedupe_key: 'alert-23',
  evidence: {},
  triggered_at: '2026-07-18T10:00:00',
  last_seen_at: '2026-07-18T10:00:00',
  acknowledged_at: null,
  dismissed_at: null,
  resolved_at: null,
  last_notified_at: null,
  occurrence_count: 1,
  clear_streak: 0,
}

describe('NotificationPanel market alerts', () => {
  beforeEach(() => {
    const pinia = createPinia()
    setActivePinia(pinia)
    push.mockReset()
    acknowledgeAlert.mockReset()
  })

  it('acknowledges before navigating and keeps a failed alert unread', async () => {
    const store = useNotificationStore()
    store.addMarketAlert(alert)
    acknowledgeAlert.mockRejectedValueOnce(new Error('offline'))
    const wrapper = mount(NotificationPanel, { global: { plugins: [store.$pinia] } })

    await wrapper.get('.notification-item').trigger('click')
    await flushPromises()
    expect(push).not.toHaveBeenCalled()
    expect(store.notifications[0].read).toBe(false)

    acknowledgeAlert.mockResolvedValueOnce({ data: { ...alert, status: 'acknowledged' } })
    await wrapper.get('.notification-item').trigger('click')
    await flushPromises()
    expect(acknowledgeAlert).toHaveBeenCalledWith(23)
    expect(push).toHaveBeenCalledWith('/market-radar?alert=23')
    expect(store.notifications[0].read).toBe(true)
  })
})

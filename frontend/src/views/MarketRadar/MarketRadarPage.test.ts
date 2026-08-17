/** @vitest-environment jsdom */

import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'
import { readFileSync } from 'node:fs'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const api = vi.hoisted(() => ({
  breadth: vi.fn(),
  limitLadder: vi.fn(),
  crowding: vi.fn(),
  sectors: vi.fn(),
  alerts: vi.fn(),
  acknowledgeAlert: vi.fn(),
  dismissAlert: vi.fn(),
}))

const store = vi.hoisted(() => ({
  overview: null as unknown,
  alerts: [] as unknown[],
  connectionState: 'live',
  realtimeMode: 'push',
  lastError: null as string | null,
  start: vi.fn(),
  stop: vi.fn(),
  refreshNow: vi.fn(),
}))

vi.mock('@/api/marketRadar', () => ({ marketRadarApi: api }))
vi.mock('@/stores/marketRadar', () => ({ useMarketRadarStore: () => store }))
vi.mock('vue-router', () => ({ useRoute: () => ({ query: { alert: '42' } }) }))
vi.mock('@/lib/echarts', () => ({
  init: () => ({ setOption: vi.fn(), resize: vi.fn(), dispose: vi.fn() }),
  graphic: {},
}))

import MarketRadarPage from './index.vue'

const envelope = (data: unknown, status = 'fresh') => ({
  as_of: '2026-07-18T15:20:00',
  computed_at: '2026-07-18T15:21:00',
  status,
  confidence: 0.8,
  realtime_mode: 'push',
  sources: [],
  data,
})

describe('market radar workbench', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    Object.values(api).forEach(mock => mock.mockReset())
    store.start.mockReset()
    store.stop.mockReset()
    store.refreshNow.mockReset()
    store.overview = envelope({
      risk_level: 'high',
      market_breadth: { median_return_pct: -2.8, decline_ratio: 0.84 },
      crowding: { value: 86, label: '高拥挤', status: 'fresh' },
      emotion: { value: 18, label: null, status: 'partial', reason: 'reduced formula' },
      alert_counts: { active: 2, active_high: 1 },
    })
    store.alerts = []
    api.breadth.mockResolvedValue(envelope({ status: 'fresh', days: [] }))
    api.limitLadder.mockResolvedValue(envelope({ status: 'stale', rows: [] }, 'stale'))
    api.crowding.mockResolvedValue(envelope({ status: 'fresh', score: { value: 86 } }))
    api.sectors.mockResolvedValue(envelope({ status: 'fresh', sectors: [] }))
    api.alerts.mockResolvedValue(envelope({ items: [], total: 0, page: 1, page_size: 50 }))
  })

  it('loads all radar surfaces and uses a scrolling page root', async () => {
    const wrapper = mount(MarketRadarPage)
    await flushPromises()

    expect(store.start).toHaveBeenCalledOnce()
    expect(api.breadth).toHaveBeenCalledWith({ days: 15, mode: 'percent' })
    expect(api.limitLadder).toHaveBeenCalledOnce()
    expect(api.crowding).toHaveBeenCalledWith({ scope: 'market' })
    expect(api.sectors).toHaveBeenCalledOnce()
    expect(api.alerts).toHaveBeenCalledOnce()
    expect(wrapper.get('main').classes()).toContain('page-scroll')
    expect(wrapper.text()).toContain('市场趋势雷达')
    expect(wrapper.text()).toContain('全 A 盈亏分布')
    expect(wrapper.text()).toContain('预警中心')
  })

  it('keeps top-level surfaces from shrinking into adjacent panels', () => {
    const css = readFileSync('src/views/MarketRadar/market-radar.css', 'utf8')

    expect(css).toMatch(/\.market-radar-page\s*>\s*\*\s*{[^}]*flex:\s*0\s+0\s+auto/)
  })

  it('renders unavailable values as dashes and keeps the reduced emotion explanation', async () => {
    const wrapper = mount(MarketRadarPage)
    await flushPromises()

    expect(wrapper.get('[data-testid="emotion-score"]').text()).toContain('18.0')
    expect(wrapper.get('[data-testid="emotion-score"]').text()).toContain('标签不可用')
    expect(wrapper.get('[data-testid="emotion-score"]').attributes('title')).toContain('reduced formula')
    expect(wrapper.text()).toContain('—')
  })

  it('renders the actual intraday compact overview contract', async () => {
    store.overview = envelope({
      mode: 'push',
      market_median_return_pct: -1.73,
      decline_ratio: 0.76,
      status: 'fresh',
      alert_counts: { active: 0, active_high: 0 },
    })

    const wrapper = mount(MarketRadarPage)
    await flushPromises()

    expect(wrapper.text()).toContain('-1.73%')
    expect(wrapper.text()).toContain('下跌 76.0%')
  })

  it('opens and focuses the alert requested by the route query', async () => {
    api.alerts.mockResolvedValueOnce(envelope({
      items: [{
        id: 42,
        rule_id: 1,
        snapshot_id: 1,
        scope: 'market',
        subject: 'ALL',
        direction: 'down',
        severity: 'high',
        status: 'active',
        title: '市场普跌',
        explanation: '下跌占比超过阈值',
        dedupe_key: 'x',
        evidence: { decline_ratio: 0.84 },
        triggered_at: '2026-07-18T14:00:00',
        last_seen_at: '2026-07-18T15:00:00',
        acknowledged_at: null,
        dismissed_at: null,
        resolved_at: null,
        last_notified_at: null,
        occurrence_count: 2,
        clear_streak: 0,
      }],
      total: 1,
      page: 1,
      page_size: 50,
    }))
    const scrollIntoView = vi.fn()
    Element.prototype.scrollIntoView = scrollIntoView

    const wrapper = mount(MarketRadarPage)
    await flushPromises()

    expect(wrapper.get('#market-alert-42').classes()).toContain('alert-row--focused')
    expect(scrollIntoView).toHaveBeenCalled()
    expect(wrapper.text()).toContain('下跌占比超过阈值')
  })

  it('isolates a failed surface request instead of discarding successful panels', async () => {
    api.breadth.mockRejectedValueOnce(new Error('breadth offline'))

    const wrapper = mount(MarketRadarPage)
    await flushPromises()

    expect(wrapper.text()).toContain('breadth offline')
    expect(wrapper.text()).toContain('86.0')
    expect(api.limitLadder).toHaveBeenCalledOnce()
    expect(api.crowding).toHaveBeenCalledOnce()
    expect(api.sectors).toHaveBeenCalledOnce()
  })

  it('refreshes independent surfaces when the overview refresh fails', async () => {
    const wrapper = mount(MarketRadarPage)
    await flushPromises()
    Object.values(api).forEach(mock => mock.mockClear())
    store.refreshNow.mockRejectedValueOnce(new Error('overview offline'))

    await wrapper.get('button[aria-label="立即刷新市场雷达"]').trigger('click')
    await flushPromises()

    expect(api.breadth).toHaveBeenCalledOnce()
    expect(api.limitLadder).toHaveBeenCalledOnce()
    expect(api.crowding).toHaveBeenCalledOnce()
    expect(api.sectors).toHaveBeenCalledOnce()
    expect(api.alerts).toHaveBeenCalledOnce()
    expect(wrapper.text()).toContain('overview offline')
  })

  it('keeps a successful acknowledgement authoritative over an older active stream event', async () => {
    const active = {
      id: 42,
      rule_id: 1,
      snapshot_id: 1,
      scope: 'market',
      subject: 'ALL',
      direction: 'down',
      severity: 'high',
      status: 'active',
      title: '市场普跌',
      explanation: '下跌占比超过阈值',
      dedupe_key: 'x',
      evidence: {},
      triggered_at: '2026-07-18T14:00:00',
      last_seen_at: '2026-07-18T15:00:00',
      acknowledged_at: null,
      dismissed_at: null,
      resolved_at: null,
      last_notified_at: null,
      occurrence_count: 2,
      clear_streak: 0,
    } as const
    store.alerts = [active]
    api.alerts.mockResolvedValueOnce(envelope({ items: [active], total: 1, page: 1, page_size: 50 }))
    api.acknowledgeAlert.mockResolvedValueOnce(envelope({
      ...active,
      status: 'acknowledged',
      acknowledged_at: '2026-07-18T15:01:00',
    }))

    const wrapper = mount(MarketRadarPage)
    await flushPromises()
    await wrapper.get('.alert-actions button').trigger('click')
    await flushPromises()

    expect(wrapper.get('#market-alert-42').text()).toContain('已确认')
    expect(wrapper.find('.alert-actions').exists()).toBe(false)
  })
})

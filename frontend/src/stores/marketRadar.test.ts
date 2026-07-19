/** @vitest-environment jsdom */

import { createPinia, setActivePinia } from 'pinia'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const api = vi.hoisted(() => ({
  overview: vi.fn(),
  activeHighAlerts: vi.fn(),
  streamUrl: vi.fn(() => '/api/market-radar/stream'),
}))
const notifications = vi.hoisted(() => ({
  syncMarketAlerts: vi.fn(),
  addMarketAlert: vi.fn(),
}))

vi.mock('@/api/marketRadar', () => ({ marketRadarApi: api }))
vi.mock('./notification', () => ({
  useNotificationStore: () => notifications,
}))

import type {
  MarketRadarAlert,
  MarketRadarEnvelope,
  MarketRadarOverview,
} from '@/api/marketRadar'
import { useMarketRadarStore } from './marketRadar'

type Listener = (event: MessageEvent<string>) => void

class FakeEventSource {
  static instances: FakeEventSource[] = []

  readonly url: string
  onopen: ((event: Event) => void) | null = null
  onerror: ((event: Event) => void) | null = null
  closed = false
  private listeners = new Map<string, Listener[]>()

  constructor(url: string | URL) {
    this.url = String(url)
    FakeEventSource.instances.push(this)
  }

  addEventListener(type: string, listener: EventListenerOrEventListenerObject): void {
    const callback = typeof listener === 'function'
      ? listener as Listener
      : event => listener.handleEvent(event)
    this.listeners.set(type, [...(this.listeners.get(type) || []), callback])
  }

  close(): void {
    this.closed = true
  }

  open(): void {
    this.onopen?.(new Event('open'))
  }

  fail(): void {
    this.onerror?.(new Event('error'))
  }

  emit(type: string, data: unknown): void {
    const event = new MessageEvent(type, { data: JSON.stringify(data) })
    for (const listener of this.listeners.get(type) || []) listener(event)
  }
}

const envelope = (
  computedAt: string,
  data: MarketRadarOverview = { alert_counts: { active_high: 0 } },
): MarketRadarEnvelope<MarketRadarOverview> => ({
  as_of: '2026-07-18T15:20:00',
  computed_at: computedAt,
  status: 'fresh',
  confidence: 0.9,
  realtime_mode: 'push',
  sources: [],
  data,
})

const alert = (id: number): MarketRadarAlert => ({
  id,
  rule_id: 1,
  snapshot_id: 2,
  scope: 'market',
  subject: '*',
  direction: 'down',
  severity: 'high',
  status: 'active',
  title: `预警 ${id}`,
  explanation: '市场快速下跌',
  dedupe_key: `market-${id}`,
  evidence: {},
  triggered_at: '2026-07-18T10:00:00',
  last_seen_at: '2026-07-18T10:00:00',
  acknowledged_at: null,
  dismissed_at: null,
  resolved_at: null,
  last_notified_at: '2026-07-18T10:00:00',
  occurrence_count: 1,
  clear_streak: 0,
})

const streamPayload = (data: Record<string, unknown>, sequence = 1) => ({
  ...data,
  schema_version: 1 as const,
  event_id: String(sequence),
  sequence,
  occurred_at: '2026-07-18T10:00:00',
})

async function settle(): Promise<void> {
  await Promise.resolve()
  await Promise.resolve()
  await Promise.resolve()
}

describe('market radar realtime store', () => {
  let store: ReturnType<typeof useMarketRadarStore>

  beforeEach(() => {
    vi.useFakeTimers()
    setActivePinia(createPinia())
    FakeEventSource.instances = []
    vi.stubGlobal('EventSource', FakeEventSource)
    api.overview.mockReset().mockResolvedValue(envelope('2026-07-18T10:00:00'))
    api.activeHighAlerts.mockReset().mockResolvedValue({
      ...envelope('2026-07-18T10:00:00'),
      data: { items: [alert(7)], total: 1, page: 1, page_size: 100 },
    })
    api.streamUrl.mockClear()
    notifications.syncMarketAlerts.mockReset()
    notifications.addMarketAlert.mockReset()
    store = useMarketRadarStore()
  })

  afterEach(() => {
    store.stop()
    vi.useRealTimers()
    vi.unstubAllGlobals()
    Reflect.deleteProperty(document, 'visibilityState')
  })

  it('loads REST state before connecting and applies fixed SSE event types', async () => {
    await store.start()

    expect(store.overview?.computed_at).toBe('2026-07-18T10:00:00')
    expect(store.alerts.map(item => item.id)).toEqual([7])
    expect(notifications.syncMarketAlerts).toHaveBeenCalledWith([expect.objectContaining({ id: 7 })])
    expect(FakeEventSource.instances).toHaveLength(1)
    expect(store.connectionState).toBe('connecting')

    const source = FakeEventSource.instances[0]
    source.open()
    await settle()
    expect(store.connectionState).toBe('live')

    source.emit('mode', streamPayload({ mode: 'polling_30s' }, 2))
    source.emit('snapshot', streamPayload({
      ...envelope('2026-07-18T10:00:02'),
      realtime_mode: 'polling_30s',
    }, 3))
    source.emit('alert', streamPayload(alert(8) as unknown as Record<string, unknown>, 4))
    source.emit('alert', streamPayload(alert(8) as unknown as Record<string, unknown>, 5))

    expect(store.realtimeMode).toBe('polling_30s')
    expect(store.overview?.computed_at).toBe('2026-07-18T10:00:02')
    expect(store.alerts.map(item => item.id)).toEqual([8, 7])
    expect(notifications.addMarketAlert).toHaveBeenCalledTimes(1)
  })

  it('normalizes a full-metrics SSE snapshot while retaining it for live panels', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    source.open()
    await settle()

    source.emit('snapshot', streamPayload({
      ...envelope('2026-07-18T10:00:02', {
        overview: {
          mode: 'push',
          market_median_return_pct: -1.6,
          decline_ratio: 0.72,
          status: 'fresh',
        },
        breadth: { status: 'fresh', buckets: { le_neg_8: { percentage: 2.1 } } },
        indices: { '000001.SH': { return_pct: -0.8, status: 'fresh' } },
        limit_ladder: { status: 'unavailable', reason: 'intraday source not loaded' },
      }),
    }, 2))

    expect(store.overview?.data).toMatchObject({ market_median_return_pct: -1.6 })
    expect(store.overview?.data).not.toHaveProperty('overview')
    expect(store.latestSnapshot?.data).toMatchObject({
      breadth: { status: 'fresh' },
      limit_ladder: { status: 'unavailable' },
    })
  })

  it('falls back after 20 seconds without heartbeat and polls at most once every 30 seconds', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    source.open()
    await settle()

    const pendingOverview = Promise.withResolvers<MarketRadarEnvelope<MarketRadarOverview>>()
    const pendingAlerts = Promise.withResolvers<Awaited<ReturnType<typeof api.activeHighAlerts>>>()
    api.overview.mockImplementationOnce(() => pendingOverview.promise)
    api.activeHighAlerts.mockImplementationOnce(() => pendingAlerts.promise)
    await vi.advanceTimersByTimeAsync(20_000)

    expect(source.closed).toBe(true)
    expect(store.connectionState).toBe('fallback_polling')
    const callsAfterFallback = api.overview.mock.calls.length
    const alertCallsAfterFallback = api.activeHighAlerts.mock.calls.length
    expect(alertCallsAfterFallback).toBe(callsAfterFallback)

    await vi.advanceTimersByTimeAsync(30_000)
    expect(api.overview).toHaveBeenCalledTimes(callsAfterFallback)
    expect(api.activeHighAlerts).toHaveBeenCalledTimes(alertCallsAfterFallback)

    pendingOverview.resolve(envelope('2026-07-18T10:00:30'))
    pendingAlerts.resolve({
      ...envelope('2026-07-18T10:00:30'),
      data: { items: [alert(7)], total: 1, page: 1, page_size: 100 },
    })
    await settle()
    await vi.advanceTimersByTimeAsync(30_000)
    expect(api.overview.mock.calls.length).toBeGreaterThan(callsAfterFallback)
    expect(api.activeHighAlerts.mock.calls.length).toBeGreaterThan(alertCallsAfterFallback)
  })

  it('resets the 20 second watchdog only when a heartbeat arrives', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    source.open()
    await settle()

    await vi.advanceTimersByTimeAsync(19_000)
    source.emit('heartbeat', streamPayload({ at: '2026-07-18T10:00:19' }, 2))
    await vi.advanceTimersByTimeAsync(19_999)
    expect(source.closed).toBe(false)
    expect(store.connectionState).toBe('live')

    await vi.advanceTimersByTimeAsync(1)
    expect(source.closed).toBe(true)
    expect(store.connectionState).toBe('fallback_polling')
  })

  it('reconnects at 5, 10, 20, 40 and capped 60 second delays', async () => {
    await store.start()

    const delays = [5_000, 10_000, 20_000, 40_000, 60_000, 60_000]
    for (const [index, delay] of delays.entries()) {
      FakeEventSource.instances.at(-1)?.fail()
      await settle()
      await vi.advanceTimersByTimeAsync(delay - 1)
      expect(FakeEventSource.instances).toHaveLength(index + 1)
      await vi.advanceTimersByTimeAsync(1)
      expect(FakeEventSource.instances).toHaveLength(index + 2)
    }
  })

  it('does not let a stale REST response overwrite a recovered stream snapshot', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    const compensation = Promise.withResolvers<MarketRadarEnvelope<MarketRadarOverview>>()
    api.overview.mockImplementationOnce(() => compensation.promise)

    source.open()
    source.emit('snapshot', streamPayload(envelope('2026-07-18T10:01:00'), 2))
    expect(store.connectionState).toBe('live')
    compensation.resolve(envelope('2026-07-18T10:00:30'))
    await settle()

    expect(store.overview?.computed_at).toBe('2026-07-18T10:01:00')
    expect(store.connectionState).toBe('live')
  })

  it('keeps the stream live when a valid snapshot beats a failed overview catch-up', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    const compensation = Promise.withResolvers<MarketRadarEnvelope<MarketRadarOverview>>()
    api.overview.mockImplementationOnce(() => compensation.promise)

    source.open()
    source.emit('snapshot', streamPayload(envelope('2026-07-18T10:01:00'), 2))
    compensation.reject(new Error('overview catch-up failed'))
    await settle()

    expect(source.closed).toBe(false)
    expect(store.connectionState).toBe('live')
    expect(store.overview?.computed_at).toBe('2026-07-18T10:01:00')
  })

  it('preserves alerts received while the onopen REST compensation is pending', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    const compensation = Promise.withResolvers<MarketRadarEnvelope<MarketRadarOverview>>()
    api.overview.mockImplementationOnce(() => compensation.promise)

    source.open()
    source.emit('alert', streamPayload(alert(8) as unknown as Record<string, unknown>, 2))
    compensation.resolve(envelope('2026-07-18T10:00:30'))
    await settle()

    expect(store.alerts.map(item => item.id)).toEqual([8, 7])
    expect(store.connectionState).toBe('live')
  })

  it('keeps a healthy SSE connection when only the alert catch-up request fails', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    api.overview.mockResolvedValueOnce(envelope('2026-07-18T10:00:30'))
    api.activeHighAlerts.mockRejectedValueOnce(new Error('alert catch-up failed'))

    source.open()
    await settle()

    expect(source.closed).toBe(false)
    expect(store.connectionState).toBe('live')
    expect(store.overview?.computed_at).toBe('2026-07-18T10:00:30')
    expect(store.alerts.map(item => item.id)).toEqual([7])
  })

  it('becomes live when a newer REST generation supersedes the onopen compensation', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    const originalCompensation = Promise.withResolvers<
      MarketRadarEnvelope<MarketRadarOverview>
    >()
    api.overview.mockImplementationOnce(() => originalCompensation.promise)
    source.open()

    api.overview.mockResolvedValueOnce(envelope('2026-07-18T10:04:00'))
    await store.refreshNow()

    expect(store.connectionState).toBe('live')
    originalCompensation.resolve(envelope('2026-07-18T10:03:00'))
    await settle()
    expect(store.overview?.computed_at).toBe('2026-07-18T10:04:00')
  })

  it('falls back when a newer REST generation supersedes then fails the onopen compensation', async () => {
    await store.start()
    const source = FakeEventSource.instances[0]
    const originalCompensation = Promise.withResolvers<
      MarketRadarEnvelope<MarketRadarOverview>
    >()
    api.overview.mockImplementationOnce(() => originalCompensation.promise)
    source.open()

    api.overview.mockRejectedValueOnce(new Error('compensation failed'))
    await expect(store.refreshNow()).rejects.toThrow('compensation failed')

    expect(source.closed).toBe(true)
    expect(store.connectionState).toBe('fallback_polling')
    originalCompensation.resolve(envelope('2026-07-18T10:03:00'))
    await settle()
    expect(store.connectionState).toBe('fallback_polling')
  })

  it('keeps SSE while hidden, defers snapshot rendering, refreshes on visibility, and stops cleanly', async () => {
    let visibility: DocumentVisibilityState = 'visible'
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => visibility,
    })
    await store.start()
    const source = FakeEventSource.instances[0]
    source.open()
    await settle()

    visibility = 'hidden'
    document.dispatchEvent(new Event('visibilitychange'))
    source.emit('snapshot', streamPayload(envelope('2026-07-18T10:02:00'), 2))
    expect(source.closed).toBe(false)
    expect(store.overview?.computed_at).not.toBe('2026-07-18T10:02:00')

    api.overview.mockResolvedValueOnce(envelope('2026-07-18T10:02:01'))
    visibility = 'visible'
    document.dispatchEvent(new Event('visibilitychange'))
    await vi.waitFor(() => {
      expect(store.overview?.computed_at).toBe('2026-07-18T10:02:01')
    })
    expect(store.overview?.computed_at).toBe('2026-07-18T10:02:01')

    const callsBeforeStop = api.overview.mock.calls.length
    store.stop()
    expect(source.closed).toBe(true)
    expect(store.connectionState).toBe('stopped')
    await vi.advanceTimersByTimeAsync(120_000)
    source.emit('snapshot', streamPayload(envelope('2026-07-18T10:03:00'), 3))
    document.dispatchEvent(new Event('visibilitychange'))
    expect(api.overview).toHaveBeenCalledTimes(callsBeforeStop)
    expect(store.overview?.computed_at).toBe('2026-07-18T10:02:01')
  })

  it('aborts an in-flight REST refresh when stopped', async () => {
    await store.start()
    const pending = Promise.withResolvers<MarketRadarEnvelope<MarketRadarOverview>>()
    api.overview.mockImplementationOnce(() => pending.promise)

    const refresh = store.refreshNow()
    const config = api.overview.mock.calls.at(-1)?.[0]
    expect(config?.signal.aborted).toBe(false)

    store.stop()
    expect(config?.signal.aborted).toBe(true)
    pending.resolve(envelope('2026-07-18T10:05:00'))
    await expect(refresh).resolves.toBe(false)
    expect(store.overview?.computed_at).toBe('2026-07-18T10:00:00')
  })
})

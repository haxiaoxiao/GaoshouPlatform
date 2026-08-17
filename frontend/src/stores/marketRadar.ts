import { defineStore } from 'pinia'
import { ref } from 'vue'
import {
  marketRadarApi,
  type MarketRadarAlert,
  type MarketRadarEnvelope,
  type MarketRadarOverview,
  type RadarRealtimeMode,
} from '@/api/marketRadar'
import { useNotificationStore } from './notification'

export type MarketRadarConnectionState =
  | 'idle'
  | 'connecting'
  | 'live'
  | 'fallback_polling'
  | 'reconnecting'
  | 'stopped'

const HEARTBEAT_TIMEOUT_MS = 20_000
const FALLBACK_POLL_INTERVAL_MS = 30_000
const RECONNECT_DELAYS_MS = [5_000, 10_000, 20_000, 40_000, 60_000] as const

const isActiveHighAlert = (alert: MarketRadarAlert) =>
  alert.status === 'active' && alert.severity === 'high'

const timestamp = (value: string): number => {
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : Number.NEGATIVE_INFINITY
}

const errorText = (error: unknown): string =>
  error instanceof Error ? error.message : '市场雷达连接失败'

type MarketRadarMetrics = Record<string, unknown> & { overview?: MarketRadarOverview }

const isEnvelope = (value: unknown): value is MarketRadarEnvelope<MarketRadarMetrics> => {
  if (!value || typeof value !== 'object') return false
  const candidate = value as Partial<MarketRadarEnvelope<MarketRadarMetrics>>
  return typeof candidate.computed_at === 'string'
    && typeof candidate.realtime_mode === 'string'
    && candidate.data !== null
    && typeof candidate.data === 'object'
}

export const useMarketRadarStore = defineStore('marketRadar', () => {
  const overview = ref<MarketRadarEnvelope<MarketRadarOverview> | null>(null)
  const latestSnapshot = ref<MarketRadarEnvelope<MarketRadarMetrics> | null>(null)
  const alerts = ref<MarketRadarAlert[]>([])
  const connectionState = ref<MarketRadarConnectionState>('idle')
  const realtimeMode = ref<RadarRealtimeMode>('offline')
  const lastError = ref<string | null>(null)
  const pageVisible = ref(typeof document === 'undefined' || document.visibilityState !== 'hidden')

  let source: EventSource | null = null
  let lifecycleGeneration = 0
  let restGeneration = 0
  let reconnectAttempt = 0
  let alertMutationSequence = 0
  const streamAlertMutations = new Map<
    number,
    { sequence: number; alert: MarketRadarAlert | null }
  >()
  let latestComputedAt = Number.NEGATIVE_INFINITY
  let deferredOverview: MarketRadarEnvelope<MarketRadarOverview> | null = null
  let deferredSnapshot: MarketRadarEnvelope<MarketRadarMetrics> | null = null
  let heartbeatTimer: ReturnType<typeof setTimeout> | null = null
  let pollingTimer: ReturnType<typeof setInterval> | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let fallbackPollInFlight = false
  let restController: AbortController | null = null
  let listeningForVisibility = false
  let openCompensation: {
    generation: number
    source: EventSource
    minRestGeneration: number
  } | null = null

  const notificationStore = useNotificationStore()

  function commitOverview(value: MarketRadarEnvelope<MarketRadarMetrics>): boolean {
    const incomingTimestamp = timestamp(value.computed_at)
    if (incomingTimestamp < latestComputedAt) return false
    latestComputedAt = incomingTimestamp
    realtimeMode.value = value.realtime_mode
    const nestedOverview = value.data.overview
    const normalizedOverview: MarketRadarEnvelope<MarketRadarOverview> = nestedOverview
      ? { ...value, data: nestedOverview }
      : value as MarketRadarEnvelope<MarketRadarOverview>
    if (!pageVisible.value) {
      deferredOverview = normalizedOverview
      if (nestedOverview) deferredSnapshot = value
      return true
    }
    deferredOverview = null
    deferredSnapshot = null
    overview.value = normalizedOverview
    if (nestedOverview) latestSnapshot.value = value
    return true
  }

  function commitDeferredOverview(): void {
    if (!deferredOverview) return
    overview.value = deferredOverview
    realtimeMode.value = deferredOverview.realtime_mode
    deferredOverview = null
    if (deferredSnapshot) latestSnapshot.value = deferredSnapshot
    deferredSnapshot = null
  }

  function replaceAlerts(items: MarketRadarAlert[]): void {
    const unique = new Map<number, MarketRadarAlert>()
    for (const item of items) {
      if (isActiveHighAlert(item)) unique.set(item.id, item)
    }
    alerts.value = [...unique.values()].sort((left, right) => {
      const timeOrder = timestamp(right.triggered_at) - timestamp(left.triggered_at)
      return timeOrder || right.id - left.id
    })
    notificationStore.syncMarketAlerts(alerts.value)
  }

  function applyAlert(item: MarketRadarAlert): void {
    alertMutationSequence += 1
    streamAlertMutations.set(item.id, {
      sequence: alertMutationSequence,
      alert: isActiveHighAlert(item) ? item : null,
    })
    const existingIndex = alerts.value.findIndex(current => current.id === item.id)
    if (!isActiveHighAlert(item)) {
      if (existingIndex >= 0) {
        alerts.value.splice(existingIndex, 1)
        notificationStore.syncMarketAlerts(alerts.value)
      }
      return
    }
    if (existingIndex >= 0) {
      alerts.value.splice(existingIndex, 1, item)
      return
    }
    alerts.value.unshift(item)
    notificationStore.addMarketAlert(item)
  }

  function completeOpenCompensation(generation: number, requestGeneration: number): void {
    const pending = openCompensation
    if (
      !pending
      || pending.generation !== generation
      || pending.source !== source
      || requestGeneration < pending.minRestGeneration
    ) return
    openCompensation = null
    stopPolling()
    reconnectAttempt = 0
    connectionState.value = 'live'
  }

  function completeOpenFromSnapshot(generation: number, currentSource: EventSource): void {
    const pending = openCompensation
    if (!pending || pending.generation !== generation || pending.source !== currentSource) return
    openCompensation = null
    stopPolling()
    reconnectAttempt = 0
    lastError.value = null
    connectionState.value = 'live'
  }

  function failOpenCompensation(
    generation: number,
    requestGeneration: number,
    error: unknown,
  ): void {
    const pending = openCompensation
    if (
      !pending
      || pending.generation !== generation
      || pending.source !== source
      || requestGeneration < pending.minRestGeneration
    ) return
    enterFallback(generation, errorText(error))
  }

  async function refreshRest(generation: number): Promise<boolean> {
    if (generation !== lifecycleGeneration) return false
    const requestGeneration = ++restGeneration
    const alertSequenceAtRequest = alertMutationSequence
    restController?.abort()
    const controller = new AbortController()
    restController = controller
    try {
      const [overviewResult, alertResult] = await Promise.allSettled([
        marketRadarApi.overview({ signal: controller.signal, notifyError: false }),
        marketRadarApi.activeHighAlerts({ signal: controller.signal }),
      ])
      if (
        generation !== lifecycleGeneration
        || requestGeneration !== restGeneration
        || controller.signal.aborted
      ) return false
      if (alertResult.status === 'fulfilled') {
        const reconciledAlerts = new Map(
          alertResult.value.data.items
            .filter(isActiveHighAlert)
            .map(item => [item.id, item]),
        )
        for (const [id, mutation] of streamAlertMutations) {
          if (mutation.sequence <= alertSequenceAtRequest) continue
          if (mutation.alert) reconciledAlerts.set(id, mutation.alert)
          else reconciledAlerts.delete(id)
        }
        replaceAlerts([...reconciledAlerts.values()])
        streamAlertMutations.clear()
      }
      if (overviewResult.status === 'rejected') throw overviewResult.reason
      commitOverview(overviewResult.value)
      lastError.value = null
      completeOpenCompensation(generation, requestGeneration)
      return true
    } catch (error) {
      if (
        generation !== lifecycleGeneration
        || requestGeneration !== restGeneration
        || controller.signal.aborted
      ) return false
      lastError.value = errorText(error)
      failOpenCompensation(generation, requestGeneration, error)
      throw error
    } finally {
      if (restController === controller) restController = null
    }
  }

  function clearHeartbeatTimer(): void {
    if (heartbeatTimer === null) return
    clearTimeout(heartbeatTimer)
    heartbeatTimer = null
  }

  function clearReconnectTimer(): void {
    if (reconnectTimer === null) return
    clearTimeout(reconnectTimer)
    reconnectTimer = null
  }

  function closeSource(): void {
    const current = source
    source = null
    if (openCompensation?.source === current) openCompensation = null
    current?.close()
  }

  function stopPolling(): void {
    if (pollingTimer !== null) {
      clearInterval(pollingTimer)
      pollingTimer = null
    }
    fallbackPollInFlight = false
  }

  async function pollFallback(generation: number): Promise<void> {
    if (generation !== lifecycleGeneration || fallbackPollInFlight) return
    fallbackPollInFlight = true
    try {
      await refreshRest(generation)
    } catch {
      // refreshRest records the transport failure; fallback must keep retrying.
    } finally {
      if (generation === lifecycleGeneration) fallbackPollInFlight = false
    }
  }

  function startPolling(generation: number): void {
    if (pollingTimer !== null) return
    pollingTimer = setInterval(() => {
      void pollFallback(generation)
    }, FALLBACK_POLL_INTERVAL_MS)
  }

  function scheduleReconnect(generation: number): void {
    if (generation !== lifecycleGeneration || reconnectTimer !== null) return
    const delay = RECONNECT_DELAYS_MS[Math.min(reconnectAttempt, RECONNECT_DELAYS_MS.length - 1)]
    reconnectAttempt += 1
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null
      if (generation !== lifecycleGeneration) return
      connectionState.value = 'reconnecting'
      connect(generation)
    }, delay)
  }

  function enterFallback(generation: number, message: string): void {
    if (generation !== lifecycleGeneration || connectionState.value === 'stopped') return
    closeSource()
    openCompensation = null
    clearHeartbeatTimer()
    lastError.value = message
    connectionState.value = 'fallback_polling'
    startPolling(generation)
    void pollFallback(generation)
    scheduleReconnect(generation)
  }

  function resetHeartbeatWatchdog(generation: number, currentSource: EventSource): void {
    clearHeartbeatTimer()
    heartbeatTimer = setTimeout(() => {
      if (generation !== lifecycleGeneration || source !== currentSource) return
      enterFallback(generation, '实时流超过 20 秒未收到心跳')
    }, HEARTBEAT_TIMEOUT_MS)
  }

  function parseEvent<T>(event: MessageEvent<string>): T {
    return JSON.parse(event.data) as T
  }

  function connect(generation: number): void {
    if (generation !== lifecycleGeneration || connectionState.value === 'stopped') return
    closeSource()
    let currentSource: EventSource
    try {
      currentSource = new EventSource(marketRadarApi.streamUrl())
    } catch (error) {
      enterFallback(generation, errorText(error))
      return
    }
    source = currentSource

    currentSource.onopen = () => {
      if (generation !== lifecycleGeneration || source !== currentSource) return
      clearReconnectTimer()
      resetHeartbeatWatchdog(generation, currentSource)
      openCompensation = {
        generation,
        source: currentSource,
        minRestGeneration: restGeneration + 1,
      }
      void (async () => {
        try {
          await refreshRest(generation)
        } catch {
          // refreshRest degrades only while this connection still needs REST compensation.
        }
      })()
    }

    currentSource.onerror = () => {
      if (generation !== lifecycleGeneration || source !== currentSource) return
      enterFallback(generation, '实时流连接已断开')
    }

    currentSource.addEventListener('heartbeat', event => {
      if (generation !== lifecycleGeneration || source !== currentSource) return
      try {
        parseEvent(event as MessageEvent<string>)
        resetHeartbeatWatchdog(generation, currentSource)
      } catch (error) {
        enterFallback(generation, errorText(error))
      }
    })
    currentSource.addEventListener('mode', event => {
      if (generation !== lifecycleGeneration || source !== currentSource) return
      try {
        const payload = parseEvent<{ mode?: RadarRealtimeMode }>(event as MessageEvent<string>)
        if (!payload.mode || !['push', 'polling_30s', 'offline', 'closed'].includes(payload.mode)) {
          throw new Error('市场雷达 mode 事件无效')
        }
        realtimeMode.value = payload.mode
      } catch (error) {
        enterFallback(generation, errorText(error))
      }
    })
    currentSource.addEventListener('snapshot', event => {
      if (generation !== lifecycleGeneration || source !== currentSource) return
      try {
        const payload = parseEvent<unknown>(event as MessageEvent<string>)
        if (!isEnvelope(payload)) throw new Error('市场雷达 snapshot 事件无效')
        commitOverview(payload)
        completeOpenFromSnapshot(generation, currentSource)
      } catch (error) {
        enterFallback(generation, errorText(error))
      }
    })
    currentSource.addEventListener('alert', event => {
      if (generation !== lifecycleGeneration || source !== currentSource) return
      try {
        const payload = parseEvent<MarketRadarAlert>(event as MessageEvent<string>)
        if (!Number.isInteger(payload.id)) throw new Error('市场雷达 alert 事件无效')
        applyAlert(payload)
      } catch (error) {
        enterFallback(generation, errorText(error))
      }
    })
  }

  function handleVisibilityChange(): void {
    pageVisible.value = document.visibilityState !== 'hidden'
    if (!pageVisible.value) return
    commitDeferredOverview()
    const generation = lifecycleGeneration
    void refreshRest(generation).catch(() => {
      // The active SSE connection remains authoritative; lastError exposes the failed catch-up.
    })
  }

  async function start(): Promise<void> {
    if (!['idle', 'stopped'].includes(connectionState.value)) return
    const generation = ++lifecycleGeneration
    connectionState.value = 'connecting'
    lastError.value = null
    reconnectAttempt = 0
    pageVisible.value = document.visibilityState !== 'hidden'
    if (!listeningForVisibility) {
      document.addEventListener('visibilitychange', handleVisibilityChange)
      listeningForVisibility = true
    }
    try {
      await refreshRest(generation)
    } catch {
      // SSE may still recover the initial REST failure.
    }
    if (generation === lifecycleGeneration) connect(generation)
  }

  async function refreshNow(): Promise<boolean> {
    return refreshRest(lifecycleGeneration)
  }

  function stop(): void {
    lifecycleGeneration += 1
    restGeneration += 1
    connectionState.value = 'stopped'
    closeSource()
    clearHeartbeatTimer()
    clearReconnectTimer()
    stopPolling()
    restController?.abort()
    restController = null
    deferredOverview = null
    deferredSnapshot = null
    openCompensation = null
    if (listeningForVisibility) {
      document.removeEventListener('visibilitychange', handleVisibilityChange)
      listeningForVisibility = false
    }
  }

  return {
    overview,
    latestSnapshot,
    alerts,
    connectionState,
    realtimeMode,
    lastError,
    pageVisible,
    start,
    stop,
    refreshNow,
  }
})

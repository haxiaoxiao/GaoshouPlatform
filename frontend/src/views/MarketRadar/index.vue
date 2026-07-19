<template>
  <main class="page-frame page-scroll market-radar-page" v-loading="initialLoading">
    <RadarStatusStrip
      :connection-state="connectionState"
      :realtime-mode="realtimeMode"
      :as-of="overviewEnvelope?.as_of || null"
      :computed-at="overviewEnvelope?.computed_at || null"
      :error="pageError || transportError"
      :refreshing="refreshing"
      @refresh="refreshAll"
    />

    <MarketPulseStrip :overview="overviewData" />

    <div class="radar-chart-grid">
      <BreadthDistributionChart
        :data="breadthEnvelope?.data || null"
        :status="breadthEnvelope?.status || 'unavailable'"
        :as-of="breadthEnvelope?.as_of || null"
        :reason="sourceReason(breadthEnvelope)"
      />
      <IndexTrendChart
        :data="breadthEnvelope?.data || null"
        :status="breadthEnvelope?.status || 'unavailable'"
        :as-of="breadthEnvelope?.as_of || null"
      />
    </div>

    <div class="radar-detail-grid">
      <LimitLadderPanel
        :data="ladderEnvelope?.data || null"
        :status="ladderEnvelope?.status || 'unavailable'"
        :as-of="ladderEnvelope?.as_of || null"
      />
      <CrowdingBreakdown
        :data="crowdingEnvelope?.data || null"
        :status="crowdingEnvelope?.status || 'unavailable'"
        :as-of="crowdingEnvelope?.as_of || null"
      />
    </div>

    <SectorTemperatureTable
      :data="sectorsEnvelope?.data || null"
      :status="sectorsEnvelope?.status || 'unavailable'"
      :as-of="sectorsEnvelope?.as_of || null"
    />

    <AlertCenter
      :alerts="mergedAlerts"
      :focus-id="focusAlertId"
      :busy-id="busyAlertId"
      @acknowledge="acknowledgeAlert"
      @dismiss="dismissAlert"
    />
  </main>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, unref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'

import {
  marketRadarApi,
  type MarketRadarAlert,
  type MarketRadarEnvelope,
  type MarketRadarOverview,
  type RadarRealtimeMode,
} from '@/api/marketRadar'
import { useMarketRadarStore } from '@/stores/marketRadar'
import AlertCenter from './AlertCenter.vue'
import BreadthDistributionChart from './BreadthDistributionChart.vue'
import CrowdingBreakdown from './CrowdingBreakdown.vue'
import IndexTrendChart from './IndexTrendChart.vue'
import LimitLadderPanel from './LimitLadderPanel.vue'
import MarketPulseStrip from './MarketPulseStrip.vue'
import RadarStatusStrip from './RadarStatusStrip.vue'
import SectorTemperatureTable from './SectorTemperatureTable.vue'
import { isUsableRadarComponent, mergeRealtimeBreadth } from './model'
import './market-radar.css'

type DataEnvelope = MarketRadarEnvelope<Record<string, unknown>>

const route = useRoute()
const radarStore = useMarketRadarStore()
const breadthEnvelope = ref<DataEnvelope | null>(null)
const ladderEnvelope = ref<DataEnvelope | null>(null)
const crowdingEnvelope = ref<DataEnvelope | null>(null)
const sectorsEnvelope = ref<DataEnvelope | null>(null)
const alertHistory = ref<MarketRadarAlert[]>([])
const alertOverrides = ref(new Map<number, MarketRadarAlert>())
const initialLoading = ref(true)
const refreshing = ref(false)
const pageError = ref<string | null>(null)
const busyAlertId = ref<number | null>(null)

const overviewEnvelope = computed(() => unref(radarStore.overview) as MarketRadarEnvelope<MarketRadarOverview> | null)
const overviewData = computed(() => overviewEnvelope.value?.data || null)
const connectionState = computed(() => String(unref(radarStore.connectionState) || 'idle'))
const realtimeMode = computed(() => (unref(radarStore.realtimeMode) || 'offline') as RadarRealtimeMode)
const transportError = computed(() => unref(radarStore.lastError) as string | null)
const streamAlerts = computed(() => (unref(radarStore.alerts) || []) as MarketRadarAlert[])
const latestSnapshot = computed(() => unref(radarStore.latestSnapshot) as DataEnvelope | null)
const focusAlertId = computed(() => {
  const raw = Array.isArray(route.query.alert) ? route.query.alert[0] : route.query.alert
  const parsed = Number(raw)
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null
})
const mergedAlerts = computed(() => {
  const byId = new Map<number, MarketRadarAlert>()
  for (const item of [...alertHistory.value, ...streamAlerts.value, ...alertOverrides.value.values()]) {
    const previous = byId.get(item.id)
    if (!previous || alertVersion(item) >= alertVersion(previous)) byId.set(item.id, item)
  }
  return [...byId.values()].sort((left, right) => right.last_seen_at.localeCompare(left.last_seen_at))
})

function alertVersion(alert: MarketRadarAlert): number {
  return Math.max(
    ...[
      alert.last_seen_at,
      alert.acknowledged_at,
      alert.dismissed_at,
      alert.resolved_at,
    ].map(value => value ? Date.parse(value) : Number.NEGATIVE_INFINITY),
  )
}

function sourceReason(envelope: DataEnvelope | null): string | null {
  return envelope?.sources.find(source => source.reason)?.reason || null
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : '市场雷达数据加载失败'
}

async function loadSurfaces(): Promise<void> {
  const results = await Promise.allSettled([
    marketRadarApi.breadth({ days: 15, mode: 'percent' }),
    marketRadarApi.limitLadder(),
    marketRadarApi.crowding({ scope: 'market' }),
    marketRadarApi.sectors(),
    marketRadarApi.alerts({ page: 1, page_size: 50 }),
  ])
  const failures: string[] = []
  const [breadth, ladder, crowding, sectors, alerts] = results
  if (breadth.status === 'fulfilled') breadthEnvelope.value = breadth.value
  else {
    breadthEnvelope.value = unavailableEnvelope(breadth.reason)
    failures.push(errorText(breadth.reason))
  }
  if (ladder.status === 'fulfilled') ladderEnvelope.value = ladder.value
  else {
    ladderEnvelope.value = unavailableEnvelope(ladder.reason)
    failures.push(errorText(ladder.reason))
  }
  if (crowding.status === 'fulfilled') crowdingEnvelope.value = crowding.value
  else {
    crowdingEnvelope.value = unavailableEnvelope(crowding.reason)
    failures.push(errorText(crowding.reason))
  }
  if (sectors.status === 'fulfilled') sectorsEnvelope.value = sectors.value
  else {
    sectorsEnvelope.value = unavailableEnvelope(sectors.reason)
    failures.push(errorText(sectors.reason))
  }
  if (alerts.status === 'fulfilled') alertHistory.value = alerts.value.data.items
  else failures.push(errorText(alerts.reason))
  pageError.value = failures.length ? [...new Set(failures)].join('；') : null
  applyLatestSnapshot(latestSnapshot.value)
}

function unavailableEnvelope(error: unknown): DataEnvelope {
  const reason = errorText(error)
  return {
    as_of: null,
    computed_at: new Date().toISOString(),
    status: 'unavailable',
    confidence: 0,
    realtime_mode: realtimeMode.value,
    sources: [{ name: 'market_radar', as_of: null, status: 'unavailable', reason }],
    data: { status: 'unavailable', reason },
  }
}

function applyLatestSnapshot(snapshot: DataEnvelope | null): void {
  if (!snapshot) return
  const metrics = snapshot.data
  if (isUsableRadarComponent(metrics.breadth)) {
    breadthEnvelope.value = {
      ...snapshot,
      data: mergeRealtimeBreadth(breadthEnvelope.value?.data, metrics, snapshot.as_of || snapshot.computed_at),
    }
  }
  if (isUsableRadarComponent(metrics.limit_ladder)) {
    ladderEnvelope.value = { ...snapshot, data: metrics.limit_ladder as Record<string, unknown> }
  }
  if (isUsableRadarComponent(metrics.crowding)) {
    crowdingEnvelope.value = { ...snapshot, data: metrics.crowding as Record<string, unknown> }
  }
  if (isUsableRadarComponent(metrics.sectors)) {
    sectorsEnvelope.value = { ...snapshot, data: metrics.sectors as Record<string, unknown> }
  }
}

async function refreshAll(): Promise<void> {
  if (refreshing.value) return
  refreshing.value = true
  pageError.value = null
  let overviewError: string | null = null
  try {
    await radarStore.refreshNow()
  } catch (error) {
    overviewError = errorText(error)
  }
  try {
    await loadSurfaces()
  } catch (error) {
    pageError.value = errorText(error)
  } finally {
    if (overviewError) {
      pageError.value = [...new Set([overviewError, pageError.value].filter(Boolean))].join('；')
    }
    refreshing.value = false
  }
}

async function acknowledgeAlert(id: number): Promise<void> {
  await mutateAlert(id, 'acknowledge')
}

async function dismissAlert(id: number): Promise<void> {
  await mutateAlert(id, 'dismiss')
}

async function mutateAlert(id: number, action: 'acknowledge' | 'dismiss'): Promise<void> {
  if (busyAlertId.value !== null) return
  busyAlertId.value = id
  try {
    const response = action === 'acknowledge'
      ? await marketRadarApi.acknowledgeAlert(id)
      : await marketRadarApi.dismissAlert(id)
    const index = alertHistory.value.findIndex(item => item.id === id)
    if (index >= 0) alertHistory.value.splice(index, 1, response.data)
    else alertHistory.value.push(response.data)
    alertOverrides.value = new Map(alertOverrides.value).set(id, response.data)
    ElMessage.success(action === 'acknowledge' ? '预警已确认' : '预警已忽略')
  } catch (error) {
    ElMessage.error(errorText(error))
  } finally {
    busyAlertId.value = null
  }
}

onMounted(async () => {
  try {
    await radarStore.start()
    await loadSurfaces()
  } catch (error) {
    pageError.value = errorText(error)
  } finally {
    initialLoading.value = false
  }
})

watch(latestSnapshot, applyLatestSnapshot)

onBeforeUnmount(() => radarStore.stop())
</script>

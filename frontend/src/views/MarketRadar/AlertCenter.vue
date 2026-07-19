<template>
  <section ref="root" class="radar-surface alert-center" aria-labelledby="alerts-title">
    <header class="surface-heading alert-heading">
      <div><h3 id="alerts-title">预警中心</h3><p>事件状态由后端维护，确认与忽略不会在刷新后复活</p></div>
      <div class="alert-filters" aria-label="预警筛选">
        <select v-model="statusFilter" aria-label="按状态筛选"><option value="active">活跃</option><option value="acknowledged">已确认</option><option value="resolved">已解除</option><option value="dismissed">已忽略</option><option value="all">全部</option></select>
        <select v-model="severityFilter" aria-label="按严重度筛选"><option value="all">全部级别</option><option value="high">高</option><option value="medium">中</option><option value="low">低</option></select>
      </div>
    </header>
    <div v-if="filtered.length" class="alert-list">
      <article
        v-for="alert in filtered"
        :id="`market-alert-${alert.id}`"
        :key="alert.id"
        class="alert-row"
        :class="[`alert-row--${alert.severity}`, { 'alert-row--focused': alert.id === focusId }]"
      >
        <button class="alert-summary" type="button" :aria-expanded="expanded.has(alert.id)" @click="toggle(alert.id)">
          <span class="alert-severity">{{ severityLabel(alert.severity) }}</span>
          <span><strong>{{ alert.title }}</strong><small>{{ alert.subject }} · {{ formatTime(alert.last_seen_at) }}</small></span>
          <em>{{ statusLabel(alert.status) }}</em>
          <b aria-hidden="true">{{ expanded.has(alert.id) ? '−' : '+' }}</b>
        </button>
        <div v-if="expanded.has(alert.id)" class="alert-detail">
          <p>{{ alert.explanation }}</p>
          <dl>
            <div v-for="(value, key) in alert.evidence" :key="key"><dt>{{ key }}</dt><dd>{{ evidence(value) }}</dd></div>
          </dl>
          <div v-if="alert.status === 'active'" class="alert-actions">
            <button type="button" :disabled="busyId === alert.id" @click="$emit('acknowledge', alert.id)">确认</button>
            <button type="button" :disabled="busyId === alert.id" @click="$emit('dismiss', alert.id)">忽略</button>
          </div>
        </div>
      </article>
    </div>
    <div v-else class="radar-empty">当前筛选条件下没有预警事件</div>
  </section>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import type { MarketRadarAlert } from '@/api/marketRadar'

const props = defineProps<{ alerts: MarketRadarAlert[]; focusId: number | null; busyId: number | null }>()
defineEmits<{ acknowledge: [id: number]; dismiss: [id: number] }>()
const root = ref<HTMLElement | null>(null)
const statusFilter = ref('active')
const severityFilter = ref('all')
const expanded = ref(new Set<number>())
const filtered = computed(() => props.alerts.filter(alert =>
  (statusFilter.value === 'all' || alert.status === statusFilter.value)
  && (severityFilter.value === 'all' || alert.severity === severityFilter.value),
))

function toggle(id: number): void {
  const next = new Set(expanded.value)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  expanded.value = next
}

function reveal(id: number | null): void {
  if (id === null) return
  statusFilter.value = 'all'
  severityFilter.value = 'all'
  expanded.value = new Set([...expanded.value, id])
  void nextTick(() => root.value?.querySelector<HTMLElement>(`#market-alert-${id}`)?.scrollIntoView({ block: 'center', behavior: 'smooth' }))
}

watch(() => props.focusId, reveal, { immediate: true })
watch(() => props.alerts.length, () => reveal(props.focusId))

function severityLabel(value: string): string { return ({ high: '高', medium: '中', low: '低' }[value] || value) }
function statusLabel(value: string): string { return ({ active: '活跃', acknowledged: '已确认', dismissed: '已忽略', resolved: '已解除' }[value] || value) }
function formatTime(value: string): string { return value.replace('T', ' ').slice(5, 16) }
function evidence(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
</script>

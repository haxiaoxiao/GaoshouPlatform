<template>
  <section class="market-pulse-strip" aria-label="市场脉冲摘要">
    <article>
      <span>市场体感</span>
      <strong :class="returnTone(medianReturn)">{{ percent(medianReturn) }}</strong>
      <small>全 A 中位数 · 下跌 {{ ratio(declineRatio) }}</small>
    </article>
    <article>
      <span>交易拥挤度</span>
      <strong :class="scoreTone(crowdingValue)">{{ score(crowdingValue) }}</strong>
      <small>{{ crowdingLabel || '标签不可用' }}</small>
    </article>
    <article data-testid="emotion-score" :title="emotionReason || undefined">
      <span>情绪温度</span>
      <strong :class="scoreTone(emotionValue)">{{ score(emotionValue) }}</strong>
      <small>{{ emotionLabel || '标签不可用' }}</small>
    </article>
    <article>
      <span>活跃预警</span>
      <strong :class="activeHigh > 0 ? 'metric-up' : ''">{{ activeCount ?? '—' }}</strong>
      <small>高严重度 {{ activeHigh }}</small>
    </article>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ overview: Record<string, unknown> | null }>()

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
}

function finite(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

const source = computed(() => record(props.overview))
const breadth = computed(() => record(source.value.market_breadth))
const crowding = computed(() => record(source.value.crowding))
const emotion = computed(() => record(source.value.emotion))
const alertCounts = computed(() => record(source.value.alert_counts))
const medianReturn = computed(() => finite(
  breadth.value.median_return_pct ?? source.value.market_median_return_pct,
))
const declineRatio = computed(() => finite(
  breadth.value.decline_ratio ?? source.value.decline_ratio,
))
const crowdingValue = computed(() => finite(crowding.value.value))
const crowdingLabel = computed(() => typeof crowding.value.label === 'string' ? crowding.value.label : null)
const emotionValue = computed(() => finite(emotion.value.value))
const emotionLabel = computed(() => typeof emotion.value.label === 'string' ? emotion.value.label : null)
const emotionReason = computed(() => typeof emotion.value.reason === 'string' ? emotion.value.reason : null)
const activeCount = computed(() => finite(alertCounts.value.active))
const activeHigh = computed(() => finite(alertCounts.value.active_high) ?? 0)

function percent(value: number | null): string {
  return value === null ? '—' : `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

function ratio(value: number | null): string {
  if (value === null) return '—'
  const normalized = Math.abs(value) <= 1 ? value * 100 : value
  return `${normalized.toFixed(1)}%`
}

function score(value: number | null): string {
  return value === null ? '—' : value.toFixed(1)
}

function returnTone(value: number | null): string {
  return value === null ? '' : value > 0 ? 'metric-up' : value < 0 ? 'metric-down' : ''
}

function scoreTone(value: number | null): string {
  if (value === null) return ''
  return value >= 80 ? 'metric-up' : value <= 30 ? 'metric-down' : ''
}
</script>

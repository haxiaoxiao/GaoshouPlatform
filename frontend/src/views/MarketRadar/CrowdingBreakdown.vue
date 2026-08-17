<template>
  <section class="radar-surface crowding-panel" aria-labelledby="crowding-title">
    <header class="surface-heading">
      <div><h3 id="crowding-title">交易拥挤度</h3><p>当前值相对 120 日历史的稳健分位</p></div>
      <span class="source-state" :class="`source-state--${freshness.tone}`">{{ freshness.label }}</span>
    </header>
    <div class="crowding-scoreline">
      <strong :class="scoreValue !== null && scoreValue >= 80 ? 'metric-up' : ''">{{ scoreValue === null ? '—' : scoreValue.toFixed(1) }}</strong>
      <span>{{ label || '标签不可用' }}</span>
      <div class="score-track" aria-hidden="true"><i :style="{ width: `${scoreValue || 0}%` }" /></div>
    </div>
    <div v-if="components.length" class="component-ledger">
      <div v-for="component in components" :key="component.key">
        <span><strong>{{ component.label }}</strong><small>{{ component.reason || '有效输入' }}</small></span>
        <b>{{ component.normalized === null ? '—' : component.normalized.toFixed(1) }}</b>
        <em>{{ component.contribution === null ? '—' : component.contribution.toFixed(1) }}</em>
      </div>
    </div>
    <div v-else class="radar-empty">拥挤度组成项暂不可用</div>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { resolveFreshness } from './model'

const props = defineProps<{ data: Record<string, unknown> | null; status: unknown; asOf: string | null }>()
const record = (value: unknown): Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
const finite = (value: unknown): number | null => typeof value === 'number' && Number.isFinite(value) ? value : null
const source = computed(() => record(props.data))
const score = computed(() => record(source.value.score))
const scoreValue = computed(() => finite(score.value.value))
const label = computed(() => typeof source.value.label === 'string' ? source.value.label : null)
const freshness = computed(() => resolveFreshness(source.value.status || props.status, props.asOf))
const labels: Record<string, string> = {
  top_1_amount_share: '成交额 Top 1% 占比', top_5_amount_share: '成交额 Top 5% 占比',
  top_3_sector_share: '前三行业成交占比', market_amount_vs_20d: '市场成交额 / 20 日',
  high_liquidity_correlation: '高流动性相关性', margin_balance_5d_change: '两融余额 5 日变化',
}
const components = computed(() => {
  const raw = Array.isArray(score.value.components) ? score.value.components : []
  return raw.map(item => record(item)).map(item => ({
    key: String(item.name || 'unknown'),
    label: labels[String(item.name)] || String(item.name || '未知组成项'),
    normalized: finite(item.normalized),
    contribution: finite(item.contribution),
    reason: typeof item.excluded_reason === 'string' ? item.excluded_reason : null,
  }))
})
</script>

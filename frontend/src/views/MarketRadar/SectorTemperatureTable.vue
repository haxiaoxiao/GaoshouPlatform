<template>
  <section class="radar-surface sectors-panel" aria-labelledby="sectors-title">
    <header class="surface-heading">
      <div><h3 id="sectors-title">行业温度</h3><p>当前行业分类 · 按成交额占比观察资金聚集</p></div>
      <span class="source-state" :class="`source-state--${freshness.tone}`">{{ freshness.label }}</span>
    </header>
    <div v-if="rows.length" class="desktop-table table-scroll">
      <table>
        <thead><tr><th>行业</th><th>中位涨跌</th><th>上涨占比</th><th>成交占比</th><th>占比 Z20</th><th>拥挤/情绪</th><th>样本</th></tr></thead>
        <tbody>
          <tr v-for="row in rows" :key="row.industry">
            <td><strong>{{ row.industry }}</strong></td>
            <td :class="returnTone(row.medianReturn)">{{ signedPercent(row.medianReturn) }}</td>
            <td>{{ ratio(row.advanceRatio) }}</td>
            <td>{{ ratio(row.amountShare) }}</td>
            <td :class="row.shareZ20 !== null && row.shareZ20 >= 2.5 ? 'metric-up' : ''">{{ decimal(row.shareZ20) }}</td>
            <td title="当前没有独立行业拥挤度或情绪温度口径">—</td>
            <td>{{ integer(row.stockCount) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else class="radar-empty">行业温度数据暂不可用</div>
    <div v-if="rows.length" class="mobile-ledger sector-mobile-ledger">
      <article v-for="row in rows" :key="row.industry">
        <header><strong>{{ row.industry }}</strong><b :class="returnTone(row.medianReturn)">{{ signedPercent(row.medianReturn) }}</b></header>
        <dl>
          <div><dt>上涨占比</dt><dd>{{ ratio(row.advanceRatio) }}</dd></div>
          <div><dt>成交占比</dt><dd>{{ ratio(row.amountShare) }}</dd></div>
          <div><dt>占比 Z20</dt><dd>{{ decimal(row.shareZ20) }}</dd></div>
          <div><dt>拥挤 / 情绪</dt><dd>—</dd></div>
        </dl>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { normalizeSectors, resolveFreshness } from './model'

const props = defineProps<{ data: Record<string, unknown> | null; status: unknown; asOf: string | null }>()
const rows = computed(() => normalizeSectors(props.data))
const freshness = computed(() => resolveFreshness(record(props.data).status || props.status, props.asOf))
function record(value: unknown): Record<string, unknown> { return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {} }
function signedPercent(value: number | null): string { return value === null ? '—' : `${value > 0 ? '+' : ''}${value.toFixed(2)}%` }
function ratio(value: number | null): string { return value === null ? '—' : `${(Math.abs(value) <= 1 ? value * 100 : value).toFixed(1)}%` }
function decimal(value: number | null): string { return value === null ? '—' : value.toFixed(2) }
function integer(value: number | null): string { return value === null ? '—' : value.toLocaleString('zh-CN') }
function returnTone(value: number | null): string { return value === null ? '' : value > 0 ? 'metric-up' : value < 0 ? 'metric-down' : '' }
</script>

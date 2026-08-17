<template>
  <section class="radar-surface ladder-panel" aria-labelledby="ladder-title">
    <header class="surface-heading">
      <div><h3 id="ladder-title">连板生态</h3><p>{{ view.tradeDate || '日期不可用' }} · {{ sourceLabel }}</p></div>
      <span class="source-state" :class="`source-state--${freshness.tone}`">{{ freshness.label }}</span>
    </header>
    <div class="ladder-metrics">
      <span><small>最高板</small><strong>{{ integer(view.highestBoard) }}</strong></span>
      <span><small>涨停</small><strong class="metric-up">{{ integer(view.upCount) }}</strong></span>
      <span><small>跌停</small><strong class="metric-down">{{ integer(view.downCount) }}</strong></span>
      <span><small>炸板率</small><strong>{{ percent(view.brokenRate) }}</strong></span>
      <span><small>晋级率</small><strong>{{ percent(view.promotionRate) }}</strong></span>
    </div>
    <div v-if="view.rows.length" class="desktop-table table-scroll">
      <table>
        <thead><tr><th>梯队</th><th>股票</th><th>行业</th><th>涨跌幅</th><th>首次 / 最后封板</th><th>换手率</th><th>封单额</th><th>开板</th></tr></thead>
        <tbody>
          <tr v-for="row in view.rows" :key="row.symbol">
            <td><strong class="board-count">{{ integer(row.boardCount) }} 板</strong></td>
            <td><strong>{{ row.name }}</strong><small>{{ row.symbol }}</small></td>
            <td>{{ row.industry }}</td>
            <td :class="returnTone(row.pctChange)">{{ signedPercent(row.pctChange) }}</td>
            <td>{{ row.firstTime || '—' }} / {{ row.lastTime || '—' }}</td>
            <td>{{ directPercent(row.turnoverRatio) }}</td>
            <td>{{ money(row.sealAmount) }}</td>
            <td>{{ integer(row.openTimes) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-else class="radar-empty">该交易日没有可用的连板明细</div>
    <div v-if="view.rows.length" class="mobile-ledger">
      <article v-for="row in view.rows" :key="row.symbol">
        <header><strong>{{ row.name }}</strong><b>{{ integer(row.boardCount) }} 板</b></header>
        <dl><div><dt>代码</dt><dd>{{ row.symbol }}</dd></div><div><dt>行业</dt><dd>{{ row.industry }}</dd></div><div><dt>涨跌幅</dt><dd :class="returnTone(row.pctChange)">{{ signedPercent(row.pctChange) }}</dd></div><div><dt>最后封板</dt><dd>{{ row.lastTime || '—' }}</dd></div><div><dt>换手率</dt><dd>{{ directPercent(row.turnoverRatio) }}</dd></div><div><dt>封单额</dt><dd>{{ money(row.sealAmount) }}</dd></div></dl>
      </article>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { normalizeLimitLadder, resolveFreshness } from './model'

const props = defineProps<{ data: Record<string, unknown> | null; status: unknown; asOf: string | null }>()
const view = computed(() => normalizeLimitLadder(props.data))
const freshness = computed(() => resolveFreshness(view.value.status || props.status, view.value.tradeDate || props.asOf))
const sourceLabel = computed(() => ({ official: '官方梯队', derived: '日线推导', unavailable: '来源不可用' }[view.value.sourceMode || ''] || '来源未标注'))

function integer(value: number | null): string { return value === null ? '—' : value.toLocaleString('zh-CN') }
function percent(value: number | null): string { return value === null ? '—' : `${(Math.abs(value) <= 1 ? value * 100 : value).toFixed(1)}%` }
function signedPercent(value: number | null): string { return value === null ? '—' : `${value > 0 ? '+' : ''}${value.toFixed(2)}%` }
function directPercent(value: number | null): string { return value === null ? '—' : `${value.toFixed(2)}%` }
function money(value: number | null): string {
  if (value === null) return '—'
  if (Math.abs(value) >= 100_000_000) return `¥${(value / 100_000_000).toFixed(2)}亿`
  if (Math.abs(value) >= 10_000) return `¥${(value / 10_000).toFixed(0)}万`
  return `¥${value.toFixed(0)}`
}
function returnTone(value: number | null): string { return value === null ? '' : value > 0 ? 'metric-up' : value < 0 ? 'metric-down' : '' }
</script>

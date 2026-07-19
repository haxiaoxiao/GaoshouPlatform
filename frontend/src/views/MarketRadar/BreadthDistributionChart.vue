<template>
  <section class="radar-surface breadth-surface" aria-labelledby="breadth-title">
    <header class="surface-heading">
      <div>
        <h3 id="breadth-title">全 A 盈亏分布</h3>
        <p>最近 {{ model.dates.length }} 个有效交易日 · 10 档横截面</p>
      </div>
      <span class="source-state" :class="`source-state--${freshness.tone}`">
        {{ freshness.label }}<template v-if="freshness.asOf"> · {{ freshness.asOf.slice(0, 10) }}</template>
      </span>
    </header>
    <div v-if="model.dates.length" ref="chartElement" class="radar-chart" role="img" :aria-label="ariaLabel" />
    <div v-else class="radar-empty">暂无可绘制的市场盈亏分布</div>
    <p v-if="reason" class="surface-reason">{{ reason }}</p>
    <details v-if="model.dates.length" class="chart-fallback">
      <summary>查看表格数据</summary>
      <div class="fallback-scroll">
        <table>
          <thead><tr><th>日期</th><th v-for="series in model.series" :key="series.key">{{ series.label }}</th><th>其中平盘</th></tr></thead>
          <tbody>
            <tr v-for="(date, dateIndex) in model.fullDates" :key="date">
              <th>{{ date }}</th>
              <td v-for="series in model.series" :key="series.key">{{ display(series.values[dateIndex]) }}</td>
              <td>{{ model.flatCounts[dateIndex] ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </details>
  </section>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

import type { EChartsOption } from '@/lib/echarts'
import { buildBreadthChart, resolveFreshness } from './model'
import { useRadarChart } from './useRadarChart'

const props = defineProps<{
  data: Record<string, unknown> | null
  status: unknown
  asOf: string | null
  reason?: string | null
}>()

const chartElement = ref<HTMLElement | null>(null)
const model = computed(() => buildBreadthChart(props.data))
const freshness = computed(() => resolveFreshness(props.status, props.asOf))
const reason = computed(() => props.reason || null)
const ariaLabel = computed(() => `全 A 盈亏分布，${model.value.dates.length} 个交易日，绿色为下跌，红色为上涨`)

function display(value: number | null): string {
  if (value === null) return '—'
  return model.value.mode === 'percent' ? `${value.toFixed(1)}%` : value.toLocaleString('zh-CN')
}

function chartOption(): EChartsOption {
  return {
    animationDuration: 280,
    aria: { enabled: true, decal: { show: false } },
    color: model.value.series.map(item => item.color),
    tooltip: {
      trigger: 'axis',
      formatter: (items: unknown) => {
        const points = Array.isArray(items) ? items as Array<Record<string, unknown>> : []
        const index = Number(points[0]?.dataIndex ?? 0)
        const lines = points
          .filter(point => point.value !== null && point.value !== undefined)
          .map(point => `${point.marker || ''}${point.seriesName}: ${display(Number(point.value))}`)
        const flat = model.value.flatCounts[index]
        if (flat !== null && flat !== undefined) lines.push(`其中平盘: ${flat} 只（计入 0~2% 档）`)
        return [`<strong>${model.value.fullDates[index] || ''}</strong>`, ...lines].join('<br>')
      },
    },
    legend: { type: 'scroll', bottom: 0, textStyle: { color: '#68756f', fontSize: 10 } },
    grid: { left: 48, right: 18, top: 16, bottom: 54 },
    xAxis: {
      type: 'category',
      data: model.value.dates,
      axisLine: { lineStyle: { color: '#cfd6d2' } },
      axisLabel: { color: '#68756f', interval: 'auto', fontSize: 10 },
    },
    yAxis: {
      type: 'value',
      max: model.value.mode === 'percent' ? 100 : undefined,
      axisLabel: { color: '#68756f', formatter: model.value.mode === 'percent' ? '{value}%' : '{value}' },
      splitLine: { lineStyle: { color: '#e7ebe8', type: 'dashed' } },
    },
    series: model.value.series.map(item => ({
      name: item.label,
      type: 'bar',
      stack: 'breadth',
      emphasis: { focus: 'series' },
      barMaxWidth: 34,
      data: item.values,
    })),
  }
}

useRadarChart(chartElement, chartOption, [() => props.data, () => props.status])
</script>

<style scoped>
.breadth-surface { min-width: 0; }
</style>

<template>
  <section class="radar-surface trend-surface" aria-labelledby="trend-title">
    <header class="surface-heading">
      <div>
        <h3 id="trend-title">指数与全 A 中位数</h3>
        <p>指数并非个股体感，缺口按真实缺失断线</p>
      </div>
      <span class="source-state" :class="`source-state--${freshness.tone}`">{{ freshness.label }}</span>
    </header>
    <div v-if="hasValues" ref="chartElement" class="radar-chart" role="img" aria-label="上证指数、深证成指、中证全指与全 A 中位数日涨跌趋势" />
    <div v-else class="radar-empty">核心指数与全 A 趋势暂不可用</div>
    <details v-if="model.dates.length" class="chart-fallback">
      <summary>查看表格数据</summary>
      <div class="fallback-scroll">
        <table>
          <thead><tr><th>日期</th><th v-for="series in model.series" :key="series.key">{{ series.label }}</th></tr></thead>
          <tbody>
            <tr v-for="(date, dateIndex) in model.fullDates" :key="date">
              <th>{{ date }}</th>
              <td v-for="series in model.series" :key="series.key">{{ percent(series.values[dateIndex]) }}</td>
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
import { buildIndexTrend, resolveFreshness } from './model'
import { useRadarChart } from './useRadarChart'

const props = defineProps<{ data: Record<string, unknown> | null; status: unknown; asOf: string | null }>()
const chartElement = ref<HTMLElement | null>(null)
const model = computed(() => buildIndexTrend(props.data))
const freshness = computed(() => resolveFreshness(props.status, props.asOf))
const hasValues = computed(() => model.value.series.some(item => item.values.some(value => value !== null)))

function percent(value: number | null | undefined): string {
  return value === null || value === undefined ? '—' : `${value > 0 ? '+' : ''}${value.toFixed(2)}%`
}

function chartOption(): EChartsOption {
  return {
    animationDuration: 280,
    aria: { enabled: true, decal: { show: false } },
    tooltip: { trigger: 'axis', valueFormatter: value => percent(typeof value === 'number' ? value : null) },
    legend: { bottom: 0, textStyle: { color: '#68756f', fontSize: 10 } },
    grid: { left: 50, right: 18, top: 16, bottom: 48 },
    xAxis: {
      type: 'category',
      data: model.value.dates,
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#cfd6d2' } },
      axisLabel: { color: '#68756f', fontSize: 10 },
    },
    yAxis: {
      type: 'value',
      axisLabel: { color: '#68756f', formatter: '{value}%' },
      splitLine: { lineStyle: { color: '#e7ebe8', type: 'dashed' } },
    },
    series: model.value.series.map(item => ({
      name: item.label,
      type: 'line',
      data: item.values,
      connectNulls: false,
      showSymbol: false,
      symbolSize: 6,
      lineStyle: { color: item.color, width: item.key === 'all' ? 2.6 : 1.6, type: item.key === 'all' ? 'solid' : 'dashed' },
      itemStyle: { color: item.color },
      markPoint: item.key === 'all' ? {
        symbolSize: 34,
        label: { fontSize: 9, formatter: ({ name }: { name?: string }) => name || '' },
        data: [{ type: 'min', name: '最低' }, { type: 'max', name: '最高' }],
      } : undefined,
    })),
  }
}

useRadarChart(chartElement, chartOption, [() => props.data, () => props.status])
</script>

<style scoped>
.trend-surface { min-width: 0; }
</style>

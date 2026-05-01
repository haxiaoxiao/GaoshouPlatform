<template>
  <div class="factor-analysis" v-loading="loading">
    <div class="page-header">
      <el-button text @click="$router.back()">← 返回</el-button>
      <h2>{{ factorName }}</h2>
      <span class="update-date">更新日期: {{ report?.update_date }}</span>
    </div>

    <div class="panel-row">
      <div class="panel">
        <h3>IC时序图</h3>
        <div ref="icChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>行业IC</h3>
        <div ref="industryChartRef" class="chart"></div>
      </div>
    </div>

    <div class="panel-row">
      <div class="panel">
        <h3>换手率</h3>
        <div ref="turnoverChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>买入信号衰减分析</h3>
        <div ref="decayChartRef" class="chart"></div>
      </div>
    </div>

    <div class="panel-row">
      <div class="panel">
        <h3>因子值最大的20只股票</h3>
        <el-table :data="report?.top20 || []" size="small" max-height="360">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="name" label="名称" />
          <el-table-column prop="value" label="因子值" align="right">
            <template #default="{ row }">{{ row.value.toLocaleString() }}</template>
          </el-table-column>
        </el-table>
      </div>
      <div class="panel">
        <h3>因子值最小的20只股票</h3>
        <el-table :data="report?.bottom20 || []" size="small" max-height="360">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="name" label="名称" />
          <el-table-column prop="value" label="因子值" align="right">
            <template #default="{ row }">{{ row.value.toLocaleString() }}</template>
          </el-table-column>
        </el-table>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import * as echarts from 'echarts'
import { evaluationApi } from '@/api/v2'
import type { FactorReport } from '@/types/factor'

const route = useRoute()
const factorName = ref(String(route.params.id || ''))
const report = ref<FactorReport | null>(null)
const loading = ref(false)

const icChartRef = ref<HTMLElement | null>(null)
const industryChartRef = ref<HTMLElement | null>(null)
const turnoverChartRef = ref<HTMLElement | null>(null)
const decayChartRef = ref<HTMLElement | null>(null)

let charts: echarts.ECharts[] = []

function movingAverage(data: number[], window: number): (number | null)[] {
  return data.map((_, i) => {
    if (i < window - 1) return null
    const slice = data.slice(i - window + 1, i + 1)
    return slice.reduce((a, b) => a + b, 0) / window
  })
}

function initCharts() {
  if (!report.value) return

  if (icChartRef.value) {
    const ic = echarts.init(icChartRef.value)
    const dates = report.value.ic_series.map(p => p.date)
    const values = report.value.ic_series.map(p => p.value)
    ic.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 40, right: 16, top: 16, bottom: 24 },
      xAxis: { type: 'category', data: dates, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      series: [
        { name: 'IC', type: 'line', data: values, symbol: 'none', lineStyle: { color: '#60a5fa', width: 1.5 } },
        { name: '22日MA', type: 'line', data: movingAverage(values, 22), symbol: 'none', lineStyle: { color: '#fb923c', width: 2 } },
      ],
    })
    charts.push(ic)
  }

  if (industryChartRef.value) {
    const ind = echarts.init(industryChartRef.value)
    const industries = report.value.industry_ic.map(p => p.industry)
    const icValues = report.value.industry_ic.map(p => p.value)
    ind.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 80, right: 16, top: 8, bottom: 8 },
      xAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      yAxis: { type: 'category', data: industries, axisLabel: { fontSize: 10 } },
      series: [{ type: 'bar', data: icValues, color: '#93c5fd' }],
    })
    charts.push(ind)
  }

  if (turnoverChartRef.value) {
    const to = echarts.init(turnoverChartRef.value)
    const tDates = report.value.turnover.map(p => p.date)
    const minQ = report.value.turnover.map(p => p.min_quantile)
    const maxQ = report.value.turnover.map(p => p.max_quantile)
    to.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 40, right: 16, top: 16, bottom: 24 },
      xAxis: { type: 'category', data: tDates, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      series: [
        { name: '最小分位', type: 'scatter', data: minQ, symbolSize: 4, color: '#60a5fa' },
        { name: '最大分位', type: 'scatter', data: maxQ, symbolSize: 6, color: '#f87171' },
      ],
    })
    charts.push(to)
  }

  if (decayChartRef.value) {
    const sd = echarts.init(decayChartRef.value)
    const lags = report.value.signal_decay.map(p => `lag${p.lag}`)
    const sdMin = report.value.signal_decay.map(p => p.min_quantile)
    const sdMax = report.value.signal_decay.map(p => p.max_quantile)
    sd.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 40, right: 16, top: 16, bottom: 24 },
      xAxis: { type: 'category', data: lags, axisLabel: { fontSize: 10 } },
      yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      series: [
        { name: '最小分位', type: 'bar', data: sdMin, color: '#60a5fa' },
        { name: '最大分位', type: 'bar', data: sdMax, color: '#9ca3af' },
      ],
    })
    charts.push(sd)
  }
}

function disposeCharts() {
  charts.forEach(c => c.dispose())
  charts = []
}

async function fetchReport() {
  loading.value = true
  try {
    report.value = await evaluationApi.report({
      expression: factorName.value,
      stock_pool: 'hs300',
      start_date: '2020-01-01',
      end_date: '2025-12-31',
    })
    await nextTick()
    initCharts()
  } finally {
    loading.value = false
  }
}

onMounted(fetchReport)
onUnmounted(disposeCharts)
</script>

<style scoped>
.factor-analysis { padding: 16px; }
.page-header { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
.page-header h2 { font-size: 18px; margin: 0; }
.update-date { font-size: 11px; color: var(--text-muted); }
.panel-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
.panel {
  background: var(--bg-surface);
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 16px;
}
.panel h3 { font-size: 13px; font-weight: 600; margin: 0 0 8px; }
.chart { width: 100%; height: 220px; }
</style>

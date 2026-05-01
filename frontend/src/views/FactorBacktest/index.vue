<template>
  <div class="factor-backtest" v-loading="loading">
    <div class="page-header">
      <h2>因子回测 — {{ factorName }}</h2>
    </div>

    <div class="layout">
      <div class="config-panel">
        <h3>回测配置</h3>
        <el-form :model="btConfig" label-width="100px" size="small">
          <el-form-item label="调仓周期">
            <el-select v-model="btConfig.rebalance_period">
              <el-option label="每日" value="daily" />
              <el-option label="每周" value="weekly" />
              <el-option label="每月" value="monthly" />
            </el-select>
          </el-form-item>
          <el-form-item label="手续费率">
            <el-input-number v-model="btConfig.fee_rate" :min="0" :max="0.05" :step="0.001" :precision="3" />
          </el-form-item>
          <el-form-item label="滑点">
            <el-input-number v-model="btConfig.slippage" :min="0" :max="0.05" :step="0.001" :precision="3" />
          </el-form-item>
          <el-form-item label="组合类型">
            <el-select v-model="btConfig.portfolio_type">
              <el-option label="纯多头组合" value="long_only" />
              <el-option label="多空组合 I" value="long_short_i" />
              <el-option label="多空组合 II" value="long_short_ii" />
            </el-select>
          </el-form-item>
          <el-form-item label="过滤涨停">
            <el-switch v-model="btConfig.filter_limit_up" />
          </el-form-item>
          <el-form-item>
            <el-button type="primary" @click="runBacktest" :loading="loading">运行回测</el-button>
          </el-form-item>
        </el-form>
      </div>

      <div class="results-panel" v-if="report">
        <h3>回测结果</h3>
        <div class="metrics-grid">
          <div class="metric-card" v-for="m in metricsList" :key="m.label">
            <div class="metric-label">{{ m.label }}</div>
            <div class="metric-value" :class="m.color">{{ m.value }}</div>
          </div>
        </div>
        <div v-if="report.nav_series.length">
          <h4>净值曲线</h4>
          <div ref="navChartRef" class="nav-chart"></div>
        </div>
        <div v-if="report.logs.length" class="logs">
          <h4>日志</h4>
          <div v-for="(log, i) in report.logs" :key="i" class="log-line">{{ log }}</div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onUnmounted, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import * as echarts from 'echarts'
import { backtestV2Api } from '@/api/v2'
import type { BacktestReport, BtConfig } from '@/types/factor'

const route = useRoute()
const factorName = ref(String(route.params.id || ''))

const btConfig = ref<BtConfig>({
  rebalance_period: 'monthly',
  fee_rate: 0.001,
  slippage: 0.001,
  portfolio_type: 'long_only',
  filter_limit_up: true,
})

const report = ref<BacktestReport | null>(null)
const loading = ref(false)
const navChartRef = ref<HTMLElement | null>(null)
let navChart: echarts.ECharts | null = null

const metricsList = computed(() => {
  if (!report.value?.metrics) return []
  const m = report.value.metrics
  return [
    { label: '总收益率', value: (m.total_return * 100).toFixed(2) + '%', color: m.total_return >= 0 ? 'positive' : 'negative' },
    { label: '年化收益', value: (m.annual_return * 100).toFixed(2) + '%', color: m.annual_return >= 0 ? 'positive' : 'negative' },
    { label: 'Sharpe', value: m.sharpe.toFixed(2), color: '' },
    { label: '最大回撤', value: (m.max_drawdown * 100).toFixed(2) + '%', color: 'negative' },
    { label: 'Alpha', value: m.alpha.toFixed(4), color: '' },
    { label: 'Beta', value: m.beta.toFixed(4), color: '' },
  ]
})

async function runBacktest() {
  loading.value = true
  try {
    report.value = await backtestV2Api.runFactor({
      expression: factorName.value,
      stock_pool: 'hs300',
      start_date: '2020-01-01',
      end_date: '2025-12-31',
    }, btConfig.value)
    await nextTick()
    if (report.value.nav_series.length && navChartRef.value) {
      navChart = echarts.init(navChartRef.value)
      navChart.setOption({
        tooltip: { trigger: 'axis' },
        grid: { left: 60, right: 16, top: 16, bottom: 24 },
        xAxis: { type: 'category', data: report.value.nav_series.map(p => p.date) },
        yAxis: { type: 'value' },
        series: [{
          type: 'line', data: report.value.nav_series.map(p => p.value),
          symbol: 'none', lineStyle: { color: '#22c55e' },
        }],
      })
    }
  } finally {
    loading.value = false
  }
}

onUnmounted(() => navChart?.dispose())
</script>

<style scoped>
.factor-backtest { padding: 16px; }
.page-header { margin-bottom: 16px; }
.page-header h2 { font-size: 18px; margin: 0; }
.layout { display: grid; grid-template-columns: 280px 1fr; gap: 16px; }
.config-panel, .results-panel {
  background: var(--bg-surface);
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 16px;
}
.config-panel h3, .results-panel h3 { font-size: 13px; font-weight: 600; margin: 0 0 12px; }
.metrics-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-bottom: 16px; }
.metric-card {
  border: 1px solid var(--border-ghost);
  border-radius: 6px;
  padding: 10px;
  text-align: center;
}
.metric-label { font-size: 10px; color: var(--text-ghost); }
.metric-value { font-size: 16px; font-weight: 700; margin-top: 4px; }
.positive { color: #d93026; }
.negative { color: #137333; }
.nav-chart { width: 100%; height: 300px; }
.logs { margin-top: 12px; }
.log-line { font-family: monospace; font-size: 11px; color: var(--text-ghost); padding: 2px 0; }
</style>

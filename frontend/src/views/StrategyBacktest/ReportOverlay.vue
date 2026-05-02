<template>
  <el-dialog
    v-model="visible"
    title="回测报告"
    fullscreen
    :show-close="true"
    class="report-overlay"
    @close="$emit('close')"
  >
    <div v-if="result" class="report-body">
      <!-- Section 1: Metrics Grid -->
      <section class="report-section">
        <h3>绩效指标</h3>
        <div class="metrics-grid">
          <div v-for="m in allMetrics" :key="m.label" class="metric-card">
            <div class="metric-card-label">{{ m.label }}</div>
            <div class="metric-card-value" :style="{ color: m.color }">{{ m.value }}</div>
          </div>
        </div>
      </section>

      <!-- Section 2: NAV Curve -->
      <section class="report-section">
        <h3>净值曲线</h3>
        <div ref="navChartRef" class="chart-container"></div>
      </section>

      <!-- Section 3: Trades Table -->
      <section class="report-section">
        <h3>成交明细 ({{ result.trades?.length || 0 }} 笔)</h3>
        <el-table :data="paginatedTrades" stripe size="small" max-height="400">
          <el-table-column prop="trade_date" label="日期" width="110" />
          <el-table-column prop="symbol" label="标的" width="100" />
          <el-table-column label="方向" width="60">
            <template #default="{ row }">
              <span :style="{ color: row.direction === 'buy' ? '#d93026' : '#137333', fontWeight: 600 }">
                {{ row.direction === 'buy' ? '买' : '卖' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="price" label="价格" width="80" />
          <el-table-column prop="quantity" label="数量" width="80" />
          <el-table-column label="金额" width="100">
            <template #default="{ row }">{{ (row.price * row.quantity).toFixed(0) }}</template>
          </el-table-column>
          <el-table-column prop="commission" label="手续费" width="80">
            <template #default="{ row }">{{ row.commission?.toFixed(2) || '0' }}</template>
          </el-table-column>
        </el-table>
        <el-pagination
          v-if="result.trades && result.trades.length > 50"
          v-model:current-page="tradePage"
          :page-size="50"
          :total="result.trades.length"
          layout="prev, pager, next"
          size="small"
          style="margin-top:8px; justify-content:center"
        />
      </section>

      <!-- Section 4: FIFO Positions -->
      <section class="report-section" v-if="result.trades?.length">
        <h3>持仓分析</h3>
        <div class="fifo-placeholder">
          <p class="text-muted">基于 FIFO 成本核算的持仓明细在完整后端集成后展示。</p>
          <p class="text-muted">交易记录包含 {{ result.total_trades || 0 }} 笔，胜率 {{ ((result.win_rate || 0) * 100).toFixed(1) }}%。</p>
        </div>
      </section>

      <!-- Section 5: Risk & Summary -->
      <section class="report-section">
        <h3>风控与回测摘要</h3>
        <div class="summary-grid">
          <div class="summary-item">
            <span class="summary-label">回测周期</span>
            <span class="summary-value">{{ result.start_date }} → {{ result.end_date }}</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">交易日</span>
            <span class="summary-value">{{ result.n_trading_days || 0 }} 天</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">初始资金</span>
            <span class="summary-value">¥{{ (result.initial_capital || 0).toLocaleString() }}</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">最终资金</span>
            <span class="summary-value">¥{{ (result.final_capital || 0).toLocaleString() }}</span>
          </div>
        </div>
      </section>
    </div>

    <div v-else class="empty-report">无回测结果数据</div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, nextTick } from 'vue'
import * as echarts from 'echarts'

interface BacktestResult {
  total_return?: number
  annual_return?: number
  annual_volatility?: number
  sharpe?: number
  sharpe_ratio?: number
  sortino?: number
  sortino_ratio?: number
  max_drawdown?: number
  calmar?: number
  calmar_ratio?: number
  alpha?: number
  beta?: number
  information_ratio?: number
  total_trades?: number
  win_trades?: number
  loss_trades?: number
  win_rate?: number
  trades?: Array<{
    trade_date?: string
    symbol?: string
    direction?: string
    price?: number
    quantity?: number
    commission?: number
  }>
  nav_series?: Array<{ date: string; nav: number }>
  daily_returns?: Array<{ date: string; return: number }>
  start_date?: string | null
  end_date?: string | null
  initial_capital?: number
  final_capital?: number
  n_trading_days?: number
}

const props = defineProps<{
  visible: boolean
  result: BacktestResult | null
}>()

const emit = defineEmits<{
  (e: 'update:visible', v: boolean): void
  (e: 'close'): void
}>()

const visible = computed({
  get: () => props.visible,
  set: (v) => emit('update:visible', v),
})

const tradePage = ref(1)
const navChartRef = ref<HTMLElement | null>(null)
let chartInstance: echarts.ECharts | null = null

function fmtPct(v: number | null | undefined): string {
  if (v == null) return '—'
  return (v * 100).toFixed(2) + '%'
}

function colorPct(v: number | null | undefined): string {
  if (v == null) return '#e2e2ea'
  return v >= 0 ? '#d93026' : '#137333'
}

const allMetrics = computed(() => {
  const r = props.result
  if (!r) return []
  return [
    { label: '总收益率', value: fmtPct(r.total_return), color: colorPct(r.total_return) },
    { label: '年化收益', value: fmtPct(r.annual_return), color: colorPct(r.annual_return) },
    { label: 'Sharpe', value: (r.sharpe ?? r.sharpe_ratio)?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: 'Sortino', value: (r.sortino ?? r.sortino_ratio)?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: '最大回撤', value: fmtPct(r.max_drawdown), color: '#137333' },
    { label: 'Calmar', value: (r.calmar ?? r.calmar_ratio)?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: 'Alpha', value: r.alpha?.toFixed(4) || '—', color: colorPct(r.alpha) },
    { label: 'Beta', value: r.beta?.toFixed(4) || '—', color: '#e2e2ea' },
    { label: '信息比率', value: r.information_ratio?.toFixed(2) || '—', color: '#e2e2ea' },
    { label: '胜率', value: r.win_rate != null ? (r.win_rate * 100).toFixed(1) + '%' : '—', color: '#e2e2ea' },
    { label: '总成交', value: r.total_trades != null ? String(r.total_trades) : '—', color: '#e2e2ea' },
    { label: '最终资金', value: r.final_capital ? '¥' + (r.final_capital / 10000).toFixed(2) + '万' : '—', color: '#a78bfa' },
  ]
})

const paginatedTrades = computed(() => {
  const trades = props.result?.trades || []
  const start = (tradePage.value - 1) * 50
  return trades.slice(start, start + 50)
})

watch(() => props.visible, async (v) => {
  if (v && props.result?.nav_series?.length) {
    await nextTick()
    renderChart()
  }
})

function renderChart() {
  if (!navChartRef.value) return
  if (chartInstance) chartInstance.dispose()

  const navData = props.result?.nav_series || []
  chartInstance = echarts.init(navChartRef.value)
  chartInstance.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
    xAxis: { type: 'category', data: navData.map(d => d.date), axisLabel: { color: '#8888a0', fontSize: 10 } },
    yAxis: { type: 'value', axisLabel: { color: '#8888a0', fontSize: 10 } },
    series: [{
      name: '净值', type: 'line', data: navData.map(d => d.nav),
      lineStyle: { color: '#38bdf8', width: 1.5 },
      areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
        { offset: 0, color: 'rgba(56,189,248,0.15)' },
        { offset: 1, color: 'rgba(56,189,248,0)' },
      ])},
      showSymbol: false,
    }],
  })
}
</script>

<style scoped>
.report-body {
  padding: 0 20px;
  max-width: 1200px;
  margin: 0 auto;
}
.report-section {
  margin-bottom: 32px;
}
.report-section h3 {
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 12px;
  color: var(--text-bright);
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
}
.metric-card {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  padding: 14px;
  text-align: center;
}
.metric-card-label {
  font-size: 11px;
  color: var(--text-label);
  font-weight: 500;
}
.metric-card-value {
  font-size: 20px;
  font-weight: 700;
  margin-top: 6px;
  font-family: var(--font-data);
}

.chart-container { width: 100%; height: 320px; }

.summary-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
}
.summary-item {
  display: flex;
  justify-content: space-between;
  padding: 10px 14px;
  background: var(--bg-surface);
  border-radius: 6px;
}
.summary-label { color: var(--text-label); font-weight: 500; font-size: 12px; }
.summary-value { color: var(--text-primary); font-weight: 600; font-size: 13px; }

.fifo-placeholder {
  padding: 24px;
  text-align: center;
  background: var(--bg-surface);
  border-radius: 8px;
  border: 1px dashed var(--border-default);
}
.text-muted { color: var(--text-muted); font-size: 12px; margin: 4px 0; }
.empty-report { text-align: center; padding: 60px; color: var(--text-muted); }
</style>

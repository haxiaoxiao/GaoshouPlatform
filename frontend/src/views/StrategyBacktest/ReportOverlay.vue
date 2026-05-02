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

      <!-- Section 2: Charts — NAV + Drawdown + Daily Returns -->
      <section class="report-section">
        <h3>净值曲线</h3>
        <div class="charts-row">
          <div ref="navChartRef" class="chart-container chart-main"></div>
          <div ref="returnChartRef" class="chart-container chart-side"></div>
        </div>
      </section>

      <!-- Section 3: Trades Table -->
      <section class="report-section">
        <div class="section-header">
          <h3>成交明细 ({{ result.trades?.length || 0 }} 笔)</h3>
          <el-button size="small" type="primary" plain @click="downloadTradesCSV">
            <el-icon><Download /></el-icon>
            导出CSV
          </el-button>
        </div>
        <el-table :data="paginatedTrades" stripe size="small" max-height="400">
          <el-table-column prop="trade_date" label="日期" width="110" />
          <el-table-column prop="symbol" label="代码" width="100" />
          <el-table-column label="名称" width="90">
            <template #default="{ row }">
              <span style="font-size:11px;color:#ccc">{{ stockNameMap[row.symbol] || '' }}</span>
            </template>
          </el-table-column>
          <el-table-column label="方向" width="60">
            <template #default="{ row }">
              <span :style="{ color: row.direction === 'buy' ? '#d93026' : '#137333', fontWeight: 600 }">
                {{ row.direction === 'buy' ? '买' : '卖' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column prop="price" label="价格" width="80">
            <template #default="{ row }">{{ row.price?.toFixed(2) }}</template>
          </el-table-column>
          <el-table-column prop="quantity" label="数量" width="80" />
          <el-table-column label="金额" width="100">
            <template #default="{ row }">{{ (row.price * row.quantity).toFixed(0) }}</template>
          </el-table-column>
          <el-table-column prop="commission" label="手续费" width="80">
            <template #default="{ row }">{{ (row.commission || 0).toFixed(2) }}</template>
          </el-table-column>
          <el-table-column label="盈亏" width="100">
            <template #default="{ row }">
              <span v-if="row.pnl != null" :style="{ color: pnlColor(row), fontWeight: 600 }">
                {{ row.pnl >= 0 ? '+' : '' }}{{ row.pnl.toFixed(2) }}
              </span>
              <span v-else style="color:#888">—</span>
            </template>
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

      <!-- Section 4: Per-Symbol PnL Analysis -->
      <section class="report-section" v-if="symbolPnls.length > 0">
        <h3>持仓盈亏分析</h3>
        <div class="symbol-pnl-summary">
          <div class="pnl-summary-item">
            <span class="pnl-summary-label">盈利标的</span>
            <span class="pnl-summary-value" style="color:#d93026">{{ symbolPnls.filter(s => s.total_pnl > 0).length }}</span>
          </div>
          <div class="pnl-summary-item">
            <span class="pnl-summary-label">亏损标的</span>
            <span class="pnl-summary-value" style="color:#137333">{{ symbolPnls.filter(s => s.total_pnl < 0).length }}</span>
          </div>
          <div class="pnl-summary-item">
            <span class="pnl-summary-label">总盈亏</span>
            <span class="pnl-summary-value" :style="{ color: totalSymbolPnl >= 0 ? '#d93026' : '#137333' }">
              {{ totalSymbolPnl >= 0 ? '+' : '' }}{{ totalSymbolPnl.toFixed(2) }}
            </span>
          </div>
        </div>
        <el-table :data="symbolPnls" stripe size="small" max-height="400" :default-sort="{ prop: 'total_pnl', order: 'descending' }">
          <el-table-column prop="symbol" label="标的" width="100" sortable />
          <el-table-column label="总盈亏" width="120" sortable prop="total_pnl">
            <template #default="{ row }">
              <span :style="{ color: row.total_pnl >= 0 ? '#d93026' : '#137333', fontWeight: 600 }">
                {{ row.total_pnl >= 0 ? '+' : '' }}{{ row.total_pnl.toFixed(2) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="买均价" width="90" sortable prop="avg_buy_price">
            <template #default="{ row }">{{ row.avg_buy_price?.toFixed(2) || '—' }}</template>
          </el-table-column>
          <el-table-column label="卖均价" width="90" sortable prop="avg_sell_price">
            <template #default="{ row }">{{ row.avg_sell_price?.toFixed(2) || '—' }}</template>
          </el-table-column>
          <el-table-column prop="buy_count" label="买入次数" width="80" sortable />
          <el-table-column prop="sell_count" label="卖出次数" width="80" sortable />
          <el-table-column label="胜率" width="80" sortable prop="win_rate">
            <template #default="{ row }">{{ (row.win_rate * 100).toFixed(0) }}%</template>
          </el-table-column>
        </el-table>
      </section>

      <!-- Section 5: Risk & Summary -->
      <section class="report-section">
        <h3>风控与回测摘要</h3>
        <div class="summary-grid">
          <div class="summary-item">
            <span class="summary-label">回测区间</span>
            <span class="summary-value">{{ result.start_date }} → {{ result.end_date }}</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">交易天数</span>
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
          <div class="summary-item">
            <span class="summary-label">最大回撤</span>
            <span class="summary-value" style="color:#137333">{{ fmtPct(result.max_drawdown) }}</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">年化波动率</span>
            <span class="summary-value">{{ result.annual_volatility ? (result.annual_volatility * 100).toFixed(2) + '%' : '—' }}</span>
          </div>
          <div class="summary-item">
            <span class="summary-label">平均单笔收益</span>
            <span class="summary-value" :style="{ color: (result.avg_return || 0) >= 0 ? '#d93026' : '#137333' }">
              {{ result.avg_return != null ? (result.avg_return >= 0 ? '+' : '') + result.avg_return.toFixed(4) : '—' }}
            </span>
          </div>
          <div class="summary-item">
            <span class="summary-label">日均换手率</span>
            <span class="summary-value">{{ result.turnover_rate ? (result.turnover_rate * 100).toFixed(2) + '%' : '—' }}</span>
          </div>
        </div>
      </section>
    </div>

    <div v-else class="empty-report">无回测结果数据</div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, nextTick } from 'vue'
import { Download } from '@element-plus/icons-vue'
import * as echarts from 'echarts'

interface Trade {
  trade_date?: string
  symbol?: string
  direction?: string
  price?: number
  quantity?: number
  commission?: number
  pnl?: number
}

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
  avg_return?: number
  turnover_rate?: number
  trades?: Trade[]
  nav_series?: Array<{ date: string; nav: number }>
  daily_returns?: Array<{ date: string; return: number }>
  start_date?: string | null
  end_date?: string | null
  initial_capital?: number
  final_capital?: number
  n_trading_days?: number
}

interface SymbolPnl {
  symbol: string
  total_pnl: number
  avg_buy_price: number | null
  avg_sell_price: number | null
  buy_count: number
  sell_count: number
  win_rate: number
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
const returnChartRef = ref<HTMLElement | null>(null)
let navChart: echarts.ECharts | null = null
let returnChart: echarts.ECharts | null = null

// ── Stock names ──
const stockNameMap = ref<Record<string, string>>({})

async function fetchStockNames(symbols: string[]) {
  if (!symbols.length) return
  const unique = [...new Set(symbols.filter(s => !stockNameMap.value[s]))]
  if (!unique.length) return
  try {
    const { default: request } = await import('@/api/request')
    const data = await request.get<Record<string, string>>(
      `/v2/backtest/stock-names?symbols=${unique.join(',')}`
    )
    if (data) {
      stockNameMap.value = { ...stockNameMap.value, ...data }
    }
  } catch { /* non-critical */ }
}

// ── CSV download ──
function downloadTradesCSV() {
  const trades = props.result?.trades || []
  if (!trades.length) return
  const header = '日期,代码,名称,方向,价格,数量,金额,手续费,盈亏'
  const rows = trades.map(t => {
    const name = stockNameMap.value[t.symbol || ''] || ''
    const amount = ((t.price || 0) * (t.quantity || 0)).toFixed(0)
    const dir = t.direction === 'buy' ? '买' : '卖'
    const pnl = t.pnl != null ? t.pnl.toFixed(2) : ''
    return `${t.trade_date || ''},${t.symbol || ''},${name},${dir},${t.price?.toFixed(2) || ''},${t.quantity || ''},${amount},${(t.commission || 0).toFixed(2)},${pnl}`
  })
  const BOM = '﻿'
  const csv = BOM + header + '\n' + rows.join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `trades_${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

function fmtPct(v: number | null | undefined): string {
  if (v == null) return '—'
  return (v * 100).toFixed(2) + '%'
}

function colorPct(v: number | null | undefined): string {
  if (v == null) return '#e2e2ea'
  return v >= 0 ? '#d93026' : '#137333'
}

function pnlColor(row: Trade): string {
  const pnl = row.pnl || 0
  return pnl >= 0 ? '#d93026' : '#137333'
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

// Compute per-symbol PnL from trades
const symbolPnls = computed<SymbolPnl[]>(() => {
  const trades = props.result?.trades || []
  const map = new Map<string, { buys: Trade[]; sells: Trade[] }>()
  for (const t of trades) {
    const sym = t.symbol || '?'
    if (!map.has(sym)) map.set(sym, { buys: [], sells: [] })
    const entry = map.get(sym)!
    if (t.direction === 'buy') entry.buys.push(t)
    else entry.sells.push(t)
  }
  const results: SymbolPnl[] = []
  for (const [sym, { buys, sells }] of map) {
    const total_pnl = sells.reduce((sum, t) => sum + (t.pnl || 0), 0)
    const avg_buy = buys.length > 0 ? buys.reduce((s, t) => s + (t.price || 0), 0) / buys.length : null
    const avg_sell = sells.length > 0 ? sells.reduce((s, t) => s + (t.price || 0), 0) / sells.length : null
    const win_sells = sells.filter(t => (t.pnl || 0) > 0).length
    results.push({
      symbol: sym,
      total_pnl,
      avg_buy_price: avg_buy,
      avg_sell_price: avg_sell,
      buy_count: buys.length,
      sell_count: sells.length,
      win_rate: sells.length > 0 ? win_sells / sells.length : 0,
    })
  }
  results.sort((a, b) => b.total_pnl - a.total_pnl)
  return results
})

const totalSymbolPnl = computed(() => symbolPnls.value.reduce((s, x) => s + x.total_pnl, 0))

// ── Charts ──
function disposeCharts() {
  if (navChart) { navChart.dispose(); navChart = null }
  if (returnChart) { returnChart.dispose(); returnChart = null }
}

watch(() => props.visible, async (v) => {
  if (v && props.result?.nav_series?.length) {
    await nextTick()
    renderCharts()
  } else {
    disposeCharts()
  }
})

// Fetch stock names when result trades change
watch(() => props.result?.trades, (trades) => {
  if (trades?.length) {
    const symbols = trades.map(t => t.symbol).filter(Boolean) as string[]
    fetchStockNames(symbols)
  }
})

function renderCharts() {
  disposeCharts()
  const navData = props.result?.nav_series || []
  const retData = props.result?.daily_returns || []

  // NAV chart with drawdown overlay
  if (navChartRef.value) {
    navChart = echarts.init(navChartRef.value)
    const nvs = navData.map(d => d.nav)
    let peak = 0
    const dd = nvs.map(v => {
      if (v > peak) peak = v
      return peak > 0 ? (v / peak - 1) * 100 : 0
    })

    navChart.setOption({
      tooltip: {
        trigger: 'axis',
        formatter: (ps: any) => {
          const pt = ps.find((p: any) => p.seriesName === '净值')
          const ddPt = ps.find((p: any) => p.seriesName === '回撤')
          return `${ps[0].axisValue}<br/>净值: ${pt?.data?.toFixed(4) || '—'}<br/>回撤: ${ddPt?.data?.toFixed(2) || '—'}%`
        },
      },
      legend: { data: ['净值', '回撤'], textStyle: { color: '#8888a0', fontSize: 11 }, top: 0 },
      grid: { left: 60, right: 60, top: 30, bottom: 30 },
      xAxis: {
        type: 'category', data: navData.map(d => d.date),
        axisLabel: { color: '#8888a0', fontSize: 10, rotate: 45 },
        axisLine: { lineStyle: { color: '#2a2a35' } },
      },
      yAxis: [
        {
          type: 'value', name: '净值', nameTextStyle: { color: '#8888a0', fontSize: 10 },
          axisLabel: { color: '#8888a0', fontSize: 10 },
          splitLine: { lineStyle: { color: '#1a1a25' } },
        },
        {
          type: 'value', name: '回撤 %', nameTextStyle: { color: '#8888a0', fontSize: 10 },
          axisLabel: { color: '#8888a0', fontSize: 10, formatter: '{value}%' },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: '净值', type: 'line', yAxisIndex: 0, data: nvs,
          lineStyle: { color: '#38bdf8', width: 1.5 },
          areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(56,189,248,0.15)' },
            { offset: 1, color: 'rgba(56,189,248,0)' },
          ])},
          showSymbol: false, smooth: true,
        },
        {
          name: '回撤', type: 'line', yAxisIndex: 1, data: dd,
          lineStyle: { color: '#d93026', width: 1 },
          areaStyle: { color: 'rgba(217,48,38,0.08)' },
          showSymbol: false,
        },
      ],
    })
  }

  // Daily returns distribution
  if (returnChartRef.value) {
    returnChart = echarts.init(returnChartRef.value)
    const returns = retData.map(d => d.return * 100)
    returnChart.setOption({
      tooltip: { trigger: 'axis', formatter: (ps: any) => `${ps[0].axisValue}<br/>日收益: ${ps[0].data?.toFixed(4)}%` },
      grid: { left: 50, right: 20, top: 30, bottom: 30 },
      xAxis: {
        type: 'category', data: retData.map(d => d.date),
        axisLabel: { color: '#8888a0', fontSize: 10, rotate: 45 },
        axisLine: { lineStyle: { color: '#2a2a35' } },
      },
      yAxis: {
        type: 'value', name: '日收益 %', nameTextStyle: { color: '#8888a0', fontSize: 10 },
        axisLabel: { color: '#8888a0', fontSize: 10, formatter: '{value}%' },
        splitLine: { lineStyle: { color: '#1a1a25' } },
      },
      series: [{
        name: '日收益', type: 'bar', data: returns,
        itemStyle: {
          color: (p: any) => p.data >= 0 ? '#d93026' : '#137333',
          borderRadius: [0, 0, 0, 0],
        },
      }],
    })
  }
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
.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}
.section-header h3 {
  margin-bottom: 0;
}
.report-section h3 {
  font-size: 14px;
  font-weight: 600;
  margin-bottom: 12px;
  color: #e2e2ea;
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
}
.metric-card {
  background: #1a1a22;
  border: 1px solid #2a2a35;
  border-radius: 8px;
  padding: 14px;
  text-align: center;
}
.metric-card-label {
  font-size: 11px;
  color: #8888a0;
  font-weight: 500;
}
.metric-card-value {
  font-size: 20px;
  font-weight: 700;
  margin-top: 6px;
  font-family: 'JetBrains Mono', 'Courier New', monospace;
}

.charts-row {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 12px;
}
.chart-container { width: 100%; height: 320px; }

.symbol-pnl-summary {
  display: flex;
  gap: 24px;
  margin-bottom: 12px;
}
.pnl-summary-item {
  display: flex;
  align-items: center;
  gap: 8px;
}
.pnl-summary-label {
  font-size: 12px;
  color: #8888a0;
}
.pnl-summary-value {
  font-size: 16px;
  font-weight: 700;
  font-family: 'JetBrains Mono', monospace;
}

.summary-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px;
}
.summary-item {
  display: flex;
  justify-content: space-between;
  padding: 10px 14px;
  background: #1a1a22;
  border: 1px solid #2a2a35;
  border-radius: 6px;
}
.summary-label {
  color: #8888a0;
  font-weight: 500;
  font-size: 12px;
}
.summary-value {
  color: #e2e2ea;
  font-weight: 600;
  font-size: 13px;
  font-family: 'JetBrains Mono', monospace;
}

.empty-report { text-align: center; padding: 60px; color: #8888a0; }

/* Override el-table dark theme */
.report-body :deep(.el-table) {
  --el-table-bg-color: #1a1a22;
  --el-table-tr-bg-color: #1a1a22;
  --el-table-header-bg-color: #141418;
  --el-table-border-color: #2a2a35;
  --el-table-text-color: #e2e2ea;
  --el-table-header-text-color: #8888a0;
  --el-table-row-hover-bg-color: #22222e;
}
</style>

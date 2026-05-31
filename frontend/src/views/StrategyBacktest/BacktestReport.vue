<template>
  <div v-loading="loading" class="backtest-report">
    <template v-if="report">
      <!-- 关键指标卡片 -->
      <el-row :gutter="16" class="metrics-row">
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">总收益率</div>
            <div class="metric-value" :class="getReturnClass(report.result?.total_return)">
              {{ formatPercent(report.result?.total_return) }}
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">年化收益率</div>
            <div class="metric-value" :class="getReturnClass(report.result?.annual_return)">
              {{ formatPercent(report.result?.annual_return) }}
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">最大回撤</div>
            <div class="metric-value negative">
              {{ formatPercent(report.result?.max_drawdown) }}
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">夏普比率</div>
            <div class="metric-value" :class="getReturnClass(report.result?.sharpe_ratio)">
              {{ formatNumber(report.result?.sharpe_ratio, 2) }}
            </div>
          </el-card>
        </el-col>
      </el-row>

      <!-- 交易统计 -->
      <el-card shadow="never" class="stats-card">
        <template #header><span>交易统计</span></template>
        <div class="stats-grid">
          <div class="stat-item">
            <span class="stat-label">总交易次数</span>
            <span class="stat-value">{{ report.result?.total_trades ?? '-' }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">盈利次数</span>
            <span class="stat-value positive">{{ report.result?.win_trades ?? '-' }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">亏损次数</span>
            <span class="stat-value negative">{{ report.result?.loss_trades ?? '-' }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">开仓标的</span>
            <span class="stat-value" style="color:#a78bfa">{{ report.result?.total_positions ?? '-' }}</span>
          </div>
          <div class="stat-item">
            <span class="stat-label">胜率</span>
            <span class="stat-value">{{ formatPercent(report.result?.win_rate) }}</span>
          </div>
        </div>
      </el-card>

      <!-- 成交明细 -->
      <section v-if="report.result?.trades?.length" class="trades-section">
        <div class="section-header">
          <h3>成交明细 ({{ report.result.trades.length }} 笔)</h3>
          <el-button size="small" type="primary" plain @click="downloadTradesCSV">
            <el-icon><Download /></el-icon>导出CSV
          </el-button>
        </div>
        <el-table :data="paginatedTrades" stripe size="small" max-height="400">
          <el-table-column prop="trade_date" label="日期" width="100" />
          <el-table-column prop="symbol" label="代码" width="100" />
          <el-table-column label="名称" width="80">
            <template #default="{ row }">{{ stockNameMap[row.symbol] || row.symbol }}</template>
          </el-table-column>
          <el-table-column label="方向" width="60">
            <template #default="{ row }">
              <span :style="{ color: row.direction === 'buy' ? '#d93026' : '#137333' }">
                {{ row.direction === 'buy' ? '买' : '卖' }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="开仓价" width="80" :formatter="(r: any) => formatTradePrice(r.entry_price ?? r.price)" />
          <el-table-column label="平仓价" width="80" :formatter="(r: any) => formatTradePrice(r.exit_price ?? r.display_price)" />
          <el-table-column prop="quantity" label="数量" width="80" />
          <el-table-column label="成交额" width="100" :formatter="(r: any) => formatMoney((tradeDisplayPrice(r) || 0) * r.quantity)" />
          <el-table-column label="盈亏" width="80">
            <template #default="{ row }">
              <span v-if="row.direction === 'sell' && row.pnl !== null" :style="{ color: pnlColor(row.pnl) }">
                {{ row.pnl > 0 ? '+' : '' }}{{ row.pnl?.toFixed(2) }}%
              </span>
              <span v-else>-</span>
            </template>
          </el-table-column>
        </el-table>
        <el-pagination
          v-if="(report.result?.trades?.length || 0) > 50"
          v-model:current-page="tradePage"
          :page-size="50"
          :total="report.result?.trades?.length || 0"
          layout="prev, pager, next"
          small
          style="margin-top:8px;justify-content:center"
        />
      </section>

      <!-- 净值曲线 -->
      <section v-if="report.result?.nav_series?.length" class="chart-section">
        <h3>净值曲线</h3>
        <div class="charts-row">
          <div ref="navChartRef" class="chart-container chart-main"></div>
          <div ref="returnChartRef" class="chart-container chart-side"></div>
        </div>
      </section>

      <!-- 回测信息 -->
      <el-card shadow="never" class="info-card">
        <template #header><span>回测信息</span></template>
        <el-descriptions :column="2" border>
          <el-descriptions-item label="回测ID">{{ report.backtest_id }}</el-descriptions-item>
          <el-descriptions-item label="策略">{{ report.strategy_name || '-' }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(report.status)" size="small">{{ getStatusLabel(report.status) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="回测区间">{{ report.start_date }} ~ {{ report.end_date }}</el-descriptions-item>
          <el-descriptions-item label="初始资金">{{ formatCapital(report.initial_capital) }}</el-descriptions-item>
          <el-descriptions-item label="最终资金">{{ formatCapital(report.result?.final_capital?.toString() ?? null) }}</el-descriptions-item>
          <el-descriptions-item v-if="report.parameters" label="股票池">{{ report.parameters.pool_label || report.parameters.pool || '-' }}</el-descriptions-item>
          <el-descriptions-item v-if="report.parameters" label="股票数量">{{ report.parameters.symbol_count ?? '-' }} 只</el-descriptions-item>
          <el-descriptions-item v-if="report.parameters" label="持仓上限">{{ report.parameters.max_positions ?? '-' }} 只</el-descriptions-item>
          <el-descriptions-item v-if="report.parameters" label="单票仓位">{{ formatPercentParam(report.parameters.single_pct) }}</el-descriptions-item>
        </el-descriptions>
      </el-card>
    </template>

    <el-empty v-else-if="!loading" description="暂无回测数据" />
  </div>
</template>

<script setup lang="ts">
import { ref, watch, computed, nextTick } from 'vue'
import { ElMessage } from 'element-plus'
import { Download } from '@element-plus/icons-vue'
import * as echarts from '@/lib/echarts'
import { backtestApi, type BacktestReport as BacktestReportType } from '@/api/backtest'
import { formatCapital, getStatusType, getStatusLabel } from '@/utils/format'

const props = defineProps<{
  backtestId: number
}>()

const formatPercentParam = (value: unknown): string =>
  typeof value === 'number' ? `${(value * 100).toFixed(0)}%` : '-'

const loading = ref(false)
const report = ref<BacktestReportType | null>(null)
const tradePage = ref(1)
const stockNameMap = ref<Record<string, string>>({})
const navChartRef = ref<HTMLDivElement | null>(null)
const returnChartRef = ref<HTMLDivElement | null>(null)
let navChart: echarts.ECharts | null = null
let returnChart: echarts.ECharts | null = null

const loadReport = async () => {
  loading.value = true
  try {
    report.value = await backtestApi.get(props.backtestId)
    if (report.value?.result?.trades?.length) {
      await fetchStockNames()
    }
    if (report.value?.result?.nav_series?.length) {
      await nextTick()
      renderCharts()
    }
  } catch {
    ElMessage.error('加载回测报告失败')
    report.value = null
  } finally {
    loading.value = false
  }
}

const fetchStockNames = async () => {
  const syms = [...new Set(report.value?.result?.trades?.map(t => t.symbol).filter(Boolean) || [])]
  if (!syms.length) return
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.get<Record<string, string>>(`/backtest/stock-names?syms=${syms.join(',')}`)
    stockNameMap.value = res || {}
  } catch { /* ignore */ }
}

const paginatedTrades = computed(() => {
  const trades = report.value?.result?.trades || []
  const start = (tradePage.value - 1) * 50
  return trades.slice(start, start + 50)
})

const formatMoney = (v: number) => v >= 10000 ? `${(v / 10000).toFixed(1)}万` : v.toFixed(0)

const tradeDisplayPrice = (row: any): number | null =>
  row.display_price ?? row.exit_price ?? row.price ?? row.entry_price ?? null

const formatTradePrice = (value: number | null | undefined): string =>
  value == null ? '-' : value.toFixed(2)

const renderCharts = () => {
  const navData = report.value?.result?.nav_series || []
  const returnData = report.value?.result?.daily_returns || []

  // NAV chart
  if (navChartRef.value && navData.length) {
    if (!navChart) navChart = echarts.init(navChartRef.value)
    const dates = navData.map(d => d.date)
    const navs = navData.map(d => d.nav)
    const alignNav = (items?: Array<{ date: string; nav: number }>) => {
      const byDate = new Map((items || []).map(item => [item.date, item.nav]))
      return dates.map(day => byDate.get(day) ?? null)
    }
    const benchmarkNavs = alignNav(report.value?.result?.benchmark_nav_series)
    const excessNavs = alignNav(report.value?.result?.excess_nav_series)
    const hasBenchmark = benchmarkNavs.some(value => value != null)
    const hasExcess = excessNavs.some(value => value != null)
    navChart.setOption({
      tooltip: { trigger: 'axis' },
      legend: {
        data: ['NAV', ...(hasBenchmark ? [report.value?.result?.benchmark_name || 'Benchmark'] : []), ...(hasExcess ? ['Excess NAV'] : [])],
        textStyle: { color: '#9ca3af' },
      },
      grid: { left: 50, right: 50, top: 20, bottom: 30 },
      xAxis: { type: 'category', data: dates, show: false },
      yAxis: { type: 'value', axisLabel: { fontSize: 10, formatter: (v: number) => v.toFixed(2) } },
      series: [{
        name: 'NAV', type: 'line', data: navs, smooth: true,
        lineStyle: { color: '#409eff', width: 1 },
        areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(64,158,255,0.2)' }, { offset: 1, color: 'rgba(64,158,255,0)' }
        ])},
      },
      ...(hasBenchmark ? [{
        name: report.value?.result?.benchmark_name || 'Benchmark',
        type: 'line',
        data: benchmarkNavs,
        smooth: true,
        symbol: 'none',
        lineStyle: { color: '#f59e0b', width: 1 },
      }] : []),
      ...(hasExcess ? [{
        name: 'Excess NAV',
        type: 'line',
        data: excessNavs,
        smooth: true,
        symbol: 'none',
        lineStyle: { color: '#a78bfa', width: 1, type: 'dashed' },
      }] : [])],
    }, true)
  }

  // Daily returns chart
  if (returnChartRef.value && returnData.length) {
    if (!returnChart) returnChart = echarts.init(returnChartRef.value)
    const dates = returnData.map(d => d.date)
    const rets = returnData.map(d => d.return)
    returnChart.setOption({
      tooltip: { trigger: 'axis' },
      grid: { left: 50, right: 20, top: 20, bottom: 30 },
      xAxis: { type: 'category', data: dates, show: false },
      yAxis: { type: 'value', axisLabel: { fontSize: 10 } },
      series: [{
        type: 'bar', data: rets,
        itemStyle: { color: (p: any) => p.data >= 0 ? '#d93026' : '#137333' },
      }],
    }, true)
  }
}

const getReturnClass = (value: number | undefined): string => {
  if (value === undefined || value === null) return ''
  return value > 0 ? 'positive' : value < 0 ? 'negative' : ''
}

const pnlColor = (pnl: number | null | undefined) =>
  (pnl || 0) > 0 ? '#d93026' : '#137333'

const formatPercent = (value: number | undefined): string =>
  value !== undefined && value !== null ? `${(value * 100).toFixed(2)}%` : '-'

const formatNumber = (value: number | undefined, decimals: number): string =>
  value !== undefined && value !== null ? value.toFixed(decimals) : '-'

const downloadTradesCSV = () => {
  const trades = report.value?.result?.trades || []
  if (!trades.length) return
  const header = '日期,代码,名称,方向,开仓价,平仓价,数量,金额,盈亏%'
  const rows = trades.map(t => {
    const name = stockNameMap.value[t.symbol || ''] || t.symbol || ''
    const dir = t.direction === 'buy' ? '买' : '卖'
    const amt = (tradeDisplayPrice(t) || 0) * (t.quantity || 0)
    const pnl = t.direction === 'sell' && t.pnl != null ? t.pnl.toFixed(2) : ''
    return `${t.trade_date || ''},${t.symbol || ''},${name},${dir},${formatTradePrice(t.entry_price ?? t.price)},${formatTradePrice(t.exit_price ?? t.display_price)},${t.quantity || ''},${amt.toFixed(0)},${pnl}`
  }).join('\n')
  const csv = '\ufeff' + header + '\n' + rows
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url; a.download = `trades_${new Date().toISOString().slice(0, 10)}.csv`
  a.click(); URL.revokeObjectURL(url)
}

watch(() => props.backtestId, () => {
  if (props.backtestId) loadReport()
}, { immediate: true })
</script>

<style scoped>
.backtest-report { display: flex; flex-direction: column; gap: 16px; }
.metrics-row { margin-bottom: 0; }
.metric-card { text-align: center; }
.metric-card :deep(.el-card__body) { padding: 20px; }
.metric-label { font-size: 14px; color: #909399; margin-bottom: 8px; }
.metric-value { font-size: 28px; font-weight: bold; color: #303133; }
.metric-value.positive { color: #d93026; }
.metric-value.negative { color: #137333; }
.stats-card :deep(.el-card__body) { padding: 20px; }
.stats-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; }
.stat-item { display: flex; flex-direction: column; gap: 8px; }
.stat-label { font-size: 14px; color: #909399; }
.stat-value { font-size: 20px; font-weight: 500; color: #303133; }
.stat-value.positive { color: #d93026; }
.stat-value.negative { color: #137333; }
.info-card :deep(.el-card__body) { padding: 20px; }

.trades-section { background: #1a1a24; border-radius: 8px; padding: 16px; }
.section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
.section-header h3 { margin: 0; font-size: 14px; color: #e2e2ea; }
.chart-section { background: #1a1a24; border-radius: 8px; padding: 16px; }
.chart-section h3 { margin: 0 0 12px; font-size: 14px; color: #e2e2ea; }
.charts-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.chart-container { height: 200px; }
</style>

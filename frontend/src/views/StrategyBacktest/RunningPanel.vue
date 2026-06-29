<template>
  <div class="running-panel">
    <div v-if="running || completed" class="progress-bar-section">
      <el-progress
        :percentage="progressPercent"
        :status="completed ? 'success' : undefined"
        :stroke-width="6"
      />
      <div class="progress-status" v-if="progressMessage">
        {{ progressMessage }} ({{ progressPercent }}%)
      </div>
      <div class="bar-progress-detail" v-if="barProgressDetail">
        {{ barProgressDetail }}
      </div>
      <div class="progress-status" v-else-if="liveData?.current_date">
        正在回测 {{ liveData.current_date }} ({{ progressPercent }}%)
      </div>
      <div v-else-if="running" class="progress-status">任务运行中... ({{ progressPercent }}%)</div>
      <div v-else-if="completed" class="progress-status" style="color:#52c41a">任务完成</div>
    </div>

    <div class="mini-metrics">
      <div v-for="m in miniMetrics" :key="m.label" class="mini-card">
        <div class="mini-label">{{ m.label }}</div>
        <div class="mini-value" :style="{ color: m.color }">{{ m.value }}</div>
      </div>
    </div>

    <div class="live-overview">
      <div class="live-chart-shell">
        <div class="panel-section-head">
          <span>Equity Curve</span>
          <small>{{ equityCurve.length }} pts</small>
        </div>
        <div v-if="equityCurve.length" ref="equityChartRef" class="live-equity-chart"></div>
        <div v-else class="empty-hint compact">等待净值快照</div>
      </div>
      <div class="live-book-shell">
        <div class="panel-section-head">
          <span>Trade Book</span>
          <small>{{ trades.length }} fills</small>
        </div>
        <div v-if="trades.length" class="trade-tape">
          <div v-for="trade in recentTrades" :key="tradeKey(trade)" class="trade-tape-row">
            <span class="trade-date">{{ trade.trade_date || trade.date || '-' }}</span>
            <span class="trade-symbol">{{ trade.symbol || '-' }}</span>
            <span :class="['trade-side', tradeSideClass(trade.direction || trade.action)]">
              {{ directionLabel(trade.direction || trade.action) }}
            </span>
            <span class="trade-price">{{ formatNumber(trade.display_price ?? trade.price ?? trade.exit_price ?? trade.entry_price, 2) }}</span>
            <span class="trade-qty">{{ formatNumber(trade.quantity, 0) }}</span>
          </div>
        </div>
        <div v-else class="empty-hint compact">暂无成交</div>
      </div>
    </div>

    <div class="panel-tabs">
      <div
        v-for="t in tabs"
        :key="t.key"
        :class="['panel-tab', { active: activeTab === t.key }]"
        @click="activeTab = t.key"
      >{{ t.label }}</div>
    </div>

    <div class="tab-body">
      <div v-if="activeTab === 'events'" class="event-stream">
        <div
          v-for="(ev, i) in events"
          :key="i"
          class="event-line"
          :style="{ color: eventColor(ev.type) }"
        >
          <span class="event-time">{{ ev.timestamp?.slice(11, 19) || '' }}</span>
          {{ formatEvent(ev) }}
        </div>
        <div v-if="!events.length" class="empty-hint">等待事件...</div>
      </div>

      <div v-if="activeTab === 'positions'">
        <el-table :data="positionList" size="small" v-if="positionList.length">
          <el-table-column prop="symbol" label="标的" width="100" />
          <el-table-column prop="shares" label="股数" width="80" />
          <el-table-column prop="avg_cost" label="均价" width="80" />
          <el-table-column prop="market_value" label="市值" width="100" />
          <el-table-column label="浮盈" width="120">
            <template #default="{ row }">
              <span :style="{ color: marketValueColor(row.unrealized_pnl) }">
                {{ row.unrealized_pnl >= 0 ? '+' : '' }}{{ row.unrealized_pnl }}
              </span>
            </template>
          </el-table-column>
        </el-table>
        <div v-else class="empty-hint">暂无持仓</div>
      </div>

      <div v-if="activeTab === 'trades'">
        <el-table :data="trades" size="small" v-if="trades.length" max-height="240">
          <el-table-column label="日期" width="102">
            <template #default="{ row }">{{ row.trade_date || row.date || '-' }}</template>
          </el-table-column>
          <el-table-column prop="symbol" label="标的" width="102" />
          <el-table-column label="方向" width="70">
            <template #default="{ row }">
              <span :class="['trade-side', tradeSideClass(row.direction || row.action)]">
                {{ directionLabel(row.direction || row.action) }}
              </span>
            </template>
          </el-table-column>
          <el-table-column label="价格" width="78">
            <template #default="{ row }">{{ formatNumber(row.display_price ?? row.price ?? row.exit_price ?? row.entry_price, 2) }}</template>
          </el-table-column>
          <el-table-column label="数量" width="78">
            <template #default="{ row }">{{ formatNumber(row.quantity, 0) }}</template>
          </el-table-column>
          <el-table-column label="PnL" min-width="84">
            <template #default="{ row }">
              <span :style="{ color: marketValueColor(row.pnl) }">{{ formatSigned(row.pnl) }}</span>
            </template>
          </el-table-column>
        </el-table>
        <div v-else class="empty-hint">暂无成交</div>
      </div>

      <div v-if="activeTab === 'curve'" class="curve-tab">
        <div v-if="equityCurve.length" ref="detailChartRef" class="detail-equity-chart"></div>
        <div v-else class="empty-hint">暂无净值曲线</div>
      </div>

      <div v-if="activeTab === 'metrics'" class="metrics-panel">
        <div v-for="m in detailMetrics" :key="m.label" class="metric-row">
          <span class="metric-label">{{ m.label }}</span>
          <span class="metric-value" :style="{ color: m.color }">{{ m.value }}</span>
        </div>
      </div>

      <div v-if="activeTab === 'logs'" class="log-stream">
        <div v-for="(log, i) in logs" :key="i" class="log-line">{{ log }}</div>
        <div v-if="!logs.length" class="empty-hint">暂无日志</div>
      </div>
    </div>

    <!-- Bottom action -->
    <div class="panel-footer">
      <el-button v-if="completed" type="primary" size="small" @click="$emit('viewReport')">
        查看完整报告
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, nextTick, onBeforeUnmount, watch } from 'vue'
import * as echarts from '@/lib/echarts'
import {
  MARKET_UP_COLOR,
  STATUS_ATTENTION_COLOR,
  marketValueColor,
} from '@/utils/colors'

interface LiveEvent {
  type: string
  timestamp: string
  message?: string
  symbol?: string
  direction?: string
  quantity?: number
  price?: number
  commission?: number
  close?: number
  order_id?: string
  trade_id?: string
  reason?: string
}

interface PositionSnapshot {
  shares: number
  avg_cost: number
  market_value: number
  unrealized_pnl: number
}

interface LiveData {
  current_date: string | null
  events: LiveEvent[]
  positions: Record<string, PositionSnapshot>
  trades?: LiveTrade[]
  orders?: LiveOrder[]
  equity_curve?: EquityPoint[]
  metrics_snapshot: Record<string, number>
  metadata?: Record<string, string | number | boolean | null | undefined>
}

interface LiveTrade {
  trade_id?: string
  order_id?: string
  trade_date?: string
  date?: string
  symbol?: string
  direction?: string
  action?: string
  price?: number
  display_price?: number
  entry_price?: number
  exit_price?: number
  quantity?: number
  commission?: number
  pnl?: number | null
}

interface LiveOrder {
  order_id?: string
  symbol?: string
  direction?: string
  price?: number
  quantity?: number
  status?: string
  filled_quantity?: number
  message?: string
}

interface EquityPoint {
  date: string
  nav?: number
  value?: number
}

const props = defineProps<{
  running: boolean
  completed: boolean
  liveData: LiveData | null
  logs: string[]
  progress?: number
}>()

const progressPercent = computed(() => Math.min(Math.round((props.progress || 0) * 100), 100))
const progressMessage = computed(() => {
  const metadata = props.liveData?.metadata as any
  return metadata?.progress_message || ''
})
const barProgressDetail = computed(() => {
  const metadata = props.liveData?.metadata as any
  const totalBars = Number(metadata?.total_bars || 0)
  const servedBars = Number(metadata?.served_bars || 0)
  const barRatio = Number(metadata?.bar_progress_ratio || 0)
  if (!totalBars || !servedBars) return ''
  const label = barTypeLabel(String(metadata?.bar_type || 'daily'))
  const symbolPart = metadata?.served_symbols && metadata?.total_symbols
    ? ` · 股票 ${Number(metadata.served_symbols).toLocaleString()} / ${Number(metadata.total_symbols).toLocaleString()}`
    : ''
  return `${label} ${Math.min(100, barRatio * 100).toFixed(1)}% · ${servedBars.toLocaleString()} / ${totalBars.toLocaleString()} bars${symbolPart}`
})

const barTypeLabel = (barType: string) => {
  if (barType === 'minute_timer') return '定时分钟Bar'
  if (barType === 'minute') return '分钟Bar'
  return '日线Bar'
}

defineEmits<{
  viewReport: []
}>()

const activeTab = ref('events')
const tabs = [
  { key: 'events', label: '事件流' },
  { key: 'trades', label: 'Trade Book' },
  { key: 'positions', label: '持仓' },
  { key: 'curve', label: '曲线' },
  { key: 'metrics', label: '指标' },
  { key: 'logs', label: '日志' },
]

const events = computed(() => props.liveData?.events || [])

const positionList = computed(() => {
  const p = props.liveData?.positions
  if (!p) return []
  return Object.entries(p).map(([symbol, pos]) => ({ symbol, ...pos }))
})

const trades = computed(() => props.liveData?.trades || [])
const recentTrades = computed(() => trades.value.slice(-8).reverse())
const equityCurve = computed(() => props.liveData?.equity_curve || [])

const miniMetrics = computed(() => {
  const m = props.liveData?.metrics_snapshot
  const p = props.liveData?.positions
  const posCount = p ? Object.keys(p).length : 0
  return [
    { label: '累计收益', value: m?.total_return != null ? (m.total_return * 100).toFixed(2) + '%' : '—', color: marketValueColor(m?.total_return) },
    { label: '总资产', value: m?.total_value != null ? (m.total_value / 10000).toFixed(1) + '万' : '—', color: 'var(--bt-text, #22302a)' },
    { label: '持仓数', value: posCount + ' 只', color: 'var(--bt-pine, #1b3d32)' },
    { label: '累计成交', value: m?.n_trades != null ? m.n_trades + ' 笔' : `${trades.value.length} 笔`, color: 'var(--bt-text, #22302a)' },
    { label: '现金', value: m?.cash != null ? (m.cash / 10000).toFixed(1) + '万' : '—', color: 'var(--bt-text, #22302a)' },
    { label: '最大回撤', value: m?.max_drawdown != null ? (m.max_drawdown * 100).toFixed(2) + '%' : '—', color: STATUS_ATTENTION_COLOR },
  ]
})

const detailMetrics = computed(() => {
  const m = props.liveData?.metrics_snapshot
  return [
    { label: '累计收益', value: m?.total_return != null ? (m.total_return * 100).toFixed(2) + '%' : '—', color: marketValueColor(m?.total_return) },
    { label: '总资产', value: m?.total_value != null ? '¥' + m.total_value.toLocaleString() : '—', color: 'var(--bt-text, #22302a)' },
    { label: '可用现金', value: m?.cash != null ? '¥' + m.cash.toLocaleString() : '—', color: 'var(--bt-text, #22302a)' },
    { label: '累计成交', value: m?.n_trades != null ? String(m.n_trades) : String(trades.value.length), color: 'var(--bt-text, #22302a)' },
    { label: '净值点数', value: String(equityCurve.value.length), color: 'var(--bt-text, #22302a)' },
  ]
})

const EVENT_COLORS: Record<string, string> = {
  ENGINE_START: '#38bdf8',
  ENGINE_END: '#38bdf8',
  BEFORE_TRADING: '#a78bfa',
  AFTER_TRADING: '#a78bfa',
  BAR: '#e2e2ea',
  ORDER_PASS: MARKET_UP_COLOR,
  TRADE: MARKET_UP_COLOR,
  ORDER_REJECT: STATUS_ATTENTION_COLOR,
}

function eventColor(type: string): string {
  return EVENT_COLORS[type] || '#8888a0'
}

function formatEvent(ev: LiveEvent): string {
  if (ev.message) return ev.message
  switch (ev.type) {
    case 'BAR': return `BAR  ${ev.symbol || ''}  close=${ev.close ?? '?'}`
    case 'ORDER_PASS': return `${ev.direction === 'buy' ? '买入' : '卖出'} ${ev.symbol || ''}  ${ev.quantity ?? 0}股 @${ev.price ?? '?'}`
    case 'TRADE': return `成交 ${ev.symbol || ''}  ${ev.quantity ?? 0}×${ev.price ?? '?'}  费${ev.commission ?? 0}`
    case 'ORDER_REJECT': return `拒绝  ${ev.symbol || ''}  — ${ev.reason || '未知原因'}`
    case 'BEFORE_TRADING': return `开盘  ${props.liveData?.current_date || ''}`
    case 'AFTER_TRADING': return `收盘  ${props.liveData?.current_date || ''}`
    default: return `${ev.type}  ${JSON.stringify(ev)}`
  }
}

const equityChartRef = ref<HTMLDivElement | null>(null)
const detailChartRef = ref<HTMLDivElement | null>(null)
let equityChart: echarts.ECharts | null = null
let detailChart: echarts.ECharts | null = null
let resizeObserver: ResizeObserver | null = null

function normalizeNav(point: EquityPoint): number | null {
  if (typeof point.nav === 'number') return point.nav
  if (typeof point.value === 'number') return point.value
  return null
}

function chartOption() {
  const rows = equityCurve.value
  const dates = rows.map(row => row.date)
  const navs = rows.map(row => normalizeNav(row))
  return {
    animation: false,
    grid: { left: 38, right: 12, top: 12, bottom: 22 },
    tooltip: {
      trigger: 'axis',
      confine: true,
      formatter: (params: any) => {
        const p = Array.isArray(params) ? params[0] : params
        return `${p?.axisValue || '-'}<br/>NAV ${Number(p?.data || 0).toFixed(4)}`
      },
    },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: { color: '#7e8d86', fontSize: 10, hideOverlap: true },
      axisLine: { lineStyle: { color: '#e5dfd3' } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLabel: { color: '#7e8d86', fontSize: 10, formatter: (v: number) => v.toFixed(2) },
      splitLine: { lineStyle: { color: 'rgba(27,61,50,0.08)' } },
    },
    series: [{
      name: 'NAV',
      type: 'line',
      data: navs,
      showSymbol: false,
      smooth: true,
      lineStyle: { color: '#1b3d32', width: 1.6 },
      areaStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(27,61,50,0.18)' },
          { offset: 1, color: 'rgba(27,61,50,0)' },
        ]),
      },
    }],
  }
}

function ensureCharts() {
  if (equityChartRef.value && !equityChart) {
    equityChart = echarts.init(equityChartRef.value)
  }
  if (detailChartRef.value && !detailChart) {
    detailChart = echarts.init(detailChartRef.value)
  }
  if (!resizeObserver && (equityChartRef.value || detailChartRef.value)) {
    resizeObserver = new ResizeObserver(() => {
      equityChart?.resize()
      detailChart?.resize()
    })
    if (equityChartRef.value) resizeObserver.observe(equityChartRef.value)
    if (detailChartRef.value) resizeObserver.observe(detailChartRef.value)
  }
}

function renderCharts() {
  if (!equityCurve.value.length) {
    equityChart?.clear()
    detailChart?.clear()
    return
  }
  ensureCharts()
  const option = chartOption()
  equityChart?.setOption(option, true)
  detailChart?.setOption(option, true)
}

watch([equityCurve, activeTab], async () => {
  await nextTick()
  renderCharts()
}, { deep: true })

onBeforeUnmount(() => {
  resizeObserver?.disconnect()
  equityChart?.dispose()
  detailChart?.dispose()
})

function formatNumber(value: number | null | undefined, decimals: number): string {
  if (value == null || Number.isNaN(Number(value))) return '-'
  return Number(value).toLocaleString('zh-CN', {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

function formatSigned(value: number | null | undefined): string {
  if (value == null || Number.isNaN(Number(value))) return '-'
  const prefix = value > 0 ? '+' : ''
  return `${prefix}${formatNumber(value, 2)}`
}

function directionLabel(direction?: string): string {
  const normalized = String(direction || '').toLowerCase()
  if (['buy', 'open', 'long'].includes(normalized)) return '买入'
  if (['sell', 'close', 'short'].includes(normalized)) return '卖出'
  return direction || '-'
}

function tradeSideClass(direction?: string): string {
  const normalized = String(direction || '').toLowerCase()
  if (['buy', 'open', 'long'].includes(normalized)) return 'is-buy'
  if (['sell', 'close', 'short'].includes(normalized)) return 'is-sell'
  return ''
}

function tradeKey(trade: LiveTrade): string {
  return trade.trade_id || trade.order_id || `${trade.trade_date || trade.date}-${trade.symbol}-${trade.direction}-${trade.price}-${trade.quantity}`
}
</script>

<style scoped>
.progress-bar-section {
  margin-bottom: 4px;
}
.progress-status {
  font-size: 11px;
  color: #8888a0;
  margin-top: 2px;
  text-align: center;
}
.bar-progress-detail {
  margin-top: 3px;
  color: var(--bt-pine, #1b3d32);
  font-size: 11px;
  font-family: var(--font-data, ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace);
  text-align: center;
}
.running-panel {
  display: flex;
  flex-direction: column;
  gap: 8px;
  height: 100%;
  min-height: 0;
  overflow: hidden;
}

.mini-metrics {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 5px;
}
.mini-card {
  background: rgba(245, 242, 234, 0.9);
  border: 1px solid var(--bt-border, #e5dfd3);
  border-radius: 6px;
  padding: 6px 8px;
  text-align: center;
}
.mini-label {
  font-size: 9px;
  color: var(--bt-muted, #7e8d86);
  font-weight: 500;
}
.mini-value {
  font-size: 14px;
  font-weight: 700;
  margin-top: 2px;
}

.panel-tabs {
  display: flex;
  border-bottom: 1px solid var(--bt-border, #e5dfd3);
  gap: 0;
  overflow-x: auto;
}
.panel-tab {
  padding: 5px 12px;
  font-size: 11px;
  color: var(--bt-secondary, #54635c);
  cursor: pointer;
  transition: color 0.15s;
  white-space: nowrap;
}
.panel-tab:hover { color: var(--bt-text, #22302a); }
.panel-tab.active { color: var(--bt-pine, #1b3d32); border-bottom: 2px solid var(--bt-pine, #1b3d32); }

.tab-body {
  flex: 1;
  overflow-y: auto;
  min-height: 0;
  border: 1px solid rgba(27, 61, 50, 0.08);
  border-radius: 6px;
  background: rgba(253, 251, 247, 0.56);
}

.event-stream {
  font-family: var(--font-display);
  font-size: 10px;
  line-height: 1.7;
  padding: 6px 0;
}
.event-line { padding: 1px 4px; }
.event-time { color: #6e6e85; margin-right: 6px; }

.empty-hint {
  color: var(--bt-muted, #7e8d86);
  font-style: italic;
  padding: 20px;
  text-align: center;
  font-size: 12px;
}
.empty-hint.compact {
  display: grid;
  min-height: 78px;
  place-items: center;
  padding: 8px;
}

.metrics-panel { padding: 12px; }
.metric-row {
  display: flex;
  justify-content: space-between;
  padding: 8px 0;
  border-bottom: 1px solid var(--border-subtle);
  font-size: 13px;
}
.metric-label { color: #b0b0c0; font-weight: 500; }
.metric-value { font-weight: 700; font-family: var(--font-data); font-size: 15px; }

.log-stream {
  font-family: var(--font-display);
  font-size: 10px;
  line-height: 1.6;
  padding: 6px 4px;
  color: var(--bt-secondary, #54635c);
}
.log-line { padding: 1px 0; }

.panel-footer {
  display: flex;
  justify-content: flex-end;
  padding-top: 4px;
  position: sticky;
  bottom: 0;
  background: rgba(253, 251, 247, 0.9);
  z-index: 10;
}

.live-overview {
  display: grid;
  grid-template-columns: minmax(0, 1.1fr) minmax(180px, 0.9fr);
  gap: 8px;
  min-height: 144px;
}

.live-chart-shell,
.live-book-shell {
  min-width: 0;
  border: 1px solid var(--bt-border, #e5dfd3);
  border-radius: 6px;
  background: rgba(253, 251, 247, 0.72);
  overflow: hidden;
}

.panel-section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 28px;
  padding: 0 8px;
  border-bottom: 1px solid rgba(27, 61, 50, 0.08);
  color: var(--bt-secondary, #54635c);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.panel-section-head small {
  color: var(--bt-muted, #7e8d86);
  font-family: var(--font-data);
  font-weight: 600;
  letter-spacing: 0;
  text-transform: none;
}

.live-equity-chart {
  width: 100%;
  height: 116px;
}

.detail-equity-chart {
  width: 100%;
  height: 240px;
}

.curve-tab {
  padding: 6px;
}

.trade-tape {
  display: flex;
  max-height: 116px;
  flex-direction: column;
  overflow: auto;
}

.trade-tape-row {
  display: grid;
  grid-template-columns: 66px minmax(68px, 1fr) 44px 58px 48px;
  gap: 5px;
  align-items: center;
  min-height: 24px;
  padding: 0 7px;
  border-bottom: 1px solid rgba(27, 61, 50, 0.06);
  color: var(--bt-text, #22302a);
  font-family: var(--font-data);
  font-size: 10px;
}

.trade-date,
.trade-qty {
  color: var(--bt-muted, #7e8d86);
}

.trade-symbol,
.trade-price {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.trade-side {
  font-weight: 800;
}

.trade-side.is-buy {
  color: var(--market-up, #d93026);
}

.trade-side.is-sell {
  color: var(--market-down, #137333);
}

@media (max-width: 900px) {
  .live-overview {
    grid-template-columns: 1fr;
  }
}
</style>

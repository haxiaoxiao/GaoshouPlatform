<template>
  <div class="running-panel">
    <!-- Mini metrics grid -->
    <div class="mini-metrics">
      <div v-for="m in miniMetrics" :key="m.label" class="mini-card">
        <div class="mini-label">{{ m.label }}</div>
        <div class="mini-value" :style="{ color: m.color }">{{ m.value }}</div>
      </div>
    </div>

    <!-- Tabs -->
    <div class="panel-tabs">
      <div
        v-for="t in tabs"
        :key="t.key"
        :class="['panel-tab', { active: activeTab === t.key }]"
        @click="activeTab = t.key"
      >{{ t.label }}</div>
    </div>

    <!-- Tab content -->
    <div class="tab-body">
      <!-- Events -->
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

      <!-- Positions -->
      <div v-if="activeTab === 'positions'">
        <el-table :data="positionList" size="small" v-if="positionList.length">
          <el-table-column prop="symbol" label="标的" width="100" />
          <el-table-column prop="shares" label="股数" width="80" />
          <el-table-column prop="avg_cost" label="均价" width="80" />
          <el-table-column prop="market_value" label="市值" width="100" />
          <el-table-column label="浮盈" width="120">
            <template #default="{ row }">
              <span :style="{ color: row.unrealized_pnl >= 0 ? '#d93026' : '#137333' }">
                {{ row.unrealized_pnl >= 0 ? '+' : '' }}{{ row.unrealized_pnl }}
              </span>
            </template>
          </el-table-column>
        </el-table>
        <div v-else class="empty-hint">暂无持仓</div>
      </div>

      <!-- Metrics -->
      <div v-if="activeTab === 'metrics'" class="metrics-panel">
        <div v-for="m in detailMetrics" :key="m.label" class="metric-row">
          <span class="metric-label">{{ m.label }}</span>
          <span class="metric-value" :style="{ color: m.color }">{{ m.value }}</span>
        </div>
      </div>

      <!-- Logs -->
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
import { ref, computed } from 'vue'

interface LiveEvent {
  type: string
  timestamp: string
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
  metrics_snapshot: Record<string, number>
}

const props = defineProps<{
  running: boolean
  completed: boolean
  liveData: LiveData | null
  logs: string[]
}>()

defineEmits<{
  viewReport: []
}>()

const activeTab = ref('events')
const tabs = [
  { key: 'events', label: '事件流' },
  { key: 'positions', label: '持仓' },
  { key: 'metrics', label: '指标' },
  { key: 'logs', label: '日志' },
]

const events = computed(() => props.liveData?.events || [])

const positionList = computed(() => {
  const p = props.liveData?.positions
  if (!p) return []
  return Object.entries(p).map(([symbol, pos]) => ({ symbol, ...pos }))
})

const miniMetrics = computed(() => {
  const m = props.liveData?.metrics_snapshot
  const p = props.liveData?.positions
  const posCount = p ? Object.keys(p).length : 0
  return [
    { label: '累计收益', value: m?.total_return != null ? (m.total_return * 100).toFixed(2) + '%' : '—', color: (m?.total_return ?? 0) >= 0 ? '#d93026' : '#137333' },
    { label: '总资产', value: m?.total_value != null ? (m.total_value / 10000).toFixed(1) + '万' : '—', color: '#e2e2ea' },
    { label: '持仓数', value: posCount + ' 只', color: '#a78bfa' },
    { label: '累计成交', value: m?.n_trades != null ? m.n_trades + ' 笔' : '—', color: '#e2e2ea' },
    { label: '现金', value: m?.cash != null ? (m.cash / 10000).toFixed(1) + '万' : '—', color: '#e2e2ea' },
    { label: '最大回撤', value: m?.max_drawdown != null ? (m.max_drawdown * 100).toFixed(2) + '%' : '—', color: '#137333' },
  ]
})

const detailMetrics = computed(() => {
  const m = props.liveData?.metrics_snapshot
  return [
    { label: '累计收益', value: m?.total_return != null ? (m.total_return * 100).toFixed(2) + '%' : '—', color: (m?.total_return ?? 0) >= 0 ? '#d93026' : '#137333' },
    { label: '总资产', value: m?.total_value != null ? '¥' + m.total_value.toLocaleString() : '—', color: '#e2e2ea' },
    { label: '可用现金', value: m?.cash != null ? '¥' + m.cash.toLocaleString() : '—', color: '#e2e2ea' },
    { label: '累计成交', value: m?.n_trades != null ? String(m.n_trades) : '—', color: '#e2e2ea' },
  ]
})

const EVENT_COLORS: Record<string, string> = {
  ENGINE_START: '#38bdf8',
  ENGINE_END: '#38bdf8',
  BEFORE_TRADING: '#a78bfa',
  AFTER_TRADING: '#a78bfa',
  BAR: '#e2e2ea',
  ORDER_PASS: '#d93026',
  TRADE: '#d93026',
  ORDER_REJECT: '#137333',
}

function eventColor(type: string): string {
  return EVENT_COLORS[type] || '#8888a0'
}

function formatEvent(ev: LiveEvent): string {
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
</script>

<style scoped>
.running-panel {
  display: flex;
  flex-direction: column;
  gap: 8px;
  height: 100%;
}

.mini-metrics {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 5px;
}
.mini-card {
  background: #1a1a22;
  border-radius: 5px;
  padding: 6px 8px;
  text-align: center;
}
.mini-label {
  font-size: 9px;
  color: #8888a0;
  font-weight: 500;
}
.mini-value {
  font-size: 14px;
  font-weight: 700;
  margin-top: 2px;
}

.panel-tabs {
  display: flex;
  border-bottom: 1px solid #2a2a35;
  gap: 0;
}
.panel-tab {
  padding: 5px 12px;
  font-size: 11px;
  color: #8888a0;
  cursor: pointer;
  transition: color 0.15s;
}
.panel-tab:hover { color: #d4d4d4; }
.panel-tab.active { color: #38bdf8; border-bottom: 2px solid #38bdf8; }

.tab-body {
  flex: 1;
  overflow-y: auto;
  min-height: 0;
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
  color: #8888a0;
  font-style: italic;
  padding: 20px;
  text-align: center;
  font-size: 12px;
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
  color: #8888a0;
}
.log-line { padding: 1px 0; }

.panel-footer {
  display: flex;
  justify-content: flex-end;
  padding-top: 4px;
}
</style>

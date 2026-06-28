<template>
  <div class="page-frame data-center-page">
    <header class="data-command-bar">
      <div class="data-command-bar__copy">
        <span class="section-kicker">DATA VIEW / MULTI-SOURCE CONSOLE</span>
        <h2>数据查看</h2>
        <p>把行情、财务、因子和舆情口径放在同一张工作台里扫描，查看与同步动作保持分离。</p>
      </div>

      <div class="data-command-bar__actions">
        <div class="layout-switcher" aria-label="切换数据查看布局">
          <button
            v-for="mode in layoutModes"
            :key="mode.key"
            type="button"
            :class="{ active: layoutMode === mode.key }"
            @click="layoutMode = mode.key"
          >
            {{ mode.label }}
          </button>
        </div>
        <el-button :icon="Refresh" :loading="loading" @click="loadDashboard">刷新口径</el-button>
        <el-button type="primary" @click="goSync">去数据同步</el-button>
      </div>
    </header>

    <section v-if="layoutMode === 'A'" class="layout-data-a">
      <aside class="stock-list-pane">
        <div class="pane-head">
          <span>STOCK LIST</span>
          <strong>{{ filteredStocks.length }}</strong>
        </div>
        <el-input v-model="stockKeyword" clearable placeholder="股票代码 / 名称" size="small" />
        <div class="stock-list">
          <button
            v-for="stock in filteredStocks"
            :key="stock.symbol"
            type="button"
            class="stock-row"
            :class="{ active: activeStockCode === stock.symbol }"
            @click="activeStockCode = stock.symbol"
          >
            <span>{{ stock.symbol }}</span>
            <strong>{{ stock.name }}</strong>
            <small>{{ stock.theme }}</small>
          </button>
        </div>
      </aside>

      <main class="data-viewport">
        <div class="viewport-head">
          <div>
            <span class="section-kicker">MULTI-DIMENSIONAL DATA PANELS</span>
            <h3>{{ activeStock.name }} · {{ activeStock.symbol }}</h3>
          </div>
          <div class="view-tabs">
            <button
              v-for="tab in tabs"
              :key="tab.key"
              type="button"
              :class="{ active: activeTab === tab.key }"
              @click="activeTab = tab.key"
            >
              {{ tab.label }}
            </button>
          </div>
        </div>

        <div class="tab-panel">
          <StockList v-if="mountedTabs.stocks" v-show="activeTab === 'stocks'" />
          <KlineQuery v-if="mountedTabs.quotes" v-show="activeTab === 'quotes'" />
          <SentimentPanel v-if="mountedTabs.sentiment" v-show="activeTab === 'sentiment'" />
        </div>
      </main>

      <aside class="sidebar-events">
        <div class="pane-head">
          <span>CORRELATED EVENTS</span>
          <strong>{{ eventRows.length }}</strong>
        </div>
        <div class="event-list">
          <div v-for="event in eventRows" :key="event.key" class="event-row" :class="`event-row--${event.tone}`">
            <span>{{ event.time }}</span>
            <strong>{{ event.title }}</strong>
            <small>{{ event.detail }}</small>
          </div>
        </div>
      </aside>
    </section>

    <section v-else-if="layoutMode === 'B'" class="layout-data-b">
      <div class="sheet-toolbar">
        <div class="sheet-filters">
          <span>行业 全部</span>
          <span>市值 全区间</span>
          <span>状态 {{ overallStatusLabel }}</span>
        </div>
        <strong>{{ freshnessCards.length }} 个数据口径</strong>
      </div>

      <div class="audit-sheet">
        <table>
          <thead>
            <tr>
              <th>数据域</th>
              <th>来源</th>
              <th>最新日期</th>
              <th>状态</th>
              <th>覆盖规模</th>
              <th>说明</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in freshnessCards" :key="item.key">
              <td>
                <strong>{{ item.title }}</strong>
                <span>{{ item.key }}</span>
              </td>
              <td>{{ item.source }}</td>
              <td class="mono">{{ item.value }}</td>
              <td><b :class="`status-pill status-pill--${item.tone}`">{{ item.status }}</b></td>
              <td>{{ item.rowCount }}</td>
              <td>{{ item.detail }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section v-else class="layout-data-c">
      <article class="stock-brief">
        <span class="section-kicker">ACTIVE STOCK</span>
        <h3>{{ activeStock.name }}</h3>
        <strong>{{ activeStock.symbol }}</strong>
        <p>{{ activeStock.theme }} · 当前页面只记录查看焦点，不改变后端查询条件。</p>
        <div class="brief-grid">
          <div v-for="row in coverageRows.slice(0, 4)" :key="row.label">
            <span>{{ row.label }}</span>
            <strong>{{ row.value }}</strong>
          </div>
        </div>
      </article>

      <div class="tile-board">
        <article
          v-for="item in dashboardTiles"
          :key="item.key"
          class="data-tile"
          :class="`data-tile--${item.tone}`"
        >
          <span>{{ item.kicker }}</span>
          <strong>{{ item.value }}</strong>
          <p>{{ item.title }}</p>
          <small>{{ item.detail }}</small>
        </article>
      </div>

      <article class="panel-strip">
        <div class="pane-head">
          <span>RECENT PIPELINE</span>
          <strong>{{ syncLogs.length }}</strong>
        </div>
        <div class="compact-log-grid">
          <div v-for="log in syncLogs.slice(0, 6)" :key="log.id" class="compact-log-row">
            <span>{{ formatDateTime(log.end_time || log.start_time || log.created_at) }}</span>
            <strong>{{ syncTypeLabel(log.sync_type) }}</strong>
            <b :class="`status-${log.status}`">{{ syncStatusLabel(log.status) }}</b>
          </div>
          <p v-if="!syncLogs.length" class="empty-copy">暂无同步记录。</p>
        </div>
      </article>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh } from '@element-plus/icons-vue'
import { usePageContext } from '@/app/pageContext'
import StockList from './StockList.vue'
import KlineQuery from './KlineQuery.vue'
import SentimentPanel from './SentimentPanel.vue'
import { systemApi, type DataSummary, type DataSummaryItem } from '@/api/system'
import { syncApi, type SyncLog } from '@/api/sync'

type FreshnessTone = 'good' | 'warn' | 'bad' | 'neutral'
type TabKey = 'stocks' | 'quotes' | 'sentiment'
type LayoutMode = 'A' | 'B' | 'C'

interface FreshnessCard {
  key: string
  title: string
  source: string
  value: string
  detail: string
  rowCount: string
  status: string
  tone: FreshnessTone
}

interface CoverageRow {
  label: string
  value: string
  hint: string
}

const router = useRouter()
const loading = ref(false)
const dataSummary = ref<DataSummary | null>(null)
const syncLogs = ref<SyncLog[]>([])
const activeTab = ref<TabKey>('quotes')
const layoutMode = ref<LayoutMode>('A')
const stockKeyword = ref('')
const activeStockCode = ref('600519.SH')
const mountedTabs = ref<Record<TabKey, boolean>>({
  stocks: false,
  quotes: true,
  sentiment: false,
})

const layoutModes: { key: LayoutMode; label: string }[] = [
  { key: 'A', label: 'A 三栏联动' },
  { key: 'B', label: 'B 大表审计' },
  { key: 'C', label: 'C 多维瓷块' },
]

const tabs: { key: TabKey; label: string }[] = [
  { key: 'quotes', label: '行情数据' },
  { key: 'stocks', label: '财务报表' },
  { key: 'sentiment', label: '舆情' },
]

const sampleStocks = [
  { symbol: '600519.SH', name: '贵州茅台', theme: '白酒 / 高股息' },
  { symbol: '000001.SZ', name: '平安银行', theme: '金融 / 低估值' },
  { symbol: '300750.SZ', name: '宁德时代', theme: '新能源 / 成长' },
  { symbol: '000333.SZ', name: '美的集团', theme: '家电 / 现金流' },
  { symbol: '399101.SZ', name: '中小综指', theme: '小市值策略池' },
]

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const overallStatusLabel = computed(() => dataSummary.value?.overall_status || 'unknown')
const activeStock = computed(() => sampleStocks.find(stock => stock.symbol === activeStockCode.value) || sampleStocks[0])
const filteredStocks = computed(() => {
  const keyword = stockKeyword.value.trim().toLowerCase()
  if (!keyword) return sampleStocks
  return sampleStocks.filter(stock => (
    stock.symbol.toLowerCase().includes(keyword)
    || stock.name.toLowerCase().includes(keyword)
    || stock.theme.toLowerCase().includes(keyword)
  ))
})

const freshnessCards = computed<FreshnessCard[]>(() => [
  buildSummaryCard('market_daily', '日线行情', '因子、回测、指数基准的最低前置。'),
  buildSummaryCard('market_minute', '分钟行情', '日内策略与 minute_timer 抽样输入。'),
  buildSummaryCard('stocks', '股票基础数据', '代码、行业、状态、市值和股本字段。'),
  buildSummaryCard('financial', '财务报表', '质量、成长、估值因子的 point-in-time 口径。'),
  buildSummaryCard('factor_values', '因子缓存', '评估页只消费已经落盘的因子值。'),
  buildSummaryCard('stock_indicators', '指标缓存', 'Indicator 体系预计算结果。'),
  buildSummaryCard('concept_membership', '概念与行业扩展', '同花顺概念成员和主题扩展信号。'),
  buildSummaryCard('sentiment', '新闻舆情数据', '用于研究假设验证，不默认参与回测。'),
])

const coverageRows = computed<CoverageRow[]>(() => [
  summaryRow('行情主口径', 'market_daily', '日线 trade_date 最新可用日'),
  summaryRow('日内口径', 'market_minute', '分钟线 datetime 最新可用分钟'),
  summaryRow('timer 抽样', 'minute_timer', 'minute_timer 回测优先读取'),
  summaryRow('基础股票', 'stocks', '股票元数据 updated_at 最新口径'),
  summaryRow('财务报表', 'financial', 'report_date 最新披露期'),
  summaryRow('舆情样本', 'sentiment', '新闻舆情 published_at 最新样本'),
])

const eventRows = computed(() => {
  const logRows = syncLogs.value.slice(0, 6).map(log => ({
    key: `log-${log.id}`,
    time: formatDateTime(log.end_time || log.start_time || log.created_at),
    title: syncTypeLabel(log.sync_type),
    detail: syncStatusLabel(log.status),
    tone: log.status === 'completed' ? 'good' as FreshnessTone : log.status === 'failed' ? 'bad' as FreshnessTone : 'warn' as FreshnessTone,
  }))
  if (logRows.length) return logRows
  return freshnessCards.value.slice(0, 6).map(card => ({
    key: `fresh-${card.key}`,
    time: card.value,
    title: card.title,
    detail: card.status,
    tone: card.tone,
  }))
})

const dashboardTiles = computed(() => [
  tileFromCard('minute', '分钟就绪度', 'market_minute', '分钟'),
  tileFromCard('sentiment', '舆情指数', 'sentiment', '舆情'),
  tileFromCard('factor', '因子覆盖率', 'factor_values', '因子'),
  tileFromCard('finance', '财务披露', 'financial', '财务'),
])

function goSync() {
  router.push('/data/sync')
}

async function loadDashboard() {
  loading.value = true
  try {
    const [summaryResult, logsResult] = await Promise.allSettled([
      systemApi.dataSummary(),
      syncApi.getLogs({ limit: 20 }),
    ])

    if (summaryResult.status === 'fulfilled') dataSummary.value = summaryResult.value
    if (logsResult.status === 'fulfilled') syncLogs.value = logsResult.value
  } finally {
    loading.value = false
  }
}

function buildSummaryCard(key: string, title: string, detail: string): FreshnessCard {
  const item = summaryMap.value[key]
  const tone = summaryTone(item)
  const rowCount = item ? formatRowCount(item.row_count, item.row_count_estimated) : '未接入'
  return {
    key,
    title,
    source: item?.source || key,
    value: latestValue(item),
    detail,
    rowCount,
    status: item?.status_text || toneLabel(tone),
    tone,
  }
}

function summaryRow(label: string, key: string, hint: string): CoverageRow {
  const item = summaryMap.value[key]
  return {
    label,
    value: latestValue(item),
    hint: item ? `${hint} · ${formatRowCount(item.row_count, item.row_count_estimated)}` : `${hint} · 暂无后端口径`,
  }
}

function tileFromCard(key: string, title: string, summaryKey: string, kicker: string) {
  const card = buildSummaryCard(summaryKey, title, `${activeStock.value.symbol} 当前聚焦 · ${title}`)
  return {
    key,
    kicker,
    title,
    value: card.value,
    detail: `${card.status} · ${card.rowCount}`,
    tone: card.tone,
  }
}

function latestValue(item?: DataSummaryItem): string {
  return formatDateTime(item?.latest_datetime || item?.latest_date)
}

function summaryTone(item?: DataSummaryItem): FreshnessTone {
  if (!item) return 'bad'
  if (item.status === 'good') return 'good'
  if (item.status === 'stale') return 'warn'
  if (item.status === 'missing' || item.status === 'error') return 'bad'
  return 'neutral'
}

function toneLabel(tone: FreshnessTone): string {
  const labels: Record<FreshnessTone, string> = {
    good: '新鲜',
    warn: '需关注',
    bad: '待补齐',
    neutral: '未知',
  }
  return labels[tone]
}

function syncTypeLabel(type: string): string {
  const labels: Record<string, string> = {
    datasync: '一键同步',
    stock_info: '股票基础',
    stock_full: '股票完整',
    financial_data: '财务报表',
    kline_daily: '日线行情',
    index_daily: '指数日线',
    kline_minute: '分钟行情',
    realtime_mv: '实时市值',
    dividends: '分红数据',
    factor_dependency: '因子依赖',
    tushare_relay: 'Tushare Relay',
    ths_concept: '同花顺概念',
    sentiment_xueqiu: '雪球舆情',
    sentiment_nga: 'NGA 舆情',
  }
  return labels[type] || type
}

function syncStatusLabel(status: string): string {
  const labels: Record<string, string> = {
    idle: '空闲',
    queued: '排队',
    running: '运行中',
    completed: '完成',
    failed: '失败',
    cancelled: '取消',
  }
  return labels[status] || status
}

function formatRowCount(value?: number | null, estimated = false): string {
  if (value === null || value === undefined) return '未统计'
  const prefix = estimated ? '约 ' : ''
  return `${prefix}${value.toLocaleString()} 行`
}

function formatDateTime(value?: string | null): string {
  if (!value) return '-'
  return value.replace('T', ' ').slice(0, value.includes(':') ? 16 : 10)
}

const pageContextBlocks = computed(() => [
  {
    title: 'Data View',
    rows: [
      { label: '布局', value: layoutMode.value },
      { label: '当前焦点', value: activeStock.value.symbol },
      { label: '当前标签', value: tabs.find(tab => tab.key === activeTab.value)?.label || activeTab.value },
      { label: '刷新状态', value: loading.value ? '刷新中' : '已就绪', tone: loading.value ? 'warn' : 'good' },
    ],
  },
  {
    title: 'Coverage',
    rows: coverageRows.value.slice(0, 4).map(row => ({
      label: row.label,
      value: row.value,
      tone: row.value === '-' ? 'warn' : 'good',
    })),
  },
])

usePageContext(pageContextBlocks)

onMounted(loadDashboard)

watch(activeTab, (tab) => {
  mountedTabs.value[tab] = true
})
</script>

<style scoped>
.data-center-page {
  overflow: auto;
  color: var(--text-primary);
}

.data-command-bar {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: end;
  gap: 18px;
  padding-bottom: 14px;
  border-bottom: 1px solid var(--border-default);
}

.data-command-bar__copy {
  display: grid;
  gap: 6px;
}

.data-command-bar h2,
.viewport-head h3,
.stock-brief h3 {
  margin: 0;
  color: var(--text-bright);
}

.data-command-bar p,
.stock-brief p,
.data-tile p,
.data-tile small {
  margin: 0;
  color: var(--text-secondary);
}

.data-command-bar__actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.layout-switcher,
.view-tabs {
  display: inline-flex;
  gap: 2px;
  padding: 3px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-elevated);
}

.layout-switcher button,
.view-tabs button {
  border: 0;
  border-radius: 6px;
  padding: 7px 10px;
  color: var(--text-secondary);
  background: transparent;
  font-size: var(--text-xs);
  font-weight: 800;
  cursor: pointer;
}

.layout-switcher button.active,
.view-tabs button.active {
  color: #fff;
  background: var(--accent-primary);
}

.layout-data-a {
  display: grid;
  grid-template-columns: 220px minmax(0, 1fr) 270px;
  min-height: 720px;
  border: 1px solid var(--border-default);
  background: var(--border-default);
  gap: 1px;
}

.stock-list-pane,
.data-viewport,
.sidebar-events,
.stock-brief,
.data-tile,
.panel-strip {
  min-width: 0;
  background: var(--bg-primary);
}

.stock-list-pane,
.sidebar-events {
  display: flex;
  min-height: 0;
  flex-direction: column;
  gap: 12px;
  padding: 14px;
}

.pane-head,
.viewport-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.pane-head span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 800;
}

.pane-head strong {
  color: var(--accent-primary);
  font-family: var(--font-data);
}

.stock-list,
.event-list {
  display: flex;
  min-height: 0;
  flex-direction: column;
  gap: 7px;
  overflow: auto;
}

.stock-row {
  display: grid;
  gap: 3px;
  width: 100%;
  padding: 10px;
  border: 1px solid transparent;
  border-radius: 7px;
  background: transparent;
  color: var(--text-primary);
  cursor: pointer;
  text-align: left;
}

.stock-row:hover,
.stock-row.active {
  border-color: var(--border-accent);
  background: var(--pine-bg-light, #eef3f0);
}

.stock-row span,
.stock-row small,
.event-row span,
.event-row small,
.audit-sheet td span,
.compact-log-row span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.stock-row strong,
.event-row strong,
.compact-log-row strong {
  color: var(--text-bright);
  font-size: var(--text-sm);
}

.data-viewport {
  display: flex;
  min-height: 0;
  flex-direction: column;
  padding: 14px;
}

.tab-panel {
  flex: 1;
  min-height: 0;
  margin-top: 12px;
  overflow: auto;
}

.event-row {
  display: grid;
  gap: 4px;
  padding: 9px 10px;
  border-left: 3px solid var(--border-default);
  background: var(--bg-elevated);
}

.event-row--good { border-left-color: #2d6a4f; }
.event-row--warn { border-left-color: #b27a1e; }
.event-row--bad { border-left-color: #a83232; }
.event-row--neutral { border-left-color: #5c6863; }

.layout-data-b {
  display: grid;
  gap: 12px;
}

.sheet-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 12px;
  border: 1px solid var(--border-default);
  background: var(--bg-elevated);
}

.sheet-filters {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.sheet-filters span {
  padding: 5px 8px;
  border: 1px solid var(--border-default);
  border-radius: 999px;
  color: var(--text-secondary);
  background: var(--bg-primary);
  font-size: var(--text-xs);
}

.audit-sheet {
  overflow: auto;
  border: 1px solid var(--border-default);
  background: var(--bg-primary);
}

.audit-sheet table {
  width: 100%;
  border-collapse: collapse;
  min-width: 920px;
}

.audit-sheet th,
.audit-sheet td {
  padding: 11px 12px;
  border-bottom: 1px solid var(--border-subtle);
  text-align: left;
  vertical-align: top;
}

.audit-sheet th {
  position: sticky;
  top: 0;
  color: var(--text-muted);
  background: var(--bg-elevated);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  z-index: 1;
}

.audit-sheet td {
  color: var(--text-primary);
  font-size: var(--text-sm);
}

.audit-sheet td:first-child {
  display: grid;
  gap: 3px;
}

.mono {
  font-family: var(--font-data);
}

.status-pill {
  display: inline-flex;
  min-width: 64px;
  justify-content: center;
  border-radius: 999px;
  padding: 3px 8px;
  font-size: var(--text-xs);
}

.status-pill--good { color: #2d6a4f; background: #eaf5f0; }
.status-pill--warn { color: #b27a1e; background: #fdf6e6; }
.status-pill--bad { color: #a83232; background: #fbf1f1; }
.status-pill--neutral { color: #5c6863; background: #f2f2ef; }

.layout-data-c {
  display: grid;
  grid-template-columns: minmax(320px, 0.85fr) minmax(0, 1.15fr);
  gap: 12px;
}

.stock-brief,
.data-tile,
.panel-strip {
  border: 1px solid var(--border-default);
  border-radius: 8px;
  padding: 16px;
}

.stock-brief {
  display: grid;
  align-content: start;
  gap: 12px;
}

.stock-brief > strong {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: var(--text-2xl);
}

.brief-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.brief-grid div {
  display: grid;
  gap: 4px;
  padding: 10px;
  border: 1px solid var(--border-subtle);
  background: var(--bg-elevated);
}

.brief-grid span {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.brief-grid strong {
  overflow: hidden;
  color: var(--text-bright);
  font-family: var(--font-data);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tile-board {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.data-tile {
  display: grid;
  min-height: 168px;
  align-content: space-between;
  gap: 8px;
}

.data-tile span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 800;
}

.data-tile strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: var(--text-xl);
}

.data-tile--good { border-color: rgba(45, 106, 79, 0.34); }
.data-tile--warn { border-color: rgba(178, 122, 30, 0.38); }
.data-tile--bad { border-color: rgba(168, 50, 50, 0.36); }

.panel-strip {
  grid-column: 1 / -1;
}

.compact-log-grid {
  display: grid;
  gap: 7px;
  margin-top: 10px;
}

.compact-log-row {
  display: grid;
  grid-template-columns: 150px minmax(0, 1fr) auto;
  gap: 10px;
  align-items: center;
  padding: 8px 10px;
  border: 1px solid var(--border-subtle);
  background: var(--bg-elevated);
}

.status-completed { color: var(--status-ready); }
.status-failed,
.status-cancelled { color: var(--status-attention); }
.status-running,
.status-queued { color: var(--accent-primary); }

:deep(.stock-list-container),
:deep(.kline-query),
:deep(.sentiment-panel) {
  min-width: 0;
}

@media (max-width: 1180px) {
  .layout-data-a,
  .layout-data-c {
    grid-template-columns: 1fr;
  }

  .layout-data-a {
    grid-auto-rows: auto;
    min-height: auto;
  }

  .stock-list-pane,
  .sidebar-events {
    max-height: none;
  }

  .layout-data-a .data-viewport {
    min-height: clamp(560px, 72vh, 720px);
  }

  .layout-data-a .tab-panel {
    min-height: 420px;
  }
}

@media (max-width: 760px) {
  .data-command-bar,
  .viewport-head,
  .sheet-toolbar,
  .compact-log-row,
  .tile-board,
  .brief-grid {
    grid-template-columns: 1fr;
  }

  .data-command-bar__actions,
  .layout-switcher,
  .view-tabs {
    justify-content: flex-start;
    width: 100%;
    overflow-x: auto;
  }

  .layout-switcher button,
  .view-tabs button {
    white-space: nowrap;
  }
}
</style>

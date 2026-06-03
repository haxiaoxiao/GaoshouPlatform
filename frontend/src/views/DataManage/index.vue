<template>
  <div class="page-frame data-center-page">
    <header class="panel-card data-hero">
      <div class="data-hero__copy">
        <span class="section-kicker">DATA VIEW / FRESHNESS FIRST</span>
        <h2>数据查看</h2>
        <p>
          首屏只看“最新口径”和数据链路状态：行情、基础信息、财务、因子指标、概念与舆情各自展示最近可用日期。
        </p>
      </div>
      <div class="data-hero__actions">
        <el-button :icon="Refresh" :loading="loading" @click="loadDashboard">刷新口径</el-button>
        <el-button type="primary" @click="goSync">去数据同步</el-button>
      </div>
    </header>

    <section class="freshness-grid">
      <article
        v-for="item in freshnessCards"
        :key="item.key"
        class="freshness-card"
        :class="`freshness-card--${item.tone}`"
      >
        <div class="freshness-card__head">
          <span>{{ item.source }}</span>
          <b>{{ item.status }}</b>
        </div>
        <strong>{{ item.title }}</strong>
        <div class="freshness-card__date">{{ item.value }}</div>
        <p>{{ item.detail }}</p>
      </article>
    </section>

    <section class="page-grid page-grid--two data-summary-grid">
      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">COVERAGE CONTRACT</span>
            <h3>核心数据口径</h3>
          </div>
        </div>
        <div class="coverage-list">
          <div v-for="row in coverageRows" :key="row.label" class="coverage-row">
            <span>{{ row.label }}</span>
            <strong>{{ row.value }}</strong>
            <small>{{ row.hint }}</small>
          </div>
        </div>
      </article>

      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RECENT PIPELINE</span>
            <h3>最近同步事件</h3>
          </div>
          <el-button size="small" text @click="goSync">管理队列</el-button>
        </div>
        <div v-if="syncLogs.length" class="sync-log-list">
          <div v-for="log in syncLogs.slice(0, 5)" :key="log.id" class="sync-log-row">
            <div>
              <strong>{{ syncTypeLabel(log.sync_type) }}</strong>
              <span>{{ formatDateTime(log.end_time || log.start_time || log.created_at) }}</span>
            </div>
            <b :class="`status-${log.status}`">{{ syncStatusLabel(log.status) }}</b>
          </div>
        </div>
        <p v-else class="empty-copy">暂无同步记录；如需补齐数据，请进入独立的数据同步页。</p>
      </article>
    </section>

    <section class="panel-card data-view-panel">
      <div class="panel-card__head data-view-panel__head">
        <div>
          <span class="section-kicker">DATA EXPLORATION</span>
          <h3>数据查看工作台</h3>
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
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh } from '@element-plus/icons-vue'
import StockList from './StockList.vue'
import KlineQuery from './KlineQuery.vue'
import SentimentPanel from './SentimentPanel.vue'
import { systemApi, type DataSummary, type DataSummaryItem } from '@/api/system'
import { syncApi, type SyncLog } from '@/api/sync'

type FreshnessTone = 'good' | 'warn' | 'bad' | 'neutral'
type TabKey = 'stocks' | 'quotes' | 'sentiment'

interface FreshnessCard {
  key: string
  title: string
  source: string
  value: string
  detail: string
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
const mountedTabs = ref<Record<TabKey, boolean>>({
  stocks: false,
  quotes: true,
  sentiment: false,
})

const tabs: { key: TabKey; label: string }[] = [
  { key: 'quotes', label: '行情查询' },
  { key: 'stocks', label: '股票基础数据' },
  { key: 'sentiment', label: '新闻舆情' },
]

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})

const freshnessCards = computed<FreshnessCard[]>(() => [
  buildSummaryCard('market_daily', '日线行情', '用于因子、回测、指数基准；关注 trade_date 最新口径，而不是容量。'),
  buildSummaryCard('market_minute', '分钟行情', '用于日内策略与 minute_timer 抽样；关注 datetime 最新可用分钟。'),
  buildSummaryCard('stocks', '股票基础数据', '代码、名称、行业、ST、股本、市值等基础字段。'),
  buildSummaryCard('financial', '财务报表', '季度报表与最新财务摘要；因子预计算依赖这里的 point-in-time 口径。'),
  buildSummaryCard('factor_values', '因子缓存', '因子定义页负责预计算；评估页只消费已经落盘的缓存。'),
  buildSummaryCard('stock_indicators', '指标缓存', 'Indicator 体系预计算结果，供筛选、评估和策略复用。'),
  buildSummaryCard('concept_membership', '概念与行业扩展', '同花顺概念成员、主题分类和股票池扩展信号。'),
  buildSummaryCard('sentiment', '新闻舆情数据', '雪球/NGA 等样本用于研究假设验证，不默认参与回测。'),
])

const coverageRows = computed<CoverageRow[]>(() => [
  summaryRow('行情主口径', 'market_daily', '日线 trade_date 最新可用日'),
  summaryRow('日内口径', 'market_minute', '分钟线 datetime 最新可用分钟'),
  summaryRow('timer 抽样', 'minute_timer', 'minute_timer 回测优先读取'),
  summaryRow('基础股票', 'stocks', '股票元数据 updated_at 最新口径'),
  summaryRow('财务报表', 'financial', 'report_date 最新披露期'),
  summaryRow('舆情样本', 'sentiment', '新闻舆情 published_at 最新样本'),
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
  const rowText = item ? formatRowCount(item.row_count, item.row_count_estimated) : '后端暂无来源'
  return {
    key,
    title,
    source: item?.source || key,
    value: latestValue(item),
    detail: `${detail} · ${rowText}`,
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

onMounted(loadDashboard)

watch(activeTab, (tab) => {
  mountedTabs.value[tab] = true
})
</script>

<style scoped>
.data-center-page {
  overflow: auto;
}

.data-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: var(--space-4);
  padding: var(--space-5);
}

.data-hero__copy {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: var(--space-2);
}

.data-hero h2,
.panel-card__head h3 {
  margin: 0;
}

.data-hero p {
  max-width: 760px;
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.data-hero__actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: var(--space-2);
}

.freshness-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: var(--space-3);
}

.freshness-card {
  display: flex;
  min-height: 154px;
  flex-direction: column;
  gap: var(--space-2);
  padding: var(--space-4);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)),
    var(--bg-elevated);
  box-shadow: var(--shadow-card);
}

.freshness-card__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-2);
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.freshness-card__head b {
  padding: 3px 7px;
  border-radius: var(--radius-full);
  background: rgba(148, 163, 184, 0.1);
  color: var(--text-secondary);
  font-weight: 600;
}

.freshness-card > strong {
  color: var(--text-primary);
  font-size: var(--text-base);
}

.freshness-card__date {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 22px;
  font-weight: 700;
  letter-spacing: -0.02em;
}

.freshness-card p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.55;
}

.freshness-card--good {
  border-color: rgba(34, 197, 94, 0.32);
}

.freshness-card--good .freshness-card__head b {
  color: var(--color-bull);
  background: rgba(34, 197, 94, 0.12);
}

.freshness-card--warn {
  border-color: rgba(245, 158, 11, 0.38);
}

.freshness-card--warn .freshness-card__head b {
  color: var(--color-warning);
  background: rgba(245, 158, 11, 0.12);
}

.freshness-card--bad {
  border-color: rgba(239, 68, 68, 0.38);
}

.freshness-card--bad .freshness-card__head b {
  color: var(--color-bear);
  background: rgba(239, 68, 68, 0.12);
}

.data-summary-grid {
  align-items: stretch;
}

.coverage-list,
.sync-log-list {
  display: flex;
  flex-direction: column;
  gap: var(--space-2);
}

.coverage-row,
.sync-log-row {
  display: grid;
  grid-template-columns: 130px minmax(0, 1fr) minmax(180px, 0.9fr);
  align-items: center;
  gap: var(--space-3);
  padding: 10px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(10, 14, 20, 0.58);
}

.coverage-row span,
.coverage-row small,
.sync-log-row span {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.coverage-row strong,
.sync-log-row strong {
  color: var(--text-primary);
  font-family: var(--font-data);
  font-size: var(--text-sm);
}

.sync-log-row {
  grid-template-columns: minmax(0, 1fr) auto;
}

.sync-log-row > div {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 3px;
}

.sync-log-row b {
  font-size: var(--text-xs);
}

.status-completed {
  color: var(--color-bull);
}

.status-failed,
.status-cancelled {
  color: var(--color-bear);
}

.status-running,
.status-queued {
  color: var(--accent-primary);
}

.data-view-panel {
  min-height: 680px;
  overflow: hidden;
}

.data-view-panel__head {
  align-items: center;
}

.view-tabs {
  display: inline-flex;
  gap: 2px;
  padding: 3px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-full);
  background: rgba(10, 14, 20, 0.65);
}

.view-tabs button {
  border: 0;
  border-radius: var(--radius-full);
  padding: 7px 12px;
  color: var(--text-secondary);
  background: transparent;
  font-size: var(--text-xs);
  font-weight: 700;
  cursor: pointer;
}

.view-tabs button.active {
  color: var(--text-bright);
  background: rgba(56, 189, 248, 0.18);
}

.tab-panel {
  min-height: 0;
  overflow: auto;
  padding: var(--space-4);
}

:deep(.stock-list-container),
:deep(.kline-query),
:deep(.sentiment-panel) {
  min-width: 0;
}

@media (max-width: 1280px) {
  .freshness-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .coverage-row {
    grid-template-columns: 110px minmax(0, 1fr);
  }

  .coverage-row small {
    grid-column: 2;
  }
}

@media (max-width: 760px) {
  .data-hero,
  .data-view-panel__head {
    grid-template-columns: 1fr;
  }

  .data-hero__actions {
    justify-content: flex-start;
  }

  .freshness-grid,
  .data-summary-grid {
    grid-template-columns: 1fr;
  }

  .coverage-row {
    grid-template-columns: 1fr;
  }

  .coverage-row small {
    grid-column: auto;
  }

  .view-tabs {
    width: 100%;
    overflow-x: auto;
  }

  .view-tabs button {
    white-space: nowrap;
  }
}
</style>

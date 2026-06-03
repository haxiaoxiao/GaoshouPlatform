<template>
  <div class="page-frame home-workbench">
    <header class="panel-card workbench-hero">
      <div>
        <span class="section-kicker">PLATFORM COCKPIT</span>
        <h2>平台工作台</h2>
        <p>一屏查看系统健康、数据新鲜度、同步任务、因子/回测链路和交易护栏；只展示代表性状态，不把各模块操作塞进首页。</p>
      </div>
      <div class="hero-actions">
        <el-button :icon="Refresh" :loading="loading" @click="loadWorkbench">刷新状态</el-button>
        <el-button type="primary" @click="router.push('/data')">查看数据口径</el-button>
      </div>
    </header>

    <section class="status-grid">
      <article v-for="card in statusCards" :key="card.key" class="status-card" :class="`status-card--${card.tone}`">
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
        <small>{{ card.hint }}</small>
      </article>
    </section>

    <section class="workbench-grid">
      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">DATA FRESHNESS</span>
            <h3>关键数据最新口径</h3>
          </div>
          <el-button text size="small" @click="router.push('/data')">进入数据查看</el-button>
        </div>
        <div class="freshness-list">
          <div v-for="row in dataFreshnessRows" :key="row.label" class="freshness-row">
            <span>{{ row.label }}</span>
            <strong>{{ row.value }}</strong>
            <small>{{ row.hint }}</small>
          </div>
        </div>
      </article>

      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RECENT ACTIVITY</span>
            <h3>最近任务</h3>
          </div>
          <el-button text size="small" @click="router.push('/monitor')">系统运维</el-button>
        </div>
        <div v-if="activityRows.length" class="activity-list">
          <div v-for="row in activityRows" :key="row.key" class="activity-row">
            <div>
              <strong>{{ row.title }}</strong>
              <span>{{ row.subtitle }}</span>
            </div>
            <b :class="`tone-${row.tone}`">{{ row.status }}</b>
          </div>
        </div>
        <p v-else class="empty-copy">暂无近期任务记录。</p>
      </article>
    </section>

    <section class="panel-card pipeline-panel">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">RESEARCH PIPELINE</span>
          <h3>投研流水线入口</h3>
        </div>
      </div>
      <div class="pipeline-grid">
        <button v-for="stage in pipelineStages" :key="stage.path" type="button" class="pipeline-card" @click="router.push(stage.path)">
          <span>{{ stage.kicker }}</span>
          <strong>{{ stage.title }}</strong>
          <p>{{ stage.description }}</p>
        </button>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh } from '@element-plus/icons-vue'
import { usePageContext } from '@/app/pageContext'
import { systemApi, type DataSummary, type DataSummaryItem, type SystemStatus } from '@/api/system'
import { syncApi, type SyncLog, type SyncStatus } from '@/api/sync'
import { runtimeTaskApi, type RuntimeTask } from '@/api/runtimeTasks'
import { backtestApi, type Backtest } from '@/api/backtest'
import { gridTradingApi, type GridStatus } from '@/api/gridTrading'

type Tone = 'good' | 'warn' | 'bad' | 'neutral'

interface StatusCard {
  key: string
  label: string
  value: string
  hint: string
  tone: Tone
}

interface ActivityRow {
  key: string
  title: string
  subtitle: string
  status: string
  tone: Tone
}

const router = useRouter()
const loading = ref(false)
const systemStatus = ref<SystemStatus | null>(null)
const dataSummary = ref<DataSummary | null>(null)
const syncStatus = ref<SyncStatus | null>(null)
const syncLogs = ref<SyncLog[]>([])
const runtimeTasks = ref<RuntimeTask[]>([])
const latestBacktests = ref<Backtest[]>([])
const gridStatus = ref<GridStatus | null>(null)

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const activeTasks = computed(() => runtimeTasks.value.filter(task => ['queued', 'running'].includes(String(task.status))))

const statusCards = computed<StatusCard[]>(() => [
  {
    key: 'backend',
    label: '后端 API',
    value: systemStatus.value?.status || '未知',
    hint: String(systemStatus.value?.database || '等待状态回传'),
    tone: systemStatus.value?.status === 'healthy' || systemStatus.value?.status === 'ok' ? 'good' : 'neutral',
  },
  {
    key: 'storage',
    label: '行情存储',
    value: String(systemStatus.value?.market_data_backend || 'parquet'),
    hint: latestFor('klines_daily') ? `日线至 ${formatDateTime(latestFor('klines_daily'))}` : '等待数据口径',
    tone: latestFor('klines_daily') ? 'good' : 'warn',
  },
  {
    key: 'sync',
    label: '同步服务',
    value: syncStatus.value ? syncStatusLabel(syncStatus.value.status) : '未知',
    hint: syncStatus.value?.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : '无运行任务',
    tone: syncStatus.value?.status === 'failed' ? 'bad' : syncStatus.value?.status === 'running' ? 'warn' : 'good',
  },
  {
    key: 'tasks',
    label: '运行任务',
    value: `${activeTasks.value.length} 个活动`,
    hint: `${runtimeTasks.value.length} 个任务记录`,
    tone: activeTasks.value.length ? 'warn' : 'good',
  },
  {
    key: 'backtest',
    label: '最近回测',
    value: latestBacktests.value[0] ? backtestStatusLabel(latestBacktests.value[0].status) : '暂无',
    hint: latestBacktests.value[0] ? `${latestBacktests.value[0].start_date} ~ ${latestBacktests.value[0].end_date}` : '等待策略运行',
    tone: latestBacktests.value[0]?.status === 'failed' ? 'bad' : latestBacktests.value[0] ? 'good' : 'neutral',
  },
  {
    key: 'trade',
    label: '交易护栏',
    value: gridStatus.value?.order_submit_enabled ? '真实下单开启' : '仅信号',
    hint: gridStatus.value?.account_id || 'QMT 可选外部依赖',
    tone: gridStatus.value?.order_submit_enabled ? 'bad' : 'good',
  },
])

const dataFreshnessRows = computed(() => [
  { label: '日线行情', value: formatDateTime(latestFor('klines_daily')), hint: 'klines_daily.trade_date' },
  { label: '分钟行情', value: formatDateTime(latestFor('klines_minute')), hint: 'klines_minute.datetime' },
  { label: '定时分钟', value: formatDateTime(latestFor('klines_minute_timer')), hint: 'minute_timer 回测优先' },
  { label: '因子缓存', value: formatDateTime(latestFor('factor_values') || latestFor('factor_cache')), hint: '评估页只消费缓存' },
  { label: '指标缓存', value: formatDateTime(latestFor('stock_indicators')), hint: 'Indicator 体系预计算' },
  { label: '概念成员', value: formatDateTime(latestFor('ths_member')), hint: '主题/行业扩展' },
])

const activityRows = computed<ActivityRow[]>(() => {
  const rows: ActivityRow[] = []
  activeTasks.value.slice(0, 3).forEach(task => {
    rows.push({
      key: `task-${task.task_id}`,
      title: task.title || task.kind,
      subtitle: `进度 ${Math.round((task.progress || 0) * 100)}%`,
      status: String(task.status),
      tone: task.status === 'failed' ? 'bad' : 'warn',
    })
  })
  syncLogs.value.slice(0, 3).forEach(log => {
    rows.push({
      key: `sync-${log.id}`,
      title: syncTypeLabel(log.sync_type),
      subtitle: formatDateTime(log.end_time || log.start_time || log.created_at),
      status: syncStatusLabel(log.status),
      tone: log.status === 'failed' ? 'bad' : 'good',
    })
  })
  latestBacktests.value.slice(0, 2).forEach(item => {
    rows.push({
      key: `bt-${item.id}`,
      title: `回测 #${item.id}`,
      subtitle: `${item.start_date} ~ ${item.end_date}`,
      status: backtestStatusLabel(item.status),
      tone: item.status === 'failed' ? 'bad' : item.status === 'running' ? 'warn' : 'good',
    })
  })
  return rows.slice(0, 6)
})

const pipelineStages = [
  { kicker: '01 DATA', title: '数据查看', path: '/data', description: '确认日线、分钟线、基础信息、舆情和指标的最新口径。' },
  { kicker: '02 SYNC', title: '数据同步', path: '/data/sync', description: '按队列补齐缺口，显式处理 QMT、Relay 和失败策略。' },
  { kicker: '03 ALPHA', title: '因子定义', path: '/factor', description: '管理因子目录、覆盖率、参数版本和预计算。' },
  { kicker: '04 EVAL', title: '因子评估', path: '/factor/evaluation', description: '检查 IC、多空收益、回撤、换手和已计算组合。' },
  { kicker: '05 RUN', title: '策略回测', path: '/backtest', description: '编辑策略代码，配置股票池、引擎、基准并查看报告。' },
  { kicker: '06 GUARD', title: '模拟 / 实盘', path: '/trade', description: '先看信号和账户护栏，再决定是否开启真实下单。' },
]

async function loadWorkbench() {
  loading.value = true
  try {
    const [systemResult, summaryResult, syncStatusResult, logsResult, tasksResult, backtestsResult, gridResult] = await Promise.allSettled([
      systemApi.getStatus(),
      systemApi.dataSummary(),
      syncApi.getStatus(),
      syncApi.getLogs({ limit: 8 }),
      runtimeTaskApi.list(true),
      backtestApi.list({ page: 1, page_size: 5 }),
      gridTradingApi.status(),
    ])
    if (systemResult.status === 'fulfilled') systemStatus.value = systemResult.value
    if (summaryResult.status === 'fulfilled') dataSummary.value = summaryResult.value
    if (syncStatusResult.status === 'fulfilled') syncStatus.value = syncStatusResult.value
    if (logsResult.status === 'fulfilled') syncLogs.value = logsResult.value
    if (tasksResult.status === 'fulfilled') runtimeTasks.value = tasksResult.value
    if (backtestsResult.status === 'fulfilled') latestBacktests.value = backtestsResult.value.items
    if (gridResult.status === 'fulfilled') gridStatus.value = gridResult.value
  } finally {
    loading.value = false
  }
}

function latestFor(tableName: string): string {
  const aliases: Record<string, string> = {
    klines_daily: 'market_daily',
    klines_minute: 'market_minute',
    klines_minute_timer: 'minute_timer',
    factor_cache: 'factor_values',
    ths_member: 'concept_membership',
  }
  const item = summaryMap.value[aliases[tableName] || tableName]
  return item?.latest_datetime || item?.latest_date || ''
}

function formatDateTime(value?: string | null): string {
  if (!value) return '-'
  return value.replace('T', ' ').slice(0, value.includes(':') ? 16 : 10)
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
    factor_dependency: '因子依赖',
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

function backtestStatusLabel(status: string): string {
  const labels: Record<string, string> = {
    pending: '待运行',
    running: '运行中',
    completed: '完成',
    failed: '失败',
  }
  return labels[status] || status
}

const pageContextBlocks = computed(() => [
  {
    title: 'Workbench',
    rows: [
      { label: '刷新状态', value: loading.value ? '刷新中' : '已就绪', tone: loading.value ? 'warn' : 'good' },
      { label: '活动任务', value: `${activeTasks.value.length} 个`, tone: activeTasks.value.length ? 'warn' : 'good' },
      {
        label: '同步服务',
        value: syncStatus.value ? syncStatusLabel(syncStatus.value.status) : '-',
        tone: syncStatus.value?.status === 'failed' ? 'bad' : syncStatus.value?.status === 'running' ? 'warn' : 'good',
      },
      {
        label: '最近回测',
        value: latestBacktests.value[0] ? backtestStatusLabel(latestBacktests.value[0].status) : '暂无',
        tone: latestBacktests.value[0]?.status === 'failed' ? 'bad' : latestBacktests.value[0] ? 'good' : 'neutral',
      },
    ],
  },
  {
    title: 'Freshness',
    rows: dataFreshnessRows.value.slice(0, 4).map(row => ({
      label: row.label,
      value: row.value,
      tone: row.value === '-' ? 'warn' : 'good',
    })),
  },
])

usePageContext(pageContextBlocks)

onMounted(loadWorkbench)
</script>

<style scoped>
.home-workbench {
  overflow: auto;
}

.workbench-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: var(--space-4);
  align-items: center;
  padding: var(--space-5);
}

.workbench-hero h2 {
  margin: var(--space-1) 0 var(--space-2);
  font-size: 24px;
}

.workbench-hero p {
  margin: 0;
  max-width: 820px;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.hero-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: var(--space-2);
}

.status-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: var(--space-3);
}

.status-card {
  display: flex;
  min-height: 116px;
  flex-direction: column;
  justify-content: space-between;
  gap: var(--space-2);
  padding: var(--space-4);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)), var(--bg-elevated);
  box-shadow: var(--shadow-card);
}

.status-card span,
.status-card small {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.status-card strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 18px;
}

.status-card--good {
  border-color: rgba(34, 197, 94, 0.28);
}

.status-card--warn {
  border-color: rgba(245, 158, 11, 0.34);
}

.status-card--bad {
  border-color: rgba(239, 68, 68, 0.34);
}

.workbench-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.15fr) minmax(360px, 0.85fr);
  gap: var(--space-4);
}

.freshness-list,
.activity-list {
  display: flex;
  flex-direction: column;
  gap: var(--space-2);
  padding: var(--space-4);
}

.freshness-row,
.activity-row {
  display: grid;
  align-items: center;
  gap: var(--space-3);
  padding: 10px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(10, 14, 20, 0.58);
}

.freshness-row {
  grid-template-columns: 120px minmax(0, 1fr) 180px;
}

.activity-row {
  grid-template-columns: minmax(0, 1fr) auto;
}

.activity-row div {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 3px;
}

.freshness-row span,
.freshness-row small,
.activity-row span {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.freshness-row strong,
.activity-row strong {
  color: var(--text-primary);
  font-family: var(--font-data);
}

.tone-good {
  color: var(--status-ready);
}

.tone-warn {
  color: var(--color-warning);
}

.tone-bad {
  color: var(--status-attention);
}

.tone-neutral {
  color: var(--text-secondary);
}

.pipeline-panel {
  padding-bottom: var(--space-4);
}

.pipeline-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: var(--space-3);
  padding: 0 var(--space-4);
}

.pipeline-card {
  min-height: 160px;
  padding: var(--space-4);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  color: var(--text-primary);
  text-align: left;
  background: rgba(10, 14, 20, 0.58);
  cursor: pointer;
}

.pipeline-card:hover {
  border-color: rgba(56, 189, 248, 0.42);
  background: rgba(56, 189, 248, 0.08);
}

.pipeline-card span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.pipeline-card strong {
  display: block;
  margin: var(--space-2) 0;
  color: var(--text-bright);
  font-size: var(--text-base);
}

.pipeline-card p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.55;
}

@media (max-width: 1400px) {
  .status-grid,
  .pipeline-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 980px) {
  .workbench-hero,
  .workbench-grid {
    grid-template-columns: 1fr;
  }

  .hero-actions {
    justify-content: flex-start;
  }
}

@media (max-width: 680px) {
  .status-grid,
  .pipeline-grid,
  .freshness-row {
    grid-template-columns: 1fr;
  }
}
</style>

<template>
  <div class="home-workbench">
    <header class="workbench-header">
      <div class="header-copy">
        <span class="section-kicker">INVESTMENT DECISION DESK</span>
        <div class="title-row">
          <h2>今日投研工作台</h2>
          <span class="decision-pill" :class="`pill--${decisionTone}`">
            <span class="status-dot"></span>
            {{ decisionSummary }}
          </span>
        </div>
      </div>

      <div class="header-actions">
        <el-button size="small" :icon="Refresh" :loading="loading" @click="loadWorkbench">刷新判断</el-button>
        <el-button size="small" type="primary" class="btn-pine" @click="router.push(primaryAction.path)">
          {{ primaryAction.label }}
        </el-button>
      </div>
    </header>

    <section class="decision-grid">
      <article class="readiness-card" :class="`tone-${decisionTone}`">
        <div>
          <span class="section-kicker">READINESS</span>
          <strong>{{ readinessScore }}</strong>
        </div>
        <p>{{ decisionSummary }}</p>
      </article>

      <article v-for="card in focusCards" :key="card.key" class="focus-card" :class="`tone-${card.tone}`">
        <span class="status-dot"></span>
        <div>
          <small>{{ card.label }}</small>
          <strong>{{ card.value }}</strong>
          <em>{{ card.hint }}</em>
        </div>
      </article>
    </section>

    <section class="workbench-grid">
      <aside class="pipeline-panel panel-block">
        <div class="panel-heading">
          <span class="section-kicker">PIPELINE</span>
          <strong>Quant Trading Pipeline</strong>
        </div>
        <nav class="pipeline-nav">
          <button v-for="stage in pipelineStages" :key="stage.path" class="pipeline-step" type="button" @click="router.push(stage.path)">
            <span class="step-index">{{ stage.kicker.split(' ')[0] }}</span>
            <span class="step-copy">
              <strong>{{ stage.title }}</strong>
              <small>{{ stage.description }}</small>
            </span>
          </button>
        </nav>
      </aside>

      <main class="primary-panel">
        <section class="panel-block action-panel">
          <div class="panel-heading">
            <div>
              <span class="section-kicker">TODAY'S CALLS</span>
              <strong>今日行动建议</strong>
            </div>
            <button class="text-action" type="button" @click="router.push('/research')">进入研究实验室</button>
          </div>

          <div class="table-wrap">
            <table class="quant-table">
              <thead>
                <tr>
                  <th>级别</th>
                  <th>模块</th>
                  <th>建议动作</th>
                  <th class="text-right">入口</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="action in actionRows"
                  :key="action.key"
                  class="clickable-row"
                  @click="router.push(action.path)"
                >
                  <td><span class="tone-chip" :class="`tone-chip--${action.tone}`">{{ action.kicker }}</span></td>
                  <td class="text-strong">{{ action.key.toUpperCase() }}</td>
                  <td>
                    <div class="task-copy">
                      <strong>{{ action.title }}</strong>
                      <span>{{ action.description }}</span>
                    </div>
                  </td>
                  <td class="text-right action-link">{{ action.cta }} -></td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
      </main>

      <aside class="telemetry-panel">
        <section class="panel-block">
          <div class="panel-heading">
            <span class="section-kicker">RESEARCH INPUTS</span>
            <button class="text-action" type="button" @click="router.push('/data')">查看详情</button>
          </div>
          <div class="status-stack">
            <div v-for="row in researchInputRows" :key="row.label" class="status-row" :class="`tone-${row.tone}`">
              <span class="status-dot"></span>
              <span class="status-name">{{ row.label }}</span>
              <strong>{{ row.value }}</strong>
            </div>
          </div>
        </section>

        <section class="panel-block">
          <div class="panel-heading">
            <span class="section-kicker">HANDOFF TAPE</span>
            <button class="text-action" type="button" @click="router.push('/monitor')">运维排障</button>
          </div>
          <div v-if="handoffRows.length" class="event-stack">
            <button v-for="row in handoffRows" :key="row.key" class="event-row" type="button">
              <span class="event-meta">
                <span>{{ row.subtitle }}</span>
                <em :class="`badge--${row.tone}`">{{ row.status }}</em>
              </span>
              <strong>{{ row.title }}</strong>
            </button>
          </div>
          <p v-else class="empty-copy">暂无影响投研决策的近期事件。</p>
        </section>
      </aside>
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
import { liveTradingApi, type LiveTradingStatus } from '@/api/liveTrading'

type Tone = 'good' | 'warn' | 'bad' | 'neutral'

interface FocusCard {
  key: string
  label: string
  value: string
  hint: string
  tone: Tone
}

interface ActionRow {
  key: string
  kicker: string
  title: string
  description: string
  cta: string
  path: string
  tone: Tone
}

interface HandoffRow {
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
const liveStatus = ref<LiveTradingStatus | null>(null)

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const activeTasks = computed(() => runtimeTasks.value.filter(task => ['queued', 'running'].includes(String(task.status))))
const latestBacktest = computed(() => latestBacktests.value[0] || null)
const dataReadyCount = computed(() => researchInputRows.value.filter(row => row.tone === 'good').length)
const hasBlockingSync = computed(() => ['queued', 'running'].includes(syncStatus.value?.status || ''))
const tradeRiskOpen = computed(() => liveStatus.value?.order_submit_enabled === true)

const readinessScore = computed(() => {
  let score = 40
  score += dataReadyCount.value * 8
  if (systemStatus.value?.status === 'healthy' || systemStatus.value?.status === 'ok') score += 10
  if (!hasBlockingSync.value) score += 8
  if (latestBacktest.value?.status === 'completed') score += 8
  if (tradeRiskOpen.value) score -= 18
  if (latestBacktest.value?.status === 'failed') score -= 10
  return `${Math.max(0, Math.min(100, score))}%`
})

const decisionTone = computed<Tone>(() => {
  const numericScore = Number(readinessScore.value.replace('%', ''))
  if (tradeRiskOpen.value || latestBacktest.value?.status === 'failed') return 'warn'
  if (numericScore >= 82) return 'good'
  if (numericScore >= 60) return 'warn'
  return 'bad'
})

const decisionSummary = computed(() => {
  if (tradeRiskOpen.value) return '真实下单已开启，先确认交易护栏'
  if (hasBlockingSync.value) return `同步任务正在${syncStatus.value?.status === 'queued' ? '排队' : '运行'}，先等数据口径稳定`
  if (!latestFor('market_daily')) return '缺少日线最新口径，先补数据'
  if (!latestBacktest.value) return '数据已可看，建议先跑一轮基准回测'
  return '可以进入因子研究或策略迭代'
})

const primaryAction = computed(() => {
  const firstAction = actionRows.value[0]
  return firstAction
    ? { label: firstAction.cta, path: firstAction.path }
    : { label: '进入数据查看', path: '/data' }
})

const focusCards = computed<FocusCard[]>(() => [
  {
    key: 'data',
    label: '研究数据',
    value: `${dataReadyCount.value}/${researchInputRows.value.length} 就绪`,
    hint: latestFor('market_daily') ? `日线至 ${formatDateTime(latestFor('market_daily'))}` : '日线口径缺失',
    tone: dataReadyCount.value >= 4 ? 'good' : 'warn',
  },
  {
    key: 'sync',
    label: '同步影响',
    value: syncStatus.value ? syncStatusLabel(syncStatus.value.status) : '未知',
    hint: syncStatus.value?.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : '当前无数据写入',
    tone: syncStatus.value?.status === 'failed' ? 'bad' : hasBlockingSync.value ? 'warn' : 'good',
  },
  {
    key: 'backtest',
    label: '策略验证',
    value: latestBacktest.value ? backtestStatusLabel(latestBacktest.value.status) : '待启动',
    hint: latestBacktest.value ? `${latestBacktest.value.start_date} ~ ${latestBacktest.value.end_date}` : '建议先跑基准回测',
    tone: latestBacktest.value?.status === 'failed' ? 'bad' : latestBacktest.value ? 'good' : 'neutral',
  },
  {
    key: 'guard',
    label: '交易护栏',
    value: tradeRiskOpen.value ? '真实下单开启' : '仅信号',
    hint: liveStatus.value?.account_id || 'QMT 为可选外部依赖',
    tone: tradeRiskOpen.value ? 'bad' : 'good',
  },
])

const researchInputRows = computed(() => [
  inputRow('日线行情', 'market_daily', '股票池、因子和日频回测的最低前置'),
  inputRow('分钟行情', 'market_minute', '日内策略和固定 timer 回测输入'),
  inputRow('基础股票', 'stocks', '行业、市值、状态过滤'),
  inputRow('财务报表', 'financial', '质量/成长/估值因子输入'),
  inputRow('因子缓存', 'factor_values', '研究评估只消费缓存'),
  inputRow('新闻舆情', 'sentiment', '研究假设和事件验证输入'),
])

const actionRows = computed<ActionRow[]>(() => {
  const rows: ActionRow[] = []
  if (tradeRiskOpen.value) {
    rows.push({
      key: 'guard',
      kicker: 'RISK FIRST',
      title: '先确认真实下单护栏',
      description: '工作台发现真实下单开关处于开启状态，进入交易页复核账户、信号和二次确认。',
      cta: '检查交易',
      path: '/trade',
      tone: 'bad',
    })
  }
  if (hasBlockingSync.value) {
    rows.push({
      key: 'sync-running',
      kicker: 'DATA MUTATING',
      title: `等待 ${syncTypeLabel(syncStatus.value?.sync_type || '')} 完成`,
      description: '同步写入期间不要急着解读因子或回测结果，先看数据同步队列和失败策略。',
      cta: '查看同步',
      path: '/data/sync',
      tone: 'warn',
    })
  }
  if (!latestFor('market_daily') || dataReadyCount.value < 4) {
    rows.push({
      key: 'data-gap',
      kicker: 'INPUT GAP',
      title: '先补齐研究输入口径',
      description: '日线、基础股票、财务和因子缓存缺口会直接影响后续评估可信度。',
      cta: '看数据',
      path: '/data',
      tone: 'warn',
    })
  }
  if (!latestBacktest.value || latestBacktest.value.status === 'failed') {
    rows.push({
      key: 'backtest',
      kicker: 'VALIDATION',
      title: '跑一轮策略基准验证',
      description: '用当前数据口径跑基准策略，先看收益、回撤和执行日志是否可信。',
      cta: '去回测',
      path: '/backtest',
      tone: latestBacktest.value?.status === 'failed' ? 'bad' : 'neutral',
    })
  }
  rows.push({
    key: 'research',
    kicker: 'NEXT IDEA',
    title: '沉淀新的研究假设',
    description: '把数据缺口、回测现象和外部证据写入研究实验室，形成可复盘链路。',
    cta: '写假设',
    path: '/research',
    tone: 'good',
  })
  return rows.slice(0, 4)
})

const handoffRows = computed<HandoffRow[]>(() => {
  const rows: HandoffRow[] = []
  syncLogs.value.slice(0, 3).forEach(log => {
    rows.push({
      key: `sync-${log.id}`,
      title: `数据同步 · ${syncTypeLabel(log.sync_type)}`,
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
  activeTasks.value.slice(0, 2).forEach(task => {
    rows.push({
      key: `task-${task.task_id}`,
      title: task.title || task.kind,
      subtitle: `运行进度 ${Math.round((task.progress || 0) * 100)}%`,
      status: String(task.status),
      tone: task.status === 'failed' ? 'bad' : 'warn',
    })
  })
  return rows.slice(0, 6)
})

const pipelineStages = [
  { kicker: '01 DATA', title: '数据查看', path: '/data', description: '确认日线、分钟线、基础信息、舆情和指标的最新口径。' },
  { kicker: '02 FACTOR', title: '因子研究', path: '/factor', description: '管理因子目录、缓存预计算、IC、多空收益和研究评估。' },
  { kicker: '03 RUN', title: '策略回测', path: '/backtest', description: '编辑策略代码，配置股票池、引擎、基准并查看报告。' },
  { kicker: '04 GUARD', title: '模拟 / 实盘', path: '/trade', description: '先看信号和账户护栏，再决定是否开启真实下单。' },
]

async function loadWorkbench() {
  loading.value = true
  try {
    const [systemResult, summaryResult, syncStatusResult, logsResult, tasksResult, backtestsResult, liveTradingResult] = await Promise.allSettled([
      systemApi.getStatus(),
      systemApi.dataSummary(),
      syncApi.getStatus(),
      syncApi.getLogs({ limit: 8 }),
      runtimeTaskApi.list(true),
      backtestApi.list({ page: 1, page_size: 5 }),
      liveTradingApi.status(),
    ])
    if (systemResult.status === 'fulfilled') systemStatus.value = systemResult.value
    if (summaryResult.status === 'fulfilled') dataSummary.value = summaryResult.value
    if (syncStatusResult.status === 'fulfilled') syncStatus.value = syncStatusResult.value
    if (logsResult.status === 'fulfilled') syncLogs.value = logsResult.value
    if (tasksResult.status === 'fulfilled') runtimeTasks.value = tasksResult.value
    if (backtestsResult.status === 'fulfilled') latestBacktests.value = backtestsResult.value.items
    if (liveTradingResult.status === 'fulfilled') liveStatus.value = liveTradingResult.value
  } finally {
    loading.value = false
  }
}

function inputRow(label: string, key: string, hint: string) {
  const value = latestFor(key)
  return {
    label,
    value: formatDateTime(value),
    hint,
    tone: value ? 'good' as Tone : 'warn' as Tone,
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
    tushare_relay: 'Tushare Relay',
    ths_concept: '同花顺概念',
    sentiment_xueqiu: '雪球舆情',
    sentiment_nga: 'NGA 舆情',
  }
  return labels[type] || type || '同步任务'
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
    title: 'Decision',
    rows: [
      { label: '就绪度', value: readinessScore.value, tone: decisionTone.value },
      { label: '建议', value: primaryAction.value.label, tone: actionRows.value[0]?.tone || 'good' },
      { label: '同步影响', value: syncStatus.value ? syncStatusLabel(syncStatus.value.status) : '-', tone: hasBlockingSync.value ? 'warn' : 'good' },
      { label: '交易护栏', value: tradeRiskOpen.value ? '真实下单开启' : '仅信号', tone: tradeRiskOpen.value ? 'bad' : 'good' },
    ],
  },
  {
    title: 'Research Inputs',
    rows: researchInputRows.value.slice(0, 4).map(row => ({
      label: row.label,
      value: row.value,
      tone: row.tone,
    })),
  },
])

usePageContext(pageContextBlocks)

onMounted(loadWorkbench)
</script>

<style scoped>
.home-workbench {
  height: 100%;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: var(--space-4);
  overflow: auto;
  color: var(--text-primary);
  background:
    linear-gradient(180deg, rgba(253, 251, 247, 0.92), rgba(245, 242, 234, 0.5)),
    var(--bg-primary);
}

.workbench-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--space-4);
  padding: var(--space-4) 0 var(--space-3);
  border-bottom: 1px solid var(--border-default);
}

.header-copy {
  min-width: 0;
  display: grid;
  gap: var(--space-2);
}

.title-row {
  display: flex;
  align-items: center;
  gap: var(--space-3);
  flex-wrap: wrap;
}

.title-row h2 {
  margin: 0;
  color: var(--accent-primary);
  font-size: var(--text-2xl);
  line-height: 1.15;
}

.header-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: var(--space-2);
  flex-wrap: wrap;
}

.section-kicker {
  color: var(--accent-secondary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 800;
  letter-spacing: var(--tracking-wider);
  text-transform: uppercase;
}

.decision-pill,
.tone-chip,
.event-row em {
  display: inline-flex;
  align-items: center;
  width: fit-content;
  border-radius: var(--radius-sm);
  font-family: var(--font-data);
  font-size: 11px;
  font-style: normal;
  font-weight: 800;
  letter-spacing: 0;
}

.decision-pill {
  gap: 7px;
  max-width: min(680px, 100%);
  padding: 5px 10px;
  border: 1px solid transparent;
  font-family: var(--font-ui);
  font-size: var(--text-xs);
  font-weight: 700;
  white-space: normal;
}

.status-dot {
  width: 7px;
  height: 7px;
  flex: 0 0 auto;
  border-radius: var(--radius-full);
  background: currentColor;
}

.pill--good,
.tone-good .status-dot,
.tone-chip--good,
.badge--good {
  color: var(--accent-success);
}

.pill--warn,
.tone-warn .status-dot,
.tone-chip--warn,
.badge--warn {
  color: var(--accent-warning);
}

.pill--bad,
.tone-bad .status-dot,
.tone-chip--bad,
.badge--bad {
  color: var(--accent-danger);
}

.pill--neutral,
.tone-neutral .status-dot,
.tone-chip--neutral,
.badge--neutral {
  color: var(--color-neutral);
}

.pill--good,
.tone-chip--good,
.badge--good {
  background: var(--status-ready-bg);
  border: 1px solid rgba(45, 106, 79, 0.2);
}

.pill--warn,
.tone-chip--warn,
.badge--warn {
  background: var(--status-warning-bg);
  border: 1px solid rgba(178, 122, 30, 0.22);
}

.pill--bad,
.tone-chip--bad,
.badge--bad {
  background: var(--status-critical-bg);
  border: 1px solid rgba(168, 50, 50, 0.22);
}

.pill--neutral,
.tone-chip--neutral,
.badge--neutral {
  background: var(--status-neutral-bg);
  border: 1px solid rgba(92, 104, 99, 0.22);
}

.decision-grid {
  display: grid;
  grid-template-columns: minmax(240px, 1.2fr) repeat(4, minmax(0, 1fr));
  gap: var(--space-3);
}

.readiness-card,
.focus-card,
.panel-block {
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  background: rgba(245, 242, 234, 0.76);
  box-shadow: var(--shadow-card);
}

.readiness-card {
  min-height: 126px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  gap: var(--space-3);
  padding: var(--space-4);
  border-left: 5px solid currentColor;
}

.readiness-card strong {
  display: block;
  margin-top: var(--space-2);
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 34px;
  line-height: 1;
}

.readiness-card p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
  font-weight: 700;
}

.readiness-card.tone-good { color: var(--accent-success); }
.readiness-card.tone-warn { color: var(--accent-warning); }
.readiness-card.tone-bad { color: var(--accent-danger); }
.readiness-card.tone-neutral { color: var(--color-neutral); }

.focus-card {
  min-height: 126px;
  display: grid;
  grid-template-columns: auto minmax(0, 1fr);
  gap: var(--space-3);
  align-items: start;
  padding: var(--space-4);
}

.focus-card small,
.focus-card em {
  display: block;
  color: var(--text-muted);
  font-size: var(--text-xs);
  font-style: normal;
}

.focus-card strong {
  display: block;
  margin: var(--space-1) 0;
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: var(--text-lg);
  line-height: 1.2;
}

.workbench-grid {
  min-height: 0;
  display: grid;
  grid-template-columns: minmax(180px, 0.55fr) minmax(420px, 1.45fr) minmax(280px, 0.8fr);
  gap: var(--space-4);
  align-items: start;
}

.primary-panel,
.telemetry-panel {
  min-width: 0;
  display: grid;
  gap: var(--space-4);
}

.panel-block {
  overflow: hidden;
}

.panel-heading {
  min-height: 48px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-3);
  padding: var(--space-3) var(--space-4);
  border-bottom: 1px solid var(--border-subtle);
  background: rgba(253, 251, 247, 0.56);
}

.panel-heading strong {
  display: block;
  margin-top: 2px;
  color: var(--text-bright);
  font-size: var(--text-sm);
}

.text-action {
  border: 0;
  background: transparent;
  color: var(--accent-secondary);
  cursor: pointer;
  font-family: var(--font-ui);
  font-size: var(--text-xs);
  font-weight: 800;
  white-space: nowrap;
}

.text-action:hover {
  color: var(--accent-primary);
  text-decoration: underline;
  text-underline-offset: 3px;
}

.pipeline-nav {
  display: grid;
  gap: var(--space-2);
  padding: var(--space-3);
}

.pipeline-step {
  display: grid;
  grid-template-columns: 34px minmax(0, 1fr);
  gap: var(--space-2);
  width: 100%;
  padding: 10px;
  border: 1px solid transparent;
  border-radius: var(--radius-md);
  background: transparent;
  color: var(--text-primary);
  cursor: pointer;
  text-align: left;
}

.pipeline-step:hover {
  border-color: var(--border-default);
  background: var(--bg-hover);
}

.step-index {
  display: grid;
  place-items: center;
  width: 28px;
  height: 28px;
  border: 1px solid var(--border-accent);
  border-radius: var(--radius-sm);
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: 11px;
  font-weight: 900;
}

.step-copy {
  min-width: 0;
  display: grid;
  gap: 3px;
}

.step-copy strong {
  color: var(--text-bright);
  font-size: var(--text-sm);
}

.step-copy small {
  color: var(--text-muted);
  font-size: 11px;
  line-height: 1.45;
}

.table-wrap {
  overflow: auto;
}

.quant-table {
  width: 100%;
  min-width: 640px;
  border-collapse: collapse;
}

.quant-table th,
.quant-table td {
  border-bottom: 1px solid var(--border-subtle);
  padding: 10px 12px;
  text-align: left;
  vertical-align: middle;
}

.quant-table th {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: 11px;
  font-weight: 800;
  text-transform: uppercase;
}

.quant-table td {
  color: var(--text-primary);
  font-size: var(--text-sm);
}

.clickable-row {
  cursor: pointer;
}

.clickable-row:hover {
  background: var(--bg-hover);
}

.text-right {
  text-align: right !important;
}

.text-strong {
  color: var(--text-bright) !important;
  font-family: var(--font-data);
  font-weight: 800;
}

.task-copy {
  display: grid;
  gap: 3px;
}

.task-copy strong {
  color: var(--text-bright);
  font-size: var(--text-sm);
}

.task-copy span {
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.45;
}

.action-link {
  color: var(--accent-secondary) !important;
  font-weight: 800;
  white-space: nowrap;
}

.tone-chip,
.event-row em {
  padding: 3px 7px;
}

.status-stack,
.event-stack {
  display: grid;
  gap: var(--space-2);
  padding: var(--space-3);
}

.status-row {
  display: grid;
  grid-template-columns: auto minmax(0, 1fr) auto;
  gap: var(--space-2);
  align-items: center;
  min-height: 38px;
  padding: 8px 10px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(253, 251, 247, 0.52);
}

.status-name {
  min-width: 0;
  overflow: hidden;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 700;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.status-row strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  white-space: nowrap;
}

.event-row {
  display: grid;
  gap: var(--space-1);
  width: 100%;
  padding: 10px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(253, 251, 247, 0.52);
  cursor: default;
  text-align: left;
}

.event-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--space-2);
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: 11px;
}

.event-row strong {
  color: var(--text-bright);
  font-size: var(--text-xs);
  line-height: 1.4;
}

.empty-copy {
  margin: var(--space-3);
  border: 1px dashed var(--border-default);
  border-radius: var(--radius-md);
  color: var(--text-muted);
  padding: var(--space-4);
  text-align: center;
}

:deep(.el-button.btn-pine) {
  background: var(--accent-primary) !important;
  border-color: var(--accent-primary) !important;
  color: #fff !important;
  box-shadow: 0 8px 18px rgba(27, 61, 50, 0.14);
}

:deep(.el-button.btn-pine:hover) {
  background: var(--accent-secondary) !important;
  border-color: var(--accent-secondary) !important;
  color: #fff !important;
}

@media (max-width: 1480px) {
  .decision-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .workbench-grid {
    grid-template-columns: minmax(180px, 0.52fr) minmax(0, 1.48fr);
  }

  .telemetry-panel {
    grid-column: 1 / -1;
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 980px) {
  .workbench-header,
  .workbench-grid,
  .telemetry-panel {
    grid-template-columns: 1fr;
  }

  .decision-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .header-actions {
    justify-content: flex-start;
  }
}

@media (max-width: 640px) {
  .decision-grid {
    grid-template-columns: 1fr;
  }

  .title-row h2 {
    font-size: var(--text-xl);
  }

  .readiness-card strong {
    font-size: 30px;
  }
}
</style>

<template>
  <div class="page-frame home-workbench">
    <header class="panel-card workbench-hero">
      <div>
        <span class="section-kicker">INVESTMENT DECISION DESK</span>
        <h2>今日投研工作台</h2>
        <p>工作台只回答一个问题：今天应该先研究、补数据、跑回测，还是保持交易护栏。底层服务细节留给系统运维。</p>
      </div>
      <div class="hero-actions">
        <el-button :icon="Refresh" :loading="loading" @click="loadWorkbench">刷新判断</el-button>
        <el-button type="primary" @click="router.push(primaryAction.path)">{{ primaryAction.label }}</el-button>
      </div>
    </header>

    <section class="decision-strip">
      <article class="decision-score" :class="`decision-score--${decisionTone}`">
        <span>Research Readiness</span>
        <strong>{{ readinessScore }}</strong>
        <small>{{ decisionSummary }}</small>
      </article>
      <article v-for="card in focusCards" :key="card.key" class="focus-card" :class="`focus-card--${card.tone}`">
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
        <small>{{ card.hint }}</small>
      </article>
    </section>

    <section class="decision-layout">
      <article class="panel-card action-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">TODAY'S CALLS</span>
            <h3>今日行动建议</h3>
          </div>
          <el-button text size="small" @click="router.push('/research')">进入研究实验室</el-button>
        </div>
        <div class="action-stack">
          <button
            v-for="action in actionRows"
            :key="action.key"
            type="button"
            class="action-row"
            :class="`action-row--${action.tone}`"
            @click="router.push(action.path)"
          >
            <span>{{ action.kicker }}</span>
            <div>
              <strong>{{ action.title }}</strong>
              <small>{{ action.description }}</small>
            </div>
            <b>{{ action.cta }}</b>
          </button>
        </div>
      </article>

      <article class="panel-card input-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RESEARCH INPUTS</span>
            <h3>投研输入口径</h3>
          </div>
          <el-button text size="small" @click="router.push('/data')">查看详情</el-button>
        </div>
        <div class="input-grid">
          <div v-for="row in researchInputRows" :key="row.label" class="input-row" :class="`input-row--${row.tone}`">
            <span>{{ row.label }}</span>
            <strong>{{ row.value }}</strong>
            <small>{{ row.hint }}</small>
          </div>
        </div>
      </article>
    </section>

    <section class="workbench-grid">
      <article class="panel-card pipeline-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RESEARCH PIPELINE</span>
            <h3>投研流水线推进</h3>
          </div>
        </div>
        <div class="pipeline-rail">
          <button v-for="stage in pipelineStages" :key="stage.path" type="button" class="pipeline-step" @click="router.push(stage.path)">
            <span>{{ stage.kicker }}</span>
            <strong>{{ stage.title }}</strong>
            <p>{{ stage.description }}</p>
          </button>
        </div>
      </article>

      <article class="panel-card handoff-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">HANDOFF TAPE</span>
            <h3>最近影响研究的事件</h3>
          </div>
          <el-button text size="small" @click="router.push('/monitor')">运维排障</el-button>
        </div>
        <div v-if="handoffRows.length" class="handoff-list">
          <div v-for="row in handoffRows" :key="row.key" class="handoff-row">
            <div>
              <strong>{{ row.title }}</strong>
              <span>{{ row.subtitle }}</span>
            </div>
            <b :class="`tone-${row.tone}`">{{ row.status }}</b>
          </div>
        </div>
        <p v-else class="empty-copy">暂无影响投研决策的近期事件。</p>
      </article>
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
const gridStatus = ref<GridStatus | null>(null)

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const activeTasks = computed(() => runtimeTasks.value.filter(task => ['queued', 'running'].includes(String(task.status))))
const latestBacktest = computed(() => latestBacktests.value[0] || null)
const dataReadyCount = computed(() => researchInputRows.value.filter(row => row.tone === 'good').length)
const hasBlockingSync = computed(() => ['queued', 'running'].includes(syncStatus.value?.status || ''))
const tradeRiskOpen = computed(() => gridStatus.value?.order_submit_enabled === true)

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
  return '可以进入因子评估或策略迭代'
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
    hint: gridStatus.value?.account_id || 'QMT 为可选外部依赖',
    tone: tradeRiskOpen.value ? 'bad' : 'good',
  },
])

const researchInputRows = computed(() => [
  inputRow('日线行情', 'market_daily', '股票池、因子和日频回测的最低前置'),
  inputRow('分钟行情', 'market_minute', '日内策略和固定 timer 回测输入'),
  inputRow('基础股票', 'stocks', '行业、市值、状态过滤'),
  inputRow('财务报表', 'financial', '质量/成长/估值因子输入'),
  inputRow('因子缓存', 'factor_values', '因子评估只消费缓存'),
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
  { kicker: '02 IDEA', title: '研究实验室', path: '/research', description: '沉淀假设、证据链接、实验记录和失败复盘。' },
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
  overflow: auto;
}

.workbench-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: var(--space-4);
  align-items: center;
  padding: var(--space-5);
  border-color: rgba(56, 189, 248, 0.2);
}

.workbench-hero h2 {
  margin: var(--space-1) 0 var(--space-2);
  font-size: 25px;
}

.workbench-hero p {
  margin: 0;
  max-width: 860px;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.hero-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: var(--space-2);
}

.decision-strip {
  display: grid;
  grid-template-columns: minmax(240px, 1.35fr) repeat(4, minmax(0, 1fr));
  gap: var(--space-3);
}

.decision-score,
.focus-card {
  display: flex;
  min-height: 122px;
  flex-direction: column;
  justify-content: space-between;
  gap: var(--space-2);
  padding: var(--space-4);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.08), rgba(10, 14, 20, 0.68));
  box-shadow: var(--shadow-card);
}

.decision-score strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 34px;
  letter-spacing: -0.04em;
}

.focus-card strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 18px;
}

.decision-score span,
.decision-score small,
.focus-card span,
.focus-card small {
  color: var(--text-muted);
  font-size: var(--text-xs);
  line-height: 1.5;
}

.decision-score--good,
.focus-card--good {
  border-color: rgba(34, 197, 94, 0.32);
}

.decision-score--warn,
.focus-card--warn {
  border-color: rgba(245, 158, 11, 0.34);
}

.decision-score--bad,
.focus-card--bad {
  border-color: rgba(239, 68, 68, 0.36);
}

.decision-layout,
.workbench-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.18fr) minmax(360px, 0.82fr);
  gap: var(--space-4);
}

.action-stack,
.input-grid,
.handoff-list {
  display: flex;
  flex-direction: column;
  gap: var(--space-2);
  padding: var(--space-4);
}

.action-row {
  display: grid;
  grid-template-columns: 118px minmax(0, 1fr) auto;
  align-items: center;
  gap: var(--space-3);
  width: 100%;
  padding: 13px 14px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  color: var(--text-primary);
  text-align: left;
  background: rgba(10, 14, 20, 0.58);
  cursor: pointer;
}

.action-row:hover {
  border-color: rgba(56, 189, 248, 0.42);
  background: rgba(56, 189, 248, 0.08);
}

.action-row span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.action-row strong,
.handoff-row strong {
  color: var(--text-bright);
}

.action-row small {
  display: block;
  margin-top: 4px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.5;
}

.action-row b {
  color: var(--text-bright);
  font-size: var(--text-xs);
  white-space: nowrap;
}

.action-row--good {
  border-color: rgba(34, 197, 94, 0.26);
}

.action-row--warn {
  border-color: rgba(245, 158, 11, 0.3);
}

.action-row--bad {
  border-color: rgba(239, 68, 68, 0.34);
}

.input-row {
  display: grid;
  grid-template-columns: 96px minmax(0, 1fr);
  gap: 4px var(--space-3);
  padding: 11px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(10, 14, 20, 0.58);
}

.input-row span,
.input-row small,
.handoff-row span {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.input-row strong {
  color: var(--text-primary);
  font-family: var(--font-data);
}

.input-row small {
  grid-column: 2;
}

.input-row--good {
  border-color: rgba(34, 197, 94, 0.2);
}

.input-row--warn {
  border-color: rgba(245, 158, 11, 0.28);
}

.pipeline-panel {
  padding-bottom: var(--space-4);
}

.pipeline-rail {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: var(--space-3);
  padding: 0 var(--space-4);
}

.pipeline-step {
  position: relative;
  min-height: 170px;
  padding: var(--space-4);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
  color: var(--text-primary);
  text-align: left;
  background: rgba(10, 14, 20, 0.58);
  cursor: pointer;
}

.pipeline-step:hover {
  border-color: rgba(56, 189, 248, 0.42);
  background: rgba(56, 189, 248, 0.08);
}

.pipeline-step span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.pipeline-step strong {
  display: block;
  margin: var(--space-2) 0;
  color: var(--text-bright);
  font-size: var(--text-base);
}

.pipeline-step p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.55;
}

.handoff-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: var(--space-3);
  padding: 11px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(10, 14, 20, 0.58);
}

.handoff-row div {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 3px;
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

@media (max-width: 1500px) {
  .decision-strip {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .pipeline-rail {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 980px) {
  .workbench-hero,
  .decision-layout,
  .workbench-grid {
    grid-template-columns: 1fr;
  }

  .hero-actions {
    justify-content: flex-start;
  }
}

@media (max-width: 680px) {
  .decision-strip,
  .pipeline-rail,
  .action-row,
  .input-row {
    grid-template-columns: 1fr;
  }

  .input-row small {
    grid-column: auto;
  }
}
</style>

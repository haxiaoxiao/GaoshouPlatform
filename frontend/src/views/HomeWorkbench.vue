<template>
  <div class="home-workbench" :class="`home-mode-${layoutMode.toLowerCase()}`">
    <header class="workbench-header">
      <div class="header-brand">
        <div class="eyebrow">INVESTMENT DECISION DESK</div>
        <div class="title-row">
          <h2>今日投研工作台</h2>
          <span class="decision-pill" :class="`pill--${decisionTone}`">
            <span class="status-dot"></span>
            {{ decisionSummary }}
          </span>
        </div>
      </div>

      <div class="header-actions">
        <div class="layout-switcher" aria-label="布局预览切换">
          <button
            v-for="option in layoutOptions"
            :key="option.mode"
            :class="{ active: layoutMode === option.mode }"
            :title="option.hint"
            :aria-pressed="layoutMode === option.mode"
            @click="layoutMode = option.mode"
          >
            <span>{{ option.mode }}</span>
            {{ option.label }}
          </button>
        </div>
        <el-button size="small" :icon="Refresh" :loading="loading" @click="loadWorkbench">刷新判断</el-button>
        <el-button size="small" type="primary" class="btn-pine" @click="router.push(primaryAction.path)">
          {{ primaryAction.label }}
        </el-button>
      </div>
    </header>

    <div v-if="layoutMode === 'A'" class="layout-a split-pane">
      <aside class="split-rail">
        <div class="panel-kicker">PIPELINE</div>
        <nav class="pipeline-nav">
          <button v-for="stage in pipelineStages" :key="stage.path" class="pipeline-step" @click="router.push(stage.path)">
            <span class="step-index">{{ stage.kicker.split(' ')[0] }}</span>
            <span class="step-copy">
              <strong>{{ stage.title }}</strong>
              <small>{{ stage.description }}</small>
            </span>
          </button>
        </nav>
      </aside>

      <main class="split-work">
        <section class="readiness-strip" :class="`strip--${decisionTone}`">
          <div>
            <span class="panel-kicker">READINESS</span>
            <strong>{{ readinessScore }}</strong>
          </div>
          <p>{{ decisionSummary }}</p>
        </section>

        <section class="pane-section">
          <div class="section-heading">
            <span>今日行动建议</span>
            <small>TODAY'S CALLS</small>
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
                  <td class="text-right action-link">{{ action.cta }} →</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>
      </main>

      <aside class="split-aside">
        <section class="pane-section">
          <div class="panel-kicker">FOCUS</div>
          <div class="status-stack">
            <div v-for="card in focusCards" :key="card.key" class="status-row" :class="`row--${card.tone}`">
              <span class="status-dot"></span>
              <span class="status-name" :title="card.hint">{{ card.label }}</span>
              <strong>{{ card.value }}</strong>
            </div>
          </div>
        </section>

        <section class="pane-section">
          <div class="panel-kicker">INPUTS</div>
          <div class="status-stack">
            <div v-for="row in researchInputRows" :key="row.label" class="status-row" :class="`row--${row.tone}`">
              <span class="status-dot"></span>
              <span class="status-name">{{ row.label }}</span>
              <strong>{{ row.value }}</strong>
            </div>
          </div>
        </section>

        <section class="pane-section">
          <div class="panel-kicker">HANDOFF</div>
          <div class="event-stack">
            <button v-for="row in handoffRows" :key="row.key" class="event-row" type="button">
              <span class="event-meta">
                <span>{{ row.subtitle }}</span>
                <em :class="`badge--${row.tone}`">{{ row.status }}</em>
              </span>
              <strong>{{ row.title }}</strong>
            </button>
          </div>
        </section>
      </aside>
    </div>

    <div v-else-if="layoutMode === 'B'" class="layout-b matrix-sheet">
      <section class="matrix-summary">
        <div>
          <span>就绪评分</span>
          <strong :class="`text--${decisionTone}`">{{ readinessScore }}</strong>
        </div>
        <div>
          <span>同步写入</span>
          <strong :class="hasBlockingSync ? 'text--warn' : 'text--good'">
            {{ hasBlockingSync ? '进行中' : '无阻塞' }}
          </strong>
        </div>
        <div>
          <span>交易护栏</span>
          <strong :class="tradeRiskOpen ? 'text--bad' : 'text--good'">
            {{ tradeRiskOpen ? '真实下单开启' : '仅信号' }}
          </strong>
        </div>
        <div>
          <span>最近回测</span>
          <strong>{{ latestBacktestText }}</strong>
        </div>
      </section>

      <section class="matrix-table-section">
        <div class="section-heading">
          <span>矩阵审计表</span>
          <small>DIAGNOSTIC MATRIX</small>
        </div>
        <div class="table-wrap">
          <table class="quant-table matrix-table">
            <thead>
              <tr>
                <th>模块</th>
                <th>时间 / 状态</th>
                <th>诊断</th>
                <th>建议动作</th>
                <th>关联页面</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="row in researchInputRows" :key="row.label">
                <td class="text-strong">{{ row.label }}</td>
                <td class="font-mono muted">{{ row.value }}</td>
                <td>
                  <span class="tone-chip" :class="`tone-chip--${row.tone}`">
                    {{ row.tone === 'good' ? 'READY' : 'GAP' }}
                  </span>
                </td>
                <td>{{ row.hint }}</td>
                <td class="font-mono muted">/data</td>
              </tr>
              <tr
                v-for="action in actionRows"
                :key="action.key"
                class="clickable-row action-audit-row"
                @click="router.push(action.path)"
              >
                <td class="text-strong">{{ action.kicker }}</td>
                <td class="font-mono muted">待执行</td>
                <td><span class="tone-chip" :class="`tone-chip--${action.tone}`">{{ action.tone.toUpperCase() }}</span></td>
                <td>
                  <strong class="action-link">{{ action.title }}</strong>
                  <span class="audit-desc">{{ action.description }}</span>
                </td>
                <td class="font-mono muted">{{ action.path }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section class="matrix-tape">
        <span class="panel-kicker">HANDOFF TAPE</span>
        <div class="tape-scroll">
          <div v-for="row in handoffRows" :key="row.key" class="tape-item">
            <span class="font-mono">[{{ row.subtitle.slice(-5) }}]</span>
            <strong>{{ row.title }}</strong>
            <em :class="`badge--${row.tone}`">{{ row.status }}</em>
          </div>
        </div>
      </section>
    </div>

    <div v-else class="layout-c console-dashboard">
      <section class="console-panel">
        <div class="console-title">ACTIVE WORKFLOW TERMINAL</div>
        <div class="console-window">
          <div class="console-line">
            <span class="prompt">$</span>
            <span>select.step --active</span>
          </div>
          <div class="console-output">
            <span class="console-muted">CURRENT WORKFLOW STATE</span>
            <div class="console-pills">
              <button v-for="stage in pipelineStages" :key="stage.path" type="button" @click="router.push(stage.path)">
                {{ stage.kicker.split(' ')[0] }} {{ stage.title }}
              </button>
            </div>
          </div>

          <div class="console-line spaced">
            <span class="prompt">$</span>
            <span>suggest.actions --target=ready</span>
          </div>
          <div class="console-output">
            <button
              v-for="action in actionRows"
              :key="action.key"
              class="console-action"
              type="button"
              @click="router.push(action.path)"
            >
              <span :class="`console-tone--${action.tone}`">[{{ action.kicker }}]</span>
              <strong>{{ action.title }}</strong>
              <em>{{ action.description }}</em>
            </button>
          </div>
        </div>
      </section>

      <section class="console-panel">
        <div class="console-title">CONSOLE METERS</div>
        <div class="console-window">
          <div class="meter-block">
            <span>READINESS METER: {{ readinessScore }}</span>
            <strong>[{{ consoleBar }}]</strong>
          </div>

          <div class="terminal-list">
            <div class="terminal-heading">SYSTEM STATE</div>
            <div v-for="row in consoleStatusRows" :key="row.label" class="terminal-row">
              <span>{{ row.label }}</span>
              <i></i>
              <strong :class="`console-tone--${row.tone}`">[{{ row.value }}]</strong>
            </div>
          </div>

          <div class="terminal-list">
            <div class="terminal-heading">DATA INGESTION</div>
            <div v-for="row in researchInputRows" :key="row.label" class="terminal-row">
              <span>{{ row.label }}</span>
              <i></i>
              <strong :class="`console-tone--${row.tone}`">[{{ row.value.split(' ')[0] || 'EMPTY' }}]</strong>
            </div>
          </div>
        </div>
      </section>
    </div>
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
type LayoutMode = 'A' | 'B' | 'C'

interface LayoutOption {
  mode: LayoutMode
  label: string
  hint: string
}

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

const layoutMode = ref<LayoutMode>('A')

const router = useRouter()
const loading = ref(false)
const systemStatus = ref<SystemStatus | null>(null)
const dataSummary = ref<DataSummary | null>(null)
const syncStatus = ref<SyncStatus | null>(null)
const syncLogs = ref<SyncLog[]>([])
const runtimeTasks = ref<RuntimeTask[]>([])
const latestBacktests = ref<Backtest[]>([])
const liveStatus = ref<LiveTradingStatus | null>(null)

const layoutOptions: LayoutOption[] = [
  { mode: 'A', label: 'Split Pane', hint: '分栏终端' },
  { mode: 'B', label: 'Matrix Audit Sheet', hint: '矩阵审计表' },
  { mode: 'C', label: 'Console Dashboard', hint: '极客命令行' },
]

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const activeTasks = computed(() => runtimeTasks.value.filter(task => ['queued', 'running'].includes(String(task.status))))
const latestBacktest = computed(() => latestBacktests.value[0] || null)
const dataReadyCount = computed(() => researchInputRows.value.filter(row => row.tone === 'good').length)
const hasBlockingSync = computed(() => ['queued', 'running'].includes(syncStatus.value?.status || ''))
const tradeRiskOpen = computed(() => liveStatus.value?.order_submit_enabled === true)
const latestBacktestText = computed(() => latestBacktest.value ? backtestStatusLabel(latestBacktest.value.status) : '未运行')

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

const readinessPercent = computed(() => Number(readinessScore.value.replace('%', '')))
const consoleBar = computed(() => {
  const completed = Math.round(readinessPercent.value / 4)
  return `${'='.repeat(completed)}>${'.'.repeat(Math.max(0, 25 - completed))}`
})

const consoleStatusRows = computed(() => [
  {
    label: 'SYSTEM HEALTH',
    value: systemStatus.value?.status === 'healthy' || systemStatus.value?.status === 'ok' ? 'OK' : 'CHECK',
    tone: systemStatus.value?.status === 'healthy' || systemStatus.value?.status === 'ok' ? 'good' as Tone : 'warn' as Tone,
  },
  {
    label: 'MUTATING SYNC',
    value: hasBlockingSync.value ? 'ACTIVE' : 'NONE',
    tone: hasBlockingSync.value ? 'warn' as Tone : 'good' as Tone,
  },
  {
    label: 'LAST BACKTEST',
    value: latestBacktest.value ? latestBacktest.value.status.toUpperCase() : 'NONE',
    tone: latestBacktest.value?.status === 'failed' ? 'bad' as Tone : latestBacktest.value ? 'good' as Tone : 'neutral' as Tone,
  },
  {
    label: 'REAL ORDER ROUTE',
    value: tradeRiskOpen.value ? 'ACTIVE' : 'SIGNAL',
    tone: tradeRiskOpen.value ? 'bad' as Tone : 'good' as Tone,
  },
])

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
/* ─── 象牙暖白松风 Theme (Quant Compact Style) ─── */
.theme-pine-quant {
  --bg-page: #fdfbf7;         /* Warm Ivory White (象牙白) */
  --bg-card: #f5f2ea;         /* Slightly darker warm tone for elements */
  --bg-hover: #ebe7dc;        /* Hover transition tone */
  --border-color: #e5dfd3;    /* Precise, hair-thin lines */
  --text-main: #22302a;       /* Deep Pine Black / Dark Ink */
  --text-sub: #54635c;        /* Moss Green Muted */
  --text-light: #7e8d86;      /* Lighter Moss Gray */
  
  /* Brand Accent Colors */
  --pine-primary: #1b3d32;    /* Deep Pine Green (松风绿) */
  --pine-secondary: #355e4f;  /* Medium Pine Green */
  --pine-bg-light: #eef3f0;   /* Muted Pine background tint */

  /* Quant Semantics (Muted Traditional Chinese Tones) */
  --color-good: #2d6a4f;      /* Soft Jade Green */
  --color-good-bg: #eaf5f0;
  --color-warn: #b27a1e;      /* Soft Ochre Yellow */
  --color-warn-bg: #fdf6e6;
  --color-bad: #a83232;       /* Soft Madder Red */
  --color-bad-bg: #fbf1f1;
  --color-neutral: #5c6863;
  --color-neutral-bg: #f2f2ef;

  background-color: var(--bg-page);
  color: var(--text-main);
  min-height: 100vh;
  box-sizing: border-box;
  padding: 16px 20px;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  letter-spacing: -0.01em;
}

/* Common Kicker Style */
.section-kicker {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  font-weight: 700;
  color: var(--pine-secondary);
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

/* Header Section */
.workbench-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 12px;
  margin-bottom: 16px;
}

.header-brand {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.title-group {
  display: flex;
  align-items: center;
  gap: 12px;
}

.title-group h2 {
  margin: 0;
  font-size: 20px;
  font-weight: 700;
  color: var(--pine-primary);
}

.decision-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 500;
  padding: 3px 8px;
  border-radius: 4px;
  border: 1px solid transparent;
}

.decision-pill .dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  display: inline-block;
}

.pill--good {
  background: var(--color-good-bg);
  color: var(--color-good);
  border-color: rgba(45, 106, 79, 0.2);
}
.pill--good .dot { background-color: var(--color-good); }

.pill--warn {
  background: var(--color-warn-bg);
  color: var(--color-warn);
  border-color: rgba(178, 122, 30, 0.2);
}
.pill--warn .dot { background-color: var(--color-warn); }

.pill--bad {
  background: var(--color-bad-bg);
  color: var(--color-bad);
  border-color: rgba(168, 50, 50, 0.2);
}
.pill--bad .dot { background-color: var(--color-bad); }

.header-actions {
  display: flex;
  align-items: center;
  gap: 12px;
}

/* Segmented Control Switcher */
.layout-switcher {
  display: flex;
  background-color: var(--bg-card);
  border: 1px solid var(--border-color);
  padding: 2px;
  border-radius: 4px;
}

.layout-switcher button {
  background: transparent;
  border: none;
  font-size: 11px;
  font-weight: 600;
  color: var(--text-sub);
  padding: 4px 8px;
  cursor: pointer;
  border-radius: 3px;
  transition: all 0.15s ease;
}

.layout-switcher button.active {
  background-color: var(--pine-primary);
  color: #ffffff;
}

/* Button overrides for element-plus */
:deep(.el-button) {
  border-radius: 4px;
  border-color: var(--border-color);
  background-color: var(--bg-card);
  color: var(--text-main);
  font-weight: 500;
}
:deep(.el-button:hover) {
  background-color: var(--bg-hover);
  border-color: var(--text-light);
  color: var(--pine-primary);
}

:deep(.el-button--primary.btn-pine) {
  background-color: var(--pine-primary) !important;
  border-color: var(--pine-primary) !important;
  color: #ffffff !important;
}
:deep(.el-button--primary.btn-pine:hover) {
  background-color: var(--pine-secondary) !important;
  border-color: var(--pine-secondary) !important;
}

/* ========================================== */
/* MODE 1: SPLIT PANE STYLES                  */
/* ========================================== */
.layout-split {
  display: grid;
  grid-template-columns: 200px minmax(0, 1fr) 280px;
  gap: 1px;
  background-color: var(--border-color);
  border: 1px solid var(--border-color);
  margin-top: 16px;
}

.layout-split > div {
  background-color: var(--bg-page);
  padding: 16px;
  box-sizing: border-box;
}

.split-sidebar {
  border-right: 1px solid var(--border-color);
}

.sidebar-title {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  font-weight: 700;
  color: var(--text-light);
  letter-spacing: 0.1em;
  margin-bottom: 12px;
}

.pipeline-nav {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.pipeline-nav-btn {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  background: transparent;
  border: none;
  width: 100%;
  text-align: left;
  padding: 6px;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.pipeline-nav-btn:hover {
  background-color: var(--bg-card);
}

.nav-num {
  font-family: "Consolas", Monaco, monospace;
  font-size: 11px;
  font-weight: 700;
  color: var(--pine-secondary);
}

.nav-body {
  display: flex;
  flex-direction: column;
}

.nav-title {
  font-size: 12px;
  font-weight: 600;
  color: var(--text-main);
}

.nav-desc {
  font-size: 10px;
  color: var(--text-light);
  margin-top: 2px;
  line-height: 1.3;
}

.readiness-banner {
  padding: 12px;
  border-radius: 4px;
  margin-bottom: 16px;
  border-left: 4px solid transparent;
}

.readiness-banner--good {
  background-color: var(--color-good-bg);
  border-left-color: var(--color-good);
}
.readiness-banner--warn {
  background-color: var(--color-warn-bg);
  border-left-color: var(--color-warn);
}
.readiness-banner--bad {
  background-color: var(--color-bad-bg);
  border-left-color: var(--color-bad);
}

.banner-title {
  font-family: "Consolas", Monaco, monospace;
  font-size: 9px;
  font-weight: 700;
  color: var(--text-sub);
  letter-spacing: 0.08em;
}

.banner-core {
  display: flex;
  align-items: baseline;
  gap: 12px;
  margin-top: 4px;
}

.banner-score {
  font-family: "Consolas", Monaco, monospace;
  font-size: 24px;
  font-weight: 700;
  color: var(--text-main);
}

.banner-hint {
  font-size: 12px;
  font-weight: 600;
  color: var(--text-main);
}

/* Shared Tables */
.quant-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 8px;
}

.quant-table th {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  color: var(--text-light);
  text-align: left;
  padding: 6px 8px;
  border-bottom: 1px solid var(--border-color);
  font-weight: 600;
}

.quant-table td {
  padding: 8px;
  border-bottom: 1px solid var(--border-color);
  font-size: 12px;
  color: var(--text-main);
  vertical-align: middle;
}

.quant-table tr.clickable-row {
  cursor: pointer;
}

.quant-table tr.clickable-row:hover {
  background-color: var(--bg-hover);
}

.text-right {
  text-align: right !important;
}

.text-bold {
  font-weight: 600;
}

.text-secondary {
  color: var(--text-sub) !important;
}

.font-mono {
  font-family: "Consolas", Monaco, monospace;
}

.text-link {
  color: var(--pine-secondary);
  font-weight: 600;
}

.table-kicker {
  font-family: "Consolas", Monaco, monospace;
  font-size: 9px;
  font-weight: 700;
  padding: 2px 4px;
  border-radius: 2px;
}

.table-kicker.kicker--good {
  background-color: var(--color-good-bg);
  color: var(--color-good);
}
.table-kicker.kicker--warn {
  background-color: var(--color-warn-bg);
  color: var(--color-warn);
}
.table-kicker.kicker--bad {
  background-color: var(--color-bad-bg);
  color: var(--color-bad);
}
.table-kicker.kicker--neutral {
  background-color: var(--color-neutral-bg);
  color: var(--color-neutral);
}

.task-title-wrap {
  display: flex;
  flex-direction: column;
}

.task-main-title {
  font-weight: 600;
  color: var(--text-main);
}

.task-sub-title {
  font-size: 10px;
  color: var(--text-light);
  margin-top: 1px;
}

.dense-status-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.dense-status-item {
  display: flex;
  align-items: center;
  padding: 6px 8px;
  background-color: var(--bg-card);
  border-radius: 4px;
  font-size: 11px;
}

.dense-status-item .status-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  margin-right: 8px;
}

.item-tone--good .status-dot { background-color: var(--color-good); }
.item-tone--warn .status-dot { background-color: var(--color-warn); }
.item-tone--bad .status-dot { background-color: var(--color-bad); }

.status-label {
  font-weight: 600;
  flex-grow: 1;
}

.status-value {
  font-family: "Consolas", Monaco, monospace;
  color: var(--text-sub);
}

.compact-feed {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.feed-node {
  border-bottom: 1px dashed var(--border-color);
  padding-bottom: 6px;
}
.feed-node:last-child {
  border-bottom: none;
}

.node-meta {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.node-time {
  font-family: "Consolas", Monaco, monospace;
  font-size: 9px;
  color: var(--text-light);
}

.node-badge {
  font-size: 9px;
  font-weight: 600;
  padding: 1px 4px;
  border-radius: 2px;
}

.badge-tone--good {
  background-color: var(--color-good-bg);
  color: var(--color-good);
}
.badge-tone--warn {
  background-color: var(--color-warn-bg);
  color: var(--color-warn);
}
.badge-tone--bad {
  background-color: var(--color-bad-bg);
  color: var(--color-bad);
}
.badge-tone--neutral {
  background-color: var(--color-neutral-bg);
  color: var(--color-neutral);
}

.node-title {
  font-size: 11px;
  font-weight: 600;
  color: var(--text-main);
  margin-top: 2px;
}

.margin-top-lg {
  margin-top: 24px;
}

.section-title {
  font-family: "Consolas", Monaco, monospace;
  font-size: 11px;
  font-weight: 700;
  color: var(--pine-secondary);
  letter-spacing: 0.08em;
  margin-bottom: 8px;
}

/* ========================================== */
/* MODE 2: MATRIX AUDIT SHEET STYLES          */
/* ========================================== */
.layout-matrix {
  display: flex;
  flex-direction: column;
  gap: 16px;
  margin-top: 16px;
}

.matrix-summary-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  border: 1px solid var(--border-color);
  background-color: var(--bg-card);
  border-radius: 4px;
}

.summary-item {
  display: flex;
  flex-direction: column;
  padding: 10px 16px;
  border-right: 1px solid var(--border-color);
}
.summary-item:last-child {
  border-right: none;
}

.summary-item .label {
  font-size: 10px;
  color: var(--text-light);
  text-transform: uppercase;
}

.summary-item .val {
  font-family: "Consolas", Monaco, monospace;
  font-size: 16px;
  font-weight: 700;
  color: var(--text-main);
  margin-top: 4px;
}

.summary-item .val.score {
  font-size: 20px;
}

.color--good { color: var(--color-good) !important; }
.color--warn { color: var(--color-warn) !important; }
.color--bad { color: var(--color-bad) !important; }

.status-badge-flat {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  font-weight: 700;
  padding: 2px 6px;
  border-radius: 2px;
}

.text-light-desc {
  color: var(--text-sub);
}

.matrix-action-tr td {
  background-color: var(--color-warn-bg);
}

.text-warn-color {
  color: var(--color-warn);
}

.action-desc-small {
  font-size: 10px;
  font-weight: normal;
  color: var(--text-light);
}

.matrix-log-tape {
  border: 1px solid var(--border-color);
  background-color: var(--bg-card);
  padding: 8px 12px;
  border-radius: 4px;
  display: flex;
  align-items: center;
  gap: 16px;
}

.tape-title {
  font-family: "Consolas", Monaco, monospace;
  font-size: 10px;
  font-weight: 700;
  color: var(--text-light);
  white-space: nowrap;
}

.tape-scroll {
  display: flex;
  gap: 18px;
  overflow-x: auto;
  flex-grow: 1;
}

.tape-log-item {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  white-space: nowrap;
}

.tape-log-item .time {
  font-family: "Consolas", Monaco, monospace;
  color: var(--text-light);
}

.tape-log-item .title {
  color: var(--text-main);
}

.tape-log-item .badge {
  font-size: 9px;
  font-weight: 700;
  padding: 1px 4px;
  border-radius: 2px;
}

/* ========================================== */
/* MODE 3: CONSOLE STYLES                     */
/* ========================================== */
.layout-console {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-top: 16px;
}

.console-title {
  font-family: "Consolas", Monaco, monospace;
  font-size: 11px;
  font-weight: 700;
  color: var(--pine-secondary);
  margin-bottom: 8px;
}

.cli-pane {
  background-color: #1a2420; /* Ultra-dark moss green console */
  border: 1px solid #2f3e38;
  border-radius: 6px;
  padding: 16px;
  color: #c9d6d0; /* Muted ivory-green code text */
  font-family: "Consolas", Monaco, monospace;
  font-size: 12px;
  min-height: 280px;
  box-sizing: border-box;
}

.cli-line {
  display: flex;
  gap: 8px;
}

.cli-prompt {
  color: #639c80; /* Pine terminal prompt symbol */
}

.cli-command {
  color: #f7f9f8;
}

.cli-response {
  margin-left: 16px;
  margin-top: 4px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.response-lead {
  color: #7b9388;
  font-size: 11px;
}

.workflow-cli-nodes {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 4px;
}

.cli-node-pill {
  background-color: #273730;
  border: 1px solid #374b42;
  color: #e6ede8;
  padding: 2px 8px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 11px;
  transition: all 0.2s ease;
}

.cli-node-pill:hover {
  background-color: var(--pine-primary);
  border-color: var(--pine-secondary);
}

.margin-top-md {
  margin-top: 16px;
}

.suggest-cli-actions {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 4px;
}

.cli-action-row {
  display: flex;
  align-items: baseline;
  gap: 8px;
  cursor: pointer;
  padding: 4px;
  border-radius: 3px;
}
.cli-action-row:hover {
  background-color: #25332d;
}

.cli-action-row .tag {
  font-weight: 700;
  font-size: 10px;
}

.tag-tone--good { color: #5bc295; }
.tag-tone--warn { color: #d09d43; }
.tag-tone--bad { color: #d96262; }
.tag-tone--neutral { color: #8ea097; }

.command-link {
  color: #ffffff;
  text-decoration: underline;
}

.cli-action-row .desc {
  color: #7b9388;
  font-size: 11px;
}

.meter-widget {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.meter-label {
  font-weight: 700;
  color: #7b9388;
}

.ascii-bar {
  font-size: 14px;
  letter-spacing: 0.1em;
  color: #5bc295;
  font-weight: bold;
}

.cli-checklist {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.checklist-header {
  font-weight: 700;
  color: #639c80;
  font-size: 11px;
  border-bottom: 1px dashed #2f3e38;
  padding-bottom: 2px;
  margin-bottom: 4px;
}

.checklist-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.checklist-item .label {
  color: #e6ede8;
}

.checklist-item .dots {
  flex-grow: 1;
  color: #31443c;
  overflow: hidden;
  text-align: center;
}

.checklist-item .status-text {
  font-weight: 700;
}

.color-good { color: #5bc295 !important; }
.color-warn { color: #d09d43 !important; }
.color-bad { color: #d96262 !important; }

/* Responsive adjustments for layouts */
@media (max-width: 1200px) {
  .layout-split {
    grid-template-columns: 1fr;
  }
  .split-sidebar, .split-right {
    border-right: none;
    border-bottom: 1px solid var(--border-color);
  }
  .layout-console {
    grid-template-columns: 1fr;
  }
}
</style>

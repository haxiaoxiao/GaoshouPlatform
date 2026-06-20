<template>
  <div class="page-frame ops-page">
    <header class="panel-card ops-hero">
      <div>
        <span class="section-kicker">OPERATIONS COMMAND CENTER</span>
        <h2>系统运维控制台</h2>
        <p>运维页只回答工程问题：哪个服务不可用、哪个队列在堆积、哪类数据口径异常、交易护栏是否越界。投研判断留在工作台。</p>
      </div>
      <div class="ops-actions">
        <el-button :icon="Refresh" :loading="loading" @click="loadOps">刷新拓扑</el-button>
        <el-button type="primary" @click="router.push('/data/sync')">进入数据同步</el-button>
      </div>
    </header>

    <section class="service-topology">
      <article
        v-for="node in serviceNodes"
        :key="node.key"
        class="service-node"
        :class="`service-node--${node.tone}`"
      >
        <div class="node-light" />
        <span>{{ node.label }}</span>
        <strong>{{ node.value }}</strong>
        <small>{{ node.detail }}</small>
      </article>
    </section>

    <section v-if="devDataMode?.enabled" class="panel-card dev-data-mode-card" :class="{ 'dev-data-mode-card--prod': devDataMode.use_prod_data }">
      <div>
        <span class="section-kicker">DEV DATA MODE</span>
        <h3>开发环境真实数据开关</h3>
        <p>
          当前数据源：
          <strong>{{ devDataMode.use_prod_data ? '生产真实数据' : '开发隔离数据' }}</strong>
          <span> · {{ devDataMode.active_data_dir }}</span>
        </p>
        <small>
          影响范围：数据同步写入目标、策略运行读取的 SQLite/Parquet 行情、财务和因子数据。
        </small>
      </div>
      <div class="dev-data-mode-card__actions">
        <el-button
          class="dev-data-mode-card__button"
          :type="devDataMode.use_prod_data ? 'warning' : 'primary'"
          :loading="switchingDataMode"
          @click="toggleDevDataMode"
        >
          {{ devDataMode.use_prod_data ? '切回开发隔离数据' : '切换到生产真实数据' }}
        </el-button>
        <small class="dev-data-mode-card__hint">
          {{ devDataMode.use_prod_data ? '当前 dev 正在使用生产真实数据' : '点击后会弹出危险操作确认' }}
        </small>
        <el-alert
          v-if="devDataMode.use_prod_data"
          title="危险：dev 将直接读写生产真实数据目录"
          type="warning"
          show-icon
          :closable="false"
        />
      </div>
    </section>

    <section class="ops-command-grid">
      <article class="panel-card incident-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">INCIDENT QUEUE</span>
            <h3>值班排障清单</h3>
          </div>
          <el-tag :type="incidentRows.some(row => row.tone === 'bad') ? 'danger' : incidentRows.some(row => row.tone === 'warn') ? 'warning' : 'success'" effect="plain">
            {{ incidentRows.length }} 条
          </el-tag>
        </div>
        <div class="incident-list">
          <button
            v-for="row in incidentRows"
            :key="row.key"
            type="button"
            class="incident-row"
            :class="`incident-row--${row.tone}`"
            @click="row.path && router.push(row.path)"
          >
            <span>{{ row.scope }}</span>
            <div>
              <strong>{{ row.title }}</strong>
              <small>{{ row.detail }}</small>
            </div>
            <b>{{ row.action }}</b>
          </button>
        </div>
      </article>

      <article class="panel-card queue-panel">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">QUEUE & GUARDRAILS</span>
            <h3>运行控制面板</h3>
          </div>
        </div>
        <div class="control-grid">
          <div v-for="item in controlRows" :key="item.label" class="control-row" :class="`control-row--${item.tone}`">
            <span>{{ item.label }}</span>
            <strong>{{ item.value }}</strong>
            <small>{{ item.hint }}</small>
          </div>
        </div>
      </article>
    </section>

    <section class="ops-grid">
      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RUNTIME TASKS</span>
            <h3>运行任务表</h3>
          </div>
          <span class="table-hint">按任务状态排障，不做投研解释</span>
        </div>
        <el-table :data="runtimeTasks" size="small" height="360" class="ops-table">
          <el-table-column prop="title" label="任务" min-width="190" show-overflow-tooltip />
          <el-table-column prop="kind" label="类型" width="140" show-overflow-tooltip />
          <el-table-column label="状态" width="110">
            <template #default="{ row }">
              <el-tag :type="taskTagType(row.status)" effect="plain" size="small">{{ row.status }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="进度" width="150">
            <template #default="{ row }">
              <el-progress :percentage="Math.round((row.progress || 0) * 100)" :stroke-width="6" :show-text="false" />
            </template>
          </el-table-column>
          <el-table-column label="错误" min-width="240" show-overflow-tooltip>
            <template #default="{ row }">{{ row.error || '-' }}</template>
          </el-table-column>
        </el-table>
      </article>

      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">STORAGE COVERAGE</span>
            <h3>存储覆盖巡检</h3>
          </div>
          <el-button text size="small" @click="router.push('/explorer')">数据浏览器</el-button>
        </div>
        <div class="dataset-list">
          <div v-for="row in datasetRows" :key="row.name" class="dataset-row" :class="`dataset-row--${row.tone}`">
            <span>{{ row.label }}</span>
            <strong>{{ row.latest }}</strong>
            <small>{{ row.rows }}</small>
            <el-tag :type="dataStatusTagType(row.tone)" effect="plain" size="small">{{ row.statusText }}</el-tag>
          </div>
        </div>
      </article>
    </section>

    <section class="panel-card">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">SYNC LOGS</span>
          <h3>最近同步审计日志</h3>
        </div>
        <span class="table-hint">失败优先看 error_message，成功只作审计留痕</span>
      </div>
      <el-table :data="syncLogs" size="small" height="280" class="ops-table">
        <el-table-column label="类型" width="170">
          <template #default="{ row }">{{ syncTypeLabel(row.sync_type) }}</template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag :type="taskTagType(row.status)" effect="plain" size="small">{{ syncStatusLabel(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="成功/失败" width="130">
          <template #default="{ row }">{{ row.success_count ?? 0 }} / {{ row.failed_count ?? 0 }}</template>
        </el-table-column>
        <el-table-column prop="start_time" label="开始" width="180" show-overflow-tooltip />
        <el-table-column prop="end_time" label="结束" width="180" show-overflow-tooltip />
        <el-table-column prop="error_message" label="错误" show-overflow-tooltip />
      </el-table>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import { systemApi, type DataSummary, type DataSummaryItem, type DevDataMode, type SystemStatus } from '@/api/system'
import { syncApi, type SyncLog, type SyncStatus } from '@/api/sync'
import { runtimeTaskApi, type RuntimeTask } from '@/api/runtimeTasks'
import { gridTradingApi, type GridStatus } from '@/api/gridTrading'

type Tone = 'good' | 'warn' | 'bad' | 'neutral'

interface ServiceNode {
  key: string
  label: string
  value: string
  detail: string
  tone: Tone
}

interface IncidentRow {
  key: string
  scope: string
  title: string
  detail: string
  action: string
  path?: string
  tone: Tone
}

interface ControlRow {
  label: string
  value: string
  hint: string
  tone: Tone
}

interface DatasetRow {
  label: string
  name: string
  latest: string
  rows: string
  statusText: string
  tone: Tone
}

const router = useRouter()
const loading = ref(false)
const systemStatus = ref<SystemStatus | null>(null)
const syncStatus = ref<SyncStatus | null>(null)
const runtimeTasks = ref<RuntimeTask[]>([])
const syncLogs = ref<SyncLog[]>([])
const dataSummary = ref<DataSummary | null>(null)
const gridStatus = ref<GridStatus | null>(null)
const devDataMode = ref<DevDataMode | null>(null)
const switchingDataMode = ref(false)

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const activeTasks = computed(() => runtimeTasks.value.filter(task => ['queued', 'running'].includes(String(task.status))))
const failedTasks = computed(() => runtimeTasks.value.filter(task => String(task.status) === 'failed'))
const queuePendingCount = computed(() => Number(syncStatus.value?.details?.queue_pending_count || 0))
const syncServiceReady = computed(() => syncStatus.value?.sync_service_available !== false && syncStatus.value?.details?.sync_service_unavailable !== true)
const staleDatasetCount = computed(() => datasetRows.value.filter(row => row.tone === 'warn' || row.tone === 'bad').length)
const latestRuntimeTask = computed(() => runtimeTasks.value[0] || null)

const serviceNodes = computed<ServiceNode[]>(() => [
  {
    key: 'api',
    label: 'FastAPI',
    value: isApiHealthy.value ? '在线' : systemStatus.value ? '异常' : '未知',
    detail: String(systemStatus.value?.database || '等待 /system/status'),
    tone: isApiHealthy.value ? 'good' : systemStatus.value ? 'bad' : 'neutral',
  },
  {
    key: 'sync',
    label: 'Sync Worker',
    value: syncServiceReady.value ? syncStatusLabel(syncStatus.value?.status || 'idle') : '不可用',
    detail: syncStatus.value?.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : syncStatus.value?.reason || '队列待命',
    tone: !syncServiceReady.value ? 'bad' : ['queued', 'running'].includes(syncStatus.value?.status || '') ? 'warn' : 'good',
  },
  {
    key: 'storage',
    label: 'Storage',
    value: dataSummary.value?.overall_status === 'good' ? '正常' : dataSummary.value ? '降级' : '未知',
    detail: `${dataSummary.value?.market_data_backend || systemStatus.value?.market_data_backend || 'parquet'} · ${dataSummary.value?.parquet_data_dir || systemStatus.value?.parquet_data_dir || '等待路径'}`,
    tone: dataSummary.value?.overall_status === 'good' ? 'good' : dataSummary.value ? 'warn' : 'neutral',
  },
  {
    key: 'qmt',
    label: 'QMT / Quote',
    value: gridStatus.value?.quote_connected ? '行情在线' : gridStatus.value?.xtdata_available ? '未连接' : '不可用',
    detail: gridStatus.value?.error || (gridStatus.value?.xttrader_available ? 'xttrader 可用' : 'xttrader 未确认'),
    tone: gridStatus.value?.quote_connected ? 'good' : 'warn',
  },
  {
    key: 'orders',
    label: 'Order Guard',
    value: gridStatus.value?.order_submit_enabled ? '真实下单开启' : '仅信号',
    detail: gridStatus.value?.account_id || '未配置账户',
    tone: gridStatus.value?.order_submit_enabled ? 'bad' : 'good',
  },
])

const isApiHealthy = computed(() => systemStatus.value?.status === 'healthy' || systemStatus.value?.status === 'ok')

const incidentRows = computed<IncidentRow[]>(() => {
  const rows: IncidentRow[] = []
  if (systemStatus.value && !isApiHealthy.value) {
    rows.push({
      key: 'api',
      scope: 'API',
      title: '后端 API 状态异常',
      detail: `状态返回：${systemStatus.value.status}`,
      action: '查健康',
      path: '/monitor',
      tone: 'bad',
    })
  }
  if (!syncServiceReady.value) {
    rows.push({
      key: 'sync-service',
      scope: 'SYNC',
      title: '同步服务不可用',
      detail: syncStatus.value?.reason || String(syncStatus.value?.details?.proxy_error || '18810 同步服务未返回可用状态'),
      action: '看同步',
      path: '/data/sync',
      tone: 'bad',
    })
  }
  if (syncStatus.value?.status === 'failed') {
    rows.push({
      key: 'sync-failed',
      scope: 'SYNC',
      title: '当前同步任务失败',
      detail: syncStatus.value.error_message || syncStatus.value.reason || syncTypeLabel(syncStatus.value.sync_type || ''),
      action: '查日志',
      path: '/data/sync',
      tone: 'bad',
    })
  }
  if (queuePendingCount.value > 0) {
    rows.push({
      key: 'sync-queue',
      scope: 'QUEUE',
      title: '同步队列存在堆积',
      detail: `后端待执行 ${queuePendingCount.value} 项，当前任务 ${syncTypeLabel(syncStatus.value?.sync_type || '')}`,
      action: '看队列',
      path: '/data/sync',
      tone: 'warn',
    })
  }
  if (failedTasks.value.length > 0) {
    rows.push({
      key: 'runtime-failed',
      scope: 'TASK',
      title: '运行任务存在失败记录',
      detail: failedTasks.value[0]?.error || `${failedTasks.value.length} 条失败任务`,
      action: '看任务',
      path: '/monitor',
      tone: 'warn',
    })
  }
  if (staleDatasetCount.value > 0) {
    rows.push({
      key: 'dataset-stale',
      scope: 'DATA',
      title: '数据覆盖存在缺口或过期',
      detail: `${staleDatasetCount.value} 个数据集需要复核最新口径`,
      action: '看数据',
      path: '/data',
      tone: 'warn',
    })
  }
  if (gridStatus.value?.order_submit_enabled) {
    rows.push({
      key: 'order-open',
      scope: 'TRADE',
      title: '真实下单护栏已开启',
      detail: gridStatus.value.account_id || '账户未返回',
      action: '复核',
      path: '/trade',
      tone: 'bad',
    })
  }
  if (!rows.length) {
    rows.push({
      key: 'clear',
      scope: 'CLEAR',
      title: '暂无阻塞型运维事件',
      detail: '服务、队列、存储和交易护栏均未发现需要立即处理的异常。',
      action: '保持巡检',
      tone: 'good',
    })
  }
  return rows
})

const controlRows = computed<ControlRow[]>(() => [
  {
    label: '同步入口',
    value: syncServiceReady.value ? (syncStatus.value?.can_trigger === false ? '拒绝提交' : '可提交') : '不可用',
    hint: syncStatus.value?.reason || 'can_trigger / service_available',
    tone: !syncServiceReady.value || syncStatus.value?.can_trigger === false ? 'bad' : 'good',
  },
  {
    label: '当前同步',
    value: syncStatus.value ? syncStatusLabel(syncStatus.value.status) : '未知',
    hint: syncStatus.value?.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : '无运行任务',
    tone: syncStatus.value?.status === 'failed' ? 'bad' : ['queued', 'running'].includes(syncStatus.value?.status || '') ? 'warn' : 'good',
  },
  {
    label: '后端排队',
    value: `${queuePendingCount.value} 项`,
    hint: 'sync queue pending_count',
    tone: queuePendingCount.value > 0 ? 'warn' : 'good',
  },
  {
    label: '活动任务',
    value: `${activeTasks.value.length} 个`,
    hint: latestRuntimeTask.value ? latestRuntimeTask.value.title || latestRuntimeTask.value.kind : '无运行任务',
    tone: activeTasks.value.length ? 'warn' : 'good',
  },
  {
    label: '失败任务',
    value: `${failedTasks.value.length} 个`,
    hint: failedTasks.value[0]?.error || 'runtime task error',
    tone: failedTasks.value.length ? 'bad' : 'good',
  },
  {
    label: '存储异常',
    value: `${staleDatasetCount.value} 项`,
    hint: dataSummary.value?.generated_at ? `生成于 ${formatDateTime(dataSummary.value.generated_at)}` : '等待 data-summary',
    tone: staleDatasetCount.value ? 'warn' : 'good',
  },
])

const datasetRows = computed<DatasetRow[]>(() => [
  datasetRow('日线行情', 'market_daily'),
  datasetRow('分钟行情', 'market_minute'),
  datasetRow('定时分钟', 'minute_timer'),
  datasetRow('基础股票', 'stocks'),
  datasetRow('财务报表', 'financial'),
  datasetRow('因子缓存', 'factor_values'),
  datasetRow('指标缓存', 'stock_indicators'),
  datasetRow('同花顺概念', 'concept_membership'),
  datasetRow('新闻舆情', 'sentiment'),
])

async function loadOps() {
  loading.value = true
  try {
    const [systemResult, syncStatusResult, tasksResult, logsResult, summaryResult, gridResult, devModeResult] = await Promise.allSettled([
      systemApi.getStatus(),
      syncApi.getStatus(),
      runtimeTaskApi.list(true),
      syncApi.getLogs({ limit: 20 }),
      systemApi.dataSummary(),
      gridTradingApi.status(),
      systemApi.getDevDataMode(),
    ])
    if (systemResult.status === 'fulfilled') systemStatus.value = systemResult.value
    if (syncStatusResult.status === 'fulfilled') syncStatus.value = syncStatusResult.value
    if (tasksResult.status === 'fulfilled') runtimeTasks.value = tasksResult.value
    if (logsResult.status === 'fulfilled') syncLogs.value = logsResult.value
    if (summaryResult.status === 'fulfilled') dataSummary.value = summaryResult.value
    if (gridResult.status === 'fulfilled') gridStatus.value = gridResult.value
    if (devModeResult.status === 'fulfilled') devDataMode.value = devModeResult.value
  } finally {
    loading.value = false
  }
}

async function confirmDevDataModeSwitch(): Promise<boolean> {
  if (!devDataMode.value) return false
  const nextUseProd = !devDataMode.value.use_prod_data
  if (!nextUseProd) {
    await ElMessageBox.confirm(
      '切回开发隔离数据后，后续同步和策略运行会写入/读取 dev 本地数据目录，不再使用生产真实数据。',
      '切换到开发隔离数据',
      { confirmButtonText: '确认切换', cancelButtonText: '取消', type: 'info' },
    )
    return true
  }
  await ElMessageBox.confirm(
    devDataMode.value.warning || 'dev 将直接读写生产真实数据目录。数据同步可能改写真实 SQLite/Parquet，策略会读取真实行情、财务和因子数据。',
    '危险操作：使用生产真实数据',
    {
      confirmButtonText: '我已确认，切换到真实数据',
      cancelButtonText: '取消',
      type: 'warning',
      distinguishCancelAndClose: true,
    },
  )
  return true
}

async function handleDevDataModeChange(value: string | number | boolean) {
  const useProdData = Boolean(value)
  switchingDataMode.value = true
  try {
    devDataMode.value = await systemApi.setDevDataMode({
      use_prod_data: useProdData,
      acknowledge_warning: useProdData,
    })
    ElMessage.success(useProdData ? 'dev 已切换为生产真实数据' : 'dev 已切换为开发隔离数据')
    await loadOps()
  } catch (error) {
    ElMessage.error('数据模式切换失败，已刷新当前状态')
    await loadOps()
  } finally {
    switchingDataMode.value = false
  }
}

async function toggleDevDataMode() {
  if (!devDataMode.value || switchingDataMode.value) return
  const nextUseProdData = !devDataMode.value.use_prod_data
  const confirmed = await confirmDevDataModeSwitch().catch(() => false)
  if (!confirmed) return
  await handleDevDataModeChange(nextUseProdData)
}

function datasetRow(label: string, name: string): DatasetRow {
  const item = summaryMap.value[name]
  const tone = dataStatusTone(item?.status)
  return {
    label,
    name: item?.dataset || item?.key || name,
    latest: formatDateTime(item?.latest_datetime || item?.latest_date),
    rows: formatRows(item),
    statusText: item?.status_text || item?.status || '未返回',
    tone,
  }
}

function dataStatusTone(status?: string): Tone {
  if (status === 'good') return 'good'
  if (status === 'error' || status === 'missing') return 'bad'
  if (status === 'stale') return 'warn'
  return 'neutral'
}

function dataStatusTagType(tone: Tone): 'success' | 'warning' | 'danger' | 'info' {
  if (tone === 'good') return 'success'
  if (tone === 'bad') return 'danger'
  if (tone === 'warn') return 'warning'
  return 'info'
}

function formatRows(item?: DataSummaryItem): string {
  if (!item) return '未返回'
  if (item.row_count == null) return '未统计'
  const suffix = item.row_count_estimated ? ' 估算' : ''
  return `${new Intl.NumberFormat('zh-CN').format(item.row_count)} 行${suffix}`
}

function taskTagType(status: string): 'success' | 'warning' | 'danger' | 'info' {
  if (status === 'done' || status === 'completed') return 'success'
  if (status === 'failed') return 'danger'
  if (status === 'running' || status === 'queued') return 'warning'
  return 'info'
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

function formatDateTime(value?: string | null): string {
  if (!value) return '-'
  return value.replace('T', ' ').slice(0, value.includes(':') ? 16 : 10)
}

const pageContextBlocks = computed(() => [
  {
    title: 'Ops Command',
    rows: [
      { label: 'API', value: isApiHealthy.value ? '在线' : '异常', tone: isApiHealthy.value ? 'good' : 'bad' },
      { label: '同步服务', value: syncServiceReady.value ? syncStatusLabel(syncStatus.value?.status || 'idle') : '不可用', tone: syncServiceReady.value ? 'good' : 'bad' },
      { label: '后端排队', value: `${queuePendingCount.value} 项`, tone: queuePendingCount.value ? 'warn' : 'good' },
      { label: '真实下单', value: gridStatus.value?.order_submit_enabled ? '开启' : '关闭', tone: gridStatus.value?.order_submit_enabled ? 'bad' : 'good' },
    ],
  },
  {
    title: 'Incidents',
    rows: incidentRows.value.slice(0, 4).map(row => ({
      label: row.scope,
      value: row.title,
      tone: row.tone,
    })),
  },
])

usePageContext(pageContextBlocks)

onMounted(loadOps)
</script>

<style scoped>
.ops-page {
  overflow: auto;
}

.ops-hero {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: var(--space-4);
  align-items: center;
  padding: var(--space-5);
  border-color: rgba(148, 163, 184, 0.2);
  background:
    linear-gradient(90deg, rgba(15, 23, 42, 0.9), rgba(12, 18, 28, 0.84)),
    var(--bg-elevated);
}

.ops-hero h2 {
  margin: var(--space-1) 0 var(--space-2);
  font-size: 24px;
}

.ops-hero p {
  margin: 0;
  max-width: 840px;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.ops-actions {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: var(--space-2);
}

.service-topology {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: var(--space-3);
}

.service-node {
  position: relative;
  display: flex;
  min-height: 128px;
  flex-direction: column;
  justify-content: space-between;
  gap: var(--space-2);
  padding: var(--space-4);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  background: linear-gradient(180deg, rgba(15, 23, 42, 0.84), rgba(10, 14, 20, 0.72));
  box-shadow: var(--shadow-card);
}

.service-node::after {
  position: absolute;
  right: -13px;
  top: 50%;
  width: 13px;
  height: 1px;
  background: rgba(148, 163, 184, 0.24);
  content: '';
}

.service-node:last-child::after {
  display: none;
}

.node-light {
  width: 9px;
  height: 9px;
  border-radius: 999px;
  background: var(--text-muted);
  box-shadow: 0 0 0 5px rgba(148, 163, 184, 0.08);
}

.service-node span,
.service-node small {
  color: var(--text-muted);
  font-size: var(--text-xs);
  line-height: 1.5;
}

.service-node strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 19px;
}

.service-node--good {
  border-color: rgba(34, 197, 94, 0.3);
}

.service-node--good .node-light {
  background: var(--status-ready);
  box-shadow: 0 0 18px rgba(34, 197, 94, 0.32);
}

.service-node--warn {
  border-color: rgba(245, 158, 11, 0.34);
}

.service-node--warn .node-light {
  background: var(--color-warning);
  box-shadow: 0 0 18px rgba(245, 158, 11, 0.32);
}

.service-node--bad {
  border-color: rgba(239, 68, 68, 0.38);
}

.service-node--bad .node-light {
  background: var(--status-attention);
  box-shadow: 0 0 18px rgba(239, 68, 68, 0.34);
}

.dev-data-mode-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(360px, auto);
  gap: var(--space-4);
  align-items: center;
  min-height: 132px;
  margin-bottom: var(--space-4);
  padding: var(--space-4);
  border-color: rgba(56, 189, 248, 0.24);
  background: linear-gradient(90deg, rgba(14, 165, 233, 0.1), rgba(15, 23, 42, 0.72));
}

.dev-data-mode-card--prod {
  border-color: rgba(245, 158, 11, 0.54);
  background: linear-gradient(90deg, rgba(245, 158, 11, 0.16), rgba(15, 23, 42, 0.74));
}

.dev-data-mode-card h3 {
  margin: var(--space-1) 0 var(--space-2);
}

.dev-data-mode-card p,
.dev-data-mode-card small {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
  line-height: 1.6;
}

.dev-data-mode-card strong {
  color: var(--text-bright);
}

.dev-data-mode-card__actions {
  display: flex;
  min-width: 340px;
  flex-direction: column;
  align-items: flex-end;
  gap: var(--space-2);
  justify-content: center;
}

.dev-data-mode-card__button {
  min-width: 180px;
  font-weight: 700;
  letter-spacing: 0.02em;
}

.dev-data-mode-card__hint {
  color: var(--text-muted);
}

.ops-command-grid,
.ops-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(380px, 0.75fr);
  gap: var(--space-4);
}

.incident-list,
.control-grid,
.dataset-list {
  display: flex;
  flex-direction: column;
  gap: var(--space-2);
  padding: var(--space-4);
}

.incident-row {
  display: grid;
  grid-template-columns: 76px minmax(0, 1fr) auto;
  align-items: center;
  gap: var(--space-3);
  width: 100%;
  padding: 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  text-align: left;
  background: rgba(10, 14, 20, 0.58);
  cursor: pointer;
}

.incident-row:hover {
  border-color: rgba(56, 189, 248, 0.42);
  background: rgba(56, 189, 248, 0.08);
}

.incident-row span {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.incident-row strong {
  color: var(--text-bright);
}

.incident-row small {
  display: block;
  margin-top: 4px;
  color: var(--text-secondary);
  font-size: var(--text-xs);
  line-height: 1.5;
}

.incident-row b {
  color: var(--text-bright);
  font-size: var(--text-xs);
  white-space: nowrap;
}

.incident-row--good {
  border-color: rgba(34, 197, 94, 0.26);
}

.incident-row--warn {
  border-color: rgba(245, 158, 11, 0.3);
}

.incident-row--bad {
  border-color: rgba(239, 68, 68, 0.34);
}

.control-row {
  display: grid;
  grid-template-columns: 96px minmax(0, 1fr);
  gap: 4px var(--space-3);
  padding: 11px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(10, 14, 20, 0.58);
}

.control-row span,
.control-row small,
.dataset-row span,
.dataset-row small,
.table-hint {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.control-row strong,
.dataset-row strong {
  color: var(--text-primary);
  font-family: var(--font-data);
}

.control-row small {
  grid-column: 2;
}

.control-row--good,
.dataset-row--good {
  border-color: rgba(34, 197, 94, 0.2);
}

.control-row--warn,
.dataset-row--warn {
  border-color: rgba(245, 158, 11, 0.28);
}

.control-row--bad,
.dataset-row--bad {
  border-color: rgba(239, 68, 68, 0.3);
}

.ops-table {
  margin: 0 var(--space-4) var(--space-4);
  width: calc(100% - var(--space-4) * 2);
}

.dataset-row {
  display: grid;
  grid-template-columns: 92px minmax(0, 1fr) minmax(92px, auto) auto;
  align-items: center;
  gap: var(--space-3);
  padding: 10px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(10, 14, 20, 0.58);
}

:deep(.el-table) {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-header-bg-color: rgba(15, 23, 42, 0.9);
  --el-table-header-text-color: #cbd5e1;
  --el-table-text-color: #dbe4f0;
  --el-table-row-hover-bg-color: rgba(56, 189, 248, 0.08);
  --el-table-border-color: rgba(148, 163, 184, 0.16);
}

@media (max-width: 1500px) {
  .service-topology {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .service-node::after {
    display: none;
  }
}

@media (max-width: 1020px) {
  .ops-hero,
  .dev-data-mode-card,
  .ops-command-grid,
  .ops-grid {
    grid-template-columns: 1fr;
  }

  .ops-actions {
    justify-content: flex-start;
  }

  .dev-data-mode-card__actions {
    min-width: 0;
    align-items: flex-start;
  }
}

@media (max-width: 680px) {
  .service-topology,
  .incident-row,
  .control-row,
  .dataset-row {
    grid-template-columns: 1fr;
  }

  .control-row small {
    grid-column: auto;
  }
}
</style>

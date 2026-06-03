<template>
  <div class="page-frame ops-page">
    <header class="panel-card ops-hero">
      <div>
        <span class="section-kicker">PLATFORM OPS</span>
        <h2>系统运维</h2>
        <p>集中查看后端、同步服务、行情存储、运行任务和 QMT/交易护栏状态；只读观察，不在这里触发重型数据写入。</p>
      </div>
      <el-button :icon="Refresh" :loading="loading" @click="loadOps">刷新</el-button>
    </header>

    <section class="ops-status-grid">
      <article v-for="card in statusCards" :key="card.key" class="ops-status-card" :class="`ops-status-card--${card.tone}`">
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
        <small>{{ card.hint }}</small>
      </article>
    </section>

    <section class="ops-grid">
      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">RUNTIME TASKS</span>
            <h3>运行任务</h3>
          </div>
        </div>
        <el-table :data="runtimeTasks" size="small" height="360" class="ops-table">
          <el-table-column prop="title" label="任务" min-width="180" show-overflow-tooltip />
          <el-table-column prop="kind" label="类型" width="140" show-overflow-tooltip />
          <el-table-column label="状态" width="110">
            <template #default="{ row }">
              <el-tag :type="taskTagType(row.status)" effect="plain" size="small">{{ row.status }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="进度" width="140">
            <template #default="{ row }">{{ Math.round((row.progress || 0) * 100) }}%</template>
          </el-table-column>
          <el-table-column label="错误" min-width="220" show-overflow-tooltip>
            <template #default="{ row }">{{ row.error || '-' }}</template>
          </el-table-column>
        </el-table>
      </article>

      <article class="panel-card">
        <div class="panel-card__head">
          <div>
            <span class="section-kicker">DATASETS</span>
            <h3>存储口径</h3>
          </div>
        </div>
        <div class="dataset-list">
          <div v-for="row in datasetRows" :key="row.name" class="dataset-row">
            <span>{{ row.label }}</span>
            <strong>{{ row.latest }}</strong>
            <small>{{ row.name }}</small>
          </div>
        </div>
      </article>
    </section>

    <section class="panel-card">
      <div class="panel-card__head">
        <div>
          <span class="section-kicker">SYNC LOGS</span>
          <h3>最近同步记录</h3>
        </div>
      </div>
      <el-table :data="syncLogs" size="small" height="260" class="ops-table">
        <el-table-column label="类型" width="160">
          <template #default="{ row }">{{ syncTypeLabel(row.sync_type) }}</template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="110" />
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
import { Refresh } from '@element-plus/icons-vue'
import { usePageContext } from '@/app/pageContext'
import { systemApi, type DataSummary, type DataSummaryItem, type SystemStatus } from '@/api/system'
import { syncApi, type SyncLog, type SyncStatus } from '@/api/sync'
import { runtimeTaskApi, type RuntimeTask } from '@/api/runtimeTasks'
import { gridTradingApi, type GridStatus } from '@/api/gridTrading'

type Tone = 'good' | 'warn' | 'bad' | 'neutral'

interface OpsStatusCard {
  key: string
  label: string
  value: string
  hint: string
  tone: Tone
}

const loading = ref(false)
const systemStatus = ref<SystemStatus | null>(null)
const syncStatus = ref<SyncStatus | null>(null)
const runtimeTasks = ref<RuntimeTask[]>([])
const syncLogs = ref<SyncLog[]>([])
const dataSummary = ref<DataSummary | null>(null)
const gridStatus = ref<GridStatus | null>(null)

const activeTaskCount = computed(() => runtimeTasks.value.filter(task => ['queued', 'running'].includes(String(task.status))).length)
const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})

const statusCards = computed<OpsStatusCard[]>(() => [
  {
    key: 'backend',
    label: '后端 API',
    value: systemStatus.value?.status || '未知',
    hint: String(systemStatus.value?.database || '未返回数据库状态'),
    tone: systemStatus.value?.status === 'healthy' || systemStatus.value?.status === 'ok' ? 'good' : 'neutral',
  },
  {
    key: 'storage',
    label: '行情后端',
    value: String(systemStatus.value?.market_data_backend || 'parquet'),
    hint: String(systemStatus.value?.parquet_data_dir || 'DuckDB / Parquet'),
    tone: 'good',
  },
  {
    key: 'sync',
    label: '同步状态',
    value: syncStatus.value ? syncStatusLabel(syncStatus.value.status) : '未知',
    hint: syncStatus.value?.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : '无当前任务',
    tone: syncStatus.value?.status === 'failed' ? 'bad' : syncStatus.value?.status === 'running' ? 'warn' : 'good',
  },
  {
    key: 'tasks',
    label: '活动任务',
    value: `${activeTaskCount.value}`,
    hint: `${runtimeTasks.value.length} 个任务记录`,
    tone: activeTaskCount.value ? 'warn' : 'good',
  },
  {
    key: 'qmt',
    label: 'QMT 行情',
    value: gridStatus.value?.quote_connected ? '已连接' : '未确认',
    hint: gridStatus.value?.xtdata_available ? 'xtdata 可用' : 'xtdata 不可用',
    tone: gridStatus.value?.quote_connected ? 'good' : 'warn',
  },
  {
    key: 'order',
    label: '下单护栏',
    value: gridStatus.value?.order_submit_enabled ? '真实下单开启' : '仅信号',
    hint: gridStatus.value?.account_id || '未配置账户',
    tone: gridStatus.value?.order_submit_enabled ? 'bad' : 'good',
  },
])

const datasetRows = computed(() => [
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
    const [systemResult, syncStatusResult, tasksResult, logsResult, summaryResult, gridResult] = await Promise.allSettled([
      systemApi.getStatus(),
      syncApi.getStatus(),
      runtimeTaskApi.list(true),
      syncApi.getLogs({ limit: 20 }),
      systemApi.dataSummary(),
      gridTradingApi.status(),
    ])
    if (systemResult.status === 'fulfilled') systemStatus.value = systemResult.value
    if (syncStatusResult.status === 'fulfilled') syncStatus.value = syncStatusResult.value
    if (tasksResult.status === 'fulfilled') runtimeTasks.value = tasksResult.value
    if (logsResult.status === 'fulfilled') syncLogs.value = logsResult.value
    if (summaryResult.status === 'fulfilled') dataSummary.value = summaryResult.value
    if (gridResult.status === 'fulfilled') gridStatus.value = gridResult.value
  } finally {
    loading.value = false
  }
}

function datasetRow(label: string, name: string) {
  const item = summaryMap.value[name]
  return {
    label,
    name: item?.dataset || item?.key || name,
    latest: formatDateTime(item?.latest_datetime || item?.latest_date),
  }
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

function formatDateTime(value?: string | null): string {
  if (!value) return '-'
  return value.replace('T', ' ').slice(0, value.includes(':') ? 16 : 10)
}

const pageContextBlocks = computed(() => [
  {
    title: 'Ops Status',
    rows: [
      { label: '刷新状态', value: loading.value ? '刷新中' : '已就绪', tone: loading.value ? 'warn' : 'good' },
      { label: '活动任务', value: `${activeTaskCount.value} 个`, tone: activeTaskCount.value ? 'warn' : 'good' },
      {
        label: '同步服务',
        value: syncStatus.value ? syncStatusLabel(syncStatus.value.status) : '-',
        tone: syncStatus.value?.status === 'failed' ? 'bad' : syncStatus.value?.status === 'running' ? 'warn' : 'good',
      },
      { label: '运行任务记录', value: `${runtimeTasks.value.length} 条` },
      { label: '下单护栏', value: gridStatus.value?.order_submit_enabled ? '已开启' : '仅信号', tone: gridStatus.value?.order_submit_enabled ? 'bad' : 'good' },
    ],
  },
  {
    title: 'Datasets',
    rows: datasetRows.value.slice(0, 4).map(row => ({
      label: row.label,
      value: row.latest,
      tone: row.latest === '-' ? 'warn' : 'good',
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
}

.ops-hero h2 {
  margin: var(--space-1) 0 var(--space-2);
}

.ops-hero p {
  margin: 0;
  max-width: 760px;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.ops-status-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: var(--space-3);
}

.ops-status-card {
  display: flex;
  min-height: 110px;
  flex-direction: column;
  justify-content: space-between;
  gap: var(--space-2);
  padding: var(--space-4);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-lg);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)), var(--bg-elevated);
  box-shadow: var(--shadow-card);
}

.ops-status-card span,
.ops-status-card small {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.ops-status-card strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 18px;
}

.ops-status-card--good {
  border-color: rgba(34, 197, 94, 0.28);
}

.ops-status-card--warn {
  border-color: rgba(245, 158, 11, 0.34);
}

.ops-status-card--bad {
  border-color: rgba(239, 68, 68, 0.34);
}

.ops-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(360px, 0.75fr);
  gap: var(--space-4);
}

.ops-table {
  margin: 0 var(--space-4) var(--space-4);
  width: calc(100% - var(--space-4) * 2);
}

.dataset-list {
  display: flex;
  flex-direction: column;
  gap: var(--space-2);
  padding: 0 var(--space-4) var(--space-4);
}

.dataset-row {
  display: grid;
  grid-template-columns: 110px minmax(0, 1fr) minmax(120px, 0.8fr);
  align-items: center;
  gap: var(--space-3);
  padding: 10px 12px;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-md);
  background: rgba(10, 14, 20, 0.58);
}

.dataset-row span,
.dataset-row small {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.dataset-row strong {
  color: var(--text-bright);
  font-family: var(--font-data);
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

@media (max-width: 1300px) {
  .ops-status-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 900px) {
  .ops-hero,
  .ops-grid {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 640px) {
  .ops-status-grid,
  .dataset-row {
    grid-template-columns: 1fr;
  }
}
</style>

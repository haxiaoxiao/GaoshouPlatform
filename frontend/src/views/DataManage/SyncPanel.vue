<template>
  <section class="sync-workbench">
    <div class="sync-head">
      <div>
        <div class="eyebrow">DATA SYNC</div>
        <h2>数据同步任务</h2>
      </div>
      <div class="sync-actions">
        <span class="queue-summary" :class="{ active: queue.length > 0 }">待执行 {{ queue.length }} 项</span>
        <el-button :icon="Refresh" :loading="catalogLoading" @click="loadCatalog(true)">刷新目录</el-button>
        <el-button v-if="isRunning" type="danger" plain @click="cancelSync">停止任务</el-button>
        <el-button v-else type="primary" :icon="VideoPlay" :disabled="queue.length === 0" :loading="executing" @click="executeQueue">
          执行队列
        </el-button>
      </div>
    </div>

    <div class="status-strip">
      <div class="status-card">
        <span>当前状态</span>
        <strong>{{ statusLabel(syncStatus.status) }}</strong>
      </div>
      <div class="status-card">
        <span>当前任务</span>
        <strong>{{ syncStatus.sync_type ? syncTypeLabel(syncStatus.sync_type) : '-' }}</strong>
      </div>
      <div class="status-card">
        <span>进度</span>
        <strong>{{ syncStatus.progress_percent.toFixed(1) }}%</strong>
      </div>
      <div class="status-card relay-state" :class="{ missing: catalog && !catalog.relay.configured }">
        <span>Relay Key</span>
        <strong>{{ catalog?.relay.configured ? '已配置' : '未配置' }}</strong>
      </div>
    </div>

    <el-progress
      class="sync-progress"
      :percentage="Math.min(100, Math.max(0, syncStatus.progress_percent || 0))"
      :status="syncStatus.status === 'failed' ? 'exception' : syncStatus.status === 'completed' ? 'success' : undefined"
      :stroke-width="10"
    />

    <div class="layout-grid">
      <section class="panel presets-panel">
        <div class="panel-title">
          <h3>预设方案</h3>
          <span>{{ catalog?.presets.length || 0 }} 个</span>
        </div>
        <div class="preset-list">
          <button
            v-for="preset in catalog?.presets || []"
            :key="preset.name"
            class="preset"
            :class="{ noisy: preset.name === 'relay_text', active: isPresetQueued(preset) }"
            type="button"
            @click="addPreset(preset)"
          >
            <span class="preset-title-row">
              <span>{{ preset.display_name }}</span>
              <strong v-if="presetQueueCount(preset) > 0">{{ presetQueueCount(preset) }}/{{ presetItemCount(preset) }} 已加入</strong>
              <strong v-else>{{ presetItemCount(preset) }} 项</strong>
            </span>
            <small>{{ preset.description }}</small>
          </button>
        </div>
      </section>

      <section class="panel controls-panel">
        <div class="panel-title">
          <h3>执行参数</h3>
          <el-tag size="small" effect="dark">1 req/s 默认</el-tag>
        </div>
        <div class="control-grid">
          <label>
            <span>日期范围</span>
            <el-date-picker
              v-model="dateRange"
              type="daterange"
              value-format="YYYY-MM-DD"
              start-placeholder="开始日期"
              end-placeholder="结束日期"
              unlink-panels
            />
          </label>
          <label>
            <span>股票范围</span>
            <el-segmented v-model="stockScope" :options="stockScopeOptions" />
          </label>
          <label class="wide">
            <span>自定义股票</span>
            <el-input
              v-model="symbolText"
              type="textarea"
              :rows="2"
              placeholder="000001.SZ, 600000.SH"
              :disabled="stockScope === 'all'"
            />
          </label>
          <label>
            <span>失败策略</span>
            <el-select v-model="failureStrategy">
              <el-option label="跳过失败项" value="skip" />
              <el-option label="失败即停止" value="stop" />
              <el-option label="重试后跳过" value="retry" />
            </el-select>
          </label>
          <label>
            <span>Relay 每日限量</span>
            <el-input-number v-model="relayDailyLimit" :min="1" :max="500" :step="20" controls-position="right" />
          </label>
          <label>
            <span>THS 成分上限</span>
            <el-input-number v-model="thsMemberLimit" :min="1" :max="500" :step="10" controls-position="right" />
          </label>
          <label>
            <span>板块资金流 limit</span>
            <el-input-number v-model="blockLimit" :min="1" :max="100" controls-position="right" />
          </label>
        </div>
      </section>
    </div>

    <section class="panel">
      <div class="panel-title">
        <h3>待执行队列</h3>
        <div class="panel-tools">
          <el-button size="small" :icon="Delete" @click="clearQueue">清空</el-button>
        </div>
      </div>
      <el-empty v-if="queue.length === 0" description="队列为空" :image-size="80" />
      <div v-else class="queue-list">
        <div v-for="item in queue" :key="item.id" class="queue-item">
          <div>
            <strong>{{ item.display_name }}</strong>
            <span>{{ item.kind === 'relay' ? 'Tushare Relay' : syncTypeLabel(item.name) }}</span>
          </div>
          <el-tag size="small" :type="riskType(item.risk_level)" effect="dark" class="risk-tag" :class="`risk-tag--${item.risk_level}`">
            {{ riskLabel(item.risk_level) }}
          </el-tag>
          <el-button text :icon="Delete" @click="removeQueueItem(item.id)" />
        </div>
      </div>
    </section>

    <section class="panel catalog-panel">
      <div class="panel-title">
        <h3>数据任务目录</h3>
        <div class="panel-tools">
          <el-input v-model="keyword" clearable placeholder="搜索数据集" />
          <el-select v-model="categoryFilter" placeholder="分类">
            <el-option label="全部" value="all" />
            <el-option label="核心" value="core" />
            <el-option label="行情" value="market" />
            <el-option label="Relay 结构化" value="relay_structured" />
            <el-option label="新闻公告" value="relay_text" />
          </el-select>
        </div>
      </div>

      <el-table :data="filteredDatasets" height="420" class="dark-table" row-key="name">
        <el-table-column label="任务" min-width="220">
          <template #default="{ row }">
            <div class="dataset-name">
              <strong>{{ row.display_name }}</strong>
              <span>{{ row.name }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="source" label="来源" width="150" show-overflow-tooltip />
        <el-table-column label="覆盖度" min-width="170">
          <template #default="{ row }">
            <span v-if="hasCoverageRows(row.coverage)">
              {{ formatNumber(coverageRows(row.coverage)) }} 行
              <small>{{ row.coverage.max_date || '-' }}</small>
            </span>
            <span v-else-if="row.coverage?.max_date">
              <small>{{ row.coverage.max_date }}{{ row.coverage.estimated ? ' · 快速估算' : '' }}</small>
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column label="频率" width="100">
          <template #default="{ row }">{{ frequencyLabel(row.recommended_frequency) }}</template>
        </el-table-column>
        <el-table-column label="依赖" width="120">
          <template #default="{ row }">
            <el-tag v-if="row.requires_qmt" size="small" effect="dark">QMT</el-tag>
            <el-tag v-if="row.requires_relay_key" size="small" effect="dark" type="success">Relay</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="风险" width="132">
          <template #default="{ row }">
            <el-tag :type="riskType(row.risk_level)" effect="dark" size="small" class="risk-tag" :class="`risk-tag--${row.risk_level}`">
              {{ riskLabel(row.risk_level) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="" width="96">
          <template #default="{ row }">
            <el-button size="small" :icon="Plus" @click="addTask(row)">加入</el-button>
          </template>
        </el-table-column>
        <el-table-column prop="description" label="说明" min-width="240" show-overflow-tooltip />
      </el-table>
    </section>

    <section class="panel">
      <div class="panel-title">
        <h3>最近记录</h3>
        <el-button size="small" :icon="Refresh" @click="loadLogs">刷新</el-button>
      </div>
      <el-table :data="logs" class="dark-table" height="220">
        <el-table-column label="类型" width="150">
          <template #default="{ row }">{{ syncTypeLabel(row.sync_type) }}</template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="110" />
        <el-table-column label="成功/失败" width="130">
          <template #default="{ row }">{{ row.success_count ?? 0 }} / {{ row.failed_count ?? 0 }}</template>
        </el-table-column>
        <el-table-column prop="start_time" label="开始时间" width="190" />
        <el-table-column prop="error_message" label="错误" show-overflow-tooltip />
      </el-table>
    </section>

    <el-alert
      v-if="catalog && !catalog.relay.configured"
      class="relay-warning"
      type="warning"
      :closable="false"
      show-icon
    >
      <template #title>未检测到 INDEVS_TUSHARE_API_KEY，Relay 任务会被后端拒绝。</template>
    </el-alert>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Delete, Plus, Refresh, VideoPlay } from '@element-plus/icons-vue'
import { syncApi, type SyncCatalog, type SyncCatalogItem, type SyncLog, type SyncPreset, type SyncStatus } from '@/api/sync'

interface QueueItem {
  id: string
  name: string
  display_name: string
  kind: 'core' | 'relay'
  risk_level: string
  text_source?: boolean
}

const today = new Date()
const weekAgo = new Date(today)
weekAgo.setDate(today.getDate() - 7)

const catalog = ref<SyncCatalog | null>(null)
const logs = ref<SyncLog[]>([])
const queue = ref<QueueItem[]>([])
const keyword = ref('')
const categoryFilter = ref('all')
const catalogLoading = ref(false)
const executing = ref(false)
const stockScope = ref<'custom' | 'all'>('custom')
const symbolText = ref('000001.SZ')
const dateRange = ref<[string, string]>([formatDate(weekAgo), formatDate(today)])
const failureStrategy = ref<'skip' | 'retry' | 'stop'>('skip')
const relayDailyLimit = ref(200)
const thsMemberLimit = ref(50)
const blockLimit = ref(5)
const syncStatus = ref<SyncStatus>(idleStatus())
const stockScopeOptions = [
  { label: '自定义', value: 'custom' },
  { label: '全市场', value: 'all' },
]
const CATALOG_CACHE_MS = 5 * 60 * 1000
const LOG_REFRESH_INTERVAL_MS = 15 * 1000
let catalogCache: { value: SyncCatalog; expiresAt: number } | null = null
let pollTimer: number | undefined
let lastLogRefreshAt = 0

const isRunning = computed(() => ['queued', 'running'].includes(syncStatus.value.status))
const queuedNames = computed(() => new Set(queue.value.map((item) => item.name)))
const filteredDatasets = computed(() => {
  const term = keyword.value.trim().toLowerCase()
  return (catalog.value?.datasets || []).filter((item) => {
    const matchesCategory = categoryFilter.value === 'all' || item.category === categoryFilter.value
    const text = `${item.name} ${item.display_name} ${item.description}`.toLowerCase()
    return matchesCategory && (!term || text.includes(term))
  })
})

onMounted(async () => {
  await Promise.all([loadCatalog(false), refreshStatus(), loadLogs()])
  startPolling()
})

onBeforeUnmount(() => {
  if (pollTimer) window.clearInterval(pollTimer)
})

async function loadCatalog(force = false) {
  const now = Date.now()
  if (!force && catalogCache && now < catalogCache.expiresAt) {
    catalog.value = catalogCache.value
    return
  }
  catalogLoading.value = true
  try {
    const next = await syncApi.getCatalog({ refresh: force })
    catalog.value = next
    catalogCache = { value: next, expiresAt: Date.now() + CATALOG_CACHE_MS }
  } finally {
    catalogLoading.value = false
  }
}

async function refreshStatus() {
  syncStatus.value = await syncApi.getStatus()
}

async function loadLogs() {
  logs.value = await syncApi.getLogs({ limit: 20 })
  lastLogRefreshAt = Date.now()
}

function hasCoverageRows(coverage: SyncCatalogItem['coverage']) {
  return typeof coverage?.row_count === 'number' && coverage.row_count > 0
}

function coverageRows(coverage: SyncCatalogItem['coverage']) {
  return typeof coverage?.row_count === 'number' ? coverage.row_count : 0
}

function startPolling() {
  pollTimer = window.setInterval(async () => {
    const wasRunning = isRunning.value
    await refreshStatus()
    const shouldRefreshLogs =
      (wasRunning && !isRunning.value) || Date.now() - lastLogRefreshAt >= LOG_REFRESH_INTERVAL_MS
    if (shouldRefreshLogs) await loadLogs()
  }, 2500)
}

function addPreset(preset: SyncPreset) {
  const beforeCount = queue.value.length
  const datasets = catalog.value?.datasets || []
  for (const type of preset.sync_types) {
    const item = datasets.find((entry) => entry.name === type)
    if (item) addTask(item, false)
  }
  for (const name of preset.relay_datasets) {
    const item = datasets.find((entry) => entry.name === name)
    if (item) addTask(item, false)
  }
  dedupeQueue()
  const addedCount = queue.value.length - beforeCount
  if (addedCount > 0) {
    ElMessage.success(`已加入 ${addedCount} 个任务，当前队列 ${queue.value.length} 项`)
  } else {
    ElMessage.info('该预设的任务已经在待执行队列中')
  }
}

function presetNames(preset: SyncPreset) {
  return [...preset.sync_types, ...preset.relay_datasets]
}

function presetItemCount(preset: SyncPreset) {
  return presetNames(preset).length
}

function presetQueueCount(preset: SyncPreset) {
  return presetNames(preset).filter((name) => queuedNames.value.has(name)).length
}

function isPresetQueued(preset: SyncPreset) {
  const total = presetItemCount(preset)
  return total > 0 && presetQueueCount(preset) === total
}

function addTask(item: SyncCatalogItem, dedupe = true) {
  const kind = item.requires_relay_key || item.category.startsWith('relay') ? 'relay' : 'core'
  queue.value.push({
    id: `${kind}:${item.name}:${Date.now()}:${Math.random().toString(16).slice(2)}`,
    name: item.name,
    display_name: item.display_name,
    kind,
    risk_level: item.risk_level,
    text_source: item.text_source,
  })
  if (dedupe) dedupeQueue()
}

function dedupeQueue() {
  const seen = new Set<string>()
  queue.value = queue.value.filter((item) => {
    const key = `${item.kind}:${item.name}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

function removeQueueItem(id: string) {
  queue.value = queue.value.filter((item) => item.id !== id)
}

function clearQueue() {
  queue.value = []
}

async function executeQueue() {
  if (queue.value.length === 0 || executing.value) return
  executing.value = true
  try {
    const coreItems = queue.value.filter((item) => item.kind === 'core')
    const relayItems = queue.value.filter((item) => item.kind === 'relay')
    for (const item of coreItems) {
      if (item.name === 'factor_dependency') {
        ElMessage.warning('因子依赖同步请从因子看板的预计算流程触发')
        continue
      }
      await syncApi.trigger({
        sync_type: item.name as any,
        ...basePayload(),
      })
      await waitUntilIdle()
    }
    if (relayItems.length) {
      await syncApi.trigger({
        sync_type: 'tushare_relay',
        ...basePayload(),
        relay_datasets: relayItems.map((item) => item.name),
        relay_options: relayOptions(relayItems),
      })
      await waitUntilIdle()
    }
    queue.value = []
    await Promise.all([refreshStatus(), loadLogs(), loadCatalog(true)])
  } catch (error: any) {
    ElMessage.error(error?.message || '同步任务提交失败')
  } finally {
    executing.value = false
  }
}

function basePayload() {
  const [start, end] = dateRange.value
  return {
    start_date: start,
    end_date: end,
    symbols: stockScope.value === 'custom' ? parseSymbols(symbolText.value) : undefined,
    failure_strategy: failureStrategy.value,
    full_sync: false,
  }
}

function relayOptions(items: QueueItem[]) {
  return {
    allow_all_symbols: stockScope.value === 'all',
    allow_text_sources: items.some((item) => item.text_source),
    daily_limit: relayDailyLimit.value,
    block_moneyflow_limit: blockLimit.value,
    ths_member_limit: thsMemberLimit.value,
    rps: catalog.value?.relay.rps || 1,
    timeout_seconds: catalog.value?.relay.timeout_seconds || 30,
  }
}

async function waitUntilIdle() {
  for (let i = 0; i < 720; i += 1) {
    await sleep(2500)
    await refreshStatus()
    if (!isRunning.value) {
      if (syncStatus.value.status === 'failed') throw new Error(syncStatus.value.error_message || '同步失败')
      return
    }
  }
  throw new Error('同步任务等待超时')
}

async function cancelSync() {
  await syncApi.cancel()
  await refreshStatus()
}

function parseSymbols(text: string) {
  return text
    .split(/[\s,，;；]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean)
}

function syncTypeLabel(type: string) {
  const map: Record<string, string> = {
    datasync: '一键同步',
    stock_info: '股票基础',
    stock_full: '股票完整',
    financial_data: '财务数据',
    kline_daily: '日 K',
    index_daily: '指数日线',
    kline_minute: '分钟 K',
    realtime_mv: '实时市值',
    dividends: 'QMT 分红',
    factor_dependency: '因子依赖',
    tushare_relay: 'Tushare Relay',
  }
  return map[type] || type
}

function statusLabel(status: string) {
  const map: Record<string, string> = {
    idle: '空闲',
    queued: '排队',
    running: '运行中',
    completed: '完成',
    failed: '失败',
    cancelled: '已取消',
  }
  return map[status] || status
}

function frequencyLabel(value: string) {
  const map: Record<string, string> = {
    daily: '每日',
    weekly: '每周',
    manual: '手动',
    on_demand: '按需',
  }
  return map[value] || value
}

function riskLabel(value: string) {
  const map: Record<string, string> = {
    low: '低风险',
    medium: '中风险',
    high: '高噪声',
  }
  return map[value] || value
}

function riskType(value: string) {
  if (value === 'high') return 'danger'
  if (value === 'medium') return 'warning'
  return 'success'
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('zh-CN').format(value || 0)
}

function formatDate(value: Date) {
  const year = value.getFullYear()
  const month = `${value.getMonth() + 1}`.padStart(2, '0')
  const day = `${value.getDate()}`.padStart(2, '0')
  return `${year}-${month}-${day}`
}

function idleStatus(): SyncStatus {
  return {
    sync_type: null,
    status: 'idle',
    total: 0,
    current: 0,
    success_count: 0,
    failed_count: 0,
    progress_percent: 0,
    start_time: null,
    end_time: null,
    error_message: null,
    details: {},
  }
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms))
}
</script>

<style scoped>
.sync-workbench {
  display: flex;
  flex-direction: column;
  gap: 14px;
  color: var(--el-text-color-primary);
}

.sync-head,
.panel-title,
.status-strip,
.queue-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.eyebrow {
  color: #22c7f4;
  font-family: var(--font-mono, ui-monospace, SFMono-Regular, Consolas, monospace);
  font-size: 12px;
  letter-spacing: 0;
}

h2,
h3 {
  margin: 0;
}

h2 {
  font-size: 22px;
}

h3 {
  font-size: 15px;
}

.sync-actions,
.panel-tools {
  display: flex;
  align-items: center;
  gap: 8px;
}

.queue-summary {
  min-width: 94px;
  padding: 7px 10px;
  border: 1px solid rgba(132, 154, 184, 0.34);
  border-radius: 6px;
  color: #b8c4d6;
  background: rgba(7, 12, 19, 0.74);
  font-size: 12px;
  font-weight: 700;
  text-align: center;
}

.queue-summary.active {
  border-color: rgba(57, 197, 255, 0.86);
  color: #07111c;
  background: #44c8ff;
  box-shadow: 0 0 18px rgba(68, 200, 255, 0.2);
}

.status-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.status-card,
.panel {
  border: 1px solid rgba(126, 151, 181, 0.42);
  background: linear-gradient(180deg, rgba(17, 27, 39, 0.98), rgba(8, 13, 20, 0.98));
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
}

.status-card {
  min-height: 68px;
  padding: 12px 14px;
  border-radius: 6px;
}

.status-card span,
.queue-item span,
.dataset-name span,
.panel-title > span,
.preset small,
label > span,
small {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.status-card strong {
  display: block;
  margin-top: 8px;
  color: #f3f7fb;
  font-size: 17px;
}

.relay-state.missing strong {
  color: #ffd166;
}

.sync-progress {
  --el-fill-color-light: rgba(255, 255, 255, 0.08);
}

:deep(.sync-progress .el-progress-bar__outer) {
  background: #101720 !important;
  border-color: rgba(126, 151, 181, 0.36);
}

:deep(.el-segmented) {
  width: 100%;
}

.layout-grid {
  display: grid;
  grid-template-columns: 340px minmax(0, 1fr);
  gap: 14px;
}

.panel {
  padding: 14px;
  border-radius: 8px;
}

.preset-list,
.queue-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 12px;
}

.preset {
  display: flex;
  min-height: 70px;
  flex-direction: column;
  align-items: flex-start;
  justify-content: center;
  gap: 6px;
  padding: 12px;
  border: 1px solid rgba(110, 139, 170, 0.58);
  border-radius: 6px;
  color: #f5f8fc;
  background: rgba(9, 16, 25, 0.9);
  cursor: pointer;
  text-align: left;
}

.preset-title-row {
  display: flex;
  width: 100%;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.preset-title-row > span {
  color: #f7fbff;
  font-weight: 800;
}

.preset-title-row > strong {
  flex: 0 0 auto;
  padding: 2px 7px;
  border: 1px solid rgba(132, 154, 184, 0.35);
  border-radius: 999px;
  color: #c7d3e4;
  background: rgba(16, 23, 32, 0.9);
  font-size: 11px;
  font-weight: 800;
}

.preset:hover {
  border-color: rgba(72, 205, 248, 0.9);
  background: rgba(18, 43, 59, 0.92);
}

.preset.active {
  border-color: rgba(68, 200, 255, 0.98);
  background: linear-gradient(180deg, rgba(21, 62, 83, 0.96), rgba(10, 27, 40, 0.96));
  box-shadow: inset 3px 0 0 #44c8ff, 0 0 20px rgba(68, 200, 255, 0.12);
}

.preset.active .preset-title-row > strong {
  border-color: rgba(68, 200, 255, 0.95);
  color: #07111c;
  background: #44c8ff;
}

.preset.noisy {
  border-color: rgba(214, 143, 47, 0.58);
}

.control-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin-top: 12px;
}

label {
  display: flex;
  flex-direction: column;
  gap: 7px;
  min-width: 0;
}

label.wide {
  grid-column: span 2;
}

.queue-item {
  min-height: 54px;
  padding: 10px 12px;
  border: 1px solid rgba(110, 139, 170, 0.54);
  border-radius: 6px;
  background: rgba(9, 16, 25, 0.9);
}

.queue-item > div:first-child,
.dataset-name {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 4px;
}

.catalog-panel {
  padding-bottom: 8px;
}

.panel-tools .el-input {
  width: 220px;
}

.panel-tools .el-select {
  width: 150px;
}

.relay-warning {
  border-radius: 6px;
}

:deep(.el-table.dark-table) {
  --el-table-bg-color: #0b1018;
  --el-table-tr-bg-color: #0f1823;
  --el-table-header-bg-color: #121d2a;
  --el-table-row-hover-bg-color: #1b3142;
  --el-table-border-color: rgba(119, 146, 176, 0.46);
  --el-table-text-color: #edf3fa;
  --el-table-header-text-color: #c9d5e6;
  background: #0b1018;
  border-radius: 6px;
  overflow: hidden;
}

:deep(.el-table.dark-table .el-table__cell) {
  color: #f2f7fd;
}

:deep(.el-table.dark-table th.el-table__cell) {
  color: #dbe7f5;
  background: #152130 !important;
}

:deep(.el-table.dark-table .dataset-name strong) {
  color: #ffffff;
  font-weight: 800;
}

:deep(.el-table.dark-table .dataset-name span),
:deep(.el-table.dark-table small) {
  color: #b9c7d8;
}

.risk-tag {
  min-width: 72px;
  justify-content: center;
  border-radius: 4px;
  font-weight: 800;
  letter-spacing: 0;
}

.risk-tag--low {
  border-color: #54d66a !important;
  color: #e8ffec !important;
  background: #176326 !important;
}

.risk-tag--medium {
  border-color: #ffbc42 !important;
  color: #fff1c2 !important;
  background: #8a590e !important;
}

.risk-tag--high {
  border-color: #ff6b7a !important;
  color: #ffe6e9 !important;
  background: #7a1f2a !important;
}

:deep(.el-table__inner-wrapper::before),
:deep(.el-table__border-left-patch) {
  background: rgba(119, 146, 176, 0.46);
}

:deep(.el-empty__description p) {
  color: var(--el-text-color-secondary);
}

:deep(.el-input__wrapper),
:deep(.el-textarea__inner),
:deep(.el-select__wrapper),
:deep(.el-input-number__decrease),
:deep(.el-input-number__increase) {
  background: rgba(8, 13, 20, 0.86);
  border-color: rgba(118, 145, 176, 0.6);
  box-shadow: 0 0 0 1px rgba(118, 145, 176, 0.6) inset;
}

:deep(.el-input__inner),
:deep(.el-textarea__inner) {
  color: #edf3fa;
}

@media (max-width: 1180px) {
  .layout-grid,
  .status-strip,
  .control-grid {
    grid-template-columns: 1fr;
  }

  label.wide {
    grid-column: auto;
  }
}
</style>

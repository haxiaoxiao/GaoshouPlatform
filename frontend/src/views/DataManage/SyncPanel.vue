<template>
  <section class="sync-workbench">
    <div class="sync-head">
      <div>
        <div class="eyebrow">DATA SYNC</div>
        <h2>数据同步任务</h2>
      </div>
      <div class="sync-actions">
        <span class="queue-summary" :class="{ active: queue.length > 0 || queuePendingCount > 0 || isRunning }">
          草稿 {{ queue.length }} · 后端 {{ queuePendingCount }} · 运行 {{ isRunning ? 1 : 0 }}
        </span>
        <el-button :icon="Refresh" :loading="catalogLoading" @click="loadCatalog(true)">刷新目录</el-button>
        <el-button v-if="isRunning" type="danger" plain @click="cancelSync">停止任务</el-button>
        <el-button type="primary" :icon="VideoPlay" :disabled="queue.length === 0 || !canTriggerSync" :loading="executing" @click="executeQueue">
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
      <div class="status-card status-card--wide">
        <span>当前数据</span>
        <strong>{{ currentWorkLabel }}</strong>
        <small v-if="currentCursorLabel !== '-'">{{ currentCursorLabel }}</small>
      </div>
      <div class="status-card">
        <span>执行单元</span>
        <strong>{{ unitProgressLabel }}</strong>
        <small>{{ remainingUnitLabel }}</small>
      </div>
      <div class="status-card">
        <span>成功 / 失败</span>
        <strong>{{ syncStatus.success_count }} / {{ syncStatus.failed_count }}</strong>
        <small>{{ datasetProgressLabel }}</small>
      </div>
      <div class="status-card relay-state" :class="{ missing: catalog && !catalog.relay.configured }">
        <span>Relay Key</span>
        <strong>{{ catalog?.relay.configured ? '已配置' : '未配置' }}</strong>
        <small>{{ rowsWrittenLabel }}</small>
      </div>
    </div>

    <el-progress
      class="sync-progress"
      :percentage="Math.min(100, Math.max(0, syncStatus.progress_percent || 0))"
      :status="syncStatus.status === 'failed' ? 'exception' : syncStatus.status === 'completed' ? 'success' : undefined"
      :stroke-width="10"
    />

    <el-alert
      v-if="!canTriggerSync"
      class="sync-service-warning"
      type="warning"
      :closable="false"
      show-icon
    >
      <template #title>{{ syncUnavailableReason }}</template>
    </el-alert>

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
            <span>同步模式</span>
            <el-segmented v-model="syncMode" :options="syncModeOptions" />
          </label>
          <label>
            <span>{{ syncMode === 'incremental' ? '截止日期' : '日期范围' }}</span>
            <el-date-picker
              v-if="syncMode === 'incremental'"
              v-model="incrementalEndDate"
              type="date"
              value-format="YYYY-MM-DD"
              placeholder="截止日期"
            />
            <el-date-picker
              v-else
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
        <div class="queue-metrics">
          <span>运行中 {{ isRunning ? 1 : 0 }} 项</span>
          <span>后端排队 {{ queuePendingCount }} 项</span>
          <span>草稿 {{ queue.length }} 项</span>
        </div>
        <div class="panel-tools">
          <el-button size="small" :icon="Delete" :disabled="queue.length === 0" @click="clearQueue">清空草稿</el-button>
        </div>
      </div>
      <el-empty v-if="queuePipelineItems.length === 0" description="队列为空" :image-size="80" />
      <div v-else class="queue-lanes">
        <div v-if="queuePipeline.running" class="queue-lane">
          <div class="queue-lane-head">
            <span>运行中</span>
            <em>后端正在执行，当前任务会自动前移</em>
          </div>
            <div class="queue-card queue-card--running">
              <div class="queue-step">
              <span class="queue-step-pulse"></span>
              </div>
            <div class="queue-copy">
              <strong>{{ queuePipeline.running.display_name }}</strong>
              <span>{{ queuePipeline.running.subtitle }}</span>
              <small v-if="queuePipeline.running.detail">{{ queuePipeline.running.detail }}</small>
            </div>
            <el-tag size="small" effect="dark" type="info">运行中</el-tag>
          </div>
        </div>

        <div v-if="queuePipeline.pending.length" class="queue-lane">
          <div class="queue-lane-head">
            <span>后端排队</span>
            <em>{{ queuePipeline.pending.length }} 项</em>
          </div>
          <div class="queue-stack">
            <div
              v-for="item in queuePipeline.pending"
              :key="item.id"
              class="queue-card queue-card--pending"
            >
              <div class="queue-step">
                <span>{{ item.order }}</span>
              </div>
              <div class="queue-copy">
                <strong>{{ item.display_name }}</strong>
                <span>{{ item.subtitle }}</span>
                <small v-if="item.detail">{{ item.detail }}</small>
              </div>
              <div class="queue-status-group">
                <el-tag size="small" effect="dark" type="warning">排队中</el-tag>
              </div>
            </div>
          </div>
        </div>

        <div v-if="queuePipeline.draft.length" class="queue-lane">
          <div class="queue-lane-head">
            <span>本地草稿</span>
            <em>尚未提交后端</em>
          </div>
          <div class="queue-stack">
            <div
              v-for="item in queuePipeline.draft"
              :key="item.id"
              class="queue-card queue-card--draft"
            >
              <div class="queue-step">
                <span>{{ item.order }}</span>
              </div>
              <div class="queue-copy">
                <strong>{{ item.display_name }}</strong>
                <span>{{ item.subtitle }}</span>
                <small v-if="item.detail">{{ item.detail }}</small>
              </div>
              <div class="queue-status-group">
                <el-tag size="small" effect="dark" :type="riskType(item.risk_level)" class="risk-tag" :class="`risk-tag--${item.risk_level}`">
                  {{ riskLabel(item.risk_level) }}
                </el-tag>
                <el-button text :icon="Delete" @click="removeQueueItem(item.id)" />
              </div>
            </div>
          </div>
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
            <el-option label="概念" value="concept" />
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
import { usePageContext } from '@/app/pageContext'
import { syncApi, type SyncCatalog, type SyncCatalogItem, type SyncLog, type SyncPreset, type SyncStatus } from '@/api/sync'

const emit = defineEmits<{
  'status-change': [status: SyncStatus]
}>()

interface QueueItem {
  id: string
  name: string
  display_name: string
  kind: 'core' | 'relay'
  risk_level: string
  text_source?: boolean
}

interface QueuePipelineItem {
  id: string
  display_name: string
  subtitle: string
  detail: string
  state: 'running' | 'pending' | 'draft'
  kind: 'core' | 'relay'
  risk_level: string
  order: number
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
const incrementalEndDate = ref(formatDate(today))
const syncMode = ref<'incremental' | 'range' | 'full'>('incremental')
const failureStrategy = ref<'skip' | 'retry' | 'stop'>('skip')
const relayDailyLimit = ref(200)
const thsMemberLimit = ref(50)
const blockLimit = ref(5)
const syncStatus = ref<SyncStatus>(idleStatus())
const syncModeOptions = [
  { label: '增量', value: 'incremental' },
  { label: '指定区间', value: 'range' },
  { label: '覆盖重刷', value: 'full' },
]
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
const queuePendingCount = computed(() => Number(syncStatus.value.details?.queue_pending_count || 0))
const queueModeEnabled = computed(() => syncStatus.value.details?.queue_mode === true)
const serviceReady = computed(() => (
  syncStatus.value.sync_service_available !== false
  && syncStatus.value.details?.sync_service_unavailable !== true
))
const backendAcceptsSubmission = computed(() => (
  serviceReady.value
  && syncStatus.value.can_trigger !== false
))
const canTriggerSync = computed(() => (
  backendAcceptsSubmission.value
  && (!isRunning.value || queueModeEnabled.value)
))
const activeTaskLabel = computed(() => (
  syncStatus.value.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : '-'
))
const syncUnavailableReason = computed(() => (
  !serviceReady.value
    ? syncStatus.value.reason
      || String(syncStatus.value.details?.proxy_error || '')
      || 'dev 同步服务未启动或状态接口不可用，请先启动 18810 同步服务。'
    : syncStatus.value.can_trigger === false
      ? syncStatus.value.reason || '后端当前拒绝新同步任务。'
      : isRunning.value && !queueModeEnabled.value
        ? `当前正在${syncStatus.value.status === 'queued' ? '排队' : '执行'}：${activeTaskLabel.value}，请等待完成或停止后再提交。`
        : '队列可接受新任务'
))
const submissionStateLabel = computed(() => {
  if (!serviceReady.value) return '服务不可用'
  if (syncStatus.value.can_trigger === false) return '不可提交'
  if (isRunning.value && queueModeEnabled.value) return '运行中，可排队'
  if (isRunning.value) return '运行中'
  return '可提交'
})
const submissionStateTone = computed(() => {
  if (!serviceReady.value || syncStatus.value.can_trigger === false) return 'bad'
  if (isRunning.value) return 'warn'
  return 'good'
})
const executionHint = computed(() => {
  if (!serviceReady.value || syncStatus.value.can_trigger === false || (isRunning.value && !queueModeEnabled.value)) {
    return syncUnavailableReason.value
  }
  if (isRunning.value) {
    return `当前执行：${activeTaskLabel.value}；后端排队 ${queuePendingCount.value} 项。`
  }
  return '选择预设或任务后提交到后端队列。'
})
const queuedNames = computed(() => new Set(queue.value.map((item) => item.name)))
const filteredDatasets = computed(() => {
  const term = keyword.value.trim().toLowerCase()
  return (catalog.value?.datasets || []).filter((item) => {
    const matchesCategory = categoryFilter.value === 'all' || item.category === categoryFilter.value
    const text = `${item.name} ${item.display_name} ${item.description}`.toLowerCase()
    return matchesCategory && (!term || text.includes(term))
  })
})
const catalogDatasetMap = computed(() => new Map((catalog.value?.datasets || []).map((item) => [item.name, item])))
const syncDetails = computed(() => syncStatus.value.details || {})
const backendQueueActiveTask = computed(() => recordValue(syncDetails.value.queue_active_task))
const backendQueuePendingTasks = computed(() => recordList(syncDetails.value.queue_pending_tasks))
const backendRunningQueueItem = computed(() => {
  if (Object.keys(backendQueueActiveTask.value).length > 0) {
    return backendTaskToPipelineItem(backendQueueActiveTask.value, 'running', 1)
  }
  if (!isRunning.value || !syncStatus.value.sync_type) return null
  return {
    id: `active:${syncStatus.value.sync_type}`,
    display_name: activeTaskLabel.value,
    subtitle: '后端正在执行',
    detail: currentWorkLabel.value !== activeTaskLabel.value ? currentWorkLabel.value : '',
    state: 'running' as const,
    kind: syncStatus.value.sync_type === 'tushare_relay' ? 'relay' as const : 'core' as const,
    risk_level: 'medium',
    order: 1,
  }
})
const backendPendingQueueItems = computed(() => (
  backendQueuePendingTasks.value.map((task, index) => backendTaskToPipelineItem(task, 'pending', index + 1))
))
const draftQueueItems = computed(() => (
  queue.value.map((item, index) => ({
    id: item.id,
    display_name: item.display_name,
    subtitle: item.kind === 'relay' ? '等待提交 · Tushare Relay' : `等待提交 · ${syncTypeLabel(item.name)}`,
    detail: item.name,
    state: 'draft' as const,
    kind: item.kind,
    risk_level: item.risk_level,
    order: index + 1,
  }))
))
const queuePipeline = computed(() => ({
  running: backendRunningQueueItem.value,
  pending: backendPendingQueueItems.value,
  draft: draftQueueItems.value,
}))
const queuePipelineItems = computed(() => [
  ...(backendRunningQueueItem.value ? [backendRunningQueueItem.value] : []),
  ...backendPendingQueueItems.value,
  ...draftQueueItems.value,
])
const syncPlanSteps = computed(() => {
  const plan = recordValue(syncDetails.value.plan)
  const steps = plan.steps
  return Array.isArray(steps) ? stringArray(steps) : []
})
const stepResults = computed(() => {
  const results = syncDetails.value.step_results
  return Array.isArray(results) ? results.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === 'object') : []
})
const relayDatasetNames = computed(() => stringArray(syncDetails.value.relay_datasets))
const datasetResults = computed(() => recordValue(syncDetails.value.datasets))
const completedDatasetNames = computed(() => Object.keys(datasetResults.value))
const currentDatasetName = computed(() => stringValue(syncDetails.value.current_dataset))
const currentDatasetDisplay = computed(() => {
  const displayName = stringValue(syncDetails.value.current_dataset_display_name)
  if (displayName) return displayName
  return currentDatasetName.value ? datasetLabel(currentDatasetName.value) : ''
})
const datasetTotal = computed(() => {
  const explicitTotal = numberValue(syncDetails.value.dataset_total)
  if (explicitTotal !== null) return explicitTotal
  if (syncPlanSteps.value.length) return syncPlanSteps.value.length
  return relayDatasetNames.value.length
})
const datasetCompletedCount = computed(() => {
  const explicitCompleted = numberValue(syncDetails.value.dataset_completed)
  if (explicitCompleted !== null) return explicitCompleted
  if (syncPlanSteps.value.length) return stepResults.value.length
  return completedDatasetNames.value.length
})
const currentDatasetIndex = computed(() => {
  const explicitIndex = numberValue(syncDetails.value.current_dataset_index)
  if (explicitIndex !== null) return explicitIndex
  const currentStep = stringValue(syncDetails.value.current_step)
  if (currentStep && syncPlanSteps.value.length) {
    const index = syncPlanSteps.value.indexOf(currentStep)
    if (index >= 0) return index + 1
  }
  if (currentDatasetName.value && relayDatasetNames.value.length) {
    const index = relayDatasetNames.value.indexOf(currentDatasetName.value)
    if (index >= 0) return index + 1
  }
  if (isRunning.value && currentDatasetName.value) return datasetCompletedCount.value + 1
  if (isRunning.value && stringValue(syncDetails.value.current_step)) return datasetCompletedCount.value + 1
  return datasetCompletedCount.value
})
const datasetRemainingCount = computed(() => {
  const explicitRemaining = numberValue(syncDetails.value.dataset_remaining)
  if (explicitRemaining !== null) return explicitRemaining
  if (!datasetTotal.value) return 0
  return Math.max(0, datasetTotal.value - datasetCompletedCount.value)
})
const currentBatchLabel = computed(() => {
  const batch = recordValue(syncDetails.value.current_batch)
  const from = stringValue(batch.from)
  const to = stringValue(batch.to)
  const size = numberValue(batch.size)
  if (!from && !to && size === null) return ''
  const range = from || to ? `${from || '?'} 至 ${to || '?'}` : ''
  return [range, size !== null ? `${formatNumber(size)} 只` : ''].filter(Boolean).join(' · ')
})
const currentCursorLabel = computed(() => {
  const parts = [
    stringValue(syncDetails.value.current_symbol),
    stringValue(syncDetails.value.current_ths_code) ? `THS ${stringValue(syncDetails.value.current_ths_code)}` : '',
    stringValue(syncDetails.value.current_date),
    stringValue(syncDetails.value.download_batch) ? `下载批次 ${stringValue(syncDetails.value.download_batch)}` : '',
    currentBatchLabel.value ? `批次 ${currentBatchLabel.value}` : '',
  ].filter(Boolean)
  return parts.length ? parts.join(' · ') : '-'
})
const currentWorkLabel = computed(() => {
  if (currentDatasetDisplay.value) return currentDatasetDisplay.value
  const displayName = stringValue(syncDetails.value.current_display_name)
  if (displayName) return displayName
  const currentStep = stringValue(syncDetails.value.current_step)
  if (currentStep) return syncTypeLabel(currentStep)
  const postSyncStep = stringValue(syncDetails.value.post_sync_step)
  if (postSyncStep) return postSyncLabel(postSyncStep)
  const phase = stringValue(syncDetails.value.phase)
  if (phase) return phaseLabel(phase)
  return activeTaskLabel.value
})
const unitProgressLabel = computed(() => {
  const total = syncStatus.value.total || 0
  if (total > 0) return `${formatNumber(syncStatus.value.current || 0)} / ${formatNumber(total)}`
  return `${(syncStatus.value.progress_percent || 0).toFixed(1)}%`
})
const remainingUnitLabel = computed(() => {
  const total = syncStatus.value.total || 0
  const percent = `${(syncStatus.value.progress_percent || 0).toFixed(1)}%`
  if (total <= 0) return `${percent} · 等待后端估算`
  return `${percent} · 剩余 ${formatNumber(Math.max(0, total - (syncStatus.value.current || 0)))} 个`
})
const datasetProgressLabel = computed(() => {
  if (datasetTotal.value > 0) {
    return `数据类 ${Math.min(currentDatasetIndex.value, datasetTotal.value)}/${datasetTotal.value} · 待完成 ${datasetRemainingCount.value}`
  }
  return `目录 ${catalog.value?.datasets.length || 0} 类`
})
const rowsWrittenCount = computed(() => {
  for (const key of ['total_klines', 'total_rows', 'yield_rows', 'rows_written']) {
    const value = numberValue(syncDetails.value[key])
    if (value !== null) return value
  }
  let total = 0
  for (const result of Object.values(datasetResults.value)) {
    const rows = numberValue(recordValue(result).rows_written)
    if (rows !== null) total += rows
  }
  return total > 0 ? total : null
})
const rowsWrittenLabel = computed(() => (
  rowsWrittenCount.value !== null ? `写入 ${formatNumber(rowsWrittenCount.value)} 行` : '写入行数待回传'
))
const queuedPreview = computed(() => {
  const items = queuePipelineItems.value
  if (!items.length) return '队列为空'
  const names = items.slice(0, 4).map((item) => `${queueStateLabel(item.state)} ${item.display_name}`)
  return items.length > 4 ? `${names.join('、')} 等 ${items.length} 项` : names.join('、')
})
const pageContextBlocks = computed(() => [
  {
    title: 'Queue',
    rows: [
      {
        label: '待提交',
        value: `${queue.value.length} 项`,
        tone: queue.value.length > 0 ? 'warn' : 'neutral',
      },
      {
        label: '后端排队',
        value: `${queuePendingCount.value} 项`,
        tone: queuePendingCount.value > 0 ? 'warn' : 'good',
      },
      {
        label: '目录数据类',
        value: `${catalog.value?.datasets.length || 0} 类`,
        tone: catalog.value?.datasets.length ? 'good' : 'neutral',
      },
      {
        label: '待提交清单',
        value: queuedPreview.value,
      },
      {
        label: '状态',
        value: statusLabel(syncStatus.value.status),
        tone: syncStatus.value.status === 'failed'
          ? 'bad'
          : ['queued', 'running'].includes(syncStatus.value.status)
            ? 'warn'
            : 'good',
      },
    ],
  },
  {
    title: 'Execution',
    rows: [
      {
        label: '当前任务',
        value: activeTaskLabel.value,
        tone: syncStatus.value.status === 'running' ? 'warn' : 'neutral',
      },
      {
        label: '当前数据',
        value: currentWorkLabel.value,
        tone: syncStatus.value.status === 'running' ? 'warn' : 'neutral',
      },
      {
        label: '数据类别',
        value: datasetProgressLabel.value,
        tone: datasetRemainingCount.value > 0 ? 'warn' : 'good',
      },
      {
        label: '进度',
        value: `${unitProgressLabel.value} · ${remainingUnitLabel.value}`,
        tone: syncStatus.value.status === 'failed' ? 'bad' : 'neutral',
      },
      {
        label: '当前游标',
        value: currentCursorLabel.value,
      },
      {
        label: '成功/失败',
        value: `${syncStatus.value.success_count || 0} / ${syncStatus.value.failed_count || 0}`,
        tone: syncStatus.value.failed_count > 0 ? 'bad' : 'neutral',
      },
      {
        label: '写入行数',
        value: rowsWrittenLabel.value,
      },
      {
        label: '服务状态',
        value: serviceReady.value ? '可用' : '不可用',
        tone: serviceReady.value ? 'good' : 'bad',
      },
      {
        label: 'Relay Key',
        value: catalog.value?.relay.configured ? '已配置' : '未配置',
        tone: catalog.value?.relay.configured ? 'good' : 'warn',
      },
      {
        label: '提交入口',
        value: submissionStateLabel.value,
        tone: submissionStateTone.value,
      },
      {
        label: '运行说明',
        value: executionHint.value,
      },
    ],
  },
])

usePageContext(pageContextBlocks)

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
  try {
    const nextStatus = normalizeStatusAvailability(await syncApi.getStatus())
    syncStatus.value = nextStatus
    emit('status-change', nextStatus)
  } catch (error: any) {
    const nextStatus = {
      ...idleStatus(),
      sync_service_available: false,
      can_trigger: false,
      reason: error?.message || 'dev 同步服务未启动或状态接口不可用',
    }
    syncStatus.value = nextStatus
    emit('status-change', nextStatus)
  }
}

function normalizeStatusAvailability(status: SyncStatus): SyncStatus {
  if (status.details?.sync_service_unavailable) {
    return {
      ...status,
      sync_service_available: false,
      can_trigger: false,
      reason: status.reason || String(status.details.proxy_error || 'dev 同步服务未启动'),
    }
  }
  return status
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

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
}

function recordList(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value)
    ? value.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === 'object' && !Array.isArray(item))
    : []
}

function stringValue(value: unknown) {
  return typeof value === 'string' && value.trim() ? value.trim() : ''
}

function stringArray(value: unknown) {
  if (!Array.isArray(value)) return []
  return value.map((item) => stringValue(item)).filter(Boolean)
}

function numberValue(value: unknown) {
  const parsed = typeof value === 'number' ? value : typeof value === 'string' ? Number(value) : Number.NaN
  return Number.isFinite(parsed) ? parsed : null
}

function datasetLabel(name: string) {
  return catalogDatasetMap.value.get(name)?.display_name || syncTypeLabel(name)
}

function backendTaskToPipelineItem(task: Record<string, unknown>, state: 'running' | 'pending', order: number): QueuePipelineItem {
  const metadata = recordValue(task.metadata)
  const syncType = stringValue(metadata.sync_type)
  const relayDatasets = stringArray(metadata.relay_datasets)
  const title = stringValue(task.title)
  const isRelay = syncType === 'tushare_relay' || relayDatasets.length > 0
  const detail = relayDatasets.length
    ? relayDatasets.map(datasetLabel).join('、')
    : stringValue(task.task_id)
  return {
    id: `${state}:${stringValue(task.task_id) || title || order}`,
    display_name: isRelay ? 'Tushare Relay' : syncType ? syncTypeLabel(syncType) : title || '数据同步',
    subtitle: state === 'running' ? '后端正在执行' : `后端排队第 ${order} 位`,
    detail,
    state,
    kind: isRelay ? 'relay' : 'core',
    risk_level: 'medium',
    order,
  }
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
  const kind = item.category.startsWith('relay') ? 'relay' : 'core'
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
  if (!canTriggerSync.value) {
    ElMessage.warning(syncUnavailableReason.value)
    return
  }
  executing.value = true
  try {
    const coreItems = queue.value.filter((item) => item.kind === 'core')
    const relayItems = queue.value.filter((item) => item.kind === 'relay')
    let submittedCount = 0
    for (const item of coreItems) {
      if (item.name === 'factor_dependency') {
        ElMessage.warning('因子依赖同步请从因子看板的预计算流程触发')
        continue
      }
      await syncApi.trigger({
        sync_type: item.name as any,
        ...basePayload(),
      })
      submittedCount += 1
    }
    if (relayItems.length) {
      await syncApi.trigger({
        sync_type: 'tushare_relay',
        ...basePayload(),
        relay_datasets: relayItems.map((item) => item.name),
        relay_options: relayOptions(relayItems),
      })
      submittedCount += 1
    }
    queue.value = []
    await Promise.all([refreshStatus(), loadLogs(), loadCatalog(true)])
    if (submittedCount > 0) {
      ElMessage.success(`已提交 ${submittedCount} 个同步任务，将按队列依次执行`)
    }
  } catch (error: any) {
    ElMessage.error(error?.message || '同步任务提交失败')
  } finally {
    executing.value = false
  }
}

function basePayload() {
  const [start, end] = dateRange.value
  const isIncremental = syncMode.value === 'incremental'
  const isFull = syncMode.value === 'full'
  return {
    start_date: isIncremental ? undefined : start,
    end_date: isIncremental ? incrementalEndDate.value : end,
    sync_mode: syncMode.value,
    symbols: stockScope.value === 'custom' ? parseSymbols(symbolText.value) : undefined,
    failure_strategy: failureStrategy.value,
    full_sync: isFull,
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
    ths_concept: '同花顺概念',
    sentiment_xueqiu: '情绪 / 雪球',
    sentiment_nga: '情绪 / NGA',
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

function phaseLabel(value: string) {
  const map: Record<string, string> = {
    download: '下载阶段',
    parse: '解析入库',
    basic_info: '基础信息',
    market_value: '市值补全',
    financial_query: '财务查询',
    insert_cash: '分红入库',
    yield: '股息率计算',
  }
  return map[value] || value
}

function postSyncLabel(value: string) {
  const map: Record<string, string> = {
    clean_local_cache: '清理本地缓存',
    compute_indicators: '计算指标',
  }
  return map[value] || value
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

function queueStateLabel(value: QueuePipelineItem['state']) {
  const map: Record<QueuePipelineItem['state'], string> = {
    running: '运行',
    pending: '排队',
    draft: '草稿',
  }
  return map[value]
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

.queue-metrics {
  display: flex;
  flex: 1 1 auto;
  flex-wrap: wrap;
  gap: 6px;
}

.queue-metrics span {
  padding: 4px 8px;
  border: 1px solid rgba(126, 151, 181, 0.32);
  border-radius: 999px;
  color: #aebdd0;
  background: rgba(8, 13, 20, 0.58);
  font-size: 12px;
  font-weight: 700;
}

.queue-lanes {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-top: 12px;
}

.queue-lane {
  padding: 12px;
  border: 1px solid rgba(94, 119, 149, 0.42);
  border-radius: 9px;
  background:
    linear-gradient(135deg, rgba(18, 31, 45, 0.82), rgba(8, 13, 20, 0.92)),
    radial-gradient(circle at top left, rgba(68, 200, 255, 0.12), transparent 34%);
}

.queue-lane-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}

.queue-lane-head span {
  color: #f4f8fd;
  font-size: 13px;
  font-weight: 900;
}

.queue-lane-head em {
  color: #99abc1;
  font-size: 12px;
  font-style: normal;
}

.queue-stack {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.queue-card {
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr) auto;
  align-items: center;
  gap: 12px;
  min-height: 62px;
  padding: 11px 12px;
  border: 1px solid rgba(110, 139, 170, 0.54);
  border-radius: 8px;
  background: rgba(9, 16, 25, 0.9);
}

.queue-card--running {
  border-color: rgba(68, 200, 255, 0.92);
  background: linear-gradient(135deg, rgba(24, 68, 90, 0.94), rgba(9, 18, 29, 0.96));
  box-shadow: inset 3px 0 0 #44c8ff, 0 0 26px rgba(68, 200, 255, 0.14);
}

.queue-card--pending {
  border-color: rgba(255, 188, 66, 0.42);
}

.queue-card--draft {
  border-color: rgba(126, 151, 181, 0.5);
}

.queue-step {
  display: flex;
  width: 30px;
  height: 30px;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  color: #dff6ff;
  background: rgba(17, 28, 40, 0.96);
  font-size: 12px;
  font-weight: 900;
}

.queue-step-pulse {
  width: 12px;
  height: 12px;
  border-radius: 999px;
  background: #44c8ff;
  box-shadow: 0 0 0 0 rgba(68, 200, 255, 0.5);
  animation: queuePulse 1.5s ease-out infinite;
}

.queue-copy {
  display: flex;
  min-width: 0;
  flex-direction: column;
  gap: 4px;
}

.queue-copy strong {
  overflow: hidden;
  color: #ffffff;
  font-size: 14px;
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.queue-copy span,
.queue-copy small {
  overflow: hidden;
  color: #aebdd0;
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.queue-copy small {
  color: #8fa3bb;
}

.queue-status-group {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
}

@keyframes queuePulse {
  0% {
    box-shadow: 0 0 0 0 rgba(68, 200, 255, 0.5);
    transform: scale(0.88);
  }

  70% {
    box-shadow: 0 0 0 10px rgba(68, 200, 255, 0);
    transform: scale(1);
  }

  100% {
    box-shadow: 0 0 0 0 rgba(68, 200, 255, 0);
    transform: scale(0.88);
  }
}

.status-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  align-items: stretch;
}

.status-card,
.panel {
  border: 1px solid rgba(126, 151, 181, 0.42);
  background: linear-gradient(180deg, rgba(17, 27, 39, 0.98), rgba(8, 13, 20, 0.98));
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05);
}

.status-card {
  min-height: 68px;
  min-width: 0;
  padding: 12px 14px;
  border-radius: 6px;
}

.status-card--wide {
  grid-column: span 2;
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
  line-height: 1.25;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.relay-state.missing strong {
  color: #ffd166;
}

.status-card small {
  display: block;
  margin-top: 6px;
  overflow: hidden;
  color: #aebdd0;
  text-overflow: ellipsis;
  white-space: nowrap;
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

.relay-warning,
.sync-service-warning {
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

  .status-card--wide {
    grid-column: auto;
  }

  .queue-card {
    grid-template-columns: 30px minmax(0, 1fr);
  }

  .queue-status-group,
  .queue-card > .el-tag {
    grid-column: 2;
    justify-content: flex-start;
  }

  label.wide {
    grid-column: auto;
  }
}
</style>

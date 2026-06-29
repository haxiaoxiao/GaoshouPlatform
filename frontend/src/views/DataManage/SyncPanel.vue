<template>
  <section class="sync-workbench">
    <header class="sync-cockpit-head">
      <div class="sync-cockpit-title">
        <span class="section-kicker">DATA SYNC / OPERATIONS COCKPIT</span>
        <h2>数据同步运营台</h2>
        <p>按数据资产健康度、执行队列和同步审计管理每日多类别数据任务。</p>
      </div>
      <div class="sync-actions">
        <span class="queue-summary" :class="{ active: queue.length > 0 || queuePendingCount > 0 || isRunning }">
          草稿 {{ queue.length }} · 后端 {{ queuePendingCount }} · 运行 {{ isRunning ? 1 : 0 }}
        </span>
        <el-button :icon="Refresh" :loading="catalogLoading" @click="loadCatalog(true)">刷新目录</el-button>
        <el-button
          type="danger"
          plain
          :icon="CircleClose"
          :disabled="stoppingAll || (!isRunning && queuePendingCount === 0)"
          :loading="stoppingAll"
          @click="cancelAllSync"
        >
          停止全部同步
        </el-button>
        <el-button type="primary" :icon="VideoPlay" :disabled="queue.length === 0 || !canTriggerSync" :loading="executing" @click="executeQueue">
          执行队列
        </el-button>
      </div>
    </header>

    <section class="service-strip" aria-label="数据健康与同步服务状态">
      <article v-for="card in topMetricCards" :key="card.label" class="service-card" :class="`tone-${card.tone}`">
        <span>{{ card.label }}</span>
        <strong>{{ card.value }}</strong>
        <small>{{ card.hint }}</small>
      </article>
    </section>

    <el-progress
      class="sync-progress"
      :percentage="Math.min(100, Math.max(0, syncStatus.progress_percent || 0))"
      :status="syncStatus.status === 'failed' ? 'exception' : syncStatus.status === 'completed' ? 'success' : undefined"
      :stroke-width="8"
    />

    <el-alert
      v-if="!canTriggerSync"
      class="sync-service-warning"
      type="warning"
      show-icon
    >
      <template #title>{{ syncUnavailableReason }}</template>
    </el-alert>

    <nav class="cockpit-tabs" aria-label="数据同步业务视图">
      <button
        v-for="tab in cockpitTabs"
        :key="tab.key"
        type="button"
        class="cockpit-tab"
        :class="{ active: activeTab === tab.key }"
        @click="activeTab = tab.key"
      >
        <span>{{ tab.label }}</span>
        <small>{{ tab.hint }}</small>
      </button>
    </nav>

    <main class="cockpit-scroll">
      <section v-show="activeTab === 'overview'" class="tab-panel">
        <div class="overview-grid">
          <section class="active-run-panel" :class="{ idle: !isRunning }" aria-live="polite">
            <div class="active-run-head">
              <span class="run-pulse" :class="{ active: isRunning }"></span>
              <div>
                <span class="section-kicker">LIVE RUN</span>
                <h3>{{ isRunning ? activeTaskLabel : '暂无运行任务' }}</h3>
              </div>
            </div>
            <strong>{{ currentWorkLabel }}</strong>
            <small v-if="currentCursorLabel !== '-'">{{ currentCursorLabel }}</small>
            <div class="run-metrics">
              <span>{{ unitProgressLabel }}</span>
              <span>{{ datasetProgressLabel }}</span>
              <span>{{ rowsWrittenLabel }}</span>
            </div>
          </section>

          <section class="cockpit-panel">
            <div class="panel-title">
              <div>
                <span class="section-kicker">RUNBOOK</span>
                <h3>今日建议执行方案</h3>
              </div>
              <span class="panel-count">{{ recommendedRunbook.length }}</span>
            </div>
            <div class="runbook-list">
              <article v-for="item in recommendedRunbook" :key="item.key" class="runbook-item" :class="`tone-${item.tone}`">
                <div>
                  <strong>{{ item.title }}</strong>
                  <span>{{ item.description }}</span>
                  <small>{{ item.meta }}</small>
                </div>
                <div class="runbook-actions">
                  <el-tag size="small" effect="plain">{{ item.badge }}</el-tag>
                  <el-button size="small" :disabled="item.disabled" @click="addPreset(item.preset)">加入队列</el-button>
                </div>
              </article>
              <p v-if="!recommendedRunbook.length" class="empty-copy">暂无预设方案，先刷新同步目录。</p>
            </div>
          </section>

          <section class="cockpit-panel">
            <div class="panel-title">
              <div>
                <span class="section-kicker">FRESHNESS SLA</span>
                <h3>优先处理的数据</h3>
              </div>
              <el-button link type="primary" @click="activeTab = 'catalog'">查看目录</el-button>
            </div>
            <div class="priority-list">
              <article v-for="item in priorityDatasets" :key="item.name" class="priority-row" :class="`tone-${datasetFreshness(item)}`">
                <div>
                  <strong>{{ item.display_name }}</strong>
                  <span>{{ item.name }} · {{ categoryLabel(item.category) }}</span>
                </div>
                <small>{{ freshnessHint(item) }}</small>
              </article>
              <p v-if="!priorityDatasets.length" class="empty-copy">今日关键数据暂无明显滞后。</p>
            </div>
          </section>
        </div>
      </section>

      <section v-show="activeTab === 'catalog'" class="tab-panel">
        <section class="cockpit-panel">
          <div class="panel-title catalog-title">
            <div>
              <span class="section-kicker">ASSET CATALOG</span>
              <h3>数据任务目录</h3>
            </div>
            <div class="panel-tools">
              <el-input v-model="keyword" clearable placeholder="搜索数据集 / 来源 / 描述" />
              <el-select v-model="categoryFilter" placeholder="分类">
                <el-option label="全部" value="all" />
                <el-option label="核心" value="core" />
                <el-option label="行情" value="market" />
                <el-option label="概念" value="concept" />
                <el-option label="Relay 结构化" value="relay_structured" />
                <el-option label="分析师研报" value="relay_analyst" />
                <el-option label="北向基金" value="relay_institution" />
                <el-option label="三表财务" value="relay_financial_statement" />
                <el-option label="新闻公告" value="relay_text" />
              </el-select>
              <el-select v-model="freshnessFilter" placeholder="新鲜度">
                <el-option label="全部" value="all" />
                <el-option label="新鲜" value="fresh" />
                <el-option label="滞后" value="stale" />
                <el-option label="失败" value="failed" />
                <el-option label="受阻" value="blocked" />
              </el-select>
              <el-select v-model="dependencyFilter" placeholder="依赖">
                <el-option label="全部" value="all" />
                <el-option label="QMT" value="qmt" />
                <el-option label="Relay" value="relay" />
                <el-option label="无外部依赖" value="none" />
              </el-select>
              <el-select v-model="riskFilter" placeholder="风险">
                <el-option label="全部" value="all" />
                <el-option label="低风险" value="low" />
                <el-option label="中风险" value="medium" />
                <el-option label="高噪声" value="high" />
              </el-select>
            </div>
          </div>

          <el-table :data="filteredDatasets" height="560" class="operation-table" row-key="name">
            <el-table-column label="任务" min-width="240" fixed>
              <template #default="{ row }">
                <div class="dataset-name">
                  <strong>{{ row.display_name }}</strong>
                  <span>{{ row.name }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="健康" width="116">
              <template #default="{ row }">
                <el-tag size="small" effect="plain" :class="`freshness-tag freshness-tag--${datasetFreshness(row)}`">
                  {{ freshnessLabel(row) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="source" label="来源" width="150" show-overflow-tooltip />
            <el-table-column label="覆盖度" min-width="190">
              <template #default="{ row }">
                <div class="coverage-cell">
                  <strong v-if="hasCoverageRows(row.coverage)">{{ formatNumber(coverageRows(row.coverage)) }} 行</strong>
                  <strong v-else>-</strong>
                  <span>{{ freshnessHint(row) }}</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="频率" width="100">
              <template #default="{ row }">{{ frequencyLabel(row.recommended_frequency) }}</template>
            </el-table-column>
            <el-table-column label="依赖" width="142">
              <template #default="{ row }">
                <div class="tag-row">
                  <el-tag v-if="row.requires_qmt" size="small" effect="plain">QMT</el-tag>
                  <el-tag v-if="row.requires_relay_key" size="small" effect="plain" type="success">Relay</el-tag>
                  <span v-if="!row.requires_qmt && !row.requires_relay_key" class="muted">无</span>
                </div>
              </template>
            </el-table-column>
            <el-table-column label="风险" width="112">
              <template #default="{ row }">
                <el-tag :type="riskType(row.risk_level)" effect="plain" size="small" class="risk-tag" :class="`risk-tag--${row.risk_level}`">
                  {{ riskLabel(row.risk_level) }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="" width="104" fixed="right">
              <template #default="{ row }">
                <el-button size="small" :icon="Plus" :disabled="isDatasetBlocked(row)" @click="addTask(row)">加入</el-button>
              </template>
            </el-table-column>
            <el-table-column prop="description" label="说明" min-width="260" show-overflow-tooltip />
          </el-table>
        </section>
      </section>

      <section v-show="activeTab === 'queue'" class="tab-panel">
        <div class="queue-workspace">
          <section class="cockpit-panel">
            <div class="panel-title">
              <div>
                <span class="section-kicker">PARAMETERS</span>
                <h3>执行参数</h3>
              </div>
              <el-tag size="small" effect="plain">Relay {{ catalog?.relay.rps || 1 }} req/s</el-tag>
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
                <span>盈利预测 limit</span>
                <el-input-number v-model="reportRcLimit" :min="1" :max="500" :step="20" controls-position="right" />
              </label>
              <label>
                <span>分析师下钻数</span>
                <el-input-number v-model="analystLimit" :min="1" :max="500" :step="10" controls-position="right" />
              </label>
              <label>
                <span>THS 成分上限</span>
                <el-input-number v-model="thsMemberLimit" :min="1" :max="500" :step="10" controls-position="right" />
              </label>
              <label>
                <span>板块资金流 limit</span>
                <el-input-number v-model="blockLimit" :min="1" :max="100" controls-position="right" />
              </label>
              <label>
                <span>港股通持股 limit</span>
                <el-input-number v-model="hsgtHoldLimit" :min="100" :max="10000" :step="500" controls-position="right" />
              </label>
              <label>
                <span>基金持仓 limit</span>
                <el-input-number v-model="fundPortfolioLimit" :min="100" :max="10000" :step="500" controls-position="right" />
              </label>
              <label>
                <span>基金报告期数</span>
                <el-input-number v-model="fundPeriodLimit" :min="1" :max="40" :step="1" controls-position="right" />
              </label>
              <label>
                <span>三表财务 limit</span>
                <el-input-number v-model="statementLimit" :min="100" :max="10000" :step="500" controls-position="right" />
              </label>
            </div>
          </section>

          <section class="cockpit-panel">
            <div class="panel-title">
              <div>
                <span class="section-kicker">QUEUE PIPELINE</span>
                <h3>执行队列</h3>
              </div>
              <div class="queue-metrics">
                <span>运行 {{ isRunning ? 1 : 0 }}</span>
                <span>后端 {{ queuePendingCount }}</span>
                <span>草稿 {{ queue.length }}</span>
              </div>
              <el-button size="small" :icon="Delete" :disabled="queue.length === 0" @click="clearQueue">清空草稿</el-button>
            </div>
            <el-empty v-if="queuePipelineItems.length === 0" description="队列为空" :image-size="80" />
            <div v-else class="queue-lanes">
              <section class="queue-lane">
                <div class="queue-lane-head">
                  <span>运行中</span>
                  <em>{{ queuePipeline.running ? 1 : 0 }} 项</em>
                </div>
                <div v-if="queuePipeline.running" class="queue-card queue-card--running">
                  <span class="queue-step-pulse"></span>
                  <div class="queue-copy">
                    <strong>{{ queuePipeline.running.display_name }}</strong>
                    <span>{{ queuePipeline.running.subtitle }}</span>
                    <small v-if="queuePipeline.running.detail">{{ queuePipeline.running.detail }}</small>
                  </div>
                  <el-tag size="small" effect="plain">运行中</el-tag>
                </div>
                <p v-else class="empty-copy compact">暂无运行任务</p>
              </section>

              <section class="queue-lane">
                <div class="queue-lane-head">
                  <span>后端排队</span>
                  <em>{{ queuePipeline.pending.length }} 项</em>
                </div>
                <div class="queue-stack">
                  <div v-for="item in queuePipeline.pending" :key="item.id" class="queue-card queue-card--pending">
                    <span class="queue-step">{{ item.order }}</span>
                    <div class="queue-copy">
                      <strong>{{ item.display_name }}</strong>
                      <span>{{ item.subtitle }}</span>
                      <small v-if="item.detail">{{ item.detail }}</small>
                    </div>
                    <el-tag size="small" effect="plain" type="warning">排队中</el-tag>
                  </div>
                  <p v-if="!queuePipeline.pending.length" class="empty-copy compact">后端队列为空</p>
                </div>
              </section>

              <section class="queue-lane">
                <div class="queue-lane-head">
                  <span>本地草稿</span>
                  <em>{{ queuePipeline.draft.length }} 项</em>
                </div>
                <div class="queue-stack">
                  <div v-for="item in queuePipeline.draft" :key="item.id" class="queue-card queue-card--draft">
                    <span class="queue-step">{{ item.order }}</span>
                    <div class="queue-copy">
                      <strong>{{ item.display_name }}</strong>
                      <span>{{ item.subtitle }}</span>
                      <small v-if="item.detail">{{ item.detail }}</small>
                    </div>
                    <div class="queue-status-group">
                      <el-tag size="small" effect="plain" :type="riskType(item.risk_level)" class="risk-tag" :class="`risk-tag--${item.risk_level}`">
                        {{ riskLabel(item.risk_level) }}
                      </el-tag>
                      <el-button text :icon="Delete" @click="removeQueueItem(item.id)" />
                    </div>
                  </div>
                  <p v-if="!queuePipeline.draft.length" class="empty-copy compact">先从总览或目录加入任务</p>
                </div>
              </section>
            </div>
          </section>
        </div>
      </section>

      <section v-show="activeTab === 'history'" class="tab-panel">
        <section class="cockpit-panel">
          <div class="panel-title">
            <div>
              <span class="section-kicker">RUN MATRIX</span>
              <h3>近 7 日同步状态矩阵</h3>
            </div>
            <el-button size="small" :icon="Refresh" @click="loadLogs">刷新记录</el-button>
          </div>
          <div class="matrix">
            <div class="matrix-row matrix-row--head">
              <span></span>
              <span v-for="day in matrixDays" :key="day.key">{{ day.label }}</span>
            </div>
            <div v-for="row in matrixRows" :key="row.key" class="matrix-row">
              <strong>{{ row.label }}</strong>
              <button
                v-for="cell in row.cells"
                :key="cell.key"
                type="button"
                class="matrix-cell"
                :class="`matrix-cell--${cell.tone}`"
                :title="cell.title"
              >
                {{ cell.symbol }}
              </button>
            </div>
          </div>
        </section>

        <div class="history-grid">
          <section class="cockpit-panel">
            <div class="panel-title">
              <div>
                <span class="section-kicker">RUN HISTORY</span>
                <h3>最近记录</h3>
              </div>
            </div>
            <el-table :data="logs" class="operation-table" height="320" highlight-current-row @row-click="selectLog">
              <el-table-column label="类型" min-width="150">
                <template #default="{ row }">{{ syncTypeLabel(row.sync_type) }}</template>
              </el-table-column>
              <el-table-column label="状态" width="110">
                <template #default="{ row }">
                  <el-tag size="small" effect="plain" :class="`status-tag status-tag--${logStatusTone(row.status)}`">
                    {{ statusLabel(row.status) }}
                  </el-tag>
                </template>
              </el-table-column>
              <el-table-column label="成功/失败" width="130">
                <template #default="{ row }">{{ row.success_count ?? 0 }} / {{ row.failed_count ?? 0 }}</template>
              </el-table-column>
              <el-table-column label="开始时间" width="170">
                <template #default="{ row }">{{ formatDateTime(row.start_time) }}</template>
              </el-table-column>
              <el-table-column prop="error_message" label="错误" min-width="220" show-overflow-tooltip />
            </el-table>
          </section>

          <section class="cockpit-panel">
            <div class="panel-title">
              <div>
                <span class="section-kicker">DIAGNOSTICS</span>
                <h3>故障与日志检查</h3>
              </div>
              <el-tag v-if="selectedLog" size="small" effect="plain">#{{ selectedLog.id }}</el-tag>
            </div>
            <div v-if="selectedLog" class="diagnostic-panel">
              <div class="diagnostic-summary" :class="`tone-${logStatusTone(selectedLog.status)}`">
                <strong>{{ syncTypeLabel(selectedLog.sync_type) }}</strong>
                <span>{{ statusLabel(selectedLog.status) }} · {{ formatDateTime(selectedLog.start_time) }}</span>
              </div>
              <div class="diagnostic-grid">
                <span>成功</span><strong>{{ selectedLog.success_count ?? 0 }}</strong>
                <span>失败</span><strong>{{ selectedLog.failed_count ?? 0 }}</strong>
                <span>总量</span><strong>{{ selectedLog.total_count ?? 0 }}</strong>
                <span>结束</span><strong>{{ formatDateTime(selectedLog.end_time) }}</strong>
              </div>
              <pre class="log-inspector">{{ selectedLog.error_message || formatLogDetails(selectedLog.details) || '暂无错误信息，选择失败记录可查看诊断。' }}</pre>
            </div>
            <p v-else class="empty-copy">暂无同步记录。</p>
          </section>
        </div>
      </section>
    </main>

    <el-alert
      v-if="catalog && !catalog.relay.configured"
      class="relay-warning"
      type="warning"
      show-icon
    >
      <template #title>未检测到 INDEVS_TUSHARE_API_KEY，Relay 任务会被后端拒绝。</template>
    </el-alert>
  </section>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { CircleClose, Delete, Plus, Refresh, VideoPlay } from '@element-plus/icons-vue'
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

type FreshnessState = 'fresh' | 'stale' | 'failed' | 'blocked'

const today = new Date()
const weekAgo = new Date(today)
weekAgo.setDate(today.getDate() - 7)

const catalog = ref<SyncCatalog | null>(null)
const logs = ref<SyncLog[]>([])
const queue = ref<QueueItem[]>([])
const keyword = ref('')
const activeTab = ref<'overview' | 'catalog' | 'queue' | 'history'>('overview')
const categoryFilter = ref('all')
const freshnessFilter = ref('all')
const dependencyFilter = ref('all')
const riskFilter = ref('all')
const selectedLogId = ref<number | null>(null)
const catalogLoading = ref(false)
const executing = ref(false)
const stoppingAll = ref(false)
const stockScope = ref<'custom' | 'all'>('custom')
const symbolText = ref('000001.SZ')
const dateRange = ref<[string, string]>([formatDate(weekAgo), formatDate(today)])
const incrementalEndDate = ref(formatDate(today))
const syncMode = ref<'incremental' | 'range' | 'full'>('incremental')
const failureStrategy = ref<'skip' | 'retry' | 'stop'>('skip')
const relayDailyLimit = ref(200)
const reportRcLimit = ref(100)
const analystLimit = ref(50)
const thsMemberLimit = ref(50)
const blockLimit = ref(5)
const hsgtHoldLimit = ref(3800)
const fundPortfolioLimit = ref(5000)
const fundPeriodLimit = ref(8)
const statementLimit = ref(5000)
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

const cockpitTabs = [
  { key: 'overview' as const, label: '今日总览', hint: '健康 / 建议' },
  { key: 'catalog' as const, label: '任务目录', hint: '资产 / 依赖' },
  { key: 'queue' as const, label: '执行与队列', hint: '参数 / 编排' },
  { key: 'history' as const, label: '历史与诊断', hint: '矩阵 / 日志' },
]

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
      || 'PROD 同步服务未启动或状态接口不可用，请先启动 8810 同步服务。'
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
    const matchesFreshness = freshnessFilter.value === 'all' || datasetFreshness(item) === freshnessFilter.value
    const matchesDependency =
      dependencyFilter.value === 'all'
      || (dependencyFilter.value === 'qmt' && item.requires_qmt)
      || (dependencyFilter.value === 'relay' && item.requires_relay_key)
      || (dependencyFilter.value === 'none' && !item.requires_qmt && !item.requires_relay_key)
    const matchesRisk = riskFilter.value === 'all' || item.risk_level === riskFilter.value
    const text = `${item.name} ${item.display_name} ${item.description} ${item.source}`.toLowerCase()
    return matchesCategory && matchesFreshness && matchesDependency && matchesRisk && (!term || text.includes(term))
  })
})
const catalogDatasetMap = computed(() => new Map((catalog.value?.datasets || []).map((item) => [item.name, item])))
const latestFailedTypes = computed(() => new Set(
  logs.value
    .filter((log) => log.status === 'failed')
    .slice(0, 20)
    .map((log) => log.sync_type)
))
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
    stringValue(syncDetails.value.current_analyst_id) ? `Analyst ${stringValue(syncDetails.value.current_analyst_id)}` : '',
    stringValue(syncDetails.value.current_exchange) ? `Exchange ${stringValue(syncDetails.value.current_exchange)}` : '',
    stringValue(syncDetails.value.current_fund_code) ? `Fund ${stringValue(syncDetails.value.current_fund_code)}` : '',
    stringValue(syncDetails.value.current_period) ? `Period ${stringValue(syncDetails.value.current_period)}` : '',
    stringValue(syncDetails.value.current_indicator),
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
const healthCounts = computed(() => {
  const counts = { fresh: 0, stale: 0, failed: 0, blocked: 0 }
  for (const item of catalog.value?.datasets || []) {
    counts[datasetFreshness(item)] += 1
  }
  return counts
})
const healthCards = computed(() => [
  { key: 'fresh', label: '今日已刷新', value: healthCounts.value.fresh, hint: 'Fresh assets', tone: 'good' },
  { key: 'stale', label: '数据落后', value: healthCounts.value.stale, hint: '超过推荐同步频率', tone: 'warn' },
  { key: 'failed', label: '最近失败', value: healthCounts.value.failed, hint: '最近日志出现失败', tone: 'bad' },
  { key: 'blocked', label: '依赖受阻', value: healthCounts.value.blocked, hint: 'Relay/QMT 等依赖不可用', tone: 'neutral' },
])
const serviceCards = computed(() => [
  {
    label: '本地同步服务',
    value: serviceReady.value ? '可用' : '不可用',
    hint: serviceReady.value ? submissionStateLabel.value : syncUnavailableReason.value,
    tone: serviceReady.value ? 'good' : 'bad',
  },
  {
    label: '当前任务',
    value: activeTaskLabel.value,
    hint: currentWorkLabel.value,
    tone: isRunning.value ? 'warn' : 'neutral',
  },
  {
    label: '执行游标',
    value: unitProgressLabel.value,
    hint: currentCursorLabel.value !== '-' ? currentCursorLabel.value : remainingUnitLabel.value,
    tone: syncStatus.value.status === 'failed' ? 'bad' : isRunning.value ? 'warn' : 'neutral',
  },
  {
    label: '写入行数',
    value: rowsWrittenCount.value !== null ? formatNumber(rowsWrittenCount.value) : '-',
    hint: queuePipelineItems.value.length ? queuedPreview.value : datasetProgressLabel.value,
    tone: queuePipelineItems.value.length ? 'warn' : 'neutral',
  },
])
const topMetricCards = computed(() => [...healthCards.value, ...serviceCards.value])
const recommendedRunbook = computed(() => {
  const presets = catalog.value?.presets || []
  const fallback = presets.slice(0, 4)
  const preferred = [
    ...presets.filter((preset) => preset.include_by_default),
    ...presets.filter((preset) => preset.name.includes('daily') || preset.name.includes('core')),
    ...presets.filter((preset) => preset.name.includes('relay')),
  ]
  const deduped = new Map<string, SyncPreset>()
  for (const preset of [...preferred, ...fallback]) deduped.set(preset.name, preset)
  return Array.from(deduped.values()).slice(0, 4).map((preset) => {
    const relayCount = preset.relay_datasets.length
    const syncCount = preset.sync_types.length
    const requiresRelay = relayCount > 0
    return {
      key: preset.name,
      title: preset.display_name,
      description: preset.description,
      meta: `${syncCount + relayCount} 项 · ${requiresRelay ? 'Relay 额度任务' : '本地/核心任务'}`,
      badge: isPresetQueued(preset) ? '已加入' : requiresRelay ? '额度敏感' : '推荐执行',
      tone: requiresRelay ? 'warn' : 'good',
      disabled: requiresRelay && catalog.value?.relay.configured === false,
      preset,
    }
  })
})
const priorityDatasets = computed(() => (
  [...(catalog.value?.datasets || [])]
    .sort((a, b) => freshnessPriority(datasetFreshness(a)) - freshnessPriority(datasetFreshness(b)))
    .slice(0, 6)
))
const selectedLog = computed(() => logs.value.find((log) => log.id === selectedLogId.value) || logs.value[0] || null)
const matrixDays = computed(() => {
  const days: { key: string; label: string }[] = []
  const todayDate = new Date()
  for (let i = 6; i >= 0; i -= 1) {
    const day = new Date(todayDate)
    day.setDate(todayDate.getDate() - i)
    days.push({ key: formatDate(day), label: `${day.getMonth() + 1}/${day.getDate()}` })
  }
  return days
})
const matrixRows = computed(() => {
  const groups = [
    { key: 'core', label: '核心数据', matcher: (type: string) => ['datasync', 'stock_info', 'stock_full', 'realtime_mv'].includes(type) },
    { key: 'market', label: '行情/K线', matcher: (type: string) => ['kline_daily', 'kline_minute', 'index_daily'].includes(type) },
    { key: 'financial', label: '财务/分红', matcher: (type: string) => ['financial_data', 'dividends'].includes(type) },
    { key: 'relay', label: 'Relay/概念', matcher: (type: string) => ['tushare_relay', 'ths_concept'].includes(type) },
    { key: 'sentiment', label: '情绪/事件', matcher: (type: string) => type.startsWith('sentiment_') },
    { key: 'factor', label: '因子依赖', matcher: (type: string) => type === 'factor_dependency' },
  ]
  return groups.map((group) => ({
    key: group.key,
    label: group.label,
    cells: matrixDays.value.map((day) => {
      const dayLogs = logs.value.filter((log) => {
        const time = log.end_time || log.start_time || log.created_at
        return time?.slice(0, 10) === day.key && group.matcher(log.sync_type)
      })
      const failed = dayLogs.some((log) => log.status === 'failed')
      const running = dayLogs.some((log) => ['queued', 'running'].includes(log.status))
      const completed = dayLogs.some((log) => log.status === 'completed')
      const tone = failed ? 'bad' : running ? 'warn' : completed ? 'good' : 'empty'
      return {
        key: `${group.key}:${day.key}`,
        tone,
        symbol: failed ? 'x' : running ? '!' : completed ? 'v' : '-',
        title: dayLogs.length ? `${group.label} ${day.label}: ${dayLogs.map((log) => statusLabel(log.status)).join(' / ')}` : `${group.label} ${day.label}: 无记录`,
      }
    }),
  }))
})
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
      reason: error?.message || 'PROD 同步服务未启动或状态接口不可用',
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
      reason: status.reason || String(status.details.proxy_error || 'PROD 同步服务未启动'),
    }
  }
  return status
}

async function loadLogs() {
  logs.value = await syncApi.getLogs({ limit: 20 })
  if (!selectedLogId.value && logs.value.length) selectedLogId.value = logs.value[0].id
  lastLogRefreshAt = Date.now()
}

function hasCoverageRows(coverage: SyncCatalogItem['coverage']) {
  return typeof coverage?.row_count === 'number' && coverage.row_count > 0
}

function coverageRows(coverage: SyncCatalogItem['coverage']) {
  return typeof coverage?.row_count === 'number' ? coverage.row_count : 0
}

function datasetFreshness(item: SyncCatalogItem): FreshnessState {
  if (isDatasetBlocked(item)) return 'blocked'
  if (latestFailedTypes.value.has(item.name)) return 'failed'
  const maxDate = item.coverage?.max_date
  if (!maxDate) return item.default_enabled ? 'stale' : 'fresh'
  const latest = new Date(maxDate)
  if (Number.isNaN(latest.getTime())) return 'fresh'
  const ageDays = Math.floor((startOfDay(new Date()).getTime() - startOfDay(latest).getTime()) / 86_400_000)
  const staleAfter = freshnessThresholdDays(item.recommended_frequency)
  return ageDays > staleAfter ? 'stale' : 'fresh'
}

function freshnessThresholdDays(value: string) {
  const map: Record<string, number> = {
    daily: 1,
    weekly: 8,
    manual: 45,
    on_demand: 45,
  }
  return map[value] ?? 8
}

function freshnessPriority(value: FreshnessState) {
  const map: Record<FreshnessState, number> = {
    failed: 0,
    blocked: 1,
    stale: 2,
    fresh: 3,
  }
  return map[value]
}

function freshnessLabel(item: SyncCatalogItem) {
  const map: Record<FreshnessState, string> = {
    fresh: '新鲜',
    stale: '滞后',
    failed: '失败',
    blocked: '受阻',
  }
  return map[datasetFreshness(item)]
}

function freshnessHint(item: SyncCatalogItem) {
  if (isDatasetBlocked(item)) return item.requires_relay_key ? 'Relay Key 未配置' : '外部依赖不可用'
  if (latestFailedTypes.value.has(item.name)) return '最近执行失败'
  const maxDate = item.coverage?.max_date
  if (!maxDate) return item.coverage?.error || '暂无覆盖日期'
  return `${maxDate}${item.coverage?.estimated ? ' · 快速估算' : ''}`
}

function isDatasetBlocked(item: SyncCatalogItem) {
  return item.requires_relay_key && catalog.value?.relay.configured === false
}

function startOfDay(value: Date) {
  return new Date(value.getFullYear(), value.getMonth(), value.getDate())
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
    report_rc_limit: reportRcLimit.value,
    analyst_limit: analystLimit.value,
    analyst_rank_limit: analystLimit.value,
    block_moneyflow_limit: blockLimit.value,
    ths_member_limit: thsMemberLimit.value,
    hk_hold_limit: hsgtHoldLimit.value,
    hsgt_hold_limit: hsgtHoldLimit.value,
    fund_portfolio_limit: fundPortfolioLimit.value,
    fund_period_limit: fundPeriodLimit.value,
    statement_limit: statementLimit.value,
    rps: catalog.value?.relay.rps || 1,
    timeout_seconds: catalog.value?.relay.timeout_seconds || 30,
  }
}

async function cancelAllSync() {
  if (stoppingAll.value) return
  stoppingAll.value = true
  try {
    const result = await syncApi.cancelAll()
    const cancelledCount = Number(result.pending_cancelled_count || 0) + (result.current_cancelled ? 1 : 0)
    await Promise.all([refreshStatus(), loadLogs(), loadCatalog(true)])
    ElMessage.success(cancelledCount > 0 ? `已停止全部同步，取消 ${cancelledCount} 项任务` : '当前没有正在运行的同步任务')
  } catch (error: any) {
    ElMessage.error(error?.message || '停止全部同步失败')
  } finally {
    stoppingAll.value = false
  }
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

function categoryLabel(value: string) {
  const map: Record<string, string> = {
    core: '核心',
    market: '行情',
    concept: '概念',
    relay_structured: 'Relay 结构化',
    relay_analyst: '分析师研报',
    relay_institution: '北向基金',
    relay_financial_statement: '三表财务',
    relay_text: '新闻公告',
  }
  return map[value] || value
}

function logStatusTone(status: string) {
  if (status === 'failed') return 'bad'
  if (status === 'queued' || status === 'running' || status === 'pending') return 'warn'
  if (status === 'completed') return 'good'
  return 'neutral'
}

function selectLog(log: SyncLog) {
  selectedLogId.value = log.id
}

function formatLogDetails(details: SyncLog['details']) {
  if (!details || Object.keys(details).length === 0) return ''
  return JSON.stringify(details, null, 2)
}

function formatDateTime(value?: string | null) {
  if (!value) return '-'
  return value.replace('T', ' ').slice(0, value.includes(':') ? 16 : 10)
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
  height: 100%;
  min-height: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
  overflow: hidden;
  color: var(--text-primary);
}

.sync-cockpit-head,
.panel-title,
.sync-actions,
.panel-tools,
.queue-metrics,
.tag-row,
.runbook-actions,
.active-run-head,
.run-metrics,
.queue-status-group {
  display: flex;
  align-items: center;
  gap: 10px;
}

.sync-cockpit-head {
  justify-content: space-between;
  flex-wrap: wrap;
  padding-bottom: 12px;
  border-bottom: 1px solid var(--border-default);
}

.sync-cockpit-title {
  display: grid;
  gap: 4px;
  min-width: 280px;
}

.sync-cockpit-title h2,
.panel-title h3,
.active-run-panel h3 {
  margin: 0;
  color: var(--text-bright);
}

.sync-cockpit-title h2 {
  font-size: 24px;
}

.sync-cockpit-title p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--gs-font-body);
}

.sync-actions {
  justify-content: flex-end;
  flex-wrap: wrap;
}

.queue-summary {
  min-height: 30px;
  padding: 6px 10px;
  border: 1px solid var(--border-default);
  border-radius: var(--gs-control-radius);
  color: var(--text-secondary);
  background: var(--bg-primary);
  font-family: var(--font-data);
  font-size: var(--gs-font-control);
  font-weight: 800;
  white-space: nowrap;
}

.queue-summary.active {
  border-color: var(--border-accent);
  color: var(--accent-primary);
  background: var(--bg-active);
}

.service-strip {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(138px, 1fr));
  gap: 10px;
}

.service-card,
.cockpit-panel,
.health-tile,
.priority-row,
.runbook-item,
.queue-lane,
.queue-card,
.diagnostic-summary,
.active-run-panel {
  border: 1px solid var(--border-default);
  background: var(--bg-primary);
  box-shadow: var(--shadow-card);
}

.service-card {
  min-width: 0;
  min-height: 70px;
  display: grid;
  gap: 4px;
  align-content: center;
  padding: 9px 11px;
  border-radius: 8px;
  border-left-width: 3px;
}

.service-card span,
.health-tile span,
.panel-title .section-kicker,
.priority-row span,
.runbook-item span,
.queue-copy span,
.diagnostic-grid span,
.coverage-cell span {
  color: var(--text-muted);
  font-size: var(--gs-font-label);
  font-weight: 800;
}

.service-card strong,
.health-tile strong,
.priority-row strong,
.runbook-item strong,
.queue-copy strong,
.diagnostic-summary strong,
.coverage-cell strong {
  min-width: 0;
  overflow: hidden;
  color: var(--text-bright);
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.service-card small,
.health-tile small,
.priority-row small,
.runbook-item small,
.queue-copy small {
  min-width: 0;
  overflow: hidden;
  color: var(--text-secondary);
  font-size: var(--gs-font-label);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.tone-good { border-left-color: var(--accent-success); }
.tone-warn { border-left-color: var(--accent-warning); }
.tone-bad { border-left-color: var(--accent-danger); }
.tone-neutral { border-left-color: var(--status-neutral, #5c6863); }

.sync-progress {
  --el-fill-color-light: var(--bg-elevated);
}

:deep(.sync-progress .el-progress-bar__outer) {
  border: 1px solid var(--border-default);
  background: var(--bg-elevated) !important;
}

.sync-service-warning,
.relay-warning {
  flex-shrink: 0;
  border-radius: 8px;
}

.cockpit-tabs {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  height: 52px;
  min-height: 52px;
  overflow: hidden;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-elevated);
  flex-shrink: 0;
}

.cockpit-tab {
  box-sizing: border-box;
  display: grid;
  grid-template-rows: 18px 14px;
  gap: 3px;
  align-content: center;
  min-width: 0;
  height: 50px;
  min-height: 50px;
  padding: 7px 14px;
  border: 0;
  border-right: 1px solid var(--border-subtle);
  color: var(--text-secondary);
  background: transparent;
  cursor: pointer;
  font-family: inherit;
  text-align: left;
}

.cockpit-tab:last-child {
  border-right: 0;
}

.cockpit-tab:hover {
  background: var(--bg-hover);
}

.cockpit-tab:focus {
  outline: none;
}

.cockpit-tab:focus-visible {
  box-shadow: inset 0 0 0 2px var(--border-accent);
}

.cockpit-tab.active {
  color: var(--accent-primary);
  background: var(--bg-primary);
  box-shadow: inset 0 -3px 0 var(--accent-primary);
}

.cockpit-tab.active:focus-visible {
  box-shadow:
    inset 0 -3px 0 var(--accent-primary),
    inset 0 0 0 2px var(--border-accent);
}

.cockpit-tab span {
  overflow: hidden;
  font-size: var(--gs-font-body);
  font-weight: 900;
  line-height: 18px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.cockpit-tab small {
  overflow: hidden;
  color: var(--text-muted);
  font-size: var(--gs-font-label);
  line-height: 14px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.cockpit-scroll {
  min-height: 0;
  overflow: auto;
  padding-right: 2px;
}

.tab-panel {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.overview-grid,
.queue-workspace,
.history-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.65fr);
  gap: 14px;
  align-items: start;
}

.panel-span-2 {
  grid-column: 1 / -1;
}

.cockpit-panel {
  min-width: 0;
  border-radius: 8px;
  overflow: hidden;
}

.panel-title {
  justify-content: space-between;
  min-height: 48px;
  padding: 12px 14px;
  border-bottom: 1px solid var(--border-subtle);
  background: var(--bg-elevated);
}

.panel-title > div:first-child {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.panel-title h3 {
  font-size: var(--gs-font-title);
}

.panel-count {
  color: var(--accent-primary);
  font-family: var(--font-data);
  font-weight: 900;
}

.health-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  padding: 14px;
}

.health-tile {
  display: grid;
  gap: 6px;
  min-height: 92px;
  padding: 14px;
  border-radius: 8px;
  border-left-width: 3px;
}

.health-tile strong {
  font-family: var(--font-data);
  font-size: 26px;
}

.active-run-panel {
  display: grid;
  gap: 12px;
  align-content: start;
  min-height: 184px;
  padding: 16px;
  border-radius: 8px;
  color: #fdfbf7;
  background: linear-gradient(135deg, var(--accent-primary), var(--accent-secondary));
}

.active-run-panel.idle {
  color: var(--text-primary);
  background: var(--bg-primary);
}

.active-run-panel h3,
.active-run-panel strong,
.active-run-panel .section-kicker {
  color: inherit;
}

.active-run-panel small {
  color: currentColor;
  opacity: 0.78;
}

.run-pulse {
  width: 10px;
  height: 10px;
  border-radius: 999px;
  background: var(--text-muted);
}

.run-pulse.active {
  background: #86efac;
  box-shadow: 0 0 0 0 rgba(134, 239, 172, 0.42);
  animation: runPulse 1.5s ease-out infinite;
}

@keyframes runPulse {
  0% { box-shadow: 0 0 0 0 rgba(134, 239, 172, 0.42); }
  70% { box-shadow: 0 0 0 10px rgba(134, 239, 172, 0); }
  100% { box-shadow: 0 0 0 0 rgba(134, 239, 172, 0); }
}

.run-metrics {
  flex-wrap: wrap;
}

.run-metrics span {
  padding: 4px 8px;
  border: 1px solid rgba(253, 251, 247, 0.35);
  border-radius: 999px;
  font-family: var(--font-data);
  font-size: var(--gs-font-label);
}

.runbook-list,
.priority-list,
.queue-stack {
  display: grid;
  gap: 9px;
  padding: 14px;
}

.runbook-item,
.priority-row {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  min-width: 0;
  padding: 12px;
  border-radius: 8px;
  border-left-width: 3px;
}

.runbook-item > div:first-child,
.priority-row > div:first-child,
.diagnostic-summary {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.runbook-actions {
  flex: 0 0 auto;
  justify-content: flex-end;
  flex-wrap: wrap;
}

.priority-row {
  align-items: center;
}

.catalog-title {
  align-items: flex-end;
}

.panel-tools {
  justify-content: flex-end;
  flex-wrap: wrap;
}

.panel-tools .el-input {
  width: 220px;
}

.panel-tools .el-select {
  width: 138px;
}

.dataset-name,
.coverage-cell {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.dataset-name span,
.muted {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--gs-font-label);
}

.tag-row {
  flex-wrap: wrap;
  gap: 5px;
}

:deep(.operation-table) {
  --el-table-bg-color: var(--bg-primary);
  --el-table-tr-bg-color: var(--bg-primary);
  --el-table-header-bg-color: var(--bg-elevated);
  --el-table-row-hover-bg-color: var(--bg-hover);
  --el-table-border-color: var(--border-subtle);
  --el-table-text-color: var(--text-primary);
  --el-table-header-text-color: var(--text-secondary);
  border-radius: 0 0 8px 8px;
}

:deep(.operation-table th.el-table__cell) {
  color: var(--text-secondary);
  background: var(--bg-elevated) !important;
  font-size: var(--gs-font-table);
}

:deep(.operation-table td.el-table__cell) {
  color: var(--text-primary);
  background: var(--bg-primary);
}

.freshness-tag,
.status-tag,
.risk-tag {
  min-width: 64px;
  justify-content: center;
  border-radius: 4px;
  font-weight: 800;
}

.freshness-tag--fresh,
.status-tag--good {
  border-color: var(--accent-success) !important;
  color: var(--accent-success) !important;
  background: var(--status-ready-bg) !important;
}

.freshness-tag--stale,
.status-tag--warn {
  border-color: var(--accent-warning) !important;
  color: var(--accent-warning) !important;
  background: var(--status-warning-bg) !important;
}

.freshness-tag--failed,
.status-tag--bad {
  border-color: var(--accent-danger) !important;
  color: var(--accent-danger) !important;
  background: var(--status-critical-bg) !important;
}

.freshness-tag--blocked,
.status-tag--neutral {
  border-color: var(--status-neutral) !important;
  color: var(--status-neutral) !important;
  background: var(--status-neutral-bg) !important;
}

.risk-tag--low {
  border-color: var(--accent-success) !important;
  color: var(--accent-success) !important;
  background: var(--status-ready-bg) !important;
}

.risk-tag--medium {
  border-color: var(--accent-warning) !important;
  color: var(--accent-warning) !important;
  background: var(--status-warning-bg) !important;
}

.risk-tag--high {
  border-color: var(--accent-danger) !important;
  color: var(--accent-danger) !important;
  background: var(--status-critical-bg) !important;
}

.control-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  padding: 14px;
}

label {
  display: grid;
  gap: 7px;
  min-width: 0;
}

label > span {
  color: var(--text-muted);
  font-size: var(--gs-font-label);
  font-weight: 900;
}

label.wide {
  grid-column: span 2;
}

:deep(.el-segmented) {
  width: 100%;
}

.queue-lanes {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  padding: 14px;
}

.queue-lane {
  min-width: 0;
  min-height: 270px;
  padding: 12px;
  border-radius: 8px;
  background: var(--bg-elevated);
  box-shadow: none;
}

.queue-lane-head {
  display: flex;
  justify-content: space-between;
  gap: 10px;
  padding-bottom: 9px;
  border-bottom: 1px solid var(--border-subtle);
  color: var(--text-secondary);
  font-size: var(--gs-font-control);
  font-weight: 900;
}

.queue-lane-head em {
  color: var(--text-muted);
  font-style: normal;
}

.queue-card {
  display: grid;
  grid-template-columns: 28px minmax(0, 1fr);
  grid-template-areas:
    "step copy"
    ". actions";
  column-gap: 10px;
  row-gap: 8px;
  align-items: start;
  min-height: 58px;
  padding: 10px;
  border-radius: 8px;
  box-shadow: none;
}

.queue-card--running { border-left: 3px solid var(--accent-success); }
.queue-card--pending { border-left: 3px solid var(--accent-warning); }
.queue-card--draft { border-left: 3px solid var(--status-neutral); }

.queue-step,
.queue-step-pulse {
  grid-area: step;
  display: inline-flex;
  width: 26px;
  height: 26px;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  color: var(--accent-primary);
  background: var(--bg-active);
  font-family: var(--font-data);
  font-size: var(--gs-font-label);
  font-weight: 900;
}

.queue-step-pulse {
  margin-top: 7px;
  width: 12px;
  height: 12px;
  background: var(--accent-success);
  animation: runPulse 1.5s ease-out infinite;
}

.queue-copy {
  grid-area: copy;
  display: grid;
  gap: 3px;
  min-width: 0;
}

.queue-card > .el-tag:not(.risk-tag) {
  grid-area: actions;
  justify-self: start;
  max-width: 100%;
}

.queue-status-group {
  grid-area: actions;
  min-width: 0;
  flex-wrap: wrap;
  justify-content: flex-start;
  gap: 6px;
}

.queue-status-group .el-button {
  width: 26px;
  height: 26px;
  flex: 0 0 auto;
}

.queue-metrics {
  flex: 1 1 auto;
  flex-wrap: wrap;
  gap: 6px;
}

.queue-metrics span {
  padding: 4px 8px;
  border: 1px solid var(--border-default);
  border-radius: 999px;
  color: var(--text-secondary);
  background: var(--bg-primary);
  font-family: var(--font-data);
  font-size: var(--gs-font-label);
  font-weight: 800;
}

.matrix {
  display: grid;
  gap: 8px;
  padding: 14px;
  overflow-x: auto;
}

.matrix-row {
  display: grid;
  grid-template-columns: 140px repeat(7, minmax(58px, 1fr));
  gap: 8px;
  align-items: center;
  min-width: 620px;
}

.matrix-row strong,
.matrix-row span {
  color: var(--text-secondary);
  font-size: var(--gs-font-control);
  font-weight: 900;
}

.matrix-cell {
  height: 34px;
  border: 1px solid var(--border-default);
  border-radius: 6px;
  background: var(--bg-primary);
  color: var(--text-muted);
  cursor: pointer;
  font-family: var(--font-data);
  font-size: var(--gs-font-label);
  font-weight: 900;
}

.matrix-cell--good {
  border-color: var(--accent-success);
  color: var(--accent-success);
  background: var(--status-ready-bg);
}

.matrix-cell--warn {
  border-color: var(--accent-warning);
  color: var(--accent-warning);
  background: var(--status-warning-bg);
}

.matrix-cell--bad {
  border-color: var(--accent-danger);
  color: var(--accent-danger);
  background: var(--status-critical-bg);
}

.diagnostic-panel {
  display: grid;
  gap: 12px;
  padding: 14px;
}

.diagnostic-summary {
  padding: 12px;
  border-radius: 8px;
  border-left-width: 3px;
}

.diagnostic-grid {
  display: grid;
  grid-template-columns: 64px minmax(0, 1fr);
  gap: 7px 10px;
  padding: 12px;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--bg-elevated);
}

.diagnostic-grid strong {
  min-width: 0;
  overflow: hidden;
  font-family: var(--font-data);
  font-size: var(--gs-font-control);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.log-inspector {
  min-height: 132px;
  max-height: 260px;
  overflow: auto;
  padding: 12px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  color: var(--text-primary);
  background: var(--bg-elevated);
  font-family: var(--font-data);
  font-size: var(--gs-font-control);
  white-space: pre-wrap;
}

.empty-copy {
  margin: 0;
  border: 1px dashed var(--border-default);
  border-radius: 8px;
  color: var(--text-muted);
  padding: 16px;
  text-align: center;
}

.empty-copy.compact {
  padding: 12px;
  font-size: var(--gs-font-control);
}

@media (max-width: 1280px) {
  .overview-grid,
  .queue-workspace,
  .history-grid {
    grid-template-columns: 1fr;
  }

  .active-run-panel {
    grid-column: auto;
  }
}

@media (max-width: 900px) {
  .sync-workbench {
    min-height: 1100px;
    overflow: visible;
  }

  .cockpit-scroll {
    overflow: visible;
  }

  .service-strip,
  .health-grid,
  .queue-lanes,
  .control-grid {
    grid-template-columns: 1fr;
  }

  .cockpit-tabs {
    display: flex;
    overflow-x: auto;
  }

  .cockpit-tab {
    min-width: 140px;
  }

  .panel-title,
  .catalog-title,
  .sync-cockpit-head,
  .runbook-item,
  .priority-row {
    align-items: stretch;
    flex-direction: column;
  }

  .sync-actions,
  .panel-tools,
  .runbook-actions {
    justify-content: flex-start;
  }

  .panel-tools .el-input,
  .panel-tools .el-select {
    width: 100%;
  }

  label.wide,
  .panel-span-2 {
    grid-column: auto;
  }
}
</style>

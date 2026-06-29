<template>
  <div class="page-frame ops-flow-page">
    <header class="ops-head">
      <div class="ops-head__copy">
        <span class="section-kicker">OPERATIONS FLOW MAP</span>
        <h2>系统运维控制台</h2>
        <p>按投流链路查看接入、同步、存储、计算、安全、实盘与审计。</p>
      </div>
      <div class="ops-head__status">
        <span class="flow-chip" :class="`flow-chip--${globalHealth.tone}`">
          <span class="flow-dot" />
          {{ globalHealth.label }}
        </span>
        <span class="flow-chip" :class="incidentSummaryToneClass">{{ incidentSummaryLabel }}</span>
        <span class="flow-chip flow-chip--neutral">{{ currentTimeLabel }}</span>
      </div>
    </header>

    <main class="flow-main">
      <section class="flow-shell">
        <div class="flow-section-head">
          <div>
            <span class="section-kicker">FLOW TOPOLOGY</span>
            <h3>量化投流管道拓扑</h3>
          </div>
          <span class="table-hint">状态由服务、队列、数据分区、任务和护栏聚合生成</span>
        </div>

        <div class="flow-canvas" aria-label="量化投流管道拓扑">
          <article
            v-for="station in flowStations"
            :key="station.key"
            class="flow-station"
            :class="`flow-station--${station.tone}`"
          >
            <span v-if="station.incidentCount > 0" class="station-incident">{{ station.incidentCount }}</span>
            <div class="station-head">
              <span class="station-num">{{ station.index }}</span>
              <span class="flow-chip" :class="`flow-chip--${station.tone}`">
                <span class="flow-dot" />
                {{ station.status }}
              </span>
            </div>
            <div class="station-title">
              <strong>{{ station.title }}</strong>
              <small>{{ station.subtitle }}</small>
            </div>
            <div class="station-metrics">
              <div v-for="metric in station.metrics" :key="metric.label" class="station-metric">
                <span>{{ metric.label }}</span>
                <strong>{{ metric.value }}</strong>
              </div>
            </div>
            <div class="station-event">{{ station.event }}</div>
            <button class="station-link" type="button" @click="station.path && router.push(station.path)">
              {{ station.action }}
            </button>
          </article>
        </div>
      </section>

      <div class="support-grid">
        <section class="support-panel">
          <div class="support-head">
            <div>
              <span class="section-kicker">INCIDENTS BY STATION</span>
              <h3>节点事件时间线</h3>
            </div>
            <el-tag :type="incidentRows.some(row => row.tone === 'bad') ? 'danger' : incidentRows.some(row => row.tone === 'warn') ? 'warning' : 'success'" effect="plain">
              {{ incidentRows.length }} 条
            </el-tag>
          </div>
          <div class="incident-timeline">
            <button
              v-for="row in topologyIncidents"
              :key="row.key"
              type="button"
              class="timeline-row"
              :class="`timeline-row--${row.tone}`"
              @click="row.path && router.push(row.path)"
            >
              <span class="timeline-scope">{{ row.station }}</span>
              <div>
                <strong>{{ row.title }}</strong>
                <small>{{ row.detail }}</small>
              </div>
              <em>{{ row.action }}</em>
            </button>
          </div>
        </section>

        <section class="support-panel">
          <div class="support-head">
            <div>
              <span class="section-kicker">TASKS / STORAGE / AUDIT</span>
              <h3>运行证据</h3>
            </div>
          </div>
          <el-table :data="opsEvidenceRows" size="small" height="206" class="ops-table">
            <el-table-column prop="name" label="对象" min-width="150" show-overflow-tooltip />
            <el-table-column prop="stage" label="阶段" width="92" />
            <el-table-column label="状态" width="96">
              <template #default="{ row }">
                <el-tag :type="dataStatusTagType(row.tone)" effect="plain" size="small">{{ row.status }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="evidence" label="证据" min-width="180" show-overflow-tooltip />
          </el-table>
          <pre class="log-well"><span v-for="line in logPreviewLines" :key="line">{{ line }}
</span></pre>
        </section>
      </div>
    </main>

    <aside class="control-dock" aria-label="运维控制台">
      <div>
        <span class="section-kicker">CONTROL DOCK</span>
        <h3>操作控制台</h3>
        <p>按钮集中在这里，拓扑节点只负责展示运行证据。</p>
      </div>

      <section class="dock-group">
        <div class="dock-row">
          <strong>巡检操作</strong>
          <el-tag :type="syncServiceReady ? 'success' : 'danger'" effect="plain" size="small">
            {{ syncServiceReady ? 'ready' : 'blocked' }}
          </el-tag>
        </div>
        <el-button type="primary" :icon="Refresh" :loading="loading" @click="loadOps">刷新拓扑</el-button>
        <el-button @click="router.push('/data/sync')">进入数据同步</el-button>
        <el-button @click="router.push('/explorer')">打开数据浏览器</el-button>
        <div class="dock-metrics" aria-label="当前操作条件">
          <div
            v-for="row in controlRows"
            :key="row.label"
            class="dock-metric"
            :class="`dock-metric--${row.tone}`"
          >
            <span>{{ row.label }}</span>
            <strong>{{ row.value }}</strong>
            <small>{{ row.hint }}</small>
          </div>
        </div>
      </section>

      <section v-if="devDataMode?.enabled" class="dock-group" :class="{ 'dock-group--warning': devDataMode.use_prod_data }">
        <div class="dock-row">
          <strong>开发数据模式</strong>
          <el-tag :type="devDataMode.use_prod_data ? 'warning' : 'info'" effect="plain" size="small">
            {{ devDataMode.use_prod_data ? 'PROD DATA' : 'DEV' }}
          </el-tag>
        </div>
        <p>{{ devDataMode.use_prod_data ? '当前 dev 正在使用生产真实数据。' : '当前使用开发隔离数据。切换生产真实数据必须确认。' }}</p>
        <small>{{ devDataMode.active_data_dir }}</small>
        <el-button
          :type="devDataMode.use_prod_data ? 'warning' : 'primary'"
          :loading="switchingDataMode"
          @click="toggleDevDataMode"
        >
          {{ devDataMode.use_prod_data ? '切回开发隔离数据' : '切换到生产真实数据' }}
        </el-button>
      </section>

      <section v-if="liveGuardrails" class="dock-group" :class="{ 'dock-group--danger': guardrailDraft.enable_order_submit || guardrailDraft.auto_execute_enabled }">
        <div class="dock-row">
          <strong>实盘交易护栏</strong>
          <el-tag :type="guardrailDraft.enable_order_submit || guardrailDraft.auto_execute_enabled ? 'danger' : 'success'" effect="plain" size="small">
            {{ guardrailDraft.enable_order_submit || guardrailDraft.auto_execute_enabled ? 'ARMED' : 'SAFE' }}
          </el-tag>
        </div>
        <label class="guardrail-row" :class="{ 'guardrail-row--on': guardrailDraft.enable_order_submit }">
          <span>
            <strong>真实下单总开关</strong>
            <small>LIVE_TRADING_ENABLE_ORDER_SUBMIT</small>
          </span>
          <el-switch
            v-model="guardrailDraft.enable_order_submit"
            :disabled="savingGuardrails"
            active-text="ON"
            inactive-text="OFF"
            inline-prompt
          />
        </label>
        <label class="guardrail-row" :class="{ 'guardrail-row--on': guardrailDraft.auto_execute_enabled }">
          <span>
            <strong>自动实盘执行</strong>
            <small>LIVE_TRADING_AUTO_EXECUTE_ENABLED</small>
          </span>
          <el-switch
            v-model="guardrailDraft.auto_execute_enabled"
            :disabled="savingGuardrails || !guardrailDraft.enable_order_submit"
            active-text="ON"
            inactive-text="OFF"
            inline-prompt
          />
        </label>
        <small class="dock-env">{{ liveGuardrails.env_file }}</small>
        <div class="dock-actions">
          <el-button :disabled="savingGuardrails || !guardrailsDirty" @click="resetLiveGuardrailDraft">恢复当前</el-button>
          <el-button
            :type="guardrailDraft.enable_order_submit || guardrailDraft.auto_execute_enabled ? 'danger' : 'primary'"
            :loading="savingGuardrails"
            :disabled="!guardrailsDirty"
            @click="saveLiveGuardrails"
          >
            保存护栏
          </el-button>
        </div>
      </section>

      <section class="dock-group">
        <div class="dock-row">
          <strong>危险操作区</strong>
          <el-tag effect="plain" size="small">locked</el-tag>
        </div>
        <el-button
          type="danger"
          plain
          :loading="cancellingAll"
          :disabled="cancellingAll || (!isSyncActive && queuePendingCount === 0)"
          @click="cancelAllSync"
        >
          停止全部同步
        </el-button>
      </section>
    </aside>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Refresh } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import { systemApi, type DataSummary, type DataSummaryItem, type DevDataMode, type LiveTradingGuardrails, type SystemStatus } from '@/api/system'
import { syncApi, type SyncLog, type SyncStatus } from '@/api/sync'
import { runtimeTaskApi, type RuntimeTask } from '@/api/runtimeTasks'
import { liveTradingApi, type LiveTradingStatus } from '@/api/liveTrading'

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

interface FlowMetric {
  label: string
  value: string
}

interface FlowStation {
  key: string
  index: string
  title: string
  subtitle: string
  status: string
  tone: Tone
  metrics: FlowMetric[]
  event: string
  action: string
  path?: string
  incidentCount: number
}

interface TopologyIncident extends IncidentRow {
  station: string
}

interface OpsEvidenceRow {
  name: string
  stage: string
  status: string
  evidence: string
  tone: Tone
}

const router = useRouter()
const loading = ref(false)
const cancellingAll = ref(false)
const systemStatus = ref<SystemStatus | null>(null)
const syncStatus = ref<SyncStatus | null>(null)
const runtimeTasks = ref<RuntimeTask[]>([])
const syncLogs = ref<SyncLog[]>([])
const dataSummary = ref<DataSummary | null>(null)
const liveStatus = ref<LiveTradingStatus | null>(null)
const devDataMode = ref<DevDataMode | null>(null)
const liveGuardrails = ref<LiveTradingGuardrails | null>(null)
const guardrailDraft = ref({
  enable_order_submit: false,
  auto_execute_enabled: false,
})
const switchingDataMode = ref(false)
const savingGuardrails = ref(false)

const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const activeTasks = computed(() => runtimeTasks.value.filter(task => ['queued', 'running'].includes(String(task.status))))
const failedTasks = computed(() => runtimeTasks.value.filter(task => String(task.status) === 'failed'))
const queuePendingCount = computed(() => Number(syncStatus.value?.details?.queue_pending_count || 0))
const syncServiceReady = computed(() => syncStatus.value?.sync_service_available !== false && syncStatus.value?.details?.sync_service_unavailable !== true)
const staleDatasetCount = computed(() => datasetRows.value.filter(row => row.tone === 'warn' || row.tone === 'bad').length)
const latestRuntimeTask = computed(() => runtimeTasks.value[0] || null)
const isSyncActive = computed(() => ['queued', 'running'].includes(syncStatus.value?.status || ''))
const guardrailsDirty = computed(() => Boolean(
  liveGuardrails.value
  && (
    guardrailDraft.value.enable_order_submit !== liveGuardrails.value.enable_order_submit
    || guardrailDraft.value.auto_execute_enabled !== liveGuardrails.value.auto_execute_enabled
  ),
))

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
    value: liveStatus.value?.quote_connected ? '行情在线' : liveStatus.value?.xtdata_available ? '未连接' : '不可用',
    detail: liveStatus.value?.error || (liveStatus.value?.xttrader_available ? 'xttrader 可用' : 'xttrader 未确认'),
    tone: liveStatus.value?.quote_connected ? 'good' : 'warn',
  },
  {
    key: 'orders',
    label: 'Order Guard',
    value: liveGuardrails.value?.enable_order_submit ? '真实下单开启' : '仅信号',
    detail: liveStatus.value?.account_id || '未配置账户',
    tone: liveGuardrails.value?.enable_order_submit ? 'bad' : 'good',
  },
])

const isApiHealthy = computed(() => ['healthy', 'ok', 'running'].includes(String(systemStatus.value?.status || '')))

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
      detail: syncStatus.value?.reason || String(syncStatus.value?.details?.proxy_error || '8810 同步服务未返回可用状态'),
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
  if (liveGuardrails.value?.enable_order_submit) {
    rows.push({
      key: 'order-open',
      scope: 'TRADE',
      title: '真实下单护栏已开启',
      detail: liveStatus.value?.account_id || '账户未返回',
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
  {
    label: '真实下单',
    value: liveGuardrails.value?.enable_order_submit ? '开启' : '关闭',
    hint: 'LIVE_TRADING_ENABLE_ORDER_SUBMIT',
    tone: liveGuardrails.value?.enable_order_submit ? 'bad' : 'good',
  },
  {
    label: '自动实盘',
    value: liveGuardrails.value?.auto_execute_enabled ? '开启' : '关闭',
    hint: 'LIVE_TRADING_AUTO_EXECUTE_ENABLED',
    tone: liveGuardrails.value?.auto_execute_enabled ? 'bad' : 'good',
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

const sourceStationTone = computed<Tone>(() => {
  if (liveStatus.value?.quote_connected && isApiHealthy.value) return 'good'
  if (systemStatus.value && !isApiHealthy.value) return 'bad'
  return 'warn'
})
const storageTone = computed<Tone>(() => {
  if (dataSummary.value?.overall_status === 'error') return 'bad'
  if (staleDatasetCount.value > 0 || dataSummary.value?.overall_status === 'degraded') return 'warn'
  if (dataSummary.value?.overall_status === 'good') return 'good'
  return 'neutral'
})
const computeTone = computed<Tone>(() => {
  if (failedTasks.value.length > 0) return 'bad'
  if (activeTasks.value.length > 0) return 'warn'
  return 'good'
})
const safetyTone = computed<Tone>(() => {
  if (liveGuardrails.value?.enable_order_submit || liveGuardrails.value?.auto_execute_enabled || guardrailDraft.value.enable_order_submit || guardrailDraft.value.auto_execute_enabled) return 'bad'
  if (devDataMode.value?.use_prod_data) return 'warn'
  return 'good'
})
const auditTone = computed<Tone>(() => {
  if (syncLogs.value.some(log => log.status === 'failed')) return 'warn'
  return syncLogs.value.length ? 'good' : 'neutral'
})
const globalHealth = computed(() => {
  if (safetyTone.value === 'bad' || incidentRows.value.some(row => row.tone === 'bad')) return { label: 'ATTENTION', tone: 'bad' as Tone }
  if (incidentRows.value.some(row => row.tone === 'warn')) return { label: 'WATCHING', tone: 'warn' as Tone }
  return { label: 'SAFE', tone: 'good' as Tone }
})
const currentTimeLabel = computed(() => {
  const generatedAt = dataSummary.value?.generated_at || liveGuardrails.value?.updated_at
  return generatedAt ? `${formatDateTime(generatedAt)} refreshed` : '等待首次巡检'
})
const incidentSummaryLabel = computed(() => {
  const actionable = incidentRows.value.filter(row => row.key !== 'clear')
  return actionable.length ? `${actionable.length} 个节点需复核` : '无阻塞事件'
})
const incidentSummaryToneClass = computed(() => {
  if (incidentRows.value.some(row => row.tone === 'bad')) return 'flow-chip--bad'
  if (incidentRows.value.some(row => row.tone === 'warn')) return 'flow-chip--warn'
  return 'flow-chip--good'
})
const topologyIncidents = computed<TopologyIncident[]>(() => (
  incidentRows.value.map(row => ({
    ...row,
    station: incidentStation(row),
  }))
))
const flowStations = computed<FlowStation[]>(() => [
  {
    key: 'source',
    index: '01',
    title: '外部源 / 接入层',
    subtitle: 'QMT · Relay · API · Local files',
    status: sourceStationStatus.value,
    tone: sourceStationTone.value,
    metrics: [
      { label: '行情心跳', value: liveStatus.value?.quote_connected ? 'QMT 在线' : liveStatus.value?.xtdata_available ? 'xtdata 可用' : 'QMT 未连接' },
      { label: 'API 状态', value: isApiHealthy.value ? '/system/status ok' : systemStatus.value ? String(systemStatus.value.status) : '等待返回' },
      { label: 'Relay', value: dataSummary.value?.market_data_backend || systemStatus.value?.market_data_backend || 'parquet' },
    ],
    event: liveStatus.value?.error || (liveStatus.value?.xttrader_available ? 'xttrader 可用，等待行情连接' : 'xttrader 未确认，实时行情可能不可用'),
    action: '查看接入诊断',
    path: '/monitor',
    incidentCount: stationIncidentCount('接入'),
  },
  {
    key: 'sync',
    index: '02',
    title: '同步采集',
    subtitle: 'Sync Worker · Queue · Trigger',
    status: syncStationStatus.value,
    tone: serviceNodes.value.find(node => node.key === 'sync')?.tone || 'neutral',
    metrics: [
      { label: 'Worker', value: syncServiceReady.value ? syncStatusLabel(syncStatus.value?.status || 'idle') : '不可用' },
      { label: '后端排队', value: `${queuePendingCount.value} 项` },
      { label: '提交状态', value: syncStatus.value?.can_trigger === false ? '拒绝提交' : syncServiceReady.value ? '可提交' : '等待服务' },
    ],
    event: syncStatus.value?.error_message || syncStatus.value?.reason || (syncStatus.value?.sync_type ? `当前任务：${syncTypeLabel(syncStatus.value.sync_type)}` : '无运行任务，等待调度'),
    action: '查看同步队列',
    path: '/data/sync',
    incidentCount: stationIncidentCount('同步'),
  },
  {
    key: 'storage',
    index: '03',
    title: '存储校验',
    subtitle: 'SQLite · Parquet · DuckDB',
    status: storageStationStatus.value,
    tone: storageTone.value,
    metrics: [
      { label: 'Backend', value: dataSummary.value?.market_data_backend || systemStatus.value?.market_data_backend || 'parquet' },
      { label: '异常数据集', value: `${staleDatasetCount.value} 项` },
      { label: '最新分区', value: latestDatasetLabel.value },
    ],
    event: staleDatasetCount.value ? `${staleDatasetCount.value} 个数据集需要复核最新口径` : '核心分区未发现明显过期',
    action: '打开数据浏览器',
    path: '/explorer',
    incidentCount: stationIncidentCount('存储'),
  },
  {
    key: 'compute',
    index: '04',
    title: '计算与任务',
    subtitle: 'Runtime · Factors · Jobs',
    status: computeStationStatus.value,
    tone: computeTone.value,
    metrics: [
      { label: '活动任务', value: `${activeTasks.value.length} 个` },
      { label: '失败任务', value: `${failedTasks.value.length} 个` },
      { label: '当前进度', value: latestRuntimeProgress.value },
    ],
    event: failedTasks.value[0]?.error || (latestRuntimeTask.value ? `${latestRuntimeTask.value.title || latestRuntimeTask.value.kind || '运行任务'} · ${latestRuntimeTask.value.status}` : '无运行任务'),
    action: '查看任务详情',
    path: '/monitor',
    incidentCount: stationIncidentCount('计算'),
  },
  {
    key: 'safety',
    index: '05',
    title: '决策与安全',
    subtitle: 'Dev mode · Guardrails',
    status: safetyStationStatus.value,
    tone: safetyTone.value,
    metrics: [
      { label: '真实下单', value: liveGuardrails.value?.enable_order_submit || guardrailDraft.value.enable_order_submit ? '开启' : '关闭' },
      { label: '自动实盘', value: liveGuardrails.value?.auto_execute_enabled || guardrailDraft.value.auto_execute_enabled ? '开启' : '关闭' },
      { label: '数据模式', value: devDataMode.value?.use_prod_data ? '生产真实' : '开发隔离' },
    ],
    event: guardrailsDirty.value ? '护栏草稿已修改，保存前不会生效' : safetyTone.value === 'good' ? '护栏处于 SAFE，未修改' : '安全配置需要复核',
    action: '查看安全状态',
    path: '/monitor',
    incidentCount: stationIncidentCount('安全'),
  },
  {
    key: 'audit',
    index: '06',
    title: '实盘接口与审计',
    subtitle: 'Order Bridge · Logs · Account',
    status: auditStationStatus.value,
    tone: auditTone.value,
    metrics: [
      { label: '账户', value: liveStatus.value?.account_id || '未配置' },
      { label: '最近同步', value: latestSyncLogLabel.value },
      { label: '失败日志', value: `${syncLogs.value.filter(log => log.status === 'failed').length} 条` },
    ],
    event: syncLogs.value.find(log => log.status === 'failed')?.error_message || (syncLogs.value[0] ? `${syncTypeLabel(syncLogs.value[0].sync_type)} · ${syncStatusLabel(syncLogs.value[0].status)}` : '等待审计日志'),
    action: '打开审计日志',
    path: '/monitor',
    incidentCount: stationIncidentCount('审计'),
  },
])
const sourceStationStatus = computed(() => {
  if (liveStatus.value?.quote_connected && isApiHealthy.value) return 'Healthy'
  if (systemStatus.value && !isApiHealthy.value) return 'Broken'
  return 'Degraded'
})
const syncStationStatus = computed(() => {
  if (!syncServiceReady.value) return 'Blocked'
  if (syncStatus.value?.status === 'failed') return 'Failed'
  if (isSyncActive.value) return 'Running'
  return 'Ready'
})
const storageStationStatus = computed(() => {
  if (storageTone.value === 'bad') return 'Broken'
  if (storageTone.value === 'warn') return 'Stale'
  if (storageTone.value === 'good') return 'Fresh'
  return 'Unknown'
})
const computeStationStatus = computed(() => {
  if (computeTone.value === 'bad') return 'Failed'
  if (activeTasks.value.length) return 'Running'
  return 'Idle'
})
const safetyStationStatus = computed(() => {
  if (safetyTone.value === 'bad') return 'Armed'
  if (safetyTone.value === 'warn') return 'Review'
  return 'Safe'
})
const auditStationStatus = computed(() => {
  if (auditTone.value === 'warn') return 'Warning'
  if (auditTone.value === 'good') return 'Audit'
  return 'Waiting'
})
const latestDatasetLabel = computed(() => {
  const item = datasetRows.value.find(row => row.latest !== '-') || datasetRows.value[0]
  return item ? item.latest : '-'
})
const latestRuntimeProgress = computed(() => {
  const task = latestRuntimeTask.value
  if (!task) return '-'
  return `${Math.round((task.progress || 0) * 100)}%`
})
const latestSyncLogLabel = computed(() => {
  const log = syncLogs.value[0]
  if (!log) return '-'
  return formatDateTime(log.end_time || log.start_time)
})
const opsEvidenceRows = computed<OpsEvidenceRow[]>(() => {
  const rows: OpsEvidenceRow[] = []
  if (latestRuntimeTask.value) {
    rows.push({
      name: latestRuntimeTask.value.title || latestRuntimeTask.value.kind || '运行任务',
      stage: '计算',
      status: String(latestRuntimeTask.value.status || '-'),
      evidence: latestRuntimeTask.value.error || latestRuntimeProgress.value,
      tone: taskTone(String(latestRuntimeTask.value.status || '')),
    })
  }
  for (const row of datasetRows.value.slice(0, 4)) {
    rows.push({
      name: row.label,
      stage: '存储',
      status: row.statusText,
      evidence: `${row.latest} · ${row.rows}`,
      tone: row.tone,
    })
  }
  for (const log of syncLogs.value.slice(0, 3)) {
    rows.push({
      name: syncTypeLabel(log.sync_type),
      stage: '审计',
      status: syncStatusLabel(log.status),
      evidence: `${log.success_count ?? 0} / ${log.failed_count ?? 0} · ${formatDateTime(log.start_time)}`,
      tone: log.status === 'failed' ? 'bad' : log.status === 'completed' ? 'good' : 'warn',
    })
  }
  if (!rows.length) {
    rows.push({ name: '等待巡检数据', stage: '巡检', status: '等待', evidence: '刷新拓扑后显示运行证据', tone: 'neutral' })
  }
  return rows.slice(0, 8)
})
const logPreviewLines = computed(() => {
  const lines = syncLogs.value.slice(0, 4).map(log => (
    `[${formatDateTime(log.start_time)}] ${syncTypeLabel(log.sync_type)} -> ${syncStatusLabel(log.status)} (${log.success_count ?? 0}/${log.failed_count ?? 0})`
  ))
  if (!lines.length) return ['等待同步审计日志...']
  return lines
})

async function loadOps() {
  loading.value = true
  try {
    const [systemResult, syncStatusResult, tasksResult, logsResult, summaryResult, liveTradingResult, devModeResult, guardrailsResult] = await Promise.allSettled([
      systemApi.getStatus(),
      syncApi.getStatus(),
      runtimeTaskApi.list(true),
      syncApi.getLogs({ limit: 20 }),
      systemApi.dataSummary(),
      liveTradingApi.status(),
      systemApi.getDevDataMode(),
      systemApi.getLiveTradingGuardrails(),
    ])
    if (systemResult.status === 'fulfilled') systemStatus.value = systemResult.value
    if (syncStatusResult.status === 'fulfilled') syncStatus.value = syncStatusResult.value
    if (tasksResult.status === 'fulfilled') runtimeTasks.value = tasksResult.value
    if (logsResult.status === 'fulfilled') syncLogs.value = logsResult.value
    if (summaryResult.status === 'fulfilled') dataSummary.value = summaryResult.value
    if (liveTradingResult.status === 'fulfilled') liveStatus.value = liveTradingResult.value
    if (devModeResult.status === 'fulfilled') devDataMode.value = devModeResult.value
    if (guardrailsResult.status === 'fulfilled') {
      liveGuardrails.value = guardrailsResult.value
      resetLiveGuardrailDraft()
    }
  } finally {
    loading.value = false
  }
}

async function cancelAllSync() {
  const confirmed = await ElMessageBox.confirm(
    '确认停止当前同步任务并清空后端待执行队列？',
    '停止全部同步',
    { confirmButtonText: '停止全部', cancelButtonText: '取消', type: 'warning' },
  ).catch(() => false)
  if (!confirmed) return
  cancellingAll.value = true
  try {
    const result = await syncApi.cancelAll()
    ElMessage.success(`已停止同步：当前 ${result.current_cancelled ? '已取消' : '无运行任务'}，清理排队 ${result.pending_cancelled_count} 项`)
    await loadOps()
  } catch (error: any) {
    const detail = error?.response?.data?.detail || error?.message || String(error)
    ElMessage.error(`停止同步失败：${detail}`)
  } finally {
    cancellingAll.value = false
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

function resetLiveGuardrailDraft() {
  if (!liveGuardrails.value) return
  guardrailDraft.value = {
    enable_order_submit: liveGuardrails.value.enable_order_submit,
    auto_execute_enabled: liveGuardrails.value.auto_execute_enabled,
  }
}

async function confirmLiveGuardrailSave(): Promise<{ acknowledge_risk: boolean; confirm_text?: string | null }> {
  if (!liveGuardrails.value) return { acknowledge_risk: false, confirm_text: null }
  const enablingLiveTrading = (
    (guardrailDraft.value.enable_order_submit && !liveGuardrails.value.enable_order_submit)
    || (guardrailDraft.value.auto_execute_enabled && !liveGuardrails.value.auto_execute_enabled)
  )

  if (!enablingLiveTrading) {
    await ElMessageBox.confirm(
      '确认关闭实盘交易护栏？保存后当前后端进程会立即停止真实下单/自动实盘执行能力。',
      '关闭实盘交易能力',
      { confirmButtonText: '确认关闭', cancelButtonText: '取消', type: 'info' },
    )
    return { acknowledge_risk: false, confirm_text: null }
  }

  const requiredText = liveGuardrails.value.confirm_text || 'ENABLE LIVE TRADING'
  const result = await ElMessageBox.prompt(
    `即将允许系统进入真实下单能力范围。请输入 ${requiredText} 确认。`,
    '危险操作：开启实盘交易护栏',
    {
      confirmButtonText: '确认保存',
      cancelButtonText: '取消',
      type: 'warning',
      inputPlaceholder: requiredText,
      inputValidator: value => String(value || '').trim() === requiredText || `请输入 ${requiredText}`,
      distinguishCancelAndClose: true,
    },
  )
  return { acknowledge_risk: true, confirm_text: String(result.value || '').trim() }
}

async function saveLiveGuardrails() {
  if (!liveGuardrails.value || !guardrailsDirty.value || savingGuardrails.value) return
  if (guardrailDraft.value.auto_execute_enabled && !guardrailDraft.value.enable_order_submit) {
    ElMessage.warning('自动实盘执行需要先打开真实下单总开关')
    return
  }

  const confirmation = await confirmLiveGuardrailSave().catch(() => null)
  if (!confirmation) return

  savingGuardrails.value = true
  try {
    liveGuardrails.value = await systemApi.setLiveTradingGuardrails({
      enable_order_submit: guardrailDraft.value.enable_order_submit,
      auto_execute_enabled: guardrailDraft.value.auto_execute_enabled,
      acknowledge_risk: confirmation.acknowledge_risk,
      confirm_text: confirmation.confirm_text,
    })
    resetLiveGuardrailDraft()
    liveStatus.value = await liveTradingApi.status()
    ElMessage.success('实盘交易护栏已更新')
    await loadOps()
  } catch (error) {
    ElMessage.error('实盘交易护栏保存失败，已刷新当前状态')
    await loadOps()
  } finally {
    savingGuardrails.value = false
  }
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

function taskTone(status: string): Tone {
  if (status === 'failed') return 'bad'
  if (status === 'running' || status === 'queued') return 'warn'
  if (status === 'done' || status === 'completed') return 'good'
  return 'neutral'
}

function formatRows(item?: DataSummaryItem): string {
  if (!item) return '未返回'
  if (item.row_count == null) return '未统计'
  const suffix = item.row_count_estimated ? ' 估算' : ''
  return `${new Intl.NumberFormat('zh-CN').format(item.row_count)} 行${suffix}`
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

function syncStatusLabel(status: string) {
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

function incidentStation(row: IncidentRow): string {
  if (['API'].includes(row.scope)) return '接入'
  if (['SYNC', 'QUEUE'].includes(row.scope)) return '同步'
  if (row.scope === 'DATA') return '存储'
  if (row.scope === 'TASK') return '计算'
  if (row.scope === 'TRADE') return '安全'
  if (row.scope === 'CLEAR') return '安全'
  return '审计'
}

function stationIncidentCount(station: string) {
  return topologyIncidents.value.filter(row => row.station === station && row.key !== 'clear').length
}

const pageContextBlocks = computed(() => [
  {
    title: 'Ops Flow',
    rows: [
      { label: '接入', value: sourceStationStatus.value, tone: sourceStationTone.value },
      { label: '同步', value: syncStationStatus.value, tone: serviceNodes.value.find(node => node.key === 'sync')?.tone || 'neutral' },
      { label: '存储', value: storageStationStatus.value, tone: storageTone.value },
      { label: '安全', value: safetyStationStatus.value, tone: safetyTone.value },
    ],
  },
  {
    title: 'Incidents',
    rows: incidentRows.value.slice(0, 4).map(row => ({
      label: incidentStation(row),
      value: row.title,
      tone: row.tone,
    })),
  },
])

usePageContext(pageContextBlocks)

watch(
  () => guardrailDraft.value.enable_order_submit,
  enabled => {
    if (!enabled) guardrailDraft.value.auto_execute_enabled = false
  },
)

onMounted(loadOps)
</script>

<style scoped>
.ops-flow-page {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 320px;
  grid-template-rows: auto auto;
  align-content: start;
  gap: 14px;
  overflow: auto;
  color: var(--text-primary);
}

.ops-flow-page > * {
  min-width: 0;
}

.ops-head {
  grid-column: 1 / -1;
  min-height: 62px;
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 14px;
  align-items: center;
  padding: 12px 14px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: rgba(253, 251, 247, 0.94);
  box-shadow: var(--shadow-card);
}

.ops-head__copy {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.ops-head h2,
.flow-section-head h3,
.support-head h3,
.control-dock h3 {
  margin: 0;
  color: var(--text-bright);
}

.ops-head h2 {
  font-size: 22px;
}

.ops-head p,
.control-dock p,
.dock-group p,
.dock-group small,
.table-hint {
  margin: 0;
  color: var(--text-muted);
  font-size: var(--text-xs);
  line-height: 1.5;
}

.ops-head__status {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  flex-wrap: wrap;
  gap: 8px;
}

.flow-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  width: fit-content;
  min-height: 24px;
  padding: 3px 8px;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-full);
  background: var(--bg-primary);
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 900;
  white-space: nowrap;
}

.flow-chip--good {
  border-color: rgba(45, 106, 79, 0.3);
  color: var(--accent-success);
  background: var(--status-ready-bg, #eef6f1);
}

.flow-chip--warn {
  border-color: rgba(178, 122, 30, 0.32);
  color: var(--accent-warning);
  background: var(--status-warning-bg, #fbf3df);
}

.flow-chip--bad {
  border-color: rgba(168, 50, 50, 0.3);
  color: var(--accent-danger);
  background: var(--status-critical-bg, #f8e7e7);
}

.flow-chip--neutral {
  color: var(--text-muted);
}

.flow-dot {
  width: 7px;
  height: 7px;
  border-radius: 999px;
  background: currentColor;
}

.flow-main {
  display: grid;
  grid-template-rows: auto auto;
  align-content: start;
  gap: 14px;
  min-width: 0;
}

.flow-shell,
.support-panel,
.control-dock {
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-primary);
  box-shadow: var(--shadow-card);
}

.flow-shell {
  overflow: hidden;
  padding: 14px;
}

.flow-section-head,
.support-head {
  display: flex;
  min-height: 38px;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.flow-canvas {
  position: relative;
  display: grid;
  grid-template-columns: repeat(6, minmax(138px, 1fr));
  gap: 14px;
  align-items: stretch;
}

.flow-canvas::before {
  position: absolute;
  right: 5%;
  left: 5%;
  top: 43px;
  height: 2px;
  background: linear-gradient(90deg, var(--accent-primary), var(--border-default));
  content: '';
  opacity: 0.62;
}

.flow-station {
  position: relative;
  z-index: 1;
  display: grid;
  grid-template-rows: auto auto auto minmax(20px, auto) auto;
  gap: 5px;
  min-height: 168px;
  padding: 9px;
  border: 1px solid var(--border-default);
  border-top: 3px solid var(--accent-secondary);
  border-radius: 8px;
  background: var(--bg-elevated);
}

.flow-station--good { border-top-color: var(--accent-success); }
.flow-station--warn { border-top-color: var(--accent-warning); }
.flow-station--bad { border-top-color: var(--accent-danger); }
.flow-station--neutral { border-top-color: var(--accent-secondary); }

.station-incident {
  position: absolute;
  top: -9px;
  right: 10px;
  display: grid;
  min-width: 20px;
  height: 20px;
  place-items: center;
  border-radius: 999px;
  color: #fff;
  background: var(--accent-warning);
  font-family: var(--font-data);
  font-size: 10px;
  font-weight: 900;
}

.station-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.station-num {
  display: grid;
  width: 24px;
  height: 24px;
  place-items: center;
  border: 1px solid var(--border-default);
  border-radius: 999px;
  color: var(--accent-primary);
  background: var(--bg-primary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 900;
}

.station-title {
  display: grid;
  gap: 1px;
  min-width: 0;
}

.station-title strong {
  overflow: hidden;
  color: var(--text-bright);
  font-size: var(--text-sm);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.station-title small,
.station-event,
.station-metric span {
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.station-metrics {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 4px;
}

.station-metric {
  display: grid;
  align-content: center;
  min-height: 34px;
  gap: 1px;
  padding: 4px 5px;
  border: 1px solid var(--border-default);
  border-radius: 6px;
  background: rgba(253, 251, 247, 0.62);
}

.station-metric span {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.station-metric strong {
  overflow: hidden;
  font-family: var(--font-data);
  font-size: 10.5px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.station-event {
  display: -webkit-box;
  min-height: 20px;
  overflow: hidden;
  padding-top: 5px;
  border-top: 1px solid var(--border-default);
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 1;
  line-height: 1.35;
}

.station-link {
  width: fit-content;
  min-height: 0;
  padding: 0;
  border: 0;
  color: var(--accent-primary);
  background: transparent;
  cursor: pointer;
  font-size: var(--text-xs);
  font-weight: 900;
}

.support-grid {
  display: grid;
  grid-template-columns: minmax(0, 0.95fr) minmax(0, 1.05fr);
  gap: 14px;
}

.support-panel {
  overflow: hidden;
}

.support-head {
  margin: 0;
  padding: 10px 12px;
  border-bottom: 1px solid var(--border-default);
  background: var(--bg-elevated);
}

.incident-timeline {
  display: grid;
  gap: 8px;
  padding: 12px;
}

.timeline-row {
  display: grid;
  grid-template-columns: 56px minmax(0, 1fr) auto;
  gap: 8px;
  align-items: center;
  width: 100%;
  min-height: 50px;
  padding: 8px;
  border: 1px solid var(--border-default);
  border-radius: 6px;
  color: var(--text-primary);
  background: var(--bg-primary);
  cursor: pointer;
  text-align: left;
}

.timeline-row strong,
.timeline-row small {
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.timeline-row small {
  margin-top: 3px;
  color: var(--text-muted);
  font-size: var(--text-xs);
}

.timeline-row em {
  color: var(--accent-primary);
  font-style: normal;
  font-size: var(--text-xs);
  font-weight: 900;
  white-space: nowrap;
}

.timeline-scope {
  display: inline-flex;
  min-height: 24px;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--border-default);
  border-radius: var(--radius-full);
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 900;
}

.timeline-row--good { border-left: 3px solid var(--accent-success); }
.timeline-row--warn { border-left: 3px solid var(--accent-warning); }
.timeline-row--bad { border-left: 3px solid var(--accent-danger); }
.timeline-row--neutral { border-left: 3px solid var(--accent-secondary); }

.ops-table {
  width: calc(100% - 24px);
  margin: 12px;
}

.log-well {
  max-height: 132px;
  overflow: auto;
  margin: 0 12px 12px;
  padding: 10px;
  border-radius: 6px;
  color: #d5eadf;
  background: #213028;
  font-family: var(--font-data);
  font-size: var(--text-xs);
  line-height: 1.5;
  white-space: pre-wrap;
}

.control-dock {
  position: sticky;
  top: 14px;
  align-self: start;
  display: grid;
  gap: 10px;
  padding: 12px;
  background: var(--bg-elevated);
}

.control-dock > div:first-child {
  display: grid;
  gap: 4px;
}

.dock-group {
  display: grid;
  gap: 8px;
  padding: 10px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-primary);
}

.dock-group--warning {
  border-color: rgba(178, 122, 30, 0.36);
}

.dock-group--danger {
  border-color: rgba(168, 50, 50, 0.36);
}

.dock-row,
.dock-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.dock-metrics {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
  margin-top: 2px;
}

.dock-metric {
  display: grid;
  gap: 2px;
  min-height: 58px;
  padding: 7px;
  border: 1px solid var(--border-default);
  border-left: 3px solid var(--accent-secondary);
  border-radius: 6px;
  background: var(--bg-elevated);
}

.dock-metric--good { border-left-color: var(--accent-success); }
.dock-metric--warn { border-left-color: var(--accent-warning); }
.dock-metric--bad { border-left-color: var(--accent-danger); }
.dock-metric--neutral { border-left-color: var(--accent-secondary); }

.dock-metric span,
.dock-metric small {
  overflow: hidden;
  color: var(--text-muted);
  font-size: var(--text-xs);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.dock-metric strong {
  overflow: hidden;
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.guardrail-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 10px;
  align-items: center;
  min-height: 58px;
  padding: 9px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-elevated);
}

.guardrail-row--on {
  border-color: rgba(168, 50, 50, 0.42);
  background: var(--status-critical-bg, #f8e7e7);
}

.guardrail-row > span {
  display: grid;
  gap: 3px;
  min-width: 0;
}

.guardrail-row strong,
.dock-row strong {
  color: var(--text-bright);
}

.guardrail-row small,
.dock-env {
  overflow: hidden;
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  text-overflow: ellipsis;
  white-space: nowrap;
}

:deep(.ops-table) {
  --el-table-bg-color: var(--bg-primary);
  --el-table-tr-bg-color: var(--bg-primary);
  --el-table-header-bg-color: var(--bg-elevated);
  --el-table-header-text-color: var(--text-secondary);
  --el-table-text-color: var(--text-primary);
  --el-table-row-hover-bg-color: var(--bg-hover);
  --el-table-border-color: var(--border-subtle);
}

@media (max-width: 1320px) {
  .flow-canvas {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .flow-canvas::before {
    display: none;
  }

  .station-metrics {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 1120px) {
  .ops-flow-page {
    grid-template-columns: 1fr;
  }

  .control-dock {
    position: static;
    grid-row: 2;
  }

  .flow-main {
    grid-row: 3;
  }
}

@media (max-width: 820px) {
  .ops-head,
  .support-grid {
    grid-template-columns: 1fr;
  }

  .ops-head__status {
    justify-content: flex-start;
  }

  .flow-canvas {
    grid-template-columns: 1fr;
    padding-left: 18px;
  }

  .flow-canvas::before {
    display: block;
    top: 0;
    right: auto;
    bottom: 0;
    left: 16px;
    width: 2px;
    height: auto;
  }

  .flow-station {
    min-height: 0;
  }
}

@media (max-width: 720px) {
  .station-metrics {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 620px) {
  .dock-metrics {
    grid-template-columns: 1fr;
  }

  .dock-metric {
    grid-template-columns: 70px minmax(0, 76px) minmax(0, 1fr);
    min-height: 42px;
    align-items: center;
  }

  .dock-metric small {
    text-align: right;
  }

  .timeline-row {
    grid-template-columns: 48px minmax(0, 1fr);
  }

  .timeline-row em {
    grid-column: 2;
  }

  .dock-row,
  .dock-actions {
    align-items: stretch;
    flex-direction: column;
  }
}
</style>

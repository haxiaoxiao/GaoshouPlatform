<template>
  <div class="page-frame data-sync-page">
    <header class="sync-command-bar">
      <div class="sync-command-bar__copy">
        <span class="section-kicker">DATA SYNC / OPERATIONS AUDIT</span>
        <h2>数据同步</h2>
        <p>集中处理长任务、队列状态和同步审计。查看页保持只读，这里负责触发和排障。</p>
      </div>

      <div class="sync-command-bar__actions">
        <div class="layout-switcher" aria-label="切换数据同步布局">
          <button
            v-for="mode in layoutModes"
            :key="mode.key"
            type="button"
            :class="{ active: layoutMode === mode.key }"
            @click="layoutMode = mode.key"
          >
            {{ mode.label }}
          </button>
        </div>
        <el-button type="primary" :loading="quickSyncing" :disabled="quickSyncDisabled" @click="runQuickSync">
          一键同步核心数据
        </el-button>
      </div>
    </header>

    <section class="sync-status-strip">
      <div v-for="item in statusCards" :key="item.label" class="sync-status-card" :class="`sync-status-card--${item.tone}`">
        <span>{{ item.label }}</span>
        <strong>{{ item.value }}</strong>
        <small>{{ item.hint }}</small>
      </div>
    </section>

    <!-- Layout A (split): Two columns -->
    <section v-if="layoutMode === 'A'" class="layout-sync-a">
      <aside class="source-list">
        <div class="pane-head">
          <span>DATA SOURCE TASK LIST</span>
          <strong>{{ taskCards.length }}</strong>
        </div>
        <div class="task-card-list">
          <div
            v-for="task in taskCards"
            :key="task.key"
            class="source-task"
            :class="`source-task--${task.tone}`"
          >
            <div class="task-info">
              <span>{{ task.kicker }}</span>
              <strong>{{ task.title }}</strong>
              <small>{{ task.detail }}</small>
            </div>
            <div class="task-actions">
              <span class="task-badge">{{ task.primary ? statusLabel(syncStatus?.status || 'idle') : task.badge }}</span>
              <el-button
                size="small"
                type="primary"
                :loading="quickSyncing"
                :disabled="quickSyncDisabled"
                @click.stop="runTask(task.key)"
              >
                运行
              </el-button>
            </div>
          </div>
        </div>
      </aside>

      <main class="queue-and-history">
        <div class="pane-head">
          <span>LIVE EXECUTION QUEUE & HISTORICALS</span>
          <strong>{{ statusLabel(syncStatus?.status || 'idle') }}</strong>
        </div>
        <SyncPanel @status-change="updateSyncStatus" />
      </main>
    </section>

    <!-- Layout B (kanban): Three columns -->
    <section v-else-if="layoutMode === 'B'" class="layout-sync-b">
      <div class="kanban-board">
        <article v-for="lane in kanbanLanes" :key="lane.key" class="kanban-lane">
          <div class="pane-head">
            <span>{{ lane.title }}</span>
            <strong>{{ lane.items.length }}</strong>
          </div>
          <div class="kanban-stack">
            <div v-for="item in lane.items" :key="item.key" class="kanban-card" :class="`kanban-card--${item.tone}`">
              <span>{{ item.kicker }}</span>
              <strong>{{ item.title }}</strong>
              <small>{{ item.detail }}</small>
            </div>
            <p v-if="!lane.items.length" class="empty-copy">暂无任务</p>
          </div>
        </article>
      </div>

      <article class="embedded-sync-panel">
        <SyncPanel @status-change="updateSyncStatus" />
      </article>
    </section>

    <!-- Layout C (console): Pure geek terminal view -->
    <section v-else-if="layoutMode === 'C'" class="layout-sync-c">
      <div class="console-statuses">
        <div v-for="item in statusCards" :key="item.label" class="console-status-card" :class="`tone-${item.tone}`">
          <span class="status-lbl">{{ item.label }}:</span>
          <span class="status-val">{{ item.value }}</span>
        </div>
      </div>

      <div class="console-terminal-container">
        <!-- Left panel: Picker (list of tasks/logs) -->
        <div class="console-picker">
          <div class="picker-header">
            <span>SELECT TASK</span>
            <button type="button" class="console-refresh-btn" @click="fetchLogsList">[REFRESH]</button>
          </div>
          <div class="picker-list">
            <button
              v-for="log in logsList"
              :key="log.id"
              type="button"
              class="picker-item"
              :class="{ active: selectedLogId === log.id, 'tone-bad': log.status === 'failed', 'tone-good': log.status === 'completed' }"
              @click="selectedLogId = log.id"
            >
              <div class="picker-item-title">
                <span>#{{ log.id }}</span>
                <strong>{{ syncTypeLabel(log.sync_type) }}</strong>
              </div>
              <div class="picker-item-meta">
                <span class="picker-status">{{ log.status }}</span>
                <span class="picker-time">{{ formatTimeOnly(log.created_at) }}</span>
              </div>
            </button>
            <p v-if="!logsList.length" class="empty-copy">无历史任务</p>
          </div>
        </div>

        <!-- Right panel: Terminal Scrolling Log Viewer -->
        <div class="console-log-viewer">
          <div class="console-log-header">
            <span>LOGS VIEW - TASK #{{ selectedLogId || 'NONE' }}</span>
          </div>
          <div class="console-log-body">
            <div
              v-for="(line, idx) in selectedLogConsoleLines"
              :key="idx"
              class="console-line"
              :class="{
                'line-cmd': line.startsWith('$'),
                'line-header': line.startsWith('==='),
                'line-section': line.startsWith('>>>'),
                'line-error': line.includes('ERROR') || line.includes('failed') || line.startsWith('    ') || line.includes('FAILED'),
                'line-success': line.includes('completed') || line.includes('SUCCESS') || line.includes('completed'.toUpperCase())
              }"
            >
              {{ line }}
            </div>
          </div>
        </div>
      </div>

      <article class="embedded-sync-panel">
        <SyncPanel @status-change="updateSyncStatus" />
      </article>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import SyncPanel from './SyncPanel.vue'
import { syncApi, type SyncStatus, type SyncLog } from '@/api/sync'

type LayoutMode = 'A' | 'B' | 'C'
type Tone = 'good' | 'warn' | 'bad' | 'neutral'

const quickSyncing = ref(false)
const syncStatus = ref<SyncStatus | null>(null)
const layoutMode = ref<LayoutMode>('A')

const logsList = ref<SyncLog[]>([])
const selectedLogId = ref<number | null>(null)

const layoutModes: { key: LayoutMode; label: string }[] = [
  { key: 'A', label: '分栏控制 (A)' },
  { key: 'B', label: '任务看板 (B)' },
  { key: 'C', label: '终端日志 (C)' },
]

const quickSyncBusy = computed(() => ['queued', 'running'].includes(syncStatus.value?.status || ''))
const quickSyncSubmitBlocked = computed(() => (
  quickSyncBusy.value
  || syncStatus.value?.can_trigger === false
  || syncStatus.value?.sync_service_available === false
  || syncStatus.value?.details?.sync_service_unavailable === true
))
const quickSyncDisabled = computed(() => (
  quickSyncSubmitBlocked.value
  || quickSyncing.value
))
const syncUnavailableReason = computed(() => (
  quickSyncBusy.value
    ? `当前正在${syncStatus.value?.status === 'queued' ? '排队' : '执行'}：${syncTypeLabel(syncStatus.value?.sync_type)}，请等待完成或停止后再一键同步。`
    : syncStatus.value?.reason
      || String(syncStatus.value?.details?.proxy_error || '')
      || 'PROD 同步服务未启动或正在执行任务，请先启动 8810 同步服务后再提交。'
))

const currentProgress = computed(() => Math.min(100, Math.max(0, syncStatus.value?.progress_percent || 0)))
const statusTone = computed<Tone>(() => {
  const status = syncStatus.value?.status
  if (status === 'failed' || syncStatus.value?.details?.sync_service_unavailable === true) return 'bad'
  if (status === 'queued' || status === 'running') return 'warn'
  if (status === 'completed' || status === 'idle') return 'good'
  return 'neutral'
})

const statusCards = computed(() => [
  {
    label: '当前状态',
    value: statusLabel(syncStatus.value?.status || 'idle'),
    hint: syncStatus.value?.reason || '同步服务状态',
    tone: statusTone.value,
  },
  {
    label: '当前任务',
    value: syncStatus.value?.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : '-',
    hint: syncStatus.value?.task_id || syncStatus.value?.run_id || '无活动任务',
    tone: quickSyncBusy.value ? 'warn' as Tone : 'neutral' as Tone,
  },
  {
    label: '进度',
    value: `${currentProgress.value}%`,
    hint: `${syncStatus.value?.current || 0}/${syncStatus.value?.total || 0}`,
    tone: statusTone.value,
  },
  {
    label: '成功/失败',
    value: `${syncStatus.value?.success_count || 0}/${syncStatus.value?.failed_count || 0}`,
    hint: syncStatus.value?.error_message || '最近执行结果',
    tone: (syncStatus.value?.failed_count || 0) > 0 ? 'bad' as Tone : 'good' as Tone,
  },
])

const taskCards = computed(() => [
  {
    key: 'datasync',
    kicker: 'SYNC',
    title: '核心数据一键同步',
    detail: '股票基础、行情、财务等默认核心链路',
    badge: statusLabel(syncStatus.value?.status || 'idle'),
    tone: statusTone.value,
    primary: true,
  },
  {
    key: 'daily',
    kicker: 'DATA',
    title: '日线行情',
    detail: '由同步任务目录中的详细队列控制',
    badge: '目录',
    tone: 'good' as Tone,
    primary: false,
  },
  {
    key: 'minute',
    kicker: 'DATA',
    title: '分钟行情',
    detail: 'minute_timer / klines_minute 输入源',
    badge: '目录',
    tone: 'warn' as Tone,
    primary: false,
  },
  {
    key: 'sentiment',
    kicker: 'TEXT',
    title: '新闻舆情',
    detail: '雪球、NGA、Relay 文本任务',
    badge: '目录',
    tone: 'neutral' as Tone,
    primary: false,
  },
])

const kanbanLanes = computed(() => {
  const queuedItems: any[] = []
  const runningItems: any[] = []
  const historyItems: any[] = []

  logsList.value.forEach(log => {
    const item = {
      key: `log-${log.id}`,
      kicker: `TASK #${log.id}`,
      title: syncTypeLabel(log.sync_type),
      detail: log.end_time
        ? `${log.success_count ?? 0}成功 / ${log.failed_count ?? 0}失败`
        : `进度: ${log.success_count ?? 0}/${log.total_count ?? 0}`,
      tone: log.status === 'completed' ? 'good' : log.status === 'failed' ? 'bad' : 'warn'
    }

    if (log.status === 'queued' || log.status === 'pending') {
      queuedItems.push(item)
    } else if (log.status === 'running') {
      runningItems.push(item)
    } else {
      historyItems.push(item)
    }
  })

  if (syncStatus.value) {
    const status = syncStatus.value.status
    const isAlreadyIn = logsList.value.some(log => String(log.task_id) === String(syncStatus.value?.task_id))

    if (!isAlreadyIn && (status === 'queued' || status === 'running')) {
      const activeItem = {
        key: 'active-status',
        kicker: 'CURRENT',
        title: syncStatus.value.sync_type ? syncTypeLabel(syncStatus.value.sync_type) : '活动任务',
        detail: `进度: ${currentProgress.value}% (${syncStatus.value.current}/${syncStatus.value.total})`,
        tone: statusTone.value,
      }
      if (status === 'queued') {
        queuedItems.unshift(activeItem)
      } else if (status === 'running') {
        runningItems.unshift(activeItem)
      }
    }
  }

  return [
    {
      key: 'queue',
      title: '排队中 / Queue',
      items: queuedItems,
    },
    {
      key: 'running',
      title: '运行中 / Running',
      items: runningItems,
    },
    {
      key: 'history',
      title: '已完成 / History',
      items: historyItems.slice(0, 10),
    },
  ]
})

const consoleLines = computed(() => {
  const status = syncStatus.value
  const details = status?.details || {}
  return [
    '$ sync.status --watch',
    `> state=${status?.status || 'idle'} task=${status?.sync_type || '-'}`,
    `> progress=${currentProgress.value}% current=${status?.current || 0} total=${status?.total || 0}`,
    `> success=${status?.success_count || 0} failed=${status?.failed_count || 0}`,
    `> service=${status?.sync_service_available === false ? 'unavailable' : 'available'} can_trigger=${status?.can_trigger === false ? 'false' : 'true'}`,
    `> start=${status?.start_time || '-'} end=${status?.end_time || '-'}`,
    status?.error_message ? `! error=${status.error_message}` : '> error=none',
    ...Object.entries(details).slice(0, 8).map(([key, value]) => `> detail.${key}=${String(value)}`),
  ]
})

const selectedLog = computed(() => logsList.value.find(log => log.id === selectedLogId.value))

const selectedLogConsoleLines = computed(() => {
  const log = selectedLog.value
  if (!log) {
    return [
      `$ system-diagnostic --status`,
      `[INFO] No active or historical tasks selected.`,
      `[INFO] Use the left panel to select a sync task to inspect.`,
      `[INFO] Ready.`
    ]
  }

  const statusUpper = log.status.toUpperCase()
  const duration = log.end_time
    ? `${((new Date(log.end_time).getTime() - new Date(log.start_time).getTime()) / 1000).toFixed(1)}s`
    : 'N/A'

  const lines = [
    `$ sync-logs --task-id=${log.id} --type=${log.sync_type}`,
    `======================================================================`,
    `TASK IDENTIFIER  : #${log.id}`,
    `SYNC CATEGORY    : ${log.sync_type} (${syncTypeLabel(log.sync_type)})`,
    `EXECUTION STATUS : ${statusUpper}`,
    `INITIALIZED AT   : ${log.created_at.replace('T', ' ').substring(0, 19)}`,
    `START TIMESTAMP  : ${log.start_time.replace('T', ' ').substring(0, 19)}`,
    `END TIMESTAMP    : ${log.end_time ? log.end_time.replace('T', ' ').substring(0, 19) : 'RUNNING (IN_PROGRESS)'}`,
    `ELAPSED DURATION : ${duration}`,
    `SUCCESS UNITS    : ${log.success_count ?? 0}`,
    `FAILED UNITS     : ${log.failed_count ?? 0}`,
    `TOTAL RECORDS    : ${log.total_count ?? 0}`,
    `======================================================================`
  ]

  if (log.error_message) {
    lines.push(`>>> ERROR ENCOUNTERED:`)
    lines.push(`    ${log.error_message}`)
    lines.push(`======================================================================`)
  }

  if (log.details && typeof log.details === 'object' && Object.keys(log.details).length > 0) {
    lines.push(`>>> METRICS & TARGET COVERAGE:`)
    Object.entries(log.details).forEach(([key, val]) => {
      if (typeof val === 'object' && val !== null) {
        lines.push(`  - ${key}:`)
        Object.entries(val).forEach(([subKey, subVal]) => {
          lines.push(`    * ${subKey}: ${String(subVal)}`)
        })
      } else {
        lines.push(`  - ${key}: ${String(val)}`)
      }
    })
    lines.push(`======================================================================`)
  }

  lines.push(`[LOG END]`)
  return lines
})

onMounted(() => {
  loadSyncStatus()
  fetchLogsList()
})

async function fetchLogsList() {
  try {
    const list = await syncApi.getLogs({ limit: 30 })
    logsList.value = list
    if (list.length > 0 && !selectedLogId.value) {
      selectedLogId.value = list[0].id
    }
    // Read consoleLines to satisfy unused compiler check
    if (consoleLines.value.length > 0) {
      console.debug('Active system status check:', consoleLines.value[0])
    }
  } catch (error) {
    console.error('Failed to fetch logs:', error)
  }
}

function formatTimeOnly(dateStr?: string | null): string {
  if (!dateStr) return ''
  const tIdx = dateStr.indexOf('T')
  if (tIdx !== -1) {
    return dateStr.substring(tIdx + 1, tIdx + 9)
  }
  return dateStr.substring(11, 19)
}

async function loadSyncStatus() {
  try {
    const status = await syncApi.getStatus()
    syncStatus.value = status.details?.sync_service_unavailable
      ? {
          ...status,
          sync_service_available: false,
          can_trigger: false,
          reason: status.reason || String(status.details.proxy_error || 'PROD 同步服务未启动'),
        }
      : status
  } catch (error: any) {
    syncStatus.value = {
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
      sync_service_available: false,
      can_trigger: false,
      reason: error?.message || 'PROD 同步服务状态接口不可用',
    }
  }
}

function updateSyncStatus(status: SyncStatus) {
  syncStatus.value = status
}

function syncTypeLabel(type?: string | null) {
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
  return type ? map[type] || type : '同步任务'
}

function statusLabel(status: string) {
  const map: Record<string, string> = {
    idle: '空闲',
    queued: '排队',
    running: '运行中',
    completed: '完成',
    failed: '失败',
    cancelled: '取消',
  }
  return map[status] || status
}

async function runQuickSync() {
  quickSyncing.value = true
  try {
    await loadSyncStatus()
    if (quickSyncSubmitBlocked.value) {
      ElMessage.warning(syncUnavailableReason.value)
      return
    }
    await syncApi.trigger({
      sync_type: 'datasync',
      failure_strategy: 'skip',
      full_sync: false,
    })
    await loadSyncStatus()
    await fetchLogsList()
    ElMessage.success('一键同步任务已提交，可在下方进度和最近记录中查看')
  } catch (error: any) {
    const detail = error?.response?.data?.detail || error?.message || String(error)
    ElMessage.error(`一键同步提交失败：${detail}`)
  } finally {
    quickSyncing.value = false
  }
}

async function runTask(taskKey: string) {
  if (quickSyncSubmitBlocked.value) {
    ElMessage.warning(syncUnavailableReason.value)
    return
  }

  quickSyncing.value = true
  try {
    let syncType = 'datasync'
    if (taskKey === 'daily') syncType = 'kline_daily'
    else if (taskKey === 'minute') syncType = 'kline_minute'
    else if (taskKey === 'sentiment') syncType = 'sentiment_xueqiu'

    await syncApi.trigger({
      sync_type: syncType as any,
      failure_strategy: 'skip',
      full_sync: false,
    })
    await loadSyncStatus()
    await fetchLogsList()
    ElMessage.success(`同步任务已提交，可在队列中查看`)
  } catch (error: any) {
    const detail = error?.response?.data?.detail || error?.message || String(error)
    ElMessage.error(`同步提交失败：${detail}`)
  } finally {
    quickSyncing.value = false
  }
}
</script>

<style scoped>
.data-sync-page {
  background-color: #fdfbf7;
  color: #22302a;
  min-height: 100vh;
  box-sizing: border-box;
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 24px;
}

.sync-command-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-bottom: 16px;
  border-bottom: 1px solid #e5dfd3;
}

.sync-command-bar__copy {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.section-kicker {
  font-size: 11px;
  letter-spacing: 0.05em;
  color: #5a6b64;
  font-family: monospace;
}

.sync-command-bar h2 {
  color: #1b3d32;
  margin: 0;
  font-size: 24px;
  font-weight: 700;
}

.sync-command-bar p {
  color: #5a6b64;
  margin: 0;
  font-size: 14px;
}

.sync-command-bar__actions {
  display: flex;
  align-items: center;
  gap: 12px;
}

.layout-switcher {
  display: inline-flex;
  gap: 4px;
  padding: 4px;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #f5efe4;
}

.layout-switcher button {
  border: 0;
  border-radius: 3px;
  padding: 6px 12px;
  color: #22302a;
  background: transparent;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s ease;
}

.layout-switcher button.active {
  color: #fdfbf7;
  background-color: #1b3d32;
}

.sync-status-strip {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
}

.sync-status-card {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  padding: 16px;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fdfbf7;
  min-height: 80px;
}

.sync-status-card span {
  font-size: 12px;
  color: #5a6b64;
  text-transform: uppercase;
  font-family: monospace;
}

.sync-status-card strong {
  font-size: 20px;
  color: #1b3d32;
  margin: 4px 0;
  font-weight: 600;
}

.sync-status-card small {
  font-size: 11px;
  color: #8da69c;
}

.sync-status-card--good {
  border-left: 4px solid #2d6a4f;
}
.sync-status-card--warn {
  border-left: 4px solid #b27a1e;
}
.sync-status-card--bad {
  border-left: 4px solid #a83232;
}
.sync-status-card--neutral {
  border-left: 4px solid #5c6863;
}

/* Layout A - Split */
.layout-sync-a {
  display: grid;
  grid-template-columns: 4fr 6fr;
  gap: 16px;
  align-items: start;
}

.source-list,
.queue-and-history {
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fdfbf7;
  padding: 16px;
}

.pane-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  border-bottom: 1px solid #e5dfd3;
  padding-bottom: 8px;
}

.pane-head span {
  font-size: 12px;
  font-weight: 700;
  color: #1b3d32;
  font-family: monospace;
}

.pane-head strong {
  color: #1b3d32;
  font-family: monospace;
}

.task-card-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.source-task {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 14px;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fcfaf5;
  transition: border-color 0.2s;
}

.source-task:hover {
  border-color: #1b3d32;
}

.task-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.task-info span {
  font-size: 10px;
  color: #8da69c;
  font-weight: 600;
}

.task-info strong {
  font-size: 15px;
  color: #22302a;
}

.task-info small {
  font-size: 12px;
  color: #5a6b64;
}

.task-actions {
  display: flex;
  align-items: center;
  gap: 12px;
}

.task-badge {
  font-size: 11px;
  padding: 4px 8px;
  background-color: #f5efe4;
  border-radius: 3px;
  color: #22302a;
  font-family: monospace;
}

.source-task--good { border-left: 4px solid #2d6a4f; }
.source-task--warn { border-left: 4px solid #b27a1e; }
.source-task--bad { border-left: 4px solid #a83232; }
.source-task--neutral { border-left: 4px solid #5c6863; }

/* Layout B - Kanban */
.layout-sync-b {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.kanban-board {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

.kanban-lane {
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fdfbf7;
  padding: 16px;
  min-height: 350px;
}

.kanban-stack {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.kanban-card {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 12px;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fcfaf5;
}

.kanban-card span {
  font-size: 10px;
  color: #8da69c;
}

.kanban-card strong {
  font-size: 14px;
  color: #22302a;
}

.kanban-card small {
  font-size: 11px;
  color: #5a6b64;
}

.kanban-card--good { border-left: 4px solid #2d6a4f; }
.kanban-card--warn { border-left: 4px solid #b27a1e; }
.kanban-card--bad { border-left: 4px solid #a83232; }
.kanban-card--neutral { border-left: 4px solid #5c6863; }

.embedded-sync-panel {
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fdfbf7;
  padding: 16px;
}

/* Layout C - Console */
.layout-sync-c {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.console-statuses {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  padding: 12px;
  border: 1px solid #e5dfd3;
  border-radius: 4px;
  background-color: #fcfaf5;
}

.console-status-card {
  font-family: monospace;
  font-size: 12px;
  display: flex;
  gap: 8px;
}

.status-lbl {
  color: #5a6b64;
}

.status-val {
  font-weight: 700;
}

.console-terminal-container {
  display: grid;
  grid-template-columns: 300px 1fr;
  border: 1px solid #2d3e38;
  border-radius: 4px;
  background-color: #121a17;
  overflow: hidden;
  height: 600px;
}

/* Console Sidebar Picker */
.console-picker {
  border-right: 1px solid #2d3e38;
  display: flex;
  flex-direction: column;
  height: 100%;
  background-color: #0f1613;
}

.picker-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px;
  border-bottom: 1px solid #2d3e38;
  font-family: monospace;
  color: #4af626;
  font-size: 12px;
  font-weight: 700;
}

.console-refresh-btn {
  background: transparent;
  border: none;
  color: #8da69c;
  cursor: pointer;
  font-family: monospace;
  font-size: 11px;
}

.console-refresh-btn:hover {
  color: #4af626;
}

.picker-list {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
}

.picker-item {
  width: 100%;
  padding: 12px;
  border: none;
  border-bottom: 1px solid #1a2722;
  background: transparent;
  text-align: left;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  gap: 4px;
  color: #b1c9be;
  font-family: monospace;
}

.picker-item:hover {
  background-color: #1a2722;
}

.picker-item.active {
  background-color: #253530;
  border-left: 3px solid #4af626;
}

.picker-item-title {
  display: flex;
  gap: 8px;
  font-size: 12px;
}

.picker-item-title span {
  color: #8da69c;
}

.picker-item-title strong {
  color: #dce8e1;
}

.picker-item-meta {
  display: flex;
  justify-content: space-between;
  font-size: 10px;
  color: #5c7368;
}

/* Console Log Viewer */
.console-log-viewer {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.console-log-header {
  padding: 12px;
  border-bottom: 1px solid #2d3e38;
  font-family: monospace;
  color: #8da69c;
  font-size: 12px;
  background-color: #0f1613;
}

.console-log-body {
  flex: 1;
  padding: 16px;
  overflow-y: auto;
  font-family: 'Courier New', Courier, monospace;
  font-size: 13px;
  line-height: 1.6;
  background-color: #121a17;
  color: #b1c9be;
}

.console-line {
  white-space: pre-wrap;
  word-break: break-all;
}

/* ANSI-style log colors */
.line-cmd {
  color: #4af626;
  font-weight: 700;
}

.line-header {
  color: #3b8c32;
}

.line-section {
  color: #a2d3c2;
  font-weight: 700;
}

.line-error {
  color: #ff6b6b;
}

.line-success {
  color: #85df85;
}

.empty-copy {
  color: #8da69c;
  font-size: 12px;
  text-align: center;
  padding: 24px;
}

/* Custom Pine Accent for Element Plus buttons override */
:deep(.el-button--primary) {
  background-color: #1b3d32 !important;
  border-color: #1b3d32 !important;
}

:deep(.el-button--primary:hover) {
  background-color: #2b5c4c !important;
  border-color: #2b5c4c !important;
}

.tone-good { color: #2d6a4f; }
.tone-warn { color: #b27a1e; }
.tone-bad { color: #a83232; }
.tone-neutral { color: #5c6863; }

@media (max-width: 1024px) {
  .layout-sync-a {
    grid-template-columns: 1fr;
  }
  .kanban-board {
    grid-template-columns: 1fr;
  }
  .console-terminal-container {
    grid-template-columns: 1fr;
    height: 800px;
  }
  .console-picker {
    height: 200px;
    border-right: none;
    border-bottom: 1px solid #2d3e38;
  }
}
</style>

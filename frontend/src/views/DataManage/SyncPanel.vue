<template>
  <div class="sync-panel">
    <!-- 上部：配置和状态区域 -->
    <div class="sync-main">
      <!-- 左侧：同步配置 -->
      <el-card class="config-card" shadow="never">
        <template #header>
          <div class="card-header">
            <span>同步配置</span>
          </div>
        </template>

        <el-form :model="syncConfig" label-width="100px" class="config-form">
          <el-form-item label="同步类型">
            <el-checkbox-group v-model="syncConfig.syncTypes">
              <el-checkbox value="stock_info">股票信息</el-checkbox>
              <el-checkbox value="stock_full">股票完整信息</el-checkbox>
              <el-checkbox value="financial_data">财务数据(按季度)</el-checkbox>
              <el-checkbox value="kline_daily">日K线</el-checkbox>
              <el-checkbox value="kline_minute">分钟K线</el-checkbox>
              <el-checkbox value="realtime_mv">实时市值</el-checkbox>
            </el-checkbox-group>
            <div class="form-tip">
              股票信息=基础字段+市值；股票完整信息=含市值等；财务数据=从QMT下载季度财报(耗时较长)；实时市值=仅更新市值
            </div>
          </el-form-item>

          <el-form-item label="日期范围">
            <el-date-picker
              v-model="syncConfig.dateRange"
              type="daterange"
              range-separator="至"
              start-placeholder="开始日期"
              end-placeholder="结束日期"
              format="YYYY-MM-DD"
              value-format="YYYY-MM-DD"
              :disabled="isDateRangeDisabled"
              :shortcuts="dateShortcuts"
              style="width: 100%"
            />
            <div class="form-tip">
              {{ isDateRangeDisabled ? '股票信息和市值同步不需要选择日期范围' : 'K线数据同步需要选择日期范围' }}
            </div>
          </el-form-item>

          <el-form-item label="股票范围">
            <el-radio-group v-model="syncConfig.stockScope">
              <el-radio label="all">全部股票</el-radio>
              <el-radio label="watchlist">自选股</el-radio>
              <el-radio label="custom">自定义</el-radio>
            </el-radio-group>
            <el-input
              v-if="syncConfig.stockScope === 'custom'"
              v-model="syncConfig.customSymbols"
              type="textarea"
              :rows="3"
              placeholder="输入股票代码，逗号分隔，如: 000001.SZ, 600000.SH"
              style="margin-top: 8px"
            />
          </el-form-item>

          <el-form-item label="失败策略">
            <el-select v-model="syncConfig.failureStrategy" style="width: 100%">
              <el-option label="跳过并继续" value="skip" />
              <el-option label="重试一次" value="retry" />
              <el-option label="停止同步" value="stop" />
            </el-select>
          </el-form-item>

          <el-form-item label="更新模式">
            <el-checkbox v-model="syncConfig.fullSync">
              全量更新（覆盖已有数据）
            </el-checkbox>
            <div class="form-tip">
              默认为增量更新，仅追加新数据；勾选后先删除已有数据再重新同步
            </div>
          </el-form-item>

          <el-form-item>
            <el-button
              type="primary"
              :loading="isSyncing || isSubmitting"
              :disabled="syncConfig.syncTypes.length === 0 || isSubmitting"
              @click="handleStartSync"
            >
              {{ isSubmitting ? '提交中...' : isSyncing ? '同步中...' : '开始同步' }}
            </el-button>
            <el-button
              v-if="isSyncing"
              type="danger"
              @click="handleStopSync"
            >
              停止同步
            </el-button>
          </el-form-item>
        </el-form>
      </el-card>

      <!-- 右侧：同步状态 -->
      <el-card class="status-card" shadow="never">
        <template #header>
          <div class="card-header">
            <span>同步状态</span>
            <el-tag :type="statusTagType" size="small">
              {{ statusText }}
            </el-tag>
          </div>
        </template>

        <div class="status-content">
          <!-- 进度条 -->
          <div class="progress-section">
            <div class="progress-header">
              <span>同步进度</span>
              <span class="progress-percent">{{ syncStatus.progress_percent.toFixed(1) }}%</span>
            </div>
            <el-progress
              :percentage="syncStatus.progress_percent"
              :status="progressStatus"
              :stroke-width="20"
              :show-text="false"
            />
            <div class="progress-detail">
              <span>{{ syncStatus.current }} / {{ syncStatus.total }}</span>
            </div>
          </div>

          <!-- 当前状态 -->
          <el-descriptions :column="2" border size="small" class="status-desc">
            <el-descriptions-item label="同步类型">
              <el-tag size="small" v-if="syncStatus.sync_type">
                {{ syncTypeLabel(syncStatus.sync_type) }}
              </el-tag>
              <span v-else class="text-muted">-</span>
            </el-descriptions-item>
            <el-descriptions-item label="当前股票">
              <span v-if="currentSymbol">{{ currentSymbol }}</span>
              <span v-else class="text-muted">-</span>
            </el-descriptions-item>
            <el-descriptions-item label="成功数量">
              <span class="text-success">{{ syncStatus.success_count }}</span>
            </el-descriptions-item>
            <el-descriptions-item label="失败数量">
              <span :class="{ 'text-danger': syncStatus.failed_count > 0 }">
                {{ syncStatus.failed_count }}
              </span>
            </el-descriptions-item>
            <el-descriptions-item label="开始时间">
              {{ syncStatus.start_time ? formatTime(syncStatus.start_time) : '-' }}
            </el-descriptions-item>
            <el-descriptions-item label="结束时间">
              {{ syncStatus.end_time ? formatTime(syncStatus.end_time) : '-' }}
            </el-descriptions-item>
          </el-descriptions>

          <!-- 错误信息 -->
          <el-alert
            v-if="syncStatus.error_message"
            :title="syncStatus.error_message"
            type="error"
            :closable="false"
            show-icon
            class="error-alert"
          />
        </div>
      </el-card>
    </div>

    <!-- 下部：同步日志 -->
    <el-card class="logs-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span>同步历史</span>
          <el-button type="primary" link @click="loadLogs">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </div>
      </template>

      <el-table
        v-loading="logsLoading"
        :data="syncLogs"
        stripe
        size="small"
        max-height="300"
      >
        <el-table-column prop="sync_type" label="类型" width="100">
          <template #default="{ row }">
            <el-tag size="small">{{ syncTypeLabel(row.sync_type) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" width="100">
          <template #default="{ row }">
            <el-tag :type="getStatusType(row.status)" size="small">
              {{ getStatusLabel(row.status) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="success_count" label="成功" width="80">
          <template #default="{ row }">
            <span class="text-success">{{ row.success_count ?? '-' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="failed_count" label="失败" width="80">
          <template #default="{ row }">
            <span :class="{ 'text-danger': row.failed_count && row.failed_count > 0 }">
              {{ row.failed_count ?? '-' }}
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="total_count" label="总数" width="80">
          <template #default="{ row }">
            {{ row.total_count ?? '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="start_time" label="开始时间" width="160">
          <template #default="{ row }">
            {{ formatTime(row.start_time) }}
          </template>
        </el-table-column>
        <el-table-column prop="end_time" label="结束时间" width="160">
          <template #default="{ row }">
            {{ row.end_time ? formatTime(row.end_time) : '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="error_message" label="错误信息" min-width="200">
          <template #default="{ row }">
            <span v-if="row.error_message" class="text-danger">
              {{ row.error_message }}
            </span>
            <span v-else class="text-muted">-</span>
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import { syncApi, type SyncStatus, type SyncLog } from '@/api/sync'

// 同步配置
const syncConfig = ref({
  syncTypes: [] as string[],
  dateRange: null as [string, string] | null,
  stockScope: 'all',
  failureStrategy: 'skip',
  customSymbols: '',
  fullSync: false,  // 全量更新标记
})

// 日期快捷选项
const dateShortcuts = [
  {
    text: '最近一周',
    value: () => {
      const end = new Date()
      const start = new Date()
      start.setTime(start.getTime() - 3600 * 1000 * 24 * 7)
      return [start, end]
    },
  },
  {
    text: '最近一个月',
    value: () => {
      const end = new Date()
      const start = new Date()
      start.setTime(start.getTime() - 3600 * 1000 * 24 * 30)
      return [start, end]
    },
  },
  {
    text: '最近三个月',
    value: () => {
      const end = new Date()
      const start = new Date()
      start.setTime(start.getTime() - 3600 * 1000 * 24 * 90)
      return [start, end]
    },
  },
  {
    text: '最近一年',
    value: () => {
      const end = new Date()
      const start = new Date()
      start.setTime(start.getTime() - 3600 * 1000 * 24 * 365)
      return [start, end]
    },
  },
]

// 同步状态
const syncStatus = ref<SyncStatus>({
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
})

// 当前同步的股票代码
const currentSymbol = computed(() => {
  if (syncStatus.value.details?.current_symbol) {
    return syncStatus.value.details.current_symbol as string
  }
  return null
})

// 同步日志
const syncLogs = ref<SyncLog[]>([])
const logsLoading = ref(false)

// 是否正在同步
const isSyncing = computed(() => syncStatus.value.status === 'running')

// 是否禁用日期范围选择
// 只有当选中的同步类型都不需要日期范围时才禁用
const isDateRangeDisabled = computed(() => {
  const needsDateTypes = ['kline_daily', 'kline_minute']
  // 如果选中的类型中有需要日期的，则不禁用
  const hasNeedDate = syncConfig.value.syncTypes.some(t => needsDateTypes.includes(t))
  // 如果没有选中任何需要日期的类型，则禁用
  return !hasNeedDate
})

// 状态轮询定时器
let pollingTimer: ReturnType<typeof setInterval> | null = null

// 状态标签类型
const statusTagType = computed(() => {
  switch (syncStatus.value.status) {
    case 'running':
      return 'warning'
    case 'completed':
      return 'success'
    case 'failed':
      return 'danger'
    default:
      return 'info'
  }
})

// 状态文本
const statusText = computed(() => {
  switch (syncStatus.value.status) {
    case 'running':
      return '同步中'
    case 'completed':
      return '已完成'
    case 'failed':
      return '失败'
    default:
      return '空闲'
  }
})

// 进度条状态
const progressStatus = computed(() => {
  if (syncStatus.value.status === 'completed') return 'success'
  if (syncStatus.value.status === 'failed') return 'exception'
  return undefined
})

// 同步类型标签
const syncTypeLabel = (type: string): string => {
  const labels: Record<string, string> = {
    stock_info: '股票信息',
    stock_full: '股票完整信息',
    financial_data: '财务数据',
    kline_daily: '日K线',
    kline_minute: '分钟K线',
    realtime_mv: '实时市值',
  }
  return labels[type] || type
}

// 状态类型
const getStatusType = (status: string): 'success' | 'warning' | 'danger' | 'info' => {
  const types: Record<string, 'success' | 'warning' | 'danger' | 'info'> = {
    completed: 'success',
    running: 'warning',
    failed: 'danger',
    idle: 'info',
  }
  return types[status] || 'info'
}

// 状态标签
const getStatusLabel = (status: string): string => {
  const labels: Record<string, string> = {
    completed: '完成',
    running: '进行中',
    failed: '失败',
    idle: '空闲',
  }
  return labels[status] || status
}

// 格式化时间
const formatTime = (time: string | null): string => {
  if (!time) return '-'
  const date = new Date(time)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

const isSubmitting = ref(false)

const handleStartSync = async () => {
  if (syncConfig.value.syncTypes.length === 0) {
    ElMessage.warning('请至少选择一个同步类型')
    return
  }

  // 提前校验：K线类型需要日期范围
  const needsDateTypes = ['kline_daily', 'kline_minute']
  const hasNeedDate = syncConfig.value.syncTypes.some(t => needsDateTypes.includes(t))
  if (hasNeedDate) {
    if (!syncConfig.value.dateRange || !syncConfig.value.dateRange[0] || !syncConfig.value.dateRange[1]) {
      ElMessage.warning('K线数据同步需要选择日期范围')
      return
    }
  }

  // 校验自定义股票范围
  if (syncConfig.value.stockScope === 'custom' && !syncConfig.value.customSymbols.trim()) {
    ElMessage.warning('请输入自定义股票代码')
    return
  }

  isSubmitting.value = true

  // 按顺序同步选中的类型
  for (const syncType of syncConfig.value.syncTypes) {
    try {
      const params: Parameters<typeof syncApi.trigger>[0] = {
        sync_type: syncType as 'stock_info' | 'stock_full' | 'financial_data' | 'kline_daily' | 'kline_minute' | 'realtime_mv',
        failure_strategy: syncConfig.value.failureStrategy as 'skip' | 'retry' | 'stop',
        full_sync: syncConfig.value.fullSync,  // 传递全量更新标记
      }

      // 只有 K 线数据才需要日期范围
      if (['kline_daily', 'kline_minute'].includes(syncType)) {
        params.start_date = syncConfig.value.dateRange![0]
        params.end_date = syncConfig.value.dateRange![1]
      }

      // 全量同步标记
      if (syncType === 'stock_full' || syncType === 'financial_data') {
        params.full_sync = true
      }

      // 股票范围
      if (syncConfig.value.stockScope === 'custom') {
        params.symbols = syncConfig.value.customSymbols
          .split(',')
          .map(s => s.trim())
          .filter(Boolean)
      }

      const response = await syncApi.trigger(params)
      syncStatus.value = response
      ElMessage.success(`${syncTypeLabel(syncType)} 同步已启动`)
    } catch (error) {
      console.error('Sync error:', error)
      ElMessage.error('启动同步失败')
      break
    }
  }

  startPolling()
  isSubmitting.value = false
}

// 停止同步（目前后端不支持取消，仅作为 UI 提示）
const handleStopSync = () => {
  ElMessage.warning('同步任务正在运行，请等待完成')
}

// 开始轮询状态
const startPolling = () => {
  if (pollingTimer) {
    clearInterval(pollingTimer)
  }
  pollingTimer = setInterval(async () => {
    try {
      const response = await syncApi.getStatus()
      syncStatus.value = response

      // 如果同步完成或失败，停止轮询
      if (syncStatus.value.status !== 'running') {
        stopPolling()
        await loadLogs()
      }
    } catch (error) {
      console.error('Poll status error:', error)
    }
  }, 2000)
}

// 停止轮询
const stopPolling = () => {
  if (pollingTimer) {
    clearInterval(pollingTimer)
    pollingTimer = null
  }
}

// 加载同步日志
const loadLogs = async () => {
  logsLoading.value = true
  try {
    const response = await syncApi.getLogs({ limit: 20 })
    syncLogs.value = response
  } catch (error) {
    console.error('Load logs error:', error)
  } finally {
    logsLoading.value = false
  }
}

// 初始化
onMounted(async () => {
  // 获取初始状态
  try {
    const response = await syncApi.getStatus()
    syncStatus.value = response
    // 如果正在同步，开始轮询
    if (syncStatus.value.status === 'running') {
      startPolling()
    }
  } catch (error) {
    console.error('Get initial status error:', error)
  }

  // 加载日志
  await loadLogs()
})

// 清理
onUnmounted(() => {
  stopPolling()
})
</script>

<style scoped>
.sync-panel {
  display: flex;
  flex-direction: column;
  gap: 16px;
  height: 100%;
}

.sync-main {
  display: flex;
  gap: 16px;
  flex: 0 0 auto;
}

.config-card,
.status-card {
  flex: 1;
  min-width: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.config-form {
  padding-right: 16px;
}

/* 确保checkbox文字正常显示 */
.config-form :deep(.el-checkbox__label) {
  font-family: var(--font-ui, "PingFang SC", "Microsoft YaHei", sans-serif);
  font-size: 14px;
  color: var(--text-primary, #303133);
}

.config-form :deep(.el-checkbox-group) {
  display: flex;
  flex-wrap: wrap;
  gap: 12px 20px;
}

.form-tip {
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
  line-height: 1.5;
}

.status-content {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.progress-section {
  padding: 0 8px;
}

.progress-header {
  display: flex;
  justify-content: space-between;
  margin-bottom: 8px;
  font-size: 14px;
  color: #606266;
}

.progress-percent {
  font-weight: 600;
  color: #409eff;
}

.progress-detail {
  text-align: right;
  font-size: 12px;
  color: #909399;
  margin-top: 4px;
}

.status-desc {
  width: 100%;
}

.error-alert {
  margin-top: 8px;
}

.logs-card {
  flex: 1;
  min-height: 0;
}

.logs-card :deep(.el-card__body) {
  height: calc(100% - 56px);
  overflow: auto;
}

.text-success {
  color: #67c23a;
  font-weight: 500;
}

.text-danger {
  color: #f56c6c;
  font-weight: 500;
}

.text-muted {
  color: #909399;
}

@media (max-width: 900px) {
  .sync-main {
    flex-direction: column;
  }

  .config-card,
  .status-card {
    width: 100%;
  }
}
</style>

<template>
  <div class="page-container">
    <div class="page-header">
      <h2>策略回测</h2>
      <el-button type="primary" @click="handleCreate">
        <el-icon><Plus /></el-icon>
        新建策略
      </el-button>
    </div>
    <el-tabs v-model="activeTab" class="strategy-tabs">
      <el-tab-pane label="策略列表" name="strategyList">
        <div class="tab-content">
          <el-table v-loading="loading" :data="strategyList" stripe style="width: 100%">
            <el-table-column prop="id" label="ID" width="80" />
            <el-table-column prop="name" label="策略名称" width="200" />
            <el-table-column prop="description" label="描述" min-width="250">
              <template #default="{ row }">
                {{ row.description || '-' }}
              </template>
            </el-table-column>
            <el-table-column prop="created_at" label="创建时间" width="180">
              <template #default="{ row }">
                {{ formatDateTime(row.created_at) }}
              </template>
            </el-table-column>
            <el-table-column label="操作" width="200" fixed="right">
              <template #default="{ row }">
                <el-button type="primary" link @click="handleBacktest(row)">回测</el-button>
                <el-button type="danger" link @click="handleDelete(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>

          <div class="pagination-container">
            <el-pagination
              v-model:current-page="currentPage"
              v-model:page-size="pageSize"
              :page-sizes="[10, 20, 50]"
              :total="total"
              layout="total, sizes, prev, pager, next, jumper"
              @size-change="handleSizeChange"
              @current-change="handlePageChange"
            />
          </div>
        </div>
      </el-tab-pane>

      <el-tab-pane label="回测记录" name="backtestList">
        <BacktestList ref="backtestListRef" />
      </el-tab-pane>

      <el-tab-pane label="回测运行" name="backtestRunner" v-if="activeStrategy">
        <div class="split-layout">
          <div class="editor-panel">
            <div class="editor-toolbar">
              <el-input v-model="activeStrategy.name" size="small" class="strategy-name-input" placeholder="策略名称" />
              <div class="toolbar-actions">
                <el-button size="small" @click="handleSaveStrategy" :loading="saving">保存</el-button>
                <el-button size="small" type="primary" @click="handleRunBacktest" :loading="btRunning">编译运行</el-button>
              </div>
            </div>
            <div class="code-editor">
              <textarea
                v-model="btCode"
                class="editor-textarea"
                spellcheck="false"
                placeholder="# 输入因子表达式作为策略信号&#10;# 例如: Mean($close, 5) / Mean($close, 20) - 1&#10;# 正值表示做多，负值表示做空"
              />
            </div>
          </div>
          <div class="right-panel">
            <div class="bt-config-bar">
              <el-date-picker v-model="btStartDate" value-format="YYYY-MM-DD" size="small" style="width:130px" placeholder="开始日期" />
              <span>至</span>
              <el-date-picker v-model="btEndDate" value-format="YYYY-MM-DD" size="small" style="width:130px" placeholder="结束日期" />
              <span>资金</span>
              <el-input-number v-model="btCapital" :min="10000" :step="100000" size="small" style="width:130px" />
              <el-select v-model="btFrequency" size="small" style="width:80px">
                <el-option label="每天" value="daily" />
                <el-option label="每周" value="weekly" />
                <el-option label="每月" value="monthly" />
              </el-select>
              <el-button type="primary" size="small" @click="handleRunBacktest" :loading="btRunning">运行回测</el-button>
            </div>

            <RunningPanel
              :running="btRunning"
              :completed="!btRunning && btFullResult != null"
              :liveData="btLiveData"
              :logs="[...btLogs, ...btErrors.map(e => '[错误] ' + e)]"
              @viewReport="showReport = true"
            />
          </div>

          <ReportOverlay v-model:visible="showReport" :result="btFullResult" />
        </div>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus } from '@element-plus/icons-vue'
import { strategyApi, type Strategy, type LiveData, type TaskStatus, type BacktestResultData } from '@/api/backtest'
import BacktestList from './BacktestList.vue'
import RunningPanel from './RunningPanel.vue'
import ReportOverlay from './ReportOverlay.vue'
import { formatDateTime } from '@/utils/format'

const SAMPLE_CODE = `// 买入条件: RSI < 30 (超卖)
// 卖出条件: RSI > 70 (超买)
// 注: 当前使用因子表达式作为策略信号
Mean($close, 5) / Mean($close, 20) - 1`

// ── State ──
const activeTab = ref('strategyList')
const loading = ref(false)
const strategyList = ref<Strategy[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)
const saving = ref(false)

const backtestListRef = ref<InstanceType<typeof BacktestList> | null>(null)

// ── Strategy list ──
const loadStrategies = async () => {
  loading.value = true
  try {
    const response = await strategyApi.list(currentPage.value, pageSize.value)
    strategyList.value = response.items
    total.value = response.total
  } catch {
    ElMessage.error('加载策略列表失败')
  } finally {
    loading.value = false
  }
}

const handleCreate = async () => {
  try {
    const result = await strategyApi.create({
      name: '新建策略',
      code: SAMPLE_CODE,
      description: '双均线交叉策略示例',
    })
    ElMessage.success('示例策略已创建')
    await loadStrategies()
    // Load the new strategy into the editor
    activeStrategy.value = {
      id: result.id,
      name: result.name,
      code: result.code,
      description: result.description,
      parameters: result.parameters,
      created_at: result.created_at,
      updated_at: result.updated_at,
    }
    btCode.value = result.code || SAMPLE_CODE
    btMetrics.value = []
    btLogs.value = []
    btErrors.value = []
    activeTab.value = 'backtestRunner'
  } catch {
    ElMessage.error('创建失败')
  }
}

const handleDelete = async (row: Strategy) => {
  try {
    await ElMessageBox.confirm(`确定要删除策略"${row.name}"吗？`, '确认删除', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning',
    })
    await strategyApi.delete(row.id)
    ElMessage.success('删除成功')
    if (activeStrategy.value?.id === row.id) {
      activeStrategy.value = null
      activeTab.value = 'strategyList'
    }
    loadStrategies()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

const handlePageChange = (page: number) => {
  currentPage.value = page
  loadStrategies()
}

const handleSizeChange = (size: number) => {
  pageSize.value = size
  currentPage.value = 1
  loadStrategies()
}

// ── Backtest runner state ──
const activeStrategy = ref<Strategy | null>(null)
const btCode = ref('')
const btStartDate = ref('2020-01-01')
const btEndDate = ref('2025-12-31')
const btCapital = ref(1_000_000)
const btFrequency = ref('monthly')
const btRunning = ref(false)
const btLiveData = ref<LiveData | null>(null)
const btFullResult = ref<BacktestResultData | null>(null)
const showReport = ref(false)
const btMetrics = ref<{ label: string; value: string; color?: string }[]>([])
const btLogs = ref<string[]>([])
const btErrors = ref<string[]>([])

const freqLabelMap: Record<string, string> = {
  daily: '每天',
  weekly: '每周',
  monthly: '每月',
}

const handleSaveStrategy = async () => {
  if (!activeStrategy.value) return
  saving.value = true
  try {
    await strategyApi.update(activeStrategy.value.id, {
      name: activeStrategy.value.name,
      code: btCode.value,
      description: activeStrategy.value.description || undefined,
    })
    ElMessage.success('保存成功')
  } catch {
    ElMessage.error('保存失败')
  } finally {
    saving.value = false
  }
}

const handleBacktest = (row: Strategy) => {
  activeStrategy.value = { ...row }
  btCode.value = row.code || ''
  btLiveData.value = null
  btFullResult.value = null
  btMetrics.value = []
  btLogs.value = []
  btErrors.value = []
  activeTab.value = 'backtestRunner'
}

const handleRunBacktest = async () => {
  if (!activeStrategy.value) return
  btRunning.value = true
  btLiveData.value = null
  btFullResult.value = null
  btMetrics.value = []
  btLogs.value = ['正在运行回测...']
  btErrors.value = []
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.post<any>('/v2/backtest/run', {
      mode: 'event_driven',
      factor_expression: btCode.value,
      symbols: ['000300.SH'],
      start_date: btStartDate.value,
      end_date: btEndDate.value,
      initial_capital: btCapital.value,
      rebalance_freq: btFrequency.value,
      n_groups: 5,
      bar_type: 'daily',
    })

    const taskId = res?.data?.task_id
    if (taskId) {
      btLogs.value = [`任务已提交 (${taskId})，等待完成...`]
      let attempts = 0
      while (attempts < 300) {
        await new Promise(r => setTimeout(r, 2000))
        const statusRes = await request.get<any>(`/v2/backtest/status/${taskId}`)
        const statusData = statusRes?.data as TaskStatus
        if (statusData?.live) {
          btLiveData.value = statusData.live
        }
        if (statusData?.status === 'done') {
          btLiveData.value = statusData.live
          const resultRes = await request.get<any>(`/v2/backtest/result/${taskId}`)
          const data = resultRes?.data as BacktestResultData
          if (data) {
            btFullResult.value = data
            btLogs.value = ['回测完成']
          }
          break
        } else if (statusData?.status === 'failed') {
          btErrors.value = ['回测失败']
          btLogs.value = ['回测执行失败']
          break
        }
        attempts++
      }
      if (attempts >= 300) {
        btErrors.value = ['回测超时']
        btLogs.value = ['回测超时（10分钟）']
      }
    } else {
      const data = res?.data as BacktestResultData
      if (data) {
        btFullResult.value = data
        btLogs.value = ['回测完成']
      }
    }
  } catch (e: any) {
    btErrors.value = [e?.message || '回测失败']
    btLogs.value = ['回测执行失败']
  } finally {
    btRunning.value = false
  }
}

onMounted(() => {
  loadStrategies()
})
</script>

<style scoped>
.page-container {
  height: 100%;
  display: flex;
  flex-direction: column;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.page-header h2 {
  margin: 0;
  font-size: 20px;
  font-weight: 600;
  color: #303133;
}

.strategy-tabs {
  height: 100%;
  background: #fff;
  border-radius: 4px;
  padding: 16px;
}

.strategy-tabs :deep(.el-tabs__content) {
  flex: 1;
  overflow: auto;
}

.strategy-tabs :deep(.el-tab-pane) {
  height: 100%;
}

.tab-content {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
}

.split-layout { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; height: calc(100vh - 200px); }
.editor-panel {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  overflow: hidden;
}
.editor-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 12px;
  background: #1e1e1e;
  color: #d4d4d4;
  font-size: 12px;
  gap: 8px;
}
.strategy-name-input {
  width: 200px;
}
.strategy-name-input :deep(.el-input__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #d4d4d4;
}
.toolbar-actions {
  display: flex;
  gap: 6px;
  flex-shrink: 0;
}
.code-editor { flex: 1; background: #1e1e1e; }
.editor-textarea {
  width: 100%;
  height: 100%;
  border: none;
  padding: 12px;
  font-family: 'JetBrains Mono', 'Courier New', monospace;
  font-size: 13px;
  line-height: 1.6;
  color: #d4d4d4;
  background: #1e1e1e;
  resize: none;
  outline: none;
  tab-size: 4;
}
.editor-textarea::placeholder { color: #6e6e6e; }
.right-panel { display: flex; flex-direction: column; gap: 12px; overflow: auto; }
.bt-config-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  background: var(--bg-surface);
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  font-size: 12px;
}
.bt-metrics-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
.bt-metric-card {
  border: 1px solid var(--border-ghost);
  border-radius: 6px;
  padding: 10px;
  text-align: center;
  background: var(--bg-surface);
}
.bt-metric-label { font-size: 10px; color: var(--text-ghost); }
.bt-metric-value { font-size: 16px; font-weight: 700; margin-top: 4px; }
.positive { color: #d93026; }
.negative { color: #137333; }
.bt-log-panel {
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  background: var(--bg-surface);
  overflow: hidden;
  flex: 1;
  min-height: 120px;
}
.bt-log-content { padding: 8px 12px; font-size: 11px; font-family: monospace; max-height: 200px; overflow: auto; }
.bt-log-line { color: var(--text-ghost); padding: 1px 0; }
.bt-error { color: #e5484d; }
.bt-log-empty { color: var(--text-muted); font-style: italic; }
</style>

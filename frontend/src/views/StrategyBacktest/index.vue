<template>
  <div class="page-container">
    <div class="page-header">
      <h2>策略回测</h2>
      <div class="page-header-actions">
        <el-button link type="primary" @click="openDocs">
          <el-icon><Document /></el-icon>
          使用手册
        </el-button>
        <el-button type="primary" @click="handleCreate">
          <el-icon><Plus /></el-icon>
          新建策略
        </el-button>
      </div>
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
              <el-radio-group v-model="btMode" size="small" class="mode-switch">
                <el-radio-button value="script">脚本</el-radio-button>
                <el-radio-button value="expression">表达式</el-radio-button>
              </el-radio-group>
              <div class="toolbar-actions">
                <el-button size="small" @click="handleSaveStrategy" :loading="saving">保存</el-button>
                <el-button size="small" type="primary" @click="handleRunBacktest" :loading="btRunning">编译运行</el-button>
              </div>
            </div>

            <!-- 脚本模式 — 代码编辑器 -->
            <div class="code-editor" v-if="btMode === 'script'">
              <textarea
                v-model="btCode"
                class="editor-textarea"
                spellcheck="false"
                placeholder="def init(context):&#10;    context.ma_fast = 5&#10;&#10;def handle_bar(context, bar):&#10;    # 在这里写你的策略逻辑&#10;    pass"
              />
            </div>

            <!-- 表达式模式 — 因子表达式输入 -->
            <div class="expression-panel" v-else>
              <div class="expression-input-row">
                <el-input
                  v-model="btExpression"
                  size="default"
                  class="expression-input"
                  placeholder="输入因子表达式，如: close/MA(close, 20) - 1"
                  clearable
                />
                <el-select
                  v-model="selectedFactorId"
                  size="default"
                  placeholder="从因子研究选择"
                  clearable
                  class="factor-select"
                  @change="handleFactorSelect"
                >
                  <el-option
                    v-for="f in savedFactors"
                    :key="f.id"
                    :label="f.name"
                    :value="f.id"
                  />
                </el-select>
              </div>
              <div class="expression-hint">
                <span>向量化回测：表达式按日计算因子值 → 分层买入 → 计算分组收益</span>
                <span class="hint-examples">示例: close/MA(close,20)-1 | RSI(close,14) | MACD(close)[0]</span>
              </div>
              <div class="expression-params">
                <span class="param-label">分组数</span>
                <el-input-number v-model="btNGroups" :min="2" :max="10" size="small" style="width:80px" />
              </div>
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
import { ref, reactive, onMounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Document } from '@element-plus/icons-vue'
import { strategyApi, type Strategy, type LiveData, type TaskStatus, type BacktestResultData } from '@/api/backtest'
import { factorApi, type Factor } from '@/api/factor'
import BacktestList from './BacktestList.vue'
import RunningPanel from './RunningPanel.vue'
import ReportOverlay from './ReportOverlay.vue'
import { formatDateTime } from '@/utils/format'

const SAMPLE_CODE = `def init(context):
    # 策略参数
    context.fast = 5
    context.slow = 20

    # 运行信息
    log(f"回测区间: {context.run_info.start_date} ~ {context.run_info.end_date}")
    log(f"初始资金: {context.run_info.capital:,.0f}")
    log(f"标的: {context.run_info.symbols}")

def handle_bar(context, bar_dict):
    # bar_dict 包含当日所有标的的 Bar，每天触发一次
    for symbol in bar_dict:
        bar = bar_dict[symbol]

        # 跳过停牌
        if bar.suspended or bar.isnan:
            continue

        # 获取历史收盘价序列
        hist = context.get_history(symbol, 252)
        if hist.empty or len(hist) < context.slow:
            continue
        close = hist['close']

        # 计算快慢均线
        mf = MA(close, context.fast)
        ms = MA(close, context.slow)

        # 金叉买入
        if CROSS(mf, ms).iloc[-1] == 1:
            if not context.portfolio.get_position(symbol):
                context.order_value(symbol, context.portfolio.total_value * 0.2)

        # 死叉卖出
        elif CROSS(mf, ms).iloc[-1] == -1:
            pos = context.portfolio.get_position(symbol)
            if pos and pos.total_shares > 0:
                context.order_shares(symbol, -pos.total_shares)

    # 定期输出状态
    now = context.now
    if now.day % 30 == 0:
        nav = context.portfolio.unit_net_value
        log(f"[{now.date()}] nav={nav:.3f} cash={context.stock_account.available_cash:,.0f}")`

const SAMPLE_EXPRESSION = 'close / MA(close, 20) - 1'

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
    activeStrategy.value = {
      id: result.id,
      name: result.name,
      code: result.code,
      description: result.description,
      parameters: result.parameters,
      created_at: result.created_at,
      updated_at: result.updated_at,
    }
    btMode.value = 'script'
    btCode.value = result.code || SAMPLE_CODE
    btExpression.value = SAMPLE_EXPRESSION
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
const btMode = ref<'script' | 'expression'>('script')
const btCode = ref('')
const btExpression = ref(SAMPLE_EXPRESSION)
const btNGroups = ref(5)
const selectedFactorId = ref<number | null>(null)
const savedFactors = ref<Factor[]>([])
const btStartDate = ref('2020-01-01')
const btEndDate = ref('2025-12-31')
const btCapital = ref(1_000_000)
const btFrequency = ref('monthly')
const btSymbols = ref<string[]>(['600178.SH'])
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

const openDocs = () => {
  window.open('/docs', '_blank')
}

// ── Factor selection ──
const loadSavedFactors = async () => {
  try {
    const data = await factorApi.getList()
    savedFactors.value = data || []
  } catch {
    // non-critical
  }
}

const handleFactorSelect = (factorId: number | null) => {
  if (!factorId) return
  const factor = savedFactors.value.find(f => f.id === factorId)
  if (factor && factor.code) {
    btExpression.value = factor.code
  }
}

const handleSaveStrategy = async () => {
  if (!activeStrategy.value) return
  saving.value = true
  try {
    const code = btMode.value === 'expression' ? btExpression.value : btCode.value
    await strategyApi.update(activeStrategy.value.id, {
      name: activeStrategy.value.name,
      code,
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
  const code = row.code || ''
  // Auto-detect mode from code content
  if (code.includes('def handle_bar') || code.includes('def init')) {
    btMode.value = 'script'
    btCode.value = code
  } else {
    btMode.value = 'expression'
    btExpression.value = code || SAMPLE_EXPRESSION
    btCode.value = code
  }
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

  const isExpression = btMode.value === 'expression'
  const code = isExpression ? btExpression.value : btCode.value
  const mode = isExpression ? 'vectorized' : 'event_driven'

  try {
    const { default: request } = await import('@/api/request')
    const res = await request.post<any>('/v2/backtest/run', {
      mode,
      factor_expression: code,
      symbols: btSymbols.value,
      start_date: btStartDate.value,
      end_date: btEndDate.value,
      initial_capital: btCapital.value,
      rebalance_freq: btFrequency.value,
      n_groups: isExpression ? btNGroups.value : 5,
      bar_type: 'daily',
    })

    // 拦截器已解包 {code, data} → data 字段直接返回
    const taskId = (res as any)?.task_id
    if (taskId) {
      btLogs.value = [`任务已提交 (${taskId})，等待完成...`]
      let attempts = 0
      while (attempts < 300) {
        await new Promise(r => setTimeout(r, 2000))
        const statusData = await request.get<any>(`/v2/backtest/status/${taskId}`)
        if (statusData?.live) {
          btLiveData.value = statusData.live
        }
        if (statusData?.status === 'done') {
          btLiveData.value = statusData.live
          const data = await request.get<any>(`/v2/backtest/result/${taskId}`)
          if (data) {
            btFullResult.value = data as BacktestResultData
            btLogs.value = ['回测完成']
          }
          break
        } else if (statusData?.status === 'failed') {
          const errData = await request.get<any>(`/v2/backtest/result/${taskId}`)
          btErrors.value = [errData?.error || '回测失败']
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
      // 同步返回结果（vectorized 模式直接返回）
      if (res) {
        btFullResult.value = res as BacktestResultData
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
  loadSavedFactors()
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
  color: #e2e2ea;
}

.page-header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.strategy-tabs {
  height: 100%;
  background: #131318;
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
  background: #1a1a22;
  border: 1px solid #2a2a35;
  border-radius: 8px;
  font-size: 12px;
  color: #c0c0cc;
}
.bt-config-bar span {
  color: #999;
}
.bt-config-bar :deep(.el-input__inner) {
  background: #141418;
  border-color: #2a2a35;
  color: #d4d4d4;
}
.bt-config-bar :deep(.el-input__wrapper) {
  background: #141418;
  box-shadow: none;
}
.bt-metrics-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
.bt-metric-card {
  border: 1px solid #2a2a35;
  border-radius: 6px;
  padding: 10px;
  text-align: center;
  background: #1a1a22;
}
.bt-metric-label { font-size: 10px; color: #8888a0; }
.bt-metric-value { font-size: 16px; font-weight: 700; margin-top: 4px; color: #e2e2ea; }
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
.bt-log-content { padding: 8px 12px; font-size: 11px; font-family: monospace; max-height: 200px; overflow: auto; background: #141418; border-radius: 4px; }
.bt-log-line { color: #8888a0; padding: 1px 0; }
.bt-error { color: #e5484d; }
.bt-log-empty { color: #8888a0; font-style: italic; }

/* ── Mode switch ── */
.mode-switch { flex-shrink: 0; }
.mode-switch :deep(.el-radio-button__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #999;
  padding: 4px 12px;
  font-size: 12px;
}
.mode-switch :deep(.el-radio-button__original-radio:checked + .el-radio-button__inner) {
  background: var(--el-color-primary);
  border-color: var(--el-color-primary);
  color: #fff;
}

/* ── Expression panel ── */
.expression-panel {
  flex: 1;
  background: #1e1e1e;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.expression-input-row {
  display: flex;
  gap: 10px;
}
.expression-input {
  flex: 1;
}
.expression-input :deep(.el-input__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #d4d4d4;
  font-family: 'JetBrains Mono', monospace;
  font-size: 14px;
  height: 40px;
}
.factor-select {
  width: 200px;
}
.factor-select :deep(.el-input__inner) {
  background: #2d2d2d;
  border-color: #404040;
  color: #d4d4d4;
}
.expression-hint {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 11px;
  color: #999;
  line-height: 1.5;
}
.hint-examples {
  color: #777;
  font-family: 'JetBrains Mono', monospace;
}
.expression-params {
  display: flex;
  align-items: center;
  gap: 8px;
}
.param-label {
  font-size: 12px;
  color: #aaa;
}
</style>

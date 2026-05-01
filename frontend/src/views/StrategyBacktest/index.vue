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
          <!-- 策略列表 -->
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
                <el-button type="primary" link @click="handleEdit(row)">编辑</el-button>
                <el-button type="success" link @click="handleBacktest(row)">回测</el-button>
                <el-button type="danger" link @click="handleDelete(row)">删除</el-button>
              </template>
            </el-table-column>
          </el-table>

          <!-- 分页 -->
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
              <span>{{ activeStrategy.name }}</span>
              <el-button size="small" type="primary" @click="handleRunBacktest" :loading="btRunning">编译运行</el-button>
            </div>
            <div class="code-editor">
              <textarea
                v-model="btCode"
                class="editor-textarea"
                spellcheck="false"
                placeholder="# 输入策略代码..."
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

            <div v-if="btMetrics.length" class="bt-metrics-grid">
              <div class="bt-metric-card" v-for="m in btMetrics" :key="m.label">
                <div class="bt-metric-label">{{ m.label }}</div>
                <div class="bt-metric-value" :class="m.color || ''">{{ m.value }}</div>
              </div>
            </div>

            <div v-if="btLogs.length || btErrors.length" class="bt-log-panel">
              <el-tabs model-value="logs" size="small">
                <el-tab-pane label="日志" name="logs">
                  <div class="bt-log-content">
                    <div v-for="(log, i) in btLogs" :key="i" class="bt-log-line">{{ log }}</div>
                    <div v-if="!btLogs.length" class="bt-log-empty">暂无日志</div>
                  </div>
                </el-tab-pane>
                <el-tab-pane label="错误" name="errors">
                  <div class="bt-log-content">
                    <div v-for="(err, i) in btErrors" :key="i" class="bt-log-line bt-error">{{ err }}</div>
                    <div v-if="!btErrors.length" class="bt-log-empty">暂无错误</div>
                  </div>
                </el-tab-pane>
              </el-tabs>
            </div>
          </div>
        </div>
      </el-tab-pane>
    </el-tabs>

    <!-- 新建/编辑策略对话框 -->
    <el-dialog
      v-model="dialogVisible"
      :title="isEdit ? '编辑策略' : '新建策略'"
      width="600px"
      :close-on-click-modal="false"
    >
      <el-form ref="formRef" :model="formData" :rules="formRules" label-width="80px">
        <el-form-item label="策略名称" prop="name">
          <el-input v-model="formData.name" placeholder="请输入策略名称" maxlength="100" />
        </el-form-item>
        <el-form-item label="描述" prop="description">
          <el-input
            v-model="formData.description"
            type="textarea"
            :rows="3"
            placeholder="请输入策略描述"
            maxlength="500"
          />
        </el-form-item>
        <el-form-item label="策略代码" prop="code">
          <el-input
            v-model="formData.code"
            type="textarea"
            :rows="10"
            placeholder="请输入策略代码"
            class="code-input"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="handleSubmit">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted, nextTick } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'
import { Plus } from '@element-plus/icons-vue'
import { strategyApi, type Strategy } from '@/api/backtest'
import BacktestList from './BacktestList.vue'
import { formatDateTime } from '@/utils/format'

// 状态
const activeTab = ref('strategyList')
const loading = ref(false)
const strategyList = ref<Strategy[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)

// 对话框状态
const dialogVisible = ref(false)
const isEdit = ref(false)
const submitting = ref(false)
const formRef = ref<FormInstance>()
const editingId = ref<number | null>(null)
const backtestListRef = ref<InstanceType<typeof BacktestList> | null>(null)

// 表单数据
const formData = reactive({
  name: '',
  description: '',
  code: '',
})

// 表单验证规则
const formRules: FormRules = {
  name: [
    { required: true, message: '请输入策略名称', trigger: 'blur' },
    { min: 2, max: 100, message: '长度在 2 到 100 个字符', trigger: 'blur' },
  ],
  code: [{ required: true, message: '请输入策略代码', trigger: 'blur' }],
}

// 加载策略列表
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

// 新建策略
const handleCreate = () => {
  isEdit.value = false
  editingId.value = null
  formData.name = ''
  formData.description = ''
  formData.code = ''
  dialogVisible.value = true
}

// 编辑策略
const handleEdit = (row: Strategy) => {
  isEdit.value = true
  editingId.value = row.id
  formData.name = row.name
  formData.description = row.description || ''
  formData.code = row.code
  dialogVisible.value = true
}

// 提交表单
const handleSubmit = async () => {
  if (!formRef.value) return

  try {
    await formRef.value.validate()
  } catch {
    return
  }

  submitting.value = true
  try {
    if (isEdit.value && editingId.value) {
      await strategyApi.update(editingId.value, {
        name: formData.name,
        description: formData.description || undefined,
        code: formData.code,
      })
      ElMessage.success('更新成功')
    } else {
      await strategyApi.create({
        name: formData.name,
        description: formData.description || undefined,
        code: formData.code,
      })
      ElMessage.success('创建成功')
    }
    dialogVisible.value = false
    loadStrategies()
  } catch {
    ElMessage.error(isEdit.value ? '更新失败' : '创建失败')
  } finally {
    submitting.value = false
  }
}

// 删除策略
const handleDelete = async (row: Strategy) => {
  try {
    await ElMessageBox.confirm(`确定要删除策略"${row.name}"吗？`, '确认删除', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning',
    })
    await strategyApi.delete(row.id)
    ElMessage.success('删除成功')
    loadStrategies()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// 跳转到回测
const handleBacktest = async (row: Strategy) => {
  activeTab.value = 'backtestList'
  await nextTick()
  backtestListRef.value?.openCreateDialogWithStrategy(row.id, row.name)
}

// 分页变化
const handlePageChange = (page: number) => {
  currentPage.value = page
  loadStrategies()
}

const handleSizeChange = (size: number) => {
  pageSize.value = size
  currentPage.value = 1
  loadStrategies()
}

// Backtest runner state
const activeStrategy = ref<Strategy | null>(null)
const btCode = ref('')
const btStartDate = ref('2020-01-01')
const btEndDate = ref('2025-12-31')
const btCapital = ref(1_000_000)
const btFrequency = ref('monthly')
const btRunning = ref(false)
const btMetrics = ref<{ label: string; value: string; color?: string }[]>([])
const btLogs = ref<string[]>([])
const btErrors = ref<string[]>([])

const handleBacktest = async (row: Strategy) => {
  activeStrategy.value = row
  btCode.value = row.code || ''
  btMetrics.value = []
  btLogs.value = []
  btErrors.value = []
  activeTab.value = 'backtestRunner'
}

const handleRunBacktest = async () => {
  if (!activeStrategy.value) return
  btRunning.value = true
  btMetrics.value = []
  btLogs.value = ['正在运行回测...']
  btErrors.value = []
  try {
    const { default: request } = await import('@/api/request')
    const res = await request.post<any>('/v2/backtest/strategy', {
      code: btCode.value,
      start_date: btStartDate.value,
      end_date: btEndDate.value,
      initial_capital: btCapital.value,
      frequency: btFrequency.value,
    })
    const data = res.metrics || res.data?.metrics || res
    const logs = res.logs || res.data?.logs || []
    btLogs.value = Array.isArray(logs) ? logs : [String(logs)]
    if (data) {
      btMetrics.value = [
        { label: '总收益率', value: data.total_return != null ? (data.total_return * 100).toFixed(2) + '%' : '-', color: data.total_return >= 0 ? 'positive' : 'negative' },
        { label: '年化收益', value: data.annual_return != null ? (data.annual_return * 100).toFixed(2) + '%' : '-', color: data.annual_return >= 0 ? 'positive' : 'negative' },
        { label: 'Sharpe', value: data.sharpe != null ? Number(data.sharpe).toFixed(2) : '-' },
        { label: '最大回撤', value: data.max_drawdown != null ? (data.max_drawdown * 100).toFixed(2) + '%' : '-', color: 'negative' },
        { label: 'Alpha', value: data.alpha != null ? Number(data.alpha).toFixed(4) : '-' },
        { label: 'Beta', value: data.beta != null ? Number(data.beta).toFixed(4) : '-' },
      ]
    }
  } catch (e: any) {
    btErrors.value = [e?.message || 'Backtest failed']
    btLogs.value = ['回测执行失败']
  } finally {
    btRunning.value = false
  }
}

// 初始化
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

.code-input :deep(.el-textarea__inner) {
  font-family: 'Courier New', Courier, monospace;
  font-size: 13px;
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

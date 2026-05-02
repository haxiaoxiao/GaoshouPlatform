<template>
  <div class="backtest-list-container">
    <!-- 工具栏 -->
    <div class="toolbar">
      <div class="toolbar-left">
        <el-button type="danger" plain size="small" :disabled="selectedIds.length === 0" @click="handleBatchDelete">
          <el-icon><Delete /></el-icon>
          批量删除 ({{ selectedIds.length }})
        </el-button>
      </div>
      <el-button type="primary" @click="handleCreate">
        <el-icon><Plus /></el-icon>
        新建回测
      </el-button>
    </div>

    <!-- 回测列表 -->
    <el-table v-loading="loading" :data="backtestList" stripe style="width: 100%" @selection-change="handleSelectionChange">
      <el-table-column type="selection" width="40" />
      <el-table-column prop="id" label="ID" width="70" />
      <el-table-column prop="status" label="状态" width="90">
        <template #default="{ row }">
          <el-tag :type="getStatusType(row.status)" size="small">
            {{ getStatusLabel(row.status) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="收益率" width="100">
        <template #default="{ row }">
          <span v-if="row.result?.total_return != null" :style="{ color: row.result.total_return >= 0 ? '#d93026' : '#137333', fontWeight: 600 }">
            {{ (row.result.total_return * 100).toFixed(1) }}%
          </span>
          <span v-else style="color:#666">—</span>
        </template>
      </el-table-column>
      <el-table-column label="Sharpe" width="80">
        <template #default="{ row }">
          {{ row.result?.sharpe_ratio?.toFixed(2) || '—' }}
        </template>
      </el-table-column>
      <el-table-column label="胜率" width="80">
        <template #default="{ row }">
          {{ row.result?.win_rate != null ? (row.result.win_rate * 100).toFixed(0) + '%' : '—' }}
        </template>
      </el-table-column>
      <el-table-column label="成交" width="70">
        <template #default="{ row }">
          {{ row.result?.total_trades ?? '—' }}
        </template>
      </el-table-column>
      <el-table-column prop="start_date" label="区间" width="200">
        <template #default="{ row }">
          {{ row.start_date }} ~ {{ row.end_date }}
        </template>
      </el-table-column>
      <el-table-column prop="created_at" label="创建时间" width="160">
        <template #default="{ row }">
          {{ formatDateTime(row.created_at) }}
        </template>
      </el-table-column>
      <el-table-column label="操作" width="160" fixed="right">
        <template #default="{ row }">
          <el-button type="success" link :disabled="row.status !== 'completed'" @click="handleViewReport(row)">
            报告
          </el-button>
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

    <!-- 新建回测对话框 -->
    <el-dialog
      v-model="createDialogVisible"
      title="新建回测"
      width="500px"
      :close-on-click-modal="false"
    >
      <el-form ref="createFormRef" :model="createFormData" :rules="createFormRules" label-width="80px">
        <el-form-item label="策略" prop="strategy_id">
          <el-select v-model="createFormData.strategy_id" placeholder="请选择策略" style="width: 100%">
            <el-option
              v-for="strategy in strategyOptions"
              :key="strategy.id"
              :label="`${strategy.name} (ID: ${strategy.id})`"
              :value="strategy.id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="开始日期" prop="start_date">
          <el-date-picker
            v-model="createFormData.start_date"
            type="date"
            placeholder="选择开始日期"
            value-format="YYYY-MM-DD"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="结束日期" prop="end_date">
          <el-date-picker
            v-model="createFormData.end_date"
            type="date"
            placeholder="选择结束日期"
            value-format="YYYY-MM-DD"
            style="width: 100%"
          />
        </el-form-item>
        <el-form-item label="初始资金" prop="initial_capital">
          <el-input-number
            v-model="createFormData.initial_capital"
            :min="10000"
            :max="100000000"
            :step="10000"
            :precision="0"
            placeholder="请输入初始资金"
            style="width: 100%"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="createDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="creating" @click="handleCreateSubmit">确定</el-button>
      </template>
    </el-dialog>

    <!-- 回测报告对话框 -->
    <el-dialog v-model="reportDialogVisible" title="回测报告" width="800px">
      <BacktestReport v-if="reportDialogVisible && selectedBacktestId !== null" :backtest-id="selectedBacktestId" />
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'
import { Plus, Delete } from '@element-plus/icons-vue'
import { backtestApi, strategyApi, type Backtest, type Strategy } from '@/api/backtest'
import BacktestReport from './BacktestReport.vue'
import { formatDateTime, formatCapital, getStatusType, getStatusLabel } from '@/utils/format'

// 状态
const loading = ref(false)
const backtestList = ref<Backtest[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(20)

// 创建对话框状态
const createDialogVisible = ref(false)
const creating = ref(false)
const createFormRef = ref<FormInstance>()
const strategyOptions = ref<Strategy[]>([])

const createFormData = reactive({
  strategy_id: undefined as number | undefined,
  start_date: '',
  end_date: '',
  initial_capital: 1000000,
})

const createFormRules: FormRules = {
  strategy_id: [{ required: true, message: '请选择策略', trigger: 'change' }],
  start_date: [{ required: true, message: '请选择开始日期', trigger: 'change' }],
  end_date: [{ required: true, message: '请选择结束日期', trigger: 'change' }],
  initial_capital: [{ required: true, message: '请输入初始资金', trigger: 'blur' }],
}

// 多选 & 删除
const selectedIds = ref<number[]>([])
const handleSelectionChange = (rows: Backtest[]) => {
  selectedIds.value = rows.map(r => r.id)
}

const handleDelete = async (row: Backtest) => {
  try {
    await ElMessageBox.confirm(`确定删除回测记录 #${row.id}？`, '确认删除', { type: 'warning' })
    await backtestApi.delete(row.id)
    ElMessage.success('已删除')
    loadBacktests()
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('删除失败')
  }
}

const handleBatchDelete = async () => {
  try {
    await ElMessageBox.confirm(
      `确定删除选中的 ${selectedIds.value.length} 条回测记录？此操作不可恢复。`,
      '批量删除',
      { type: 'warning', confirmButtonText: '删除', cancelButtonText: '取消' }
    )
    const res = await backtestApi.deleteBatch(selectedIds.value)
    ElMessage.success(`已删除 ${res.deleted_count} 条`)
    selectedIds.value = []
    loadBacktests()
  } catch (e) {
    if (e !== 'cancel') ElMessage.error('批量删除失败')
  }
}

// 报告对话框状态
const reportDialogVisible = ref(false)
const selectedBacktestId = ref<number | null>(null)

// 加载回测列表
const loadBacktests = async () => {
  loading.value = true
  try {
    const response = await backtestApi.list({
      page: currentPage.value,
      page_size: pageSize.value,
    })
    backtestList.value = response.items
    total.value = response.total
  } catch {
    ElMessage.error('加载回测列表失败')
  } finally {
    loading.value = false
  }
}

// 加载策略选项
const loadStrategies = async () => {
  try {
    const response = await strategyApi.list(1, 100)
    strategyOptions.value = response.items
  } catch {
    console.error('加载策略列表失败')
  }
}

// 新建回测
const handleCreate = () => {
  createFormData.strategy_id = undefined
  createFormData.start_date = ''
  createFormData.end_date = ''
  createFormData.initial_capital = 1000000
  createDialogVisible.value = true
}

// 提交创建
const handleCreateSubmit = async () => {
  if (!createFormRef.value) return

  try {
    await createFormRef.value.validate()
  } catch {
    return
  }

  creating.value = true
  try {
    await backtestApi.create({
      strategy_id: createFormData.strategy_id!,
      start_date: createFormData.start_date,
      end_date: createFormData.end_date,
      initial_capital: createFormData.initial_capital,
    })
    ElMessage.success('创建成功')
    createDialogVisible.value = false
    loadBacktests()
  } catch {
    ElMessage.error('创建失败')
  } finally {
    creating.value = false
  }
}

// 运行回测
const handleRun = async (row: Backtest) => {
  try {
    ElMessage.info('开始运行回测...')
    await backtestApi.run(row.id)
    ElMessage.success('回测运行完成')
    loadBacktests()
  } catch {
    ElMessage.error('回测运行失败')
  }
}

// 查看报告
const handleViewReport = (row: Backtest) => {
  selectedBacktestId.value = row.id
  reportDialogVisible.value = true
}

// 分页变化
const handlePageChange = (page: number) => {
  currentPage.value = page
  loadBacktests()
}

const handleSizeChange = (size: number) => {
  pageSize.value = size
  currentPage.value = 1
  loadBacktests()
}

// 暴露方法给父组件
const openCreateDialogWithStrategy = (strategyId: number, _strategyName: string) => {
  createFormData.strategy_id = strategyId
  createFormData.start_date = ''
  createFormData.end_date = ''
  createFormData.initial_capital = 1000000
  createDialogVisible.value = true
}

defineExpose({
  openCreateDialogWithStrategy,
})

// 初始化
onMounted(() => {
  loadBacktests()
  loadStrategies()
})
</script>

<style scoped>
.backtest-list-container {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.toolbar {
  display: flex;
  justify-content: flex-end;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
}
</style>

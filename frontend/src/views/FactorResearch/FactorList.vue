<template>
  <div class="factor-list">
    <!-- 顶部操作栏 -->
    <div class="header">
      <el-button type="primary" @click="handleCreate">
        <el-icon><Plus /></el-icon>
        新建因子
      </el-button>
    </div>

    <!-- 因子列表 -->
    <el-table :data="factors" v-loading="loading" stripe>
      <el-table-column prop="name" label="因子名称" min-width="120">
        <template #default="{ row }">
          <el-link type="primary" @click="handleDetail(row)">{{ row.name }}</el-link>
        </template>
      </el-table-column>
      <el-table-column prop="category" label="分类" width="120" />
      <el-table-column prop="source" label="来源" width="100" />
      <el-table-column prop="description" label="描述" min-width="200" show-overflow-tooltip />
      <el-table-column prop="created_at" label="创建时间" width="180">
        <template #default="{ row }">
          {{ formatDateTime(row.created_at) }}
        </template>
      </el-table-column>
      <el-table-column label="操作" width="200" fixed="right">
        <template #default="{ row }">
          <el-button-group>
            <el-button size="small" @click="handleAnalyze(row)">分析</el-button>
            <el-button size="small" @click="handleEdit(row)">编辑</el-button>
            <el-button size="small" type="danger" @click="handleDelete(row)">删除</el-button>
          </el-button-group>
        </template>
      </el-table-column>
    </el-table>

    <!-- 新建/编辑因子对话框 -->
    <el-dialog
      v-model="dialogVisible"
      :title="editingFactor ? '编辑因子' : '新建因子'"
      width="600px"
    >
      <el-form :model="formData" :rules="formRules" ref="formRef" label-width="100px">
        <el-form-item label="因子名称" prop="name">
          <el-input v-model="formData.name" placeholder="请输入因子名称" />
        </el-form-item>
        <el-form-item label="分类" prop="category">
          <el-select v-model="formData.category" placeholder="选择分类" clearable>
            <el-option label="技术因子" value="技术因子" />
            <el-option label="基本面因子" value="基本面因子" />
            <el-option label="情绪因子" value="情绪因子" />
            <el-option label="其他" value="其他" />
          </el-select>
        </el-form-item>
        <el-form-item label="来源" prop="source">
          <el-select v-model="formData.source" placeholder="选择来源" clearable>
            <el-option label="自定义" value="custom" />
            <el-option label="QMT" value="qmt" />
          </el-select>
        </el-form-item>
        <el-form-item label="因子代码" prop="code">
          <el-input
            v-model="formData.code"
            type="textarea"
            :rows="5"
            placeholder="请输入因子计算代码"
          />
        </el-form-item>
        <el-form-item label="参数配置" prop="parameters">
          <el-input
            v-model="parametersJson"
            type="textarea"
            :rows="3"
            placeholder='{"normalize_window": 5, "factor_window": 20}'
          />
        </el-form-item>
        <el-form-item label="描述" prop="description">
          <el-input
            v-model="formData.description"
            type="textarea"
            :rows="3"
            placeholder="请输入因子描述"
          />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="handleSubmit" :loading="submitting">确定</el-button>
      </template>
    </el-dialog>

    <!-- 因子分析对话框 -->
    <el-dialog v-model="analyzeDialogVisible" title="因子分析" width="500px">
      <el-form :model="analyzeForm" label-width="120px">
        <el-form-item label="分析起始日期">
          <el-date-picker
            v-model="analyzeForm.start_date"
            type="date"
            placeholder="选择日期"
            value-format="YYYY-MM-DD"
          />
        </el-form-item>
        <el-form-item label="分析结束日期">
          <el-date-picker
            v-model="analyzeForm.end_date"
            type="date"
            placeholder="选择日期"
            value-format="YYYY-MM-DD"
          />
        </el-form-item>
        <el-form-item label="标准化窗口">
          <el-input-number v-model="analyzeForm.normalize_window" :min="1" :max="60" />
        </el-form-item>
        <el-form-item label="因子计算窗口">
          <el-input-number v-model="analyzeForm.factor_window" :min="1" :max="120" />
        </el-form-item>
        <el-form-item label="收益前瞻期">
          <el-input-number v-model="analyzeForm.forward_period" :min="1" :max="60" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="analyzeDialogVisible = false">取消</el-button>
        <el-button type="primary" @click="submitAnalyze" :loading="analyzing">
          开始分析
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { Plus } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import type { FormInstance, FormRules } from 'element-plus'
import { factorApi, type Factor, type FactorCreateRequest } from '@/api/factor'

const router = useRouter()

// 状态
const loading = ref(false)
const factors = ref<Factor[]>([])
const dialogVisible = ref(false)
const submitting = ref(false)
const editingFactor = ref<Factor | null>(null)
const formRef = ref<FormInstance>()

// 表单数据
const formData = ref<FactorCreateRequest>({
  name: '',
  category: '',
  source: '',
  code: '',
  parameters: {},
  description: '',
})

const parametersJson = computed({
  get: () => JSON.stringify(formData.value.parameters || {}, null, 2),
  set: (val: string) => {
    try {
      formData.value.parameters = JSON.parse(val)
    } catch {
      // 忽略解析错误
    }
  },
})

const formRules: FormRules = {
  name: [{ required: true, message: '请输入因子名称', trigger: 'blur' }],
}

// 分析对话框
const analyzeDialogVisible = ref(false)
const analyzing = ref(false)
const analyzingFactor = ref<Factor | null>(null)
const analyzeForm = ref({
  start_date: '',
  end_date: '',
  normalize_window: 5,
  factor_window: 20,
  forward_period: 20,
})

// 加载因子列表
const loadFactors = async () => {
  loading.value = true
  try {
    factors.value = await factorApi.getList()
  } catch (error) {
    console.error('加载因子列表失败:', error)
    ElMessage.error('加载因子列表失败')
  } finally {
    loading.value = false
  }
}

// 新建因子
const handleCreate = () => {
  editingFactor.value = null
  formData.value = {
    name: '',
    category: '',
    source: '',
    code: '',
    parameters: {},
    description: '',
  }
  dialogVisible.value = true
}

// 编辑因子
const handleEdit = (factor: Factor) => {
  editingFactor.value = factor
  formData.value = {
    name: factor.name,
    category: factor.category || '',
    source: factor.source || '',
    code: factor.code || '',
    parameters: factor.parameters || {},
    description: factor.description || '',
  }
  dialogVisible.value = true
}

// 查看详情
const handleDetail = (factor: Factor) => {
  handleEdit(factor)
}

// 提交表单
const handleSubmit = async () => {
  if (!formRef.value) return

  await formRef.value.validate(async (valid) => {
    if (!valid) return

    submitting.value = true
    try {
      if (editingFactor.value) {
        await factorApi.update(editingFactor.value.id, formData.value)
        ElMessage.success('更新成功')
      } else {
        await factorApi.create(formData.value)
        ElMessage.success('创建成功')
      }
      dialogVisible.value = false
      loadFactors()
    } catch (error: unknown) {
      const err = error as { response?: { data?: { detail?: string } } }
      ElMessage.error(err.response?.data?.detail || '操作失败')
    } finally {
      submitting.value = false
    }
  })
}

// 删除因子
const handleDelete = async (factor: Factor) => {
  try {
    await ElMessageBox.confirm(`确定删除因子 "${factor.name}" 吗？`, '确认删除', {
      type: 'warning',
    })
    await factorApi.delete(factor.id)
    ElMessage.success('删除成功')
    loadFactors()
  } catch (error) {
    if (error !== 'cancel') {
      ElMessage.error('删除失败')
    }
  }
}

// 打开分析对话框
const handleAnalyze = (factor: Factor) => {
  analyzingFactor.value = factor
  // 设置默认日期范围
  const end = new Date()
  const start = new Date()
  start.setFullYear(start.getFullYear() - 1)
  analyzeForm.value = {
    start_date: formatDate(start),
    end_date: formatDate(end),
    normalize_window: factor.parameters?.normalize_window as number || 5,
    factor_window: factor.parameters?.factor_window as number || 20,
    forward_period: factor.parameters?.forward_period as number || 20,
  }
  analyzeDialogVisible.value = true
}

// 提交分析
const submitAnalyze = async () => {
  if (!analyzingFactor.value) return

  analyzing.value = true
  try {
    const result = await factorApi.analyze(analyzingFactor.value.id, analyzeForm.value)
    ElMessage.success('分析完成')
    analyzeDialogVisible.value = false
    // 跳转到分析结果页面
    router.push(`/factor/analysis/${result.id}`)
  } catch (error: unknown) {
    const err = error as { response?: { data?: { detail?: string } } }
    ElMessage.error(err.response?.data?.detail || '分析失败')
  } finally {
    analyzing.value = false
  }
}

// 格式化日期
const formatDate = (date: Date): string => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

// 格式化日期时间
const formatDateTime = (datetime: string | null): string => {
  if (!datetime) return '-'
  return datetime.replace('T', ' ').substring(0, 19)
}

// 初始化
onMounted(() => {
  loadFactors()
})
</script>

<style scoped>
.factor-list {
  padding: 20px;
}

.header {
  margin-bottom: 20px;
}
</style>

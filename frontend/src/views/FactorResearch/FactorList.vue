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
      <el-table-column label="操作" width="150" fixed="right">
        <template #default="{ row }">
          <el-button-group>
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
            <el-option label="自定义因子库" value="custom" />
          </el-select>
        </el-form-item>
        <el-form-item label="来源" prop="source">
          <el-select v-model="formData.source" placeholder="选择来源" clearable>
            <el-option label="自定义" value="custom" />
            <el-option label="QMT" value="qmt" />
          </el-select>
        </el-form-item>
        <el-form-item label="因子代码" prop="code">
          <CodeEditor
            v-model="factorCode"
            language="expression"
            :min-height="180"
            placeholder="请输入因子计算代码"
          />
        </el-form-item>
        <el-form-item label="参数配置" prop="parameters">
          <CodeEditor
            v-model="parametersJson"
            language="json"
            :min-height="130"
            placeholder='{"source_type": "dsl"}'
          />
          <div v-if="parametersJsonError" class="json-error">{{ parametersJsonError }}</div>
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
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { Plus } from '@element-plus/icons-vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import type { FormInstance, FormRules } from 'element-plus'
import { factorApi, type Factor, type FactorCreateRequest } from '@/api/factor'
import CodeEditor from '@/components/CodeEditor.vue'

// 状态
const loading = ref(false)
const factors = ref<Factor[]>([])
const dialogVisible = ref(false)
const submitting = ref(false)
const editingFactor = ref<Factor | null>(null)
const formRef = ref<FormInstance>()
const parametersJsonError = ref('')

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
      parametersJsonError.value = ''
    } catch {
      parametersJsonError.value = 'JSON 格式错误，修正后才能保存'
    }
  },
})

const factorCode = computed({
  get: () => formData.value.code || '',
  set: (val: string) => {
    formData.value.code = val
  },
})

const formRules: FormRules = {
  name: [{ required: true, message: '请输入因子名称', trigger: 'blur' }],
}

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
    if (parametersJsonError.value) {
      ElMessage.warning(parametersJsonError.value)
      return
    }

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

.json-error {
  margin-top: 6px;
  color: var(--el-color-danger);
  font-size: 12px;
  line-height: 1.4;
}
</style>

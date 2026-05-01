<template>
  <el-dialog v-model="visible" title="新建因子" width="720px" @close="resetForm">
    <div class="step-section">
      <h4>选择模板</h4>
      <div class="template-grid">
        <div
          v-for="tpl in templates"
          :key="tpl.id"
          :class="['template-card', { selected: selectedTemplate?.id === tpl.id }]"
          @click="selectTemplate(tpl)"
        >
          <div class="template-name">{{ tpl.name }}</div>
          <div class="template-desc">{{ tpl.description }}</div>
          <div class="template-cat">{{ tpl.category }}</div>
        </div>
      </div>
    </div>

    <div class="step-section" v-if="selectedTemplate">
      <h4>因子表达式</h4>
      <el-input
        v-model="expression"
        type="textarea"
        :rows="3"
        placeholder="输入因子表达式，例如 (1/PE_TTM) + ROE"
        class="expression-input"
      />
      <div class="validate-row">
        <el-button size="small" @click="doValidate" :loading="validating">
          {{ validationDone ? (valid ? '✓ 表达式有效' : '✗ 表达式无效') : '验证表达式' }}
        </el-button>
        <span v-if="validationDone && !valid" class="error-msg">{{ errorMsg }}</span>
      </div>
    </div>

    <div class="step-section" v-if="selectedTemplate">
      <h4>参数配置</h4>
      <el-form :model="params" label-width="80px" size="small">
        <el-form-item label="股票池">
          <el-select v-model="params.stock_pool">
            <el-option label="沪深300" value="hs300" />
            <el-option label="中证500" value="zz500" />
            <el-option label="中证800" value="zz800" />
            <el-option label="中证1000" value="zz1000" />
            <el-option label="中证全指" value="zz_quanzhi" />
          </el-select>
        </el-form-item>
        <el-form-item label="日期范围">
          <el-date-picker
            v-model="dateRange"
            type="daterange"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            value-format="YYYY-MM-DD"
          />
        </el-form-item>
        <el-form-item label="因子方向">
          <el-radio-group v-model="params.direction">
            <el-radio value="desc">越大越好</el-radio>
            <el-radio value="asc">越小越好</el-radio>
          </el-radio-group>
        </el-form-item>
      </el-form>
    </div>

    <template #footer>
      <el-button @click="visible = false">取消</el-button>
      <el-button type="primary" @click="doCreate" :disabled="!canCreate">创建并计算</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { factorApi, computeApi } from '@/api/v2'
import type { FactorTemplate } from '@/types/factor'

const props = defineProps<{ visible: boolean }>()
const emit = defineEmits<{ (e: 'update:visible', v: boolean): void; (e: 'created'): void }>()

const visible = computed({
  get: () => props.visible,
  set: (v) => emit('update:visible', v),
})

const templates = ref<FactorTemplate[]>([])
const selectedTemplate = ref<FactorTemplate | null>(null)
const expression = ref('')
const params = ref({ stock_pool: 'hs300', direction: 'desc' })
const dateRange = ref<[string, string]>(['2020-01-01', '2025-12-31'])
const validating = ref(false)
const validationDone = ref(false)
const valid = ref(false)
const errorMsg = ref('')

const canCreate = computed(() =>
  selectedTemplate.value && expression.value.trim() && validationDone.value && valid.value
)

function selectTemplate(tpl: FactorTemplate) {
  selectedTemplate.value = tpl
  expression.value = tpl.preset_expression
  params.value = { stock_pool: 'hs300', direction: tpl.preset_params?.direction || 'desc' }
  validationDone.value = false
}

async function doValidate() {
  validating.value = true
  try {
    const res = await factorApi.validate({ expression: expression.value })
    valid.value = res.valid
    errorMsg.value = res.error || ''
    validationDone.value = true
  } catch {
    valid.value = false
    errorMsg.value = '验证请求失败'
    validationDone.value = true
  } finally {
    validating.value = false
  }
}

async function doCreate() {
  if (!canCreate.value) return
  await computeApi.evaluate({
    expression: expression.value,
    stock_pool: params.value.stock_pool as any,
    start_date: dateRange.value[0],
    end_date: dateRange.value[1],
    direction: params.value.direction as any,
  })
  emit('created')
  visible.value = false
}

function resetForm() {
  selectedTemplate.value = null
  expression.value = ''
  validationDone.value = false
  valid.value = false
}

onMounted(async () => {
  const res = await factorApi.getTemplates()
  templates.value = res
})
</script>

<style scoped>
.step-section { margin-bottom: 20px; }
.step-section h4 { font-size: 13px; font-weight: 600; margin-bottom: 10px; color: var(--text-bright); }
.template-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.template-card {
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 12px;
  cursor: pointer;
  transition: border-color 0.2s;
}
.template-card:hover { border-color: var(--accent-primary); }
.template-card.selected { border-color: var(--accent-primary); background: rgba(56, 189, 248, 0.05); }
.template-name { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.template-desc { font-size: 11px; color: var(--text-ghost); margin-bottom: 4px; }
.template-cat { font-size: 10px; color: var(--text-muted); }
.expression-input { font-family: 'JetBrains Mono', monospace; font-size: 13px; }
.validate-row { margin-top: 8px; display: flex; align-items: center; gap: 12px; }
.error-msg { font-size: 12px; color: var(--color-red); }
</style>

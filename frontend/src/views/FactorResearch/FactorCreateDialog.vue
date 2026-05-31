<template>
  <el-dialog v-model="visible" title="新建因子" width="900px" @close="resetForm">
    <!-- 因子名称 -->
    <div class="step-section">
      <h4>因子名称</h4>
      <el-input
        v-model="factorName"
        placeholder="输入因子名称，例如：动量因子、低估值因子"
        maxlength="50"
      />
    </div>

    <!-- 编写模式 -->
    <div class="step-section">
      <h4>编写模式</h4>
      <el-radio-group v-model="sourceType" @change="onSourceTypeChange">
        <el-radio value="dsl">DSL 表达式</el-radio>
        <el-radio value="python">Python 代码（本地可信执行）</el-radio>
      </el-radio-group>
    </div>

    <!-- 模板选择 (仅 DSL 模式) -->
    <div class="step-section" v-if="sourceType === 'dsl'">
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

    <!-- 表达式编辑 (DSL 模式) -->
    <div class="step-section" v-if="sourceType === 'dsl' && selectedTemplate">
      <div class="expr-header">
        <h4>因子表达式</h4>
        <el-button size="small" text @click="showRef = !showRef">
          {{ showRef ? '收起参考' : '展开参考' }}
          <el-icon class="toggle-icon" :class="{ rotated: showRef }"><ArrowRight /></el-icon>
        </el-button>
      </div>
      <div class="expression-layout" :class="{ 'has-ref': showRef }">
        <div class="expression-main">
          <CodeEditor
            v-model="expression"
            language="expression"
            :min-height="140"
            class="expression-input"
            placeholder="输入因子表达式，例如 ts_mean($close, 20) / ts_std($close, 20)"
          />
          <div class="validate-row">
            <el-button size="small" @click="doValidate" :loading="validating">
              {{ validationDone ? (valid ? '✓ 表达式有效' : '✗ 表达式无效') : '验证表达式' }}
            </el-button>
            <span v-if="validationDone && !valid" class="error-msg">{{ errorMsg }}</span>
          </div>
        </div>

        <div v-if="showRef" class="reference-panel">
          <div class="ref-section">
            <div class="ref-title">基础字段 <code>$</code> 前缀</div>
            <div class="ref-tags">
              <el-tooltip v-for="f in rawFields" :key="f.name" :content="f.description" placement="top">
                <span class="ref-tag ref-raw" @click="insertField(f.name)">{{ f.name }}</span>
              </el-tooltip>
            </div>
          </div>
          <div class="ref-section">
            <div class="ref-title">算子函数</div>
            <div v-for="cat in operatorGroups" :key="cat.label" class="ref-sub">
              <div class="ref-subtitle">{{ cat.label }}</div>
              <div class="ref-tags">
                <el-tooltip v-for="op in cat.items" :key="op.name" :content="op.signature + ' — ' + op.description" placement="top">
                  <span class="ref-tag ref-op" @click="insertOperator(op.name)">{{ op.name }}</span>
                </el-tooltip>
              </div>
            </div>
          </div>
          <el-button size="small" text @click="loadOperators" :loading="loadingRef" style="margin-top:8px">
            {{ operators.length ? '刷新' : '加载参考数据' }}
          </el-button>
        </div>
      </div>
    </div>

    <!-- Python 代码编辑 -->
    <div class="step-section" v-if="sourceType === 'python'">
      <div class="expr-header">
        <h4>Python 代码</h4>
        <span class="python-hint">函数签名: <code>def compute(data, context) -> pd.DataFrame</code></span>
      </div>
      <CodeEditor
        v-model="pythonCode"
        language="python"
        :min-height="300"
        class="python-input"
        placeholder="def compute(data, context):
    &quot;&quot;&quot;
    data: dict[str, pd.DataFrame] — daily / stock_info / financial / factor_values
    context: dict — symbols / start_date / end_date / params / stock_pool / benchmark / trading_calendar
    return: pd.DataFrame with columns [symbol, trade_date, value]
    &quot;&quot;&quot;
    ..."
      />
      <div class="validate-row" style="margin-top: 10px">
        <el-tag type="warning" size="small">本地可信执行 — 代码将在后端服务器上运行，请确保代码安全</el-tag>
        <el-button size="small" @click="doPythonTest" :loading="pythonTesting">
          试运行
        </el-button>
        <span v-if="pythonTestResult" class="test-result">{{ pythonTestResult }}</span>
      </div>
    </div>

    <!-- 参数配置 -->
    <div class="step-section" v-if="selectedTemplate">
      <h4>参数配置</h4>
      <el-form :model="params" label-width="80px" size="small">
        <el-form-item label="股票池">
          <el-select v-model="params.stock_pool">
            <el-option-group label="指数股票池">
              <el-option
                v-for="pool in stockPoolOptions"
                :key="pool.value"
                :label="pool.label"
                :value="pool.value"
              />
            </el-option-group>
            <el-option-group v-if="props.watchlistGroups?.length" label="自选股分组">
              <el-option
                v-for="g in props.watchlistGroups"
                :key="'wl_'+g.id"
                :label="g.name"
                :value="'watchlist_'+g.id"
              />
            </el-option-group>
          </el-select>
        </el-form-item>
        <el-form-item label="日期范围">
          <el-date-picker
            v-model="startDate"
            type="date"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            value-format="YYYY-MM-DD"
          />
          <span class="date-separator">至</span>
          <el-date-picker
            v-model="endDate"
            type="date"
            placeholder="结束日期"
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
      <el-button type="primary" @click="doCreate" :disabled="!canCreate">创建因子</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { ArrowRight } from '@element-plus/icons-vue'
import { factorApi, computeApi } from '@/api/factorResearch'
import CodeEditor from '@/components/CodeEditor.vue'
import type { FactorTemplate } from '@/types/factor'

interface WatchlistGroup { id: number; name: string }

const props = defineProps<{ visible: boolean; watchlistGroups?: WatchlistGroup[] }>()
const emit = defineEmits<{ (e: 'update:visible', v: boolean): void; (e: 'created'): void }>()

const visible = computed({
  get: () => props.visible,
  set: (v) => emit('update:visible', v),
})

const factorName = ref('')
const sourceType = ref<'dsl' | 'python'>('dsl')
const pythonCode = ref('')
const pythonTesting = ref(false)
const pythonTestResult = ref('')
const templates = ref<FactorTemplate[]>([])
const selectedTemplate = ref<FactorTemplate | null>(null)
const expression = ref('')
const params = ref({ stock_pool: 'hs300', direction: 'desc' })
const startDate = ref('2020-01-01')
const endDate = ref('2025-12-31')
const validating = ref(false)
const validationDone = ref(false)
const valid = ref(false)
const errorMsg = ref('')
const showRef = ref(false)

const operators = ref<{ name: string; signature: string; description: string; level: number; category: string }[]>([])
const loadingRef = ref(false)
const stockPoolOptions = [
  { label: '沪深300', value: 'hs300' },
  { label: '中证500', value: 'zz500' },
  { label: '中证800', value: 'zz800' },
  { label: '中证1000', value: 'zz1000' },
  { label: '创业板指', value: 'chinext' },
  { label: '创业板50', value: 'chinext50' },
  { label: '创业板综', value: 'chinext_composite' },
  { label: '科创50', value: 'star50' },
  { label: '科创100', value: 'star100' },
]

const canCreate = computed(() => {
  if (!factorName.value.trim()) return false
  if (sourceType.value === 'python') {
    return pythonCode.value.trim().length > 0
  }
  return selectedTemplate.value && expression.value.trim() && validationDone.value && valid.value
})

const rawFields = computed(() => operators.value.filter(o => o.level === 0))
const operatorGroups = computed(() => {
  const groups: Record<string, { label: string; items: typeof operators.value }> = {
    math: { label: '数学运算', items: [] },
    rolling: { label: '滚动窗口', items: [] },
    ta: { label: '技术指标', items: [] },
  }
  for (const op of operators.value) {
    if (op.level === 0) continue
    if (op.level === 1) groups.math.items.push(op)
    else if (op.level === 2) groups.rolling.items.push(op)
    else groups.ta.items.push(op)
  }
  return Object.values(groups).filter(g => g.items.length)
})

function onSourceTypeChange() {
  validationDone.value = false
  valid.value = false
  pythonTestResult.value = ''
}

async function doPythonTest() {
  pythonTesting.value = true
  pythonTestResult.value = ''
  try {
    const res = await factorApi.validatePython({ code: pythonCode.value } as any)
    if (res.valid) {
      pythonTestResult.value = '✓ 校验通过'
    } else {
      pythonTestResult.value = '✗ ' + (res.error || '校验失败')
    }
  } catch (e: any) {
    pythonTestResult.value = '✗ ' + (e?.response?.data?.detail || e?.message || '校验失败')
  } finally {
    pythonTesting.value = false
  }
}

function selectTemplate(tpl: FactorTemplate) {
  selectedTemplate.value = tpl
  expression.value = tpl.preset_expression
  params.value = { stock_pool: 'hs300', direction: tpl.preset_params?.direction || 'desc' }
  validationDone.value = false
}

function insertField(name: string) {
  expression.value = expression.value + ' ' + name
}

function insertOperator(name: string) {
  expression.value = expression.value + ' ' + name + '()'
}

async function loadOperators() {
  loadingRef.value = true
  try {
    const res = await computeApi.operators() as any
    operators.value = res || []
  } catch {
    // operators endpoint unavailable, use fallback
  } finally {
    loadingRef.value = false
  }
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
  try {
    if (sourceType.value === 'python') {
      await factorApi.create({
        name: factorName.value.trim(),
        expression: pythonCode.value,
        source_type: 'python',
        engine: 'python',
        stock_pool: params.value.stock_pool,
        direction: params.value.direction,
        default_stock_pool: params.value.stock_pool,
        default_benchmark: '000905.SH',
        cache_enabled: true,
        category: 'custom',
        description: '',
        params: {
          ...params.value,
          kind: 'factor',
          engine: 'python',
          source_type: 'python',
        },
      } as any)
    } else {
      await factorApi.create({
        name: factorName.value.trim(),
        expression: expression.value,
        source_type: 'dsl',
        engine: 'builtin',
        stock_pool: params.value.stock_pool,
        direction: params.value.direction,
        default_stock_pool: params.value.stock_pool,
        default_benchmark: '000905.SH',
        cache_enabled: true,
        category: 'custom',
        params: {
          ...params.value,
          kind: selectedTemplate.value?.type === 'technical' ? 'indicator' : 'factor',
          engine: 'builtin',
          source_type: 'dsl',
        },
      } as any)
    }
    ElMessage.success(`因子"${factorName.value}"创建成功`)
    emit('created')
    visible.value = false
  } catch {
    ElMessage.error('创建失败')
  }
}

function resetForm() {
  factorName.value = ''
  selectedTemplate.value = null
  expression.value = ''
  validationDone.value = false
  valid.value = false
  showRef.value = false
}

onMounted(async () => {
  const [tplRes] = await Promise.all([
    factorApi.getTemplates(),
    loadOperators(),
  ])
  templates.value = tplRes
})
</script>

<style scoped>
.step-section { margin-bottom: 24px; }
.step-section h4 { font-size: 13px; font-weight: 600; margin-bottom: 10px; color: var(--text-bright); }

.date-separator {
  margin: 0 8px;
  color: var(--text-secondary);
}

.template-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.template-card {
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 14px;
  cursor: pointer;
  transition: border-color 0.2s;
}
.template-card:hover { border-color: var(--accent-primary); }
.template-card.selected { border-color: var(--accent-primary); background: rgba(56, 189, 248, 0.05); }
.template-name { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.template-desc { font-size: 11px; color: var(--text-ghost); margin-bottom: 4px; }
.template-cat { font-size: 10px; color: var(--text-muted); }

.expr-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
}
.expr-header h4 { margin-bottom: 0; }

.toggle-icon {
  font-size: 14px;
  transition: transform 0.2s;
}
.toggle-icon.rotated { transform: rotate(90deg); }

.expression-layout { display: block; }
.expression-layout.has-ref { display: grid; grid-template-columns: 1fr 280px; gap: 20px; }

.expression-main { min-width: 0; }
.expression-input,
.python-input {
  width: 100%;
}
.python-hint { font-size: 11px; color: var(--text-ghost); }
.python-hint code { background: var(--bg-surface); padding: 2px 6px; border-radius: 3px; font-size: 11px; }
.test-result { font-size: 12px; color: var(--text-bright); }
.validate-row { margin-top: 10px; display: flex; align-items: center; gap: 12px; }
.error-msg { font-size: 12px; color: var(--color-red); }

.reference-panel {
  border: 1px solid var(--border-ghost);
  border-radius: 8px;
  padding: 14px;
  background: var(--bg-surface);
  max-height: 240px;
  overflow-y: auto;
  font-size: 12px;
}
.ref-section { margin-bottom: 12px; }
.ref-title { font-weight: 600; margin-bottom: 6px; color: var(--text-bright); }
.ref-sub { margin-top: 8px; }
.ref-subtitle { font-size: 11px; color: var(--text-ghost); margin-bottom: 4px; }
.ref-tags { display: flex; flex-wrap: wrap; gap: 5px; }
.ref-tag {
  display: inline-block;
  padding: 3px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-family: 'JetBrains Mono', monospace;
  cursor: pointer;
  transition: background 0.15s;
}
.ref-tag:hover { opacity: 0.75; }
.ref-raw { background: #e6f7ff; color: #1890ff; border: 1px solid #91d5ff; }
.ref-op { background: #f6ffed; color: #52c41a; border: 1px solid #b7eb8f; }
</style>

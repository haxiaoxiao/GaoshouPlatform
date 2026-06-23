<template>
  <div class="factor-value-store">
    <section class="toolbar">
      <div class="toolbar-header">
        <div class="store-title">
          <span class="panel-kicker">FACTOR DEFINITION / PRECOMPUTE</span>
          <strong>因子定义与预计算</strong>
          <span>管理因子目录、缓存覆盖、截面预览和预计算；表达式只在创建或编辑时打开。</span>
        </div>
        <div class="toolbar-meta">
          <span>{{ selectedDefinition?.frequency || '频率未知' }}</span>
          <strong>查询 {{ form.startDate }} 至 {{ form.endDate }}</strong>
          <small v-if="coverage">实际 {{ actualCoverageRange }}</small>
        </div>
      </div>
      <el-form :inline="true" :model="form" label-width="76px" class="control-form">
        <el-form-item label="分组" class="group-form-item">
          <el-select-v2
            v-model="form.groupName"
            :options="groupOptions"
            filterable
            :item-height="48"
            :height="420"
            popper-class="factor-select-dropdown factor-select-dropdown--group"
            class="factor-select factor-select--group"
          >
            <template #default="{ item }">
              <div class="factor-option">
                <span class="factor-option__label">{{ item.displayName || item.label }}</span>
                <span class="factor-option__meta">{{ item.value }}</span>
              </div>
            </template>
          </el-select-v2>
        </el-form-item>
        <el-form-item label="因子" class="factor-form-item factor-form-item--wide">
          <el-select-v2
            v-model="form.factorName"
            :options="factorOptions"
            filterable
            :item-height="48"
            :height="420"
            popper-class="factor-select-dropdown factor-select-dropdown--wide"
            class="factor-select factor-select--wide"
          >
            <template #default="{ item }">
              <div class="factor-option">
                <span class="factor-option__label">{{ item.displayName || item.label }}</span>
                <span class="factor-option__meta">{{ item.value }}</span>
              </div>
            </template>
          </el-select-v2>
        </el-form-item>
        <el-form-item label="股票池">
          <el-select-v2
            v-model="form.indexSymbol"
            :options="indexOptions"
            filterable
            clearable
            class="factor-select"
          />
        </el-form-item>
        <el-form-item label="查询日期">
          <div class="date-control">
            <div class="date-control__row">
              <el-date-picker
                v-model="form.startDate"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="开始日期"
                class="date-range"
              />
              <span class="date-separator">至</span>
              <el-date-picker
                v-model="form.endDate"
                type="date"
                value-format="YYYY-MM-DD"
                placeholder="结束日期"
                class="date-range"
              />
            </div>
            <div class="date-shortcuts">
              <el-tag
                v-for="shortcut in dateShortcuts"
                :key="shortcut.key"
                size="small"
                effect="plain"
                class="date-shortcut"
                @click="setDateShortcut(shortcut)"
              >
                {{ shortcut.label }}
              </el-tag>
            </div>
          </div>
        </el-form-item>
        <el-form-item
          v-for="param in paramEntries"
          :key="param.name"
          :label="param.label"
        >
          <el-switch
            v-if="param.type === 'boolean'"
            v-model="paramValues[param.name]"
          />
          <el-input-number
            v-else-if="param.type === 'integer' || param.type === 'number'"
            v-model="paramValues[param.name]"
            :step="param.type === 'integer' ? 1 : 0.05"
            controls-position="right"
            class="number-input"
          />
          <el-input
            v-else
            v-model="paramValues[param.name]"
            class="param-input"
          />
        </el-form-item>
        <el-form-item class="action-item">
          <el-button :icon="Search" @click="loadCoverage" :loading="coverageLoading">
            覆盖率
          </el-button>
          <el-button type="primary" :icon="Refresh" @click="runPrecompute" :loading="precomputeLoading">
            预计算
          </el-button>
          <el-button :icon="Refresh" @click="runGroupPrecompute" :loading="groupPrecomputeLoading">
            分组预计算
          </el-button>
          <el-button :icon="View" @click="loadPreview" :loading="previewLoading">
            预览
          </el-button>
        </el-form-item>
      </el-form>
      <div class="action-help">
        <span>覆盖率：只检查当前因子在缓存里的实际日期、股票数和行数，不会重新计算。</span>
        <span>预览：读取实际最新可用日期的截面样本，展示最高和最低的因子值。</span>
        <span>预计算：真正生成并写入因子缓存。</span>
      </div>
    </section>

    <el-alert
      v-if="definitionsError"
      type="warning"
      class="result-alert"
      :title="`因子定义加载失败：${definitionsError}`"
    />

    <el-alert
      v-if="coverageError"
      type="warning"
      class="result-alert"
      :title="`覆盖率查询失败：${coverageError}`"
    />

    <section class="summary" v-if="coverage">
      <div>
        <span class="label">缓存行数</span>
        <strong>{{ coverage.total_rows.toLocaleString() }}</strong>
      </div>
      <div>
        <span class="label">实际股票数</span>
        <strong>{{ coverage.symbol_count.toLocaleString() }}</strong>
        <small v-if="coverage.requested_symbol_count">/ {{ coverage.requested_symbol_count }}</small>
      </div>
      <div>
        <span class="label">实际日期数</span>
        <strong>{{ coverage.date_count.toLocaleString() }}</strong>
      </div>
      <div>
        <span class="label">实际缓存范围</span>
        <strong>{{ actualCoverageRange }}</strong>
        <small>查询 {{ form.startDate }} - {{ form.endDate }}</small>
      </div>
      <div>
        <span class="label">预览日期</span>
        <strong>{{ previewTradeDate }}</strong>
        <small v-if="hasPreviewDateFallback">使用实际最新有数日</small>
      </div>
    </section>

    <section class="precompute-progress" v-if="activeTask">
      <div class="precompute-progress__header">
        <div>
          <strong>{{ activeTask.title || '因子预计算' }}</strong>
          <span>
            阶段
            <template v-if="taskStageIndex && taskStageTotal">{{ taskStageIndex }} / {{ taskStageTotal }} · </template>
            {{ taskStage }}
          </span>
        </div>
        <span>总进度 {{ taskPercent.toFixed(1) }}%</span>
      </div>
      <div class="precompute-progress__bars">
        <el-progress :percentage="taskPercent" :stroke-width="10" :status="taskProgressStatus" />
        <div class="precompute-progress__stage-bar">
          <span>阶段内 {{ taskStagePercent.toFixed(1) }}%</span>
          <el-progress :percentage="taskStagePercent" :stroke-width="7" :show-text="false" />
        </div>
      </div>
      <div class="precompute-progress__grid" v-if="taskDetailRows.length">
        <div v-for="row in taskDetailRows" :key="row.label" class="precompute-progress__metric">
          <span>{{ row.label }}</span>
          <strong>{{ row.value }}</strong>
        </div>
      </div>
      <div class="precompute-progress__stages" v-if="taskStageWeights.length">
        <span
          v-for="stage in taskStageWeights"
          :key="stage.name"
          :class="{
            'is-active': stage.name === activeStageKey,
            'is-done': stage.index < taskStageIndex,
          }"
        >
          <b>{{ stage.index }}</b>
          {{ stage.name }}
          <small>{{ Math.round(stage.weight * 100) }}%</small>
        </span>
      </div>
    </section>

    <el-alert
      v-if="lastResult"
      type="success"
      :closable="false"
      class="result-alert"
      :title="`已为 ${lastResult.symbols.toLocaleString()} 只股票预计算 ${lastResult.rows_written.toLocaleString()} 行`"
    />

    <section class="group-result" v-if="hasGroupResultRows">
      <div class="group-result__header">
        <div>
          <strong>分组预计算结果</strong>
          <span>
            请求 {{ lastResult?.requested_factor_count || groupResultRows.length }} 个因子，
            有写入 {{ lastResult?.written_factor_count || groupWrittenFactorCount }} 个
          </span>
        </div>
        <el-tag v-if="lastResult?.zero_row_factor_count" type="warning" effect="plain">
          {{ lastResult.zero_row_factor_count }} 个 0 行
        </el-tag>
      </div>
      <el-table :data="groupResultRows" size="small" max-height="280">
        <el-table-column prop="displayName" label="因子" min-width="180" show-overflow-tooltip>
          <template #default="{ row }">
            <div class="group-result__factor">
              <strong>{{ row.displayName }}</strong>
              <span>{{ row.name }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="rows" label="本次写入" width="120" align="right">
          <template #default="{ row }">{{ row.rows.toLocaleString() }}</template>
        </el-table-column>
        <el-table-column label="实际覆盖" min-width="180">
          <template #default="{ row }">{{ formatCoverageRange(row) }}</template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag :type="row.rows > 0 ? 'success' : 'warning'" effect="plain" size="small">
              {{ row.rows > 0 ? '已写入' : '0 行' }}
            </el-tag>
          </template>
        </el-table-column>
      </el-table>
    </section>

    <el-alert
      v-if="dependencySyncStatus && ['queued', 'running'].includes(dependencySyncStatus.status)"
      type="info"
      :closable="false"
      class="result-alert"
      :title="`正在同步因子依赖数据：${dependencySyncStatus.progress_percent.toFixed(1)}%，可在数据同步模块查看进度。`"
    />

    <section class="coverage-warning" v-if="incompleteCoverageRanges.length">
      <div>
        <strong>部分因子未覆盖到结束日期</strong>
        <span>通常是上游数据只同步到更早日期，预计算会按实际有数日期写入。</span>
      </div>
      <div class="coverage-warning__tags">
        <el-tag
          v-for="item in incompleteCoverageRanges"
          :key="item.factor_name"
          type="warning"
          effect="plain"
          size="small"
        >
          {{ item.factor_name }} 至 {{ item.max_date || '无数据' }}
        </el-tag>
      </div>
    </section>

    <el-alert
      v-if="previewError"
      type="warning"
      class="result-alert"
      :title="`预览失败：${previewError}`"
    />

    <el-alert
      v-if="preview && preview.total === 0"
      type="warning"
      class="result-alert"
      :title="`${preview.trade_date} 没有可预览数据，请检查实际缓存范围或先预计算。`"
    />

    <div class="preview-dual" v-if="preview && preview.total > 0">
      <div class="preview-col">
        <div class="preview-title">
          <h5>最高值</h5>
          <span>{{ preview.trade_date }} / {{ preview.total.toLocaleString() }} 只</span>
        </div>
        <el-table :data="topItems" stripe size="small" max-height="300">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="name" label="名称" min-width="120" />
          <el-table-column prop="value" label="值" align="right" width="120">
            <template #default="{ row }">{{ formatValue(row.value) }}</template>
          </el-table-column>
        </el-table>
      </div>
      <div class="preview-col">
        <div class="preview-title">
          <h5>最低值</h5>
          <span>{{ preview.trade_date }} / {{ preview.total.toLocaleString() }} 只</span>
        </div>
        <el-table :data="bottomItems" stripe size="small" max-height="300">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="name" label="名称" min-width="120" />
          <el-table-column prop="value" label="值" align="right" width="120">
            <template #default="{ row }">{{ formatValue(row.value) }}</template>
          </el-table-column>
        </el-table>
      </div>
    </div>

    <section class="group-panel" v-if="selectedGroup">
      <div>
        <strong>{{ selectedGroup.display_name }}</strong>
        <span>{{ selectedGroup.description }}</span>
      </div>
      <div class="group-tags">
        <el-tag v-for="name in selectedGroup.factor_names" :key="name" size="small">
          {{ name }}
        </el-tag>
      </div>
    </section>

    <section class="table-panel">
      <div class="table-panel__header">
        <div>
          <strong>因子定义</strong>
          <span>{{ filteredDefinitions.length.toLocaleString() }} 个可用因子</span>
        </div>
      </div>
      <el-table class="definition-table" :data="filteredDefinitions" v-loading="definitionsLoading" stripe height="100%">
        <el-table-column prop="display_name" label="名称" min-width="190" />
        <el-table-column prop="name" label="键名" min-width="180" />
        <el-table-column prop="factor_type" label="类型" width="110" />
        <el-table-column prop="category" label="分类" width="120" />
        <el-table-column prop="frequency" label="频率" width="120" />
        <el-table-column prop="as_of_time" label="时间点" width="100" />
        <el-table-column label="时间安全" width="90" align="center">
          <template #default="{ row }">
            <el-tag :type="row.point_in_time_safe ? 'success' : 'warning'" size="small">
              {{ row.point_in_time_safe ? '是' : '待检查' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="依赖" min-width="220">
          <template #default="{ row }">
            <el-tag
              v-for="dep in row.dependencies"
              :key="dep"
              size="small"
              class="dep-tag"
            >
              {{ dep }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="description" label="描述" min-width="320" show-overflow-tooltip />
      </el-table>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Refresh, Search, View } from '@element-plus/icons-vue'
import { indexCatalogApi, type IndexCatalogItem } from '@/api/data'
import { syncApi, type SyncStatus } from '@/api/sync'
import { runtimeTaskApi, type RuntimeTask } from '@/api/runtimeTasks'
import {
  factorValueApi,
  type FactorCoverageGap,
  type FactorValueCoverage,
  type FactorValueDefinition,
  type FactorValueGroup,
  type FactorValuePreview,
  type FactorValuePrecomputeResult,
} from '@/api/factorValues'

type PrecomputeMode = 'single' | 'group'
type PrecomputeParamValue = string | number | boolean

interface PersistedPrecomputeTask {
  taskId: string
  mode: PrecomputeMode
  factorName: string
  groupName: string
  indexSymbol: string
  startDate: string
  endDate: string
  params: Record<string, PrecomputeParamValue>
  updatedAt: number
}

const ACTIVE_PRECOMPUTE_TASK_KEY = 'gaoshou.factorValueStore.activePrecomputeTask'
const PRECOMPUTE_POLL_CANCELLED = 'PRECOMPUTE_POLL_CANCELLED'
const COMPLETED_TASK_STATUSES = new Set(['done', 'completed'])
const TERMINAL_TASK_STATUSES = new Set(['done', 'completed', 'failed', 'cancelled'])

const definitions = ref<FactorValueDefinition[]>([])
const groups = ref<FactorValueGroup[]>([])
const coverage = ref<FactorValueCoverage | null>(null)
const coverageError = ref('')
const definitionsError = ref('')
const preview = ref<FactorValuePreview | null>(null)
const previewError = ref('')
const lastResult = ref<FactorValuePrecomputeResult | null>(null)
const definitionsLoading = ref(false)
const coverageLoading = ref(false)
const precomputeLoading = ref(false)
const groupPrecomputeLoading = ref(false)
const previewLoading = ref(false)
const dependencySyncStatus = ref<SyncStatus | null>(null)
const activeTask = ref<RuntimeTask | null>(null)
let precomputePollTimer: number | null = null
let precomputePollResolve: (() => void) | null = null
let precomputePollRunId = 0
let isSyncingFactorFromGroup = false
let isAutoQuerySuspended = true

const legacyIndexOptions = [
  { label: '中小综指 / 小市值 399101.SZ', value: '399101.SZ' },
  { label: '沪深300 000300.SH', value: '000300.SH' },
  { label: '中证500 000905.SH', value: '000905.SH' },
  { label: '中证1000 000852.SH', value: '000852.SH' },
  { label: '中证全指 000985.SH', value: '000985.SH' },
  { label: '上证指数 000001.SH', value: '000001.SH' },
  { label: '深证成指 399001.SZ', value: '399001.SZ' },
  { label: '创业板指 399006.SZ', value: '399006.SZ' },
  { label: '全部已缓存股票', value: '' },
]

const indexCatalog = ref<IndexCatalogItem[]>([])
void legacyIndexOptions
const indexOptions = computed(() => {
  const poolOptions = indexCatalog.value
    .filter(item => item.pool_enabled)
    .map(item => ({
      label: `${item.display_name} ${item.symbol}`,
      value: item.symbol,
    }))
  return [...poolOptions, { label: '全部已缓存股票', value: '' }]
})

const formatDate = (date: Date) => date.toISOString().slice(0, 10)

const getDateBefore = ({ months = 0, years = 0 }: { months?: number; years?: number }) => {
  const value = new Date()
  value.setMonth(value.getMonth() - months)
  value.setFullYear(value.getFullYear() - years)
  return formatDate(value)
}

const dateShortcuts = [
  { key: '3m', label: '过去3个月', months: 3, years: 0 },
  { key: '1y', label: '过去1年', months: 0, years: 1 },
  { key: '3y', label: '过去3年', months: 0, years: 3 },
  { key: '5y', label: '过去5年', months: 0, years: 5 },
]

const form = reactive({
  groupName: '',
  factorName: 'market_cap',
  indexSymbol: '399101.SZ',
  startDate: getDateBefore({ years: 1 }),
  endDate: formatDate(new Date()),
})
const paramValues = reactive<Record<string, PrecomputeParamValue>>({})

const setDateShortcut = (shortcut: { months: number; years: number }) => {
  form.startDate = getDateBefore({ months: shortcut.months, years: shortcut.years })
  form.endDate = formatDate(new Date())
}

const selectedDefinition = computed(() => definitions.value.find(item => item.name === form.factorName))
const selectedGroup = computed(() => groups.value.find(item => item.name === form.groupName))
const selectedGroupFactorNames = computed(() => selectedGroup.value?.factor_names || [])
const definitionByName = computed(() => new Map(definitions.value.map(item => [item.name, item])))
const filteredDefinitions = computed(() => {
  const factorNames = selectedGroupFactorNames.value
  if (!factorNames.length) return definitions.value
  const selectedNames = new Set(factorNames)
  return definitions.value.filter(item => selectedNames.has(item.name))
})
const groupOptions = computed(() => groups.value.map(group => ({
  label: group.display_name,
  displayName: group.display_name,
  value: group.name,
})))
const factorOptions = computed(() => filteredDefinitions.value.map(item => ({
  label: `${item.display_name} (${item.name})`,
  displayName: item.display_name,
  value: item.name,
})))
const previewTradeDate = computed(() => coverage.value?.max_date || form.endDate)
const hasPreviewDateFallback = computed(() => Boolean(coverage.value?.max_date && coverage.value.max_date !== form.endDate))
const actualCoverageRange = computed(() => {
  if (!coverage.value?.min_date || !coverage.value?.max_date) return '无数据'
  return `${coverage.value.min_date} - ${coverage.value.max_date}`
})
const taskPercent = computed(() => {
  const raw = Number(activeTask.value?.progress || 0) * 100
  return Math.max(0, Math.min(100, raw))
})
const taskStage = computed(() => String(activeTask.value?.meta?.stage || activeTask.value?.status || '等待开始'))
const taskMeta = computed(() => activeTask.value?.meta || {})
const taskStagePercent = computed(() => clampPercent(Number(taskMeta.value.stage_percent || 0)))
const taskStageIndex = computed(() => Number(taskMeta.value.stage_index || 0))
const taskStageTotal = computed(() => Number(taskMeta.value.stage_total || 0))
const activeStageKey = computed(() => String(taskMeta.value.stage_key || taskStage.value))
const taskStageWeights = computed(() => {
  const raw = taskMeta.value.stage_weights
  if (!Array.isArray(raw)) return []
  return raw
    .map(item => ({
      name: String((item as Record<string, unknown>).name || ''),
      weight: Number((item as Record<string, unknown>).weight || 0),
      index: Number((item as Record<string, unknown>).index || 0),
    }))
    .filter(item => item.name && item.index > 0)
})
const taskDetailRows = computed(() => {
  const meta = taskMeta.value
  const rows = [
    { label: '当前日期', value: formatTaskValue(meta.current_date) },
    { label: '交易日进度', value: formatRatio(meta.current_day, meta.total_days) },
    { label: '记录进度', value: formatRatio(meta.current, meta.total) },
    { label: '当前因子', value: formatTaskValue(meta.factor_name) },
    { label: '待写入', value: formatNumberValue(meta.rows_buffered, '行') },
    { label: '已写入', value: formatNumberValue(meta.rows_written, '行') },
  ]
  return rows.filter(row => row.value)
})
const taskProgressStatus = computed(() => {
  if (activeTask.value?.status === 'failed') return 'exception'
  if (['done', 'completed'].includes(String(activeTask.value?.status))) return 'success'
  return undefined
})

const clampPercent = (value: number) => {
  if (!Number.isFinite(value)) return 0
  return Math.max(0, Math.min(100, value))
}

const formatTaskValue = (value: unknown) => {
  if (value === undefined || value === null || value === '') return ''
  return String(value)
}

const formatNumberValue = (value: unknown, suffix = '') => {
  const numberValue = Number(value)
  if (!Number.isFinite(numberValue)) return ''
  return `${numberValue.toLocaleString()}${suffix}`
}

const formatRatio = (current: unknown, total: unknown) => {
  const currentValue = Number(current)
  const totalValue = Number(total)
  if (!Number.isFinite(currentValue) || !Number.isFinite(totalValue) || totalValue <= 0) return ''
  return `${currentValue.toLocaleString()} / ${totalValue.toLocaleString()}`
}

const formatRequestError = (error: unknown) => error instanceof Error ? error.message : '请求失败'

const isPrecomputeParamValue = (value: unknown): value is PrecomputeParamValue => {
  return ['string', 'number', 'boolean'].includes(typeof value)
}

const toParamRecord = (value: unknown): Record<string, PrecomputeParamValue> => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {}
  const params: Record<string, PrecomputeParamValue> = {}
  Object.entries(value as Record<string, unknown>).forEach(([key, item]) => {
    if (isPrecomputeParamValue(item)) params[key] = item
  })
  return params
}

const replaceParamValues = (params: Record<string, PrecomputeParamValue>) => {
  Object.keys(paramValues).forEach(key => delete paramValues[key])
  Object.entries(params).forEach(([key, value]) => {
    paramValues[key] = value
  })
}

const readPersistedPrecomputeTask = (): PersistedPrecomputeTask | null => {
  try {
    const raw = window.sessionStorage.getItem(ACTIVE_PRECOMPUTE_TASK_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as Partial<PersistedPrecomputeTask>
    if (!parsed.taskId) return null
    return {
      taskId: parsed.taskId,
      mode: parsed.mode === 'group' ? 'group' : 'single',
      factorName: parsed.factorName || form.factorName,
      groupName: parsed.groupName || form.groupName,
      indexSymbol: parsed.indexSymbol || form.indexSymbol,
      startDate: parsed.startDate || form.startDate,
      endDate: parsed.endDate || form.endDate,
      params: toParamRecord(parsed.params),
      updatedAt: Number(parsed.updatedAt || Date.now()),
    }
  } catch {
    return null
  }
}

const clearPersistedPrecomputeTask = (taskId?: string) => {
  const saved = readPersistedPrecomputeTask()
  if (taskId && saved?.taskId && saved.taskId !== taskId) return
  window.sessionStorage.removeItem(ACTIVE_PRECOMPUTE_TASK_KEY)
}

const suspendAutoQueryForFormRestore = () => {
  isAutoQuerySuspended = true
  window.setTimeout(() => {
    isAutoQuerySuspended = false
  }, 0)
}

const applyPersistedPrecomputeTask = (snapshot: PersistedPrecomputeTask) => {
  suspendAutoQueryForFormRestore()
  form.groupName = snapshot.groupName
  form.factorName = snapshot.factorName
  form.indexSymbol = snapshot.indexSymbol
  form.startDate = snapshot.startDate
  form.endDate = snapshot.endDate
  syncDefaultsFromDefinition()
  replaceParamValues(snapshot.params)
}

const persistPrecomputeTask = (taskId: string, mode: PrecomputeMode) => {
  const snapshot: PersistedPrecomputeTask = {
    taskId,
    mode,
    factorName: form.factorName,
    groupName: form.groupName,
    indexSymbol: form.indexSymbol,
    startDate: form.startDate,
    endDate: form.endDate,
    params: toParamRecord(paramValues),
    updatedAt: Date.now(),
  }
  window.sessionStorage.setItem(ACTIVE_PRECOMPUTE_TASK_KEY, JSON.stringify(snapshot))
}

const precomputeModeFromTask = (
  task: RuntimeTask,
  saved: PersistedPrecomputeTask | null = null,
): PrecomputeMode => {
  if (saved?.mode) return saved.mode
  if (String(task.task_id).startsWith('factor-group-') || task.meta?.group_name) return 'group'
  return 'single'
}

const precomputeSnapshotFromTask = (task: RuntimeTask, saved: PersistedPrecomputeTask | null = null) => {
  const meta = task.meta || {}
  const factorNames = Array.isArray(meta.factor_names) ? meta.factor_names.map(String).filter(Boolean) : []
  return {
    taskId: task.task_id,
    mode: precomputeModeFromTask(task, saved),
    factorName: saved?.factorName || factorNames[0] || form.factorName,
    groupName: saved?.groupName || String(meta.group_name || form.groupName),
    indexSymbol: saved?.indexSymbol || String(meta.index_symbol || form.indexSymbol || ''),
    startDate: saved?.startDate || String(meta.start_date || form.startDate),
    endDate: saved?.endDate || String(meta.end_date || form.endDate),
    params: saved?.params || toParamRecord(meta.params),
    updatedAt: Date.now(),
  } satisfies PersistedPrecomputeTask
}

const isPrecomputeRuntimeTask = (task: RuntimeTask) => {
  return task.kind === 'factor_precompute'
    || String(task.task_id).startsWith('factor-')
    || String(task.task_id).startsWith('factor-group-')
}

const isTaskCompleted = (task: RuntimeTask) => COMPLETED_TASK_STATUSES.has(String(task.status))
const isTaskTerminal = (task: RuntimeTask) => TERMINAL_TASK_STATUSES.has(String(task.status))
const isPollCancelledError = (error: unknown) => {
  return error instanceof Error && error.message === PRECOMPUTE_POLL_CANCELLED
}
const isUserDismissedDialog = (error: unknown) => {
  return error === 'cancel' || error === 'close'
}

const formatCoverageRange = (row: { coverage: { min_date: string | null; max_date: string | null } | null }) => {
  if (!row.coverage?.min_date || !row.coverage?.max_date) return '无数据'
  return `${row.coverage.min_date} - ${row.coverage.max_date}`
}

const incompleteCoverageRanges = computed(() => {
  return (lastResult.value?.coverage_ranges || []).filter(item => !item.is_complete_to_end)
})
const groupResultRows = computed(() => {
  const result = lastResult.value
  if (!result) return []
  const rowCounts = result.rows || {}
  const factorNames = result.factor_names?.length ? result.factor_names : Object.keys(rowCounts)
  const coverageByName = new Map((result.coverage_ranges || []).map(item => [item.factor_name, item]))
  return factorNames.map(name => ({
    name,
    displayName: definitionByName.value.get(name)?.display_name || name,
    rows: Number(rowCounts[name] || 0),
    coverage: coverageByName.get(name) || null,
  }))
})
const hasGroupResultRows = computed(() => groupResultRows.value.length > 1)
const groupWrittenFactorCount = computed(() => groupResultRows.value.filter(row => row.rows > 0).length)
const paramEntries = computed(() => {
  const schema = (selectedDefinition.value?.params_schema || {}) as Record<string, Record<string, unknown>>
  return Object.entries(schema).map(([name, meta]) => ({
    name,
    label: String(meta.label || meta.title || name),
    type: String(meta.type || 'string'),
  }))
})

const ensureFactorInSelectedGroup = () => {
  if (!filteredDefinitions.value.length) return false
  if (filteredDefinitions.value.some(item => item.name === form.factorName)) return false
  isSyncingFactorFromGroup = true
  form.factorName = filteredDefinitions.value[0].name
  return true
}

const syncDefaultsFromDefinition = () => {
  const def = selectedDefinition.value
  if (!def) return
  Object.keys(paramValues).forEach(key => delete paramValues[key])
  const schema = def.params_schema as Record<string, { type?: string; default?: unknown }>
  Object.entries(schema).forEach(([name, meta]) => {
    const raw = meta.default ?? (name === 'time' || name === 'as_of_time' ? def.as_of_time : undefined)
    if (meta.type === 'integer' || meta.type === 'number') {
      paramValues[name] = raw === undefined || raw === '' ? 0 : Number(raw)
    } else if (meta.type === 'boolean') {
      paramValues[name] = Boolean(raw)
    } else {
      paramValues[name] = raw === undefined || raw === null ? '' : String(raw)
    }
  })
}

const buildFactorParams = () => {
  const params: Record<string, unknown> = {}
  Object.entries(paramValues).forEach(([key, value]) => {
    if (value !== '' && value !== null && value !== undefined) {
      params[key] = value
    }
  })
  return params
}

const buildQueryParams = () => {
  const params = buildFactorParams()
  const query: {
    factor_name: string
    start_date: string
    end_date: string
    index_symbol?: string
    as_of_time?: string
    window?: number
    threshold?: number
    daily_volume_to_share_multiplier?: number
  } = {
    factor_name: form.factorName,
    start_date: form.startDate,
    end_date: form.endDate,
    index_symbol: form.indexSymbol || undefined,
  }
  const time = params.time ?? params.as_of_time
  if (time) query.as_of_time = String(time)
  if (params.window !== undefined) query.window = Number(params.window)
  if (params.threshold !== undefined) query.threshold = Number(params.threshold)
  if (params.daily_volume_to_share_multiplier !== undefined) {
    query.daily_volume_to_share_multiplier = Number(params.daily_volume_to_share_multiplier)
  }
  return query
}

const loadDefinitions = async () => {
  definitionsLoading.value = true
  definitionsError.value = ''
  try {
    const [nextDefinitions, nextGroups] = await Promise.all([
      factorValueApi.definitions(),
      factorValueApi.groups(),
    ])
    definitions.value = nextDefinitions
    groups.value = nextGroups
    if (!form.groupName && groups.value.length) {
      form.groupName = groups.value[0].name
    }
    ensureFactorInSelectedGroup()
    syncDefaultsFromDefinition()
  } catch (error) {
    definitionsError.value = formatRequestError(error)
  } finally {
    definitionsLoading.value = false
  }
}

const loadIndexCatalog = async () => {
  const items = await indexCatalogApi.list()
  indexCatalog.value = items
  const firstPoolSymbol = items.find(item => item.pool_enabled)?.symbol || ''
  const availableSymbols = new Set(items.filter(item => item.pool_enabled).map(item => item.symbol))
  if (form.indexSymbol && !availableSymbols.has(form.indexSymbol)) {
    form.indexSymbol = firstPoolSymbol
  } else if (!form.indexSymbol && firstPoolSymbol) {
    form.indexSymbol = firstPoolSymbol
  }
}

const loadCoverage = async () => {
  coverageLoading.value = true
  coverageError.value = ''
  try {
    coverage.value = await factorValueApi.coverage({
      ...buildQueryParams(),
      full_range: true,
    })
  } catch (error) {
    coverage.value = null
    coverageError.value = formatRequestError(error)
  } finally {
    coverageLoading.value = false
  }
}

const singlePrecomputePayload = () => ({
  factor_names: [form.factorName],
  start_date: form.startDate,
  end_date: form.endDate,
  index_symbol: form.indexSymbol || undefined,
  params: buildFactorParams(),
})

const groupPrecomputePayload = () => ({
  group_name: form.groupName,
  factor_names: selectedGroupFactorNames.value,
  start_date: form.startDate,
  end_date: form.endDate,
  index_symbol: form.indexSymbol || undefined,
  params: buildFactorParams(),
})

const finishPrecompute = async (message: string) => {
  ElMessage.success(message)
  await loadCoverage()
  await loadPreview()
}

const clearPrecomputePolling = () => {
  precomputePollRunId += 1
  if (precomputePollTimer !== null) {
    window.clearTimeout(precomputePollTimer)
    precomputePollTimer = null
  }
  const resolve = precomputePollResolve
  precomputePollResolve = null
  if (resolve) resolve()
}

const waitForNextPrecomputePoll = () => {
  return new Promise<void>(resolve => {
    precomputePollResolve = resolve
    precomputePollTimer = window.setTimeout(() => {
      precomputePollTimer = null
      precomputePollResolve = null
      resolve()
    }, 1000)
  })
}

const completePrecomputeFromTask = async (
  task: RuntimeTask,
  mode: PrecomputeMode,
  message: string,
) => {
  const recoveredResult = lastResult.value
  const taskResult = task.meta?.result as FactorValuePrecomputeResult | undefined
  if (taskResult) {
    lastResult.value = taskResult
  } else if (!recoveredResult) {
    await recoverCompletedPrecompute(task, mode)
  }
  clearPersistedPrecomputeTask(task.task_id)
  await finishPrecompute(message)
}

const resumePrecomputeTask = async (
  taskId: string,
  mode: PrecomputeMode,
  message: string,
) => {
  if (mode === 'group') {
    groupPrecomputeLoading.value = true
  } else {
    precomputeLoading.value = true
  }
  try {
    const task = await pollPrecomputeTask(taskId, mode)
    await completePrecomputeFromTask(task, mode, message)
  } catch (error) {
    if (isPollCancelledError(error)) return
    clearPersistedPrecomputeTask(taskId)
    throw error
  } finally {
    if (mode === 'group') {
      groupPrecomputeLoading.value = false
    } else {
      precomputeLoading.value = false
    }
  }
}

const pollPrecomputeTask = async (taskId: string, mode: PrecomputeMode = 'single'): Promise<RuntimeTask> => {
  clearPrecomputePolling()
  const pollRunId = precomputePollRunId
  for (;;) {
    if (pollRunId !== precomputePollRunId) throw new Error(PRECOMPUTE_POLL_CANCELLED)
    const task = await runtimeTaskApi.get(taskId)
    activeTask.value = task
    if (isTaskCompleted(task)) return task
    if (task.status === 'failed') {
      const recovered = await recoverCompletedPrecompute(task, mode)
      if (recovered) return task
      throw new Error(task.error || '因子预计算失败')
    }
    await waitForNextPrecomputePoll()
  }
}

const recoverCompletedPrecompute = async (task: RuntimeTask, mode: PrecomputeMode) => {
  const result = task.meta?.result as FactorValuePrecomputeResult | undefined
  if (result && Number(result.rows_written || 0) > 0) return true
  if (mode === 'group') return recoverCompletedGroupPrecompute(task, result)
  const coverage = await factorValueApi.coverage({
    ...buildQueryParams(),
    full_range: false,
  })
  if (Number(coverage.total_rows || 0) <= 0) return false
  lastResult.value = {
    task_id: task.task_id,
    symbols: Number(coverage.symbol_count || coverage.requested_symbol_count || 0),
    start_date: form.startDate,
    end_date: form.endDate,
    as_of_time: String(buildFactorParams().time || ''),
    window: Number(buildFactorParams().window || 0),
    threshold: Number(buildFactorParams().threshold || 0),
    factor_names: [form.factorName],
    rows: { [form.factorName]: Number(coverage.total_rows || 0) },
    rows_written: Number(coverage.total_rows || 0),
    coverage_ranges: [{
      factor_name: form.factorName,
      total_rows: Number(coverage.total_rows || 0),
      symbol_count: Number(coverage.symbol_count || 0),
      date_count: Number(coverage.date_count || 0),
      min_date: coverage.min_date,
      max_date: coverage.max_date,
      is_complete_to_end: coverage.max_date === form.endDate,
    }],
  }
  ElMessage.warning('任务状态返回失败，但已检测到因子缓存写入，已按完成处理并刷新覆盖率。')
  return true
}

const recoverCompletedGroupPrecompute = async (
  task: RuntimeTask,
  result: FactorValuePrecomputeResult | undefined,
) => {
  const factorNames = result?.factor_names?.length
    ? result.factor_names
    : selectedGroupFactorNames.value
  if (!factorNames.length) return false
  const baseParams = buildQueryParams()
  const coverageItems = await Promise.all(factorNames.map(async factorName => {
    try {
      const item = await factorValueApi.coverage({
        ...baseParams,
        factor_name: factorName,
        full_range: false,
      })
      return { factorName, item }
    } catch {
      return null
    }
  }))
  const rows: Record<string, number> = {}
  const ranges: NonNullable<FactorValuePrecomputeResult['coverage_ranges']> = []
  coverageItems.forEach(entry => {
    if (!entry) return
    const totalRows = Number(entry.item.total_rows || 0)
    rows[entry.factorName] = totalRows
    ranges.push({
      factor_name: entry.factorName,
      total_rows: totalRows,
      symbol_count: Number(entry.item.symbol_count || 0),
      date_count: Number(entry.item.date_count || 0),
      min_date: entry.item.min_date,
      max_date: entry.item.max_date,
      is_complete_to_end: entry.item.max_date === form.endDate,
    })
  })
  const rowsWritten = Object.values(rows).reduce((sum, count) => sum + count, 0)
  if (rowsWritten <= 0) return false
  lastResult.value = {
    task_id: task.task_id,
    symbols: ranges.reduce((max, item) => Math.max(max, item.symbol_count), 0),
    start_date: form.startDate,
    end_date: form.endDate,
    as_of_time: String(buildFactorParams().time || ''),
    window: Number(buildFactorParams().window || 0),
    threshold: Number(buildFactorParams().threshold || 0),
    factor_names: factorNames,
    rows,
    rows_written: rowsWritten,
    requested_factor_count: factorNames.length,
    written_factor_count: Object.values(rows).filter(count => count > 0).length,
    zero_row_factor_count: Object.values(rows).filter(count => count <= 0).length,
    zero_row_factor_names: factorNames.filter(name => Number(rows[name] || 0) <= 0),
    coverage_ranges: ranges,
  }
  ElMessage.warning('任务状态返回失败，但已检测到因子集合缓存写入，已按完成处理并刷新覆盖率。')
  return true
}

const resultFromTask = (task: RuntimeTask, fallback: FactorValuePrecomputeResult) => {
  return (task.meta?.result as FactorValuePrecomputeResult | undefined) || fallback
}

const attachPrecomputeTask = async (
  task: RuntimeTask,
  saved: PersistedPrecomputeTask | null = null,
) => {
  const snapshot = precomputeSnapshotFromTask(task, saved)
  const mode = snapshot.mode
  applyPersistedPrecomputeTask(snapshot)
  activeTask.value = task
  if (isTaskCompleted(task)) {
    await completePrecomputeFromTask(task, mode, '因子预计算已完成')
    return true
  }
  if (task.status === 'failed') {
    clearPersistedPrecomputeTask(task.task_id)
    ElMessage.error(task.error || '因子预计算失败')
    return true
  }
  if (isTaskTerminal(task)) {
    clearPersistedPrecomputeTask(task.task_id)
    return false
  }
  persistPrecomputeTask(task.task_id, mode)
  void resumePrecomputeTask(
    task.task_id,
    mode,
    mode === 'group' ? '因子集合预计算完成' : '因子值预计算完成',
  ).catch(error => {
    if (!isPollCancelledError(error)) {
      ElMessage.error(formatRequestError(error))
    }
  })
  return true
}

const restorePrecomputeTask = async () => {
  const saved = readPersistedPrecomputeTask()
  if (saved) {
    try {
      const task = await runtimeTaskApi.get(saved.taskId)
      return await attachPrecomputeTask(task, saved)
    } catch {
      clearPersistedPrecomputeTask(saved.taskId)
    }
  }
  try {
    const tasks = await runtimeTaskApi.list(false)
    const task = tasks.find(isPrecomputeRuntimeTask)
    if (task) return await attachPrecomputeTask(task)
  } catch {
    return false
  }
  return false
}

const runDirectPrecompute = async () => {
  activeTask.value = null
  const started = await factorValueApi.precompute({ ...singlePrecomputePayload(), async_task: true })
  if (started.task_id) {
    persistPrecomputeTask(started.task_id, 'single')
    try {
      const task = await pollPrecomputeTask(started.task_id, 'single')
      const recoveredResult = lastResult.value
      lastResult.value = resultFromTask(task, recoveredResult || started)
      clearPersistedPrecomputeTask(started.task_id)
      await finishPrecompute('因子值预计算完成')
      return
    } catch (error) {
      if (isPollCancelledError(error)) return
      clearPersistedPrecomputeTask(started.task_id)
      throw error
    }
  }
  lastResult.value = started
  await finishPrecompute('因子值预计算完成')
}

const runDirectGroupPrecompute = async () => {
  activeTask.value = null
  const started = await factorValueApi.precomputeGroup({ ...groupPrecomputePayload(), async_task: true })
  if (started.task_id) {
    persistPrecomputeTask(started.task_id, 'group')
    try {
      const task = await pollPrecomputeTask(started.task_id, 'group')
      const recoveredResult = lastResult.value
      lastResult.value = task ? resultFromTask(task, recoveredResult || started) : started
      clearPersistedPrecomputeTask(started.task_id)
      await finishPrecompute('因子集合预计算完成')
      return
    } catch (error) {
      if (isPollCancelledError(error)) return
      clearPersistedPrecomputeTask(started.task_id)
      throw error
    }
  }
  lastResult.value = started
  await finishPrecompute('因子集合预计算完成')
}

const formatGapText = (gap: FactorCoverageGap) => {
  const latest = gap.latest_date || '无数据'
  return `${gap.label}: 当前至 ${latest}，目标至 ${gap.required_end}（${gap.reason}）`
}

const confirmDependencySync = async (gaps: FactorCoverageGap[]) => {
  const message = gaps.map(formatGapText).join('\n')
  await ElMessageBox.confirm(
    `预计算依赖数据不足，是否先生成数据同步任务？\n\n${message}`,
    '需要先同步数据',
    {
      confirmButtonText: '确认同步并继续',
      cancelButtonText: '取消',
      type: 'warning',
      dangerouslyUseHTMLString: false,
    },
  )
}

const waitForDependencySync = async () => {
  for (;;) {
    const status = await syncApi.getStatus()
    dependencySyncStatus.value = status
    if (status.status === 'completed') return
    if (status.status === 'failed') {
      throw new Error(status.error_message || '数据同步失败')
    }
    await new Promise(resolve => window.setTimeout(resolve, 3000))
  }
}

const prepareAndRunPrecompute = async (mode: 'single' | 'group') => {
  const isGroup = mode === 'group'
  if (isGroup && !selectedGroupFactorNames.value.length) {
    throw new Error('当前分组没有可预计算因子')
  }
  const prepare = await factorValueApi.prepare({
    ...(isGroup ? groupPrecomputePayload() : singlePrecomputePayload()),
    mode,
    factor_names: isGroup ? selectedGroupFactorNames.value : [form.factorName],
    group_name: isGroup ? form.groupName : null,
  })
  if (!prepare.can_precompute && prepare.sync_plan) {
    await confirmDependencySync(prepare.coverage_gaps)
    const status = await syncApi.trigger({
      sync_type: 'factor_dependency',
      failure_strategy: 'stop',
      factor_sync_plan: prepare.sync_plan,
    })
    dependencySyncStatus.value = status
    ElMessage.success(`数据同步已启动：${status.task_id || status.details?.run_id || ''}`)
    await waitForDependencySync()
    ElMessage.success('依赖数据同步完成，继续预计算')
  }
  if (isGroup) {
    await runDirectGroupPrecompute()
  } else {
    await runDirectPrecompute()
  }
}

const runPrecompute = async () => {
  precomputeLoading.value = true
  try {
    await prepareAndRunPrecompute('single')
  } catch (error) {
    if (!isUserDismissedDialog(error)) throw error
  } finally {
    precomputeLoading.value = false
  }
}

const runGroupPrecompute = async () => {
  groupPrecomputeLoading.value = true
  try {
    await prepareAndRunPrecompute('group')
  } catch (error) {
    if (!isUserDismissedDialog(error)) throw error
  } finally {
    groupPrecomputeLoading.value = false
  }
}

const loadPreview = async () => {
  previewLoading.value = true
  previewError.value = ''
  try {
    preview.value = await factorValueApi.preview({
      ...buildQueryParams(),
      trade_date: previewTradeDate.value,
      limit: 80,
    })
    if (preview.value?.items?.length)
      await loadStockNames(preview.value.items.map(item => item.symbol))
  } catch (error) {
    preview.value = null
    previewError.value = formatRequestError(error)
  } finally {
    previewLoading.value = false
  }
}

const stockNames = ref<Record<string, string>>({})

const sortedItems = computed(() => {
  if (!preview.value) return []
  return [...preview.value.items].sort((a, b) => a.value - b.value)
})

const topItems = computed(() => sortedItems.value.slice(-20).reverse().map(item => ({
  ...item,
  name: stockNames.value[item.symbol] || '',
})))

const bottomItems = computed(() => sortedItems.value.slice(0, 20).map(item => ({
  ...item,
  name: stockNames.value[item.symbol] || '',
})))

const loadStockNames = async (symbols: string[]) => {
  if (!symbols.length) return
  const missing = symbols.filter(s => !stockNames.value[s])
  if (!missing.length) return
  try {
    const params = new URLSearchParams()
    missing.forEach(s => params.append('symbols', s))
    const res = await fetch('/api/skill/stocks/batch?' + params.toString(), { method: 'POST' })
    if (res.ok) {
      const json = await res.json()
      const snapshots: Record<string, { name?: string }> = json?.data || {}
      for (const [symbol, snap] of Object.entries(snapshots)) {
        if (snap?.name) {
          stockNames.value[symbol] = snap.name
        }
      }
    }
  } catch { /* ignore */ }
}

const formatValue = (value: number) => {
  if (Math.abs(value) >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  return Number(value).toFixed(6).replace(/\.?0+$/, '')
}

const resetQueryResults = () => {
  coverage.value = null
  coverageError.value = ''
  preview.value = null
  previewError.value = ''
  lastResult.value = null
}

const autoQuery = async () => {
  await loadCoverage()
  if (coverageError.value) return
  await loadPreview()
}

watch(() => form.factorName, () => {
  syncDefaultsFromDefinition()
  resetQueryResults()
  if (isSyncingFactorFromGroup) {
    isSyncingFactorFromGroup = false
    return
  }
  if (!isAutoQuerySuspended) void autoQuery()
})

watch(
  () => form.groupName,
  () => {
    ensureFactorInSelectedGroup()
    syncDefaultsFromDefinition()
    resetQueryResults()
    if (!isAutoQuerySuspended) void autoQuery()
  }
)

watch(
  () => [form.indexSymbol, form.startDate, form.endDate],
  () => {
    resetQueryResults()
    if (!isAutoQuerySuspended) void autoQuery()
  }
)

onMounted(async () => {
  await Promise.all([loadDefinitions(), loadIndexCatalog()])
  const restored = await restorePrecomputeTask()
  isAutoQuerySuspended = false
  if (!restored) await autoQuery()
})

onBeforeUnmount(() => {
  clearPrecomputePolling()
})
</script>

<style scoped>
.factor-value-store {
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-height: 0;
  --factor-surface: rgba(19, 19, 24, 0.96);
  --factor-surface-strong: rgba(14, 15, 20, 0.98);
  --factor-surface-soft: rgba(36, 39, 51, 0.72);
  --factor-border: rgba(108, 117, 137, 0.22);
  --factor-border-strong: rgba(96, 165, 250, 0.24);
  --factor-row-striped: rgba(255, 255, 255, 0.025);
  --factor-row-hover: rgba(56, 189, 248, 0.06);
}

.toolbar {
  position: sticky;
  top: 0;
  z-index: 12;
  padding: 13px 14px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(135deg, rgba(56, 189, 248, 0.07), transparent 32%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)),
    var(--bg-surface);
  box-shadow: var(--shadow-sm);
  backdrop-filter: blur(12px);
}

.toolbar-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 12px;
}

.store-title {
  display: flex;
  flex-direction: column;
  gap: 3px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.panel-kicker {
  font-family: var(--font-data);
  font-size: 10px;
  color: var(--accent-primary);
}

.store-title strong {
  font-size: 15px;
  color: var(--text-bright);
}

.toolbar-meta {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
  padding: 7px 10px;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  background: rgba(10, 10, 12, 0.38);
  color: var(--text-muted);
  font-size: 11px;
  white-space: nowrap;
}

.toolbar-meta strong {
  font-family: var(--font-data);
  color: var(--text-bright);
  font-size: 12px;
  font-weight: 500;
}

.toolbar-meta small {
  color: var(--text-secondary);
  font-family: var(--font-data);
}

.control-form {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
  gap: 2px 10px;
  align-items: start;
}

.control-form :deep(.el-form-item) {
  margin-right: 0;
  margin-bottom: 10px;
  min-width: 0;
}

.control-form :deep(.el-form-item__label) {
  color: var(--text-secondary);
  font-size: 12px;
}

.factor-select {
  width: 100%;
}

.group-form-item {
  grid-column: span 2;
}

.factor-select--group {
  min-width: min(100%, 520px);
}

.factor-form-item--wide {
  grid-column: span 2;
}

.factor-select--wide {
  min-width: min(100%, 520px);
}

.short-input {
  width: 100%;
}

.date-range {
  width: 100%;
}

.date-control {
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.date-control__row,
.date-shortcuts {
  display: flex;
  align-items: center;
  gap: 6px;
}

.date-shortcut {
  cursor: pointer;
  user-select: none;
  border-color: rgba(56, 189, 248, 0.24);
  background: rgba(56, 189, 248, 0.055);
  color: var(--accent-primary);
}

.date-shortcut:hover {
  border-color: rgba(56, 189, 248, 0.48);
  background: rgba(56, 189, 248, 0.11);
}

.date-separator {
  color: var(--el-text-color-secondary);
}

.param-input {
  width: 120px;
}

.number-input {
  width: 100%;
}

.action-item {
  grid-column: 1 / -1;
  margin-left: 0;
}

.action-help {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 14px;
  padding-top: 2px;
  color: var(--text-secondary);
  font-size: 12px;
  line-height: 1.5;
}

.summary {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 8px;
}

.summary > div {
  padding: 11px 12px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(180deg, rgba(56, 189, 248, 0.045), transparent),
    var(--bg-elevated);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.025);
}

.summary .label {
  display: block;
  margin-bottom: 4px;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.summary strong {
  font-family: var(--font-data);
  font-size: 18px;
  font-weight: 600;
  color: var(--text-bright);
}

.summary small {
  margin-left: 4px;
  color: var(--el-text-color-secondary);
}

.result-alert {
  margin: 0;
}

.group-result {
  display: grid;
  gap: 10px;
  padding: 12px 14px;
  border: 1px solid rgba(56, 189, 248, 0.2);
  border-radius: 8px;
  background: var(--bg-elevated);
}

.group-result__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.group-result__header strong,
.group-result__factor strong {
  display: block;
  color: var(--text-bright);
  font-size: 13px;
}

.group-result__header span,
.group-result__factor span {
  color: var(--text-secondary);
  font-size: 12px;
}

.group-result__factor {
  display: grid;
  gap: 2px;
  min-width: 0;
}

.precompute-progress {
  padding: 12px 14px;
  border: 1px solid rgba(56, 189, 248, 0.26);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(56, 189, 248, 0.08), transparent 36%),
    var(--bg-elevated);
}

.precompute-progress__header {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 8px;
}

.precompute-progress__header div {
  display: flex;
  flex-direction: column;
  gap: 3px;
}

.precompute-progress__header strong {
  color: var(--text-bright);
  font-size: 13px;
}

.precompute-progress__header span,
.precompute-progress__stage-bar,
.precompute-progress__metric span,
.precompute-progress__stages {
  color: var(--text-secondary);
  font-size: 12px;
}

.precompute-progress__bars {
  display: grid;
  gap: 8px;
}

.precompute-progress__stage-bar {
  display: grid;
  grid-template-columns: 96px 1fr;
  align-items: center;
  gap: 10px;
}

.precompute-progress__grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(132px, 1fr));
  gap: 8px;
  margin-top: 8px;
}

.precompute-progress__metric {
  display: grid;
  gap: 3px;
  min-width: 0;
  padding: 7px 9px;
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 7px;
  background: rgba(15, 23, 42, 0.42);
}

.precompute-progress__metric strong {
  overflow: hidden;
  color: var(--text-bright);
  font-size: 12px;
  font-family: var(--font-data);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.precompute-progress__stages {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 10px;
}

.precompute-progress__stages span {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  min-height: 25px;
  padding: 3px 7px;
  border: 1px solid rgba(148, 163, 184, 0.18);
  border-radius: 999px;
  background: rgba(15, 23, 42, 0.38);
}

.precompute-progress__stages b {
  display: inline-grid;
  place-items: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: rgba(148, 163, 184, 0.16);
  color: var(--text-secondary);
  font-size: 10px;
  font-weight: 700;
}

.precompute-progress__stages small {
  color: var(--text-muted);
  font-size: 11px;
}

.precompute-progress__stages span.is-done {
  border-color: rgba(34, 197, 94, 0.28);
  color: rgba(187, 247, 208, 0.92);
}

.precompute-progress__stages span.is-active {
  border-color: rgba(56, 189, 248, 0.46);
  background: rgba(56, 189, 248, 0.12);
  color: var(--text-bright);
}

.precompute-progress__stages span.is-active b {
  background: rgba(56, 189, 248, 0.24);
  color: var(--accent-primary);
}

.coverage-warning {
  display: grid;
  grid-template-columns: minmax(220px, 360px) 1fr;
  gap: 12px;
  padding: 11px 12px;
  border: 1px solid rgba(251, 191, 36, 0.28);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(251, 191, 36, 0.075), transparent 34%),
    var(--bg-elevated);
}

.coverage-warning strong {
  display: block;
  margin-bottom: 4px;
  color: var(--accent-warning);
  font-size: 13px;
}

.coverage-warning span {
  color: var(--text-secondary);
  font-size: 12px;
}

.coverage-warning__tags {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}

.group-panel {
  display: grid;
  grid-template-columns: minmax(220px, 340px) 1fr;
  gap: 12px;
  padding: 12px 14px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(167, 139, 250, 0.055), transparent 28%),
    var(--bg-elevated);
}

.group-panel strong {
  display: block;
  margin-bottom: 4px;
}

.group-panel span {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.group-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.preview-dual {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.preview-col {
  padding: 11px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.026), transparent),
    var(--bg-elevated);
  min-width: 0;
}

.preview-title {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 7px;
}

.preview-title h5 {
  margin: 0;
  font-size: 13px;
  color: var(--el-text-color-primary);
}

.preview-title span {
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 11px;
}

.preview-table {
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
}

.dep-tag {
  margin-right: 4px;
  margin-bottom: 4px;
}

.table-panel {
  display: flex;
  flex-direction: column;
  min-height: 0;
  height: min(58vh, 720px);
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-elevated);
  overflow: hidden;
}

.table-panel__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 12px;
  border-bottom: 1px solid var(--border-subtle);
  background: rgba(10, 10, 12, 0.42);
}

.table-panel__header div {
  display: flex;
  flex-direction: column;
  gap: 3px;
}

.table-panel__header strong {
  color: var(--text-bright);
  font-size: 14px;
}

.table-panel__header span {
  color: var(--text-secondary);
  font-size: 12px;
}

:deep(.el-table) {
  --el-table-bg-color: transparent;
  --el-table-tr-bg-color: transparent;
  --el-table-row-hover-bg-color: var(--factor-row-hover);
  --el-table-border-color: var(--factor-border);
  --el-table-header-bg-color: rgba(10, 10, 12, 0.72);
  --el-table-text-color: var(--text-primary);
  --el-table-header-text-color: var(--text-secondary);
  border: 0;
  border-radius: 0;
  overflow: hidden;
  background: transparent;
}

:deep(.el-table__body tr),
:deep(.el-table__body td.el-table__cell) {
  background: transparent !important;
}

:deep(.el-table--striped .el-table__body tr.el-table__row--striped > td.el-table__cell) {
  background: var(--factor-row-striped) !important;
}

:deep(.el-table .el-table-fixed-column--left),
:deep(.el-table .el-table-fixed-column--right) {
  background: var(--factor-surface) !important;
}

:deep(.el-table th.el-table__cell) {
  background: rgba(10, 10, 12, 0.72);
  color: var(--text-secondary);
  font-size: 12px;
  font-weight: 600;
}

:deep(.el-table td.el-table__cell) {
  border-bottom-color: var(--border-subtle);
}

:deep(.el-table__inner-wrapper::before) {
  background-color: var(--factor-border);
}

:deep(.el-button) {
  border-radius: 6px;
}

:deep(.el-button:not(.el-button--primary)) {
  color: var(--text-primary);
  background: linear-gradient(180deg, rgba(255, 255, 255, 0.03), rgba(255, 255, 255, 0)), var(--factor-surface-soft);
  border-color: var(--factor-border);
}

:deep(.el-button:not(.el-button--primary):hover) {
  color: var(--text-bright);
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.08), rgba(56, 189, 248, 0)), rgba(31, 41, 55, 0.92);
  border-color: var(--factor-border-strong);
}

:deep(.el-input__wrapper),
:deep(.el-select__wrapper),
:deep(.el-date-editor .el-input__wrapper) {
  background: var(--factor-surface-strong);
  box-shadow: 0 0 0 1px var(--factor-border) inset;
}

:deep(.el-tag) {
  color: #cbd5e1;
  background: linear-gradient(180deg, rgba(148, 163, 184, 0.14), rgba(148, 163, 184, 0.06));
  border-color: rgba(148, 163, 184, 0.2);
}

:deep(.group-tags .el-tag),
:deep(.dep-tag.el-tag),
:deep(.coverage-warning__tags .el-tag),
:deep(.date-shortcut.el-tag) {
  color: #dbe4f0 !important;
  background-color: rgba(51, 65, 85, 0.38) !important;
  background-image: linear-gradient(180deg, rgba(148, 163, 184, 0.14), rgba(148, 163, 184, 0.06)) !important;
  border-color: rgba(148, 163, 184, 0.24) !important;
}

:deep(.el-tag--success) {
  color: #86efac;
  background: linear-gradient(180deg, rgba(34, 197, 94, 0.16), rgba(34, 197, 94, 0.06));
  border-color: rgba(34, 197, 94, 0.22);
}

:deep(.el-tag--warning) {
  color: #fcd34d;
  background: linear-gradient(180deg, rgba(245, 158, 11, 0.16), rgba(245, 158, 11, 0.06));
  border-color: rgba(245, 158, 11, 0.22);
}

:deep(.el-tag--danger) {
  color: #fca5a5;
  background: linear-gradient(180deg, rgba(239, 68, 68, 0.16), rgba(239, 68, 68, 0.06));
  border-color: rgba(239, 68, 68, 0.22);
}

:deep(.el-tag--info) {
  color: #93c5fd;
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.14), rgba(56, 189, 248, 0.06));
  border-color: rgba(96, 165, 250, 0.22);
}

:deep(.el-alert) {
  border-color: var(--factor-border);
}

:deep(.el-alert--success) {
  background: linear-gradient(180deg, rgba(34, 197, 94, 0.14), rgba(34, 197, 94, 0.05));
}

:deep(.el-alert--info) {
  background: linear-gradient(180deg, rgba(56, 189, 248, 0.14), rgba(56, 189, 248, 0.05));
}

:deep(.el-alert--warning) {
  background: linear-gradient(180deg, rgba(245, 158, 11, 0.14), rgba(245, 158, 11, 0.05));
}

:deep(.el-alert__title),
:deep(.el-alert__description) {
  color: var(--text-primary);
}

:global(.factor-select-dropdown--wide) {
  min-width: min(720px, calc(100vw - 32px)) !important;
  border: 1px solid rgba(96, 165, 250, 0.22) !important;
  border-radius: 8px !important;
  background:
    linear-gradient(180deg, rgba(30, 41, 59, 0.96), rgba(12, 15, 22, 0.98)),
    #0c0f16 !important;
  box-shadow: 0 18px 48px rgba(0, 0, 0, 0.48), 0 0 0 1px rgba(148, 163, 184, 0.08) inset !important;
}

:global(.factor-select-dropdown--wide .el-select-dropdown),
:global(.factor-select-dropdown--wide .el-select-dropdown__wrap),
:global(.factor-select-dropdown--wide .el-select-dropdown__list),
:global(.factor-select-dropdown--wide .el-virtual-list),
:global(.factor-select-dropdown--wide .el-virtual-list__container),
:global(.factor-select-dropdown--wide .el-vl__wrapper),
:global(.factor-select-dropdown--wide .el-vl__window),
:global(.factor-select-dropdown--wide .el-vl__list),
:global(.factor-select-dropdown--wide ul[role="listbox"]) {
  width: 100% !important;
  background: transparent !important;
}

:global(.factor-select-dropdown--wide .el-select-dropdown) {
  width: 100% !important;
}

:global(.factor-select-dropdown--wide .el-vl__window) {
  width: 100% !important;
  scrollbar-color: rgba(96, 165, 250, 0.32) rgba(15, 23, 42, 0.6);
}

:global(.factor-select-dropdown--wide .el-popper__arrow::before) {
  border-color: rgba(96, 165, 250, 0.22) !important;
  background: #151b26 !important;
}

:global(.factor-select-dropdown--wide .el-select-dropdown__item) {
  height: 48px;
  width: 100% !important;
  line-height: normal;
  padding: 6px 12px;
  color: #dbe7f5 !important;
  background: transparent !important;
}

:global(.factor-select-dropdown--wide .el-select-dropdown__item:hover),
:global(.factor-select-dropdown--wide .el-select-dropdown__item.hover) {
  color: #f8fbff !important;
  background: linear-gradient(90deg, rgba(56, 189, 248, 0.16), rgba(99, 102, 241, 0.08)) !important;
}

:global(.factor-select-dropdown--wide .el-select-dropdown__item.is-selected),
:global(.factor-select-dropdown--wide .el-select-dropdown__item.selected) {
  color: #ffffff !important;
  background: linear-gradient(90deg, rgba(14, 165, 233, 0.28), rgba(37, 99, 235, 0.16)) !important;
}

:global(.factor-select-dropdown--wide .el-select-dropdown__item.is-disabled) {
  color: rgba(148, 163, 184, 0.45) !important;
  background: transparent !important;
}

:global(.factor-option) {
  display: flex;
  min-width: 0;
  flex-direction: column;
  justify-content: center;
  gap: 3px;
}

:global(.factor-option__label) {
  overflow: hidden;
  color: #e8f1fb;
  font-size: 13px;
  font-weight: 600;
  text-overflow: ellipsis;
  white-space: nowrap;
}

:global(.factor-option__meta) {
  overflow: hidden;
  color: #9fb2c7;
  font-family: var(--font-data);
  font-size: 11px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

:global(.factor-select-dropdown--wide .el-select-dropdown__item:hover .factor-option__label),
:global(.factor-select-dropdown--wide .el-select-dropdown__item.hover .factor-option__label),
:global(.factor-select-dropdown--wide .el-select-dropdown__item.is-selected .factor-option__label) {
  color: #ffffff;
}

:global(.factor-select-dropdown--wide .el-select-dropdown__item:hover .factor-option__meta),
:global(.factor-select-dropdown--wide .el-select-dropdown__item.hover .factor-option__meta),
:global(.factor-select-dropdown--wide .el-select-dropdown__item.is-selected .factor-option__meta) {
  color: #bfdbfe;
}

:global(.factor-select-dropdown--group) {
  min-width: min(640px, calc(100vw - 32px)) !important;
  border: 1px solid rgba(96, 165, 250, 0.22) !important;
  border-radius: 8px !important;
  background:
    linear-gradient(180deg, rgba(30, 41, 59, 0.96), rgba(12, 15, 22, 0.98)),
    #0c0f16 !important;
  box-shadow: 0 18px 48px rgba(0, 0, 0, 0.48), 0 0 0 1px rgba(148, 163, 184, 0.08) inset !important;
}

:global(.factor-select-dropdown--group .el-select-dropdown),
:global(.factor-select-dropdown--group .el-select-dropdown__wrap),
:global(.factor-select-dropdown--group .el-select-dropdown__list),
:global(.factor-select-dropdown--group .el-virtual-list),
:global(.factor-select-dropdown--group .el-virtual-list__container),
:global(.factor-select-dropdown--group .el-vl__wrapper),
:global(.factor-select-dropdown--group .el-vl__window),
:global(.factor-select-dropdown--group .el-vl__list),
:global(.factor-select-dropdown--group ul[role="listbox"]) {
  width: 100% !important;
  background: transparent !important;
}

:global(.factor-select-dropdown--group .el-select-dropdown__item) {
  height: 48px;
  width: 100% !important;
  line-height: normal;
  padding: 6px 12px;
  color: #dbe7f5 !important;
  background: transparent !important;
}

:global(.factor-select-dropdown--group .el-select-dropdown__item:hover),
:global(.factor-select-dropdown--group .el-select-dropdown__item.hover) {
  color: #f8fbff !important;
  background: linear-gradient(90deg, rgba(56, 189, 248, 0.16), rgba(99, 102, 241, 0.08)) !important;
}

:global(.factor-select-dropdown--group .el-select-dropdown__item.is-selected),
:global(.factor-select-dropdown--group .el-select-dropdown__item.selected) {
  color: #ffffff !important;
  background: linear-gradient(90deg, rgba(14, 165, 233, 0.28), rgba(37, 99, 235, 0.16)) !important;
}

:global(.factor-select-dropdown--group .el-select-dropdown__item:hover .factor-option__label),
:global(.factor-select-dropdown--group .el-select-dropdown__item.hover .factor-option__label),
:global(.factor-select-dropdown--group .el-select-dropdown__item.is-selected .factor-option__label) {
  color: #ffffff;
}

:global(.factor-select-dropdown--group .el-select-dropdown__item:hover .factor-option__meta),
:global(.factor-select-dropdown--group .el-select-dropdown__item.hover .factor-option__meta),
:global(.factor-select-dropdown--group .el-select-dropdown__item.is-selected .factor-option__meta) {
  color: #bfdbfe;
}

@media (max-width: 1100px) {
  .toolbar-header,
  .coverage-warning,
  .group-panel,
  .preview-dual {
    grid-template-columns: 1fr;
  }

  .toolbar-header {
    flex-direction: column;
  }

  .toolbar-meta {
    align-items: flex-start;
  }

  .action-item {
    margin-left: 0;
  }

  .table-panel {
    height: 520px;
  }
}

@media (max-width: 760px) {
  .toolbar {
    padding: 10px;
  }

  .toolbar-meta {
    white-space: normal;
  }

  .control-form {
    grid-template-columns: 1fr;
  }

  .factor-form-item--wide {
    grid-column: 1;
  }

  .date-control__row,
  .date-shortcuts {
    flex-wrap: wrap;
  }

  .preview-dual {
    grid-template-columns: 1fr;
  }

  .table-panel {
    height: 420px;
  }
}
</style>

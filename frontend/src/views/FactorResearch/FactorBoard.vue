<template>
  <div class="factor-board">
    <div class="filter-bar">
      <div class="filter-title">
        <div>
          <span class="panel-kicker">BOARD FILTERS</span>
          <strong>研究样本与组合设置</strong>
        </div>
        <span class="filter-count">{{ activeFilterSummary }}</span>
      </div>
      <div class="filter-row">
        <span class="filter-label">分类:</span>
        <el-select
          v-model="filters.categories"
          multiple
          collapse-tags
          collapse-tags-tooltip
          clearable
          size="small"
          placeholder="全部分类"
          class="filter-select"
        >
          <el-option v-for="cat in categoryOptions" :key="cat.value" :label="cat.label" :value="cat.value" />
        </el-select>

        <span class="filter-label filter-label--spaced">因子组:</span>
        <el-select
          v-model="filters.factor_groups"
          multiple
          collapse-tags
          collapse-tags-tooltip
          clearable
          size="small"
          placeholder="全部因子组"
          class="filter-select filter-select--wide"
        >
          <el-option
            v-for="group in factorGroups"
            :key="group.name"
            :label="group.display_name"
            :value="group.name"
          />
        </el-select>
      </div>

      <div class="filter-row">
        <span class="filter-label">股票池:</span>
        <el-select v-model="filters.stock_pool" size="small" style="width:210px" filterable>
          <el-option-group label="指数股票池">
            <el-option
              v-for="item in poolEnabledIndexes"
              :key="item.symbol"
              :label="`${item.display_name} ${item.symbol}`"
              :value="item.stock_pool_alias || item.symbol"
            />
          </el-option-group>
          <el-option-group v-if="watchlistGroups.length" label="自选股分组">
            <el-option
              v-for="g in watchlistGroups"
              :key="'wl_'+g.id"
              :label="g.name"
              :value="'watchlist_'+g.id"
            />
          </el-option-group>
        </el-select>

        <span class="filter-label filter-label--spaced">回测周期:</span>
        <el-radio-group v-model="filters.period" size="small">
          <el-radio-button value="3m">近3个月</el-radio-button>
          <el-radio-button value="1y">近1年</el-radio-button>
          <el-radio-button value="3y">近3年</el-radio-button>
          <el-radio-button value="10y">近10年</el-radio-button>
        </el-radio-group>
      </div>

      <div class="filter-row">
        <span class="filter-label help-label">
          组合构建
          <el-tooltip placement="top" effect="dark">
            <template #content>
              <div class="tooltip-content">
                纯多头组合：只买入目标分位股票。<br />
                多空组合 I：最高分位做多、最低分位做空。<br />
                多空组合 II：最高分位相对基准增强，最低分位作为对冲腿。
              </div>
            </template>
            <el-icon><QuestionFilled /></el-icon>
          </el-tooltip>
          :
        </span>
        <el-radio-group v-model="filters.portfolio_type" size="small">
          <el-radio-button value="long_only">纯多头组合</el-radio-button>
          <el-radio-button value="long_short_i">多空组合 I</el-radio-button>
          <el-radio-button value="long_short_ii">多空组合 II</el-radio-button>
        </el-radio-group>

        <span class="filter-label filter-label--spaced">手续费:</span>
        <el-radio-group v-model="filters.fee_config" size="small">
          <el-radio-button value="none">无</el-radio-button>
          <el-radio-button value="commission_stamp">3‰佣金+1‰印花税</el-radio-button>
          <el-radio-button value="commission_stamp_slippage">+1‰滑点</el-radio-button>
        </el-radio-group>

        <span class="filter-label filter-label--spaced">过滤涨停:</span>
        <el-switch v-model="filters.filter_limit_up" size="small" />
      </div>
    </div>

    <div class="toolbar">
      <div class="board-title">
        <strong>因子看板</strong>
        <span>展示保存因子的研究表现，因子值缓存负责数据覆盖和预计算。</span>
      </div>
      <div class="toolbar-actions">
        <el-button @click="showBatchDialog = true">批量计算</el-button>
        <el-button type="primary" :icon="Plus" @click="showCreateDialog = true">新建因子</el-button>
      </div>
    </div>

    <el-table :data="rows" stripe v-loading="loading" @sort-change="handleSortChange">
      <el-table-column prop="factor_name" label="因子" min-width="220" sortable="custom" show-overflow-tooltip>
        <template #default="{ row }">
          <div class="factor-name-cell">
            <strong>{{ row.display_name || row.factor_name }}</strong>
            <span>{{ row.factor_name }}</span>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="factor_group_display_name" label="因子组" min-width="150" show-overflow-tooltip>
        <template #default="{ row }">{{ row.factor_group_display_name || '未分组' }}</template>
      </el-table-column>
      <el-table-column prop="category" label="分类" width="130" show-overflow-tooltip />
      <el-table-column prop="source" label="来源" width="130" show-overflow-tooltip />
      <el-table-column label="缓存覆盖" min-width="190">
        <template #default="{ row }">
          <div class="coverage-cell">
            <el-tag :type="coverageTagType(row.coverage_status)" effect="plain" size="small">
              {{ coverageStatusText(row.coverage_status) }}
            </el-tag>
            <span>{{ coverageRangeText(row) }}</span>
          </div>
        </template>
      </el-table-column>
      <el-table-column prop="latest_ic_mean" label="最新IC均值" width="120" sortable="custom" align="right">
        <template #default="{ row }">
          <span :class="(row.latest_ic_mean ?? row.ic_mean) >= 0 ? 'positive' : 'negative'">{{ formatNumber(row.latest_ic_mean ?? row.ic_mean) }}</span>
        </template>
      </el-table-column>
      <el-table-column prop="latest_icir" label="最新ICIR" width="110" sortable="custom" align="right">
        <template #default="{ row }">{{ formatNumber(row.latest_icir ?? row.ir) }}</template>
      </el-table-column>
      <el-table-column prop="latest_long_short_return" label="多空收益" width="120" sortable="custom" align="right">
        <template #default="{ row }">
          <span :class="(row.latest_long_short_return ?? 0) >= 0 ? 'positive' : 'negative'">
            {{ formatPercent(row.latest_long_short_return) }}
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="latest_max_drawdown" label="最大回撤" width="120" sortable="custom" align="right">
        <template #default="{ row }">{{ formatPercent(row.latest_max_drawdown) }}</template>
      </el-table-column>
      <el-table-column prop="latest_turnover" label="换手率" width="110" sortable="custom" align="right">
        <template #default="{ row }">{{ formatPercent(row.latest_turnover) }}</template>
      </el-table-column>
      <el-table-column prop="latest_run_at" label="最近计算" width="130" show-overflow-tooltip>
        <template #default="{ row }">{{ formatDate(row.latest_run_at) }}</template>
      </el-table-column>
      <el-table-column label="操作" width="100" fixed="right">
        <template #default="{ row }">
          <el-button class="detail-action" size="small" type="primary" plain @click="goToDetail(row)">详情</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-pagination
      v-model:current-page="filters.page"
      v-model:page-size="filters.page_size"
      :total="total"
      :page-sizes="[10, 20, 50]"
      layout="total, sizes, prev, pager, next"
      @change="fetchBoard"
      style="margin-top:16px;justify-content:flex-end"
    />

    <el-dialog v-model="showBatchDialog" title="批量计算因子研究" width="720px">
      <el-form label-width="96px" class="batch-form">
        <el-form-item label="因子组">
          <el-select v-model="batchForm.factor_group" placeholder="选择因子组" filterable style="width:100%">
            <el-option
              v-for="group in factorGroups"
              :key="group.name"
              :label="`${group.display_name} (${group.factor_names.length})`"
              :value="group.name"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="股票池">
          <el-select v-model="batchForm.stock_pool_value" filterable style="width:100%">
            <el-option-group label="指数股票池">
              <el-option
                v-for="item in poolEnabledIndexes"
                :key="item.symbol"
                :label="`${item.display_name} ${item.symbol}`"
                :value="item.stock_pool_alias || item.symbol"
              />
            </el-option-group>
            <el-option-group v-if="watchlistGroups.length" label="自选股分组">
              <el-option
                v-for="g in watchlistGroups"
                :key="'batch_wl_'+g.id"
                :label="g.name"
                :value="'watchlist_'+g.id"
              />
            </el-option-group>
          </el-select>
        </el-form-item>
        <el-form-item label="日期范围">
          <div class="date-range">
            <el-date-picker v-model="batchForm.start_date" value-format="YYYY-MM-DD" type="date" />
            <span>至</span>
            <el-date-picker v-model="batchForm.end_date" value-format="YYYY-MM-DD" type="date" />
          </div>
        </el-form-item>
        <el-form-item label="组合构建">
          <el-radio-group v-model="batchForm.portfolio_type">
            <el-radio-button value="long_only">纯多头</el-radio-button>
            <el-radio-button value="long_short_i">多空 I</el-radio-button>
            <el-radio-button value="long_short_ii">多空 II</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="分组数">
          <el-input-number v-model="batchForm.group_count" :min="2" :max="20" />
        </el-form-item>
      </el-form>
      <div v-if="batchResult.length" class="batch-result">
        <div v-for="item in batchResult" :key="item.factor_name" class="batch-result-row">
          <span>{{ item.factor_name }}</span>
          <el-tag :type="item.status === 'success' ? 'success' : 'danger'" size="small">{{ item.status }}</el-tag>
          <span>{{ item.run_id || item.error_message }}</span>
        </div>
      </div>
      <template #footer>
        <el-button @click="showBatchDialog = false">关闭</el-button>
        <el-button type="primary" :loading="batchLoading" @click="submitBatchRun">开始计算</el-button>
      </template>
    </el-dialog>

    <FactorCreateDialog v-model:visible="showCreateDialog" :watchlist-groups="watchlistGroups" @created="fetchBoard" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, reactive, watch, onMounted } from 'vue'
import { Plus, QuestionFilled } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { useRouter } from 'vue-router'
import { evaluationApi } from '@/api/factorResearch'
import { indexCatalogApi, watchlistApi } from '@/api/data'
import { factorValueApi, type FactorValueGroup } from '@/api/factorValues'
import { factorResearchRunApi } from '@/api/factorResearchRuns'
import type { BoardRow, BoardQuery } from '@/types/factor'
import type { IndexCatalogItem, WatchlistGroup } from '@/api/data'
import FactorCreateDialog from './FactorCreateDialog.vue'

const router = useRouter()

const categoryBuckets = [
  { value: 'custom', label: '自定义因子库', categories: ['custom'] },
  { value: 'small_cap_core', label: '小市值核心', categories: ['valuation', 'status', 'technical', 'liquidity'] },
  { value: 'ta_lib', label: 'TA-Lib', categories: ['ta_trend', 'ta_momentum', 'ta_volatility', 'ta_volume', 'ta_price', 'ta_regression', 'ta_pattern'] },
  { value: 'alpha101', label: 'Alpha101', categories: ['alpha101'] },
  {
    value: 'research',
    label: '海外研究因子',
    categories: ['research_quality', 'research_investment', 'research_risk', 'research_momentum', 'research_reversal', 'research_liquidity'],
  },
]

const filters = reactive<BoardQuery>({
  categories: [],
  factor_groups: [],
  stock_pool: 'zz500',
  period: '3y',
  portfolio_type: 'long_only',
  fee_config: 'none',
  filter_limit_up: true,
  sort_by: 'ic_mean',
  sort_order: 'desc',
  page: 1,
  page_size: 20,
})

const rows = ref<BoardRow[]>([])
const total = ref(0)
const loading = ref(false)
const showCreateDialog = ref(false)
const showBatchDialog = ref(false)
const batchLoading = ref(false)
const watchlistGroups = ref<WatchlistGroup[]>([])
const indexCatalog = ref<IndexCatalogItem[]>([])
const factorGroups = ref<FactorValueGroup[]>([])
const batchResult = ref<Array<{ factor_name: string; status: string; run_id?: string | null; error_message?: string | null }>>([])
const poolEnabledIndexes = computed(() => indexCatalog.value.filter(item => item.pool_enabled))
const categoryOptions = computed(() => categoryBuckets)
const queryCategories = computed(() => {
  const selected = new Set(filters.categories || [])
  if (!selected.size) return undefined
  return categoryBuckets
    .filter(item => selected.has(item.value))
    .flatMap(item => item.categories)
})
const activeFilterSummary = computed(() => {
  const categoryCount = filters.categories?.length || 0
  const groupCount = filters.factor_groups?.length || 0
  return `${categoryCount || '全部'} 分类 / ${groupCount || '全部'} 组`
})
const todayText = new Date().toISOString().slice(0, 10)
const batchForm = reactive({
  factor_group: '',
  stock_pool_value: 'zz500',
  start_date: '2023-01-01',
  end_date: todayText,
  portfolio_type: 'long_only' as 'long_only' | 'long_short_i' | 'long_short_ii',
  rebalance_period: 'monthly' as 'daily' | 'weekly' | 'monthly',
  fee_rate: 0.001,
  slippage: 0.001,
  filter_limit_up: true,
  filter_limit_down: true,
  group_count: 5,
  direction: 'desc' as 'asc' | 'desc',
  industry_neutralization: false,
  standardize: false,
})

function resolveDateRangeFromPeriod(period: string) {
  const end = new Date()
  const start = new Date(end)
  if (period === '3m') start.setMonth(start.getMonth() - 3)
  else if (period === '1y') start.setFullYear(start.getFullYear() - 1)
  else if (period === '3y') start.setFullYear(start.getFullYear() - 3)
  else if (period === '10y') start.setFullYear(start.getFullYear() - 10)
  return {
    start_date: start.toISOString().slice(0, 10),
    end_date: end.toISOString().slice(0, 10),
  }
}

async function loadWatchlistGroups() {
  try {
    watchlistGroups.value = await watchlistApi.getGroups()
  } catch {
    watchlistGroups.value = []
  }
}

async function loadIndexCatalog() {
  try {
    indexCatalog.value = await indexCatalogApi.list()
    if (!poolEnabledIndexes.value.some(item => (item.stock_pool_alias || item.symbol) === filters.stock_pool)) {
      filters.stock_pool = poolEnabledIndexes.value[0]?.stock_pool_alias || poolEnabledIndexes.value[0]?.symbol || 'zz500'
    }
  } catch {
    indexCatalog.value = []
  }
}

async function loadFactorCatalog() {
  try {
    const groups = await factorValueApi.groups()
    factorGroups.value = groups
  } catch {
    factorGroups.value = []
  }
}

async function fetchBoard() {
  loading.value = true
  try {
    const query: BoardQuery = {
      ...filters,
      categories: queryCategories.value,
      factor_groups: filters.factor_groups?.length ? filters.factor_groups : undefined,
    }
    const res = await evaluationApi.board(query)
    rows.value = res.rows
    total.value = res.total
  } catch {
    rows.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

function handleSortChange({ prop, order }: { prop: string; order: string | null }) {
  if (prop) {
    filters.sort_by = prop
    filters.sort_order = order === 'ascending' ? 'asc' : 'desc'
  }
  fetchBoard()
}

function goToDetail(row: BoardRow) {
  const { start_date, end_date } = resolveDateRangeFromPeriod(filters.period || '3y')
  router.push({
    path: `/factor/detail/${row.factor_name}`,
    query: {
      stock_pool: filters.stock_pool,
      period: filters.period,
      start_date,
      end_date,
      portfolio_type: filters.portfolio_type,
      fee_config: filters.fee_config,
      filter_limit_up: String(filters.filter_limit_up),
    },
  })
}

function formatNumber(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return Number(value).toFixed(4)
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return `${(Number(value) * 100).toFixed(2)}%`
}

function formatDate(value?: string | null) {
  if (!value) return '未计算'
  return value.slice(0, 10)
}

function coverageStatusText(status: BoardRow['coverage_status']) {
  if (status === 'covered') return '已覆盖'
  if (status === 'partial') return '部分覆盖'
  if (status === 'empty') return '无缓存'
  return '未知'
}

function coverageTagType(status: BoardRow['coverage_status']) {
  if (status === 'covered') return 'success'
  if (status === 'partial') return 'warning'
  if (status === 'empty') return 'info'
  return 'danger'
}

function coverageRangeText(row: BoardRow) {
  const rowsText = `${(row.coverage_total_rows || 0).toLocaleString()} 行`
  if (!row.coverage_min_date || !row.coverage_max_date) return rowsText
  return `${row.coverage_min_date} - ${row.coverage_max_date} / ${rowsText}`
}

async function submitBatchRun() {
  const group = factorGroups.value.find(item => item.name === batchForm.factor_group)
  if (!group) {
    ElMessage.warning('请选择因子组')
    return
  }
  batchLoading.value = true
  batchResult.value = []
  try {
    const result = await factorResearchRunApi.batch({
      factor_names: group.factor_names,
      stock_pool_value: batchForm.stock_pool_value,
      start_date: batchForm.start_date,
      end_date: batchForm.end_date,
      portfolio_type: batchForm.portfolio_type,
      rebalance_period: batchForm.rebalance_period,
      fee_rate: batchForm.fee_rate,
      slippage: batchForm.slippage,
      filter_limit_up: batchForm.filter_limit_up,
      filter_limit_down: batchForm.filter_limit_down,
      group_count: batchForm.group_count,
      direction: batchForm.direction,
      industry_neutralization: batchForm.industry_neutralization,
      standardize: batchForm.standardize,
      force: false,
    })
    batchResult.value = result.items
    ElMessage.success('批量计算已完成')
    fetchBoard()
  } finally {
    batchLoading.value = false
  }
}

watch(
  () => [
    filters.stock_pool,
    filters.period,
    filters.portfolio_type,
    filters.fee_config,
    filters.filter_limit_up,
    JSON.stringify(filters.categories || []),
    JSON.stringify(filters.factor_groups || []),
  ],
  () => { filters.page = 1; fetchBoard() }
)

onMounted(() => {
  loadWatchlistGroups()
  loadIndexCatalog()
  loadFactorCatalog()
  fetchBoard()
})
</script>

<style scoped>
.factor-board {
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-height: 0;
}
.filter-bar {
  background:
    linear-gradient(135deg, rgba(251, 191, 36, 0.055), transparent 30%),
    linear-gradient(180deg, rgba(255, 255, 255, 0.035), rgba(255, 255, 255, 0.01)),
    var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: 8px;
  padding: 13px 14px;
  box-shadow: var(--shadow-sm);
}
.filter-title {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}
.filter-title > div {
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.panel-kicker {
  font-family: var(--font-data);
  font-size: 10px;
  color: var(--accent-warning);
}
.filter-title strong {
  color: var(--text-bright);
  font-size: 15px;
}
.filter-count {
  padding: 4px 8px;
  border: 1px solid rgba(251, 191, 36, 0.22);
  border-radius: 6px;
  background: rgba(251, 191, 36, 0.055);
  color: var(--accent-warning);
  font-family: var(--font-data);
  font-size: 11px;
  white-space: nowrap;
}
.filter-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 9px;
  margin-bottom: 10px;
  padding: 8px 9px;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  background: rgba(8, 8, 10, 0.22);
}
.filter-row:last-child { margin-bottom: 0; }
.filter-label {
  font-size: 12px;
  color: var(--text-secondary);
  white-space: nowrap;
}
.filter-label--spaced {
  margin-left: 16px;
}
.help-label,
.toolbar-actions,
.date-range,
.batch-result-row {
  display: flex;
  align-items: center;
  gap: 6px;
}
.tooltip-content {
  line-height: 1.7;
}
.filter-select {
  width: 190px;
}
.filter-select--wide {
  width: 230px;
}
.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 11px 12px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background:
    linear-gradient(90deg, rgba(56, 189, 248, 0.055), transparent 24%),
    var(--bg-elevated);
}
.board-title { display: flex; flex-direction: column; gap: 3px; font-size: 12px; color: var(--text-secondary); }
.board-title strong { font-size: 15px; color: var(--text-bright); }
.batch-form {
  padding-right: 12px;
}
.batch-result {
  display: grid;
  gap: 7px;
  margin-top: 12px;
  padding: 10px;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: rgba(8, 8, 10, 0.24);
}
.batch-result-row {
  justify-content: space-between;
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 12px;
}
.factor-name-cell,
.coverage-cell {
  display: grid;
  gap: 3px;
  min-width: 0;
}
.factor-name-cell strong {
  overflow: hidden;
  color: var(--text-bright);
  font-size: 13px;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.factor-name-cell span,
.coverage-cell span {
  overflow: hidden;
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 12px;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.coverage-cell {
  align-items: start;
}
.positive { color: #d93026; }
.negative { color: #137333; }
.detail-action {
  color: #eaf6ff;
  border-color: rgba(56, 189, 248, 0.55);
  background: rgba(56, 189, 248, 0.22);
}
.detail-action:hover {
  color: #ffffff;
  border-color: rgba(56, 189, 248, 0.86);
  background: rgba(56, 189, 248, 0.34);
}

:deep(.el-table) {
  border: 1px solid var(--border-default);
  border-radius: 8px;
  overflow: hidden;
  background: var(--bg-elevated);
  box-shadow: var(--shadow-sm);
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

:deep(.el-pagination) {
  margin-top: 2px !important;
}

:deep(.el-checkbox__label),
:deep(.el-radio-button__inner) {
  font-size: 12px;
}

:deep(.el-radio-button__inner),
:deep(.el-button) {
  border-radius: 6px;
}

:deep(.el-select__wrapper) {
  background: rgba(8, 8, 10, 0.42);
  box-shadow: 0 0 0 1px var(--border-subtle) inset;
}

@media (max-width: 1100px) {
  .filter-label--spaced {
    margin-left: 0;
  }

  .toolbar {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>

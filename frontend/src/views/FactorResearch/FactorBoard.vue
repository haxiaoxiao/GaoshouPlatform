<template>
  <div class="factor-board">
    <div class="filter-bar">
      <div class="filter-row">
        <span class="filter-label">分类:</span>
        <el-checkbox-group v-model="filters.categories" size="small">
          <el-checkbox v-for="cat in categories" :key="cat" :label="cat" :value="cat" />
        </el-checkbox-group>
      </div>

      <div class="filter-row">
        <span class="filter-label">股票池:</span>
        <el-select v-model="filters.stock_pool" size="small" style="width:160px">
          <el-option-group label="指数股票池">
            <el-option label="沪深300" value="hs300" />
            <el-option label="中证500" value="zz500" />
            <el-option label="中证800" value="zz800" />
            <el-option label="中证1000" value="zz1000" />
            <el-option label="中证全指" value="zz_quanzhi" />
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

        <span class="filter-label" style="margin-left:24px">回测周期:</span>
        <el-radio-group v-model="filters.period" size="small">
          <el-radio-button value="3m">近3个月</el-radio-button>
          <el-radio-button value="1y">近1年</el-radio-button>
          <el-radio-button value="3y">近3年</el-radio-button>
          <el-radio-button value="10y">近10年</el-radio-button>
        </el-radio-group>
      </div>

      <div class="filter-row">
        <span class="filter-label">组合构建:</span>
        <el-radio-group v-model="filters.portfolio_type" size="small">
          <el-radio-button value="long_only">纯多头组合</el-radio-button>
          <el-radio-button value="long_short_i">多空组合 I</el-radio-button>
          <el-radio-button value="long_short_ii">多空组合 II</el-radio-button>
        </el-radio-group>

        <span class="filter-label" style="margin-left:24px">手续费:</span>
        <el-radio-group v-model="filters.fee_config" size="small">
          <el-radio-button value="none">无</el-radio-button>
          <el-radio-button value="commission_stamp">3‰佣金+1‰印花税</el-radio-button>
          <el-radio-button value="commission_stamp_slippage">+1‰滑点</el-radio-button>
        </el-radio-group>

        <span class="filter-label" style="margin-left:24px">过滤涨停:</span>
        <el-switch v-model="filters.filter_limit_up" size="small" />
      </div>
    </div>

    <div class="toolbar">
      <el-button type="primary" @click="showCreateDialog = true">+ 新建因子</el-button>
    </div>

    <el-table :data="rows" stripe v-loading="loading" @sort-change="handleSortChange">
      <el-table-column prop="factor_name" label="因子名称" min-width="180" sortable="custom" />
      <el-table-column prop="category" label="分类" width="140" />
      <el-table-column prop="min_quantile_excess_return" label="最小分位超额年化收益" width="180" sortable="custom" align="right">
        <template #default="{ row }">
          <span :class="row.min_quantile_excess_return >= 0 ? 'positive' : 'negative'">
            {{ (row.min_quantile_excess_return * 100).toFixed(2) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="max_quantile_excess_return" label="最大分位超额年化收益" width="180" sortable="custom" align="right">
        <template #default="{ row }">
          <span :class="row.max_quantile_excess_return >= 0 ? 'positive' : 'negative'">
            {{ (row.max_quantile_excess_return * 100).toFixed(2) }}%
          </span>
        </template>
      </el-table-column>
      <el-table-column prop="min_quantile_turnover" label="最小分位换手率" width="140" sortable="custom" align="right">
        <template #default="{ row }">
          {{ (row.min_quantile_turnover * 100).toFixed(2) }}%
        </template>
      </el-table-column>
      <el-table-column prop="max_quantile_turnover" label="最大分位换手率" width="140" sortable="custom" align="right">
        <template #default="{ row }">
          {{ (row.max_quantile_turnover * 100).toFixed(2) }}%
        </template>
      </el-table-column>
      <el-table-column prop="ic_mean" label="IC均值" width="100" sortable="custom" align="right" />
      <el-table-column prop="ir" label="IR值" width="100" sortable="custom" align="right" />
      <el-table-column label="操作" width="160" fixed="right">
        <template #default="{ row }">
          <el-button size="small" @click="goToAnalysis(row)">分析</el-button>
          <el-button size="small" @click="goToBacktest(row)">回测</el-button>
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

    <FactorCreateDialog v-model:visible="showCreateDialog" :watchlist-groups="watchlistGroups" @created="fetchBoard" />
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, watch, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { evaluationApi } from '@/api/v2'
import { watchlistApi } from '@/api/data'
import type { BoardRow, BoardQuery } from '@/types/factor'
import type { WatchlistGroup } from '@/api/data'
import FactorCreateDialog from './FactorCreateDialog.vue'

const router = useRouter()

const categories = [
  '基础科目及衍生类因子', '情绪类因子', '动量类因子', '质量类因子',
  '成长类因子', '风险因子-新风格因子', '每股指标因子', '风险类因子',
  '风险因子-风格因子', '技术指标因子',
]

const filters = reactive<BoardQuery>({
  categories: [...categories],
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
const watchlistGroups = ref<WatchlistGroup[]>([])

async function loadWatchlistGroups() {
  try {
    watchlistGroups.value = await watchlistApi.getGroups()
  } catch {
    watchlistGroups.value = []
  }
}

async function fetchBoard() {
  loading.value = true
  try {
    const res = await evaluationApi.board({ ...filters })
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

function goToAnalysis(row: BoardRow) {
  router.push(`/factor/analysis-new/${row.factor_name}`)
}

function goToBacktest(row: BoardRow) {
  router.push(`/backtest/factor/${row.factor_name}`)
}

watch(
  () => [filters.stock_pool, filters.period, filters.portfolio_type, filters.fee_config, filters.filter_limit_up],
  () => { filters.page = 1; fetchBoard() }
)

onMounted(() => {
  loadWatchlistGroups()
  fetchBoard()
})
</script>

<style scoped>
.factor-board { padding: 16px; }
.filter-bar {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  padding: 12px 16px;
  margin-bottom: 12px;
}
.filter-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.filter-row:last-child { margin-bottom: 0; }
.filter-label {
  font-size: 12px;
  color: var(--text-secondary);
  white-space: nowrap;
}
.toolbar { margin-bottom: 12px; }
.positive { color: #d93026; }
.negative { color: #137333; }
</style>

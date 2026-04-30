<template>
  <div class="stock-list-container">
    <!-- Search & Filter Bar -->
    <div class="filter-bar">
      <div class="filter-bar__left">
        <div class="search-box">
          <svg class="search-box__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <circle cx="11" cy="11" r="8"/>
            <path d="m21 21-4.35-4.35"/>
          </svg>
          <input
            v-model="searchText"
            type="text"
            placeholder="搜索股票代码或名称..."
            class="search-box__input"
            @keyup.enter="handleSearch"
          />
        </div>

        <div class="filter-chips">
          <button
            v-for="industry in industryOptions"
            :key="industry.value"
            class="chip"
            :class="{ 'chip--active': selectedIndustry === industry.value }"
            @click="toggleIndustry(industry.value)"
          >
            {{ industry.label }}
          </button>
        </div>
      </div>

      <div class="filter-bar__right">
        <el-select
          v-model="selectedMarket"
          placeholder="市场"
          clearable
          class="filter-select"
          @change="handleFilterChange"
        >
          <el-option label="全部市场" value="" />
          <el-option label="沪市" value="SH" />
          <el-option label="深市" value="SZ" />
          <el-option label="北交所" value="BJ" />
        </el-select>

        <el-button type="primary" @click="handleSearch">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px; margin-right: 6px;">
            <circle cx="11" cy="11" r="8"/>
            <path d="m21 21-4.35-4.35"/>
          </svg>
          搜索
        </el-button>
      </div>
    </div>

    <!-- Results Summary -->
    <div class="results-summary">
      <span class="results-count">
        找到 <strong>{{ total }}</strong> 只股票
      </span>
      <div class="view-toggles">
        <button class="view-toggle view-toggle--active" title="列表视图">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/>
          </svg>
        </button>
        <button class="view-toggle" title="网格视图">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <rect x="3" y="3" width="7" height="7"/>
            <rect x="14" y="3" width="7" height="7"/>
            <rect x="14" y="14" width="7" height="7"/>
            <rect x="3" y="14" width="7" height="7"/>
          </svg>
        </button>
      </div>
    </div>

    <!-- Stock Table -->
    <div class="table-container" v-loading="loading">
      <table class="data-table">
        <thead>
          <tr>
            <th>代码</th>
            <th>名称</th>
            <th>市场</th>
            <th>行业</th>
            <th>上市日期</th>
            <th>总市值</th>
            <th>流通市值</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="stock in stockList"
            :key="stock.symbol"
            class="data-row"
            @click="handleRowClick(stock)"
          >
            <td>
              <span class="stock-symbol">{{ stock.symbol }}</span>
            </td>
            <td>
              <div class="stock-name-cell">
                <span class="stock-name" :class="{ 'stock-name--st': stock.is_st }">
                  {{ stock.name }}
                </span>
                <span v-if="stock.is_st" class="st-badge">ST</span>
              </div>
            </td>
            <td>
              <span class="market-tag" :class="`market-tag--${stock.exchange?.toLowerCase()}`">
                {{ stock.exchange || '-' }}
              </span>
            </td>
            <td>
              <span v-if="stock.industry" class="industry-tag">{{ stock.industry }}</span>
              <span v-else class="text-muted">-</span>
            </td>
            <td>
              <span class="text-data">{{ stock.list_date || '-' }}</span>
            </td>
            <td class="text-right">
              <span v-if="stock.total_mv" class="text-data">{{ formatAmount(stock.total_mv) }}</span>
              <span v-else class="text-muted">-</span>
            </td>
            <td class="text-right">
              <span v-if="stock.circ_mv" class="text-data">{{ formatAmount(stock.circ_mv) }}</span>
              <span v-else class="text-muted">-</span>
            </td>
            <td>
              <button class="action-btn action-btn--primary" @click.stop="handleViewKline(stock)">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M3 3v18h18"/>
                  <path d="m19 9-5 5-4-4-3 3"/>
                </svg>
                K线
              </button>
            </td>
          </tr>
          <tr v-if="!loading && stockList.length === 0">
            <td colspan="8" class="empty-cell">
              <div class="empty-state">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                  <circle cx="11" cy="11" r="8"/>
                  <path d="m21 21-4.35-4.35"/>
                  <path d="M8 11h6"/>
                </svg>
                <p>未找到匹配的股票</p>
                <span>请尝试其他搜索条件</span>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Pagination -->
    <div class="pagination-bar" v-if="total > 0">
      <div class="pagination-info">
        显示 {{ (currentPage - 1) * pageSize + 1 }} - {{ Math.min(currentPage * pageSize, total) }} 条，共 {{ total }} 条
      </div>
      <div class="pagination-controls">
        <button
          class="page-btn"
          :disabled="currentPage === 1"
          @click="handlePageChange(currentPage - 1)"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M15 18l-6-6 6-6"/>
          </svg>
        </button>
        <template v-for="page in visiblePages" :key="page">
          <button
            v-if="page === '...'"
            class="page-btn page-btn--ellipsis"
            disabled
          >...</button>
          <button
            v-else
            class="page-btn"
            :class="{ 'page-btn--active': currentPage === page }"
            @click="handlePageChange(page as number)"
          >
            {{ page }}
          </button>
        </template>
        <button
          class="page-btn"
          :disabled="currentPage >= totalPages"
          @click="handlePageChange(currentPage + 1)"
        >
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M9 18l6-6-6-6"/>
          </svg>
        </button>
      </div>
    </div>

    <!-- K-line Dialog -->
    <el-dialog
      v-model="klineDialogVisible"
      :title="`${currentStock?.symbol} ${currentStock?.name}`"
      width="90%"
      top="5vh"
      destroy-on-close
      class="kline-dialog"
    >
      <div class="kline-dialog-content">
        <div class="kline-toolbar">
          <el-date-picker
            v-model="klineDateRange"
            type="daterange"
            range-separator="至"
            start-placeholder="开始日期"
            end-placeholder="结束日期"
            format="YYYY-MM-DD"
            value-format="YYYY-MM-DD"
            @change="loadKlineData"
          />
          <div class="quick-ranges">
            <button
              v-for="range in quickRanges"
              :key="range.days"
              class="quick-range-btn"
              @click="setDateRange(range.days)"
            >
              {{ range.label }}
            </button>
          </div>
        </div>
        <div v-loading="klineLoading" class="chart-wrapper">
          <KlineChart :data="klineData" />
          <el-empty v-if="!klineLoading && klineData.length === 0" description="暂无K线数据" />
        </div>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import KlineChart from './KlineChart.vue'
import request from '@/api/request'
import { toDisplayFormat, type KlineDataDisplay } from '@/api/kline'

interface StockInfo {
  symbol: string
  name: string
  exchange: string
  industry: string
  list_date: string
  is_st: number
  total_mv: number
  circ_mv: number
}

interface KlineDataRaw {
  symbol: string
  trade_date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  amount: number
}

interface Industry {
  name: string
  count: number
}

// State
const loading = ref(false)
const stockList = ref<StockInfo[]>([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(50)

// Filters
const searchText = ref('')
const selectedIndustry = ref<string>('')
const selectedMarket = ref<string>('')
const industries = ref<Industry[]>([])

// Industry options for chips
const industryOptions = computed(() => {
  const top = industries.value.slice(0, 6).map(i => ({ label: i.name, value: i.name }))
  return [{ label: '全部', value: '' }, ...top]
})

// K-line dialog
const klineDialogVisible = ref(false)
const klineLoading = ref(false)
const currentStock = ref<StockInfo | null>(null)
const klineData = ref<KlineDataDisplay[]>([])
const klineDateRange = ref<string[]>([])

const quickRanges = [
  { label: '1月', days: 30 },
  { label: '3月', days: 90 },
  { label: '半年', days: 180 },
  { label: '1年', days: 365 },
  { label: '2年', days: 730 }
]

// Pagination
const totalPages = computed(() => Math.ceil(total.value / pageSize.value))

const visiblePages = computed(() => {
  const pages: (number | string)[] = []
  const total = totalPages.value
  const current = currentPage.value

  if (total <= 7) {
    for (let i = 1; i <= total; i++) pages.push(i)
  } else {
    if (current <= 3) {
      pages.push(1, 2, 3, 4, '...', total)
    } else if (current >= total - 2) {
      pages.push(1, '...', total - 3, total - 2, total - 1, total)
    } else {
      pages.push(1, '...', current - 1, current, current + 1, '...', total)
    }
  }
  return pages
})

// Methods
const loadStocks = async () => {
  loading.value = true
  try {
    const params: Record<string, unknown> = {
      page: currentPage.value,
      page_size: pageSize.value,
    }
    if (searchText.value) params.search = searchText.value
    if (selectedIndustry.value) params.industry = selectedIndustry.value
    if (selectedMarket.value) params.exchange = selectedMarket.value

    const response = await request.get<{ items: StockInfo[]; total: number }>('/data/stocks', { params })
    stockList.value = response.items || []
    total.value = response.total || 0
  } catch {
    ElMessage.error('加载股票列表失败')
    stockList.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const loadIndustries = async () => {
  try {
    const response = await request.get<{ industries: Industry[] }>('/data/industries')
    industries.value = response.industries || []
  } catch {
    industries.value = []
  }
}

const handleSearch = () => {
  currentPage.value = 1
  loadStocks()
}

const handleFilterChange = () => {
  currentPage.value = 1
  loadStocks()
}

const toggleIndustry = (value: string) => {
  selectedIndustry.value = value
  handleFilterChange()
}

const handlePageChange = (page: number) => {
  currentPage.value = page
  loadStocks()
}

const handleRowClick = (stock: StockInfo) => {
  handleViewKline(stock)
}

const handleViewKline = (stock: StockInfo) => {
  currentStock.value = stock
  klineDialogVisible.value = true
  setDateRange(365)
}

const setDateRange = (days: number) => {
  const end = new Date()
  const start = new Date()
  start.setDate(start.getDate() - days)
  klineDateRange.value = [
    start.toISOString().slice(0, 10),
    end.toISOString().slice(0, 10)
  ]
  // 不在这里清空数据，让图表保持显示直到新数据到达
  loadKlineData()
}

const loadKlineData = async () => {
  if (!currentStock.value || klineDateRange.value.length !== 2) return
  klineLoading.value = true
  try {
    const response = await request.get<{ items: KlineDataRaw[] }>('/data/klines', {
      params: {
        symbol: currentStock.value.symbol,
        start_date: klineDateRange.value[0],
        end_date: klineDateRange.value[1],
        page_size: 1000
      }
    })
    // 只有在成功获取数据后才更新
    const newData = toDisplayFormat(response.items || [])
    klineData.value = newData
  } catch {
    ElMessage.error('加载K线数据失败')
    // 失败时不清空数据，保持当前显示
  } finally {
    klineLoading.value = false
  }
}

const formatAmount = (amount: number): string => {
  if (!amount) return '-'
  if (amount >= 100000000) return (amount / 100000000).toFixed(2) + '亿'
  if (amount >= 10000) return (amount / 10000).toFixed(2) + '万'
  return amount.toLocaleString()
}

onMounted(() => {
  loadStocks()
  loadIndustries()
})
</script>

<style scoped>
/* ═══════════════════════════════════════════════════════════════
   STOCK LIST CONTAINER
   ═══════════════════════════════════════════════════════════════ */

.stock-list-container {
  display: flex;
  flex-direction: column;
  gap: var(--space-4);
  height: 100%;
}

/* ═══════════════════════════════════════════════════════════════
   FILTER BAR
   ═══════════════════════════════════════════════════════════════ */

.filter-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: var(--space-4);
  flex-wrap: wrap;
}

.filter-bar__left {
  display: flex;
  align-items: center;
  gap: var(--space-4);
  flex: 1;
}

.filter-bar__right {
  display: flex;
  align-items: center;
  gap: var(--space-3);
}

/* Search Box */
.search-box {
  position: relative;
  width: 320px;
}

.search-box__icon {
  position: absolute;
  left: 12px;
  top: 50%;
  transform: translateY(-50%);
  width: 16px;
  height: 16px;
  color: var(--text-muted);
  pointer-events: none;
}

.search-box__input {
  width: 100%;
  height: 40px;
  padding: 0 var(--space-4) 0 40px;
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-md);
  color: var(--text-primary);
  font-size: var(--text-sm);
  font-family: var(--font-ui);
  transition: all var(--duration-normal);
}

.search-box__input::placeholder {
  color: var(--text-muted);
}

.search-box__input:focus {
  outline: none;
  border-color: var(--accent-primary);
  box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.1);
}

/* Filter Chips */
.filter-chips {
  display: flex;
  gap: var(--space-2);
}

.chip {
  padding: var(--space-2) var(--space-3);
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-full);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  font-weight: 500;
  cursor: pointer;
  transition: all var(--duration-normal);
}

.chip:hover {
  border-color: var(--accent-primary);
  color: var(--text-primary);
}

.chip--active {
  background: var(--accent-primary);
  border-color: var(--accent-primary);
  color: var(--bg-void);
}

/* Filter Select */
.filter-select {
  width: 120px;
}

/* ═══════════════════════════════════════════════════════════════
   RESULTS SUMMARY
   ═══════════════════════════════════════════════════════════════ */

.results-summary {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.results-count {
  font-size: var(--text-sm);
  color: var(--text-secondary);
}

.results-count strong {
  color: var(--accent-primary);
  font-weight: 600;
}

.view-toggles {
  display: flex;
  gap: var(--space-1);
}

.view-toggle {
  width: 32px;
  height: 32px;
  background: transparent;
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-sm);
  color: var(--text-muted);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all var(--duration-normal);
}

.view-toggle:hover {
  background: var(--bg-hover);
  color: var(--text-primary);
}

.view-toggle--active {
  background: var(--accent-primary);
  border-color: var(--accent-primary);
  color: var(--bg-void);
}

.view-toggle svg {
  width: 16px;
  height: 16px;
}

/* ═══════════════════════════════════════════════════════════════
   DATA TABLE
   ═══════════════════════════════════════════════════════════════ */

.table-container {
  flex: 1;
  overflow: auto;
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
  border-radius: var(--radius-lg);
}

.data-table {
  width: 100%;
  border-collapse: collapse;
}

.data-table th {
  padding: var(--space-3) var(--space-4);
  text-align: left;
  font-size: var(--text-xs);
  font-weight: 600;
  letter-spacing: var(--tracking-wider);
  text-transform: uppercase;
  color: var(--text-secondary);
  background: var(--bg-elevated);
  border-bottom: 1px solid var(--border-subtle);
  position: sticky;
  top: 0;
  z-index: 1;
}

.data-table td {
  padding: var(--space-3) var(--space-4);
  border-bottom: 1px solid var(--border-subtle);
}

.data-row {
  cursor: pointer;
  transition: background var(--duration-fast);
}

.data-row:hover {
  background: var(--bg-hover);
}

.data-row:last-child td {
  border-bottom: none;
}

/* Stock Symbol */
.stock-symbol {
  font-family: var(--font-display);
  font-weight: 600;
  color: var(--accent-primary);
  font-size: var(--text-sm);
}

/* Stock Name Cell */
.stock-name-cell {
  display: flex;
  align-items: center;
  gap: var(--space-2);
}

.stock-name {
  font-weight: 500;
}

.stock-name--st {
  color: var(--color-bear);
}

.st-badge {
  padding: 2px 6px;
  background: rgba(239, 68, 68, 0.15);
  border-radius: var(--radius-sm);
  font-size: 10px;
  font-weight: 700;
  color: var(--color-bear);
}

/* Market Tag */
.market-tag {
  padding: 2px 8px;
  border-radius: var(--radius-sm);
  font-size: var(--text-xs);
  font-weight: 600;
}

.market-tag--sh {
  background: rgba(239, 68, 68, 0.15);
  color: var(--color-bear);
}

.market-tag--sz {
  background: rgba(34, 197, 94, 0.15);
  color: var(--color-bull);
}

.market-tag--bj {
  background: rgba(251, 191, 36, 0.15);
  color: var(--accent-warning);
}

/* Industry Tag */
.industry-tag {
  padding: 2px 8px;
  background: var(--bg-hover);
  border-radius: var(--radius-sm);
  font-size: var(--text-xs);
  color: var(--text-secondary);
}

/* Action Button */
.action-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--space-1);
  padding: var(--space-1) var(--space-2);
  border-radius: var(--radius-sm);
  font-size: var(--text-xs);
  font-weight: 500;
  cursor: pointer;
  transition: all var(--duration-normal);
}

.action-btn svg {
  width: 14px;
  height: 14px;
}

.action-btn--primary {
  background: transparent;
  border: 1px solid var(--accent-primary);
  color: var(--accent-primary);
}

.action-btn--primary:hover {
  background: var(--accent-primary);
  color: var(--bg-void);
}

/* Empty State */
.empty-cell {
  padding: var(--space-10) !important;
}

.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--space-2);
  color: var(--text-muted);
}

.empty-state svg {
  width: 48px;
  height: 48px;
  opacity: 0.5;
}

.empty-state p {
  font-size: var(--text-base);
  color: var(--text-secondary);
  margin: 0;
}

.empty-state span {
  font-size: var(--text-sm);
}

/* ═══════════════════════════════════════════════════════════════
   PAGINATION
   ═══════════════════════════════════════════════════════════════ */

.pagination-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-top: var(--space-4);
}

.pagination-info {
  font-size: var(--text-sm);
  color: var(--text-secondary);
}

.pagination-controls {
  display: flex;
  gap: var(--space-1);
}

.page-btn {
  min-width: 32px;
  height: 32px;
  padding: 0 var(--space-2);
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  font-size: var(--text-sm);
  cursor: pointer;
  transition: all var(--duration-normal);
}

.page-btn:hover:not(:disabled) {
  background: var(--bg-hover);
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

.page-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.page-btn--active {
  background: var(--accent-primary);
  border-color: var(--accent-primary);
  color: var(--bg-void);
}

.page-btn--ellipsis {
  background: transparent;
  border: none;
}

.page-btn svg {
  width: 14px;
  height: 14px;
}

/* ═══════════════════════════════════════════════════════════════
   K-LINE DIALOG
   ═══════════════════════════════════════════════════════════════ */

.kline-dialog-content {
  height: 70vh;
  display: flex;
  flex-direction: column;
  gap: var(--space-4);
}

.kline-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--space-3);
}

.quick-ranges {
  display: flex;
  gap: var(--space-2);
}

.quick-range-btn {
  padding: var(--space-1) var(--space-3);
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--radius-sm);
  color: var(--text-secondary);
  font-size: var(--text-xs);
  cursor: pointer;
  transition: all var(--duration-normal);
}

.quick-range-btn:hover {
  border-color: var(--accent-primary);
  color: var(--accent-primary);
}

.chart-wrapper {
  flex: 1;
  min-height: 0;
}

/* Utility Classes */
.text-right {
  text-align: right;
}

.text-muted {
  color: var(--text-muted);
}

.text-data {
  font-family: var(--font-data);
  font-variant-numeric: tabular-nums;
}
</style>

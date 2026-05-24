<template>
  <div class="kline-query">
    <!-- 查询条件 -->
    <el-form :inline="true" class="query-form">
      <el-form-item label="股票代码">
        <el-select
          v-model="queryParams.symbol"
          filterable
          remote
          reserve-keyword
          placeholder="输入股票代码或名称搜索"
          :remote-method="searchStocks"
          :loading="stockSearchLoading"
          style="width: 220px"
          @change="handleSymbolChange"
        >
          <el-option
            v-for="item in stockOptions"
            :key="item.symbol"
            :label="`${item.symbol} ${item.name}`"
            :value="item.symbol"
          />
        </el-select>
      </el-form-item>
      <el-form-item label="周期">
        <el-select v-model="queryParams.period" style="width: 100px">
          <el-option label="分钟" value="minute" />
          <el-option label="日线" value="daily" />
          <el-option label="周线" value="weekly" />
          <el-option label="月线" value="monthly" />
        </el-select>
      </el-form-item>
      <el-form-item label="日期范围">
        <el-date-picker
          v-model="queryParams.start_date"
          type="date"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          format="YYYY-MM-DD"
          value-format="YYYY-MM-DD"
          style="width: 130px"
        />
        <span class="date-separator">至</span>
        <el-date-picker
          v-model="queryParams.end_date"
          type="date"
          placeholder="结束日期"
          format="YYYY-MM-DD"
          value-format="YYYY-MM-DD"
          style="width: 130px"
        />
      </el-form-item>
      <el-form-item>
        <el-button-group>
          <el-button size="small" @click="setDateRange(30)">1月</el-button>
          <el-button size="small" @click="setDateRange(90)">3月</el-button>
          <el-button size="small" @click="setDateRange(180)">半年</el-button>
          <el-button size="small" @click="setDateRange(365)">1年</el-button>
          <el-button size="small" @click="setDateRange(1825)">5年</el-button>
          <el-button size="small" @click="setDateRange(3650)">10年</el-button>
        </el-button-group>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="handleQuery" :loading="loading">
          查询
        </el-button>
        <el-button @click="handleExport" :disabled="!tableData.length">
          <el-icon><Download /></el-icon>
          导出 CSV
        </el-button>
      </el-form-item>
    </el-form>

    <!-- 数据统计 -->
    <div class="data-stats" v-if="tableData.length > 0">
      <el-tag type="info">共 {{ total }} 条记录</el-tag>
      <el-tag type="success" style="margin-left: 10px">
        {{ queryParams.symbol }}
      </el-tag>
      <el-tag type="warning" style="margin-left: 10px">
        {{ queryParams.start_date || '起始' }} ~ {{ queryParams.end_date || '至今' }}
      </el-tag>
      <el-tag style="margin-left: 10px">
        价格区间: {{ priceRange.min?.toFixed(2) || '-' }} ~ {{ priceRange.max?.toFixed(2) || '-' }}
      </el-tag>
    </div>

    <!-- 图表区域 -->
    <el-card v-if="tableData.length > 0" class="chart-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span>K线图</span>
        </div>
      </template>
      <KlineChart :data="chartData" />
    </el-card>

    <!-- 数据表格 -->
    <el-card ref="tableCardRef" class="table-card" shadow="never">
      <el-table-v2
        :data="tableData"
        :columns="tableColumns"
        v-loading="loading"
        :width="tableWidth"
        :height="400"
        fixed
      />
    </el-card>

    <!-- 分页 -->
    <div class="pagination-container" v-if="total > 0">
      <el-pagination
        v-model:current-page="queryParams.page"
        v-model:page-size="queryParams.page_size"
        :page-sizes="[20, 50, 100, 200, 500]"
        :total="total"
        layout="total, sizes, prev, pager, next, jumper"
        @size-change="handleQuery"
        @current-change="handleQuery"
      />
    </div>
  </div>
</template>

<script setup lang="tsx">
import { ref, reactive, computed, onMounted, h } from 'vue'
import { useResizeObserver } from '@vueuse/core'
import { ElMessage } from 'element-plus'
import { Download } from '@element-plus/icons-vue'
import request from '@/api/request'
import KlineChart from './KlineChart.vue'
import type { Column } from 'element-plus'

interface KlineRow {
  symbol: string
  trade_date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  amount: number
  turnover?: number
}

interface StockOption {
  symbol: string
  name: string
}

type KlineCellParams = {
  rowData: KlineRow
  rowIndex: number
}

const loading = ref(false)
const tableData = ref<KlineRow[]>([])
const total = ref(0)
const tableWidth = ref(1180)
const tableCardRef = ref<HTMLElement | null>(null)

// 股票搜索
const stockSearchLoading = ref(false)
const stockOptions = ref<StockOption[]>([])

const queryParams = reactive({
  symbol: '',
  period: 'daily',
  start_date: '',
  end_date: '',
  page: 1,
  page_size: 100,
})

// 价格区间
const priceRange = computed(() => {
  if (tableData.value.length === 0) return { min: null, max: null }
  const highs = tableData.value.map(d => d.high).filter(Boolean)
  const lows = tableData.value.map(d => d.low).filter(Boolean)
  return {
    min: Math.min(...lows),
    max: Math.max(...highs),
  }
})

const chartData = computed(() =>
  tableData.value.map((row) => ({
    datetime: row.trade_date,
    open: row.open,
    high: row.high,
    low: row.low,
    close: row.close,
    volume: row.volume,
    amount: row.amount,
  }))
)

// 设置默认日期范围
const setDateRange = (days: number) => {
  const end = new Date()
  const start = new Date()
  start.setDate(start.getDate() - days)
  queryParams.start_date = formatDate(start)
  queryParams.end_date = formatDate(end)
}

const formatDate = (date: Date): string => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

// 搜索股票
const searchStocks = async (query: string) => {
  if (!query) {
    stockOptions.value = []
    return
  }
  stockSearchLoading.value = true
  try {
    const response = await request.get<{ items: StockOption[] }>('/data/stocks', {
      params: { search: query, page_size: 20 },
    })
    stockOptions.value = response.items || []
  } catch {
    stockOptions.value = []
  } finally {
    stockSearchLoading.value = false
  }
}

// 股票选择变化
const handleSymbolChange = () => {
  if (queryParams.symbol && (!queryParams.start_date || !queryParams.end_date)) {
    setDateRange(365)
  }
}

const handleQuery = async () => {
  if (!queryParams.symbol) {
    ElMessage.warning('请输入股票代码')
    return
  }

  // 更新日期参数
  loading.value = true
  try {
    const params: Record<string, unknown> = {
      symbol: queryParams.symbol,
      period: queryParams.period,
      page: queryParams.page,
      page_size: queryParams.page_size,
    }

    if (queryParams.start_date) {
      params.start_date = queryParams.start_date
    }
    if (queryParams.end_date) {
      params.end_date = queryParams.end_date
    }

    const response = await request.get<{ items: KlineRow[]; total: number }>('/data/klines', {
      params,
    })

    tableData.value = response.items || []
    total.value = response.total || 0

  } catch (error: unknown) {
    const err = error as { response?: { data?: { detail?: string } } }
    ElMessage.error(err.response?.data?.detail || '查询失败')
    tableData.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const handleExport = () => {
  if (!tableData.value.length) return

  const headers = ['交易日期', '开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']
  const rows = tableData.value.map((row) => [
    row.trade_date,
    row.open?.toFixed(2) || '',
    row.high?.toFixed(2) || '',
    row.low?.toFixed(2) || '',
    row.close?.toFixed(2) || '',
    row.volume,
    row.amount?.toFixed(2) || '',
  ])

  const csvContent = [headers.join(','), ...rows.map((r) => r.join(','))].join('\n')

  const blob = new Blob(['\ufeff' + csvContent], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = `klines_${queryParams.symbol}_${queryParams.start_date}_${queryParams.end_date}.csv`
  link.click()
  URL.revokeObjectURL(url)

  ElMessage.success('导出成功')
}

const formatVolume = (volume: number): string => {
  if (!volume) return '-'
  if (volume >= 100000000) {
    return (volume / 100000000).toFixed(2) + '亿'
  }
  if (volume >= 10000) {
    return (volume / 10000).toFixed(2) + '万'
  }
  return volume.toLocaleString()
}

const formatAmount = (amount: number): string => {
  if (!amount) return '-'
  if (amount >= 100000000) {
    return (amount / 100000000).toFixed(2) + '亿'
  }
  if (amount >= 10000) {
    return (amount / 10000).toFixed(2) + '万'
  }
  return amount.toLocaleString()
}

const calcChange = (current: KlineRow, previous: KlineRow): string => {
  if (!current.close || !previous?.close) return '-'
  const change = ((current.close - previous.close) / previous.close) * 100
  return (change >= 0 ? '+' : '') + change.toFixed(2) + '%'
}

const calcAmplitude = (current: KlineRow, previous: KlineRow): string => {
  if (!current.high || !current.low || !previous?.close) return '-'
  const amplitude = ((current.high - current.low) / previous.close) * 100
  return amplitude.toFixed(2) + '%'
}

const getChangeClass = (current: KlineRow, previous: KlineRow): string => {
  if (!current.close || !previous?.close) return ''
  const change = ((current.close - previous.close) / previous.close) * 100
  return change >= 0 ? 'change-up' : 'change-down'
}

const getPriceClass = (row: KlineRow): string => {
  if (!row.open || !row.close) return ''
  return row.close >= row.open ? 'change-up' : 'change-down'
}

const tableColumns = computed<Column<unknown>[]>(() => [
  { key: 'trade_date', dataKey: 'trade_date', title: '交易日期', width: 130, fixed: true },
  { key: 'open', dataKey: 'open', title: '开盘价', width: 100, align: 'right', cellRenderer: (params: KlineCellParams) => h('span', formatPrice(params.rowData.open)) },
  { key: 'high', dataKey: 'high', title: '最高价', width: 100, align: 'right', cellRenderer: (params: KlineCellParams) => h('span', formatPrice(params.rowData.high)) },
  { key: 'low', dataKey: 'low', title: '最低价', width: 100, align: 'right', cellRenderer: (params: KlineCellParams) => h('span', formatPrice(params.rowData.low)) },
  {
    key: 'close',
    dataKey: 'close',
    title: '收盘价',
    width: 100,
    align: 'right',
    cellRenderer: (params: KlineCellParams) => h('span', { class: getPriceClass(params.rowData) }, formatPrice(params.rowData.close)),
  },
  {
    key: 'change_pct',
    dataKey: 'change_pct',
    title: '涨跌幅',
    width: 100,
    align: 'right',
    cellRenderer: (params: KlineCellParams) => {
      const previous = tableData.value[params.rowIndex + 1]
      return previous
        ? h('span', { class: getChangeClass(params.rowData, previous) }, calcChange(params.rowData, previous))
        : h('span', '-')
    },
  },
  { key: 'volume', dataKey: 'volume', title: '成交量', width: 130, align: 'right', cellRenderer: (params: KlineCellParams) => h('span', formatVolume(params.rowData.volume)) },
  { key: 'amount', dataKey: 'amount', title: '成交额', width: 150, align: 'right', cellRenderer: (params: KlineCellParams) => h('span', formatAmount(params.rowData.amount)) },
  {
    key: 'amplitude',
    dataKey: 'amplitude',
    title: '振幅',
    width: 100,
    align: 'right',
    cellRenderer: (params: KlineCellParams) => {
      const previous = tableData.value[params.rowIndex + 1]
      return h('span', previous ? calcAmplitude(params.rowData, previous) : '-')
    },
  },
  {
    key: 'turnover',
    dataKey: 'turnover',
    title: '换手率',
    width: 100,
    align: 'right',
    cellRenderer: (params: KlineCellParams) => h('span', params.rowData.turnover ? `${params.rowData.turnover.toFixed(2)}%` : '-'),
  },
])

const formatPrice = (value?: number) => value === undefined || value === null ? '-' : value.toFixed(2)

onMounted(() => {
  setDateRange(365)
})

useResizeObserver(tableCardRef, ([entry]) => {
  const width = Math.floor(entry.contentRect.width)
  if (width > 0) tableWidth.value = Math.max(760, width - 2)
})
</script>

<style scoped>
.kline-query {
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.query-form {
  padding: 16px;
  background: #f5f7fa;
  border-radius: 4px;
  flex-shrink: 0;
}

.data-stats {
  display: flex;
  align-items: center;
  flex-shrink: 0;
}

.date-separator {
  margin: 0 8px;
  color: #606266;
}

.chart-card {
  flex-shrink: 0;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.kline-chart {
  width: 100%;
  height: 400px;
}

.table-card {
  flex: 1;
  min-height: 0;
}

.pagination-container {
  display: flex;
  justify-content: flex-end;
  flex-shrink: 0;
}

.change-up {
  color: #f56c6c;
  font-weight: bold;
}

.change-down {
  color: #67c23a;
  font-weight: bold;
}
</style>

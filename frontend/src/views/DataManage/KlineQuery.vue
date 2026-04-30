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
          v-model="dateRange"
          type="daterange"
          range-separator="至"
          start-placeholder="开始日期"
          end-placeholder="结束日期"
          format="YYYY-MM-DD"
          value-format="YYYY-MM-DD"
          style="width: 260px"
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
          <el-checkbox v-model="showMA" label="显示均线" />
        </div>
      </template>
      <div ref="chartRef" class="kline-chart"></div>
    </el-card>

    <!-- 数据表格 -->
    <el-card class="table-card" shadow="never">
      <el-table
        :data="tableData"
        v-loading="loading"
        stripe
        border
        max-height="400"
        @sort-change="handleSortChange"
      >
        <el-table-column prop="trade_date" label="交易日期" width="120" sortable="custom" fixed />
        <el-table-column prop="open" label="开盘价" width="100" align="right">
          <template #default="{ row }">
            {{ row.open?.toFixed(2) || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="high" label="最高价" width="100" align="right">
          <template #default="{ row }">
            {{ row.high?.toFixed(2) || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="low" label="最低价" width="100" align="right">
          <template #default="{ row }">
            {{ row.low?.toFixed(2) || '-' }}
          </template>
        </el-table-column>
        <el-table-column prop="close" label="收盘价" width="100" align="right">
          <template #default="{ row }">
            <span :class="getPriceClass(row)">{{ row.close?.toFixed(2) || '-' }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="change_pct" label="涨跌幅" width="100" align="right">
          <template #default="{ row, $index }">
            <span v-if="$index < tableData.length - 1" :class="getChangeClass(row, tableData[$index + 1])">
              {{ calcChange(row, tableData[$index + 1]) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="volume" label="成交量" width="120" align="right">
          <template #default="{ row }">
            {{ formatVolume(row.volume) }}
          </template>
        </el-table-column>
        <el-table-column prop="amount" label="成交额" width="140" align="right">
          <template #default="{ row }">
            {{ formatAmount(row.amount) }}
          </template>
        </el-table-column>
        <el-table-column prop="amplitude" label="振幅" width="90" align="right">
          <template #default="{ row, $index }">
            <span v-if="$index < tableData.length - 1">
              {{ calcAmplitude(row, tableData[$index + 1]) }}
            </span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column prop="turnover" label="换手率" width="90" align="right">
          <template #default="{ row }">
            {{ row.turnover ? row.turnover.toFixed(2) + '%' : '-' }}
          </template>
        </el-table-column>
      </el-table>
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

<script setup lang="ts">
import { ref, reactive, computed, watch, nextTick, onMounted, onUnmounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Download } from '@element-plus/icons-vue'
import * as echarts from 'echarts'
import request from '@/api/request'

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

const loading = ref(false)
const tableData = ref<KlineRow[]>([])
const total = ref(0)
const showMA = ref(true)

// 股票搜索
const stockSearchLoading = ref(false)
const stockOptions = ref<StockOption[]>([])

// 图表
const chartRef = ref<HTMLDivElement | null>(null)
let chartInstance: echarts.ECharts | null = null

const dateRange = ref<string[]>([])
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

// 设置默认日期范围
const setDateRange = (days: number) => {
  const end = new Date()
  const start = new Date()
  start.setDate(start.getDate() - days)
  dateRange.value = [formatDate(start), formatDate(end)]
  queryParams.start_date = dateRange.value[0]
  queryParams.end_date = dateRange.value[1]
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
  if (queryParams.symbol && dateRange.value.length === 0) {
    setDateRange(365)
  }
}

const handleQuery = async () => {
  if (!queryParams.symbol) {
    ElMessage.warning('请输入股票代码')
    return
  }

  // 更新日期参数
  if (dateRange.value && dateRange.value.length === 2) {
    queryParams.start_date = dateRange.value[0]
    queryParams.end_date = dateRange.value[1]
  }

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

    // 更新图表
    nextTick(() => {
      updateChart()
    })
  } catch (error: unknown) {
    const err = error as { response?: { data?: { detail?: string } } }
    ElMessage.error(err.response?.data?.detail || '查询失败')
    tableData.value = []
    total.value = 0
  } finally {
    loading.value = false
  }
}

const handleSortChange = ({ prop, order }: { prop: string; order: string }) => {
  console.log('Sort:', prop, order)
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

// 初始化图表
const initChart = () => {
  if (!chartRef.value) return

  chartInstance = echarts.init(chartRef.value)
}

// 更新图表
const updateChart = () => {
  if (!chartInstance || tableData.value.length === 0) return

  // 数据按日期升序排列
  const sortedData = [...tableData.value].sort((a, b) =>
    new Date(a.trade_date).getTime() - new Date(b.trade_date).getTime()
  )

  const dates = sortedData.map(d => d.trade_date)
  const klineData = sortedData.map(d => [d.open, d.close, d.low, d.high])
  const volumeData = sortedData.map(d => d.volume)

  // 计算均线
  const calcMA = (data: number[], period: number): (number | null)[] => {
    const result: (number | null)[] = []
    for (let i = 0; i < data.length; i++) {
      if (i < period - 1) {
        result.push(null)
      } else {
        const sum = data.slice(i - period + 1, i + 1).reduce((a, b) => a + b, 0)
        result.push(sum / period)
      }
    }
    return result
  }

  const closes = sortedData.map(d => d.close)
  const ma5 = calcMA(closes, 5)
  const ma10 = calcMA(closes, 10)
  const ma20 = calcMA(closes, 20)
  const ma60 = calcMA(closes, 60)

  const option: echarts.EChartsOption = {
    animation: false,
    legend: {
      data: showMA.value ? ['K线', 'MA5', 'MA10', 'MA20', 'MA60', '成交量'] : ['K线', '成交量'],
      bottom: 10,
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'cross' },
      formatter: (params: unknown) => {
        const items = Array.isArray(params) ? params as Array<{ seriesName: string; axisValue: string; dataIndex: number }> : []
        if (!items || items.length === 0) return ''
        const date = items[0].axisValue
        const idx = items[0].dataIndex
        if (idx < 0 || idx >= sortedData.length) return ''
        const d = sortedData[idx]
        const open = d.open
        const close = d.close
        const high = d.high
        const low = d.low
        const change = close - open
        const pct = open !== 0 ? ((change / open) * 100).toFixed(2) : '0.00'
        const color = change >= 0 ? 'color:#f56c6c' : 'color:#67c23a'
        const vol = d.volume
        const fmtVol = (v: number) => v >= 1e8 ? (v / 1e8).toFixed(2) + '亿' : v >= 1e4 ? (v / 1e4).toFixed(2) + '万' : String(Math.round(v))
        return `<div style="font-size:13px;line-height:1.8">
          <b>${date}</b><br/>
          开盘: <b>${open.toFixed(2)}</b><br/>
          收盘: <b style="${color}">${close.toFixed(2)}</b><br/>
          最高: ${high.toFixed(2)}<br/>
          最低: ${low.toFixed(2)}<br/>
          涨跌: <b style="${color}">${change >= 0 ? '+' : ''}${pct}%</b><br/>
          成交量: ${fmtVol(vol)}
        </div>`
      },
    },
    grid: [
      { left: '8%', right: '3%', top: '5%', height: '55%' },
      { left: '8%', right: '3%', top: '68%', height: '18%' },
    ],
    xAxis: [
      {
        type: 'category',
        data: dates,
        boundaryGap: false,
        axisLine: { onZero: false },
        splitLine: { show: false },
        min: 'dataMin',
        max: 'dataMax',
        axisLabel: { formatter: (v: string) => v.substring(5) },
      },
      {
        type: 'category',
        gridIndex: 1,
        data: dates,
        boundaryGap: false,
        axisLine: { onZero: false },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
        min: 'dataMin',
        max: 'dataMax',
      },
    ],
    yAxis: [
      { scale: true, splitArea: { show: true } },
      { scale: true, gridIndex: 1, splitNumber: 2, axisLabel: { formatter: formatVolume } },
    ],
    dataZoom: [
      { type: 'inside', xAxisIndex: [0, 1], start: 50, end: 100 },
      { show: true, xAxisIndex: [0, 1], type: 'slider', bottom: 0, start: 50, end: 100, height: 20 },
    ],
    series: [
      {
        name: 'K线',
        type: 'candlestick',
        data: klineData,
        itemStyle: {
          color: '#f56c6c',
          color0: '#67c23a',
          borderColor: '#f56c6c',
          borderColor0: '#67c23a',
        },
      },
      ...(showMA.value ? [
        { name: 'MA5', type: 'line' as const, data: ma5, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
        { name: 'MA10', type: 'line' as const, data: ma10, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
        { name: 'MA20', type: 'line' as const, data: ma20, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
        { name: 'MA60', type: 'line' as const, data: ma60, smooth: true, lineStyle: { width: 1 }, symbol: 'none' },
      ] : []),
      {
        name: '成交量',
        type: 'bar',
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: volumeData,
        itemStyle: {
          color: (params: { dataIndex: number }) => {
            const item = sortedData[params.dataIndex]
            return item.close >= item.open ? '#f56c6c' : '#67c23a'
          },
        },
      },
    ],
  }

  chartInstance.setOption(option, true)
}

// 监听均线显示
watch(showMA, () => {
  updateChart()
})

// 窗口大小变化
const handleResize = () => {
  chartInstance?.resize()
}

onMounted(() => {
  setDateRange(365)
  initChart()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chartInstance?.dispose()
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

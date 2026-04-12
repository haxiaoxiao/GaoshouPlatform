<template>
  <div class="stock-detail-container">
    <!-- 顶部导航 -->
    <div class="header">
      <el-button @click="handleBack" :icon="ArrowLeft">返回列表</el-button>
      <div class="stock-info" v-if="stockInfo">
        <span class="stock-name">{{ stockInfo.name }}</span>
        <span class="stock-symbol">{{ stockInfo.symbol }}</span>
        <el-tag :type="stockInfo.market === 'SH' ? 'danger' : 'success'" size="small">
          {{ stockInfo.market }}
        </el-tag>
        <el-tag v-if="stockInfo.industry" size="small" type="info">
          {{ stockInfo.industry }}
        </el-tag>
      </div>
    </div>

    <!-- 控制面板 -->
    <el-card class="control-card" shadow="never">
      <el-form :inline="true" class="control-form">
        <el-form-item label="K线类型">
          <el-select v-model="klineType" style="width: 120px" @change="loadKlineData">
            <el-option label="日线" value="daily" />
            <el-option label="周线" value="weekly" />
            <el-option label="月线" value="monthly" />
            <el-option label="1分钟" value="minute1" />
            <el-option label="5分钟" value="minute5" />
            <el-option label="15分钟" value="minute15" />
            <el-option label="30分钟" value="minute30" />
            <el-option label="60分钟" value="minute60" />
          </el-select>
        </el-form-item>
        <el-form-item label="开始日期">
          <el-date-picker
            v-model="startDate"
            type="date"
            placeholder="选择开始日期"
            format="YYYY-MM-DD"
            value-format="YYYY-MM-DD"
            style="width: 150px"
            @change="loadKlineData"
          />
        </el-form-item>
        <el-form-item label="结束日期">
          <el-date-picker
            v-model="endDate"
            type="date"
            placeholder="选择结束日期"
            format="YYYY-MM-DD"
            value-format="YYYY-MM-DD"
            style="width: 150px"
            @change="loadKlineData"
          />
        </el-form-item>
        <el-form-item>
          <el-button-group>
            <el-button size="small" @click="setDateRange('1m')">1月</el-button>
            <el-button size="small" @click="setDateRange('3m')">3月</el-button>
            <el-button size="small" @click="setDateRange('6m')">6月</el-button>
            <el-button size="small" @click="setDateRange('1y')">1年</el-button>
            <el-button size="small" @click="setDateRange('3y')">3年</el-button>
          </el-button-group>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- K线图表 -->
    <el-card class="chart-card" shadow="never" v-loading="loading">
      <template #header>
        <div class="card-header">
          <span>K线图表</span>
          <span class="data-count" v-if="klineData.length">
            共 {{ klineData.length }} 条数据
          </span>
        </div>
      </template>

      <div v-if="!loading && klineData.length === 0" class="empty-container">
        <el-empty description="暂无K线数据" />
      </div>

      <KlineChart v-else :data="klineData" ref="klineChartRef" />
    </el-card>

    <!-- 股票详细信息 -->
    <el-card class="info-card" shadow="never" v-if="stockDetail">
      <template #header>
        <span>股票详情</span>
      </template>

      <el-descriptions :column="4" border>
        <el-descriptions-item label="股票代码">{{ stockDetail.symbol }}</el-descriptions-item>
        <el-descriptions-item label="股票名称">{{ stockDetail.name }}</el-descriptions-item>
        <el-descriptions-item label="所属市场">{{ stockDetail.market }}</el-descriptions-item>
        <el-descriptions-item label="所属行业">{{ stockDetail.industry || '-' }}</el-descriptions-item>
        <el-descriptions-item label="上市日期">{{ stockDetail.list_date || '-' }}</el-descriptions-item>
        <el-descriptions-item label="交易状态">
          <el-tag :type="stockDetail.is_active ? 'success' : 'danger'" size="small">
            {{ stockDetail.is_active ? '正常' : '停牌' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="市值">
          {{ stockDetail.market_cap ? formatNumber(stockDetail.market_cap) + '亿' : '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="市盈率(PE)">
          {{ stockDetail.pe_ratio?.toFixed(2) || '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="市净率(PB)">
          {{ stockDetail.pb_ratio?.toFixed(2) || '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="ROE">
          {{ stockDetail.roe ? (stockDetail.roe * 100).toFixed(2) + '%' : '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="营收增长">
          {{ stockDetail.revenue_growth ? (stockDetail.revenue_growth * 100).toFixed(2) + '%' : '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="利润增长">
          {{ stockDetail.profit_growth ? (stockDetail.profit_growth * 100).toFixed(2) + '%' : '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="资产负债率">
          {{ stockDetail.debt_ratio ? (stockDetail.debt_ratio * 100).toFixed(2) + '%' : '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="流动比率">
          {{ stockDetail.current_ratio?.toFixed(2) || '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="毛利率">
          {{ stockDetail.gross_margin ? (stockDetail.gross_margin * 100).toFixed(2) + '%' : '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="净利率">
          {{ stockDetail.net_margin ? (stockDetail.net_margin * 100).toFixed(2) + '%' : '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="股息率">
          {{ stockDetail.dividend_yield ? (stockDetail.dividend_yield * 100).toFixed(2) + '%' : '-' }}
        </el-descriptions-item>
      </el-descriptions>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ArrowLeft } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import KlineChart from './DataManage/KlineChart.vue'
import { klineApi, type KlineData, type KlineType } from '@/api/kline'
import { stockApi, type StockDetail } from '@/api/data'

// Router
const route = useRoute()
const router = useRouter()

// 状态
const loading = ref(false)
const klineData = ref<KlineData[]>([])
const stockDetail = ref<StockDetail | null>(null)
const klineType = ref<KlineType>('daily')
const startDate = ref<string>('')
const endDate = ref<string>('')
const klineChartRef = ref<InstanceType<typeof KlineChart> | null>(null)

// 从路由获取股票代码
const symbol = computed(() => route.params.symbol as string)

// 股票基本信息（从详情中获取）
const stockInfo = computed(() => stockDetail.value)

// 加载K线数据
const loadKlineData = async () => {
  if (!symbol.value) return

  loading.value = true
  try {
    const params: {
      symbol: string
      kline_type?: KlineType
      start_date?: string
      end_date?: string
    } = {
      symbol: symbol.value,
      kline_type: klineType.value,
    }

    if (startDate.value) {
      params.start_date = startDate.value
    }
    if (endDate.value) {
      params.end_date = endDate.value
    }

    klineData.value = await klineApi.getKlines(params)
  } catch (error) {
    console.error('加载K线数据失败:', error)
    ElMessage.error('加载K线数据失败')
    klineData.value = []
  } finally {
    loading.value = false
  }
}

// 加载股票详情
const loadStockDetail = async () => {
  // 根据symbol获取股票详情
  // 由于API需要ID，我们先通过列表搜索获取
  try {
    const response = await stockApi.getList({ search: symbol.value })
    const stock = response.items.find((s) => s.symbol === symbol.value)
    if (stock) {
      stockDetail.value = await stockApi.getDetail(stock.id)
    }
  } catch (error) {
    console.error('加载股票详情失败:', error)
  }
}

// 设置日期范围
const setDateRange = (range: string) => {
  const end = new Date()
  const start = new Date()

  switch (range) {
    case '1m':
      start.setMonth(start.getMonth() - 1)
      break
    case '3m':
      start.setMonth(start.getMonth() - 3)
      break
    case '6m':
      start.setMonth(start.getMonth() - 6)
      break
    case '1y':
      start.setFullYear(start.getFullYear() - 1)
      break
    case '3y':
      start.setFullYear(start.getFullYear() - 3)
      break
  }

  startDate.value = formatDate(start)
  endDate.value = formatDate(end)
  loadKlineData()
}

// 格式化日期
const formatDate = (date: Date): string => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

// 格式化数字
const formatNumber = (num: number) => {
  if (num >= 10000) {
    return (num / 10000).toFixed(2) + '万'
  }
  return num.toFixed(2)
}

// 返回列表
const handleBack = () => {
  router.push('/data')
}

// 初始化
onMounted(() => {
  // 设置默认日期范围（最近1年）
  const end = new Date()
  const start = new Date()
  start.setFullYear(start.getFullYear() - 1)

  startDate.value = formatDate(start)
  endDate.value = formatDate(end)

  loadKlineData()
  loadStockDetail()
})
</script>

<style scoped>
.stock-detail-container {
  display: flex;
  flex-direction: column;
  gap: 16px;
  height: 100%;
  overflow: auto;
}

.header {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 8px 0;
}

.stock-info {
  display: flex;
  align-items: center;
  gap: 8px;
}

.stock-name {
  font-size: 20px;
  font-weight: bold;
  color: #303133;
}

.stock-symbol {
  font-size: 16px;
  color: #606266;
}

.control-card {
  margin-bottom: 0;
}

.control-form {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.control-form :deep(.el-form-item) {
  margin-bottom: 0;
  margin-right: 16px;
}

.chart-card {
  flex: 1;
  min-height: 600px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.data-count {
  font-size: 12px;
  color: #909399;
}

.empty-container {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 400px;
}

.info-card {
  margin-bottom: 0;
}
</style>

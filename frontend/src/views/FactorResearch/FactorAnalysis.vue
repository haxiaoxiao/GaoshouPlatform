<template>
  <div class="factor-analysis" v-loading="loading">
    <!-- 顶部信息 -->
    <el-card class="info-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span>因子分析报告</span>
          <el-button @click="handleBack">返回列表</el-button>
        </div>
      </template>
      <el-descriptions :column="4" border v-if="analysis">
        <el-descriptions-item label="因子名称">{{ analysis.factor_name }}</el-descriptions-item>
        <el-descriptions-item label="分析周期">{{ analysis.start_date }} ~ {{ analysis.end_date }}</el-descriptions-item>
        <el-descriptions-item label="股票数量">{{ analysis.details?.total_stocks || '-' }}</el-descriptions-item>
        <el-descriptions-item label="分析日期数">{{ analysis.details?.total_dates || '-' }}</el-descriptions-item>
      </el-descriptions>
    </el-card>

    <!-- 核心指标 -->
    <el-card class="metrics-card" shadow="never" v-if="analysis?.details">
      <template #header>
        <span>核心指标</span>
      </template>
      <el-row :gutter="20">
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">IC 均值</div>
            <div class="metric-value" :class="analysis.details.ic_mean < 0 ? 'negative' : 'positive'">
              {{ analysis.details.ic_mean?.toFixed(4) || '-' }}
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">年化 ICIR</div>
            <div class="metric-value" :class="analysis.details.annual_icir < 0 ? 'negative' : 'positive'">
              {{ analysis.details.annual_icir?.toFixed(4) || '-' }}
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">年化收益</div>
            <div class="metric-value" :class="analysis.details.annual_return >= 0 ? 'positive' : 'negative'">
              {{ (analysis.details.annual_return * 100)?.toFixed(2) || '-' }}%
            </div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">信息比率</div>
            <div class="metric-value">{{ analysis.details.information_ratio?.toFixed(4) || '-' }}</div>
          </div>
        </el-col>
      </el-row>
      <el-row :gutter="20" style="margin-top: 20px;">
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">月度胜率</div>
            <div class="metric-value">{{ (analysis.details.win_rate * 100)?.toFixed(2) || '-' }}%</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">最大回撤</div>
            <div class="metric-value negative">{{ (analysis.details.max_drawdown * 100)?.toFixed(2) || '-' }}%</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">年化波动</div>
            <div class="metric-value">{{ (analysis.details.annual_vol * 100)?.toFixed(2) || '-' }}%</div>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="metric-item">
            <div class="metric-label">IC 标准差</div>
            <div class="metric-value">{{ analysis.details.ic_std?.toFixed(4) || '-' }}</div>
          </div>
        </el-col>
      </el-row>
    </el-card>

    <!-- IC 图表 -->
    <el-card class="chart-card" shadow="never" v-if="analysis?.details?.ic_series?.length">
      <template #header>
        <span>IC 序列</span>
      </template>
      <div ref="icChartRef" class="chart-container"></div>
    </el-card>

    <!-- 分组收益图表 -->
    <el-card class="chart-card" shadow="never" v-if="analysis?.details?.group_returns?.length">
      <template #header>
        <span>分组收益</span>
      </template>
      <div ref="groupChartRef" class="chart-container"></div>
    </el-card>

    <!-- 多空净值曲线 -->
    <el-card class="chart-card" shadow="never" v-if="analysis?.details?.group_returns?.length">
      <template #header>
        <span>多空净值曲线</span>
      </template>
      <div ref="cumulativeChartRef" class="chart-container"></div>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, nextTick, onUnmounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import * as echarts from 'echarts'
import { factorApi, type FactorAnalysis } from '@/api/factor'

const route = useRoute()
const router = useRouter()

// 状态
const loading = ref(false)
const analysis = ref<FactorAnalysis | null>(null)
const icChartRef = ref<HTMLDivElement | null>(null)
const groupChartRef = ref<HTMLDivElement | null>(null)
const cumulativeChartRef = ref<HTMLDivElement | null>(null)

let icChart: echarts.ECharts | null = null
let groupChart: echarts.ECharts | null = null
let cumulativeChart: echarts.ECharts | null = null

// 加载分析结果
const loadAnalysis = async () => {
  const analysisId = route.params.id as string
  if (!analysisId) {
    ElMessage.error('缺少分析ID')
    return
  }

  loading.value = true
  try {
    analysis.value = await factorApi.getAnalysis(parseInt(analysisId))
    // 渲染图表
    await nextTick()
    renderCharts()
  } catch (error) {
    console.error('加载分析结果失败:', error)
    ElMessage.error('加载分析结果失败')
  } finally {
    loading.value = false
  }
}

// 渲染图表
const renderCharts = () => {
  if (!analysis.value?.details) return

  renderICChart()
  renderGroupChart()
  renderCumulativeChart()
}

// IC 图表
const renderICChart = () => {
  if (!icChartRef.value || !analysis.value?.details?.ic_series) return

  if (icChart) {
    icChart.dispose()
  }

  icChart = echarts.init(icChartRef.value)

  const icSeries = analysis.value.details.ic_series
  const dates = icSeries.map((item) => item.trade_date.substring(0, 10))
  const ics = icSeries.map((item) => item.ic)

  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'axis',
    },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: {
        rotate: 45,
      },
    },
    yAxis: {
      type: 'value',
      name: 'IC',
    },
    series: [
      {
        name: 'IC',
        type: 'bar',
        data: ics,
        itemStyle: {
          color: (params: { dataIndex: number }) =>
            ics[params.dataIndex] >= 0 ? '#f56c6c' : '#67c23a',
        },
      },
    ],
    dataZoom: [
      {
        type: 'inside',
        start: 0,
        end: 100,
      },
    ],
  }

  icChart.setOption(option)
}

// 分组收益图表
const renderGroupChart = () => {
  if (!groupChartRef.value || !analysis.value?.details?.group_returns) return

  if (groupChart) {
    groupChart.dispose()
  }

  groupChart = echarts.init(groupChartRef.value)

  const groupReturns = analysis.value.details.group_returns
  const dates = groupReturns.map((item) => item.trade_date.substring(0, 10))

  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'axis',
    },
    legend: {
      data: ['组1(做多)', '组2', '组3', '组4', '组5(做空)', '多空'],
    },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: {
        rotate: 45,
      },
    },
    yAxis: {
      type: 'value',
      name: '收益率',
      axisLabel: {
        formatter: (value: number) => (value * 100).toFixed(1) + '%',
      },
    },
    series: [
      {
        name: '组1(做多)',
        type: 'line',
        data: groupReturns.map((item) => item.group_1),
      },
      {
        name: '组2',
        type: 'line',
        data: groupReturns.map((item) => item.group_2),
      },
      {
        name: '组3',
        type: 'line',
        data: groupReturns.map((item) => item.group_3),
      },
      {
        name: '组4',
        type: 'line',
        data: groupReturns.map((item) => item.group_4),
      },
      {
        name: '组5(做空)',
        type: 'line',
        data: groupReturns.map((item) => item.group_5),
      },
      {
        name: '多空',
        type: 'line',
        data: groupReturns.map((item) => item.long_short),
        lineStyle: {
          width: 3,
        },
      },
    ],
    dataZoom: [
      {
        type: 'inside',
        start: 0,
        end: 100,
      },
    ],
  }

  groupChart.setOption(option)
}

// 累计净值曲线
const renderCumulativeChart = () => {
  if (!cumulativeChartRef.value || !analysis.value?.details?.group_returns) return

  if (cumulativeChart) {
    cumulativeChart.dispose()
  }

  cumulativeChart = echarts.init(cumulativeChartRef.value)

  const groupReturns = analysis.value.details.group_returns
  const dates = groupReturns.map((item) => item.trade_date.substring(0, 10))

  // 计算累计净值
  const longShortReturns = groupReturns.map((item) => item.long_short)
  let cumulative = 1
  const cumulativeValues = longShortReturns.map((ret) => {
    cumulative *= 1 + (ret || 0)
    return cumulative
  })

  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'axis',
    },
    xAxis: {
      type: 'category',
      data: dates,
      axisLabel: {
        rotate: 45,
      },
    },
    yAxis: {
      type: 'value',
      name: '累计净值',
    },
    series: [
      {
        name: '多空净值',
        type: 'line',
        data: cumulativeValues,
        areaStyle: {
          opacity: 0.3,
        },
        lineStyle: {
          width: 2,
        },
      },
    ],
    dataZoom: [
      {
        type: 'inside',
        start: 0,
        end: 100,
      },
    ],
  }

  cumulativeChart.setOption(option)
}

// 返回列表
const handleBack = () => {
  router.push('/factor')
}

// 窗口大小变化
const handleResize = () => {
  icChart?.resize()
  groupChart?.resize()
  cumulativeChart?.resize()
}

// 初始化
onMounted(() => {
  loadAnalysis()
  window.addEventListener('resize', handleResize)
})

// 清理
onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  icChart?.dispose()
  groupChart?.dispose()
  cumulativeChart?.dispose()
})
</script>

<style scoped>
.factor-analysis {
  padding: 20px;
}

.info-card,
.metrics-card,
.chart-card {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.metric-item {
  text-align: center;
  padding: 10px;
  background: #f5f7fa;
  border-radius: 4px;
}

.metric-label {
  font-size: 14px;
  color: #909399;
  margin-bottom: 8px;
}

.metric-value {
  font-size: 24px;
  font-weight: bold;
  color: #303133;
}

.metric-value.positive {
  color: #f56c6c;
}

.metric-value.negative {
  color: #67c23a;
}

.chart-container {
  width: 100%;
  height: 400px;
}
</style>

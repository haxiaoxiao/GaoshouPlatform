<template>
  <div v-loading="loading" class="backtest-report">
    <template v-if="report">
      <!-- 关键指标卡片 -->
      <el-row :gutter="16" class="metrics-row">
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">总收益率</div>
            <div class="metric-value" :class="getReturnClass(report.result?.total_return)">
              {{ formatPercent(report.result?.total_return) }}
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">年化收益率</div>
            <div class="metric-value" :class="getReturnClass(report.result?.annual_return)">
              {{ formatPercent(report.result?.annual_return) }}
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">最大回撤</div>
            <div class="metric-value negative">
              {{ formatPercent(report.result?.max_drawdown) }}
            </div>
          </el-card>
        </el-col>
        <el-col :span="6">
          <el-card shadow="hover" class="metric-card">
            <div class="metric-label">夏普比率</div>
            <div class="metric-value" :class="getReturnClass(report.result?.sharpe_ratio)">
              {{ formatNumber(report.result?.sharpe_ratio, 2) }}
            </div>
          </el-card>
        </el-col>
      </el-row>

      <!-- 交易统计 -->
      <el-card shadow="never" class="stats-card">
        <template #header>
          <span>交易统计</span>
        </template>
        <el-row :gutter="24">
          <el-col :span="6">
            <div class="stat-item">
              <span class="stat-label">总交易次数</span>
              <span class="stat-value">{{ report.result?.total_trades ?? '-' }}</span>
            </div>
          </el-col>
          <el-col :span="6">
            <div class="stat-item">
              <span class="stat-label">盈利次数</span>
              <span class="stat-value positive">{{ report.result?.win_trades ?? '-' }}</span>
            </div>
          </el-col>
          <el-col :span="6">
            <div class="stat-item">
              <span class="stat-label">亏损次数</span>
              <span class="stat-value negative">{{ report.result?.loss_trades ?? '-' }}</span>
            </div>
          </el-col>
          <el-col :span="6">
            <div class="stat-item">
              <span class="stat-label">胜率</span>
              <span class="stat-value">{{ formatPercent(report.result?.win_rate) }}</span>
            </div>
          </el-col>
        </el-row>
      </el-card>

      <!-- 回测信息 -->
      <el-card shadow="never" class="info-card">
        <template #header>
          <span>回测信息</span>
        </template>
        <el-descriptions :column="2" border>
          <el-descriptions-item label="回测ID">{{ report.backtest_id }}</el-descriptions-item>
          <el-descriptions-item label="策略ID">{{ report.strategy_id }}</el-descriptions-item>
          <el-descriptions-item label="策略名称">{{ report.strategy_name || '-' }}</el-descriptions-item>
          <el-descriptions-item label="状态">
            <el-tag :type="getStatusType(report.status)" size="small">
              {{ getStatusLabel(report.status) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="开始日期">{{ report.start_date }}</el-descriptions-item>
          <el-descriptions-item label="结束日期">{{ report.end_date }}</el-descriptions-item>
          <el-descriptions-item label="初始资金">{{ formatCapital(report.initial_capital) }}</el-descriptions-item>
          <el-descriptions-item label="最终资金">{{ formatCapital(report.result?.final_capital?.toString() ?? null) }}</el-descriptions-item>
          <el-descriptions-item label="创建时间">{{ formatDateTime(report.created_at) }}</el-descriptions-item>
          <el-descriptions-item label="更新时间">{{ formatDateTime(report.updated_at) }}</el-descriptions-item>
        </el-descriptions>
      </el-card>
    </template>

    <el-empty v-else-if="!loading" description="暂无回测数据" />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { backtestApi, type BacktestReport as BacktestReportType } from '@/api/backtest'

const props = defineProps<{
  backtestId: number
}>()

// 状态
const loading = ref(false)
const report = ref<BacktestReportType | null>(null)

// 加载报告
const loadReport = async () => {
  loading.value = true
  try {
    report.value = await backtestApi.get(props.backtestId)
  } catch {
    ElMessage.error('加载回测报告失败')
    report.value = null
  } finally {
    loading.value = false
  }
}

// 获取状态标签类型
const getStatusType = (status: string): 'info' | 'warning' | 'success' | 'danger' => {
  const types: Record<string, 'info' | 'warning' | 'success' | 'danger'> = {
    pending: 'info',
    running: 'warning',
    completed: 'success',
    failed: 'danger',
  }
  return types[status] || 'info'
}

// 获取状态标签文本
const getStatusLabel = (status: string): string => {
  const labels: Record<string, string> = {
    pending: '待运行',
    running: '运行中',
    completed: '已完成',
    failed: '失败',
  }
  return labels[status] || status
}

// 获取收益率样式类
const getReturnClass = (value: number | undefined): string => {
  if (value === undefined || value === null) return ''
  if (value > 0) return 'positive'
  if (value < 0) return 'negative'
  return ''
}

// 格式化百分比
const formatPercent = (value: number | undefined): string => {
  if (value === undefined || value === null) return '-'
  return `${(value * 100).toFixed(2)}%`
}

// 格式化数字
const formatNumber = (value: number | undefined, decimals: number): string => {
  if (value === undefined || value === null) return '-'
  return value.toFixed(decimals)
}

// 格式化资金
const formatCapital = (capital: string | null | undefined): string => {
  if (!capital) return '-'
  const num = parseFloat(capital)
  if (isNaN(num)) return '-'
  return num.toLocaleString('zh-CN', { style: 'currency', currency: 'CNY' })
}

// 格式化日期时间
const formatDateTime = (dateStr: string | null | undefined): string => {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

// 监听 backtestId 变化
watch(() => props.backtestId, () => {
  if (props.backtestId) {
    loadReport()
  }
}, { immediate: true })

// 初始化
onMounted(() => {
  if (props.backtestId) {
    loadReport()
  }
})
</script>

<style scoped>
.backtest-report {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.metrics-row {
  margin-bottom: 0;
}

.metric-card {
  text-align: center;
}

.metric-card :deep(.el-card__body) {
  padding: 20px;
}

.metric-label {
  font-size: 14px;
  color: #909399;
  margin-bottom: 8px;
}

.metric-value {
  font-size: 28px;
  font-weight: bold;
  color: #303133;
}

.metric-value.positive {
  color: #67c23a;
}

.metric-value.negative {
  color: #f56c6c;
}

.stats-card :deep(.el-card__body) {
  padding: 20px;
}

.stat-item {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.stat-label {
  font-size: 14px;
  color: #909399;
}

.stat-value {
  font-size: 20px;
  font-weight: 500;
  color: #303133;
}

.stat-value.positive {
  color: #67c23a;
}

.stat-value.negative {
  color: #f56c6c;
}

.info-card :deep(.el-card__body) {
  padding: 20px;
}
</style>

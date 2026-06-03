<template>
  <div v-loading="loading" class="optimization-report">
    <div class="report-header">
      <div>
        <el-button link type="primary" @click="router.push('/backtest')">← 返回回测</el-button>
        <h2>{{ title }}</h2>
        <p v-if="report" class="subtitle">
          ID {{ report.backtest_id || report.id }} · {{ report.start_date }} ~ {{ report.end_date }} ·
          {{ rows.length.toLocaleString() }} 行原始结果
        </p>
      </div>
      <el-button v-if="rows.length" type="primary" plain @click="downloadCsv">导出 CSV</el-button>
    </div>

    <template v-if="report">
      <el-row :gutter="12" class="summary-row">
        <el-col :span="6">
          <div class="summary-card">
            <span>优化类型</span>
            <strong>{{ typeLabel }}</strong>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="summary-card">
            <span>优化目标</span>
            <strong>{{ metricLabel }}</strong>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="summary-card">
            <span>训练/测试</span>
            <strong>{{ valueToString(report.result?.train_period || report.parameters?.train_period) }} / {{ valueToString(report.result?.test_period || report.parameters?.test_period) }} bar</strong>
          </div>
        </el-col>
        <el-col :span="6">
          <div class="summary-card">
            <span>并行数</span>
            <strong>{{ valueToString(report.parameters?.max_workers) }}</strong>
          </div>
        </el-col>
      </el-row>

      <el-card v-if="isWalkForward" shadow="never" class="result-card">
        <template #header>
          <div class="card-header">
            <span>Walk-forward 窗口汇总</span>
            <span class="muted">点击列头排序；目标值按当前优化目标计算，默认降序看最优窗口</span>
          </div>
        </template>
        <el-table :data="windowRows" stripe height="560" size="small" :default-sort="{ prop: 'objective_value', order: 'descending' }">
          <el-table-column prop="window" label="#" width="60" fixed sortable />
          <el-table-column prop="train_start" label="训练开始" min-width="170" sortable />
          <el-table-column prop="train_end" label="训练结束" min-width="170" sortable />
          <el-table-column prop="row_count" label="测试行数" width="100" sortable />
          <el-table-column prop="start_equity" label="期初权益" width="120" sortable :sort-method="sortBy('start_equity')">
            <template #default="{ row }">{{ valueToString(row.start_equity) }}</template>
          </el-table-column>
          <el-table-column prop="end_equity" label="期末权益" width="120" sortable :sort-method="sortBy('end_equity')">
            <template #default="{ row }">{{ valueToString(row.end_equity) }}</template>
          </el-table-column>
          <el-table-column prop="return_pct" label="收益率" width="110" sortable :sort-method="sortBy('return_pct')">
            <template #default="{ row }">
              <span :class="returnClass(row.return_pct)">{{ formatPercentValue(row.return_pct) }}</span>
            </template>
          </el-table-column>
          <el-table-column prop="max_drawdown" label="最大回撤" width="110" sortable :sort-method="sortBy('max_drawdown')">
            <template #default="{ row }">{{ formatPercentValue(row.max_drawdown) }}</template>
          </el-table-column>
          <el-table-column prop="objective_value" :label="objectiveColumnLabel" width="140" sortable :sort-method="sortBy('objective_value')">
            <template #default="{ row }">{{ valueToString(row.objective_value) }}</template>
          </el-table-column>
          <el-table-column
            v-for="col in paramKeys"
            :key="col"
            :label="columnDisplayName(col)"
            min-width="140"
            show-overflow-tooltip
            sortable
            :sort-method="sortParamBy(col)"
          >
            <template #default="{ row }">{{ valueToString(row.params[col]) }}</template>
          </el-table-column>
        </el-table>
      </el-card>

      <el-card shadow="never" class="result-card">
        <template #header>
          <div class="card-header">
            <span>{{ isWalkForward ? '原始逐 bar 结果' : 'Grid Search 结果' }}</span>
            <span class="muted">共 {{ rows.length.toLocaleString() }} 行；这里的“组合权益”不是股票市值</span>
          </div>
        </template>
        <el-table :data="pagedRows" stripe height="520" size="small">
          <el-table-column
            v-for="col in rawColumns"
            :key="col"
            :prop="col"
            :label="columnDisplayName(col)"
            min-width="140"
            show-overflow-tooltip
            sortable
          >
            <template #default="{ row }">{{ rawCell(row, col) }}</template>
          </el-table-column>
        </el-table>
        <el-pagination
          v-if="rows.length > pageSize"
          v-model:current-page="page"
          :page-size="pageSize"
          :total="rows.length"
          layout="prev, pager, next, total"
          small
          class="pager"
        />
      </el-card>
    </template>

    <el-empty v-else-if="!loading" description="暂无优化结果" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import { backtestApi, type BacktestReport } from '@/api/backtest'
import {
  columnDisplayName,
  compareValues,
  formatPercentValue,
  isOptimizationRecord,
  metricDisplayName,
  optimizationType,
  paramColumns,
  summarizeWalkForwardRows,
  valueToString,
} from '@/utils/optimizationReport'

const route = useRoute()
const router = useRouter()
const loading = ref(false)
const report = ref<BacktestReport | null>(null)
const page = ref(1)
const pageSize = 200

const rows = computed<Record<string, unknown>[]>(() => report.value?.result?.rows || [])
const type = computed(() => optimizationType(report.value?.parameters, report.value?.result as Record<string, unknown> | null))
const isWalkForward = computed(() => type.value === 'walk_forward')
const typeLabel = computed(() => isWalkForward.value ? 'Walk-forward' : type.value === 'grid_search' ? 'Grid Search' : '优化结果')
const title = computed(() => `${typeLabel.value} 报告`)
const metricKey = computed(() => report.value?.result?.metric || report.value?.result?.sort_by || report.value?.parameters?.metric || report.value?.parameters?.sort_by || 'calmar_ratio')
const metricLabel = computed(() => metricDisplayName(metricKey.value))
const objectiveColumnLabel = computed(() => `目标值 ${metricDisplayName(metricKey.value)}`)
const windowRows = computed(() => summarizeWalkForwardRows(rows.value, metricKey.value))
const paramKeys = computed(() => paramColumns(rows.value))
const rawColumns = computed(() => {
  const keys = new Set<string>()
  rows.value.slice(0, 200).forEach(row => Object.keys(row).forEach(key => keys.add(key)))
  return Array.from(keys)
})
const pagedRows = computed(() => {
  const start = (page.value - 1) * pageSize
  return rows.value.slice(start, start + pageSize)
})

const returnClass = (value: number | null) =>
  value == null ? '' : value >= 0 ? 'positive' : 'negative'

const sortBy = (key: string) => (a: Record<string, unknown>, b: Record<string, unknown>) =>
  compareValues(a[key], b[key])

const sortParamBy = (key: string) => (a: { params: Record<string, unknown> }, b: { params: Record<string, unknown> }) =>
  compareValues(a.params?.[key], b.params?.[key])

const rawCell = (row: Record<string, unknown>, col: string) => {
  if (['return', 'returns', 'total_return', 'annual_return', 'max_drawdown', 'win_rate'].includes(col)) {
    const n = Number(row[col])
    return Number.isFinite(n) ? formatPercentValue(n) : valueToString(row[col])
  }
  return valueToString(row[col])
}

const loadReport = async () => {
  const id = Number(route.params.id)
  if (!Number.isFinite(id)) return
  loading.value = true
  try {
    report.value = await backtestApi.get(id)
    if (!isOptimizationRecord(report.value.parameters, report.value.result as Record<string, unknown> | null)) {
      ElMessage.warning('这条记录不是优化任务')
    }
  } catch (e: any) {
    ElMessage.error(e?.message || '加载优化报告失败')
    report.value = null
  } finally {
    loading.value = false
  }
}

const downloadCsv = () => {
  if (!rows.value.length) return
  const cols = rawColumns.value
  const lines = [
    cols.map(columnDisplayName).join(','),
    ...rows.value.map(row => cols.map(col => JSON.stringify(row[col] ?? '')).join(',')),
  ]
  const blob = new Blob(['\ufeff' + lines.join('\n')], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `optimization_${report.value?.backtest_id || route.params.id}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

const pageContextBlocks = computed(() => [
  {
    title: 'Optimization',
    rows: [
      { label: '加载状态', value: loading.value ? '加载中' : '已就绪', tone: loading.value ? 'warn' : 'good' },
      { label: '类型', value: typeLabel.value },
      { label: '目标', value: metricLabel.value },
      { label: '参数列', value: `${paramKeys.value.length}` },
    ],
  },
  {
    title: 'Rows',
    rows: [
      { label: '原始结果', value: `${rows.value.length.toLocaleString()} 行` },
      { label: '窗口汇总', value: `${windowRows.value.length}` },
      { label: '分页', value: `${page.value} / ${Math.max(1, Math.ceil(rows.value.length / pageSize))}` },
    ],
  },
])

usePageContext(pageContextBlocks)

watch(() => route.params.id, loadReport, { immediate: true })
</script>

<style scoped>
.optimization-report {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 16px;
}
.report-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
}
.report-header h2 {
  margin: 8px 0 4px;
}
.subtitle,
.muted {
  color: #909399;
  font-size: 12px;
}
.summary-card {
  border: 1px solid #2b2f3a;
  border-radius: 8px;
  background: #151922;
  padding: 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.summary-card span {
  color: #909399;
  font-size: 12px;
}
.summary-card strong {
  color: #e5e7eb;
  font-size: 18px;
}
.result-card {
  background: #111827;
}
.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.positive {
  color: #d93026;
}
.negative {
  color: #137333;
}
.pager {
  margin-top: 12px;
  justify-content: flex-end;
}
</style>

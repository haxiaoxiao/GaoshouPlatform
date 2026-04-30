<template>
  <div class="page-container">
    <div class="page-header">
      <h2>趋势资金事件驱动策略</h2>
      <span class="subtitle">研报十一 · 基于趋势资金日内交易行为</span>
    </div>

    <!-- 参数栏 -->
    <el-card class="param-card">
      <el-form :inline="true" size="small">
        <el-form-item label="开始日期">
          <el-date-picker v-model="startDate" type="date" value-format="YYYY-MM-DD" />
        </el-form-item>
        <el-form-item label="结束日期">
          <el-date-picker v-model="endDate" type="date" value-format="YYYY-MM-DD" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" :loading="loading" @click="loadSummary">
            查询信号
          </el-button>
          <el-button type="success" :loading="btLoading" @click="runBacktest">
            运行回测
          </el-button>
        </el-form-item>
      </el-form>
      <div class="param-info">
        <span>趋势资金识别: 成交量 &gt; 过去5日90%分位数</span>
        <el-divider direction="vertical" />
        <span>信号融合: 信号B+C同时满足3日中≥2日触发</span>
        <el-divider direction="vertical" />
        <span>资金通道: 10通道 × 20日持仓，周度调仓</span>
      </div>
    </el-card>

    <!-- 信号统计 -->
    <el-row :gutter="16" class="stats-row">
      <el-col :span="6">
        <el-statistic title="信号日期数" :value="summary?.trading_days_with_signals ?? '-'" />
      </el-col>
      <el-col :span="6">
        <el-statistic title="综合信号总数" :value="summary?.total_composite_triggers ?? '-'" />
      </el-col>
      <el-col :span="6">
        <el-statistic title="日均触发" :value="summary?.avg_daily_triggers ?? '-'">
          <template #suffix>只</template>
        </el-statistic>
      </el-col>
      <el-col :span="6">
        <el-statistic title="样本空间" value="全A股" />
      </el-col>
    </el-row>

    <!-- 信号图表 + 回测结果 -->
    <el-row :gutter="16" class="content-row">
      <el-col :span="14">
        <el-card header="每日综合信号触发数">
          <div ref="chartDom" class="chart-container"></div>
        </el-card>
      </el-col>
      <el-col :span="10">
        <el-card header="回测结果">
          <div v-if="backtest" class="bt-results">
            <el-row :gutter="12">
              <el-col :span="12">
                <el-statistic title="交易次数" :value="backtest.total_trades" />
              </el-col>
              <el-col :span="12">
                <el-statistic title="胜率" :value="(backtest.win_rate * 100).toFixed(1)">
                  <template #suffix>%</template>
                </el-statistic>
              </el-col>
              <el-col :span="12">
                <el-statistic title="平均收益" :value="backtest.avg_return?.toFixed(2) ?? '0'">
                  <template #suffix>%</template>
                </el-statistic>
              </el-col>
              <el-col :span="12">
                <el-statistic title="累计收益" :value="backtest.total_return?.toFixed(2) ?? '0'">
                  <template #suffix>%</template>
                </el-statistic>
              </el-col>
            </el-row>

            <h4 style="margin-top:16px">最近交易</h4>
            <el-table :data="backtest.trades?.slice(-5) ?? []" size="small" stripe>
              <el-table-column prop="symbol" label="股票" width="100" />
              <el-table-column label="入场" width="90">
                <template #default="{ row }">{{ row.entry_date?.slice(5) }}</template>
              </el-table-column>
              <el-table-column label="出场" width="90">
                <template #default="{ row }">{{ row.exit_date?.slice(5) ?? '-' }}</template>
              </el-table-column>
              <el-table-column label="PnL" width="80">
                <template #default="{ row }">
                  <span :class="(row.pnl_pct ?? 0) >= 0 ? 'profit' : 'loss'">
                    {{ row.pnl_pct?.toFixed(2) ?? '-' }}%
                  </span>
                </template>
              </el-table-column>
            </el-table>
          </div>
          <el-empty v-else description="点击「运行回测」查看结果" />
        </el-card>
      </el-col>
    </el-row>

    <!-- 最新交易日信号明细 -->
    <el-card header="最新交易日综合信号" v-if="latestSignals.length">
      <el-table :data="latestSignals" size="small" stripe max-height="400">
        <el-table-column prop="symbol" label="股票代码" width="110" />
        <el-table-column label="信号B" width="75">
          <template #default="{ row }">
            <el-tag :type="row.signal_b_triggered ? 'success' : 'info'" size="small">
              {{ row.signal_b_triggered ? '✓' : '✗' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="信号C" width="75">
          <template #default="{ row }">
            <el-tag :type="row.signal_c_triggered ? 'success' : 'info'" size="small">
              {{ row.signal_c_triggered ? '✓' : '✗' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="均价偏离(B_val)" width="120">
          <template #default="{ row }">
            {{ row.signal_b_value != null ? (row.signal_b_value * 100).toFixed(2) + '%' : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="净支撑量(C_val)" width="110">
          <template #default="{ row }">
            {{ row.signal_c_value != null ? row.signal_c_value.toFixed(0) : '-' }}
          </template>
        </el-table-column>
        <el-table-column label="趋势分钟" width="90">
          <template #default="{ row }">
            {{ row.trend_minute_count }}/{{ row.total_minute_count }}
          </template>
        </el-table-column>
      </el-table>
    </el-card>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, nextTick } from 'vue'
import * as echarts from 'echarts'
import { strategyApi } from '@/api/strategy'
import type { DailySignals, SignalsSummary, BacktestResult } from '@/api/strategy'

const startDate = ref('2026-04-01')
const endDate = ref('2026-04-21')
const loading = ref(false)
const btLoading = ref(false)

const summary = ref<SignalsSummary | null>(null)
const backtest = ref<BacktestResult | null>(null)
const latestSignals = ref<DailySignals['signals']>([])

const chartDom = ref<HTMLElement | null>(null)
let chart: echarts.ECharts | null = null

async function loadSummary() {
  loading.value = true
  try {
    const res = await strategyApi.getSignalsSummary({
      start_date: startDate.value,
      end_date: endDate.value,
    })
    summary.value = res.data

    if (res.data.daily_summary.length > 0) {
      const last = res.data.daily_summary[res.data.daily_summary.length - 1]
      latestSignals.value = last.stocks.map(s => ({
        symbol: s.symbol,
        trade_date: last.trade_date,
        signal_b_value: s.b_val,
        signal_c_value: s.c_val,
        signal_b_triggered: s.b,
        signal_c_triggered: s.c,
        composite_triggered: true,
        trend_minute_count: 0,
        total_minute_count: 0,
      }))
    }
    await nextTick()
    renderChart()
  } finally {
    loading.value = false
  }
}

function renderChart() {
  if (!chartDom.value) return
  if (!chart) {
    chart = echarts.init(chartDom.value)
  }
  const data = summary.value?.daily_summary ?? []
  chart.setOption({
    tooltip: { trigger: 'axis' },
    xAxis: {
      type: 'category',
      data: data.map(d => d.trade_date.slice(5)),
    },
    yAxis: { type: 'value', name: '触发数' },
    series: [{
      type: 'bar',
      data: data.map(d => d.count),
      itemStyle: { color: '#409EFF', borderRadius: [4, 4, 0, 0] },
    }],
    grid: { left: 50, right: 20, top: 20, bottom: 30 },
  })
  chart.resize()
}

async function runBacktest() {
  btLoading.value = true
  try {
    const res = await strategyApi.runChannelBacktest({
      start_date: startDate.value,
      end_date: endDate.value,
    })
    backtest.value = res.data
  } finally {
    btLoading.value = false
  }
}
</script>

<style scoped>
.page-container { padding: 20px; }
.page-header { margin-bottom: 16px; }
.page-header .subtitle { color: #909399; font-size: 13px; margin-left: 12px; }

.param-card { margin-bottom: 16px; }
.param-info { color: #909399; font-size: 12px; margin-top: 8px; }

.stats-row { margin-bottom: 16px; }
.content-row { margin-bottom: 16px; }
.chart-container { height: 250px; }

.profit { color: #F56C6C; font-weight: 600; }
.loss { color: #67C23A; font-weight: 600; }

.bt-results .el-statistic { margin-bottom: 8px; }
</style>

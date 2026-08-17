<template>
  <main class="page-frame page-scroll intraday-page" v-loading="initialLoading">
    <header class="desk-header">
      <div class="desk-title">
        <span class="section-kicker">INTRADAY T DESK</span>
        <div class="title-line">
          <h2>日内做 T</h2>
          <span class="paper-seal">仅模拟</span>
        </div>
        <p>利通电子与澜起科技底仓成本管理</p>
      </div>
      <div class="universe-tape" aria-label="固定策略股票池">
        <div v-for="symbol in symbolCards" :key="symbol.symbol" class="ticker-cell">
          <span>{{ symbol.symbol }}</span>
          <strong>{{ symbol.name }}</strong>
          <small>{{ symbol.board === 'STAR' ? '科创板 · 最少 200 股' : '主板 · 100 股整数倍' }}</small>
        </div>
        <div class="ticker-cell ticker-cell--guardrail">
          <span>RESTORE</span>
          <strong>14:49</strong>
          <small>底仓恢复信号</small>
        </div>
      </div>
    </header>

    <el-alert
      v-if="pageError"
      class="page-alert"
      type="error"
      :title="pageError"
      show-icon
      closable
      @close="pageError = ''"
    />

    <section class="desk-band configuration-band">
      <div class="band-heading">
        <div>
          <span>01 / STRATEGY</span>
          <h3>回测参数</h3>
        </div>
        <div class="data-state">
          <span :class="['state-dot', coverageReady ? 'state-dot--ready' : '']"></span>
          {{ coverageReady ? '分钟数据就绪' : '等待覆盖检查' }}
        </div>
      </div>

      <div class="configuration-grid">
        <div class="control-surface">
          <div class="field-grid">
            <label class="field field--wide">
              <span>回测区间</span>
              <el-date-picker
                v-model="dateRange"
                type="daterange"
                value-format="YYYY-MM-DD"
                range-separator="至"
                start-placeholder="开始日期"
                end-placeholder="结束日期"
                :clearable="false"
              />
            </label>
            <label class="field">
              <span>初始资金</span>
              <el-input-number v-model="form.initialCapital" :min="100000" :step="100000" controls-position="right" />
            </label>
            <label class="field">
              <span>现金预留</span>
              <el-input-number v-model="form.cashBuffer" :min="0.1" :max="0.8" :step="0.05" :precision="2" controls-position="right" />
            </label>
            <label class="field">
              <span>利通电子底仓</span>
              <el-input-number v-model="form.litongQuantity" :min="0" :step="100" controls-position="right" />
            </label>
            <label class="field">
              <span>澜起科技底仓</span>
              <el-input-number v-model="form.montageQuantity" :min="0" :step="100" controls-position="right" />
            </label>
          </div>

          <div class="parameter-rail">
            <label class="slider-field">
              <span><b>单次做 T 比例</b><strong>{{ tradeFractionPercent }}%</strong></span>
              <el-slider v-model="tradeFractionPercent" :min="10" :max="30" :step="5" :show-tooltip="false" />
            </label>
            <label class="slider-field">
              <span><b>入场 Z-Score</b><strong>{{ entryZValue.toFixed(2) }}</strong></span>
              <el-slider
                v-model="form.entryZ"
                :min="1"
                :max="Math.min(2.95, maxEntryZValue - 0.05)"
                :step="0.05"
                :show-tooltip="false"
              />
            </label>
            <label class="slider-field">
              <span><b>每日最多 T 对</b><strong>{{ form.maxPairs }}</strong></span>
              <el-slider v-model="form.maxPairs" :min="1" :max="4" :step="1" :show-tooltip="false" />
            </label>
          </div>

          <div class="v2-gate-panel">
            <div class="v2-control-grid">
              <label class="compact-field">
                <span>极端 Z 上限</span>
                <el-input-number
                  v-model="form.maxEntryZ"
                  :min="entryZValue + 0.05"
                  :max="2.95"
                  :step="0.05"
                  :precision="2"
                  controls-position="right"
                  size="small"
                />
              </label>
              <label class="compact-field">
                <span>波动窗口 <small>分钟</small></span>
                <el-input-number
                  v-model="form.realizedVolWindow"
                  :min="5"
                  :max="60"
                  :step="1"
                  controls-position="right"
                  size="small"
                />
              </label>
              <label class="compact-field">
                <span>最低实现波动 <small>bp</small></span>
                <el-input-number
                  v-model="form.minRealizedVolBps"
                  :min="0"
                  :max="100"
                  :step="1"
                  :precision="1"
                  controls-position="right"
                  size="small"
                />
              </label>
            </div>
            <div class="v2-risk-summary" data-testid="v2-risk-summary">
              <div>
                <span>v2 候选门控</span>
                <strong>样本外未晋级</strong>
              </div>
              <p>
                Z {{ entryZValue.toFixed(2) }}–{{ maxEntryZValue.toFixed(2) }} ·
                {{ minRealizedVolBpsValue > 0
                  ? `${realizedVolWindowValue} 分钟波动 ≥ ${minRealizedVolBpsValue.toFixed(0)} bp`
                  : '最低波动门控关闭' }} ·
                10:00–10:29 入场
              </p>
            </div>
          </div>

          <div class="command-row">
            <el-button :loading="coverageLoading" @click="loadCoverage">检查数据</el-button>
            <el-button
              type="primary"
              data-testid="run-backtest"
              :loading="backtestLoading"
              :disabled="!capabilities"
              @click="runBacktest"
            >
              运行分钟回测
            </el-button>
          </div>
        </div>

        <aside class="coverage-surface">
          <div class="surface-head">
            <div>
              <span>LOCAL PARQUET</span>
              <h4>数据覆盖</h4>
            </div>
            <small>{{ dateRange[0] }} / {{ dateRange[1] }}</small>
          </div>
          <div v-if="coverage?.coverage.length" class="coverage-list">
            <div v-for="item in coverage.coverage" :key="item.symbol" class="coverage-row">
              <div>
                <strong>{{ item.name }}</strong>
                <span>{{ item.symbol }}</span>
              </div>
              <div>
                <strong>{{ item.trade_days.toLocaleString() }}</strong>
                <span>交易日</span>
              </div>
              <div>
                <strong>{{ compactNumber(item.bars) }}</strong>
                <span>分钟线</span>
              </div>
            </div>
          </div>
          <el-empty v-else :image-size="54" description="尚未检查当前区间" />
          <div class="guardrail-list">
            <span><i></i>信号后一根分钟线成交</span>
            <span><i></i>当日买入不增加可卖库存</span>
            <span><i></i>费用与滑点进入收益核算</span>
            <span><i></i>持仓未恢复即标记失败</span>
          </div>
        </aside>
      </div>
    </section>

    <section class="desk-band result-band">
      <div class="band-heading">
        <div>
          <span>02 / REPLAY</span>
          <h3>增量收益</h3>
        </div>
        <span v-if="backtestResult" class="period-label">
          {{ backtestResult.period.trade_days }} 日 · {{ compactNumber(backtestResult.period.bars) }} bars
        </span>
      </div>

      <template v-if="backtestResult">
        <el-alert
          v-if="limitPriceWarning"
          class="limit-price-warning"
          data-testid="limit-price-warning"
          type="warning"
          title="涨跌停价缺失"
          :description="limitPriceWarning"
          show-icon
          :closable="false"
        />
        <div class="metric-strip">
          <div class="primary-metric">
            <span>相对持有增量</span>
            <strong :class="metricTone(backtestResult.metrics.incremental_pnl)">
              {{ formatCurrency(backtestResult.metrics.incremental_pnl, true) }}
            </strong>
            <small>{{ formatPercent(backtestResult.metrics.incremental_return, true) }}</small>
          </div>
          <div>
            <span>每股降本</span>
            <strong>{{ formatCurrency(backtestResult.metrics.cost_reduction_per_share) }}</strong>
            <small>按期初底仓</small>
          </div>
          <div>
            <span>完整 T 对</span>
            <strong>{{ backtestResult.metrics.completed_pairs }}</strong>
            <small>{{ backtestResult.metrics.entry_count }} 次入场</small>
          </div>
          <div>
            <span>底仓恢复率</span>
            <strong :class="backtestResult.metrics.restoration_rate === 1 ? 'metric-good' : 'metric-bad'">
              {{ formatPercent(backtestResult.metrics.restoration_rate) }}
            </strong>
            <small>{{ backtestResult.metrics.restoration_failures }} 次失败</small>
          </div>
          <div>
            <span>总费用</span>
            <strong>{{ formatCurrency(backtestResult.metrics.total_fees) }}</strong>
            <small>{{ backtestResult.metrics.rejection_count }} 次拒单</small>
          </div>
        </div>

        <div class="analytics-grid">
          <div class="chart-surface">
            <div class="chart-head">
              <h4>策略权益 / 被动持有</h4>
              <div class="chart-legend"><span class="strategy-line">策略</span><span class="passive-line">持有</span></div>
            </div>
            <div ref="equityChartRef" class="equity-chart" aria-label="策略权益和被动持有对比图"></div>
          </div>
          <div class="symbol-ledger">
            <div v-for="item in backtestResult.symbol_summaries" :key="item.symbol" class="symbol-result">
              <div>
                <span>{{ item.symbol }}</span>
                <strong>{{ item.name }}</strong>
              </div>
              <dl>
                <div><dt>净收益</dt><dd :class="metricTone(item.net_pnl)">{{ formatCurrency(item.net_pnl, true) }}</dd></div>
                <div><dt>完整 T 对</dt><dd>{{ item.completed_pairs }}</dd></div>
                <div><dt>期末底仓</dt><dd>{{ item.ending_quantity.toLocaleString() }}</dd></div>
              </dl>
            </div>
          </div>
        </div>

        <el-tabs v-model="resultTab" class="ledger-tabs">
          <el-tab-pane label="成交账本" name="trades">
            <el-table :data="backtestResult.trades" height="340" empty-text="当前参数未产生可成交信号">
              <el-table-column prop="fill_at" label="成交时间" width="168" />
              <el-table-column prop="name" label="股票" min-width="104" />
              <el-table-column label="方向" width="74">
                <template #default="scope"><span>{{ directionLabel(scope.row.direction) }}</span></template>
              </el-table-column>
              <el-table-column label="买卖" width="70">
                <template #default="scope"><span :class="scope.row.side === 'BUY' ? 'trade-buy' : 'trade-sell'">{{ tradeSideLabel(scope.row.side) }}</span></template>
              </el-table-column>
              <el-table-column prop="quantity" label="数量" width="90" align="right" />
              <el-table-column prop="fill_price" label="成交价" width="100" align="right" />
              <el-table-column prop="fees" label="费用" width="90" align="right" />
              <el-table-column label="净收益" width="116" align="right">
                <template #default="scope"><span :class="metricTone(scope.row.net_pnl)">{{ formatCurrency(scope.row.net_pnl, true) }}</span></template>
              </el-table-column>
              <el-table-column label="原因" min-width="132">
                <template #default="scope">{{ reasonLabel(scope.row.reason) }}</template>
              </el-table-column>
            </el-table>
          </el-tab-pane>
          <el-tab-pane :label="`拒单 ${backtestResult.rejections.length}`" name="rejections">
            <el-table :data="backtestResult.rejections" height="340" empty-text="没有拒单">
              <el-table-column prop="attempted_at" label="尝试时间" width="168" />
              <el-table-column prop="symbol" label="股票" width="110" />
              <el-table-column prop="side" label="方向" width="80" />
              <el-table-column prop="quantity" label="数量" width="90" align="right" />
              <el-table-column label="拒单原因" min-width="160">
                <template #default="scope">{{ reasonLabel(scope.row.reason) }}</template>
              </el-table-column>
            </el-table>
          </el-tab-pane>
        </el-tabs>
      </template>
      <div v-else class="result-empty">
        <div class="empty-axis"><span></span><span></span><span></span><span></span></div>
        <strong>等待分钟回放</strong>
        <p>选择区间并运行回测后显示相对持有底仓的增量结果</p>
      </div>
    </section>

    <section class="desk-band paper-band">
      <div class="band-heading">
        <div>
          <span>03 / PAPER</span>
          <h3>持久化模拟盘</h3>
        </div>
        <div v-if="paperSession" class="session-identity">
          <span :class="paperSession.status === 'RUNNING' ? 'session-running' : ''">{{ paperSession.status }}</span>
          <code>{{ paperSession.session_id }}</code>
        </div>
      </div>

      <div class="paper-toolbar">
        <el-segmented v-model="accountSource" :options="accountSourceOptions" :disabled="Boolean(paperSession)" />
        <div class="paper-actions">
          <el-button
            v-if="!paperSession"
            type="primary"
            data-testid="start-paper"
            :loading="paperLoading"
            @click="startPaper"
          >
            启动模拟会话
          </el-button>
          <template v-else>
            <el-button :loading="paperLoading" @click="refreshPaper">刷新</el-button>
            <el-button v-if="paperSession.status === 'RUNNING' && !paperSession.runner_active" type="primary" :loading="paperLoading" @click="evaluatePaper">评估当前分钟</el-button>
            <el-button v-if="paperSession.status === 'RUNNING' && !paperSession.runner_active" :loading="paperLoading" @click="startRunner">启动 Runner</el-button>
            <el-button v-if="paperSession.status === 'RUNNING' && paperSession.runner_active" type="warning" :loading="paperLoading" @click="stopRunner">停止 Runner</el-button>
            <el-button v-if="paperSession.status === 'RUNNING'" :loading="paperLoading" @click="stopPaper">停止会话</el-button>
            <el-button v-else :loading="paperLoading" @click="resetPaper">重置账本</el-button>
          </template>
        </div>
      </div>

      <div v-if="accountSource === 'manual' && !paperSession" class="manual-account-grid">
        <label class="field"><span>模拟现金</span><el-input-number v-model="manual.cash" :min="0" :step="100000" /></label>
        <label class="field"><span>利通底仓 / 可卖</span><div><el-input-number v-model="manual.litongQuantity" :min="0" :step="100" /><el-input-number v-model="manual.litongAvailable" :min="0" :step="100" /></div></label>
        <label class="field"><span>澜起底仓 / 可卖</span><div><el-input-number v-model="manual.montageQuantity" :min="0" :step="100" /><el-input-number v-model="manual.montageAvailable" :min="0" :step="100" /></div></label>
      </div>

      <template v-if="paperSession">
        <div class="paper-state-grid">
          <div v-for="state in paperStates" :key="state.symbol" class="paper-state-row">
            <div class="paper-symbol"><span>{{ state.symbol }}</span><strong>{{ symbolName(state.symbol) }}</strong></div>
            <div><span>状态</span><strong :class="state.state.includes('OPEN') || state.state === 'FORCE_RESTORE' ? 'metric-bad' : 'metric-good'">{{ stateLabel(state.state) }}</strong></div>
            <div><span>当前 / 底仓</span><strong>{{ state.current_quantity.toLocaleString() }} / {{ state.opening_quantity.toLocaleString() }}</strong></div>
            <div><span>剩余可卖</span><strong>{{ state.sellable_remaining.toLocaleString() }}</strong></div>
            <div><span>已完成</span><strong>{{ state.completed_pairs }} 对</strong></div>
            <div><span>模拟净收益</span><strong :class="metricTone(state.realized_net_pnl)">{{ formatCurrency(state.realized_net_pnl, true) }}</strong></div>
          </div>
        </div>
        <el-table :data="paperTrades" height="300" empty-text="模拟会话尚无成交">
          <el-table-column prop="fill_at" label="成交时间" width="168" />
          <el-table-column prop="name" label="股票" min-width="100" />
          <el-table-column label="方向" width="74"><template #default="scope">{{ directionLabel(scope.row.direction) }}</template></el-table-column>
          <el-table-column label="买卖" width="70"><template #default="scope"><span :class="scope.row.side === 'BUY' ? 'trade-buy' : 'trade-sell'">{{ tradeSideLabel(scope.row.side) }}</span></template></el-table-column>
          <el-table-column prop="quantity" label="数量" width="90" align="right" />
          <el-table-column prop="fill_price" label="模拟成交价" width="120" align="right" />
          <el-table-column label="净收益" width="116" align="right"><template #default="scope"><span :class="metricTone(scope.row.net_pnl)">{{ formatCurrency(scope.row.net_pnl, true) }}</span></template></el-table-column>
          <el-table-column label="状态" width="90"><template #default><span class="paper-only">SIM</span></template></el-table-column>
        </el-table>
      </template>
      <div v-else class="paper-empty">
        <span>QMT 账户只读快照 / 手工底仓</span>
        <strong>模拟成交写入独立账本</strong>
        <small>真实下单不可用</small>
      </div>
    </section>
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from 'vue'

import {
  intradayTApi,
  type IntradayTBacktestResult,
  type IntradayTCapabilities,
  type IntradayTCoverage,
  type IntradayTPaperSession,
  type IntradayTPaperSymbolState,
  type IntradayTStrategyParams,
  type IntradayTSymbol,
  type IntradayTTrade,
} from '@/api/intradayT'
import { init, type ECharts } from '@/lib/echarts'
import {
  buildEquitySeries,
  directionLabel,
  formatCurrency,
  formatPercent,
  marketDateString,
  reasonLabel,
  stateLabel,
  tradeSideLabel,
} from './model'

const today = new Date()
const endDate = marketDateString(today)
const startDate = `${today.getFullYear()}-01-01`

const initialLoading = ref(true)
const coverageLoading = ref(false)
const backtestLoading = ref(false)
const paperLoading = ref(false)
const pageError = ref('')
const capabilities = ref<IntradayTCapabilities | null>(null)
const coverage = ref<IntradayTCoverage | null>(null)
const backtestResult = ref<IntradayTBacktestResult | null>(null)
const paperSession = ref<IntradayTPaperSession | null>(null)
const paperTrades = ref<IntradayTTrade[]>([])
const dateRange = ref<[string, string]>([startDate, endDate])
const resultTab = ref('trades')
const accountSource = ref<'qmt' | 'manual'>('qmt')
const accountSourceOptions = [
  { label: 'QMT 账户', value: 'qmt' },
  { label: '手工底仓', value: 'manual' },
]

const SAFE_DEFAULTS = {
  initialCapital: 1_000_000,
  cashBuffer: 0.3,
  maxTradeFraction: 0.25,
  entryZ: 1.75,
  maxEntryZ: 2.4,
  exitZ: 0.25,
  realizedVolWindow: 10,
  minRealizedVolBps: 0,
  maxPairs: 1,
  cooldownMinutes: 20,
} as const

const form = reactive({
  initialCapital: 1_000_000,
  cashBuffer: 0.3,
  litongQuantity: 0,
  montageQuantity: 0,
  maxTradeFraction: 0.25,
  entryZ: 1.75,
  maxEntryZ: 2.4,
  exitZ: 0.25,
  realizedVolWindow: 10,
  minRealizedVolBps: 0,
  maxPairs: 1,
  cooldownMinutes: 20,
})

const manual = reactive({
  cash: 300_000,
  litongQuantity: 2_000,
  litongAvailable: 2_000,
  montageQuantity: 1_000,
  montageAvailable: 1_000,
})

const equityChartRef = ref<HTMLElement | null>(null)
let equityChart: ECharts | null = null

const symbolCards = computed(() => capabilities.value?.symbols || [
  { symbol: '603629.SH' as IntradayTSymbol, name: '利通电子', board: 'MAIN' as const },
  { symbol: '688008.SH' as IntradayTSymbol, name: '澜起科技', board: 'STAR' as const },
])
const coverageReady = computed(() => Boolean(
  coverage.value?.coverage.length && coverage.value.coverage.every(item => item.bars > 0),
))
const entryZValue = computed(() => finiteNumber(form.entryZ, SAFE_DEFAULTS.entryZ))
const maxEntryZValue = computed(() => {
  const configured = finiteNumber(form.maxEntryZ, SAFE_DEFAULTS.maxEntryZ)
  return configured > entryZValue.value
    ? configured
    : Math.min(2.95, entryZValue.value + 0.05)
})
const realizedVolWindowValue = computed(() => positiveInteger(
  form.realizedVolWindow,
  SAFE_DEFAULTS.realizedVolWindow,
))
const minRealizedVolBpsValue = computed(() => nonNegativeNumber(
  form.minRealizedVolBps,
  SAFE_DEFAULTS.minRealizedVolBps,
))
const tradeFractionPercent = computed({
  get: () => Math.round(finiteNumber(form.maxTradeFraction, SAFE_DEFAULTS.maxTradeFraction) * 100),
  set: value => { form.maxTradeFraction = finiteNumber(value, 25) / 100 },
})
const paperStates = computed<IntradayTPaperSymbolState[]>(() =>
  Object.values(paperSession.value?.states || {}).filter(
    (value): value is IntradayTPaperSymbolState => Boolean(value),
  ),
)
const limitPriceWarning = computed(() => {
  const quality = backtestResult.value?.data_quality?.limit_prices
  if (!quality?.missing_symbol_days.length) return ''
  const preview = quality.missing_symbol_days
    .slice(0, 6)
    .map(item => item.replace('|', ' · '))
    .join('、')
  const remainder = quality.missing_symbol_days.length - 6
  const suffix = remainder > 0 ? `，另有 ${remainder} 个股票日` : ''
  return `已启用 ${quality.mode.replace('_', '-')} 安全模式，缺失日禁止入场：${preview}${suffix}`
})

function finiteNumber(value: unknown, fallback: number) {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback
}

function nonNegativeNumber(value: unknown, fallback: number) {
  return Math.max(0, finiteNumber(value, fallback))
}

function positiveInteger(value: unknown, fallback: number) {
  return Math.max(1, Math.round(finiteNumber(value, fallback)))
}

function nonNegativeInteger(value: unknown, fallback: number) {
  return Math.max(0, Math.round(finiteNumber(value, fallback)))
}

function strategyPayload(): IntradayTStrategyParams {
  return {
    entry_z: entryZValue.value,
    max_entry_z: maxEntryZValue.value,
    exit_z: nonNegativeNumber(form.exitZ, SAFE_DEFAULTS.exitZ),
    realized_vol_window: realizedVolWindowValue.value,
    min_realized_vol_bps: minRealizedVolBpsValue.value,
    max_trade_fraction: finiteNumber(form.maxTradeFraction, SAFE_DEFAULTS.maxTradeFraction),
    max_pairs_per_day: positiveInteger(form.maxPairs, SAFE_DEFAULTS.maxPairs),
    cooldown_minutes: nonNegativeInteger(form.cooldownMinutes, SAFE_DEFAULTS.cooldownMinutes),
  }
}

async function loadInitial() {
  initialLoading.value = true
  try {
    capabilities.value = await intradayTApi.capabilities()
    const defaults = capabilities.value.defaults
    form.initialCapital = finiteNumber(defaults.initial_capital, SAFE_DEFAULTS.initialCapital)
    form.cashBuffer = finiteNumber(defaults.cash_buffer_fraction, SAFE_DEFAULTS.cashBuffer)
    form.maxTradeFraction = finiteNumber(
      defaults.strategy.max_trade_fraction,
      SAFE_DEFAULTS.maxTradeFraction,
    )
    form.entryZ = finiteNumber(defaults.strategy.entry_z, SAFE_DEFAULTS.entryZ)
    form.maxEntryZ = finiteNumber(defaults.strategy.max_entry_z, SAFE_DEFAULTS.maxEntryZ)
    form.exitZ = finiteNumber(defaults.strategy.exit_z, SAFE_DEFAULTS.exitZ)
    form.realizedVolWindow = positiveInteger(
      defaults.strategy.realized_vol_window,
      SAFE_DEFAULTS.realizedVolWindow,
    )
    form.minRealizedVolBps = nonNegativeNumber(
      defaults.strategy.min_realized_vol_bps,
      SAFE_DEFAULTS.minRealizedVolBps,
    )
    form.maxPairs = positiveInteger(
      defaults.strategy.max_pairs_per_day,
      SAFE_DEFAULTS.maxPairs,
    )
    form.cooldownMinutes = nonNegativeInteger(
      defaults.strategy.cooldown_minutes,
      SAFE_DEFAULTS.cooldownMinutes,
    )
    await Promise.all([loadCoverage(), recoverPaperSession()])
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    initialLoading.value = false
  }
}

async function loadCoverage() {
  coverageLoading.value = true
  const requestedRange = [...dateRange.value] as [string, string]
  try {
    const result = await intradayTApi.coverage(
      ['603629.SH', '688008.SH'],
      requestedRange[0],
      requestedRange[1],
    )
    if (requestedRange[0] === dateRange.value[0] && requestedRange[1] === dateRange.value[1]) {
      coverage.value = result
    }
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    coverageLoading.value = false
  }
}

async function runBacktest() {
  backtestLoading.value = true
  pageError.value = ''
  const requestedRange = [...dateRange.value] as [string, string]
  const baseQuantities: Partial<Record<IntradayTSymbol, number>> = {}
  const litongQuantity = nonNegativeInteger(form.litongQuantity, 0)
  const montageQuantity = nonNegativeInteger(form.montageQuantity, 0)
  if (litongQuantity > 0) baseQuantities['603629.SH'] = litongQuantity
  if (montageQuantity > 0) baseQuantities['688008.SH'] = montageQuantity
  try {
    const result = await intradayTApi.runBacktest({
      symbols: ['603629.SH', '688008.SH'],
      start_date: requestedRange[0],
      end_date: requestedRange[1],
      initial_capital: Math.max(100_000, finiteNumber(form.initialCapital, SAFE_DEFAULTS.initialCapital)),
      base_quantities: baseQuantities,
      cash_buffer_fraction: finiteNumber(form.cashBuffer, SAFE_DEFAULTS.cashBuffer),
      max_bar_volume_fraction: capabilities.value?.defaults.max_bar_volume_fraction || 0.05,
      strategy: strategyPayload(),
      cost: capabilities.value?.defaults.cost,
    })
    if (requestedRange[0] !== dateRange.value[0] || requestedRange[1] !== dateRange.value[1]) return
    backtestResult.value = result
    await nextTick()
    renderEquityChart()
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    backtestLoading.value = false
  }
}

async function recoverPaperSession() {
  try {
    paperSession.value = await intradayTApi.paperStatus()
    await loadPaperTrades()
  } catch {
    paperSession.value = null
  }
}

async function startPaper() {
  paperLoading.value = true
  pageError.value = ''
  try {
    const manualLitongQuantity = nonNegativeInteger(manual.litongQuantity, 0)
    const manualMontageQuantity = nonNegativeInteger(manual.montageQuantity, 0)
    const payload = accountSource.value === 'manual'
      ? {
          strategy: strategyPayload(),
          manual_account: {
            cash: nonNegativeNumber(manual.cash, 300_000),
            positions: {
              '603629.SH': {
                quantity: manualLitongQuantity,
                available: Math.min(
                  manualLitongQuantity,
                  nonNegativeInteger(manual.litongAvailable, 0),
                ),
                avg_cost: 0,
              },
              '688008.SH': {
                quantity: manualMontageQuantity,
                available: Math.min(
                  manualMontageQuantity,
                  nonNegativeInteger(manual.montageAvailable, 0),
                ),
                avg_cost: 0,
              },
            },
          },
        }
      : { strategy: strategyPayload() }
    paperSession.value = await intradayTApi.startPaper(payload)
    paperTrades.value = []
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    paperLoading.value = false
  }
}

async function refreshPaper() {
  if (!paperSession.value) return
  paperLoading.value = true
  try {
    paperSession.value = await intradayTApi.paperStatus(paperSession.value.session_id)
    await loadPaperTrades()
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    paperLoading.value = false
  }
}

async function evaluatePaper() {
  if (!paperSession.value) return
  paperLoading.value = true
  try {
    paperSession.value = await intradayTApi.evaluatePaper(paperSession.value.session_id)
    await loadPaperTrades()
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    paperLoading.value = false
  }
}

async function stopPaper() {
  if (!paperSession.value) return
  paperLoading.value = true
  try {
    paperSession.value = await intradayTApi.stopPaper(paperSession.value.session_id)
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    paperLoading.value = false
  }
}

async function startRunner() {
  if (!paperSession.value) return
  paperLoading.value = true
  try {
    paperSession.value = await intradayTApi.startPaperRunner(paperSession.value.session_id, 30)
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    paperLoading.value = false
  }
}

async function stopRunner() {
  if (!paperSession.value) return
  paperLoading.value = true
  try {
    paperSession.value = await intradayTApi.stopPaperRunner(paperSession.value.session_id)
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    paperLoading.value = false
  }
}

async function resetPaper() {
  if (!paperSession.value) return
  paperLoading.value = true
  try {
    paperSession.value = await intradayTApi.resetPaper(paperSession.value.session_id)
    paperTrades.value = []
  } catch (error) {
    pageError.value = errorMessage(error)
  } finally {
    paperLoading.value = false
  }
}

async function loadPaperTrades() {
  if (!paperSession.value) return
  paperTrades.value = await intradayTApi.paperTrades(paperSession.value.session_id)
}

function renderEquityChart() {
  if (!equityChartRef.value || !backtestResult.value) return
  equityChart?.dispose()
  equityChart = init(equityChartRef.value)
  const series = buildEquitySeries(backtestResult.value.equity_curve)
  equityChart.setOption({
    animationDuration: 380,
    color: ['#a83232', '#75827d'],
    tooltip: { trigger: 'axis' },
    grid: { left: 58, right: 18, top: 22, bottom: 34 },
    xAxis: {
      type: 'category',
      data: series.dates,
      boundaryGap: false,
      axisLine: { lineStyle: { color: '#cfd6d2' } },
      axisLabel: { color: '#68756f', fontFamily: 'JetBrains Mono, Consolas, monospace' },
    },
    yAxis: {
      type: 'value',
      scale: true,
      splitLine: { lineStyle: { color: '#e7ebe8', type: 'dashed' } },
      axisLabel: { color: '#68756f', formatter: (value: number) => compactNumber(value) },
    },
    series: [
      { name: '策略权益', type: 'line', data: series.strategy, showSymbol: false, lineStyle: { width: 2.5 } },
      { name: '被动持有', type: 'line', data: series.passive, showSymbol: false, lineStyle: { width: 1.5, type: 'dashed' } },
    ],
  })
}

function compactNumber(value: number) {
  return new Intl.NumberFormat('zh-CN', { notation: 'compact', maximumFractionDigits: 1 }).format(value)
}

function metricTone(value: number) {
  return value > 0 ? 'metric-up' : value < 0 ? 'metric-down' : ''
}

function symbolName(symbol: IntradayTSymbol) {
  return symbolCards.value.find(item => item.symbol === symbol)?.name || symbol
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : '请求失败'
}

function resizeChart() {
  equityChart?.resize()
}

watch(dateRange, () => {
  coverage.value = null
  backtestResult.value = null
  equityChart?.dispose()
  equityChart = null
})

onMounted(() => {
  window.addEventListener('resize', resizeChart)
  void loadInitial()
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', resizeChart)
  equityChart?.dispose()
})
</script>

<style scoped>
.intraday-page {
  --t-ink: #18231f;
  --t-muted: #68756f;
  --t-line: #d8dfdb;
  --t-soft: #f3f6f4;
  --t-paper: #fffefa;
  --t-green: #1b3d32;
  --t-red: #a83232;
  min-width: 0;
  color: var(--t-ink);
  background:
    repeating-linear-gradient(90deg, transparent 0, transparent 79px, rgba(27, 61, 50, 0.025) 80px),
    linear-gradient(180deg, rgba(239, 244, 241, 0.72), rgba(255, 254, 250, 0) 360px);
  font-family: var(--font-ui);
}

.desk-header {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 28px;
  padding: 8px 0 24px;
}

.desk-title { min-width: 260px; }
.section-kicker,
.band-heading > div > span,
.surface-head span {
  color: var(--t-green);
  font-family: var(--font-data);
  font-size: 11px;
  font-weight: 700;
}
.title-line { display: flex; align-items: center; gap: 12px; margin: 4px 0; }
.title-line h2 { margin: 0; font-family: var(--font-display); font-size: 30px; line-height: 1.1; letter-spacing: 0; }
.desk-title p { margin: 7px 0 0; color: var(--t-muted); font-size: 14px; }
.paper-seal,
.paper-only {
  border: 1px solid rgba(168, 50, 50, 0.45);
  color: var(--t-red);
  font-family: var(--font-data);
  font-size: 10px;
  font-weight: 800;
  padding: 3px 7px;
}

.universe-tape { display: grid; grid-template-columns: repeat(3, minmax(150px, 1fr)); border: 1px solid var(--t-line); background: rgba(255, 254, 250, 0.82); }
.ticker-cell { min-width: 0; padding: 11px 16px; border-right: 1px solid var(--t-line); }
.ticker-cell:last-child { border-right: 0; }
.ticker-cell span { display: block; color: var(--t-muted); font-family: var(--font-data); font-size: 10px; }
.ticker-cell strong { display: block; margin-top: 2px; font-family: var(--font-data); font-size: 16px; }
.ticker-cell small { display: block; margin-top: 4px; color: var(--t-muted); font-size: 11px; }
.ticker-cell--guardrail { background: #edf3ef; }
.page-alert { margin-bottom: 16px; }
.limit-price-warning { margin-bottom: 16px; }

.desk-band { padding: 24px 0 28px; border-top: 1px solid var(--t-line); }
.band-heading { display: flex; justify-content: space-between; align-items: end; gap: 16px; margin-bottom: 18px; }
.band-heading h3 { margin: 3px 0 0; font-family: var(--font-display); font-size: 20px; letter-spacing: 0; }
.data-state, .period-label { color: var(--t-muted); font-size: 12px; }
.state-dot { display: inline-block; width: 7px; height: 7px; margin-right: 7px; border-radius: 50%; background: #aeb8b3; }
.state-dot--ready { background: #2d6a4f; box-shadow: 0 0 0 3px rgba(45, 106, 79, 0.12); }

.configuration-grid { display: grid; grid-template-columns: minmax(0, 1.65fr) minmax(300px, 0.75fr); gap: 16px; }
.control-surface, .coverage-surface, .chart-surface, .symbol-result {
  border: 1px solid var(--t-line);
  border-radius: 6px;
  background: rgba(255, 254, 250, 0.92);
}
.control-surface { padding: 18px; }
.field-grid { display: grid; grid-template-columns: 1.4fr repeat(2, minmax(150px, 0.8fr)); gap: 13px; }
.field { display: flex; flex-direction: column; gap: 7px; min-width: 0; color: var(--t-muted); font-size: 12px; }
.field--wide { grid-row: span 2; }
.field :deep(.el-input-number), .field :deep(.el-date-editor) { width: 100%; }
.parameter-rail { display: grid; grid-template-columns: repeat(3, 1fr); gap: 18px; margin-top: 18px; padding-top: 16px; border-top: 1px solid var(--t-line); }
.slider-field > span { display: flex; justify-content: space-between; gap: 8px; color: var(--t-muted); font-size: 12px; }
.slider-field b { color: var(--t-ink); font-weight: 600; }
.slider-field strong { color: var(--t-green); font-family: var(--font-data); }
.v2-gate-panel { display: grid; grid-template-columns: minmax(0, 1.4fr) minmax(260px, 0.8fr); gap: 18px; margin-top: 17px; padding-top: 16px; border-top: 1px solid var(--t-line); }
.v2-control-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
.compact-field { display: flex; flex-direction: column; gap: 6px; min-width: 0; color: var(--t-muted); font-size: 11px; }
.compact-field > span { display: flex; justify-content: space-between; gap: 6px; }
.compact-field small { color: #8b9691; font-family: var(--font-data); font-size: 9px; }
.compact-field :deep(.el-input-number) { width: 100%; }
.v2-risk-summary { min-width: 0; padding-left: 18px; border-left: 1px solid var(--t-line); }
.v2-risk-summary > div { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.v2-risk-summary span { color: var(--t-green); font-family: var(--font-data); font-size: 10px; font-weight: 750; }
.v2-risk-summary strong { color: var(--t-red); font-size: 10px; font-weight: 700; }
.v2-risk-summary p { margin: 8px 0 0; color: var(--t-muted); font-family: var(--font-data); font-size: 10px; line-height: 1.65; }
.command-row { display: flex; justify-content: flex-end; gap: 8px; margin-top: 15px; }

.coverage-surface { padding: 18px; }
.surface-head { display: flex; justify-content: space-between; align-items: start; gap: 8px; }
.surface-head h4, .chart-head h4 { margin: 3px 0 0; font-size: 14px; }
.surface-head small { color: var(--t-muted); font-family: var(--font-data); font-size: 10px; }
.coverage-list { margin: 14px 0; border-top: 1px solid var(--t-line); }
.coverage-row { display: grid; grid-template-columns: 1.4fr 0.7fr 0.7fr; gap: 8px; padding: 12px 0; border-bottom: 1px solid var(--t-line); }
.coverage-row > div:not(:first-child) { text-align: right; }
.coverage-row strong, .coverage-row span { display: block; }
.coverage-row strong { font-family: var(--font-data); font-size: 13px; }
.coverage-row span { margin-top: 2px; color: var(--t-muted); font-size: 10px; }
.guardrail-list { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 12px; padding-top: 8px; color: var(--t-muted); font-size: 11px; }
.guardrail-list span { display: flex; align-items: center; gap: 7px; }
.guardrail-list i { flex: 0 0 auto; width: 4px; height: 4px; background: var(--t-green); }

.metric-strip { display: grid; grid-template-columns: 1.35fr repeat(4, 1fr); border: 1px solid var(--t-line); background: var(--t-paper); }
.metric-strip > div { min-width: 0; padding: 15px 17px; border-right: 1px solid var(--t-line); }
.metric-strip > div:last-child { border-right: 0; }
.metric-strip span, .metric-strip small { display: block; color: var(--t-muted); font-size: 11px; }
.metric-strip strong { display: block; margin: 7px 0 4px; font-family: var(--font-data); font-size: 18px; }
.metric-strip .primary-metric strong { font-size: 23px; }
.metric-up, .metric-bad, .trade-buy { color: var(--t-red) !important; }
.metric-down, .metric-good, .trade-sell { color: #27704e !important; }
.analytics-grid { display: grid; grid-template-columns: minmax(0, 1.75fr) minmax(260px, 0.65fr); gap: 16px; margin-top: 16px; }
.chart-surface { padding: 16px; }
.chart-head { display: flex; justify-content: space-between; align-items: center; }
.chart-legend { display: flex; gap: 14px; color: var(--t-muted); font-size: 11px; }
.chart-legend span::before { content: ''; display: inline-block; width: 18px; height: 2px; margin-right: 5px; vertical-align: middle; }
.strategy-line::before { background: var(--t-red); }
.passive-line::before { background: #75827d; }
.equity-chart { width: 100%; height: 280px; }
.symbol-ledger { display: grid; gap: 12px; }
.symbol-result { padding: 16px; }
.symbol-result > div span, .symbol-result > div strong { display: block; }
.symbol-result > div span { color: var(--t-muted); font-family: var(--font-data); font-size: 10px; }
.symbol-result > div strong { margin-top: 3px; font-size: 15px; }
.symbol-result dl { margin: 14px 0 0; }
.symbol-result dl > div { display: flex; justify-content: space-between; padding: 8px 0; border-top: 1px solid var(--t-line); }
.symbol-result dt { color: var(--t-muted); font-size: 11px; }
.symbol-result dd { margin: 0; font-family: var(--font-data); font-size: 12px; font-weight: 700; }
.ledger-tabs { margin-top: 18px; }

.result-empty { position: relative; min-height: 270px; display: flex; flex-direction: column; align-items: center; justify-content: center; overflow: hidden; border: 1px dashed var(--t-line); background: rgba(255, 254, 250, 0.6); }
.result-empty strong { z-index: 1; font-size: 15px; }
.result-empty p { z-index: 1; margin: 7px 0 0; color: var(--t-muted); font-size: 12px; }
.empty-axis { position: absolute; inset: 32px; display: flex; flex-direction: column; justify-content: space-between; }
.empty-axis span { border-top: 1px dashed #e2e7e4; }

.session-identity { display: flex; align-items: center; gap: 10px; }
.session-identity > span { color: var(--t-muted); font-family: var(--font-data); font-size: 10px; font-weight: 700; }
.session-identity .session-running { color: #27704e; }
.session-identity code { color: var(--t-muted); font-size: 11px; }
.paper-toolbar { display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 16px; }
.paper-actions { display: flex; gap: 8px; }
.manual-account-grid { display: grid; grid-template-columns: 0.7fr 1fr 1fr; gap: 14px; padding: 16px; margin-bottom: 16px; border: 1px solid var(--t-line); background: var(--t-paper); }
.manual-account-grid .field > div { display: grid; grid-template-columns: 1fr 1fr; gap: 7px; }
.paper-state-grid { display: grid; gap: 8px; margin-bottom: 14px; }
.paper-state-row { display: grid; grid-template-columns: 1.2fr repeat(5, 1fr); align-items: center; gap: 12px; padding: 13px 15px; border: 1px solid var(--t-line); background: var(--t-paper); }
.paper-state-row > div span, .paper-state-row > div strong { display: block; }
.paper-state-row > div span { color: var(--t-muted); font-size: 10px; }
.paper-state-row > div strong { margin-top: 4px; font-family: var(--font-data); font-size: 12px; }
.paper-symbol strong { font-family: var(--font-ui) !important; font-size: 14px !important; }
.paper-empty { display: grid; grid-template-columns: 1fr auto auto; align-items: center; gap: 24px; padding: 24px; border: 1px dashed var(--t-line); color: var(--t-muted); }
.paper-empty strong { color: var(--t-ink); font-size: 14px; }
.paper-empty small { color: var(--t-red); font-family: var(--font-data); }

:deep(.el-button) { border-radius: 4px; font-weight: 650; }
:deep(.el-button--primary) { --el-button-bg-color: var(--t-green); --el-button-border-color: var(--t-green); --el-button-hover-bg-color: #285748; --el-button-hover-border-color: #285748; }
:deep(.el-input__wrapper), :deep(.el-select__wrapper) { border-radius: 4px; }
:deep(.el-slider) { --el-slider-main-bg-color: var(--t-green); }
:deep(.el-tabs__item.is-active) { color: var(--t-green); }
:deep(.el-tabs__active-bar) { background: var(--t-green); }
:deep(.el-table) { --el-table-header-bg-color: #f1f5f2; --el-table-border-color: var(--t-line); font-size: 12px; }

@media (max-width: 1120px) {
  .desk-header { align-items: flex-start; flex-direction: column; }
  .universe-tape { width: 100%; }
  .configuration-grid, .analytics-grid { grid-template-columns: 1fr; }
  .symbol-ledger { grid-template-columns: 1fr 1fr; }
  .metric-strip { grid-template-columns: repeat(3, 1fr); }
  .metric-strip > div { border-bottom: 1px solid var(--t-line); }
  .paper-state-row { grid-template-columns: repeat(3, 1fr); }
  .v2-gate-panel { grid-template-columns: 1fr; }
  .v2-risk-summary { padding: 12px 0 0; border-top: 1px solid var(--t-line); border-left: 0; }
}

@media (max-width: 720px) {
  .intraday-page { padding-inline: 2px; }
  .universe-tape { grid-template-columns: 1fr; }
  .ticker-cell { border-right: 0; border-bottom: 1px solid var(--t-line); }
  .ticker-cell:last-child { border-bottom: 0; }
  .field-grid, .parameter-rail, .manual-account-grid, .v2-control-grid { grid-template-columns: 1fr; }
  .field--wide { grid-row: auto; }
  .guardrail-list { grid-template-columns: 1fr; }
  .metric-strip { grid-template-columns: 1fr 1fr; }
  .metric-strip > div { border-right: 1px solid var(--t-line); }
  .symbol-ledger { grid-template-columns: 1fr; }
  .paper-toolbar { align-items: stretch; flex-direction: column; }
  .paper-actions { flex-wrap: wrap; }
  .paper-state-row { grid-template-columns: 1fr 1fr; }
  .paper-empty { grid-template-columns: 1fr; gap: 8px; }
  .band-heading { align-items: flex-start; flex-direction: column; }
}
</style>

<template>
  <div class="factor-detail" v-loading="loading">
    <div class="detail-header">
      <el-button text @click="$router.back()">返回</el-button>
      <div class="title-block">
        <span class="panel-kicker">FACTOR DETAIL</span>
        <h2>{{ definition?.display_name || factorName }}</h2>
        <div class="meta-line">
          <span>{{ factorName }}</span>
          <span>{{ definition?.source || 'custom' }}</span>
          <span>{{ definition?.category || '未分类' }}</span>
        </div>
      </div>
      <div class="header-actions">
        <el-button :loading="running" @click="runResearch(false)">计算</el-button>
        <el-button type="primary" :loading="running" @click="runResearch(true)">重新计算</el-button>
      </div>
    </div>

    <section class="info-panel">
      <div class="info-card">
        <span class="info-label">因子逻辑</span>
        <p>{{ factorExplanation.logic }}</p>
      </div>
      <div class="info-card info-card--wide">
        <span class="info-label">真实计算公式</span>
        <code>{{ factorExplanation.formula }}</code>
      </div>
      <div class="info-card">
        <span class="info-label">依赖项</span>
        <div class="dependency-list">
          <el-tag v-for="dep in factorExplanation.dependencies" :key="dep" size="small" effect="plain">{{ dep }}</el-tag>
          <span v-if="!factorExplanation.dependencies.length">无显式依赖</span>
        </div>
      </div>
      <div class="info-card info-card--wide">
        <span class="info-label">数据处理流程</span>
        <ol>
          <li v-for="step in factorExplanation.process" :key="step">{{ step }}</li>
        </ol>
      </div>
      <div class="info-card">
        <span class="info-label">来源 / 备注</span>
        <p>{{ factorExplanation.source }}</p>
      </div>
    </section>

    <section class="param-panel">
      <el-form label-width="94px" class="param-form">
        <el-form-item label="股票池">
          <el-select v-model="form.stock_pool_value" filterable>
            <el-option-group label="指数股票池">
              <el-option
                v-for="item in poolEnabledIndexes"
                :key="item.symbol"
                :label="`${item.display_name} ${item.symbol}`"
                :value="item.stock_pool_alias || item.symbol"
              />
            </el-option-group>
            <el-option-group v-if="watchlistGroups.length" label="自选股分组">
              <el-option
                v-for="g in watchlistGroups"
                :key="'detail_wl_'+g.id"
                :label="g.name"
                :value="'watchlist_'+g.id"
              />
            </el-option-group>
          </el-select>
        </el-form-item>
        <el-form-item label="回测区间">
          <div class="date-range">
            <el-date-picker v-model="form.start_date" value-format="YYYY-MM-DD" type="date" />
            <span>至</span>
            <el-date-picker v-model="form.end_date" value-format="YYYY-MM-DD" type="date" />
          </div>
        </el-form-item>
        <el-form-item label="组合构建">
          <el-radio-group v-model="form.portfolio_type">
            <el-radio-button value="long_only">纯多头</el-radio-button>
            <el-radio-button value="long_short_i">多空 I</el-radio-button>
            <el-radio-button value="long_short_ii">多空 II</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="调仓周期">
          <el-radio-group v-model="form.rebalance_period">
            <el-radio-button value="daily">日</el-radio-button>
            <el-radio-button value="weekly">周</el-radio-button>
            <el-radio-button value="monthly">月</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="参数">
          <div class="inline-controls">
            <el-input-number v-model="form.group_count" :min="2" :max="20" />
            <el-select v-model="form.direction" style="width:110px">
              <el-option label="值越大越好" value="desc" />
              <el-option label="值越小越好" value="asc" />
            </el-select>
            <el-input-number v-model="form.fee_rate" :min="0" :max="0.05" :step="0.0005" />
            <el-input-number v-model="form.slippage" :min="0" :max="0.05" :step="0.0005" />
          </div>
        </el-form-item>
        <el-form-item label="处理选项">
          <div class="switch-row">
            <el-checkbox v-model="form.filter_limit_up">过滤涨停</el-checkbox>
            <el-checkbox v-model="form.filter_limit_down">过滤跌停</el-checkbox>
            <el-checkbox v-model="form.industry_neutralization">行业中性化</el-checkbox>
            <el-checkbox v-model="form.standardize">标准化</el-checkbox>
          </div>
        </el-form-item>
      </el-form>
    </section>

    <el-alert
      v-if="prepareMessage"
      :title="prepareMessage"
      type="warning"
      :closable="false"
      show-icon
    />

    <section class="summary-grid">
      <div v-for="item in summaryCards" :key="item.label" class="summary-card">
        <span>{{ item.label }}</span>
        <strong :class="item.valueClass">{{ item.value }}</strong>
      </div>
    </section>

    <section class="chart-grid">
      <div class="panel panel-wide">
        <h3>分位组净值</h3>
        <div ref="navChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>IC 时序</h3>
        <div ref="icChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>行业 IC</h3>
        <div ref="industryChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>换手率</h3>
        <div ref="turnoverChartRef" class="chart"></div>
      </div>
      <div class="panel">
        <h3>信号衰减</h3>
        <div ref="decayChartRef" class="chart"></div>
      </div>
    </section>

    <section class="stock-grid">
      <div class="panel">
        <h3>Top 股票</h3>
        <el-table :data="detail?.top || []" size="small" max-height="360">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="value" label="因子值" align="right">
            <template #default="{ row }">{{ formatNumber(row.value) }}</template>
          </el-table-column>
        </el-table>
      </div>
      <div class="panel">
        <h3>Bottom 股票</h3>
        <el-table :data="detail?.bottom || []" size="small" max-height="360">
          <el-table-column prop="symbol" label="代码" width="120" />
          <el-table-column prop="value" label="因子值" align="right">
            <template #default="{ row }">{{ formatNumber(row.value) }}</template>
          </el-table-column>
        </el-table>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import { ElMessage } from 'element-plus'
import * as echarts from '@/lib/echarts'
import { indexCatalogApi, watchlistApi, type IndexCatalogItem, type WatchlistGroup } from '@/api/data'
import { factorValueApi, type FactorValueDefinition } from '@/api/factorValues'
import { factorResearchRunApi, type FactorResearchRunDetail } from '@/api/factorResearchRuns'

const route = useRoute()
const factorName = computed(() => String(route.params.factorName || ''))
const todayText = new Date().toISOString().slice(0, 10)
const defaultStart = new Date()
defaultStart.setFullYear(defaultStart.getFullYear() - 3)

function queryDate(key: 'start_date' | 'end_date', fallback: string) {
  const value = route.query[key]
  if (typeof value !== 'string') return fallback
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? value : fallback
}

const loading = ref(false)
const running = ref(false)
const prepareMessage = ref('')
const definition = ref<FactorValueDefinition | null>(null)
const detail = ref<FactorResearchRunDetail | null>(null)
const indexCatalog = ref<IndexCatalogItem[]>([])
const watchlistGroups = ref<WatchlistGroup[]>([])
const poolEnabledIndexes = computed(() => indexCatalog.value.filter(item => item.pool_enabled))

const form = reactive({
  stock_pool_value: String(route.query.stock_pool || 'zz500'),
  start_date: queryDate('start_date', defaultStart.toISOString().slice(0, 10)),
  end_date: queryDate('end_date', todayText),
  portfolio_type: String(route.query.portfolio_type || 'long_only') as 'long_only' | 'long_short_i' | 'long_short_ii',
  rebalance_period: 'monthly' as 'daily' | 'weekly' | 'monthly',
  fee_rate: feeRateFromQuery(),
  slippage: route.query.fee_config === 'commission_stamp_slippage' ? 0.001 : 0,
  filter_limit_up: route.query.filter_limit_up !== 'false',
  filter_limit_down: true,
  group_count: 5,
  direction: 'desc' as 'asc' | 'desc',
  industry_neutralization: false,
  standardize: false,
})

const factorExplanation = computed(() => {
  const item = definition.value as (FactorValueDefinition & { formula?: string; expression?: string }) | null
  const factor = factorName.value
  const dependencies = item?.dependencies?.length ? item.dependencies : inferDependencies(factor)
  return {
    logic: inferLogic(item, factor),
    formula: inferFormula(item, factor),
    dependencies,
    process: inferProcess(item, factor),
    source: inferSource(item, factor),
  }
})

const summaryCards = computed(() => {
  const summary = detail.value?.summary || {}
  return [
    { label: 'IC均值', value: formatNumber(summary.ic_mean), valueClass: valueClass(summary.ic_mean) },
    { label: 'ICIR', value: formatNumber(summary.icir), valueClass: valueClass(summary.icir) },
    { label: '|IC| > 0.02', value: formatPercent(summary.abs_ic_gt_002_ratio), valueClass: '' },
    { label: '多空收益', value: formatPercent(summary.long_short_return), valueClass: valueClass(summary.long_short_return) },
    { label: '最大回撤', value: formatPercent(summary.max_drawdown), valueClass: 'negative' },
    { label: '换手率', value: formatPercent(summary.turnover), valueClass: '' },
    { label: '覆盖率', value: formatPercent(summary.coverage_ratio), valueClass: '' },
    { label: '股票数', value: String(summary.symbol_count || '-'), valueClass: '' },
  ]
})

const navChartRef = ref<HTMLElement | null>(null)
const icChartRef = ref<HTMLElement | null>(null)
const industryChartRef = ref<HTMLElement | null>(null)
const turnoverChartRef = ref<HTMLElement | null>(null)
const decayChartRef = ref<HTMLElement | null>(null)
let charts: echarts.ECharts[] = []

function feeRateFromQuery() {
  if (route.query.fee_config === 'commission_stamp' || route.query.fee_config === 'commission_stamp_slippage') return 0.004
  return 0.001
}

function inferLogic(item: FactorValueDefinition | null, factor: string) {
  const alphaLogic = inferAlpha101Logic(factor)
  if (alphaLogic) return alphaLogic
  if (item?.description) return item.description
  if (factor.startsWith('ta_')) return '技术分析因子，基于日线 OHLCV 序列按 TA-Lib 同名函数计算。'
  if (factor.startsWith('alpha101_')) return 'WorldQuant Alpha101 横截面/时序公式因子，本地实现使用 A 股日线面板数据计算。'
  if (factor.startsWith('research_')) return '海外研究文献启发的截面因子，按当前本地可用的行情、财务和行业字段近似实现。'
  return '自定义或平台内置因子。详情页计算优先读取因子值缓存；自定义 DSL/Python 因子可即时计算。'
}

function inferFormula(item: (FactorValueDefinition & { formula?: string; expression?: string }) | null, factor: string) {
  if (item?.formula) return item.formula
  if (item?.expression) return item.expression
  const taFormula = inferTaFormula(factor)
  if (taFormula) return taFormula
  const builtInFormula = builtInFormulaMap[factor]
  if (builtInFormula) return builtInFormula
  const researchFormula = researchFormulaMap[factor]
  if (researchFormula) return researchFormula
  if (factor.startsWith('alpha101_')) {
    const id = factor.slice(-3)
    return `Alpha101 #${id}: 调用 backend/app/services/alpha101_calculator.py 中 alpha_${Number(id)} 对应公式；输入为 open/high/low/close/volume/vwap/return/market_value 面板，算子包括 rank、delta、correlation、decay_linear、ts_rank 等。`
  }
  return item?.description || '未登记公式；如为自定义因子，公式来自 factors.code / parameters.expression。'
}

function inferAlpha101Logic(factor: string) {
  if (!factor.startsWith('alpha101_')) return ''
  if (factor === 'alpha101_002') {
    return 'Alpha101 #002 衡量“成交量变化”和“日内价格强弱”之间的 6 日滚动相关性，并取负值。具体做法是：先计算 log(volume) 的 2 日变化并做当日横截面排名，再计算 (close - open) / open 代表日内收益并做横截面排名，最后对每只股票滚动计算两者 6 日相关系数并乘以 -1。值越高通常表示量能变化与日内强弱越负相关，属于短周期价量背离/反转类信号。'
  }
  return 'Alpha101 因子通常把“横截面排序 rank”和“单只股票滚动时序算子”组合起来。你可以按公式从内向外读：先构造价格、成交量、收益率等基础序列，再在每个交易日做横截面 rank，或在每只股票内部做 rolling correlation/delta/ts_rank，最后得到每日每只股票的截面分值。'
}

const builtInFormulaMap: Record<string, string> = {
  market_cap: 'market_cap = stock_daily_basic.circ_mv 优先；缺失时回退 total_mv，单位万元，按交易日 point-in-time 对齐。',
  market_cap_rank: 'market_cap_rank = rank_asc(market_cap) within selected stock pool；市值越小排名越靠前。',
  is_st: 'is_st = 1 if 股票名称/状态命中 ST、退市、摘牌等过滤条件 else 0。',
  is_paused: 'is_paused = 1 if 指定 timer 分钟线没有可用成交 bar else 0。',
  is_limit_up: 'is_limit_up = 1 if timer_price >= stock_limit_prices.up_limit else 0。',
  is_limit_down: 'is_limit_down = 1 if timer_price <= stock_limit_prices.down_limit else 0。',
  yesterday_limit_up: 'yesterday_limit_up = lag(close >= up_limit, 1)。',
  v4gv: 'V4GV = 平台小市值策略 V4 技术指标，使用日线 OHLC 窗口计算。',
  v4gv_signal: 'v4gv_signal = V4GV 的信号线。',
  macd_positive: 'macd_positive = 1 if MACD(close, 12, 26, 9).dif > dea and dif > 0 else 0。',
  indicator_buy_signal: 'indicator_buy_signal = 1 if v4gv > v4gv_signal and v4gv > 0 and macd_positive == 1 else 0。',
  v4gv_dead_cross: 'v4gv_dead_cross = 1 if v4gv < v4gv_signal and macd_positive == 0 else 0。',
  cum_volume_at_time: 'cum_volume_at_time = sum(minute.volume from market open to configured time)。',
  rolling_max_volume: 'rolling_max_volume = rolling_max(daily.volume * multiplier, window)，并与 timer 累计成交量口径对齐。',
  high_volume_ratio: 'high_volume_ratio = cum_volume_at_time / rolling_max_volume。',
  high_volume_signal: 'high_volume_signal = 1 if high_volume_ratio >= threshold else 0。',
}

const researchFormulaMap: Record<string, string> = {
  research_gross_profitability: 'gross_profitability ≈ gross_margin * revenue / total_assets；本地字段不足时使用可用质量代理。',
  research_asset_growth: 'asset_growth = total_assets / lag(total_assets, 4 quarters) - 1；资产增长越低通常越偏保守。',
  research_accruals: 'accruals ≈ (net_profit - operating_cash_flow proxy) / total_assets；现金流字段不足时使用本地可用应计代理。',
  research_low_beta: 'low_beta = -rolling_beta(stock_return, market_return, 252)。',
  research_idiosyncratic_volatility: 'idiosyncratic_volatility = -std(residual_return versus market_return, 252)。',
  research_residual_momentum: 'residual_momentum = sum(residual_return over 12-1 months)。',
  research_short_reversal: 'short_reversal = -return_5d。',
  research_turnover_liquidity: 'turnover_liquidity = rolling_mean(amount / volume, 20) 或可用流动性代理。',
}

function inferTaFormula(factor: string) {
  if (factor === 'ta_sma_20') return 'SMA20 = SMA(close, timeperiod=20)。'
  if (factor === 'ta_ema_20') return 'EMA20 = EMA(close, timeperiod=20)。'
  if (factor === 'ta_rsi_14') return 'RSI14 = RSI(close, timeperiod=14)。'
  if (factor.startsWith('ta_macd_')) return 'MACD = MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)，拆分输出 DIF / DEA / HIST。'
  if (factor.startsWith('ta_bbands_')) return 'BBANDS = BBANDS(close, timeperiod=20)，拆分输出 upper / middle / lower。'
  if (factor === 'ta_atr_14') return 'ATR14 = ATR(high, low, close, timeperiod=14)。'
  if (factor === 'ta_natr_14') return 'NATR14 = NATR(high, low, close, timeperiod=14)。'
  if (factor === 'ta_obv') return 'OBV = OBV(close, volume)。'
  if (factor === 'ta_ad') return 'AD = AD(high, low, close, volume)。'
  if (factor === 'ta_mfi_14') return 'MFI14 = MFI(high, low, close, volume, timeperiod=14)。'
  if (factor === 'ta_cci_14') return 'CCI14 = CCI(high, low, close, timeperiod=14)。'
  if (factor === 'ta_willr_14') return 'WILLR14 = WILLR(high, low, close, timeperiod=14)。'
  if (factor === 'ta_roc_10') return 'ROC10 = ROC(close, timeperiod=10)。'
  if (factor === 'ta_adx_14') return 'ADX14 = ADX(high, low, close, timeperiod=14)。'
  if (factor === 'ta_aroonosc_14') return 'AROONOSC14 = AROONOSC(high, low, timeperiod=14)。'
  if (factor === 'ta_typprice') return 'TYPPRICE = (high + low + close) / 3。'
  return ''
}

function inferDependencies(factor: string) {
  if (factor.startsWith('ta_')) return ['klines_daily.open', 'klines_daily.high', 'klines_daily.low', 'klines_daily.close', 'klines_daily.volume']
  if (factor.startsWith('alpha101_')) return ['klines_daily.open', 'klines_daily.high', 'klines_daily.low', 'klines_daily.close', 'klines_daily.volume', 'stocks.total_mv']
  if (factor.startsWith('research_')) return ['klines_daily', 'financial_data', 'stocks']
  return []
}

function inferProcess(item: FactorValueDefinition | null, factor: string) {
  const process = [
    '按股票池和日期范围读取因子值缓存 factor_values；若为内置/目录因子且缓存为空，会提示先预计算。',
    '读取同一股票池的 forward return（日线收益矩阵），与因子矩阵按交易日和股票代码取交集。',
    '按配置处理涨跌停过滤、分组数、方向、手续费和滑点；详情页当前不会把缺数据静默当作 0。',
    '计算 IC 序列、ICIR、分位组净值、多空收益、最大回撤、换手率、行业 IC、信号衰减和 Top/Bottom 股票。',
    '相同参数优先读取 factor_research_runs 最近成功结果；点击“重新计算”才创建新 run。',
  ]
  if (item?.source?.includes('ta_lib')) process.unshift('预计算阶段调用 AKQuant TA-Lib 包装函数，按每只股票的历史日线序列滚动生成。')
  if (factor.startsWith('alpha101_')) process.unshift('预计算阶段先构建全市场日线 panel，再执行 Alpha101 对应横截面/时序算子。')
  if (factor.startsWith('research_')) process.unshift('预计算阶段按因子需要合并行情、财务和股票基础信息，字段缺失时保留 NaN。')
  return process
}

function inferSource(item: FactorValueDefinition | null, factor: string) {
  const description = item?.description || ''
  const sourceMatch = description.match(/Source:\\s*(.+)$/i)
  if (sourceMatch) return sourceMatch[1]
  if (factor.startsWith('ta_')) return 'TA-Lib/AKQuant 技术指标实现，当前平台目录源为 catalog.ta_lib。'
  if (factor.startsWith('alpha101_')) return 'WorldQuant 101 Formulaic Alphas；本地实现参考 backend/app/services/alpha101_calculator.py。'
  if (factor.startsWith('research_')) return '海外研究因子目录；源地址写在因子描述或 data_policy 中。'
  return item?.source || '平台自定义/内置因子。'
}

function requestPayload(force = false) {
  return {
    factor_name: factorName.value,
    stock_pool_value: form.stock_pool_value,
    start_date: form.start_date,
    end_date: form.end_date,
    portfolio_type: form.portfolio_type,
    rebalance_period: form.rebalance_period,
    fee_rate: form.fee_rate,
    slippage: form.slippage,
    filter_limit_up: form.filter_limit_up,
    filter_limit_down: form.filter_limit_down,
    group_count: form.group_count,
    direction: form.direction,
    industry_neutralization: form.industry_neutralization,
    standardize: form.standardize,
    force,
  }
}

async function loadCatalogs() {
  const [definitions, indexes, watchlists] = await Promise.all([
    factorValueApi.definitions(),
    indexCatalogApi.list(),
    watchlistApi.getGroups(),
  ])
  definition.value = definitions.find(item => item.name === factorName.value) || null
  indexCatalog.value = indexes
  watchlistGroups.value = watchlists
}

async function loadLatest() {
  prepareMessage.value = ''
  const prepared = await factorResearchRunApi.prepare(requestPayload(false))
  if (prepared.message && !prepared.can_run) prepareMessage.value = prepared.message
  if (!prepared.cache_hit || !prepared.latest_run?.run_id) {
    detail.value = null
    disposeCharts()
    return
  }
  detail.value = await factorResearchRunApi.get(prepared.latest_run.run_id)
  await renderCharts()
}

async function runResearch(force: boolean) {
  running.value = true
  prepareMessage.value = ''
  try {
    const payload = requestPayload(force)
    const prepared = await factorResearchRunApi.prepare(payload)
    if (!prepared.can_run && prepared.message) {
      prepareMessage.value = prepared.message
      return
    }
    detail.value = await factorResearchRunApi.run(payload)
    ElMessage.success(force ? '已重新计算并保存' : '计算完成')
    await renderCharts()
  } finally {
    running.value = false
  }
}

function disposeCharts() {
  charts.forEach(chart => chart.dispose())
  charts = []
}

async function renderCharts() {
  disposeCharts()
  await nextTick()
  if (!detail.value) return
  renderNavChart()
  renderIcChart()
  renderIndustryChart()
  renderTurnoverChart()
  renderDecayChart()
}

function renderNavChart() {
  if (!navChartRef.value || !detail.value?.quantile_nav?.groups) return
  const chart = echarts.init(navChartRef.value)
  const groups = detail.value.quantile_nav.groups
  const firstSeries = Object.values(groups)[0] || []
  chart.setOption({
    tooltip: { trigger: 'axis' },
    legend: { textStyle: { color: '#9ca3af' } },
    grid: { left: 42, right: 20, top: 30, bottom: 28 },
    xAxis: { type: 'category', data: firstSeries.map(item => item.date) },
    yAxis: { type: 'value' },
    series: [
      ...Object.entries(groups).map(([name, data]) => ({
        name,
        type: 'line',
        symbol: 'none',
        data: data.map(item => item.value),
      })),
      {
        name: 'long_short',
        type: 'line',
        symbol: 'none',
        lineStyle: { width: 2.5 },
        data: (detail.value.quantile_nav.long_short || []).map(item => item.value),
      },
    ],
  })
  charts.push(chart)
}

function renderIcChart() {
  if (!icChartRef.value || !detail.value?.ic_series?.length) return
  const chart = echarts.init(icChartRef.value)
  chart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 42, right: 16, top: 18, bottom: 28 },
    xAxis: { type: 'category', data: detail.value.ic_series.map(item => item.date) },
    yAxis: { type: 'value' },
    series: [{ name: 'IC', type: 'line', symbol: 'none', data: detail.value.ic_series.map(item => item.value), color: '#60a5fa' }],
  })
  charts.push(chart)
}

function renderIndustryChart() {
  if (!industryChartRef.value || !detail.value?.industry_ic?.length) return
  const data = detail.value.industry_ic.slice(0, 18)
  const chart = echarts.init(industryChartRef.value)
  chart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 82, right: 12, top: 10, bottom: 20 },
    xAxis: { type: 'value' },
    yAxis: { type: 'category', data: data.map(item => item.industry) },
    series: [{ type: 'bar', data: data.map(item => item.value), color: '#34d399' }],
  })
  charts.push(chart)
}

function renderTurnoverChart() {
  if (!turnoverChartRef.value || !detail.value?.turnover?.length) return
  const chart = echarts.init(turnoverChartRef.value)
  chart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 42, right: 12, top: 18, bottom: 28 },
    xAxis: { type: 'category', data: detail.value.turnover.map(item => item.date) },
    yAxis: { type: 'value' },
    series: [
      { name: '最低分位', type: 'scatter', data: detail.value.turnover.map(item => item.min_quantile), color: '#60a5fa' },
      { name: '最高分位', type: 'scatter', data: detail.value.turnover.map(item => item.max_quantile), color: '#f97316' },
    ],
  })
  charts.push(chart)
}

function renderDecayChart() {
  if (!decayChartRef.value || !detail.value?.signal_decay?.length) return
  const chart = echarts.init(decayChartRef.value)
  chart.setOption({
    tooltip: { trigger: 'axis' },
    grid: { left: 42, right: 12, top: 18, bottom: 28 },
    xAxis: { type: 'category', data: detail.value.signal_decay.map(item => `lag${item.lag}`) },
    yAxis: { type: 'value' },
    series: [
      { name: '最低分位', type: 'bar', data: detail.value.signal_decay.map(item => item.min_quantile), color: '#a78bfa' },
      { name: '最高分位', type: 'bar', data: detail.value.signal_decay.map(item => item.max_quantile), color: '#38bdf8' },
    ],
  })
  charts.push(chart)
}

function formatNumber(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return Number(value).toFixed(4)
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return `${(Number(value) * 100).toFixed(2)}%`
}

function valueClass(value?: number | null) {
  if (value === null || value === undefined) return ''
  return Number(value) >= 0 ? 'positive' : 'negative'
}

watch(() => form.stock_pool_value, loadLatest)

onMounted(async () => {
  loading.value = true
  try {
    await loadCatalogs()
    await loadLatest()
  } finally {
    loading.value = false
  }
})
onUnmounted(disposeCharts)
</script>

<style scoped>
.factor-detail {
  display: flex;
  flex-direction: column;
  gap: 12px;
}
.detail-header,
.param-panel,
.panel,
.summary-card {
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-elevated);
  box-shadow: var(--shadow-sm);
}
.detail-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  padding: 14px;
}
.title-block { flex: 1; min-width: 0; }
.panel-kicker {
  color: var(--accent-info);
  font-family: var(--font-data);
  font-size: 10px;
}
h2, h3, p { margin: 0; }
h2 {
  color: var(--text-bright);
  font-size: 20px;
}
.meta-line {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 4px;
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: 12px;
}
.header-actions,
.inline-controls,
.switch-row,
.date-range {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.info-panel {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}
.info-card {
  min-width: 0;
  padding: 12px;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-elevated);
  box-shadow: var(--shadow-sm);
}
.info-card--wide {
  grid-column: span 2;
}
.info-label {
  display: block;
  margin-bottom: 5px;
  color: var(--text-secondary);
  font-size: 12px;
}
.info-panel p,
.info-panel li {
  color: var(--text-primary);
  line-height: 1.6;
}
.info-panel code {
  display: block;
  padding: 9px 10px;
  border: 1px solid var(--border-subtle);
  border-radius: 6px;
  color: var(--text-bright);
  background: rgba(8, 8, 10, 0.42);
  font-family: var(--font-data);
  font-size: 12px;
  line-height: 1.6;
  white-space: pre-wrap;
}
.info-panel ol {
  margin: 0;
  padding-left: 18px;
}
.dependency-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}
.param-panel {
  padding: 12px;
}
.param-form {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  column-gap: 18px;
}
.summary-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
}
.summary-card {
  display: grid;
  gap: 5px;
  padding: 12px;
}
.summary-card span {
  color: var(--text-secondary);
  font-size: 12px;
}
.summary-card strong {
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: 20px;
}
.chart-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}
.panel {
  padding: 12px;
}
.panel-wide {
  grid-column: 1 / -1;
}
.panel h3 {
  color: var(--text-bright);
  font-size: 14px;
  margin-bottom: 10px;
}
.chart {
  height: 280px;
  width: 100%;
}
.stock-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}
.positive { color: #d93026 !important; }
.negative { color: #137333 !important; }
@media (max-width: 1100px) {
  .detail-header,
  .info-panel,
  .param-form,
  .chart-grid,
  .stock-grid,
  .summary-grid {
    grid-template-columns: 1fr;
  }
  .detail-header {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>

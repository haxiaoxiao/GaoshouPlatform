<template>
  <div class="page-frame data-workbench-page">
    <header class="data-command-bar">
      <div class="data-command-bar__copy">
        <span class="section-kicker">DATA VIEW / SINGLE-STOCK WORKBENCH</span>
        <h2>数据查看</h2>
        <p>选择一只股票后，在行情、财务、资金、舆情和原始口径之间切换；查看与同步动作保持分离。</p>
      </div>

      <div class="data-command-bar__actions">
        <el-button :icon="Refresh" :loading="loading" @click="refreshWorkbench">刷新口径</el-button>
        <el-button plain @click="goSync">去数据同步</el-button>
      </div>
    </header>

    <section class="stock-focus-strip">
      <div class="stock-search-block">
        <span class="field-label">股票</span>
        <el-select
          v-model="selectedSymbol"
          filterable
          remote
          reserve-keyword
          :remote-method="searchStocks"
          :loading="stockSearchLoading"
          placeholder="输入股票代码或名称"
          class="stock-search"
          @change="handleStockChange"
        >
          <el-option
            v-for="item in stockOptions"
            :key="item.symbol"
            :label="`${item.symbol} ${item.name}`"
            :value="item.symbol"
          />
        </el-select>
      </div>

      <div class="active-stock-card">
        <div>
          <span class="section-kicker">ACTIVE STOCK</span>
          <h3>{{ activeStockName }}</h3>
          <p>{{ selectedSymbol }} · {{ stockDetail?.industry || selectedStockOption?.industry || '行业未标注' }}</p>
        </div>
        <div class="stock-status-cluster">
          <span class="status-pill" :class="stockDetail?.is_active === false ? 'status-pill--bad' : 'status-pill--good'">
            {{ stockDetail?.is_active === false ? '停牌/退市关注' : '可用' }}
          </span>
          <span class="status-pill status-pill--neutral">
            {{ stockDetail?.latest_report_date || financialLatestDate || '财报待同步' }}
          </span>
        </div>
      </div>
    </section>

    <section class="workbench-grid">
      <main class="domain-workspace">
        <nav class="domain-tabs" aria-label="数据域切换">
          <button
            v-for="tab in domainTabs"
            :key="tab.key"
            type="button"
            :class="{ active: activeTab === tab.key }"
            @click="activeTab = tab.key"
          >
            <span>{{ tab.label }}</span>
            <small>{{ tab.hint }}</small>
          </button>
        </nav>

        <section class="domain-panel">
          <div v-show="activeTab === 'market'" class="tab-runtime-panel">
            <div class="panel-head">
              <div>
                <span class="section-kicker">MARKET / K-LINE</span>
                <h3>行情与 K 线</h3>
              </div>
              <div class="panel-tools">
                <el-select v-model="klinePeriod" class="compact-control" @change="loadKlines">
                  <el-option label="日线" value="daily" />
                  <el-option label="分钟" value="minute" />
                </el-select>
                <el-date-picker
                  v-model="klineDateRange"
                  type="daterange"
                  unlink-panels
                  value-format="YYYY-MM-DD"
                  start-placeholder="开始日期"
                  end-placeholder="结束日期"
                  class="date-range-control"
                  @change="loadKlines"
                />
                <el-button :icon="Download" :disabled="!klineRows.length" @click="exportKlines">导出 CSV</el-button>
              </div>
            </div>

            <div class="metric-strip">
              <article v-for="metric in marketMetrics" :key="metric.label" class="metric-block">
                <span>{{ metric.label }}</span>
                <strong>{{ metric.value }}</strong>
                <small>{{ metric.hint }}</small>
              </article>
            </div>

            <div class="chart-panel" v-loading="klineLoading">
              <KlineChart v-if="klineChartRows.length" :data="klineChartRows" />
              <el-empty v-else description="暂无行情数据，请调整日期或同步数据" :image-size="72" />
            </div>

            <div class="table-panel">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>日期/时间</th>
                    <th>开盘</th>
                    <th>最高</th>
                    <th>最低</th>
                    <th>收盘</th>
                    <th>成交量</th>
                    <th>成交额</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in klineRows.slice(0, 12)" :key="row.datetime">
                    <td class="mono">{{ row.datetime }}</td>
                    <td>{{ formatPrice(row.open) }}</td>
                    <td>{{ formatPrice(row.high) }}</td>
                    <td>{{ formatPrice(row.low) }}</td>
                    <td>{{ formatPrice(row.close) }}</td>
                    <td>{{ formatNumber(row.volume) }}</td>
                    <td>{{ formatYuanMoney(row.amount) }}</td>
                  </tr>
                  <tr v-if="!klineLoading && !klineRows.length">
                    <td colspan="7" class="empty-cell">当前股票在所选区间没有可展示行情。</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div v-show="activeTab === 'financial'" class="tab-runtime-panel">
            <div class="panel-head">
              <div>
                <span class="section-kicker">FINANCIAL / VALUATION</span>
                <h3>财务与估值</h3>
              </div>
              <div class="panel-tools">
                <el-select v-model="financialPeriodFilter" class="compact-control">
                  <el-option label="全部报告" value="all" />
                  <el-option label="年报" value="annual" />
                  <el-option label="中报/季报" value="quarter" />
                </el-select>
                <el-switch v-model="pitMode" active-text="PIT" inactive-text="最新" inline-prompt />
              </div>
            </div>

            <div class="finance-section">
              <div class="finance-group">
                <div class="group-title">
                  <span>估值</span>
                  <small>价格相对基本面</small>
                </div>
                <div class="metric-grid metric-grid--four">
                  <article v-for="metric in valuationMetrics" :key="metric.label" class="metric-block">
                    <span>{{ metric.label }}</span>
                    <strong>{{ metric.value }}</strong>
                    <small>{{ metric.hint }}</small>
                  </article>
                </div>
              </div>

              <div class="finance-group">
                <div class="group-title">
                  <span>盈利质量</span>
                  <small>利润率、回报率与单股指标</small>
                </div>
                <div class="metric-grid metric-grid--four">
                  <article v-for="metric in profitabilityMetrics" :key="metric.label" class="metric-block">
                    <span>{{ metric.label }}</span>
                    <strong>{{ metric.value }}</strong>
                    <small>{{ metric.hint }}</small>
                  </article>
                </div>
              </div>

              <div class="finance-group">
                <div class="group-title">
                  <span>成长与结构</span>
                  <small>同比变化和资产负债结构</small>
                </div>
                <div class="metric-grid metric-grid--four">
                  <article v-for="metric in growthMetrics" :key="metric.label" class="metric-block">
                    <span>{{ metric.label }}</span>
                    <strong>{{ metric.value }}</strong>
                    <small>{{ metric.hint }}</small>
                  </article>
                </div>
              </div>
            </div>

            <div class="table-panel" v-loading="financialLoading">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>报告期</th>
                    <th>披露日</th>
                    <th>营收</th>
                    <th>净利润</th>
                    <th>EPS</th>
                    <th>ROE</th>
                    <th>PE</th>
                    <th>PB</th>
                    <th>PS</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in filteredFinancialReports" :key="row.report_date">
                    <td class="mono">{{ row.report_date }}</td>
                    <td class="mono">{{ row.ann_date || '-' }}</td>
                    <td>{{ formatYuanMoney(row.revenue) }}</td>
                    <td>{{ formatYuanMoney(row.net_profit) }}</td>
                    <td>{{ formatRatioNumber(row.eps) }}</td>
                    <td>{{ formatPercent(normalizePercent(row.roe)) }}</td>
                    <td>{{ formatRatioNumber(reportPe(row)) }}</td>
                    <td>{{ formatRatioNumber(reportPb(row)) }}</td>
                    <td>{{ formatRatioNumber(reportPs(row)) }}</td>
                  </tr>
                  <tr v-if="!financialLoading && !filteredFinancialReports.length">
                    <td colspan="9" class="empty-cell">暂无财务报告。可从右侧入口进入数据同步。</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <div v-show="activeTab === 'capital'" class="tab-runtime-panel">
            <div class="panel-head">
              <div>
                <span class="section-kicker">CAPITAL / LIQUIDITY</span>
                <h3>资金与成交</h3>
              </div>
              <div class="panel-tools">
                <el-radio-group v-model="capitalMode" size="small">
                  <el-radio-button label="historical">历史</el-radio-button>
                  <el-radio-button label="intraday">日内</el-radio-button>
                </el-radio-group>
              </div>
            </div>

            <div class="metric-strip">
              <article v-for="metric in capitalMetrics" :key="metric.label" class="metric-block">
                <span>{{ metric.label }}</span>
                <strong>{{ metric.value }}</strong>
                <small>{{ metric.hint }}</small>
              </article>
            </div>

            <div class="liquidity-board">
              <article class="flow-card">
                <div class="group-title">
                  <span>成交额分布</span>
                  <small>基于当前行情样本估算</small>
                </div>
                <div class="flow-bars">
                  <div v-for="bar in amountDistribution" :key="bar.label" class="flow-bar">
                    <span>{{ bar.label }}</span>
                    <div><i :style="{ width: bar.width }"></i></div>
                    <strong>{{ bar.value }}</strong>
                  </div>
                </div>
              </article>

              <article class="flow-card">
                <div class="group-title">
                  <span>流动性口径</span>
                  <small>用于回测前检查成交可行性</small>
                </div>
                <div class="schema-list">
                  <div v-for="row in liquidityRows" :key="row.label">
                    <span>{{ row.label }}</span>
                    <strong>{{ row.value }}</strong>
                    <small>{{ row.hint }}</small>
                  </div>
                </div>
              </article>
            </div>
          </div>

          <div v-show="activeTab === 'sentiment'" class="tab-runtime-panel">
            <div class="panel-head">
              <div>
                <span class="section-kicker">SENTIMENT / EVENTS</span>
                <h3>舆情与事件</h3>
              </div>
              <div class="panel-tools">
                <el-select v-model="sentimentSource" class="compact-control">
                  <el-option label="全部来源" value="all" />
                  <el-option label="雪球" value="xueqiu" />
                  <el-option label="东财股吧" value="eastmoney" />
                  <el-option label="淘股吧" value="taoguba" />
                  <el-option label="NGA" value="nga" />
                </el-select>
              </div>
            </div>

            <div class="sentiment-layout">
              <article class="sentiment-score">
                <span>情绪评分</span>
                <strong>{{ sentimentScore }}</strong>
                <p>基于缓存帖子数量、最近同步时间和关键词热度展示；无缓存时显示待同步状态。</p>
              </article>

              <div class="keyword-cloud">
                <span v-for="keyword in sentimentKeywords" :key="keyword">{{ keyword }}</span>
              </div>
            </div>

            <div class="event-timeline">
              <article v-for="event in sentimentEvents" :key="event.title" class="timeline-row">
                <span>{{ event.time }}</span>
                <strong>{{ event.title }}</strong>
                <p>{{ event.detail }}</p>
              </article>
            </div>
          </div>

          <div v-show="activeTab === 'schema'" class="tab-runtime-panel">
            <div class="panel-head">
              <div>
                <span class="section-kicker">RAW DATA / LINEAGE</span>
                <h3>原始数据与口径</h3>
              </div>
              <div class="panel-tools">
                <el-select v-model="schemaScope" class="compact-control">
                  <el-option label="当前股票" value="stock" />
                  <el-option label="全局表" value="global" />
                </el-select>
              </div>
            </div>

            <div class="schema-grid">
              <article v-for="item in sourceRows" :key="item.key" class="schema-card">
                <div>
                  <span>{{ item.label }}</span>
                  <strong>{{ item.latest }}</strong>
                </div>
                <p>{{ item.source }}</p>
                <small>{{ item.rows }}</small>
              </article>
            </div>

            <div class="table-panel">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>字段</th>
                    <th>含义</th>
                    <th>来源</th>
                    <th>用途</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="field in schemaFields" :key="field.name">
                    <td class="mono">{{ field.name }}</td>
                    <td>{{ field.label }}</td>
                    <td>{{ field.source }}</td>
                    <td>{{ field.usage }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </section>
      </main>

      <aside class="context-rail">
        <section class="rail-panel">
          <div class="rail-head">
            <span>最近 / 关注</span>
            <strong>{{ recentStocks.length }}</strong>
          </div>
          <div class="recent-stock-list">
            <button
              v-for="stock in recentStocks"
              :key="stock.symbol"
              type="button"
              :class="{ active: selectedSymbol === stock.symbol }"
              @click="selectRecentStock(stock.symbol)"
            >
              <span>{{ stock.symbol }}</span>
              <strong>{{ stock.name }}</strong>
              <small>{{ stock.industry || stock.theme }}</small>
            </button>
          </div>
        </section>

        <section class="rail-panel">
          <div class="rail-head">
            <span>数据源状态</span>
            <el-button link type="primary" @click="goSync">同步</el-button>
          </div>
          <div class="source-status-list">
            <article v-for="item in sourceRows" :key="item.key" :class="`source-status source-status--${item.tone}`">
              <div>
                <strong>{{ item.label }}</strong>
                <span>{{ item.latest }}</span>
              </div>
              <small>{{ item.rows }}</small>
            </article>
          </div>
        </section>

        <section class="rail-panel">
          <div class="rail-head">
            <span>最近同步</span>
            <strong>{{ syncLogs.length }}</strong>
          </div>
          <div class="sync-log-list">
            <article v-for="log in syncLogs.slice(0, 5)" :key="log.id" class="sync-log-row">
              <span>{{ formatDateTime(log.end_time || log.start_time || log.created_at) }}</span>
              <strong>{{ syncTypeLabel(log.sync_type) }}</strong>
              <small>{{ syncStatusLabel(log.status) }}</small>
            </article>
            <p v-if="!syncLogs.length" class="empty-copy">暂无同步记录。</p>
          </div>
        </section>
      </aside>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { Download, Refresh } from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import { usePageContext } from '@/app/pageContext'
import request from '@/api/request'
import { klineApi, toDisplayFormat, type KlineDataDisplay, type KlineType } from '@/api/kline'
import { systemApi, type DataSummary, type DataSummaryItem } from '@/api/system'
import { syncApi, type SyncLog } from '@/api/sync'
import KlineChart from './KlineChart.vue'

type FreshnessTone = 'good' | 'warn' | 'bad' | 'neutral'
type TabKey = 'market' | 'financial' | 'capital' | 'sentiment' | 'schema'
type FinancialPeriodFilter = 'all' | 'annual' | 'quarter'

interface StockOption {
  symbol: string
  name: string
  exchange?: string | null
  industry?: string | null
  theme?: string
  total_mv?: number | null
  circ_mv?: number | null
}

interface StockDetail {
  symbol: string
  name: string
  exchange?: string | null
  industry?: string | null
  industry2?: string | null
  industry3?: string | null
  concept?: string | null
  is_active?: boolean
  market_cap?: number | null
  float_market_cap?: number | null
  pe_ratio?: number | null
  pb_ratio?: number | null
  roe?: number | null
  eps?: number | null
  bvps?: number | null
  revenue_growth?: number | null
  profit_growth?: number | null
  debt_ratio?: number | null
  gross_margin?: number | null
  net_margin?: number | null
  latest_report_date?: string | null
  latest_ann_date?: string | null
  updated_at?: string | null
}

interface FinancialReport {
  report_date: string
  ann_date?: string | null
  report_type?: string | null
  eps?: number | null
  bvps?: number | null
  roe?: number | null
  revenue?: number | null
  net_profit?: number | null
  revenue_yoy?: number | null
  profit_yoy?: number | null
  gross_margin?: number | null
  total_assets?: number | null
  total_liability?: number | null
  total_equity?: number | null
  total_mv?: number | null
  circ_mv?: number | null
  pe_ttm?: number | null
  pb?: number | null
}

interface DisplayMetric {
  label: string
  value: string
  hint: string
}

const router = useRouter()
const loading = ref(false)
const klineLoading = ref(false)
const financialLoading = ref(false)
const stockSearchLoading = ref(false)
const dataSummary = ref<DataSummary | null>(null)
const syncLogs = ref<SyncLog[]>([])
const stockOptions = ref<StockOption[]>([])
const stockDetail = ref<StockDetail | null>(null)
const financialReports = ref<FinancialReport[]>([])
const klineRows = ref<KlineDataDisplay[]>([])

const selectedSymbol = ref('600519.SH')
const activeTab = ref<TabKey>('market')
const klinePeriod = ref<KlineType>('daily')
const klineDateRange = ref<[string, string]>(defaultDateRange(365))
const financialPeriodFilter = ref<FinancialPeriodFilter>('all')
const pitMode = ref(true)
const capitalMode = ref<'historical' | 'intraday'>('historical')
const sentimentSource = ref('all')
const schemaScope = ref('stock')

const domainTabs: { key: TabKey; label: string; hint: string }[] = [
  { key: 'market', label: '行情/K线', hint: '价格与成交' },
  { key: 'financial', label: '财务', hint: '估值与质量' },
  { key: 'capital', label: '资金/成交', hint: '流动性' },
  { key: 'sentiment', label: '情绪/事件', hint: '文本信号' },
  { key: 'schema', label: '原始数据/口径', hint: '来源与字段' },
]

const recentStocks = ref<StockOption[]>([
  { symbol: '600519.SH', name: '贵州茅台', industry: '食品饮料', theme: '白酒 / 高股息' },
  { symbol: '000001.SZ', name: '平安银行', industry: '银行', theme: '金融 / 低估值' },
  { symbol: '300750.SZ', name: '宁德时代', industry: '电力设备', theme: '新能源 / 成长' },
  { symbol: '000333.SZ', name: '美的集团', industry: '家用电器', theme: '家电 / 现金流' },
  { symbol: '399101.SZ', name: '中小综指', industry: '指数', theme: '小市值策略池' },
])

const selectedStockOption = computed(() =>
  stockOptions.value.find(item => item.symbol === selectedSymbol.value)
  || recentStocks.value.find(item => item.symbol === selectedSymbol.value)
)

const activeStockName = computed(() =>
  stockDetail.value?.name || selectedStockOption.value?.name || selectedSymbol.value
)

const latestFinancial = computed(() => financialReports.value[0])
const financialLatestDate = computed(() => latestFinancial.value?.report_date || null)
const summaryMap = computed<Record<string, DataSummaryItem>>(() => dataSummary.value?.by_key || {})
const klineChartRows = computed(() => [...klineRows.value].reverse())

const filteredFinancialReports = computed(() => {
  if (financialPeriodFilter.value === 'all') return financialReports.value
  return financialReports.value.filter((row) => {
    const type = String(row.report_type || row.report_date || '').toLowerCase()
    if (financialPeriodFilter.value === 'annual') return type.includes('annual') || row.report_date?.endsWith('12-31')
    return !row.report_date?.endsWith('12-31')
  })
})

const latestClose = computed(() => klineRows.value[0]?.close ?? null)
const previousClose = computed(() => klineRows.value[1]?.close ?? null)
const priceChange = computed(() => {
  if (!latestClose.value || !previousClose.value) return null
  return (latestClose.value - previousClose.value) / previousClose.value
})

const marketMetrics = computed<DisplayMetric[]>(() => [
  { label: '最新收盘', value: formatPrice(latestClose.value), hint: klineRows.value[0]?.datetime || '暂无行情' },
  { label: '区间涨跌', value: formatPercent(priceChange.value), hint: latestClose.value ? '相对上一条记录' : '等待查询' },
  { label: '样本数量', value: `${klineRows.value.length.toLocaleString()} 条`, hint: klinePeriod.value === 'daily' ? '日线样本' : '分钟样本' },
  { label: '区间成交额', value: formatYuanMoney(sumBy(klineRows.value, row => row.amount)), hint: '当前日期范围合计' },
])

const valuationMetrics = computed<DisplayMetric[]>(() => [
  { label: 'PE TTM', value: formatRatioNumber(currentPeRatio.value), hint: '市值 / 近年净利润' },
  { label: 'PB', value: formatRatioNumber(currentPbRatio.value), hint: '市值 / 净资产' },
  { label: 'PS', value: formatRatioNumber(currentPsRatio.value), hint: '市值 / 营收' },
  { label: '总市值', value: formatYiMoney(marketCapInYuan.value), hint: '股票详情口径' },
])

const profitabilityMetrics = computed<DisplayMetric[]>(() => [
  { label: 'ROE', value: formatPercent(normalizePercent(latestFinancial.value?.roe) ?? stockDetail.value?.roe), hint: '净资产收益率' },
  { label: '毛利率', value: formatPercent(normalizePercent(latestFinancial.value?.gross_margin) ?? stockDetail.value?.gross_margin), hint: '盈利质量' },
  { label: '净利率', value: formatPercent(stockDetail.value?.net_margin ?? latestNetMargin.value), hint: '净利润 / 营收' },
  { label: 'EPS', value: formatRatioNumber(latestFinancial.value?.eps ?? stockDetail.value?.eps), hint: '每股收益' },
])

const growthMetrics = computed<DisplayMetric[]>(() => [
  { label: '营收同比', value: formatPercent(normalizePercent(latestFinancial.value?.revenue_yoy) ?? stockDetail.value?.revenue_growth), hint: '成长速度' },
  { label: '利润同比', value: formatPercent(normalizePercent(latestFinancial.value?.profit_yoy) ?? stockDetail.value?.profit_growth), hint: '利润弹性' },
  { label: '资产负债率', value: formatPercent(latestDebtRatio.value ?? stockDetail.value?.debt_ratio), hint: '总负债 / 总资产' },
  { label: 'BVPS', value: formatRatioNumber(latestFinancial.value?.bvps ?? stockDetail.value?.bvps), hint: '每股净资产' },
])

const marketCapInYuan = computed(() => {
  if (stockDetail.value?.market_cap === null || stockDetail.value?.market_cap === undefined) return null
  return stockDetail.value.market_cap * 100_000_000
})

const valuationBaseReport = computed(() =>
  financialReports.value.find(row => row.revenue || row.net_profit || row.total_equity) || latestFinancial.value
)

const currentPeRatio = computed(() =>
  cleanValuationRatio(reportPe(valuationBaseReport.value) ?? stockDetail.value?.pe_ratio)
)

const currentPbRatio = computed(() =>
  cleanValuationRatio(reportPb(valuationBaseReport.value) ?? stockDetail.value?.pb_ratio)
)

const currentPsRatio = computed(() => {
  const report = valuationBaseReport.value
  if (!report?.revenue || !marketCapInYuan.value) return null
  return marketCapInYuan.value / report.revenue
})

const latestNetMargin = computed(() => {
  const revenue = valuationBaseReport.value?.revenue
  const netProfit = valuationBaseReport.value?.net_profit
  if (!revenue || netProfit === null || netProfit === undefined) return null
  return netProfit / revenue
})

const latestDebtRatio = computed(() => {
  const assets = valuationBaseReport.value?.total_assets
  const liability = valuationBaseReport.value?.total_liability
  if (!assets || liability === null || liability === undefined) return null
  return liability / assets
})

const capitalMetrics = computed<DisplayMetric[]>(() => [
  { label: '平均成交额', value: formatYuanMoney(avgBy(klineRows.value, row => row.amount)), hint: '当前样本均值' },
  { label: '最大成交额', value: formatYuanMoney(maxBy(klineRows.value, row => row.amount)), hint: '区间峰值' },
  { label: '平均成交量', value: formatNumber(avgBy(klineRows.value, row => row.volume)), hint: capitalMode.value === 'intraday' ? '日内口径' : '历史口径' },
  { label: '流动性状态', value: klineRows.value.length ? '可评估' : '待同步', hint: '依赖行情样本' },
])

const amountDistribution = computed(() => {
  const amounts = klineRows.value.map(row => row.amount || 0)
  const max = Math.max(...amounts, 1)
  return klineRows.value.slice(0, 8).map(row => ({
    label: row.datetime.slice(0, 10),
    value: formatYuanMoney(row.amount),
    width: `${Math.max(6, Math.round(((row.amount || 0) / max) * 100))}%`,
  }))
})

const liquidityRows = computed(() => [
  { label: '成交额覆盖', value: `${klineRows.value.length.toLocaleString()} 条`, hint: '来自行情查询结果' },
  { label: '最近成交日', value: klineRows.value[0]?.datetime || '-', hint: '按接口返回顺序展示' },
  { label: '最大样本成交额', value: formatYuanMoney(maxBy(klineRows.value, row => row.amount)), hint: '用于大额下单可行性判断' },
  { label: '数据口径', value: klinePeriod.value === 'daily' ? 'klines_daily' : 'klines_minute', hint: '与同步服务保持一致' },
])

const sentimentScore = computed(() => {
  const item = summaryMap.value.sentiment
  if (!item || item.status === 'missing') return '待同步'
  if (item.status === 'good') return '64'
  if (item.status === 'stale') return '48'
  return '需检查'
})

const sentimentKeywords = computed(() => {
  if (sentimentSource.value === 'xueqiu') return ['业绩', '分红', '机构', '估值']
  if (sentimentSource.value === 'eastmoney') return ['股吧', '龙虎榜', '资金', '公告']
  return ['业绩', '估值', '资金', '公告', '分红', '情绪反转']
})

const sentimentEvents = computed(() => {
  const item = summaryMap.value.sentiment
  return [
    {
      time: latestValue(item),
      title: item?.status_text || '舆情缓存状态',
      detail: item ? `${item.source} · ${formatRowCount(item.row_count, item.row_count_estimated)}` : '暂无舆情口径，请先同步。',
    },
    {
      time: stockDetail.value?.latest_ann_date || '-',
      title: '最近财报披露',
      detail: stockDetail.value?.latest_report_date ? `报告期 ${stockDetail.value.latest_report_date}` : '当前股票未读取到披露日期。',
    },
  ]
})

const sourceRows = computed(() => [
  sourceRow('market_daily', '日线行情'),
  sourceRow('market_minute', '分钟行情'),
  sourceRow('financial', '财务报表'),
  sourceRow('sentiment', '舆情数据'),
  sourceRow('stocks', '股票基础'),
])

const schemaFields = computed(() => [
  { name: 'symbol', label: '股票代码', source: 'stocks / klines / financial_data', usage: '跨表联动主键' },
  { name: 'trade_date', label: '交易日期', source: 'klines_daily', usage: '行情和因子对齐' },
  { name: 'report_date', label: '报告期', source: 'financial_data', usage: 'PIT 财务对齐' },
  { name: 'pe_ttm / pb / ps', label: '估值指标', source: 'financial_data + 市值/营收估算', usage: '财务 tab 与估值因子' },
  { name: 'published_at', label: '发布时间', source: 'sentiment_posts', usage: '事件时间线' },
])

function goSync() {
  router.push('/data/sync')
}

async function refreshWorkbench() {
  loading.value = true
  try {
    await Promise.all([loadDashboard(), loadStockBundle(selectedSymbol.value)])
  } finally {
    loading.value = false
  }
}

async function handleStockChange() {
  addRecentStock(selectedSymbol.value)
  await loadStockBundle(selectedSymbol.value)
}

async function selectRecentStock(symbol: string) {
  selectedSymbol.value = symbol
  await handleStockChange()
}

async function searchStocks(query: string) {
  const keyword = query.trim()
  if (!keyword) {
    stockOptions.value = recentStocks.value
    return
  }
  stockSearchLoading.value = true
  try {
    const response = await request.get<{ items: StockOption[] }>('/data/stocks', {
      params: { search: keyword, page_size: 20 },
    })
    stockOptions.value = response.items || []
  } catch {
    stockOptions.value = []
  } finally {
    stockSearchLoading.value = false
  }
}

async function loadDashboard() {
  const [summaryResult, logsResult] = await Promise.allSettled([
    systemApi.dataSummary(),
    syncApi.getLogs({ limit: 20 }),
  ])
  if (summaryResult.status === 'fulfilled') dataSummary.value = summaryResult.value
  if (logsResult.status === 'fulfilled') syncLogs.value = logsResult.value
}

async function loadStockBundle(symbol: string) {
  await Promise.all([loadStockDetail(symbol), loadFinancialReports(symbol), loadKlines()])
}

async function loadStockDetail(symbol: string) {
  try {
    stockDetail.value = await request.get<StockDetail>(`/data/stocks/${symbol}`)
  } catch {
    stockDetail.value = null
  }
}

async function loadFinancialReports(symbol = selectedSymbol.value) {
  financialLoading.value = true
  try {
    financialReports.value = await request.get<FinancialReport[]>(`/indicators/financial/${symbol}`, {
      params: { report_count: 8 },
    })
  } catch {
    financialReports.value = []
  } finally {
    financialLoading.value = false
  }
}

async function loadKlines() {
  klineLoading.value = true
  try {
    const [startDate, endDate] = klineDateRange.value
    const response = await klineApi.getKlines({
      symbol: selectedSymbol.value,
      period: klinePeriod.value,
      start_date: startDate,
      end_date: endDate,
    })
    klineRows.value = toDisplayFormat(response.items || [])
  } catch {
    klineRows.value = []
  } finally {
    klineLoading.value = false
  }
}

function exportKlines() {
  if (!klineRows.value.length) return
  const header = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'amount']
  const rows = klineRows.value.map(row => [
    row.datetime,
    row.open,
    row.high,
    row.low,
    row.close,
    row.volume,
    row.amount,
  ])
  const csv = [header, ...rows].map(row => row.join(',')).join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const link = document.createElement('a')
  link.href = URL.createObjectURL(blob)
  link.download = `klines_${selectedSymbol.value}_${klineDateRange.value[0]}_${klineDateRange.value[1]}.csv`
  link.click()
  URL.revokeObjectURL(link.href)
  ElMessage.success('已导出当前行情样本')
}

function addRecentStock(symbol: string) {
  const existing = recentStocks.value.find(stock => stock.symbol === symbol)
  const option = stockOptions.value.find(stock => stock.symbol === symbol)
  const detail = stockDetail.value?.symbol === symbol ? stockDetail.value : null
  const nextStock: StockOption = existing || option || {
    symbol,
    name: detail?.name || symbol,
    industry: detail?.industry || undefined,
    theme: detail?.concept || undefined,
  }
  recentStocks.value = [nextStock, ...recentStocks.value.filter(stock => stock.symbol !== symbol)].slice(0, 6)
}

function sourceRow(key: string, label: string) {
  const item = summaryMap.value[key]
  const tone = summaryTone(item)
  return {
    key,
    label,
    tone,
    latest: latestValue(item),
    source: item?.source || key,
    rows: item ? formatRowCount(item.row_count, item.row_count_estimated) : '未接入',
  }
}

function reportPe(report?: FinancialReport): number | null {
  if (!report) return null
  const direct = cleanValuationRatio(report.pe_ttm)
  if (direct !== null) return direct
  if (!marketCapInYuan.value || !report.net_profit) return null
  return marketCapInYuan.value / report.net_profit
}

function reportPb(report?: FinancialReport): number | null {
  if (!report) return null
  const direct = cleanValuationRatio(report.pb)
  if (direct !== null) return direct
  if (!marketCapInYuan.value || !report.total_equity) return null
  return marketCapInYuan.value / report.total_equity
}

function reportPs(report?: FinancialReport): number | null {
  if (!report?.revenue || !marketCapInYuan.value) return null
  return marketCapInYuan.value / report.revenue
}

function cleanValuationRatio(value?: number | null): number | null {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  if (value > 0 && value < 0.1) return null
  return value
}

function latestValue(item?: DataSummaryItem): string {
  return formatDateTime(item?.latest_datetime || item?.latest_date)
}

function summaryTone(item?: DataSummaryItem): FreshnessTone {
  if (!item) return 'bad'
  if (item.status === 'good') return 'good'
  if (item.status === 'stale') return 'warn'
  if (item.status === 'missing' || item.status === 'error') return 'bad'
  return 'neutral'
}

function defaultDateRange(days: number): [string, string] {
  const end = new Date()
  const start = new Date()
  start.setDate(start.getDate() - days)
  return [formatDate(start), formatDate(end)]
}

function formatDate(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function formatDateTime(value?: string | null): string {
  if (!value) return '-'
  return value.replace('T', ' ').slice(0, value.includes(':') ? 16 : 10)
}

function formatRowCount(value?: number | null, estimated = false): string {
  if (value === null || value === undefined) return '未统计'
  const prefix = estimated ? '约 ' : ''
  return `${prefix}${value.toLocaleString()} 行`
}

function formatNumber(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return Math.round(value).toLocaleString()
}

function formatPrice(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return value.toFixed(2)
}

function formatRatioNumber(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return value.toFixed(Math.abs(value) >= 100 ? 0 : 2)
}

function formatYiMoney(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return `${(value / 100_000_000).toFixed(2)} 亿`
}

function formatYuanMoney(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  const abs = Math.abs(value)
  if (abs >= 100_000_000) return `${(value / 100_000_000).toFixed(2)} 亿`
  if (abs >= 10_000) return `${(value / 10_000).toFixed(2)} 万`
  return value.toFixed(0)
}

function normalizePercent(value?: number | null): number | null {
  if (value === null || value === undefined || Number.isNaN(value)) return null
  return Math.abs(value) > 1.5 ? value / 100 : value
}

function formatPercent(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '-'
  return `${(value * 100).toFixed(2)}%`
}

function sumBy<T>(rows: T[], picker: (row: T) => number | null | undefined): number | null {
  if (!rows.length) return null
  return rows.reduce((sum, row) => sum + Number(picker(row) || 0), 0)
}

function avgBy<T>(rows: T[], picker: (row: T) => number | null | undefined): number | null {
  if (!rows.length) return null
  return sumBy(rows, picker)! / rows.length
}

function maxBy<T>(rows: T[], picker: (row: T) => number | null | undefined): number | null {
  if (!rows.length) return null
  return Math.max(...rows.map(row => Number(picker(row) || 0)))
}

function syncTypeLabel(type: string): string {
  const labels: Record<string, string> = {
    datasync: '一键同步',
    stock_info: '股票基础',
    stock_full: '股票完整',
    financial_data: '财务报表',
    kline_daily: '日线行情',
    index_daily: '指数日线',
    kline_minute: '分钟行情',
    realtime_mv: '实时市值',
    dividends: '分红数据',
    factor_dependency: '因子依赖',
    tushare_relay: 'Tushare Relay',
    ths_concept: '同花顺概念',
    sentiment_xueqiu: '雪球舆情',
    sentiment_nga: 'NGA 舆情',
  }
  return labels[type] || type
}

function syncStatusLabel(status: string): string {
  const labels: Record<string, string> = {
    idle: '空闲',
    queued: '排队',
    running: '运行中',
    completed: '完成',
    failed: '失败',
    cancelled: '取消',
  }
  return labels[status] || status
}

const pageContextBlocks = computed(() => [
  {
    title: 'Data Workbench',
    rows: [
      { label: '当前股票', value: selectedSymbol.value },
      { label: '数据域', value: domainTabs.find(tab => tab.key === activeTab.value)?.label || activeTab.value },
      { label: '行情样本', value: `${klineRows.value.length} 条`, tone: klineRows.value.length ? 'good' : 'warn' },
      { label: '财报期数', value: `${financialReports.value.length} 期`, tone: financialReports.value.length ? 'good' : 'warn' },
    ],
  },
  {
    title: 'Sources',
    rows: sourceRows.value.slice(0, 4).map(row => ({
      label: row.label,
      value: row.latest,
      tone: row.tone === 'good' ? 'good' : row.tone === 'bad' ? 'bad' : 'warn',
    })),
  },
])

usePageContext(pageContextBlocks)

onMounted(async () => {
  stockOptions.value = recentStocks.value
  await refreshWorkbench()
})

watch(activeTab, (tab) => {
  if (tab === 'financial' && !financialReports.value.length) void loadFinancialReports()
  if ((tab === 'capital' || tab === 'market') && !klineRows.value.length) void loadKlines()
})
</script>

<style scoped>
.data-workbench-page {
  overflow: auto;
  color: var(--text-primary);
}

.data-command-bar,
.stock-focus-strip,
.workbench-grid {
  display: grid;
  gap: 16px;
}

.data-command-bar {
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: end;
  padding-bottom: 14px;
  border-bottom: 1px solid var(--border-default);
}

.data-command-bar__copy,
.active-stock-card,
.tab-runtime-panel,
.finance-section,
.context-rail,
.rail-panel,
.source-status-list,
.sync-log-list {
  display: grid;
  gap: 10px;
}

.data-command-bar h2,
.active-stock-card h3,
.panel-head h3 {
  margin: 0;
  color: var(--text-bright);
}

.data-command-bar p,
.active-stock-card p,
.sentiment-score p,
.schema-card p,
.empty-copy {
  margin: 0;
  color: var(--text-secondary);
}

.data-command-bar__actions,
.stock-status-cluster,
.panel-tools,
.rail-head,
.group-title,
.flow-bar,
.sync-log-row {
  display: flex;
  align-items: center;
  gap: 10px;
}

.data-command-bar__actions,
.stock-status-cluster,
.panel-tools {
  justify-content: flex-end;
  flex-wrap: wrap;
}

.stock-focus-strip {
  grid-template-columns: 360px minmax(0, 1fr);
  align-items: stretch;
}

.stock-search-block,
.active-stock-card,
.domain-workspace,
.context-rail,
.rail-panel {
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-primary);
}

.stock-search-block {
  display: grid;
  align-content: center;
  gap: 8px;
  padding: 14px;
}

.field-label {
  color: var(--text-muted);
  font-size: var(--text-xs);
  font-weight: 800;
}

.stock-search {
  width: 100%;
}

.active-stock-card {
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  padding: 14px 16px;
}

.status-pill {
  display: inline-flex;
  min-width: 72px;
  justify-content: center;
  border-radius: 999px;
  padding: 4px 9px;
  font-size: var(--text-xs);
  font-weight: 800;
}

.status-pill--good {
  color: #2d6a4f;
  background: #eaf5f0;
}

.status-pill--warn {
  color: #b27a1e;
  background: #fdf6e6;
}

.status-pill--bad {
  color: #a83232;
  background: #fbf1f1;
}

.status-pill--neutral {
  color: var(--text-secondary);
  background: var(--bg-elevated);
}

.workbench-grid {
  grid-template-columns: minmax(0, 1fr) 320px;
  align-items: start;
}

.domain-workspace {
  min-width: 0;
  overflow: hidden;
}

.domain-tabs {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  border-bottom: 1px solid var(--border-default);
  background: var(--bg-elevated);
}

.domain-tabs button {
  display: grid;
  gap: 3px;
  min-width: 0;
  padding: 12px 14px;
  border: 0;
  border-right: 1px solid var(--border-subtle);
  color: var(--text-secondary);
  background: transparent;
  cursor: pointer;
  text-align: left;
}

.domain-tabs button:last-child {
  border-right: 0;
}

.domain-tabs button:hover {
  background: var(--bg-hover);
}

.domain-tabs button.active {
  color: var(--accent-primary);
  background: var(--bg-primary);
  box-shadow: inset 0 -3px 0 var(--accent-primary);
}

.domain-tabs span {
  overflow: hidden;
  color: inherit;
  font-size: var(--text-sm);
  font-weight: 900;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.domain-tabs small {
  overflow: hidden;
  color: var(--text-muted);
  font-size: var(--text-xs);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.domain-panel {
  padding: 16px;
}

.panel-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.compact-control {
  width: 132px;
}

.date-range-control {
  width: 260px;
}

.metric-strip,
.metric-grid,
.schema-grid,
.liquidity-board,
.sentiment-layout {
  display: grid;
  gap: 10px;
}

.metric-strip {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.metric-grid--four {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.metric-block,
.flow-card,
.sentiment-score,
.schema-card {
  min-width: 0;
  border: 1px solid var(--border-subtle);
  border-radius: 8px;
  background: var(--bg-elevated);
}

.metric-block {
  display: grid;
  gap: 5px;
  padding: 12px;
}

.metric-block span,
.schema-card span,
.sentiment-score span,
.rail-head span,
.group-title small,
.sync-log-row span {
  color: var(--text-muted);
  font-size: var(--text-xs);
  font-weight: 800;
}

.metric-block strong,
.schema-card strong,
.sentiment-score strong {
  overflow: hidden;
  color: var(--text-bright);
  font-family: var(--font-data);
  font-size: var(--text-lg);
  text-overflow: ellipsis;
  white-space: nowrap;
}

.metric-block small,
.schema-card small,
.source-status small,
.sync-log-row small {
  color: var(--text-secondary);
  font-size: var(--text-xs);
}

.chart-panel {
  min-height: 360px;
}

.finance-section {
  gap: 14px;
}

.finance-group {
  display: grid;
  gap: 8px;
}

.group-title {
  justify-content: space-between;
}

.group-title span {
  color: var(--text-bright);
  font-weight: 900;
}

.table-panel {
  overflow: auto;
  border: 1px solid var(--border-default);
  border-radius: 8px;
  background: var(--bg-primary);
}

.data-table {
  width: 100%;
  min-width: 820px;
  border-collapse: collapse;
}

.data-table th,
.data-table td {
  padding: 10px 12px;
  border-bottom: 1px solid var(--border-subtle);
  text-align: left;
  vertical-align: middle;
}

.data-table th {
  color: var(--text-muted);
  background: var(--bg-elevated);
  font-family: var(--font-data);
  font-size: var(--text-xs);
  font-weight: 900;
}

.data-table td {
  color: var(--text-primary);
  font-size: var(--text-sm);
}

.data-table tbody tr:hover {
  background: var(--bg-hover);
}

.mono {
  font-family: var(--font-data);
}

.empty-cell {
  color: var(--text-muted);
  text-align: center !important;
}

.liquidity-board {
  grid-template-columns: minmax(0, 1fr) minmax(0, 0.9fr);
}

.flow-card,
.sentiment-score {
  padding: 14px;
}

.flow-bars,
.schema-list,
.event-timeline,
.keyword-cloud {
  display: grid;
  gap: 8px;
}

.flow-bar {
  display: grid;
  grid-template-columns: 90px minmax(0, 1fr) 92px;
  align-items: center;
}

.flow-bar div {
  height: 8px;
  overflow: hidden;
  border-radius: 999px;
  background: var(--bg-hover);
}

.flow-bar i {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: var(--accent-primary);
}

.flow-bar span,
.flow-bar strong,
.schema-list span,
.schema-list strong {
  color: var(--text-secondary);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.schema-list div {
  display: grid;
  gap: 4px;
  padding: 9px 0;
  border-bottom: 1px solid var(--border-subtle);
}

.sentiment-layout {
  grid-template-columns: 220px minmax(0, 1fr);
}

.sentiment-score {
  align-content: center;
  min-height: 180px;
}

.sentiment-score strong {
  color: var(--accent-primary);
  font-size: 48px;
}

.keyword-cloud {
  grid-template-columns: repeat(auto-fit, minmax(86px, 1fr));
  align-content: start;
}

.keyword-cloud span {
  border: 1px solid var(--border-subtle);
  border-radius: 999px;
  padding: 8px 10px;
  color: var(--accent-primary);
  background: var(--bg-elevated);
  font-size: var(--text-xs);
  font-weight: 800;
  text-align: center;
}

.event-timeline {
  border-left: 2px solid var(--border-default);
  padding-left: 12px;
}

.timeline-row {
  display: grid;
  gap: 4px;
  padding: 10px 0;
}

.timeline-row span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.timeline-row strong {
  color: var(--text-bright);
}

.timeline-row p {
  margin: 0;
  color: var(--text-secondary);
  font-size: var(--text-sm);
}

.schema-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.schema-card {
  display: grid;
  gap: 8px;
  padding: 12px;
}

.schema-card div {
  display: grid;
  gap: 4px;
}

.context-rail {
  position: sticky;
  top: 12px;
  padding: 0;
  border: 0;
  background: transparent;
}

.rail-panel {
  padding: 14px;
}

.rail-head {
  justify-content: space-between;
}

.rail-head strong {
  color: var(--accent-primary);
  font-family: var(--font-data);
}

.recent-stock-list {
  display: grid;
  gap: 7px;
}

.recent-stock-list button {
  display: grid;
  gap: 3px;
  width: 100%;
  padding: 10px;
  border: 1px solid transparent;
  border-radius: 7px;
  color: var(--text-primary);
  background: transparent;
  cursor: pointer;
  text-align: left;
}

.recent-stock-list button:hover,
.recent-stock-list button.active {
  border-color: var(--border-accent);
  background: var(--pine-bg-light, #eef3f0);
}

.recent-stock-list span,
.recent-stock-list small {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.recent-stock-list strong {
  color: var(--text-bright);
}

.source-status {
  display: grid;
  gap: 6px;
  padding: 10px;
  border-left: 3px solid var(--border-default);
  background: var(--bg-elevated);
}

.source-status--good {
  border-left-color: #2d6a4f;
}

.source-status--warn {
  border-left-color: #b27a1e;
}

.source-status--bad {
  border-left-color: #a83232;
}

.source-status div {
  display: flex;
  justify-content: space-between;
  gap: 8px;
}

.source-status strong {
  color: var(--text-bright);
}

.source-status span {
  color: var(--text-muted);
  font-family: var(--font-data);
  font-size: var(--text-xs);
}

.sync-log-row {
  display: grid;
  grid-template-columns: 96px minmax(0, 1fr) auto;
  align-items: center;
  padding: 8px 0;
  border-bottom: 1px solid var(--border-subtle);
}

.sync-log-row strong {
  overflow: hidden;
  color: var(--text-bright);
  font-size: var(--text-sm);
  text-overflow: ellipsis;
  white-space: nowrap;
}

@media (max-width: 1180px) {
  .stock-focus-strip,
  .workbench-grid,
  .liquidity-board,
  .sentiment-layout {
    grid-template-columns: 1fr;
  }

  .context-rail {
    position: static;
  }
}

@media (max-width: 860px) {
  .data-command-bar,
  .active-stock-card,
  .panel-head,
  .schema-grid,
  .metric-strip,
  .metric-grid--four {
    grid-template-columns: 1fr;
  }

  .data-command-bar__actions,
  .panel-tools,
  .stock-status-cluster {
    justify-content: flex-start;
  }

  .domain-tabs {
    display: flex;
    overflow-x: auto;
  }

  .domain-tabs button {
    min-width: 128px;
  }

  .date-range-control,
  .compact-control {
    width: 100%;
  }
}
</style>

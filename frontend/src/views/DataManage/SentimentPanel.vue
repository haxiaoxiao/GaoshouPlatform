<template>
  <section class="sentiment-workbench">
    <div class="sentiment-head">
      <div>
        <div class="eyebrow">DATA SENTIMENT</div>
        <h2>情绪数据模块</h2>
        <p>把雪球个股讨论和 NGA 主题讨论统一进平台内的数据面板，按股票汇总、缓存和回看。</p>
      </div>
      <div class="sentiment-actions">
        <el-button :icon="Refresh" :loading="isRefreshing" @click="reloadAll">刷新面板</el-button>
        <el-button type="primary" :icon="Connection" :loading="ingesting" @click="runIngest">
          拉取情绪
        </el-button>
      </div>
    </div>

    <div class="status-strip">
      <div class="status-card">
        <span>缓存帖子</span>
        <strong>{{ formatNumber(overview?.total_posts || 0) }}</strong>
      </div>
      <div class="status-card">
        <span>覆盖股票</span>
        <strong>{{ formatNumber(overview?.symbol_count || 0) }}</strong>
      </div>
      <div class="status-card">
        <span>最近缓存</span>
        <strong>{{ formatDateTime(overview?.latest_published_at) }}</strong>
      </div>
      <div class="status-card status-card--accent">
        <span>当前股票</span>
        <strong>{{ selectedSymbol || '未选择' }}</strong>
      </div>
    </div>

    <div class="layout-grid">
      <section class="panel controls-panel">
        <div class="panel-title">
          <h3>查询与抓取</h3>
          <span>单一入口，统一控制两类舆情源</span>
        </div>

        <div class="control-grid">
          <label class="wide">
            <span>股票</span>
            <el-select
              v-model="selectedSymbol"
              filterable
              remote
              reserve-keyword
              placeholder="输入股票代码或名称"
              :remote-method="searchStocks"
              :loading="stockSearchLoading"
              @change="reloadSnapshot"
            >
              <el-option
                v-for="item in stockOptions"
                :key="item.symbol"
                :label="`${item.symbol} ${item.name}`"
                :value="item.symbol"
              />
            </el-select>
            <small class="field-hint">
              {{ requiresSymbolForIngest ? '雪球抓取需要股票；东财热门吧、集思录、NGA 可按全市场讨论抓取。' : '抓取东财热门吧、集思录或 NGA 时这里可以留空。' }}
            </small>
          </label>

          <label class="wide">
            <span>时间范围</span>
            <el-date-picker
              v-model="dateRange"
              type="daterange"
              unlink-panels
              value-format="YYYY-MM-DD"
              start-placeholder="开始日期"
              end-placeholder="结束日期"
            />
          </label>

          <label class="wide">
            <span>来源</span>
            <el-checkbox-group v-model="selectedSources" class="source-checkboxes">
              <el-checkbox-button
                v-for="source in sourceOptions"
                :key="source.value"
                :label="source.value"
              >
                {{ source.label }}
              </el-checkbox-button>
            </el-checkbox-group>
          </label>

          <label>
            <span>抓取页数</span>
            <el-input-number v-model="maxPages" :min="1" :max="30" controls-position="right" />
          </label>

          <label>
            <span>最少回复</span>
            <el-input-number v-model="minReply" :min="0" :max="10000" controls-position="right" />
          </label>

          <label class="toggle-row">
            <span>强制刷新 NGA 日缓存</span>
            <el-switch v-model="forceRefresh" />
          </label>
        </div>

        <div class="control-actions">
          <el-button :loading="loadingSummary || loadingPosts" @click="reloadSnapshot">读取缓存</el-button>
          <el-button type="primary" plain :loading="ingesting" @click="runIngest">抓取并入库</el-button>
        </div>
      </section>

      <section class="panel source-panel">
        <div class="panel-title">
          <h3>来源状态</h3>
          <span>外部目录与鉴权状态</span>
        </div>

        <div class="source-card-list">
          <article
            v-for="source in sourceCards"
            :key="source.source"
            class="source-card"
            :class="{ 'source-card--ready': source.ready, 'source-card--muted': !source.ready }"
          >
            <div class="source-card__head">
              <div>
                <strong>{{ source.label }}</strong>
                <small>{{ source.source }}</small>
              </div>
              <el-tag :type="source.ready ? 'success' : 'warning'" effect="dark" size="small">
                {{ source.ready ? '可用' : '待配置' }}
              </el-tag>
            </div>
            <ul class="source-card__meta">
              <li>缓存帖子 {{ formatNumber(source.post_count) }}</li>
              <li>覆盖股票 {{ formatNumber(source.symbol_count) }}</li>
              <li>最近时间 {{ formatDateTime(source.latest_published_at) }}</li>
              <li>目录 {{ source.project_ready ? '已连接' : '缺失' }}</li>
              <li>Cookie {{ source.cookie_configured ? '已配置' : '未配置' }}</li>
              <li v-if="source.cache_dir">缓存文件 {{ formatNumber(source.cache_file_count) }}</li>
            </ul>
          </article>
        </div>
      </section>
    </div>

    <section class="panel summary-panel">
      <div class="panel-title">
        <h3>股票摘要</h3>
        <span>{{ selectedSymbol || '请选择股票' }}</span>
      </div>

      <el-empty v-if="!selectedSymbol" description="先选择一只股票" :image-size="72" />
      <template v-else>
        <div class="summary-grid" v-loading="loadingSummary">
          <article v-for="row in summarySourceRows" :key="row.source" class="summary-card">
            <div class="summary-card__head">
              <strong>{{ row.label }}</strong>
              <span>{{ row.post_count }} 帖</span>
            </div>
            <div class="summary-card__metrics">
              <div>
                <span>均值情绪</span>
                <strong>{{ formatScore(row.avg_sentiment) }}</strong>
              </div>
              <div>
                <span>看多占比</span>
                <strong class="bull">{{ formatRatio(row.bullish_ratio) }}</strong>
              </div>
              <div>
                <span>看空占比</span>
                <strong class="bear">{{ formatRatio(row.bearish_ratio) }}</strong>
              </div>
              <div>
                <span>互动量</span>
                <strong>{{ formatNumber(row.comment_count) }}</strong>
              </div>
            </div>
            <div class="summary-card__keywords">
              <span
                v-for="keyword in splitKeywords(row.top_keywords)"
                :key="`${row.source}-${keyword}`"
                class="keyword-chip"
              >
                {{ keyword }}
              </span>
              <span v-if="splitKeywords(row.top_keywords).length === 0" class="keyword-chip keyword-chip--ghost">暂无关键词</span>
            </div>
          </article>
        </div>

        <div class="hot-posts">
          <div class="hot-posts__head">
            <strong>热帖预览</strong>
            <span>按回复、点赞与发布时间排序</span>
          </div>
          <el-empty
            v-if="!summary?.hottest_posts?.length && !loadingSummary"
            description="当前时间范围内没有缓存热帖"
            :image-size="60"
          />
          <div v-else class="hot-posts__list">
            <article v-for="post in summary?.hottest_posts || []" :key="post.id" class="hot-post">
              <div class="hot-post__meta">
                <el-tag size="small" effect="plain">{{ sourceLabel(post.source) }}</el-tag>
                <span>{{ formatDateTime(post.published_at) }}</span>
                <span>{{ post.author || '匿名' }}</span>
              </div>
              <strong class="hot-post__title">{{ post.title || trimText(post.content, 48) || '无标题帖子' }}</strong>
              <p class="hot-post__content">{{ trimText(post.content, 140) || '暂无摘要' }}</p>
              <div class="hot-post__footer">
                <span>回复 {{ formatNumber(post.reply_count || post.comment_count || 0) }}</span>
                <span>点赞 {{ formatNumber(post.like_count || 0) }}</span>
                <a v-if="post.url" :href="post.url" target="_blank" rel="noreferrer">原帖</a>
              </div>
            </article>
          </div>
        </div>
      </template>
    </section>

    <section class="panel results-panel" v-if="lastIngest">
      <div class="panel-title">
        <h3>最近抓取结果</h3>
        <span>{{ ingestTargetLabel(lastIngest) }}</span>
      </div>

      <div class="result-strip">
        <div class="status-card">
          <span>成功来源</span>
          <strong>{{ lastIngest.succeeded_sources.length }}</strong>
        </div>
        <div class="status-card">
          <span>失败来源</span>
          <strong>{{ lastIngest.failed_sources.length }}</strong>
        </div>
        <div class="status-card">
          <span>抓取总量</span>
          <strong>{{ formatNumber(lastIngest.total_collected) }}</strong>
        </div>
        <div class="status-card">
          <span>入库总量</span>
          <strong>{{ formatNumber(lastIngest.total_upserted) }}</strong>
        </div>
      </div>

      <el-table :data="lastIngest.results" size="small" class="sentiment-table" max-height="240">
        <el-table-column label="来源" min-width="120">
          <template #default="{ row }">{{ sourceLabel(row.source) }}</template>
        </el-table-column>
        <el-table-column label="状态" width="110">
          <template #default="{ row }">
            <el-tag :type="row.ok === false ? 'danger' : 'success'" effect="dark" size="small">
              {{ row.ok === false ? '失败' : '完成' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="collected" label="抓取" width="90" />
        <el-table-column prop="matched" label="匹配" width="90" />
        <el-table-column prop="upserted" label="入库" width="90" />
        <el-table-column label="详情" min-width="280" show-overflow-tooltip>
          <template #default="{ row }">
            {{ row.error || row.page_url || row.mode || '-' }}
          </template>
        </el-table-column>
      </el-table>
    </section>

    <section class="panel posts-panel">
      <div class="panel-title">
        <h3>缓存帖子</h3>
        <span>{{ posts.length }} 条</span>
      </div>

      <el-table :data="posts" v-loading="loadingPosts" size="small" class="sentiment-table" max-height="420">
        <el-table-column label="时间" width="150">
          <template #default="{ row }">{{ formatDateTime(row.published_at) }}</template>
        </el-table-column>
        <el-table-column label="来源" width="110">
          <template #default="{ row }">{{ sourceLabel(row.source) }}</template>
        </el-table-column>
        <el-table-column label="标题 / 内容" min-width="420" show-overflow-tooltip>
          <template #default="{ row }">
            <div class="post-main">
              <strong>{{ row.title || trimText(row.content, 64) || '无标题帖子' }}</strong>
              <small>{{ trimText(row.content, 140) || '暂无正文' }}</small>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="author" label="作者" width="120" show-overflow-tooltip />
        <el-table-column label="情绪" width="120">
          <template #default="{ row }">
            <el-tag :type="sentimentTagType(row.sentiment_label)" effect="dark" size="small">
              {{ sentimentLabel(row.sentiment_label) }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column label="热度" width="148">
          <template #default="{ row }">
            回{{ formatNumber(row.reply_count || row.comment_count || 0) }} / 赞{{ formatNumber(row.like_count || 0) }}
          </template>
        </el-table-column>
        <el-table-column label="链接" width="92">
          <template #default="{ row }">
            <a v-if="row.url" :href="row.url" target="_blank" rel="noreferrer">打开</a>
            <span v-else>-</span>
          </template>
        </el-table-column>
      </el-table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Connection, Refresh } from '@element-plus/icons-vue'
import request from '@/api/request'
import {
  sentimentApi,
  type SentimentIngestBatchResult,
  type SentimentIngestSourceResult,
  type SentimentOverview,
  type SentimentOverviewSource,
  type SentimentPost,
  type SentimentSource,
  type SentimentSummary,
  type SentimentSummarySource,
} from '@/api/sentiment'

interface StockOption {
  symbol: string
  name: string
}

const sourceOptions: Array<{ value: SentimentSource; label: string }> = [
  { value: 'xueqiu_spyder', label: '雪球' },
  { value: 'eastmoney_guba', label: '东方财富股吧' },
  { value: 'jisilu', label: '集思录股票' },
  { value: 'flocktrader', label: 'NGA' },
]

const selectedSymbol = ref('')
const selectedSources = ref<SentimentSource[]>(sourceOptions.map(item => item.value))
const dateRange = ref<[string, string] | []>([])
const maxPages = ref(3)
const minReply = ref(20)
const forceRefresh = ref(false)
const stockSearchLoading = ref(false)
const stockOptions = ref<StockOption[]>([])
const loadingOverview = ref(false)
const loadingSummary = ref(false)
const loadingPosts = ref(false)
const ingesting = ref(false)
const overview = ref<SentimentOverview | null>(null)
const summary = ref<SentimentSummary | null>(null)
const posts = ref<SentimentPost[]>([])
const lastIngest = ref<SentimentIngestBatchResult | null>(null)

const isRefreshing = computed(() => loadingOverview.value || loadingSummary.value || loadingPosts.value)
const requiresSymbolForIngest = computed(() =>
  selectedSources.value.some(source => source === 'xueqiu_spyder'),
)

const summarySourceMap = computed(() => {
  const map = new Map<SentimentSource, SentimentSummarySource>()
  for (const row of summary.value?.sources || []) {
    map.set(row.source, row)
  }
  return map
})

const overviewSourceMap = computed(() => {
  const map = new Map<SentimentSource, SentimentOverviewSource>()
  for (const row of overview.value?.sources || []) {
    map.set(row.source, row)
  }
  return map
})

const sourceCards = computed(() =>
  sourceOptions.map((option) => {
    const row = overviewSourceMap.value.get(option.value)
    return row ? { ...row, label: sourceLabel(option.value) } : {
      source: option.value,
      label: option.label,
      project_dir: '',
      project_ready: false,
      cookie_configured: false,
      cache_dir: null,
      cache_file_count: 0,
      ready: false,
      post_count: 0,
      symbol_count: 0,
      latest_published_at: null,
    }
  }),
)

const summarySourceRows = computed(() =>
  selectedSources.value.map((source) => {
    const summaryRow = summarySourceMap.value.get(source)
    return {
      source,
      label: sourceLabel(source),
      post_count: summaryRow?.post_count || 0,
      comment_count: summaryRow?.comment_count || 0,
      bullish_ratio: summaryRow?.bullish_ratio || 0,
      bearish_ratio: summaryRow?.bearish_ratio || 0,
      avg_sentiment: summaryRow?.avg_sentiment ?? null,
      top_keywords: summaryRow?.top_keywords || '',
    }
  }),
)

function buildQueryParams() {
  const [start_date, end_date] = Array.isArray(dateRange.value) ? dateRange.value : []
  return {
    start_date: start_date || undefined,
    end_date: end_date || undefined,
    sources: selectedSources.value,
  }
}

function sourceLabel(source: SentimentSource) {
  return sourceOptions.find(item => item.value === source)?.label || source
}

function ingestTargetLabel(result?: SentimentIngestBatchResult | null) {
  if (!result) return '-'
  return result.symbol || 'NGA 日期抓取'
}

function formatNumber(value: number) {
  return Number(value || 0).toLocaleString()
}

function formatDateTime(value?: string | null) {
  if (!value) return '-'
  return value.replace('T', ' ').slice(0, 16)
}

function formatRatio(value?: number | null) {
  return `${Math.round(Number(value || 0) * 100)}%`
}

function formatScore(value?: number | null) {
  if (value == null) return '-'
  return Number(value).toFixed(2)
}

function trimText(value?: string | null, limit = 80) {
  const text = String(value || '').replace(/\s+/g, ' ').trim()
  if (!text) return ''
  return text.length > limit ? `${text.slice(0, limit)}...` : text
}

function splitKeywords(value?: string | null) {
  return String(value || '')
    .split(',')
    .map(item => item.trim())
    .filter(Boolean)
    .slice(0, 6)
}

function sentimentLabel(value?: string | null) {
  if (value === 'bullish') return '看多'
  if (value === 'bearish') return '看空'
  return '中性'
}

function sentimentTagType(value?: string | null) {
  if (value === 'bullish') return 'danger'
  if (value === 'bearish') return 'success'
  return 'info'
}

async function searchStocks(query: string) {
  if (!query) {
    stockOptions.value = []
    return
  }
  stockSearchLoading.value = true
  try {
    const response = await request.get<{ items: StockOption[] }>('/data/stocks', {
      params: { search: query, page: 1, page_size: 20 },
    })
    stockOptions.value = response.items || []
  } finally {
    stockSearchLoading.value = false
  }
}

async function loadOverview() {
  loadingOverview.value = true
  try {
    overview.value = await sentimentApi.overview()
  } finally {
    loadingOverview.value = false
  }
}

async function loadSummary() {
  if (!selectedSymbol.value) {
    summary.value = null
    return
  }
  loadingSummary.value = true
  try {
    summary.value = await sentimentApi.summary(selectedSymbol.value, buildQueryParams())
  } finally {
    loadingSummary.value = false
  }
}

async function loadPosts() {
  if (!selectedSymbol.value) {
    posts.value = []
    return
  }
  loadingPosts.value = true
  try {
    posts.value = await sentimentApi.posts(selectedSymbol.value, {
      ...buildQueryParams(),
      limit: 100,
    })
  } finally {
    loadingPosts.value = false
  }
}

async function reloadSnapshot() {
  if (!selectedSymbol.value) {
    ElMessage.warning('请先选择股票')
    return
  }
  if (!selectedSources.value.length) {
    ElMessage.warning('请至少选择一个来源')
    return
  }
  await Promise.all([loadSummary(), loadPosts()])
}

async function reloadAll() {
  await loadOverview()
  if (selectedSymbol.value) {
    await reloadSnapshot()
  }
}

function normalizeBatchResult(
  result: SentimentIngestBatchResult | SentimentIngestSourceResult,
): SentimentIngestBatchResult {
  if ('results' in result) {
    return result
  }
  const ok = result.ok !== false
  return {
    symbol: result.symbol,
    requested_sources: [result.source],
    succeeded_sources: ok ? [result.source] : [],
    failed_sources: ok ? [] : [result.source],
    all_succeeded: ok,
    total_upserted: Number(result.upserted || 0),
    total_collected: Number(result.collected || 0),
    total_matched: Number(result.matched || 0),
    results: [{ ok, ...result }],
  }
}

async function runIngest() {
  if (requiresSymbolForIngest.value && !selectedSymbol.value) {
    ElMessage.warning('请先选择股票')
    return
  }
  if (!selectedSources.value.length) {
    ElMessage.warning('请至少选择一个来源')
    return
  }

  const missingProjects = sourceCards.value
    .filter(source => selectedSources.value.includes(source.source) && !source.project_ready)
    .map(source => `${sourceLabel(source.source)}: ${source.project_dir || '未配置目录'}`)
  if (missingProjects.length) {
    ElMessage.error(`抓取器目录缺失：${missingProjects.join('；')}`)
    return
  }

  ingesting.value = true
  try {
    const [start_date, end_date] = Array.isArray(dateRange.value) ? dateRange.value : []
    const result = await sentimentApi.ingest({
      symbol: selectedSymbol.value || undefined,
      sources: selectedSources.value,
      max_pages: maxPages.value,
      min_reply: minReply.value,
      start_date: start_date || undefined,
      end_date: end_date || undefined,
      force_refresh: forceRefresh.value,
    })
    const normalized = normalizeBatchResult(result)
    lastIngest.value = normalized

    if (normalized.failed_sources.length && normalized.succeeded_sources.length) {
      ElMessage.warning(
        `部分完成：成功 ${normalized.succeeded_sources.join(', ')}，失败 ${normalized.failed_sources.join(', ')}`,
      )
    } else if (normalized.failed_sources.length) {
      ElMessage.error(`抓取失败：${normalized.failed_sources.join(', ')}`)
    } else {
      ElMessage.success(`抓取完成，新增/更新 ${normalized.total_upserted} 条情绪记录`)
    }

    await reloadAll()
  } finally {
    ingesting.value = false
  }
}

onMounted(() => {
  void reloadAll()
})
</script>

<style scoped>
.sentiment-workbench {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 16px;
  color: var(--text-primary);
  background:
    linear-gradient(140deg, rgba(56, 189, 248, 0.05), transparent 24%),
    linear-gradient(320deg, rgba(251, 191, 36, 0.05), transparent 24%),
    #0d141d;
  min-height: 100%;
}

.sentiment-head {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  align-items: flex-start;
  padding: 20px 22px;
  border-radius: 18px;
  background: linear-gradient(180deg, rgba(17, 27, 39, 0.96), rgba(11, 18, 27, 0.96));
  border: 1px solid rgba(95, 123, 155, 0.28);
}

.eyebrow {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.18em;
  color: #73d2ff;
}

.sentiment-head h2 {
  margin: 6px 0 8px;
  font-size: 26px;
  line-height: 1.1;
  color: #f5f8ff;
}

.sentiment-head p {
  margin: 0;
  max-width: 780px;
  line-height: 1.7;
  color: #99abc0;
}

.sentiment-actions {
  display: flex;
  gap: 10px;
  flex-shrink: 0;
}

.status-strip,
.result-strip {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.status-card {
  padding: 14px 16px;
  border-radius: 14px;
  background: rgba(14, 22, 33, 0.92);
  border: 1px solid rgba(92, 118, 147, 0.22);
}

.status-card span {
  display: block;
  font-size: 12px;
  color: #7e95ae;
  margin-bottom: 6px;
}

.status-card strong {
  font-size: 18px;
  color: #eef4ff;
}

.status-card--accent {
  background: linear-gradient(135deg, rgba(18, 48, 73, 0.94), rgba(17, 31, 47, 0.94));
}

.layout-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.7fr) minmax(320px, 1fr);
  gap: 16px;
}

.panel {
  background: rgba(12, 19, 28, 0.94);
  border: 1px solid rgba(95, 123, 155, 0.22);
  border-radius: 18px;
  padding: 18px;
}

.panel-title {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;
}

.panel-title h3 {
  margin: 0;
  font-size: 18px;
  color: #f4f7fd;
}

.panel-title span {
  color: #8096af;
  font-size: 13px;
}

.control-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px 16px;
}

.control-grid label {
  display: flex;
  flex-direction: column;
  gap: 8px;
  color: #d8e3ef;
  font-size: 13px;
}

.control-grid label.wide {
  grid-column: 1 / -1;
}

.field-hint {
  color: #7f93ab;
  font-size: 12px;
  line-height: 1.5;
}

.toggle-row {
  flex-direction: row !important;
  align-items: center;
  justify-content: space-between;
}

.control-actions {
  display: flex;
  gap: 10px;
  margin-top: 16px;
}

.source-checkboxes {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.source-card-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.source-card {
  padding: 14px;
  border-radius: 14px;
  background: linear-gradient(180deg, rgba(18, 28, 41, 0.92), rgba(12, 18, 27, 0.92));
  border: 1px solid rgba(94, 118, 147, 0.2);
}

.source-card--ready {
  border-color: rgba(34, 197, 94, 0.28);
  box-shadow: inset 0 0 0 1px rgba(34, 197, 94, 0.08);
}

.source-card--muted {
  opacity: 0.9;
}

.source-card__head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: flex-start;
}

.source-card__head strong {
  display: block;
  color: #f4f7fd;
}

.source-card__head small {
  color: #7f93ab;
}

.source-card__meta {
  list-style: none;
  padding: 0;
  margin: 12px 0 0;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px 12px;
  color: #a8bad0;
  font-size: 12px;
}

.summary-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}

.summary-card {
  border-radius: 16px;
  background: linear-gradient(180deg, rgba(18, 28, 42, 0.94), rgba(11, 17, 25, 0.94));
  border: 1px solid rgba(91, 116, 144, 0.22);
  padding: 16px;
}

.summary-card__head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  margin-bottom: 14px;
}

.summary-card__head strong {
  color: #f4f7fd;
}

.summary-card__head span {
  color: #7f93ab;
  font-size: 12px;
}

.summary-card__metrics {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px 14px;
}

.summary-card__metrics span {
  display: block;
  font-size: 12px;
  color: #7e95ae;
}

.summary-card__metrics strong {
  display: block;
  margin-top: 4px;
  color: #eef4ff;
}

.summary-card__metrics .bull {
  color: var(--market-up);
}

.summary-card__metrics .bear {
  color: var(--market-down);
}

.summary-card__keywords {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 14px;
}

.keyword-chip {
  display: inline-flex;
  align-items: center;
  padding: 5px 9px;
  border-radius: 999px;
  background: rgba(40, 58, 80, 0.86);
  color: #c7d6e7;
  font-size: 12px;
}

.keyword-chip--ghost {
  color: #7e95ae;
  background: rgba(28, 39, 53, 0.8);
}

.hot-posts {
  margin-top: 18px;
}

.hot-posts__head {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  margin-bottom: 12px;
}

.hot-posts__head strong {
  color: #f4f7fd;
}

.hot-posts__head span {
  color: #8096af;
  font-size: 12px;
}

.hot-posts__list {
  display: grid;
  gap: 12px;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.hot-post {
  padding: 14px;
  border-radius: 14px;
  background: rgba(16, 25, 37, 0.96);
  border: 1px solid rgba(91, 116, 144, 0.18);
}

.hot-post__meta,
.hot-post__footer {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: center;
  color: #7f93ab;
  font-size: 12px;
}

.hot-post__title {
  display: block;
  margin: 12px 0 8px;
  color: #eef4ff;
  line-height: 1.45;
}

.hot-post__content {
  margin: 0 0 12px;
  color: #aebfd3;
  line-height: 1.6;
  font-size: 13px;
}

.post-main {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.post-main strong {
  color: #eef4ff;
  font-weight: 600;
}

.post-main small {
  color: #9fb2c7;
}

.sentiment-table :deep(.el-table__cell) {
  background: transparent;
}

.sentiment-table :deep(.el-table tr) {
  background: rgba(0, 0, 0, 0);
}

.sentiment-table :deep(.el-table th.el-table__cell) {
  background: rgba(17, 27, 39, 0.96);
  color: #d4deeb;
}

.sentiment-table :deep(.el-table td.el-table__cell) {
  color: #d5e0ec;
  border-bottom-color: rgba(95, 123, 155, 0.16);
}

a {
  color: #7ad3ff;
  text-decoration: none;
}

a:hover {
  color: #a5e3ff;
}

:deep(.el-input__wrapper),
:deep(.el-select__wrapper),
:deep(.el-textarea__inner),
:deep(.el-input-number__decrease),
:deep(.el-input-number__increase) {
  background: rgba(15, 24, 35, 0.96);
  border-color: rgba(97, 124, 153, 0.24);
  box-shadow: none;
}

:deep(.el-input__inner),
:deep(.el-select__selected-item),
:deep(.el-textarea__inner) {
  color: #eef4ff;
}

:deep(.el-date-editor),
:deep(.el-input-number) {
  width: 100%;
}

:deep(.el-checkbox-button__inner) {
  background: rgba(15, 24, 35, 0.96);
  border-color: rgba(97, 124, 153, 0.24);
  color: #d6e2ef;
}

:deep(.el-checkbox-button.is-checked .el-checkbox-button__inner) {
  background: #5bc4ff;
  border-color: #5bc4ff;
  color: #07111c;
}

@media (max-width: 1200px) {
  .layout-grid,
  .summary-grid,
  .hot-posts__list,
  .status-strip,
  .result-strip {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 900px) {
  .sentiment-head {
    flex-direction: column;
  }

  .sentiment-actions,
  .control-actions {
    width: 100%;
    flex-wrap: wrap;
  }

  .control-grid {
    grid-template-columns: 1fr;
  }

  .source-card__meta {
    grid-template-columns: 1fr;
  }
}
</style>

<template>
  <div class="watchlist-page theme-pine-quant">
    <header class="watchlist-command-bar">
      <div class="brand-left">
        <span class="section-kicker">PORTFOLIO POOL TRACKING</span>
        <h2>自选股管理</h2>
        <p>按市值、行业、概念和主力资金流快速扫描自选池。</p>
      </div>

      <div class="actions-right">
        <button class="icon-button" @click="showCreateDialog = true" title="新建分组" aria-label="新建分组">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 5v14M5 12h14" />
          </svg>
        </button>
      </div>
    </header>

    <div class="watchlist-body">
      <aside class="group-panel">
        <div class="group-panel__header">
          <span class="panel-kicker">GROUPS</span>
          <strong>{{ groups.length }}</strong>
        </div>

        <div class="group-list">
          <button
            v-for="group in groups"
            :key="group.id"
            class="group-item"
            :class="{ 'group-item--active': selectedGroupId === group.id }"
            @click="selectGroup(group.id)"
          >
            <span class="group-item__info">
              <span class="group-item__name">{{ group.name }}</span>
              <span class="group-item__count">{{ group.stock_count ?? 0 }}只</span>
            </span>
            <span
              class="group-item__delete"
              role="button"
              tabindex="0"
              title="删除分组"
              @click.stop="confirmDeleteGroup(group)"
              @keydown.enter.stop="confirmDeleteGroup(group)"
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                <path d="M18 6L6 18M6 6l12 12" />
              </svg>
            </span>
          </button>

          <div v-if="!groups.length" class="empty-hint empty-hint--compact">
            暂无分组，点击右上角 + 创建
          </div>
        </div>
      </aside>

      <main class="stock-panel">
        <div v-if="!selectedGroupId" class="stock-panel__placeholder">
          <div class="placeholder-icon">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" width="48" height="48">
              <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z" />
            </svg>
          </div>
          <p>选择左侧分组查看自选股热力图</p>
        </div>

        <template v-else>
          <section class="metric-strip">
            <div class="metric-card">
              <span>股票总数</span>
              <strong>{{ stocks.length }} 只</strong>
            </div>
            <div class="metric-card">
              <span>市值覆盖率</span>
              <strong>{{ marketValueCoverage }}</strong>
            </div>
            <div class="metric-card">
              <span>平均涨跌幅</span>
              <strong :class="toneClass(averageChangePct)">{{ formatPercent(averageChangePct) }}</strong>
            </div>
            <div class="metric-card">
              <span>主力净流入</span>
              <strong :class="toneClass(totalMainInflow)">{{ formatMoneyflowAmount(totalMainInflow) }}</strong>
            </div>
            <div class="metric-card">
              <span>领涨个股</span>
              <strong>{{ topMoverLabel }}</strong>
            </div>
            <div class="metric-card">
              <span>最新交易日</span>
              <strong>{{ latestTradeDate || '-' }}</strong>
            </div>
          </section>

          <section class="control-row">
            <div class="control-block">
              <span class="control-label">热力维度</span>
              <div class="segmented-control">
                <button
                  v-for="option in heatDimensionOptions"
                  :key="option.value"
                  class="segment-button"
                  :class="{ active: heatDimension === option.value }"
                  @click="setHeatDimension(option.value)"
                >
                  {{ option.label }}
                </button>
              </div>
            </div>

            <div class="control-block">
              <span class="control-label">面积大小</span>
              <select v-model="sizeMetric" class="control-select">
                <option value="circ_mv">流通市值</option>
                <option value="total_mv">总市值</option>
                <option value="equal">等权重</option>
                <option value="moneyflow">主力净流入规模</option>
              </select>
            </div>

            <div class="control-block">
              <span class="control-label">颜色指标</span>
              <select v-model="colorMetric" class="control-select">
                <option value="change_pct">今日涨跌幅</option>
                <option value="moneyflow">主力净流入</option>
              </select>
            </div>

            <div class="control-block control-block--search">
              <input
                v-model="searchText"
                class="input-mini input-mini--wide"
                type="text"
                placeholder="搜索代码、名称、行业、概念"
              />
            </div>

            <div class="control-block control-block--add">
              <input
                v-model="addSymbol"
                type="text"
                class="input-mini"
                placeholder="代码如 600519.SH"
                @keyup.enter="handleAddStock"
              />
              <button class="btn-pine btn--small" @click="handleAddStock" :disabled="!addSymbol.trim()">添加</button>
              <button class="ghost-button" @click="refreshCurrentGroup" :disabled="loadingStocks">刷新</button>
            </div>
          </section>

          <div v-if="loadingStocks" class="loading-state">
            <div class="skeleton-map">
              <span v-for="idx in 12" :key="idx" />
            </div>
            <span>加载自选股行情与资金流...</span>
          </div>

          <div v-else-if="!stocks.length" class="empty-hint">
            该分组暂无股票，使用上方输入框添加
          </div>

          <template v-else>
            <section class="heatmap-workspace">
              <div class="treemap-panel">
                <div class="treemap-panel__head">
                  <div>
                    <span class="panel-kicker">{{ currentGroup?.name || '自选股' }}</span>
                    <h3>{{ activeDimensionTitle }}</h3>
                  </div>
                  <div class="legend-bar">
                    <span>{{ colorMetric === 'moneyflow' ? '净流出' : '下跌' }}</span>
                    <i class="legend-gradient"></i>
                    <span>{{ colorMetric === 'moneyflow' ? '净流入' : '上涨' }}</span>
                  </div>
                </div>

                <div ref="treemapEl" class="treemap-canvas">
                  <div
                    v-for="frame in industryFrames"
                    :key="frame.key"
                    class="industry-frame"
                    :style="industryFrameStyle(frame)"
                  >
                    <span>{{ frame.label }} · {{ frame.count }}只</span>
                  </div>

                  <button
                    v-for="item in layoutItems"
                    :key="item.key"
                    class="treemap-node"
                    :class="{
                      'treemap-node--focused': focusedKey === item.key,
                      'treemap-node--missing': item.missingValue,
                      'treemap-node--tiny': item.w < 90 || item.h < 62,
                    }"
                    :style="tileStyle(item)"
                    :title="item.tooltip"
                    @mouseenter="focusedKey = item.key"
                    @focus="focusedKey = item.key"
                    @click="handleTileClick(item)"
                    @keydown.enter="handleTileClick(item)"
                  >
                    <span class="node-symbol">{{ item.symbolLabel }}</span>
                    <span class="node-name">{{ item.label }}</span>
                    <span class="node-value">{{ item.displayValue }}</span>
                    <span class="node-meta">{{ item.meta }}</span>
                  </button>

                  <div v-if="!layoutItems.length" class="empty-map">
                    未找到匹配的热力图项目
                  </div>
                </div>
              </div>

              <aside class="right-rail">
                <section class="rail-card">
                  <span class="rail-title">焦点数据</span>
                  <div v-if="focusedItem" class="focus-card">
                    <div class="focus-card__title">
                      <strong>{{ focusedItem.label }}</strong>
                      <span>{{ focusedItem.symbolLabel }}</span>
                    </div>
                    <div class="detail-row">
                      <span>涨跌幅</span>
                      <strong :class="toneClass(focusedItem.changePct)">{{ formatPercent(focusedItem.changePct) }}</strong>
                    </div>
                    <div class="detail-row">
                      <span>主力净流入</span>
                      <strong :class="toneClass(focusedItem.mainInflow)">{{ formatMoneyflowAmount(focusedItem.mainInflow) }}</strong>
                    </div>
                    <div class="detail-row">
                      <span>超大单</span>
                      <strong :class="toneClass(focusedItem.extraLargeInflow)">{{ formatMoneyflowAmount(focusedItem.extraLargeInflow) }}</strong>
                    </div>
                    <div class="detail-row">
                      <span>大单</span>
                      <strong :class="toneClass(focusedItem.largeInflow)">{{ formatMoneyflowAmount(focusedItem.largeInflow) }}</strong>
                    </div>
                    <div class="detail-row">
                      <span>成员/市值</span>
                      <strong>{{ focusedItem.memberCount }} / {{ formatMarketValue(focusedItem.marketValue) }}</strong>
                    </div>
                  </div>
                </section>

                <section class="rail-card">
                  <span class="rail-title">{{ rankingTitle }}</span>
                  <div class="rank-list">
                    <button
                      v-for="(item, index) in primaryRanking"
                      :key="item.key"
                      class="rank-item"
                      @mouseenter="focusedKey = item.key"
                      @focus="focusedKey = item.key"
                      @click="handleTileClick(item)"
                    >
                      <span class="rank-badge" :class="rankBadgeTone(item)">
                        {{ index + 1 }}
                      </span>
                      <span class="rank-name">{{ item.label }}</span>
                      <strong :class="colorMetric === 'moneyflow' ? toneClass(item.mainInflow) : toneClass(item.changePct)">
                        {{ colorMetric === 'moneyflow' ? formatMoneyflowAmount(item.mainInflow) : formatPercent(item.changePct) }}
                      </strong>
                    </button>
                  </div>
                </section>

                <section class="rail-card">
                  <span class="rail-title">资金维度预览</span>
                  <div class="rank-list">
                    <button
                      v-for="item in moneyflowPreview"
                      :key="'money-' + item.key"
                      class="dimension-chip"
                      @mouseenter="focusedKey = item.key"
                      @focus="focusedKey = item.key"
                      @click="handleTileClick(item)"
                    >
                      <span>
                        <strong>{{ item.label }}</strong>
                        <small>超大 {{ formatMoneyflowAmount(item.extraLargeInflow) }} · 大单 {{ formatMoneyflowAmount(item.largeInflow) }}</small>
                      </span>
                      <strong :class="toneClass(item.mainInflow)">{{ formatMoneyflowAmount(item.mainInflow) }}</strong>
                    </button>
                  </div>
                </section>

                <section class="rail-card">
                  <span class="rail-title">数据缺口</span>
                  <div v-if="dataGaps.length" class="gap-list">
                    <div v-for="gap in dataGaps" :key="gap.key" class="gap-item">
                      <strong>{{ gap.label }}</strong>
                      <span>{{ gap.reason }}</span>
                    </div>
                  </div>
                  <div v-else class="rail-empty">当前分组关键字段覆盖良好</div>
                </section>
              </aside>
            </section>

            <section class="table-panel">
              <div class="table-panel__head">
                <span class="panel-kicker">DETAIL TABLE</span>
                <strong>{{ filteredStocks.length }} / {{ stocks.length }} 只</strong>
              </div>
              <div class="stock-table-wrap">
                <table class="stock-table">
                  <thead>
                    <tr>
                      <th>股票</th>
                      <th>行业</th>
                      <th class="text-right">涨跌幅</th>
                      <th class="text-right">流通市值</th>
                      <th class="text-right">主力净流入</th>
                      <th class="text-right">资金日期</th>
                      <th class="text-right">操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="stock in filteredStocks" :key="stock.id" class="stock-row">
                      <td>
                        <router-link :to="`/stock/${stock.symbol}`" class="symbol-link">{{ stock.symbol }}</router-link>
                        <span class="stock-name">{{ stock.stock_name || '-' }}</span>
                      </td>
                      <td>{{ stock.industry || stock.industry2 || '-' }}</td>
                      <td class="text-right font-data" :class="toneClass(stock.change_pct)">{{ formatPercent(stock.change_pct) }}</td>
                      <td class="text-right font-data">{{ formatMarketValue(stock.circ_mv ?? stock.total_mv) }}</td>
                      <td class="text-right font-data" :class="toneClass(getMainNetInflow(stock))">{{ formatMoneyflowAmount(getMainNetInflow(stock)) }}</td>
                      <td class="text-right font-data">{{ stock.moneyflow_trade_date || '-' }}</td>
                      <td class="text-right">
                        <button class="btn-text-danger" @click="handleRemoveStock(stock.symbol)">移除</button>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </section>
          </template>
        </template>
      </main>
    </div>

    <div v-if="showCreateDialog" class="modal-overlay" @click.self="showCreateDialog = false">
      <div class="modal">
        <h3 class="modal__title">新建自选股分组</h3>
        <div class="modal__body">
          <label class="form-label">分组名称</label>
          <input v-model="newGroupName" type="text" class="input" placeholder="例如：白马股、高股息..." @keyup.enter="handleCreateGroup" />
          <label class="form-label form-label--spaced">描述（可选）</label>
          <input v-model="newGroupDesc" type="text" class="input" placeholder="分组描述" />
        </div>
        <div class="modal__footer">
          <button class="ghost-button" @click="showCreateDialog = false">取消</button>
          <button class="btn-pine" @click="handleCreateGroup" :disabled="!newGroupName.trim()">创建</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { watchlistApi, type WatchlistGroup, type WatchlistStock } from '@/api/data'
import { usePageContext } from '@/app/pageContext'

type HeatDimension = 'stock' | 'industry' | 'concept' | 'money'
type SizeMetric = 'circ_mv' | 'total_mv' | 'equal' | 'moneyflow'
type ColorMetric = 'change_pct' | 'moneyflow'

interface HeatmapItem {
  key: string
  label: string
  symbolLabel: string
  displayValue: string
  meta: string
  tooltip: string
  weight: number
  colorValue: number | null
  colorMetric: ColorMetric
  missingValue: boolean
  memberCount: number
  marketValue: number | null
  changePct: number | null
  mainInflow: number | null
  extraLargeInflow: number | null
  largeInflow: number | null
  symbols: string[]
  groupLabel?: string
  stock?: WatchlistStock
}

interface LayoutItem extends HeatmapItem {
  x: number
  y: number
  w: number
  h: number
}

interface LayoutFrame {
  key: string
  label: string
  x: number
  y: number
  w: number
  h: number
  count: number
}

interface DataGap {
  key: string
  label: string
  reason: string
}

const router = useRouter()

const groups = ref<WatchlistGroup[]>([])
const selectedGroupId = ref<number | null>(null)
const stocks = ref<WatchlistStock[]>([])
const loadingStocks = ref(false)
const showCreateDialog = ref(false)
const newGroupName = ref('')
const newGroupDesc = ref('')
const addSymbol = ref('')
const searchText = ref('')
const heatDimension = ref<HeatDimension>('stock')
const sizeMetric = ref<SizeMetric>('circ_mv')
const colorMetric = ref<ColorMetric>('change_pct')
const focusedKey = ref<string | null>(null)
const treemapEl = ref<HTMLElement | null>(null)
const treemapSize = ref({ width: 900, height: 560 })

let resizeObserver: ResizeObserver | null = null

const heatDimensionOptions: Array<{ value: HeatDimension; label: string }> = [
  { value: 'stock', label: '股票' },
  { value: 'industry', label: '行业' },
  { value: 'concept', label: '概念' },
  { value: 'money', label: '资金' },
]

const currentGroup = computed(() => groups.value.find(group => group.id === selectedGroupId.value))

const filteredStocks = computed(() => {
  const keyword = searchText.value.trim().toLowerCase()
  if (!keyword) return stocks.value
  return stocks.value.filter(stock => {
    const haystack = [
      stock.symbol,
      stock.stock_name,
      stock.industry,
      stock.industry2,
      stock.industry3,
      stock.sector,
      stock.concept,
      ...(stock.ths_concepts || []),
    ]
      .filter(Boolean)
      .join(' ')
      .toLowerCase()
    return haystack.includes(keyword)
  })
})

const activeDimensionTitle = computed(() => {
  if (heatDimension.value === 'industry') return '行业热力图'
  if (heatDimension.value === 'concept') return '概念热力图'
  if (heatDimension.value === 'money') return '资金热力：超大单 + 大单净流入'
  return '个股市值热力图 · 行业聚合'
})

const heatmapItems = computed<HeatmapItem[]>(() => {
  if (heatDimension.value === 'industry') return buildAggregateItems('industry')
  if (heatDimension.value === 'concept') return buildAggregateItems('concept')
  return filteredStocks.value.map(stock => stockToHeatmapItem(stock, heatDimension.value === 'money'))
})

const layoutResult = computed(() => (
  heatDimension.value === 'stock'
    ? layoutStockTreemapByIndustry(heatmapItems.value, treemapSize.value.width, treemapSize.value.height)
    : { items: layoutTreemap(heatmapItems.value, treemapSize.value.width, treemapSize.value.height), frames: [] }
))

const layoutItems = computed<LayoutItem[]>(() => layoutResult.value.items)
const industryFrames = computed<LayoutFrame[]>(() => layoutResult.value.frames)

const focusedItem = computed(() => {
  if (!layoutItems.value.length) return null
  return layoutItems.value.find(item => item.key === focusedKey.value) || layoutItems.value[0]
})

const averageChangePct = computed(() => average(stocks.value.map(stock => stock.change_pct)))

const latestTradeDate = computed(() => {
  const dates = stocks.value.map(stock => stock.latest_trade_date).filter(Boolean) as string[]
  return dates.sort().at(-1) || null
})

const marketValueCoverage = computed(() => {
  if (!stocks.value.length) return '-'
  const covered = stocks.value.filter(stock => getMarketValue(stock, 'circ_mv') || getMarketValue(stock, 'total_mv')).length
  return `${((covered / stocks.value.length) * 100).toFixed(0)}%`
})

const totalMainInflow = computed(() => {
  const values = stocks.value.map(stock => getMainNetInflow(stock)).filter(isFiniteNumber)
  if (!values.length) return null
  return values.reduce((sum, value) => sum + value, 0)
})

const topMoverLabel = computed(() => {
  const ranked = [...stocks.value]
    .filter(stock => isFiniteNumber(stock.change_pct))
    .sort((left, right) => Math.abs(right.change_pct || 0) - Math.abs(left.change_pct || 0))
  const top = ranked[0]
  return top ? top.stock_name || top.symbol : '-'
})

const primaryRanking = computed(() => {
  const metric = colorMetric.value
  return [...heatmapItems.value]
    .filter(item => metric === 'moneyflow' ? isFiniteNumber(item.mainInflow) : isFiniteNumber(item.changePct))
    .sort((left, right) => {
      const leftValue = metric === 'moneyflow' ? Math.abs(left.mainInflow || 0) : Math.abs(left.changePct || 0)
      const rightValue = metric === 'moneyflow' ? Math.abs(right.mainInflow || 0) : Math.abs(right.changePct || 0)
      return rightValue - leftValue
    })
    .slice(0, 6)
})

const rankingTitle = computed(() => colorMetric.value === 'moneyflow' ? '资金异动排行' : '涨跌异动排行')

const moneyflowPreview = computed(() => (
  filteredStocks.value
    .map(stock => stockToHeatmapItem(stock, true))
    .filter(item => isFiniteNumber(item.mainInflow))
    .sort((left, right) => Math.abs(right.mainInflow || 0) - Math.abs(left.mainInflow || 0))
    .slice(0, 4)
))

const dataGaps = computed<DataGap[]>(() => {
  const gaps: DataGap[] = []
  for (const stock of stocks.value) {
    const label = stock.stock_name || stock.symbol
    if (!getMarketValue(stock, 'circ_mv') && !getMarketValue(stock, 'total_mv')) {
      gaps.push({ key: `${stock.symbol}-mv`, label, reason: '缺少市值' })
    }
    if (!isFiniteNumber(stock.change_pct)) {
      gaps.push({ key: `${stock.symbol}-change`, label, reason: '缺少涨跌幅' })
    }
    if (!isFiniteNumber(getMainNetInflow(stock))) {
      gaps.push({ key: `${stock.symbol}-money`, label, reason: '缺少资金流' })
    }
    if (!stock.industry && !stock.industry2) {
      gaps.push({ key: `${stock.symbol}-industry`, label, reason: '缺少行业' })
    }
    if (!(stock.ths_concepts || []).length && !stock.concept) {
      gaps.push({ key: `${stock.symbol}-concept`, label, reason: '缺少概念' })
    }
  }
  return gaps.slice(0, 8)
})

const pageContextBlocks = computed(() => [
  {
    title: 'Watchlist Heatmap',
    rows: [
      { label: '热力维度', value: heatDimensionOptions.find(option => option.value === heatDimension.value)?.label || '-' },
      { label: '当前分组', value: currentGroup.value?.name || '未选择' },
      { label: '股票数量', value: selectedGroupId.value ? `${stocks.value.length} 只` : '-' },
      { label: '加载状态', value: loadingStocks.value ? '加载中' : '已就绪', tone: loadingStocks.value ? 'warn' : 'good' },
    ],
  },
  {
    title: 'Market Snapshot',
    rows: [
      { label: '平均涨跌', value: formatPercent(averageChangePct.value), tone: toneName(averageChangePct.value) },
      { label: '主力净流入', value: formatMoneyflowAmount(totalMainInflow.value), tone: toneName(totalMainInflow.value) },
      { label: '交易日', value: latestTradeDate.value || '-' },
    ],
  },
])

usePageContext(pageContextBlocks)

onMounted(async () => {
  await loadGroups()
  await nextTick()
  attachResizeObserver()
})

onBeforeUnmount(() => {
  resizeObserver?.disconnect()
})

async function loadGroups() {
  try {
    const loaded = await watchlistApi.getGroups()
    groups.value = loaded
    if (!selectedGroupId.value && loaded.length) {
      selectedGroupId.value = loaded[0].id
      await loadStocks()
    } else if (selectedGroupId.value && !loaded.some(group => group.id === selectedGroupId.value)) {
      selectedGroupId.value = loaded[0]?.id ?? null
      stocks.value = []
      if (selectedGroupId.value) await loadStocks()
    }
  } catch (e: any) {
    ElMessage.error('加载分组失败: ' + (e.message || ''))
  }
}

async function loadStocks() {
  if (!selectedGroupId.value) return
  loadingStocks.value = true
  try {
    stocks.value = await watchlistApi.getGroupStocks(selectedGroupId.value)
    focusedKey.value = null
  } catch (e: any) {
    ElMessage.error('加载股票失败: ' + (e.message || ''))
  } finally {
    loadingStocks.value = false
  }
}

async function selectGroup(id: number) {
  selectedGroupId.value = id
  searchText.value = ''
  await loadStocks()
}

async function refreshCurrentGroup() {
  await loadStocks()
  await loadGroups()
}

async function handleCreateGroup() {
  const name = newGroupName.value.trim()
  if (!name) return
  try {
    const group = await watchlistApi.createGroup({ name, description: newGroupDesc.value.trim() || undefined })
    showCreateDialog.value = false
    newGroupName.value = ''
    newGroupDesc.value = ''
    await loadGroups()
    selectedGroupId.value = group.id
    stocks.value = []
    ElMessage.success('分组创建成功')
  } catch (e: any) {
    ElMessage.error('创建失败: ' + (e.message || ''))
  }
}

async function confirmDeleteGroup(group: WatchlistGroup) {
  try {
    await ElMessageBox.confirm(`确定删除分组「${group.name}」及其所有股票？`, '删除确认', {
      confirmButtonText: '删除',
      cancelButtonText: '取消',
      type: 'warning',
    })
  } catch {
    return
  }
  try {
    await watchlistApi.deleteGroup(group.id)
    if (selectedGroupId.value === group.id) {
      selectedGroupId.value = null
      stocks.value = []
    }
    await loadGroups()
    ElMessage.success('分组已删除')
  } catch (e: any) {
    ElMessage.error('删除失败: ' + (e.message || ''))
  }
}

async function handleAddStock() {
  const symbol = addSymbol.value.trim().toUpperCase()
  if (!symbol || !selectedGroupId.value) return
  try {
    await watchlistApi.addStock(selectedGroupId.value, symbol)
    addSymbol.value = ''
    await loadStocks()
    await loadGroups()
    ElMessage.success(`${symbol} 已添加`)
  } catch (e: any) {
    ElMessage.error('添加失败: ' + (e.message || ''))
  }
}

async function handleRemoveStock(symbol: string) {
  if (!selectedGroupId.value) return
  try {
    await watchlistApi.removeStock(selectedGroupId.value, symbol)
    await loadStocks()
    await loadGroups()
    ElMessage.success(`${symbol} 已移除`)
  } catch (e: any) {
    ElMessage.error('移除失败: ' + (e.message || ''))
  }
}

function setHeatDimension(dimension: HeatDimension) {
  heatDimension.value = dimension
  focusedKey.value = null
  if (dimension === 'money') {
    sizeMetric.value = 'moneyflow'
    colorMetric.value = 'moneyflow'
    return
  }
  if (sizeMetric.value === 'moneyflow') sizeMetric.value = 'circ_mv'
  if (colorMetric.value === 'moneyflow') colorMetric.value = 'change_pct'
}

function attachResizeObserver() {
  if (!treemapEl.value) return
  const update = () => {
    if (!treemapEl.value) return
    const rect = treemapEl.value.getBoundingClientRect()
    treemapSize.value = {
      width: Math.max(rect.width, 320),
      height: Math.max(rect.height, 360),
    }
  }
  update()
  resizeObserver = new ResizeObserver(update)
  resizeObserver.observe(treemapEl.value)
}

function stockToHeatmapItem(stock: WatchlistStock, forceMoneyMode: boolean): HeatmapItem {
  const mainInflow = getMainNetInflow(stock)
  const extraLargeInflow = getExtraLargeInflow(stock)
  const largeInflow = getLargeInflow(stock)
  const marketValue = getMarketValue(stock, 'circ_mv') ?? getMarketValue(stock, 'total_mv')
  const useMoneyColor = forceMoneyMode || colorMetric.value === 'moneyflow'
  const weight = forceMoneyMode || sizeMetric.value === 'moneyflow'
    ? Math.abs(mainInflow || 0) || 1
    : getWeight(stock)
  const colorValue: number | null = useMoneyColor
    ? (isFiniteNumber(stock.net_pct_main) ? stock.net_pct_main : mainInflow)
    : (stock.change_pct ?? null)
  const displayValue = forceMoneyMode
    ? `主力 ${formatMoneyflowAmount(mainInflow)}`
    : useMoneyColor
      ? formatMoneyflowAmount(mainInflow)
      : formatPercent(stock.change_pct)
  const meta = forceMoneyMode
    ? `超大 ${formatMoneyflowAmount(extraLargeInflow)} · 大单 ${formatMoneyflowAmount(largeInflow)}`
    : formatMarketValue(marketValue)

  return {
    key: `stock-${stock.symbol}`,
    label: stock.stock_name || stock.symbol,
    symbolLabel: stock.symbol,
    displayValue,
    meta,
    tooltip: [
      stock.symbol,
      stock.stock_name || '',
      `涨跌幅 ${formatPercent(stock.change_pct)}`,
      `市值 ${formatMarketValue(marketValue)}`,
      `主力净流入 ${formatMoneyflowAmount(mainInflow)}`,
      `资金日期 ${stock.moneyflow_trade_date || '-'}`,
    ].filter(Boolean).join('\n'),
    weight,
    colorValue,
    colorMetric: useMoneyColor ? 'moneyflow' : 'change_pct',
    missingValue: useMoneyColor ? !isFiniteNumber(mainInflow) : !isFiniteNumber(stock.change_pct),
    memberCount: 1,
    marketValue,
    changePct: stock.change_pct ?? null,
    mainInflow,
    extraLargeInflow,
    largeInflow,
    symbols: [stock.symbol],
    groupLabel: industryKey(stock),
    stock,
  }
}

function buildAggregateItems(kind: 'industry' | 'concept'): HeatmapItem[] {
  const buckets = new Map<string, WatchlistStock[]>()
  for (const stock of filteredStocks.value) {
    const keys = kind === 'industry' ? [stock.industry || stock.industry2 || '未分行业'] : conceptKeys(stock)
    for (const key of keys) {
      const bucket = buckets.get(key) || []
      bucket.push(stock)
      buckets.set(key, bucket)
    }
  }

  return [...buckets.entries()].map(([key, members]) => {
    const marketValue = sumNumbers(members.map(stock => getMarketValue(stock, 'circ_mv') ?? getMarketValue(stock, 'total_mv')))
    const weightedChange = weightedAverage(
      members.map(stock => ({ value: stock.change_pct, weight: getMarketValue(stock, 'circ_mv') ?? getMarketValue(stock, 'total_mv') ?? 1 })),
    )
    const mainInflow = sumNumbers(members.map(stock => getMainNetInflow(stock)))
    const extraLargeInflow = sumNumbers(members.map(stock => getExtraLargeInflow(stock)))
    const largeInflow = sumNumbers(members.map(stock => getLargeInflow(stock)))
    const leader = [...members].sort((left, right) => Math.abs(right.change_pct || 0) - Math.abs(left.change_pct || 0))[0]
    const useMoneyColor = colorMetric.value === 'moneyflow'
    const colorValue = useMoneyColor ? mainInflow : weightedChange

    return {
      key: `${kind}-${key}`,
      label: key,
      symbolLabel: `${members.length}只`,
      displayValue: useMoneyColor ? formatMoneyflowAmount(mainInflow) : formatPercent(weightedChange),
      meta: `${formatMarketValue(marketValue)} · ${leader?.stock_name || leader?.symbol || '-'}`,
      tooltip: [
        key,
        `${members.length} 只股票`,
        `市值 ${formatMarketValue(marketValue)}`,
        `加权涨跌 ${formatPercent(weightedChange)}`,
        `主力净流入 ${formatMoneyflowAmount(mainInflow)}`,
      ].join('\n'),
      weight: sizeMetric.value === 'equal' ? members.length : sizeMetric.value === 'moneyflow' ? Math.abs(mainInflow || 0) || 1 : marketValue || members.length,
      colorValue,
      colorMetric: useMoneyColor ? 'moneyflow' : 'change_pct',
      missingValue: useMoneyColor ? !isFiniteNumber(mainInflow) : !isFiniteNumber(weightedChange),
      memberCount: members.length,
      marketValue,
      changePct: weightedChange,
      mainInflow,
      extraLargeInflow,
      largeInflow,
      symbols: members.map(stock => stock.symbol),
    }
  })
}

function industryKey(stock: WatchlistStock): string {
  return stock.industry || stock.industry2 || stock.industry3 || '未分行业'
}

function conceptKeys(stock: WatchlistStock): string[] {
  const keys = new Set<string>()
  for (const concept of stock.ths_concepts || []) {
    if (concept.trim()) keys.add(concept.trim())
  }
  if (!keys.size && stock.concept) {
    stock.concept
      .split(/[,，;；、\s]+/)
      .map(item => item.trim())
      .filter(Boolean)
      .slice(0, 8)
      .forEach(item => keys.add(item))
  }
  if (!keys.size) keys.add('未分概念')
  return [...keys]
}

function getWeight(stock: WatchlistStock): number {
  if (sizeMetric.value === 'equal') return 1
  if (sizeMetric.value === 'moneyflow') return Math.abs(getMainNetInflow(stock) || 0) || 1
  return getMarketValue(stock, sizeMetric.value) || getMarketValue(stock, 'total_mv') || 1
}

function getMarketValue(stock: WatchlistStock, metric: 'circ_mv' | 'total_mv'): number | null {
  const value = stock[metric]
  return isFiniteNumber(value) && value > 0 ? value : null
}

function getExtraLargeInflow(stock: WatchlistStock): number | null {
  if (isFiniteNumber(stock.net_amount_xl)) return stock.net_amount_xl
  if (isFiniteNumber(stock.buy_elg_amount) && isFiniteNumber(stock.sell_elg_amount)) {
    return (stock.buy_elg_amount || 0) - (stock.sell_elg_amount || 0)
  }
  return null
}

function getLargeInflow(stock: WatchlistStock): number | null {
  if (isFiniteNumber(stock.net_amount_l)) return stock.net_amount_l
  if (isFiniteNumber(stock.buy_lg_amount) && isFiniteNumber(stock.sell_lg_amount)) {
    return (stock.buy_lg_amount || 0) - (stock.sell_lg_amount || 0)
  }
  return null
}

function getMainNetInflow(stock: WatchlistStock): number | null {
  if (isFiniteNumber(stock.net_mf_amount)) return stock.net_mf_amount
  const extraLarge = getExtraLargeInflow(stock)
  const large = getLargeInflow(stock)
  if (isFiniteNumber(extraLarge) || isFiniteNumber(large)) {
    return (extraLarge || 0) + (large || 0)
  }
  return null
}

function layoutTreemap(items: HeatmapItem[], width: number, height: number): LayoutItem[] {
  const safeItems = items
    .filter(item => item.weight > 0)
    .sort((left, right) => right.weight - left.weight)
  const total = safeItems.reduce((sum, item) => sum + item.weight, 0)
  if (!safeItems.length || total <= 0) return []

  const result: LayoutItem[] = []

  function split(slice: HeatmapItem[], x: number, y: number, w: number, h: number) {
    if (!slice.length) return
    if (slice.length === 1) {
      result.push({ ...slice[0], x, y, w, h })
      return
    }

    const sliceTotal = slice.reduce((sum, item) => sum + item.weight, 0)
    let acc = 0
    let splitIndex = 1
    for (let index = 0; index < slice.length - 1; index += 1) {
      const next = acc + slice[index].weight
      if (Math.abs(sliceTotal / 2 - next) <= Math.abs(sliceTotal / 2 - acc)) {
        acc = next
        splitIndex = index + 1
      } else {
        break
      }
    }

    const first = slice.slice(0, splitIndex)
    const second = slice.slice(splitIndex)
    const firstWeight = first.reduce((sum, item) => sum + item.weight, 0)
    const ratio = firstWeight / sliceTotal

    if (w >= h) {
      const firstWidth = Math.max(36, w * ratio)
      split(first, x, y, firstWidth, h)
      split(second, x + firstWidth, y, Math.max(0, w - firstWidth), h)
    } else {
      const firstHeight = Math.max(36, h * ratio)
      split(first, x, y, w, firstHeight)
      split(second, x, y + firstHeight, w, Math.max(0, h - firstHeight))
    }
  }

  split(safeItems, 0, 0, width, height)
  return result.map(item => ({
    ...item,
    x: item.x + 2,
    y: item.y + 2,
    w: Math.max(0, item.w - 4),
    h: Math.max(0, item.h - 4),
  }))
}

function layoutStockTreemapByIndustry(items: HeatmapItem[], width: number, height: number): { items: LayoutItem[]; frames: LayoutFrame[] } {
  const buckets = new Map<string, HeatmapItem[]>()
  for (const item of items) {
    const label = item.groupLabel || '未分行业'
    const bucket = buckets.get(label) || []
    bucket.push(item)
    buckets.set(label, bucket)
  }

  const industryItems: HeatmapItem[] = [...buckets.entries()].map(([label, members]) => ({
    key: `industry-frame-${label}`,
    label,
    symbolLabel: `${members.length}只`,
    displayValue: '',
    meta: '',
    tooltip: `${label}\n${members.length} 只股票`,
    weight: members.reduce((sum, item) => sum + Math.max(0, item.weight || 0), 0) || members.length,
    colorValue: null,
    colorMetric: colorMetric.value,
    missingValue: false,
    memberCount: members.length,
    marketValue: sumNumbers(members.map(item => item.marketValue)),
    changePct: weightedAverage(members.map(item => ({ value: item.changePct, weight: item.weight }))),
    mainInflow: sumNumbers(members.map(item => item.mainInflow)),
    extraLargeInflow: sumNumbers(members.map(item => item.extraLargeInflow)),
    largeInflow: sumNumbers(members.map(item => item.largeInflow)),
    symbols: members.flatMap(item => item.symbols),
  }))

  const frames: LayoutFrame[] = layoutTreemap(industryItems, width, height).map(frame => ({
    key: frame.key,
    label: frame.label,
    x: frame.x,
    y: frame.y,
    w: frame.w,
    h: frame.h,
    count: frame.memberCount,
  }))

  const childItems: LayoutItem[] = []
  for (const frame of frames) {
    const members = buckets.get(frame.label) || []
    const showLabel = frame.w >= 120 && frame.h >= 86
    const headerHeight = showLabel ? 22 : 0
    const inset = 4
    const childX = frame.x + inset
    const childY = frame.y + headerHeight + inset
    const childW = Math.max(0, frame.w - inset * 2)
    const childH = Math.max(0, frame.h - headerHeight - inset * 2)
    if (childW < 24 || childH < 24) continue
    const laidOut = layoutTreemap(members, childW, childH).map(item => ({
      ...item,
      x: item.x + childX,
      y: item.y + childY,
    }))
    childItems.push(...laidOut)
  }

  return { items: childItems, frames }
}

function tileStyle(item: LayoutItem): Record<string, string> {
  const colors = colorForItem(item)
  const fontVars = tileFontVars(item)
  return {
    left: `${item.x}px`,
    top: `${item.y}px`,
    width: `${item.w}px`,
    height: `${item.h}px`,
    background: colors.background,
    color: colors.color,
    borderColor: colors.border,
    ...fontVars,
  }
}

function industryFrameStyle(frame: LayoutFrame): Record<string, string> {
  return {
    left: `${frame.x}px`,
    top: `${frame.y}px`,
    width: `${frame.w}px`,
    height: `${frame.h}px`,
    '--industry-label-size': `${clamp(Math.min(frame.w, frame.h) / 8, 10, 13)}px`,
  }
}

function tileFontVars(item: LayoutItem): Record<string, string> {
  const area = Math.max(1, item.w * item.h)
  const shortSide = Math.max(1, Math.min(item.w, item.h))
  const areaScale = Math.sqrt(area)
  const compact = item.w < 96 || item.h < 66
  return {
    '--tile-symbol-size': `${clamp(shortSide / 8, 8, 11)}px`,
    '--tile-name-size': `${compact ? clamp(areaScale / 12, 10, 13) : clamp(areaScale / 9.5, 12, 23)}px`,
    '--tile-value-size': `${compact ? clamp(areaScale / 11, 10, 14) : clamp(areaScale / 8.8, 13, 25)}px`,
    '--tile-meta-size': `${clamp(shortSide / 9, 8, 11)}px`,
  }
}

function colorForItem(item: HeatmapItem): { background: string; color: string; border: string } {
  if (item.missingValue || !isFiniteNumber(item.colorValue)) {
    return {
      background: 'repeating-linear-gradient(45deg, #f2f2ef, #f2f2ef 10px, #e5dfd3 10px, #e5dfd3 20px)',
      color: '#5c6863',
      border: '#d8d0c2',
    }
  }

  const value = item.colorValue || 0
  const abs = Math.abs(value)
  if (abs < (item.colorMetric === 'moneyflow' ? 0.0001 : 0.1)) {
    return { background: '#f2f2ef', color: '#54635c', border: '#e5dfd3' }
  }

  if (value > 0) {
    if (abs >= 5) return { background: '#a83232', color: '#fff', border: '#8d2929' }
    if (abs >= 2) return { background: '#c95a5a', color: '#fff', border: '#a83232' }
    return { background: '#fbe6e6', color: '#a83232', border: '#edc6c6' }
  }

  if (abs >= 5) return { background: '#1b4d3e', color: '#fff', border: '#14392f' }
  if (abs >= 2) return { background: '#2d6a4f', color: '#fff', border: '#1f503b' }
  return { background: '#eaf5f0', color: '#2d6a4f', border: '#c6dfd3' }
}

function handleTileClick(item: HeatmapItem) {
  if (item.stock) {
    router.push(`/stock/${item.stock.symbol}`)
    return
  }
  if (item.symbols.length === 1) {
    router.push(`/stock/${item.symbols[0]}`)
    return
  }
  searchText.value = item.label.startsWith('未分') ? '' : item.label
}

function average(values: Array<number | null | undefined>): number | null {
  const valid = values.filter(isFiniteNumber)
  if (!valid.length) return null
  return valid.reduce((sum, value) => sum + value, 0) / valid.length
}

function weightedAverage(items: Array<{ value: number | null | undefined; weight: number | null | undefined }>): number | null {
  let weighted = 0
  let weights = 0
  for (const item of items) {
    if (!isFiniteNumber(item.value)) continue
    const weight = isFiniteNumber(item.weight) && item.weight > 0 ? item.weight : 1
    weighted += item.value * weight
    weights += weight
  }
  return weights ? weighted / weights : null
}

function sumNumbers(values: Array<number | null | undefined>): number | null {
  const valid = values.filter(isFiniteNumber)
  if (!valid.length) return null
  return valid.reduce((sum, value) => sum + value, 0)
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value))
}

function formatPercent(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return '-'
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`
}

function formatMarketValue(value: number | null | undefined): string {
  if (!isFiniteNumber(value) || value <= 0) return '-'
  if (value >= 10000) return `${(value / 10000).toFixed(value >= 100000 ? 1 : 2)}亿`
  return `${value.toFixed(0)}万`
}

function formatMoneyflowAmount(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return '-'
  const sign = value > 0 ? '+' : value < 0 ? '-' : ''
  const abs = Math.abs(value)
  if (abs >= 10000) return `${sign}${(abs / 10000).toFixed(abs >= 100000 ? 1 : 2)}亿`
  if (abs >= 1) return `${sign}${abs.toFixed(abs >= 100 ? 0 : 1)}万`
  return `${sign}${abs.toFixed(2)}万`
}

function toneClass(value: number | null | undefined): string {
  if (!isFiniteNumber(value) || Math.abs(value) < 0.0001) return 'tone-neutral'
  return value > 0 ? 'tone-up' : 'tone-down'
}

function rankBadgeTone(item: HeatmapItem): 'up' | 'down' {
  const value = colorMetric.value === 'moneyflow' ? item.mainInflow : item.changePct
  return isFiniteNumber(value) && value < 0 ? 'down' : 'up'
}

function toneName(value: number | null | undefined): 'good' | 'bad' | 'neutral' {
  if (!isFiniteNumber(value) || Math.abs(value) < 0.0001) return 'neutral'
  return value > 0 ? 'bad' : 'good'
}
</script>

<style scoped>
.theme-pine-quant {
  --bg-page: var(--bg-primary, #fdfbf7);
  --bg-card: var(--bg-elevated, #f5f2ea);
  --bg-hover-local: var(--bg-hover, #ebe7dc);
  --border-color: var(--border-default, #e5dfd3);
  --text-main: var(--text-primary, #22302a);
  --text-sub: var(--text-secondary, #54635c);
  --text-light: var(--text-muted, #7e8d86);
  --pine-primary: var(--accent-primary, #1b3d32);
  --pine-secondary: var(--accent-secondary, #355e4f);
  --color-up: var(--color-bull, #a83232);
  --color-down: var(--color-bear, #2d6a4f);
  --font-data-local: var(--font-data, "Consolas", Monaco, monospace);

  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
  overflow: hidden;
  box-sizing: border-box;
  padding: 16px 20px;
  background:
    linear-gradient(135deg, rgba(238, 243, 240, 0.55), rgba(253, 251, 247, 0.28)),
    var(--bg-page);
  color: var(--text-main);
  font-family: var(--font-ui, "Microsoft YaHei UI", system-ui, sans-serif);
}

.section-kicker,
.panel-kicker {
  font-family: var(--font-data-local);
  font-size: 10px;
  font-weight: 800;
  color: var(--pine-secondary);
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.watchlist-command-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border-color);
  padding-bottom: 12px;
  margin-bottom: 14px;
  flex-shrink: 0;
}

.brand-left {
  display: flex;
  flex-direction: column;
  gap: 3px;
}

.brand-left h2 {
  margin: 0;
  font-size: 20px;
  font-weight: 760;
  color: var(--pine-primary);
}

.brand-left p {
  margin: 0;
  font-size: 13px;
  color: var(--text-sub);
}

.actions-right {
  display: flex;
  align-items: center;
  gap: 10px;
}

.watchlist-body {
  display: grid;
  grid-template-columns: 224px minmax(0, 1fr);
  gap: 14px;
  flex: 1;
  min-height: 0;
}

.group-panel,
.table-panel,
.rail-card,
.treemap-panel {
  background: rgba(255, 255, 255, 0.72);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  box-shadow: 0 1px 0 rgba(253, 251, 247, 0.9) inset, 0 10px 26px rgba(34, 48, 42, 0.05);
}

.group-panel {
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.group-panel__header,
.table-panel__head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 12px;
  border-bottom: 1px solid var(--border-color);
}

.group-panel__header strong,
.table-panel__head strong {
  font-family: var(--font-data-local);
  font-size: 12px;
  color: var(--pine-primary);
}

.group-list {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  padding: 8px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.group-item {
  width: 100%;
  border: 1px solid transparent;
  background: transparent;
  color: var(--text-main);
  border-radius: 5px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  padding: 8px 9px;
  cursor: pointer;
  text-align: left;
}

.group-item:hover {
  background: var(--bg-hover-local);
}

.group-item--active {
  background: var(--bg-page);
  border-color: var(--pine-secondary);
  box-shadow: inset 3px 0 0 var(--pine-primary);
}

.group-item__info {
  display: flex;
  align-items: center;
  min-width: 0;
  gap: 8px;
}

.group-item__name {
  font-size: 13px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.group-item__count {
  font-family: var(--font-data-local);
  font-size: 10px;
  color: var(--text-light);
  background: var(--bg-hover-local);
  padding: 1px 5px;
  border-radius: 2px;
}

.group-item__delete {
  display: inline-flex;
  color: var(--text-light);
  opacity: 0.45;
}

.group-item:hover .group-item__delete {
  opacity: 1;
}

.group-item__delete:hover {
  color: var(--color-up);
}

.stock-panel {
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.stock-panel__placeholder,
.empty-hint,
.loading-state {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: var(--text-light);
  gap: 12px;
  text-align: center;
}

.empty-hint--compact {
  flex: initial;
  padding: 24px 10px;
  font-size: 12px;
}

.placeholder-icon {
  opacity: 0.55;
}

.metric-strip {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
  flex-shrink: 0;
}

.metric-card {
  min-height: 62px;
  border: 1px solid var(--border-color);
  border-radius: 8px;
  background: rgba(255, 255, 255, 0.76);
  padding: 10px 12px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  min-width: 0;
}

.metric-card span {
  color: var(--text-light);
  font-size: 11px;
}

.metric-card strong {
  color: var(--text-main);
  font-family: var(--font-data-local);
  font-size: 16px;
  font-weight: 850;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.control-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  background: var(--bg-card);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 8px 10px;
  margin-bottom: 12px;
  flex-shrink: 0;
}

.control-block {
  display: flex;
  align-items: center;
  gap: 7px;
}

.control-block--search {
  flex: 1;
  min-width: 180px;
}

.control-block--add {
  margin-left: auto;
}

.control-label {
  color: var(--text-sub);
  font-size: 11px;
  font-weight: 800;
  white-space: nowrap;
}

.segmented-control {
  display: flex;
  gap: 2px;
  background: rgba(34, 48, 42, 0.06);
  padding: 2px;
  border-radius: 5px;
}

.segment-button,
.ghost-button,
.icon-button,
.btn-pine {
  border: 1px solid transparent;
  border-radius: 5px;
  cursor: pointer;
  font-size: 12px;
  font-weight: 700;
  transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease, transform 0.15s ease;
}

.segment-button {
  background: transparent;
  color: var(--text-sub);
  padding: 5px 9px;
}

.segment-button.active {
  background: var(--pine-primary);
  color: #fff;
}

.ghost-button {
  background: transparent;
  border-color: var(--border-color);
  color: var(--text-sub);
  padding: 6px 10px;
}

.ghost-button:hover {
  border-color: var(--pine-secondary);
  color: var(--pine-primary);
}

.btn-pine {
  background: var(--pine-primary);
  color: #fff;
  padding: 6px 12px;
}

.btn-pine:hover {
  background: var(--pine-secondary);
}

.btn-pine:disabled,
.ghost-button:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

.btn--small {
  padding: 6px 10px;
}

.icon-button {
  width: 30px;
  height: 30px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: var(--bg-card);
  border-color: var(--border-color);
  color: var(--text-sub);
}

.icon-button:hover {
  color: var(--pine-primary);
  border-color: var(--pine-secondary);
}

.icon-button svg {
  width: 15px;
  height: 15px;
}

.control-select,
.input-mini,
.input {
  background: var(--bg-page);
  border: 1px solid var(--border-color);
  border-radius: 5px;
  color: var(--text-main);
  font-size: 12px;
  outline: none;
}

.control-select {
  min-height: 30px;
  padding: 4px 8px;
}

.input-mini {
  min-height: 30px;
  padding: 5px 8px;
  width: 142px;
}

.input-mini--wide {
  width: 100%;
  min-width: 180px;
}

.control-select:focus,
.input-mini:focus,
.input:focus {
  border-color: var(--pine-secondary);
}

.heatmap-workspace {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 300px;
  gap: 12px;
  min-height: 0;
  flex: 1;
}

.treemap-panel {
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.treemap-panel__head {
  min-height: 54px;
  padding: 10px 12px;
  border-bottom: 1px solid var(--border-color);
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.treemap-panel__head h3 {
  margin: 2px 0 0;
  color: var(--pine-primary);
  font-size: 15px;
  font-weight: 800;
}

.legend-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--text-light);
  font-size: 11px;
  white-space: nowrap;
}

.legend-gradient {
  width: 132px;
  height: 8px;
  border-radius: 999px;
  background: linear-gradient(90deg, var(--color-down), #f2f2ef 50%, var(--color-up));
}

.treemap-canvas {
  position: relative;
  flex: 1;
  min-height: 460px;
  background:
    linear-gradient(0deg, rgba(253, 251, 247, 0.72), rgba(253, 251, 247, 0.72)),
    repeating-linear-gradient(90deg, rgba(34, 48, 42, 0.03) 0, rgba(34, 48, 42, 0.03) 1px, transparent 1px, transparent 44px),
    repeating-linear-gradient(0deg, rgba(34, 48, 42, 0.025) 0, rgba(34, 48, 42, 0.025) 1px, transparent 1px, transparent 44px);
  overflow: hidden;
}

.industry-frame {
  position: absolute;
  z-index: 1;
  border: 1px solid rgba(53, 94, 79, 0.26);
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.22);
  pointer-events: none;
  overflow: hidden;
}

.industry-frame span {
  position: absolute;
  top: 4px;
  left: 8px;
  right: 8px;
  color: var(--pine-primary);
  font-size: var(--industry-label-size, 11px);
  font-weight: 900;
  line-height: 1.2;
  opacity: 0.74;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.treemap-node {
  position: absolute;
  z-index: 2;
  border: 1px solid;
  border-radius: 4px;
  padding: 9px 8px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 4px;
  text-align: center;
  cursor: pointer;
  overflow: hidden;
  transition: box-shadow 0.15s ease, opacity 0.15s ease, transform 0.15s ease;
}

.treemap-node:hover,
.treemap-node:focus,
.treemap-node--focused {
  z-index: 5;
  box-shadow: inset 0 0 0 2px var(--pine-primary), 0 8px 20px rgba(34, 48, 42, 0.14);
  outline: none;
}

.treemap-node--missing {
  color: var(--text-sub);
}

.node-symbol {
  position: absolute;
  top: 6px;
  left: 8px;
  font-family: var(--font-data-local);
  font-size: var(--tile-symbol-size, 10px);
  font-weight: 800;
  opacity: 0.78;
  max-width: calc(100% - 16px);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.node-name {
  font-size: var(--tile-name-size, 14px);
  font-weight: 900;
  line-height: 1.1;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
}

.node-value {
  font-family: var(--font-data-local);
  font-size: var(--tile-value-size, 15px);
  font-weight: 950;
  line-height: 1;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
}

.node-meta {
  max-width: 100%;
  color: currentColor;
  font-size: var(--tile-meta-size, 10px);
  opacity: 0.72;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.treemap-node--tiny {
  padding: 6px;
  gap: 2px;
}

.treemap-node--tiny .node-symbol,
.treemap-node--tiny .node-meta {
  display: none;
}

.treemap-node--tiny .node-name {
  font-size: 12px;
}

.treemap-node--tiny .node-value {
  font-size: 12px;
}

.empty-map {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--text-light);
  font-size: 13px;
}

.right-rail {
  display: flex;
  flex-direction: column;
  gap: 10px;
  min-height: 0;
  overflow: auto;
}

.rail-card {
  padding: 12px;
}

.rail-title {
  display: block;
  color: var(--pine-primary);
  font-size: 12px;
  font-weight: 900;
  margin-bottom: 9px;
  padding-bottom: 6px;
  border-bottom: 1px solid var(--border-color);
}

.focus-card {
  display: flex;
  flex-direction: column;
  gap: 7px;
}

.focus-card__title {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  align-items: baseline;
}

.focus-card__title strong {
  font-size: 14px;
  color: var(--text-main);
}

.focus-card__title span,
.detail-row span {
  color: var(--text-light);
  font-family: var(--font-data-local);
  font-size: 11px;
}

.detail-row {
  display: flex;
  justify-content: space-between;
  gap: 12px;
}

.detail-row strong {
  font-family: var(--font-data-local);
  font-size: 12px;
}

.rank-list,
.gap-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.rank-item,
.dimension-chip {
  width: 100%;
  border: 0;
  background: transparent;
  padding: 4px 0;
  color: var(--text-main);
  display: grid;
  grid-template-columns: 24px minmax(0, 1fr) auto;
  align-items: center;
  gap: 7px;
  cursor: pointer;
  text-align: left;
}

.rank-item:hover,
.dimension-chip:hover {
  background: rgba(238, 243, 240, 0.58);
}

.rank-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  border-radius: 3px;
  color: #fff;
  font-family: var(--font-data-local);
  font-size: 10px;
  font-weight: 900;
}

.rank-badge.up {
  background: var(--color-up);
}

.rank-badge.down {
  background: var(--color-down);
}

.rank-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 12px;
  font-weight: 750;
}

.rank-item strong,
.dimension-chip strong {
  font-family: var(--font-data-local);
  font-size: 12px;
  white-space: nowrap;
}

.dimension-chip {
  grid-template-columns: minmax(0, 1fr) auto;
}

.dimension-chip span {
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.dimension-chip small {
  color: var(--text-light);
  font-size: 10px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.gap-item {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  color: var(--text-sub);
  font-size: 12px;
}

.gap-item strong {
  color: var(--text-main);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.gap-item span,
.rail-empty {
  color: var(--text-light);
  font-size: 11px;
}

.table-panel {
  margin-top: 12px;
  min-height: 162px;
  max-height: 240px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  flex-shrink: 0;
}

.stock-table-wrap {
  overflow: auto;
}

.stock-table {
  width: 100%;
  border-collapse: collapse;
}

.stock-table th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: var(--bg-card);
  color: var(--text-sub);
  font-size: 11px;
  font-weight: 800;
  padding: 8px 10px;
  text-align: left;
  border-bottom: 1px solid var(--border-color);
}

.stock-table td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--border-color);
  font-size: 12px;
  color: var(--text-main);
}

.stock-row:hover {
  background: rgba(238, 243, 240, 0.45);
}

.symbol-link {
  display: block;
  font-family: var(--font-data-local);
  font-weight: 850;
  color: var(--pine-secondary);
  text-decoration: none;
}

.symbol-link:hover {
  color: var(--pine-primary);
  text-decoration: underline;
}

.stock-name {
  display: block;
  color: var(--text-light);
  font-size: 11px;
  margin-top: 2px;
}

.font-data {
  font-family: var(--font-data-local);
}

.text-right {
  text-align: right;
}

.btn-text-danger {
  background: transparent;
  border: none;
  color: var(--color-up);
  font-size: 11px;
  cursor: pointer;
}

.btn-text-danger:hover {
  text-decoration: underline;
}

.tone-up {
  color: var(--color-up) !important;
}

.tone-down {
  color: var(--color-down) !important;
}

.tone-neutral {
  color: var(--text-sub) !important;
}

.skeleton-map {
  width: min(680px, 90%);
  height: 220px;
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  grid-auto-rows: 1fr;
  gap: 5px;
  padding: 5px;
  border-radius: 8px;
  background: var(--bg-card);
}

.skeleton-map span {
  border-radius: 4px;
  background: linear-gradient(90deg, #f2eee5, #e7e0d3, #f2eee5);
  background-size: 200% 100%;
  animation: shimmer 1.4s ease-in-out infinite;
}

@keyframes shimmer {
  from { background-position: 200% 0; }
  to { background-position: -200% 0; }
}

.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.34);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal {
  width: 340px;
  background: var(--bg-page);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  padding: 20px;
  box-shadow: 0 20px 42px rgba(34, 48, 42, 0.18);
}

.modal__title {
  font-size: 16px;
  font-weight: 800;
  color: var(--pine-primary);
  margin: 0 0 14px;
}

.modal__body {
  display: flex;
  flex-direction: column;
}

.form-label {
  color: var(--text-light);
  font-size: 11px;
  font-weight: 800;
  margin-bottom: 5px;
}

.form-label--spaced {
  margin-top: 12px;
}

.input {
  min-height: 34px;
  padding: 7px 10px;
}

.modal__footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 16px;
}

@media (max-width: 1180px) {
  .metric-strip {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .heatmap-workspace {
    grid-template-columns: 1fr;
  }

  .right-rail {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    overflow: visible;
  }
}

@media (max-width: 900px) {
  .theme-pine-quant {
    height: auto;
    min-height: 100%;
    overflow: auto;
  }

  .watchlist-command-bar {
    align-items: flex-start;
    gap: 12px;
  }

  .watchlist-body {
    grid-template-columns: 1fr;
    overflow: visible;
  }

  .group-panel {
    max-height: 220px;
  }

  .stock-panel {
    overflow: visible;
  }

  .metric-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .control-block--add {
    width: 100%;
    margin-left: 0;
  }

  .input-mini {
    flex: 1;
  }

  .treemap-canvas {
    min-height: 520px;
  }

  .right-rail {
    grid-template-columns: 1fr;
  }

  .table-panel {
    max-height: none;
  }
}
</style>

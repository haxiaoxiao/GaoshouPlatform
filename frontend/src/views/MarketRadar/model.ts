import type { RadarFreshness } from '@/api/marketRadar'

export interface BreadthBucketDefinition {
  key: string
  label: string
  color: string
}

export const BREADTH_BUCKETS: readonly BreadthBucketDefinition[] = [
  { key: 'le_neg_8', label: '≤-8%', color: '#174c36' },
  { key: 'neg_8_to_neg_6', label: '-8~-6%', color: '#226044' },
  { key: 'neg_6_to_neg_4', label: '-6~-4%', color: '#327656' },
  { key: 'neg_4_to_neg_2', label: '-4~-2%', color: '#579476' },
  { key: 'neg_2_to_0', label: '-2~0%', color: '#93bba7' },
  { key: 'pos_0_to_2', label: '0~2%', color: '#e7aaa4' },
  { key: 'pos_2_to_4', label: '2~4%', color: '#d77d75' },
  { key: 'pos_4_to_6', label: '4~6%', color: '#c3544e' },
  { key: 'pos_6_to_8', label: '6~8%', color: '#ae3835' },
  { key: 'ge_pos_8', label: '≥8%', color: '#8f2226' },
] as const

export interface ChartSeries {
  key: string
  label: string
  color: string
  values: Array<number | null>
}

export interface BreadthChartModel {
  dates: string[]
  fullDates: string[]
  mode: 'percent' | 'count'
  flatCounts: Array<number | null>
  series: ChartSeries[]
}

export interface IndexTrendModel {
  dates: string[]
  fullDates: string[]
  series: ChartSeries[]
}

export interface FreshnessView {
  status: RadarFreshness
  label: string
  tone: 'good' | 'warn' | 'bad' | 'muted'
  asOf: string | null
}

export interface LimitLadderRowView {
  symbol: string
  name: string
  industry: string
  boardCount: number | null
  pctChange: number | null
  turnoverRatio: number | null
  limitTimes: number | null
  amount: number | null
  sealAmount: number | null
  firstTime: string | null
  lastTime: string | null
  openTimes: number | null
}

export interface LimitLadderView {
  status: RadarFreshness
  tradeDate: string | null
  sourceMode: string | null
  highestBoard: number | null
  upCount: number | null
  downCount: number | null
  brokenCount: number | null
  brokenRate: number | null
  promotionRate: number | null
  distribution: Array<{ boardCount: number; count: number }>
  rows: LimitLadderRowView[]
}

export interface SectorRowView {
  industry: string
  medianReturn: number | null
  advanceRatio: number | null
  amountShare: number | null
  shareZ20: number | null
  stockCount: number | null
  crowdingScore: null
  emotionScore: null
}

const CORE_INDEX_SERIES = [
  { key: '000001.SH', label: '上证指数', color: '#a83232' },
  { key: '399001.SZ', label: '深证成指', color: '#b27a1e' },
  { key: '000985.SH', label: '中证全指', color: '#355e4f' },
] as const

function record(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
}

function list(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function finite(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function text(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null
}

function compactDate(value: string): string {
  return value.length >= 10 ? value.slice(5, 10) : value
}

function clock(value: unknown): string | null {
  const raw = text(value)
  if (!raw) return null
  if (/^\d{6}$/.test(raw)) return `${raw.slice(0, 2)}:${raw.slice(2, 4)}:${raw.slice(4, 6)}`
  return raw
}

function freshness(value: unknown): RadarFreshness {
  return ['fresh', 'partial', 'stale', 'unavailable'].includes(String(value))
    ? value as RadarFreshness
    : 'unavailable'
}

export function resolveFreshness(status: unknown, asOf: unknown): FreshnessView {
  const normalized = freshness(status)
  const labels: Record<RadarFreshness, FreshnessView['label']> = {
    fresh: '实时有效',
    partial: '部分可用',
    stale: '数据过期',
    unavailable: '不可用',
  }
  const tones: Record<RadarFreshness, FreshnessView['tone']> = {
    fresh: 'good',
    partial: 'warn',
    stale: 'bad',
    unavailable: 'muted',
  }
  return {
    status: normalized,
    label: labels[normalized],
    tone: tones[normalized],
    asOf: text(asOf),
  }
}

export function buildBreadthChart(value: unknown): BreadthChartModel {
  const source = record(value)
  const days = list(source.days).map(record)
  const fullDates = days.map(day => text(day.trade_date) || '—')
  return {
    dates: fullDates.map(compactDate),
    fullDates,
    mode: source.mode === 'count' ? 'count' : 'percent',
    flatCounts: days.map(day => finite(record(day.breadth).flat_count)),
    series: BREADTH_BUCKETS.map(bucket => ({
      ...bucket,
      values: days.map(day => {
        const bucketValue = record(record(record(day.breadth).buckets)[bucket.key])
        return finite(bucketValue.value ?? bucketValue.percentage)
      }),
    })),
  }
}

export function buildIndexTrend(value: unknown): IndexTrendModel {
  const days = list(record(value).days).map(record)
  const fullDates = days.map(day => text(day.trade_date) || '—')
  const allMarket: ChartSeries = {
    key: 'all',
    label: '全 A 中位数',
    color: '#27332e',
    values: days.map(day => {
      const all = list(day.breakdowns).map(record).find(item => item.key === 'all')
      return finite(all?.median_return)
    }),
  }
  return {
    dates: fullDates.map(compactDate),
    fullDates,
    series: [
      allMarket,
      ...CORE_INDEX_SERIES.map(definition => ({
        ...definition,
        values: days.map(day => finite(record(record(day.indices)[definition.key]).return_pct)),
      })),
    ],
  }
}

export function normalizeLimitLadder(value: unknown): LimitLadderView {
  const source = record(value)
  return {
    status: freshness(source.status),
    tradeDate: text(source.trade_date ?? source.source_date),
    sourceMode: text(source.source_mode),
    highestBoard: finite(source.highest_board),
    upCount: finite(source.up_count),
    downCount: finite(source.down_count),
    brokenCount: finite(source.broken_count),
    brokenRate: finite(source.broken_rate),
    promotionRate: finite(source.promotion_rate),
    distribution: list(source.distribution).flatMap(item => {
      const pair = list(item)
      const boardCount = finite(pair[0])
      const count = finite(pair[1])
      return boardCount === null || count === null ? [] : [{ boardCount, count }]
    }),
    rows: list(source.rows).map(record).map(row => ({
      symbol: text(row.symbol) || '—',
      name: text(row.name) || '未命名',
      industry: text(row.industry) || '—',
      boardCount: finite(row.board_count ?? row.consecutive_limit),
      pctChange: finite(row.pct_change),
      turnoverRatio: finite(row.turnover_ratio),
      limitTimes: finite(row.limit_times),
      amount: finite(row.amount),
      sealAmount: finite(row.seal_amount),
      firstTime: clock(row.first_time),
      lastTime: clock(row.last_time),
      openTimes: finite(row.open_times),
    })),
  }
}

export function normalizeSectors(value: unknown): SectorRowView[] {
  const source = record(value)
  return list(source.sectors ?? source.items).map(record).map(row => ({
    industry: text(row.industry) || '未分类',
    medianReturn: finite(row.median_return),
    advanceRatio: finite(row.advance_ratio),
    amountShare: finite(row.amount_share),
    shareZ20: finite(row.share_z20 ?? row.amount_share_z20),
    stockCount: finite(row.stock_count),
    crowdingScore: null,
    emotionScore: null,
  }))
}

export function isUsableRadarComponent(value: unknown): boolean {
  const source = record(value)
  return Object.keys(source).length > 0 && freshness(source.status) !== 'unavailable'
}

export function mergeRealtimeBreadth(
  historyValue: unknown,
  metricsValue: unknown,
  asOf: string,
): Record<string, unknown> & { days: unknown[] } {
  const history = record(historyValue)
  const metrics = record(metricsValue)
  const breadth = record(metrics.breadth)
  if (!isUsableRadarComponent(breadth)) {
    return { ...history, days: list(history.days) }
  }
  if (list(breadth.days).length) {
    return { ...history, ...breadth, days: list(breadth.days).slice(-15) }
  }

  const tradeDate = asOf.slice(0, 10)
  const overview = record(metrics.overview)
  const medianReturn = finite(
    overview.market_median_return_pct
      ?? record(overview.market_breadth).median_return_pct,
  )
  const latest = {
    trade_date: tradeDate,
    breadth,
    breakdowns: [{ key: 'all', median_return: medianReturn }],
    indices: record(metrics.indices),
  }
  const days = list(history.days).filter(item => text(record(item).trade_date) !== tradeDate)
  days.push(latest)
  return {
    ...history,
    status: breadth.status,
    days: days.slice(-15),
    indices: record(metrics.indices),
  }
}

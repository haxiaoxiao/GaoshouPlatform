type Row = Record<string, unknown>

export type OptimizationType = 'grid_search' | 'walk_forward' | 'unknown'

export interface WindowSummary {
  window: number
  train_start: string
  train_end: string
  row_count: number
  start_equity: number | null
  end_equity: number | null
  return_pct: number | null
  max_drawdown: number | null
  objective_value: number | null
  params: Row
}

export const reservedOptimizationKeys = new Set([
  'equity',
  'nav',
  'portfolio_value',
  'value',
  'train_start',
  'train_end',
  'test_start',
  'test_end',
  'date',
  'datetime',
  'timestamp',
  'return',
  'returns',
  'total_return',
  'annual_return',
  'max_drawdown',
  'sharpe_ratio',
  'sharpe',
  'calmar_ratio',
  'calmar',
  'sortino_ratio',
  'sortino',
  'win_rate',
  'total_trades',
])

export const isOptimizationRecord = (parameters?: Row | null, result?: Row | null): boolean =>
  parameters?.record_type === 'optimization' ||
  parameters?.optimization_type != null ||
  Array.isArray(result?.rows)

export const optimizationType = (parameters?: Row | null, result?: Row | null): OptimizationType => {
  const value = String(parameters?.optimization_type || result?.optimization_type || '').toLowerCase()
  if (value === 'grid_search') return 'grid_search'
  if (value === 'walk_forward') return 'walk_forward'
  return Array.isArray(result?.rows) ? 'walk_forward' : 'unknown'
}

export const valueToString = (value: unknown): string => {
  if (value === null || value === undefined || value === '') return '-'
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return '-'
    if (Math.abs(value) >= 1000) return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
    return Number.isInteger(value) ? String(value) : value.toFixed(4).replace(/0+$/, '').replace(/\.$/, '')
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  return String(value)
}

export const formatPercentValue = (value: number | null | undefined): string =>
  value === null || value === undefined || !Number.isFinite(value)
    ? '-'
    : `${(value * 100).toFixed(2)}%`

export const metricDisplayName = (metric: unknown): string => {
  const raw = Array.isArray(metric) ? String(metric[0] || '') : String(metric || '')
  const key = raw.toLowerCase()
  const labels: Record<string, string> = {
    calmar: '卡尔玛比率',
    calmar_ratio: '卡尔玛比率',
    sharpe: '夏普比率',
    sharpe_ratio: '夏普比率',
    total_return: '总收益率',
    annual_return: '年化收益率',
    max_drawdown: '最大回撤',
    sortino: 'Sortino 比率',
    sortino_ratio: 'Sortino 比率',
    win_rate: '胜率',
  }
  return labels[key] ? `${labels[key]} (${raw})` : (raw || '-')
}

export const paramColumns = (rows: Row[]): string[] => {
  const keys = new Set<string>()
  rows.forEach(row => {
    Object.keys(row || {}).forEach(key => {
      if (!reservedOptimizationKeys.has(key)) keys.add(key)
    })
  })
  return Array.from(keys)
}

export const toNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim() !== '') {
    const n = Number(value)
    return Number.isFinite(n) ? n : null
  }
  return null
}

export const compareValues = (a: unknown, b: unknown): number => {
  const an = toNumber(a)
  const bn = toNumber(b)
  if (an !== null && bn !== null) return an - bn
  return valueToString(a).localeCompare(valueToString(b))
}

const equityOf = (row: Row): number | null =>
  toNumber(row.equity ?? row.nav ?? row.portfolio_value ?? row.value)

const maxDrawdown = (values: number[]): number | null => {
  if (!values.length) return null
  let peak = values[0]
  let drawdown = 0
  values.forEach(value => {
    peak = Math.max(peak, value)
    if (peak > 0) drawdown = Math.min(drawdown, value / peak - 1)
  })
  return drawdown
}

const objectiveValue = (
  metric: unknown,
  returnPct: number | null,
  drawdown: number | null,
  equities: number[],
  representativeRow: Row,
): number | null => {
  const key = String(Array.isArray(metric) ? metric[0] : metric || 'calmar_ratio').toLowerCase()
  const direct = toNumber(representativeRow[key] ?? representativeRow[String(metric || '')])
  if (direct !== null) return direct
  if (key === 'total_return' || key === 'return') return returnPct
  if (key === 'max_drawdown') return drawdown
  if ((key === 'calmar' || key === 'calmar_ratio') && returnPct !== null && drawdown !== null && drawdown !== 0) {
    return returnPct / Math.abs(drawdown)
  }
  if ((key === 'sharpe' || key === 'sharpe_ratio') && equities.length > 2) {
    const returns = equities.slice(1).map((value, idx) => equities[idx] ? value / equities[idx] - 1 : 0)
    const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length
    const variance = returns.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(1, returns.length - 1)
    const std = Math.sqrt(variance)
    return std > 0 ? mean / std : null
  }
  return returnPct
}

export const summarizeWalkForwardRows = (rows: Row[], metric: unknown = 'calmar_ratio'): WindowSummary[] => {
  const groups = new Map<string, Row[]>()
  rows.forEach(row => {
    const key = `${valueToString(row.train_start)}|${valueToString(row.train_end)}`
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key)!.push(row)
  })
  const params = paramColumns(rows)
  return Array.from(groups.values()).map((group, index) => {
    const first = group[0] || {}
    const equities = group.map(equityOf).filter((value): value is number => value !== null)
    const start = equities[0] ?? null
    const end = equities.length ? equities[equities.length - 1] : null
    const returnPct = start && end ? end / start - 1 : null
    const drawdown = maxDrawdown(equities)
    const extractedParams: Row = {}
    params.forEach(key => {
      extractedParams[key] = first[key]
    })
    return {
      window: index + 1,
      train_start: valueToString(first.train_start),
      train_end: valueToString(first.train_end),
      row_count: group.length,
      start_equity: start,
      end_equity: end,
      return_pct: returnPct,
      max_drawdown: drawdown,
      objective_value: objectiveValue(metric, returnPct, drawdown, equities, first),
      params: extractedParams,
    }
  })
}

export const columnDisplayName = (column: string): string => {
  const labels: Record<string, string> = {
    equity: '组合权益',
    nav: '净值',
    portfolio_value: '组合权益',
    value: '数值',
    train_start: '训练开始',
    train_end: '训练结束',
    test_start: '测试开始',
    test_end: '测试结束',
    grid_pct: '网格间距',
    anchor_window_minutes: '中枢窗口(分钟)',
    max_grid_levels: '最大网格层数',
    grid_sleeve_pct: '网格仓比例',
    anchor_reset_pct: '中枢重置阈值',
    base_position_pct: '底仓比例',
    cash_buffer_pct: '现金缓冲',
    total_return: '总收益率',
    annual_return: '年化收益率',
    max_drawdown: '最大回撤',
    sharpe_ratio: '夏普比率',
    calmar_ratio: '卡尔玛比率',
    total_trades: '交易次数',
  }
  return labels[column] || column
}

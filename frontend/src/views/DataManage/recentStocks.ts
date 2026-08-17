export const RECENT_STOCKS_STORAGE_KEY = 'gaoshou:data-workbench:recent-stocks:v1'
export const MAX_RECENT_STOCKS = 10

export interface RecentStock {
  symbol: string
  name: string
  exchange?: string | null
  industry?: string | null
  theme?: string
  total_mv?: number | null
  circ_mv?: number | null
}

type StorageReader = Pick<Storage, 'getItem'>
type StorageWriter = Pick<Storage, 'setItem'>

function browserStorage(): Storage | null {
  return typeof localStorage === 'undefined' ? null : localStorage
}

function optionalText(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

function optionalNullableText(value: unknown): string | null | undefined {
  if (value === null) return null
  return optionalText(value)
}

function optionalNullableNumber(value: unknown): number | null | undefined {
  if (value === null) return null
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function normalizeStock(value: unknown): RecentStock | null {
  if (!value || typeof value !== 'object') return null
  const record = value as Record<string, unknown>
  const symbol = optionalText(record.symbol)
  const name = optionalText(record.name)
  if (!symbol || !name) return null

  const stock: RecentStock = { symbol, name }
  const exchange = optionalNullableText(record.exchange)
  const industry = optionalNullableText(record.industry)
  const theme = optionalText(record.theme)
  const totalMv = optionalNullableNumber(record.total_mv)
  const circMv = optionalNullableNumber(record.circ_mv)
  if (exchange !== undefined) stock.exchange = exchange
  if (industry !== undefined) stock.industry = industry
  if (theme !== undefined) stock.theme = theme
  if (totalMv !== undefined) stock.total_mv = totalMv
  if (circMv !== undefined) stock.circ_mv = circMv
  return stock
}

function normalizeList(value: unknown): RecentStock[] {
  if (!Array.isArray(value)) return []
  const seen = new Set<string>()
  const result: RecentStock[] = []
  for (const item of value) {
    const stock = normalizeStock(item)
    if (!stock || seen.has(stock.symbol)) continue
    seen.add(stock.symbol)
    result.push(stock)
    if (result.length === MAX_RECENT_STOCKS) break
  }
  return result
}

export function loadRecentStocks(storage: StorageReader | null = browserStorage()): RecentStock[] {
  if (!storage) return []
  try {
    const raw = storage.getItem(RECENT_STOCKS_STORAGE_KEY)
    return raw ? normalizeList(JSON.parse(raw)) : []
  } catch {
    return []
  }
}

export function rememberRecentStock(
  stock: RecentStock,
  current: RecentStock[],
  storage: StorageWriter | null = browserStorage(),
): RecentStock[] {
  const normalized = normalizeStock(stock)
  if (!normalized) return normalizeList(current)
  const next = normalizeList([
    normalized,
    ...current.filter(item => item.symbol !== normalized.symbol),
  ])
  if (storage) {
    try {
      storage.setItem(RECENT_STOCKS_STORAGE_KEY, JSON.stringify(next))
    } catch {
      // Browsing remains usable when storage is unavailable or full.
    }
  }
  return next
}

import type { KlineDataDisplay } from '@/api/kline'

export function mergeKlinePages(
  current: KlineDataDisplay[],
  incoming: KlineDataDisplay[],
): KlineDataDisplay[] {
  const rows = new Map(current.map(item => [item.datetime, item]))
  for (const item of incoming) {
    if (!rows.has(item.datetime)) rows.set(item.datetime, item)
  }
  return [...rows.values()].sort((left, right) => right.datetime.localeCompare(left.datetime))
}

export function hasOlderKlinePage(state: {
  page: number
  totalPages: number
  loading: boolean
}): boolean {
  return !state.loading && state.page > 0 && state.page < state.totalPages
}

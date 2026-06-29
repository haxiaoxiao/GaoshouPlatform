import request from './request'

// 股票相关接口
export interface Stock {
  id?: number
  symbol: string
  name: string
  exchange?: string | null
  industry: string | null
  market?: string | null
  list_date: string | null
  is_active?: boolean
  is_st?: boolean
  total_mv?: number | null
  circ_mv?: number | null
}

export interface StockListParams {
  search?: string
  industry?: string
  group_id?: number
  page?: number
  page_size?: number
}

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface StockDetail extends Stock {
  industry2?: string | null
  industry3?: string | null
  sector?: string | null
  concept?: string | null
  is_suspend?: boolean
  is_delist?: boolean
  float_market_cap?: number | null
  market_cap: number | null
  pe_ratio: number | null
  pb_ratio: number | null
  roe: number | null
  eps?: number | null
  bvps?: number | null
  revenue_growth: number | null
  profit_growth: number | null
  debt_ratio: number | null
  current_ratio: number | null
  gross_margin: number | null
  net_margin: number | null
  dividend_yield: number | null
  latest_report_date?: string | null
  latest_ann_date?: string | null
  updated_at?: string | null
}

export const stockApi = {
  // 获取股票列表
  getList: (params?: StockListParams) =>
    request.get<PaginatedResponse<Stock>>('/data/stocks', { params }),

  // 获取股票详情
  getDetail: (symbol: string) =>
    request.get<StockDetail>(`/data/stocks/${symbol}`),
}

// 行业相关接口
export interface Industry {
  name: string
  count: number
}

export const industryApi = {
  // 获取行业列表
  getList: () =>
    request.get<Industry[]>('/data/industries'),
}

export interface IndexCatalogItem {
  symbol: string
  display_name: string
  provider: string
  provider_symbol: string
  market_family: string
  benchmark_enabled: boolean
  pool_enabled: boolean
  requires_daily_market_data: boolean
  requires_components_when_pool: boolean
  component_mode: 'snapshot' | 'derived_union' | 'none'
  available_from: string | null
  notes: string
  stock_pool_alias?: string | null
  jq_symbol?: string | null
  common_benchmark?: boolean
  component_status: 'available' | 'unavailable'
  reason: string | null
}

export const indexCatalogApi = {
  list: (params?: { benchmark_only?: boolean; pool_only?: boolean }) =>
    request.get<IndexCatalogItem[]>('/data/index-catalog', { params }),
}

export interface WatchlistGroup {
  id: number
  name: string
  description: string | null
  stock_count?: number
  created_at: string | null
  updated_at: string | null
}

export interface WatchlistStock {
  id: number
  group_id: number
  symbol: string
  stock_name: string | null
  added_at: string
  industry?: string | null
  industry2?: string | null
  industry3?: string | null
  sector?: string | null
  concept?: string | null
  ths_concepts?: string[]
  total_mv?: number | null
  circ_mv?: number | null
  pe_ttm?: number | null
  pb?: number | null
  roe?: number | null
  change_pct?: number | null
  latest_trade_date?: string | null
}

export interface CreateGroupParams {
  name: string
  description?: string
}

export const watchlistApi = {
  getGroups: () =>
    request.get<WatchlistGroup[]>('/data/watchlist/groups'),

  createGroup: (data: CreateGroupParams) =>
    request.post<WatchlistGroup>('/data/watchlist/groups', data),

  deleteGroup: (groupId: number) =>
    request.delete(`/data/watchlist/groups/${groupId}`),

  getGroupStocks: (groupId: number) =>
    request.get<WatchlistStock[]>(`/data/watchlist/groups/${groupId}/stocks`),

  addStock: (groupId: number, symbol: string) =>
    request.post<WatchlistStock>(`/data/watchlist/groups/${groupId}/stocks`, { symbol }),

  removeStock: (groupId: number, symbol: string) =>
    request.delete(`/data/watchlist/groups/${groupId}/stocks/${symbol}`),
}

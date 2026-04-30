import request from './request'

// 股票相关接口
export interface Stock {
  id: number
  symbol: string
  name: string
  industry: string | null
  market: string
  list_date: string | null
  is_active: boolean
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
  market_cap: number | null
  pe_ratio: number | null
  pb_ratio: number | null
  roe: number | null
  revenue_growth: number | null
  profit_growth: number | null
  debt_ratio: number | null
  current_ratio: number | null
  gross_margin: number | null
  net_margin: number | null
  dividend_yield: number | null
}

export const stockApi = {
  // 获取股票列表
  getList: (params?: StockListParams) =>
    request.get<PaginatedResponse<Stock>>('/data/stocks', { params }),

  // 获取股票详情
  getDetail: (id: number) =>
    request.get<StockDetail>(`/data/stocks/${id}`),
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

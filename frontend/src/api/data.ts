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

// 自选股分组相关接口
export interface WatchlistGroup {
  id: number
  name: string
  description: string | null
  created_at: string
  updated_at: string
  stock_count?: number
}

export interface CreateWatchlistGroupParams {
  name: string
  description?: string
}

export interface AddStockToGroupParams {
  stock_id: number
}

export const watchlistApi = {
  // 获取自选股分组列表
  getGroups: () =>
    request.get<WatchlistGroup[]>('/data/watchlist/groups'),

  // 创建自选股分组
  createGroup: (data: CreateWatchlistGroupParams) =>
    request.post<WatchlistGroup>('/data/watchlist/groups', data),

  // 添加股票到分组
  addStockToGroup: (groupId: number, data: AddStockToGroupParams) =>
    request.post(`/data/watchlist/groups/${groupId}/stocks`, data),

  // 从分组移除股票
  removeStockFromGroup: (groupId: number, stockId: number) =>
    request.delete(`/data/watchlist/groups/${groupId}/stocks/${stockId}`),

  // 删除分组
  deleteGroup: (groupId: number) =>
    request.delete(`/data/watchlist/groups/${groupId}`),
}

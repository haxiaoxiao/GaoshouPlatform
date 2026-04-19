import request from './request'

export interface CategoryInfo {
  key: string
  label: string
  count: number
}

export interface IndicatorInfo {
  name: string
  display_name: string
  category: string
  category_label: string
  tags: string[]
  data_type: string
  is_precomputed: boolean
  dependencies: string[]
  description: string
}

export interface IndicatorValueItem {
  symbol: string
  name: string | null
  indicators: Record<string, number | null>
}

export interface QueryResponse {
  trade_date: string
  items: IndicatorValueItem[]
}

export interface ComputeRequest {
  indicator_names?: string[]
  symbols?: string[]
  full_compute?: boolean
}

export interface ComputeResponse {
  results: Record<string, number>
  message: string
}

export interface ScreenFilter {
  indicator_name: string
  op: string
  value: number | number[]
}

export interface ScreenRequest {
  filters: ScreenFilter[]
  trade_date?: string
  sort_by?: string
  sort_order?: 'asc' | 'desc'
  limit?: number
}

export interface ScreenResult {
  items: Array<{
    symbol: string
    name: string | null
    indicators: Record<string, number | null>
  }>
  total: number
  trade_date: string
}

export const indicatorApi = {
  getCategories() {
    return request.get<CategoryInfo[]>('/indicators/categories')
  },

  listIndicators(category?: string) {
    return request.get<IndicatorInfo[]>('/indicators/list', {
      params: category ? { category } : {},
    })
  },

  getIndicatorDescription(name: string) {
    return request.get<IndicatorInfo>(`/indicators/${name}/description`)
  },

  queryIndicators(params: {
    symbols: string[]
    indicator_names: string[]
    trade_date?: string
  }) {
    return request.get<QueryResponse>('/indicators/query', {
      params: {
        symbols: params.symbols.join(','),
        indicator_names: params.indicator_names.join(','),
        trade_date: params.trade_date,
      },
    })
  },

  computeIndicators(data: ComputeRequest) {
    return request.post<ComputeResponse>('/indicators/compute', data)
  },

  screenStocks(data: ScreenRequest) {
    return request.post<ScreenResult>('/indicators/screen', data)
  },
}

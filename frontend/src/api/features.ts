import request from './request'

export interface FeatureDefinition {
  name: string
  display_name: string
  feature_type: string
  category: string
  frequency: string
  description: string
  unit: string
  as_of_time?: string | null
  params_schema: Record<string, unknown>
  dependencies: string[]
  lookback_days: number
  point_in_time_safe: boolean
  source: string
}

export interface FeatureCoverage {
  feature_name: string
  total_rows: number
  symbol_count: number
  date_count: number
  min_date: string | null
  max_date: string | null
  symbols_sample: string[]
  requested_symbol_count?: number
}

export interface FeaturePrecomputeRequest {
  feature_names: string[]
  start_date: string
  end_date: string
  symbols?: string[]
  index_symbol?: string
  params: Record<string, unknown>
}

export interface FeaturePrecomputeResult {
  symbols: number
  start_date: string
  end_date: string
  as_of_time: string
  window: number
  threshold: number
  rows: Record<string, number>
  rows_written: number
  high_volume?: FeaturePrecomputeResult | null
}

export interface FeatureGroup {
  name: string
  display_name: string
  description: string
  feature_names: string[]
}

export interface FeaturePreviewItem {
  symbol: string
  value: number
}

export interface FeaturePreview {
  feature_name: string
  trade_date: string
  items: FeaturePreviewItem[]
  total: number
}

export const featureApi = {
  definitions() {
    return request.get<FeatureDefinition[]>('/features/definitions')
  },

  groups() {
    return request.get<FeatureGroup[]>('/features/groups')
  },

  coverage(params: {
    feature_name: string
    start_date: string
    end_date: string
    index_symbol?: string
    symbols?: string
    as_of_time?: string
    window?: number
    threshold?: number
    daily_volume_to_share_multiplier?: number
  }) {
    return request.get<FeatureCoverage>('/features/coverage', { params })
  },

  precompute(data: FeaturePrecomputeRequest) {
    return request.post<FeaturePrecomputeResult>('/features/precompute', data)
  },

  precomputeGroup(data: {
    group_name: string
    start_date: string
    end_date: string
    symbols?: string[]
    index_symbol?: string
    params: Record<string, unknown>
  }) {
    return request.post<FeaturePrecomputeResult>('/features/groups/precompute', data)
  },

  preview(params: {
    feature_name: string
    trade_date: string
    index_symbol?: string
    symbols?: string
    as_of_time?: string
    window?: number
    threshold?: number
    limit?: number
  }) {
    return request.get<FeaturePreview>('/features/preview', { params })
  },
}

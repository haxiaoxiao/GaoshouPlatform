import request from './request'

export interface FactorValueDefinition {
  name: string
  display_name: string
  factor_type: string
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
  formula?: string
  human_description?: string
  version?: string
  data_policy?: Record<string, unknown>
}

export interface FactorValueCoverage {
  factor_name: string
  total_rows: number
  symbol_count: number
  date_count: number
  min_date: string | null
  max_date: string | null
  symbols_sample: string[]
  requested_symbol_count?: number
}

export interface FactorValuePrecomputeRequest {
  factor_names: string[]
  start_date: string
  end_date: string
  symbols?: string[]
  index_symbol?: string
  params: Record<string, unknown>
  async_task?: boolean
}

export interface FactorCoverageGap {
  dependency: string
  label: string
  latest_date: string | null
  required_start: string
  required_end: string
  missing_start: string
  missing_end: string
  sync_step: string
  reason: string
  severity: string
  timer_time?: string
}

export interface FactorPrecomputePrepareRequest extends FactorValuePrecomputeRequest {
  mode: 'single' | 'group'
  group_name?: string | null
}

export interface FactorPrecomputePrepareResult {
  can_precompute: boolean
  coverage_gaps: FactorCoverageGap[]
  sync_plan: Record<string, unknown> | null
  precompute_payload: Record<string, unknown>
}

export interface FactorValuePrecomputeResult {
  task_id?: string
  symbols: number
  start_date: string
  end_date: string
  as_of_time: string
  window: number
  threshold: number
  factor_names?: string[]
  rows: Record<string, number>
  rows_written: number
  requested_factor_count?: number
  written_factor_count?: number
  zero_row_factor_count?: number
  zero_row_factor_names?: string[]
  failed_factor_names?: string[]
  errors?: Record<string, string>
  coverage_ranges?: Array<{
    factor_name: string
    total_rows: number
    symbol_count: number
    date_count: number
    min_date: string | null
    max_date: string | null
    is_complete_to_end: boolean
  }>
  high_volume?: FactorValuePrecomputeResult | null
}

export interface FactorValueGroup {
  name: string
  display_name: string
  description: string
  factor_names: string[]
}

export interface FactorValueParamHash {
  factor_name: string
  params_hash: string
  as_of_time: string
  total_rows: number
  symbol_count: number
  date_count: number
  min_date: string | null
  max_date: string | null
  latest_created_at: string | null
  source: string
  is_default: boolean
}

export interface FactorPaperManifestItem {
  paper_id: number
  title: string
  strategy_type: string
  data_frequency: string
  data_dependencies: string[]
  factor_rules: string
  rebalance_frequency: string
  landing_status: string
  landing_grade: string
  platform_mapping: string
  validation_metrics: string[]
  factor_names: string[]
  notes: string
}

export interface FactorPaperExperimentSpec {
  paper_ids: number[]
  name: string
  model_family: string[]
  feature_groups: string[]
  default_factor_names: string[]
  status: string
  target_policy: string
}

export interface FactorValuePreviewItem {
  symbol: string
  value: number
}

export interface FactorValuePreview {
  factor_name: string
  trade_date: string
  items: FactorValuePreviewItem[]
  total: number
}

export const factorValueApi = {
  definitions() {
    return request.get<FactorValueDefinition[]>('/factor-values/definitions', { timeout: 30000 })
  },

  groups() {
    return request.get<FactorValueGroup[]>('/factor-values/groups', { timeout: 30000 })
  },

  paramHashes(data: {
    factor_names: string[]
    start_date?: string
    end_date?: string
    symbols?: string[]
    index_symbol?: string
    limit_per_factor?: number
  }) {
    return request.post<FactorValueParamHash[]>('/factor-values/param-hashes', data, { timeout: 60000 })
  },

  paperManifest() {
    return request.get<FactorPaperManifestItem[]>('/factor-values/paper-manifest', { timeout: 30000 })
  },

  paperExperiments() {
    return request.get<FactorPaperExperimentSpec[]>('/factor-values/paper-experiments', { timeout: 30000 })
  },

  prepare(data: FactorPrecomputePrepareRequest) {
    return request.post<FactorPrecomputePrepareResult>('/factor-values/precompute/prepare', data, { timeout: 60000 })
  },

  coverage(params: {
    factor_name: string
    start_date: string
    end_date: string
    index_symbol?: string
    symbols?: string
    as_of_time?: string
    window?: number
    threshold?: number
    daily_volume_to_share_multiplier?: number
    full_range?: boolean
    [key: string]: unknown
  }) {
    return request.get<FactorValueCoverage>('/factor-values/coverage', {
      params,
      timeout: 30000,
    })
  },

  precompute(data: FactorValuePrecomputeRequest) {
    return request.post<FactorValuePrecomputeResult>('/factor-values/precompute', data, { timeout: 600000 })
  },

  precomputeGroup(data: {
    group_name: string
    factor_names?: string[]
    start_date: string
    end_date: string
    symbols?: string[]
    index_symbol?: string
    params: Record<string, unknown>
    async_task?: boolean
  }) {
    return request.post<FactorValuePrecomputeResult>('/factor-values/groups/precompute', data, { timeout: 600000 })
  },

  preview(params: {
    factor_name: string
    trade_date: string
    index_symbol?: string
    symbols?: string
    as_of_time?: string
    window?: number
    threshold?: number
    limit?: number
    [key: string]: unknown
  }) {
    return request.get<FactorValuePreview>('/factor-values/preview', {
      params,
      timeout: 30000,
    })
  },
}

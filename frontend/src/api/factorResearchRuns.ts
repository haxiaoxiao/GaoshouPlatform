import request from './request'

export interface FactorResearchRunRequest {
  factor_name: string
  stock_pool_value: string
  start_date: string
  end_date: string
  portfolio_type: 'long_only' | 'long_short_i' | 'long_short_ii'
  rebalance_period: 'daily' | 'weekly' | 'monthly'
  fee_rate: number
  slippage: number
  filter_limit_up: boolean
  filter_limit_down: boolean
  group_count: number
  direction: 'asc' | 'desc'
  industry_neutralization: boolean
  standardize: boolean
  force?: boolean
}

export interface FactorResearchBatchRequest extends Omit<FactorResearchRunRequest, 'factor_name'> {
  factor_names: string[]
}

export interface FactorResearchRunSummary {
  run_id: string
  factor_name: string
  factor_display_name?: string | null
  stock_pool_value: string
  start_date: string
  end_date: string
  status: string
  created_at?: string | null
  completed_at?: string | null
  symbol_count?: number
  coverage_ratio?: number
  ic_mean?: number
  ic_std?: number
  icir?: number
  abs_ic_gt_002_ratio?: number
  long_short_return?: number
  max_drawdown?: number
  sharpe?: number
  turnover?: number
}

export interface FactorResearchRunDetail {
  run_id: string
  factor_name: string
  factor_display_name?: string | null
  stock_pool_value: string
  start_date: string
  end_date: string
  status: string
  created_at?: string | null
  completed_at?: string | null
  error_message?: string | null
  params?: Record<string, unknown>
  summary?: Record<string, number>
  ic_series?: Array<{ date: string; value: number }>
  industry_ic?: Array<{ industry: string; value: number }>
  turnover?: Array<{ date: string; min_quantile: number; max_quantile: number }>
  signal_decay?: Array<{ lag: number; min_quantile: number; max_quantile: number }>
  quantile_nav?: {
    groups?: Record<string, Array<{ date: string; value: number }>>
    long_short?: Array<{ date: string; value: number }>
  }
  quantile_summary?: Record<string, number>
  top?: Array<{ symbol: string; value: number }>
  bottom?: Array<{ symbol: string; value: number }>
  logs?: string[]
}

export interface FactorResearchPrepareResult {
  cache_hit: boolean
  latest_run: FactorResearchRunSummary | null
  params_hash: string
  coverage: Record<string, unknown>
  can_run: boolean
  message?: string | null
}

export interface FactorResearchBatchResult {
  batch_run_id: string
  items: Array<{
    factor_name: string
    status: string
    run_id?: string | null
    error_message?: string | null
  }>
}

export const factorResearchRunApi = {
  prepare: (data: FactorResearchRunRequest) =>
    request.post<FactorResearchPrepareResult>('/factor-research/runs/prepare', data),

  run: (data: FactorResearchRunRequest) =>
    request.post<FactorResearchRunDetail>('/factor-research/runs/run', data, { timeout: 0 }),

  batch: (data: FactorResearchBatchRequest) =>
    request.post<FactorResearchBatchResult>('/factor-research/runs/batch', data, { timeout: 0 }),

  get: (runId: string) =>
    request.get<FactorResearchRunDetail>(`/factor-research/runs/${runId}`),

  latest: (params: { factor_name: string; stock_pool_value?: string; params_hash?: string }) =>
    request.get<FactorResearchRunSummary | null>('/factor-research/runs/latest', { params }),
}

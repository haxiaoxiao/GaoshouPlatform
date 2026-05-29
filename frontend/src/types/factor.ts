/** Unified factor config — matches backend FactorConfig Pydantic model */
export interface FactorConfig {
  expression: string
  stock_pool: string
  start_date: string
  end_date: string
  benchmark?: string
  direction?: FactorDirection
}

export type StockPool =
  | 'hs300'
  | 'zz500'
  | 'zz800'
  | 'zz1000'
  | 'zz_quanzhi'
  | 'chinext'
  | 'chinext50'
  | 'chinext_composite'
  | 'star50'
  | 'star100'
export type FactorDirection = 'asc' | 'desc'

/** Evaluation config */
export interface EvalConfig {
  ic_method?: 'pearson' | 'spearman'
  group_count?: number
  outlier_handling?: 'none' | 'winsorize' | 'standardize'
  industry_neutralization?: boolean
  include_st?: boolean
  include_new?: boolean
}

/** Backtest config */
export interface BtConfig {
  rebalance_period?: 'daily' | 'weekly' | 'monthly'
  fee_rate?: number
  slippage?: number
  filter_limit_up?: boolean
  portfolio_type?: 'long_only' | 'long_short_i' | 'long_short_ii'
}

/** Factor template */
export interface FactorTemplate {
  id: string
  type: 'financial' | 'technical' | 'custom_operator' | 'custom_base'
  name: string
  description: string
  preset_expression: string
  preset_params: Record<string, string>
  category: string
}

/** Factor analysis report — 6 modules */
export interface FactorReport {
  ic_series: ICPoint[]
  industry_ic: IndustryIC[]
  turnover: TurnoverPoint[]
  signal_decay: DecayPoint[]
  top20: StockFactorValue[]
  bottom20: StockFactorValue[]
  update_date: string
}

export interface ICPoint {
  date: string
  value: number
}

export interface IndustryIC {
  industry: string
  value: number
}

export interface TurnoverPoint {
  date: string
  min_quantile: number
  max_quantile: number
}

export interface DecayPoint {
  lag: number
  min_quantile: number
  max_quantile: number
}

export interface StockFactorValue {
  symbol: string
  name: string
  value: number
}

/** Backtest report */
export interface BacktestReport {
  nav_series: NAVPoint[]
  benchmark_series: NAVPoint[]
  metrics: BacktestMetrics | null
  logs: string[]
}

export interface NAVPoint {
  date: string
  value: number
}

export interface BacktestMetrics {
  total_return: number
  annual_return: number
  sharpe: number
  max_drawdown: number
  alpha: number
  beta: number
  ir: number
}

/** Board query & response */
export interface BoardQuery {
  categories?: string[]
  factor_groups?: string[]
  factor_keyword?: string
  stock_pool?: string
  period?: '3m' | '1y' | '3y' | '10y'
  start_date?: string | null
  end_date?: string | null
  portfolio_type?: 'long_only' | 'long_short_i' | 'long_short_ii'
  fee_config?: 'none' | 'commission_stamp' | 'commission_stamp_slippage'
  fee_rate?: number
  stamp_tax_rate?: number
  transfer_fee_rate?: number
  slippage?: number
  filter_limit_up?: boolean
  pool_membership_mode?: 'static_latest' | 'point_in_time' | 'union'
  factor_value_params_hashes?: Record<string, string>
  sort_by?: string
  sort_order?: 'asc' | 'desc'
  page?: number
  page_size?: number
}

export interface BoardRow {
  factor_name: string
  display_name?: string | null
  description?: string | null
  source?: string | null
  factor_group?: string | null
  factor_group_display_name?: string | null
  category: string
  coverage_min_date?: string | null
  coverage_max_date?: string | null
  coverage_total_rows?: number
  coverage_symbol_count?: number
  coverage_date_count?: number
  coverage_status?: 'covered' | 'partial' | 'empty' | 'unknown'
  latest_run_id?: string | null
  latest_run_at?: string | null
  latest_ic_mean?: number | null
  latest_icir?: number | null
  latest_long_short_return?: number | null
  latest_max_drawdown?: number | null
  latest_turnover?: number | null
  latest_active_symbol_count?: number | null
  min_quantile_excess_return: number
  max_quantile_excess_return: number
  min_quantile_turnover: number
  max_quantile_turnover: number
  ic_mean: number
  ir: number
}

export interface BoardResponse {
  rows: BoardRow[]
  total: number
  page: number
  page_size: number
}

/** Validation */
export interface ValidateRequest {
  expression: string
  stock_pool?: StockPool
  date?: string
}

export interface ValidateResponse {
  valid: boolean
  error?: string | null
  preview_rows?: Record<string, unknown>[] | null
}

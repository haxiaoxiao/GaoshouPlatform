import request from './request'

export interface Strategy {
  id: number
  name: string
  code: string
  parameters: Record<string, unknown> | null
  description: string | null
  created_at: string | null
  updated_at: string | null
}

export interface StrategyCreate {
  name: string
  code: string
  parameters?: Record<string, unknown>
  description?: string
}

export interface StrategyUpdate {
  name?: string
  code?: string
  parameters?: Record<string, unknown>
  description?: string
}

export interface Backtest {
  id: number
  strategy_id: number
  status: 'pending' | 'running' | 'completed' | 'failed'
  start_date: string
  end_date: string
  initial_capital: string | null
  parameters: Record<string, unknown> | null
  result: BacktestResult | null
  created_at: string | null
}

export interface BacktestTrade {
  trade_date?: string
  symbol?: string
  direction?: string
  price?: number
  display_price?: number
  entry_price?: number
  exit_price?: number
  quantity?: number
  commission?: number
  pnl?: number | null
}

export interface BacktestResult {
  rows?: Record<string, unknown>[]
  count?: number
  row_count?: number
  metric?: string | string[]
  sort_by?: string | string[]
  ascending?: boolean | boolean[]
  train_period?: number
  test_period?: number
  total_return?: number
  annual_return?: number
  annual_volatility?: number
  max_drawdown?: number
  sharpe_ratio?: number
  sharpe?: number
  sortino?: number
  sortino_ratio?: number
  calmar?: number
  calmar_ratio?: number
  alpha?: number
  beta?: number
  information_ratio?: number
  total_trades?: number
  win_trades?: number
  loss_trades?: number
  total_positions?: number
  win_rate?: number
  avg_return?: number
  final_capital?: number
  trades?: BacktestTrade[]
  nav_series?: Array<{ date: string; nav: number }>
  daily_returns?: Array<{ date: string; return: number }>
  start_date?: string
  end_date?: string
  initial_capital?: number
  n_trading_days?: number
}

export interface BacktestCreate {
  strategy_id: number
  start_date: string
  end_date: string
  initial_capital?: number
  parameters?: Record<string, unknown>
}

export interface BacktestReport {
  id?: number
  backtest_id: number
  strategy_id: number
  strategy_name: string | null
  status: 'pending' | 'running' | 'completed' | 'failed'
  start_date: string
  end_date: string
  initial_capital: string | null
  parameters: Record<string, unknown> | null
  result: BacktestResult | null
  created_at: string | null
  updated_at: string | null
}

export interface LiveData {
  current_date: string | null
  events: Array<{
    type: string
    timestamp: string
    message?: string
    symbol?: string
    direction?: string
    quantity?: number
    price?: number
    close?: number
    order_id?: string
    trade_id?: string
    commission?: number
    reason?: string
  }>
  positions: Record<string, {
    shares: number
    avg_cost: number
    market_value: number
    unrealized_pnl: number
  }>
  metrics_snapshot: {
    total_return?: number
    max_drawdown?: number
    sharpe?: number
    cash?: number
    total_value?: number
    n_trades?: number
    chunk_index?: number
    chunk_total?: number
    symbol_count?: number
  }
  metadata?: {
    phase?: string
    chunk_index?: number
    chunk_total?: number
    chunk_start?: string
    chunk_end?: string
    symbol_count?: number
    bar_type?: string
  }
}

export interface TaskStatus {
  status: string
  progress: number
  live: LiveData | null
}

export interface EngineOption {
  name: string
  label: string
  modes: string[]
}

export interface AkquantCapabilities {
  available: boolean
  version: string | null
  features: Record<string, boolean>
  optional_modules?: Record<string, boolean>
  talib_function_count?: number
  talib_functions?: string[]
}

export interface BacktestCapabilities {
  engines: EngineOption[]
  akquant: AkquantCapabilities
}

export interface StrategyParamsSchemaRequest {
  strategy_code?: string | null
  strategy_id?: number | null
}

export interface StrategyParamsValidateRequest extends StrategyParamsSchemaRequest {
  payload?: Record<string, unknown>
}

export interface GridOptimizeRequest {
  engine: 'akquant'
  mode?: string
  strategy_code?: string
  strategy_id?: number
  param_grid: Record<string, unknown[]>
  symbols?: string[]
  universe_mode?: 'symbols' | 'index'
  index_symbol?: string
  start_date: string
  end_date: string
  initial_capital?: number
  bar_type?: string
  timer_times?: string[]
  commission_rate?: number
  slippage?: number
  stamp_tax_rate?: number
  transfer_fee_rate?: number
  min_commission?: number
  volume_limit_pct?: number | null
  t_plus_one?: boolean
  lot_size?: number | Record<string, number> | null
  max_positions?: number | null
  risk_config?: Record<string, unknown> | null
  sort_by?: string | string[]
  ascending?: boolean | boolean[]
  max_workers?: number
  timeout?: number | null
}

export interface WalkForwardOptimizeRequest extends GridOptimizeRequest {
  train_period?: number
  test_period?: number
  metric?: string | string[]
}

export interface BacktestDataCoverageFactorItem {
  factor_name: string
  total_rows: number
  symbol_count: number
  date_count: number
  min_date: string | null
  max_date: string | null
  coverage_ratio: number
  required: boolean
}

export interface BacktestDataCoverage {
  start_date: string
  end_date: string
  bar_type: string
  index_symbol?: string | null
  symbol_count: number
  ok: boolean
  warnings: string[]
  market: {
    dataset: string
    total_rows: number
    requested_symbol_count: number
    covered_symbol_count: number
    missing_symbol_count: number
    coverage_ratio: number
    date_range?: string | null
    missing_symbols_sample?: string[]
    symbols_covered_sample?: string[]
  }
  factor?: {
    strategy: string
    min_required_symbol_coverage: number
    warning: boolean
    items: BacktestDataCoverageFactorItem[]
  } | null
}

export const backtestEngines = {
  list: () => request.get<EngineOption[]>('/backtest/engines'),
  capabilities: () => request.get<BacktestCapabilities>('/backtest/capabilities'),
  dataCoverage: (data: Record<string, unknown>) =>
    request.post<BacktestDataCoverage>('/backtest/data-coverage', data),
  optimizeGrid: (data: GridOptimizeRequest) =>
    request.post<{ task_id: string }>('/backtest/optimize/grid', data),
  optimizeWalkForward: (data: WalkForwardOptimizeRequest) =>
    request.post<{ task_id: string }>('/backtest/optimize/walk-forward', data),
  strategyParamsSchema: (data: StrategyParamsSchemaRequest) =>
    request.post<Record<string, unknown>>('/backtest/strategy-params/schema', data),
  validateStrategyParams: (data: StrategyParamsValidateRequest) =>
    request.post<Record<string, unknown>>('/backtest/strategy-params/validate', data),
  report: (taskId: string) => request.get<string>(`/backtest/report/${taskId}`),
  dualStockGridPreset: () => request.get<Record<string, unknown>>('/backtest/presets/dual-stock-grid'),
  createDualStockGridStrategy: () => request.post<Strategy>('/backtest/presets/dual-stock-grid/strategy', {}),
  createMultiFactorStrategy: () => request.post<Strategy>('/backtest/presets/multi-factor/strategy', {}),
}


export interface BacktestResultData {
  total_return?: number
  annual_return?: number
  annual_volatility?: number
  sharpe?: number
  sharpe_ratio?: number
  sortino?: number
  sortino_ratio?: number
  max_drawdown?: number
  calmar?: number
  calmar_ratio?: number
  alpha?: number
  beta?: number
  information_ratio?: number
  total_trades?: number
  win_trades?: number
  loss_trades?: number
  total_positions?: number
  win_rate?: number
  avg_return?: number
  turnover_rate?: number
  trades?: Array<{
    trade_date?: string
    symbol?: string
    direction?: string
    price?: number
    display_price?: number
    entry_price?: number
    exit_price?: number
    quantity?: number
    commission?: number
    pnl?: number | null
  }>
  nav_series?: Array<{ date: string; nav: number }>
  daily_returns?: Array<{ date: string; return: number }>
  start_date?: string | null
  end_date?: string | null
  initial_capital?: number
  final_capital?: number
  n_trading_days?: number
  report_path?: string | null
  basket_returns?: number[]
  basket_count?: number
}

export const strategyApi = {
  list: (page = 1, pageSize = 20) =>
    request.get<{ items: Strategy[]; total: number }>(`/backtest/strategies?page=${page}&page_size=${pageSize}`),
  get: (id: number) => request.get<Strategy>(`/backtest/strategies/${id}`),
  create: (data: StrategyCreate) => request.post<Strategy>('/backtest/strategies', data),
  update: (id: number, data: StrategyUpdate) => request.put<Strategy>(`/backtest/strategies/${id}`, data),
  delete: (id: number) => request.delete<{ deleted: boolean }>(`/backtest/strategies/${id}`),
}

export const backtestApi = {
  list: (params?: { strategy_id?: number; status?: string; page?: number; page_size?: number }) => {
    const query = new URLSearchParams()
    if (params?.strategy_id) query.set('strategy_id', String(params.strategy_id))
    if (params?.status) query.set('status', params.status)
    if (params?.page) query.set('page', String(params.page))
    if (params?.page_size) query.set('page_size', String(params.page_size))
    return request.get<{ items: Backtest[]; total: number }>(`/backtest/backtests?${query}`)
  },
  get: (id: number) => request.get<BacktestReport>(`/backtest/backtests/${id}`),
  create: (data: BacktestCreate) => request.post<Backtest>('/backtest/backtests', data),
  run: (id: number) => request.post<BacktestResult>(`/backtest/backtests/${id}/run`),
  delete: (id: number) => request.delete<{ deleted: boolean }>(`/backtest/backtests/${id}`),
  deleteBatch: (ids: number[]) => request.delete<{ deleted_count: number }>('/backtest/backtests/batch', { data: { ids } }),
}

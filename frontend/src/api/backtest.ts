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
  result: BacktestResult | null
  created_at: string | null
}

export interface BacktestResult {
  total_return: number
  annual_return: number
  max_drawdown: number
  sharpe_ratio: number
  total_trades: number
  win_trades: number
  loss_trades: number
  win_rate: number
  final_capital: number
}

export interface BacktestCreate {
  strategy_id: number
  start_date: string
  end_date: string
  initial_capital?: number
  parameters?: Record<string, unknown>
}

export interface BacktestReport {
  backtest: Backtest & { strategy_name: string | null }
  summary: BacktestResult
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
}

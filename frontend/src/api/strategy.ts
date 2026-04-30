import request from './request'

export interface SignalItem {
  symbol: string
  trade_date: string
  signal_a_value: number | null
  signal_b_value: number | null
  signal_c_value: number | null
  signal_a_triggered: boolean
  signal_b_triggered: boolean
  signal_c_triggered: boolean
  composite_triggered: boolean
  trend_minute_count: number
  total_minute_count: number
}

export interface DailySignals {
  trade_date: string
  total_stocks: number
  triggered_count: number
  composite_count: number
  signals: SignalItem[]
}

export interface DailySummary {
  trade_date: string
  count: number
  stocks: Array<{
    symbol: string
    a: boolean
    b: boolean
    c: boolean
    a_val: number | null
    b_val: number | null
    c_val: number | null
  }>
}

export interface SignalsSummary {
  start_date: string
  end_date: string
  trading_days_with_signals: number
  total_composite_triggers: number
  avg_daily_triggers: number
  daily_summary: DailySummary[]
}

export interface BacktestResult {
  total_trades: number
  win_count: number
  win_rate: number
  avg_return: number
  total_return: number
  best_trade: number
  worst_trade: number
  trades: Array<{
    symbol: string
    channel: number
    entry_date: string
    entry_price: number
    exit_date: string | null
    exit_price: number | null
    pnl_pct: number | null
  }>
}

export const strategyApi = {
  getDailySignals: (params: { start_date: string; end_date: string; symbols?: string }) =>
    request.get<DailySignals[]>('/strategy/signals/daily', { params }),

  getSignalsSummary: (params: { start_date: string; end_date: string; symbols?: string }) =>
    request.get<SignalsSummary>('/strategy/signals/summary', { params }),

  runChannelBacktest: (params: { start_date: string; end_date: string; symbols?: string }) =>
    request.get<BacktestResult>('/strategy/channel-backtest', { params }),
}

import request from './request'

export type IntradayTSymbol = '603629.SH' | '688008.SH'
export type IntradayTDirection = 'POSITIVE' | 'REVERSE'
export type IntradayTSide = 'BUY' | 'SELL'
export type IntradayTState =
  | 'READY'
  | 'POSITIVE_T_OPEN'
  | 'REVERSE_T_OPEN'
  | 'FORCE_RESTORE'
  | 'RESTORED'
  | 'LOCKED'

export interface IntradayTStrategyParams {
  warmup_bars?: number
  volatility_window?: number
  fast_ema_span?: number
  slow_ema_span?: number
  vwap_slope_bars?: number
  entry_z?: number
  max_entry_z?: number
  exit_z?: number
  stop_z?: number
  realized_vol_window?: number
  min_realized_vol_bps?: number
  max_trade_fraction?: number
  max_pairs_per_day?: number
  cooldown_minutes?: number
  edge_buffer_bps?: number
  max_daily_loss_bps?: number
}

export interface IntradayTCostModel {
  commission_rate?: number
  min_commission?: number
  stamp_duty_rate?: number
  transfer_fee_rate?: number
  slippage_bps?: number
}

export interface IntradayTCapabilities {
  symbols: Array<{ symbol: IntradayTSymbol; name: string; board: 'MAIN' | 'STAR' }>
  modes: Array<'backtest' | 'paper'>
  real_order_submit_enabled: false
  defaults: {
    strategy: IntradayTStrategyParams
    cost: Required<IntradayTCostModel>
    initial_capital: number
    cash_buffer_fraction: number
    max_bar_volume_fraction: number
  }
  risk_controls: {
    next_bar_fill: boolean
    t_plus_one_sellable_inventory: boolean
    lunch_restore_time: string
    force_restore_time: string
    max_daily_loss_bps: number
    max_trade_fraction: number
    simulated_only: boolean
  }
}

export interface IntradayTCoverageItem {
  symbol: IntradayTSymbol
  name: string
  bars: number
  trade_days: number
  start: string | null
  end: string | null
}

export interface IntradayTCoverage {
  requested: { start_date: string; end_date: string }
  coverage: IntradayTCoverageItem[]
}

export interface IntradayTBacktestRequest {
  symbols: IntradayTSymbol[]
  start_date: string
  end_date: string
  initial_capital: number
  base_quantities?: Partial<Record<IntradayTSymbol, number>>
  cash_buffer_fraction: number
  max_bar_volume_fraction: number
  strategy: IntradayTStrategyParams
  cost?: IntradayTCostModel
}

export interface IntradayTBacktestMetrics {
  initial_capital: number
  final_equity: number
  passive_final_equity: number
  incremental_pnl: number
  incremental_return: number
  cost_reduction_per_share: number
  completed_pairs: number
  entry_count: number
  restoration_failures: number
  restoration_rate: number
  open_pairs_at_end: number
  total_fees: number
  rejection_count: number
  gross_t_pnl: number
  net_t_pnl: number
  win_rate: number
  profit_loss_ratio: number | null
  max_drawdown: number
  max_daily_t_loss: number
}

export interface IntradayTEquityPoint {
  trade_date: string
  equity: number
  passive_equity: number
  incremental_pnl: number
  daily_t_pnl?: number
}

export interface IntradayTTrade {
  trade_id?: string
  session_id?: string
  trade_date?: string
  pair_id: string
  symbol: IntradayTSymbol
  name: string
  direction: IntradayTDirection
  leg: 'entry' | 'restore'
  side: IntradayTSide
  quantity: number
  signal_at: string
  fill_at: string
  reference_price: number
  fill_price: number
  fees: number
  gross_pnl: number
  net_pnl: number
  reason: string
  status?: string
  simulated?: true
}

export interface IntradayTSymbolSummary {
  symbol: IntradayTSymbol
  name: string
  opening_quantity: number
  ending_quantity: number
  completed_pairs: number
  net_pnl: number
}

export interface IntradayTBacktestResult {
  symbols: IntradayTSymbol[]
  period: {
    start: string
    end: string
    trade_days: number
    common_trade_days: number
    symbol_trade_days: Record<IntradayTSymbol, number>
    missing_observed_days: Record<IntradayTSymbol, string[]>
    bars: number
  }
  parameters: Record<string, unknown>
  metrics: IntradayTBacktestMetrics
  equity_curve: IntradayTEquityPoint[]
  daily_results: IntradayTEquityPoint[]
  direction_metrics: Record<IntradayTDirection, {
    completed_pairs: number
    gross_pnl: number
    net_pnl: number
    win_rate: number
  }>
  symbol_summaries: IntradayTSymbolSummary[]
  trades: IntradayTTrade[]
  rejections: Array<{
    symbol: IntradayTSymbol
    side: IntradayTSide
    quantity: number
    signal_at: string
    attempted_at: string
    reason: string
  }>
  data_quality: {
    limit_prices: {
      mode: 'fail_closed' | 'best_effort'
      expected_symbol_days: number
      available_symbol_days: number
      missing_symbol_days: string[]
    }
  }
}

export interface IntradayTPositionInput {
  quantity: number
  available: number
  avg_cost: number
}

export interface IntradayTPaperStartRequest {
  manual_account?: {
    cash: number
    positions: Partial<Record<IntradayTSymbol, IntradayTPositionInput>>
  }
  strategy: IntradayTStrategyParams
}

export interface IntradayTPaperSymbolState {
  symbol: IntradayTSymbol
  opening_quantity: number
  opening_sellable: number
  current_quantity: number
  sellable_remaining: number
  state: IntradayTState
  completed_pairs: number
  active_quantity: number
  active_direction: IntradayTDirection | null
  active_entry_price: number | null
  active_entry_at: string | null
  last_completed_at: string | null
  realized_net_pnl: number
}

export interface IntradayTPaperSession {
  session_id: string
  trade_date: string
  status: 'RUNNING' | 'STOPPED'
  mode: 'paper'
  account_source: 'manual' | 'qmt' | string
  params: Required<IntradayTStrategyParams>
  baseline: {
    cash: number
    positions: Partial<Record<IntradayTSymbol, IntradayTPositionInput>>
  }
  cash: number
  states: Partial<Record<IntradayTSymbol, IntradayTPaperSymbolState>>
  pending: Partial<Record<IntradayTSymbol, IntradayTSignal>>
  last_evaluated_at: string | null
  last_error: string | null
  runner_active: boolean
  recoverable: boolean
  real_order_submit_enabled: false
  duplicate?: boolean
  signals?: IntradayTSignal[]
  fills?: IntradayTTrade[]
}

export interface IntradayTSignal {
  symbol: IntradayTSymbol
  side: IntradayTSide
  quantity: number
  direction: IntradayTDirection
  reason: string
  signal_at: string
  reference_price: number
}

export const intradayTApi = {
  capabilities: () => request.get<IntradayTCapabilities>('/intraday-t/capabilities'),
  coverage: (symbols: IntradayTSymbol[], startDate: string, endDate: string) =>
    request.get<IntradayTCoverage>('/intraday-t/coverage', {
      params: {
        symbols: symbols.join(','),
        start_date: startDate,
        end_date: endDate,
      },
    }),
  runBacktest: (payload: IntradayTBacktestRequest) =>
    request.post<IntradayTBacktestResult>('/intraday-t/backtest', payload),
  startPaper: (payload: IntradayTPaperStartRequest) =>
    request.post<IntradayTPaperSession>('/intraday-t/paper/start', payload),
  paperStatus: (sessionId?: string) =>
    request.get<IntradayTPaperSession>('/intraday-t/paper/status', {
      params: sessionId ? { session_id: sessionId } : {},
      notifyError: false,
    }),
  evaluatePaper: (sessionId: string) =>
    request.post<IntradayTPaperSession>(`/intraday-t/paper/${sessionId}/evaluate`),
  startPaperRunner: (sessionId: string, intervalSeconds = 30) =>
    request.post<IntradayTPaperSession>(`/intraday-t/paper/${sessionId}/runner/start`, {
      interval_seconds: intervalSeconds,
    }),
  stopPaperRunner: (sessionId: string) =>
    request.post<IntradayTPaperSession>(`/intraday-t/paper/${sessionId}/runner/stop`),
  stopPaper: (sessionId: string) =>
    request.post<IntradayTPaperSession>(`/intraday-t/paper/${sessionId}/stop`),
  resetPaper: (sessionId: string) =>
    request.post<IntradayTPaperSession>(`/intraday-t/paper/${sessionId}/reset`),
  paperTrades: (sessionId: string) =>
    request.get<IntradayTTrade[]>(`/intraday-t/paper/${sessionId}/trades`),
}

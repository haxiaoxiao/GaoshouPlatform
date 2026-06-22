import request from './request'

export type LiveTradingMode = 'paper' | 'live'

export interface LiveStrategyProfile {
  id: number
  strategy_id: number
  profile_key: string
  display_name: string
  description?: string | null
  enabled: boolean
  is_default: boolean
  adapter_type: string
  params_override: Record<string, unknown>
  universe_config: Record<string, unknown>
  execution_policy: Record<string, unknown>
  strategy_name?: string | null
}

export interface LiveTradingStatus {
  xtdata_available: boolean
  xttrader_available: boolean
  quote_connected: boolean
  account_configured: boolean
  account_id: string
  account_type: string
  trader_path: string
  data_dir?: string | null
  order_submit_enabled: boolean
  auto_execute_enabled: boolean
  default_profile: string
  profile_count: number
  error?: string | null
  runner: {
    status: string
    mode?: LiveTradingMode | null
    profile_key?: string | null
    run_id?: string | null
    last_cycle_at?: string | null
    last_signal_hash?: string | null
    last_error?: string | null
    last_wait_reason?: string | null
    takeover?: boolean
  }
}

export interface LiveOrder {
  profile_key: string
  strategy_id: number
  strategy_name?: string
  symbol: string
  stock_name?: string | null
  side: 'BUY' | 'SELL'
  quantity: number
  price_type?: string
  reference_price?: number
  remark?: string
  signal_hash?: string
}

export interface LiveSkippedOrder {
  profile_key: string
  strategy_id: number
  symbol: string
  stock_name?: string | null
  side: 'BUY' | 'SELL'
  quantity: number
  reason: string
  signal_hash?: string
}

export interface LiveSignalsResponse {
  timestamp: string
  profile: LiveStrategyProfile
  mode: LiveTradingMode
  strategy_id: number
  strategy_name: string
  trade_date: string
  account: {
    cash: number
    total_asset: number
    market_value: number
    positions: Record<string, Record<string, unknown>>
    source: string
    error?: string | null
    meta?: Record<string, unknown>
  }
  universe_size: number
  candidate_count: number
  excluded_symbol_count: number
  target_symbols: string[]
  target_weights: Record<string, number>
  entry_filter: Record<string, unknown>
  quote_error?: string | null
  heat_filter_note?: string | null
  order_submit_enabled: boolean
  auto_execute_enabled: boolean
  signal_hash: string
  orders: LiveOrder[]
  skipped_orders: LiveSkippedOrder[]
  top_candidates: Record<string, unknown>[]
  factor_dates?: Record<string, unknown>
  preflight?: LivePreflightResponse | null
}

export interface LivePreflightResponse {
  timestamp: string
  profile: LiveStrategyProfile
  mode: LiveTradingMode
  trade_date: string
  market_phase: Record<string, unknown>
  settings: Record<string, unknown>
  qmt_status: Record<string, unknown>
  account: Record<string, unknown>
  policy: Record<string, unknown>
  universe: {
    index_symbol?: string | null
    symbol_count: number
    sample_symbols?: string[]
    error?: string | null
  }
  strategy: {
    factor_names: string[]
    filter_names: string[]
    min_factor_coverage: number
    rebalance_time: string
  }
  dependency_prepare?: Record<string, unknown> | null
  dependency_error?: string | null
  factor_coverage: Array<Record<string, unknown>>
  pipeline_probe?: Record<string, unknown> | null
  factor_dates?: Record<string, unknown>
  intraday_prepare?: Record<string, unknown> | null
  blocking_reasons: string[]
  runner_blocking_reasons: string[]
  warnings: string[]
  next_actions: string[]
  can_generate: boolean
  can_start_runner: boolean
  can_auto_submit: boolean
}

export interface LiveOrderAudit {
  audit_id: string
  run_id?: string | null
  profile_key: string
  strategy_id: number
  trade_date?: string | null
  signal_hash?: string | null
  trigger_source: string
  mode: LiveTradingMode
  status: string
  order_payload: Record<string, unknown>
  result_payload?: Record<string, unknown> | null
  skip_reason?: string | null
  created_at?: string | null
}

export interface LiveTradeRecord {
  record_id: string
  run_id?: string | null
  profile_key: string
  strategy_id: number
  trade_date?: string | null
  signal_hash?: string | null
  trigger_source: string
  mode: LiveTradingMode
  status: string
  symbol: string
  stock_name?: string | null
  side: 'BUY' | 'SELL' | string
  quantity: number
  reference_price: number
  order_value: number
  order_id?: string | null
  message?: string | null
  order_payload: Record<string, unknown>
  result_payload?: Record<string, unknown> | null
  account_snapshot?: Record<string, unknown> | null
  created_at?: string | null
}

export interface LiveSubmitOrderResult {
  enabled?: boolean
  submitted: boolean
  pending?: boolean
  paper?: boolean
  status?: string
  order_id?: string | number | null
  message?: string | null
  filled_quantity?: number
  filled_price?: number
  filled_value?: number
  realized_pnl?: number
  order?: Record<string, unknown>
  account_snapshot?: Record<string, unknown> | null
}

export interface LiveSubmitOrdersResponse {
  enabled?: boolean
  submitted: boolean
  paper?: boolean
  duplicate?: boolean
  pending_count?: number
  message?: string | null
  orders?: Record<string, unknown>[]
  results?: LiveSubmitOrderResult[]
}

export interface LiveWeeklyAnalysis {
  generated_at: string
  week_start: string
  week_end: string
  profile_key?: string | null
  mode?: LiveTradingMode | null
  summary: {
    total_records: number
    completed_records: number
    failed_records: number
    buy_notional: number
    sell_notional: number
    net_notional: number
    paper_realized_pnl: number
    live_submitted_records: number
  }
  by_day: Array<{
    trade_date: string
    records: number
    buy_notional: number
    sell_notional: number
    failed: number
    paper_realized_pnl: number
  }>
  by_profile: Array<{
    profile_key: string
    records: number
    notional: number
  }>
  by_status: Array<{
    status: string
    records: number
  }>
  by_side: Array<{
    side: string
    records: number
    notional: number
  }>
  top_symbols: Array<{
    symbol: string
    stock_name?: string | null
    records: number
    notional: number
    net_notional: number
  }>
  notes: string[]
}

export interface LiveAccountPosition {
  symbol: string
  stock_name?: string | null
  quantity: number
  available: number
  avg_cost: number
  market_value: number
  cost_value?: number
  unrealized_pnl?: number
  unrealized_pnl_pct?: number | null
}

export interface LiveAccountSnapshot {
  timestamp: string
  mode: LiveTradingMode
  cash: number
  total_asset: number
  market_value: number
  unrealized_pnl: number
  position_count: number
  positions: LiveAccountPosition[]
  positions_by_symbol: Record<string, Record<string, unknown>>
  source: string
  error?: string | null
  meta?: Record<string, unknown>
  broker_account?: LiveAccountSnapshot
}

export const liveTradingApi = {
  status: () => request.get<LiveTradingStatus>('/live-trading/status'),
  account: (mode: LiveTradingMode = 'live', profileKey?: string) => {
    const query = new URLSearchParams({ mode })
    if (profileKey) query.set('profile_key', profileKey)
    return request.get<LiveAccountSnapshot>(`/live-trading/account?${query.toString()}`)
  },
  initializeAccount: (data: { profile_key?: string | null; mode: LiveTradingMode; capital: number; reset_existing?: boolean }) =>
    request.post<LiveAccountSnapshot>('/live-trading/account/initialize', data),
  profiles: (includeDisabled = true) =>
    request.get<LiveStrategyProfile[]>(`/live-trading/strategy-profiles?include_disabled=${includeDisabled ? 'true' : 'false'}`),
  createProfile: (data: Record<string, unknown>) =>
    request.post<LiveStrategyProfile>('/live-trading/strategy-profiles', data),
  updateProfile: (profileKey: string, data: Record<string, unknown>) =>
    request.put<LiveStrategyProfile>(`/live-trading/strategy-profiles/${encodeURIComponent(profileKey)}`, data),
  preflight: (data: {
    profile_key?: string | null
    mode?: LiveTradingMode
    params?: Record<string, unknown>
    manual_account?: Record<string, unknown> | null
    evaluate_pipeline?: boolean
  }) => request.post<LivePreflightResponse>('/live-trading/preflight', data),
  signals: (data: {
    profile_key?: string | null
    mode?: LiveTradingMode
    params?: Record<string, unknown>
    manual_account?: Record<string, unknown> | null
  }) => request.post<LiveSignalsResponse>('/live-trading/signals', data),
  startRunner: (data: {
    profile_key?: string | null
    mode: LiveTradingMode
    params?: Record<string, unknown>
    interval_seconds?: number
  }) => request.post<LiveTradingStatus['runner']>('/live-trading/runner/start', data),
  stopRunner: () => request.post<LiveTradingStatus['runner']>('/live-trading/runner/stop', {}),
  takeover: (reason = 'human takeover') =>
    request.post<LiveTradingStatus['runner']>('/live-trading/runner/takeover', { reason }),
  submitOrders: (data: { mode: LiveTradingMode; orders: Record<string, unknown>[]; confirm?: boolean }) =>
    request.post<LiveSubmitOrdersResponse>('/live-trading/orders/submit', data),
  audits: (params?: { profile_key?: string; limit?: number }) => {
    const query = new URLSearchParams()
    if (params?.profile_key) query.set('profile_key', params.profile_key)
    if (params?.limit) query.set('limit', String(params.limit))
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return request.get<LiveOrderAudit[]>(`/live-trading/orders/audit${suffix}`)
  },
  pendingOrders: (params?: { profile_key?: string; mode?: LiveTradingMode; limit?: number }) => {
    const query = new URLSearchParams()
    if (params?.profile_key) query.set('profile_key', params.profile_key)
    if (params?.mode) query.set('mode', params.mode)
    if (params?.limit) query.set('limit', String(params.limit))
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return request.get<LiveTradeRecord[]>(`/live-trading/orders/pending${suffix}`)
  },
  trades: (params?: { profile_key?: string; mode?: LiveTradingMode; start_date?: string; end_date?: string; limit?: number }) => {
    const query = new URLSearchParams()
    if (params?.profile_key) query.set('profile_key', params.profile_key)
    if (params?.mode) query.set('mode', params.mode)
    if (params?.start_date) query.set('start_date', params.start_date)
    if (params?.end_date) query.set('end_date', params.end_date)
    if (params?.limit) query.set('limit', String(params.limit))
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return request.get<LiveTradeRecord[]>(`/live-trading/trades${suffix}`)
  },
  weeklyAnalysis: (params?: { profile_key?: string; mode?: LiveTradingMode; week_start?: string }) => {
    const query = new URLSearchParams()
    if (params?.profile_key) query.set('profile_key', params.profile_key)
    if (params?.mode) query.set('mode', params.mode)
    if (params?.week_start) query.set('week_start', params.week_start)
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return request.get<LiveWeeklyAnalysis>(`/live-trading/trades/weekly${suffix}`)
  },
}

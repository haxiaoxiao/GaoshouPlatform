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
    takeover?: boolean
  }
}

export interface LiveOrder {
  profile_key: string
  strategy_id: number
  strategy_name?: string
  symbol: string
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

export const liveTradingApi = {
  status: () => request.get<LiveTradingStatus>('/live-trading/status'),
  profiles: (includeDisabled = true) =>
    request.get<LiveStrategyProfile[]>(`/live-trading/strategy-profiles?include_disabled=${includeDisabled ? 'true' : 'false'}`),
  createProfile: (data: Record<string, unknown>) =>
    request.post<LiveStrategyProfile>('/live-trading/strategy-profiles', data),
  updateProfile: (profileKey: string, data: Record<string, unknown>) =>
    request.put<LiveStrategyProfile>(`/live-trading/strategy-profiles/${encodeURIComponent(profileKey)}`, data),
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
    request.post<Record<string, unknown>>('/live-trading/orders/submit', data),
  audits: (params?: { profile_key?: string; limit?: number }) => {
    const query = new URLSearchParams()
    if (params?.profile_key) query.set('profile_key', params.profile_key)
    if (params?.limit) query.set('limit', String(params.limit))
    const suffix = query.toString() ? `?${query.toString()}` : ''
    return request.get<LiveOrderAudit[]>(`/live-trading/orders/audit${suffix}`)
  },
}

import request from './request'

export interface GridSignal {
  symbol: string
  name: string
  action: 'BUY' | 'SELL' | 'HOLD' | 'NO_QUOTE'
  quantity: number
  current_price: number | null
  anchor_price: number | null
  anchor_source?: string | null
  last_grid_price: number | null
  trigger_price: number | null
  next_buy_price?: number
  next_sell_price?: number
  grid_pct: number | null
  grid_level?: number
  position_qty?: number
  available_qty?: number
  base_position_qty?: number
  reason: string
  timestamp: string
  signal_key?: string
  order_preview?: Record<string, unknown> | null
}

export interface GridStatus {
  xtdata_available: boolean
  xttrader_available: boolean
  quote_connected: boolean
  account_configured: boolean
  account_id: string
  account_type: string
  trader_path: string
  data_dir?: string | null
  order_submit_enabled: boolean
  error?: string | null
}

export interface GridAccount {
  cash: number
  total_asset: number
  market_value: number
  positions: Record<string, Record<string, unknown>>
  source: string
  error?: string | null
}

export interface GridSignalsResponse {
  timestamp: string
  symbols: string[]
  account: {
    cash: number
    total_asset: number
    market_value: number
    source: string
    error?: string | null
  }
  quote_error?: string | null
  order_submit_enabled: boolean
  signals: GridSignal[]
}

export interface GridSignalRequest {
  params?: Record<string, unknown>
  manual_account?: Record<string, unknown> | null
}

export interface TechSmallCapVariant {
  key: string
  name: string
  description: string
  params: Record<string, unknown>
}

export interface TechSmallCapOrder {
  symbol: string
  side: 'BUY' | 'SELL'
  quantity: number
  reference_price?: number
  price_type?: string
  strategy_name?: string
  remark?: string
}

export interface TechSmallCapSignalsResponse {
  timestamp: string
  strategy: string
  variant: string
  trade_date: string
  account: {
    cash: number
    total_asset: number
    market_value: number
    source: string
    error?: string | null
  }
  universe_size: number
  candidate_count: number
  target_symbols: string[]
  target_weights: Record<string, number>
  entry_filter: Record<string, unknown>
  quote_error?: string | null
  order_submit_enabled: boolean
  orders: TechSmallCapOrder[]
  top_candidates: Record<string, unknown>[]
  excluded_symbol_count: number
}

export const gridTradingApi = {
  status: () =>
    request.get<GridStatus>('/grid-trading/status'),
  account: () =>
    request.get<GridAccount>('/grid-trading/account'),
  signals: (data: GridSignalRequest = {}) =>
    request.post<GridSignalsResponse>('/grid-trading/signals', data),
  submitPreview: (orderPreview: Record<string, unknown>) =>
    request.post<Record<string, unknown>>('/grid-trading/orders/preview', { order_preview: orderPreview }),
  submitOrder: (order: Record<string, unknown>) =>
    request.post<Record<string, unknown>>('/grid-trading/orders/submit', { order }),
  techSmallCapVariants: () =>
    request.get<TechSmallCapVariant[]>('/grid-trading/tech-small-cap/variants'),
  techSmallCapSignals: (data: GridSignalRequest = {}) =>
    request.post<TechSmallCapSignalsResponse>('/grid-trading/tech-small-cap/signals', data),
  submitTechSmallCapOrders: (orders: Record<string, unknown>[], confirm = false) =>
    request.post<Record<string, unknown>>('/grid-trading/tech-small-cap/orders/submit', { orders, confirm }),
}

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
  order_submit_enabled: boolean
  signals: GridSignal[]
}

export interface GridSignalRequest {
  params?: Record<string, unknown>
  manual_account?: Record<string, unknown> | null
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
}

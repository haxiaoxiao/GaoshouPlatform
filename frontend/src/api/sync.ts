import request from './request'

export interface SyncStatus {
  task_id?: string
  run_id?: string
  sync_type: string | null
  status: 'idle' | 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'
  total: number
  current: number
  success_count: number
  failed_count: number
  progress_percent: number
  start_time: string | null
  end_time: string | null
  error_message: string | null
  details: Record<string, unknown>
}

export interface SyncLog {
  id: number
  task_id: number | null
  sync_type: string
  status: string
  total_count: number | null
  success_count: number | null
  failed_count: number | null
  start_time: string
  end_time: string | null
  error_message: string | null
  details: Record<string, unknown> | null
  created_at: string
}

export interface SyncRequest {
  sync_type:
    | 'datasync'
    | 'stock_info'
    | 'stock_full'
    | 'financial_data'
    | 'kline_daily'
    | 'index_daily'
    | 'kline_minute'
    | 'realtime_mv'
    | 'dividends'
    | 'factor_dependency'
    | 'tushare_relay'
    | 'ths_concept'
    | 'sentiment_xueqiu'
    | 'sentiment_nga'
  symbols?: string[]
  index_symbols?: string[]
  start_date?: string
  end_date?: string
  sync_mode?: 'incremental' | 'range' | 'full'
  failure_strategy?: 'skip' | 'retry' | 'stop'
  full_sync?: boolean
  factor_sync_plan?: Record<string, unknown>
  relay_datasets?: string[]
  relay_options?: Record<string, unknown>
}

export interface SyncCatalogItem {
  name: string
  display_name: string
  category: string
  source: string
  storage_dataset?: string
  date_col?: string
  recommended_frequency: string
  requires_qmt: boolean
  requires_relay_key: boolean
  risk_level: 'low' | 'medium' | 'high' | string
  description: string
  default_enabled?: boolean
  text_source?: boolean
  symbol_scoped?: boolean
  default_params?: Record<string, unknown>
  coverage?: {
    row_count: number | null
    min_date: string | null
    max_date: string | null
    estimated?: boolean
    partition_count?: number
    error?: string
  } | null
}

export interface SyncPreset {
  name: string
  display_name: string
  description: string
  sync_types: string[]
  relay_datasets: string[]
  include_by_default?: boolean
}

export interface SyncCatalog {
  presets: SyncPreset[]
  datasets: SyncCatalogItem[]
  relay: {
    configured: boolean
    rps: number
    timeout_seconds: number
    base_url_count: number
  }
  guardrails: Record<string, unknown>
}

export interface SyncLogsParams {
  sync_type?: string
  task_id?: number
  limit?: number
}

export const syncApi = {
  trigger: (params: SyncRequest) =>
    request.post<SyncStatus>('/data/sync', params),

  getStatus: () =>
    request.get<SyncStatus>('/data/sync/status'),

  cancel: () =>
    request.post<{ cancelled: boolean }>('/data/sync/cancel', {}),

  getCatalog: (params?: { refresh?: boolean }) =>
    request.get<SyncCatalog>('/data/sync/catalog', { params }),

  getLogs: (params?: SyncLogsParams) =>
    request.get<SyncLog[]>('/data/sync/logs', { params }),
}

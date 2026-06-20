import request from './request'

export interface SystemStatus {
  status: string
  database: string
  market_data_backend?: string
  parquet_data_dir?: string
  data_dir?: string
  dev_data_mode?: DevDataMode
  [key: string]: unknown
}

export interface DataSummaryItem {
  key: string
  label: string
  source: string
  latest_date: string | null
  latest_datetime?: string | null
  row_count: number | null
  row_count_estimated: boolean
  status: 'good' | 'stale' | 'missing' | 'error' | string
  status_text: string
  storage?: string
  dataset?: string | null
  date_column?: string | null
  min_date?: string | null
  error?: string | null
  notes?: string | null
}

export interface DataSummary {
  generated_at: string
  overall_status: 'good' | 'degraded' | 'error' | string
  market_data_backend: string
  parquet_data_dir: string
  data_dir?: string
  dev_data_mode?: DevDataMode
  items: DataSummaryItem[]
  by_key: Record<string, DataSummaryItem>
}

export interface DevDataMode {
  enabled: boolean
  environment: 'dev' | 'prod' | string
  use_prod_data: boolean
  active_data_dir: string
  active_database_url: string
  active_parquet_data_dir: string
  dev_local_data_dir: string
  dev_prod_data_dir: string
  warning: string | null
  updated_at?: string | null
}

export const systemApi = {
  getStatus: () => request.get<SystemStatus>('/system/status'),

  healthCheck: () => request.get<{ status: string }>('/system/health'),

  dataSummary: () => request.get<DataSummary>('/system/data-summary'),

  getDevDataMode: () => request.get<DevDataMode>('/system/dev-data-mode'),

  setDevDataMode: (payload: { use_prod_data: boolean; acknowledge_warning?: boolean }) =>
    request.put<DevDataMode>('/system/dev-data-mode', payload),
}

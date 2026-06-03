import request from './request'

export interface SystemStatus {
  status: string
  database: string
  market_data_backend?: string
  clickhouse_enabled?: boolean
  parquet_data_dir?: string
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
  items: DataSummaryItem[]
  by_key: Record<string, DataSummaryItem>
}

export const systemApi = {
  getStatus: () => request.get<SystemStatus>('/system/status'),

  healthCheck: () => request.get<{ status: string }>('/system/health'),

  dataSummary: () => request.get<DataSummary>('/system/data-summary'),
}

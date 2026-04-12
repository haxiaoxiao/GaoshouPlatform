import request from './request'

// 同步状态接口
export interface SyncStatus {
  sync_type: string | null
  status: 'idle' | 'running' | 'completed' | 'failed'
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

// 同步日志接口
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

// 同步请求参数
export interface SyncRequest {
  sync_type: 'stock_info' | 'kline_daily' | 'kline_minute'
  symbols?: string[]
  start_date?: string
  end_date?: string
  failure_strategy?: 'skip' | 'retry' | 'stop'
}

// 同步日志查询参数
export interface SyncLogsParams {
  sync_type?: string
  task_id?: number
  limit?: number
}

// API 响应包装 (拦截器已自动解包，此接口仅供类型参考)
// interface ApiResponse<T> {
//   code: number
//   message: string
//   data: T
// }

export const syncApi = {
  // 触发数据同步
  trigger: (params: SyncRequest) =>
    request.post<SyncStatus>('/data/sync', params),

  // 获取同步状态
  getStatus: () =>
    request.get<SyncStatus>('/data/sync/status'),

  // 获取同步日志
  getLogs: (params?: SyncLogsParams) =>
    request.get<SyncLog[]>('/data/sync/logs', { params }),
}

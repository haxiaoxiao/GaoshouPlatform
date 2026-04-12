import request from './request'

export interface SystemStatus {
  status: string
  database: string
}

export const systemApi = {
  getStatus: () => request.get<SystemStatus>('/system/status'),

  healthCheck: () => request.get<{ status: string }>('/system/health'),
}

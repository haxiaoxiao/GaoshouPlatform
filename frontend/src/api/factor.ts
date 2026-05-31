import request from './request'

// 因子类型定义
export interface Factor {
  id: number
  name: string
  category: string | null
  source: string | null
  code: string | null
  parameters: Record<string, unknown> | null
  description: string | null
  created_at: string | null
  updated_at: string | null
}

export interface FactorCreateRequest {
  name: string
  category?: string
  source?: string
  code?: string
  parameters?: Record<string, unknown>
  description?: string
}

export interface FactorUpdateRequest {
  name?: string
  category?: string
  source?: string
  code?: string
  parameters?: Record<string, unknown>
  description?: string
}

export const factorApi = {
  // 获取因子列表
  getList: (params?: { category?: string; source?: string }) =>
    request.get<Factor[]>('/factor/factors', { params }),

  // 创建因子
  create: (data: FactorCreateRequest) =>
    request.post<Factor>('/factor/factors', data),

  // 获取因子详情
  getDetail: (id: number) =>
    request.get<Factor>(`/factor/factors/${id}`),

  // 更新因子
  update: (id: number, data: FactorUpdateRequest) =>
    request.put<Factor>(`/factor/factors/${id}`, data),

  // 删除因子
  delete: (id: number) =>
    request.delete<{ deleted: boolean }>(`/factor/factors/${id}`),

}

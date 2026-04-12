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

export interface FactorAnalysis {
  id: number
  factor_id: number
  factor_name: string | null
  start_date: string
  end_date: string
  ic_mean: number | null
  ic_std: number | null
  ir: number | null
  details: FactorAnalysisDetails | null
  created_at: string
}

export interface FactorAnalysisDetails {
  ic_mean: number
  ic_std: number
  icir: number
  annual_icir: number
  annual_return: number
  annual_vol: number
  information_ratio: number
  win_rate: number
  max_drawdown: number
  total_stocks: number
  total_dates: number
  ic_series: Array<{ trade_date: string; ic: number }>
  group_returns: Array<{
    trade_date: string
    group_1: number
    group_2: number
    group_3: number
    group_4: number
    group_5: number
    long_short: number
  }>
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

export interface FactorAnalyzeRequest {
  start_date: string
  end_date: string
  symbols?: string[]
  normalize_window?: number
  factor_window?: number
  forward_period?: number
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

  // 运行因子分析
  analyze: (factorId: number, data: FactorAnalyzeRequest) =>
    request.post<FactorAnalysis>(`/factor/factors/${factorId}/analyze`, data),

  // 获取分析记录列表
  getAnalyses: (params?: { factor_id?: number; limit?: number }) =>
    request.get<FactorAnalysis[]>('/factor/analyses', { params }),

  // 获取分析详情
  getAnalysis: (analysisId: number) =>
    request.get<FactorAnalysis>(`/factor/analyses/${analysisId}`),
}

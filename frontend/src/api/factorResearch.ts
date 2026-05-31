import request from './request'
import type {
  FactorConfig, EvalConfig,
  FactorTemplate, FactorReport,
  BoardQuery, BoardResponse,
  ValidateRequest, ValidateResponse,
} from '@/types/factor'

export const factorApi = {
  getTemplates: () =>
    request.get<FactorTemplate[]>('/factors/templates'),

  validate: (data: ValidateRequest) =>
    request.post<ValidateResponse>('/factors/validate', data),

  validatePython: (data: { code: string }) =>
    request.post<{ valid: boolean; error: string | null }>('/factors/validate-python', data),

  create: (data: {
    name: string
    expression: string
    source_type?: 'dsl' | 'python' | 'builtin'
    engine?: 'builtin' | 'akquant' | 'python'
    stock_pool: string
    direction?: 'asc' | 'desc'
    default_stock_pool?: string
    default_benchmark?: string
    cache_enabled?: boolean
    default_eval_config?: Record<string, unknown>
    category?: string | null
    description?: string | null
    params?: Record<string, unknown>
  }) =>
    request.post('/factors/create', data),
}

export const computeApi = {
  evaluate: (config: FactorConfig) =>
    request.post<unknown>('/compute/evaluate', config),

  batch: (configs: FactorConfig[]) =>
    request.post<unknown>('/compute/batch', configs),

  operators: () =>
    request.get('/compute/operators'),
}

export const evaluationApi = {
  report: (config: FactorConfig, evalConfig?: EvalConfig) =>
    request.post<FactorReport>('/evaluation/report', { config, eval_config: evalConfig }),

  board: (query: BoardQuery) =>
    request.post<BoardResponse>('/evaluation/board', query),
}

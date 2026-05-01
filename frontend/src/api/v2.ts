import request from './request'
import type {
  FactorConfig, EvalConfig, BtConfig,
  FactorTemplate, FactorReport, BacktestReport,
  BoardQuery, BoardResponse,
  ValidateRequest, ValidateResponse,
} from '@/types/factor'

export const factorApi = {
  getTemplates: () =>
    request.get<FactorTemplate[]>('/v2/factors/templates'),

  validate: (data: ValidateRequest) =>
    request.post<ValidateResponse>('/v2/factors/validate', data),
}

export const computeApi = {
  evaluate: (config: FactorConfig) =>
    request.post<unknown>('/v2/compute/evaluate', config),

  batch: (configs: FactorConfig[]) =>
    request.post<unknown>('/v2/compute/batch', configs),
}

export const evaluationApi = {
  report: (config: FactorConfig, evalConfig?: EvalConfig) =>
    request.post<FactorReport>('/v2/evaluation/report', { config, eval_config: evalConfig }),

  board: (query: BoardQuery) =>
    request.post<BoardResponse>('/v2/evaluation/board', query),
}

export const backtestV2Api = {
  runFactor: (config: FactorConfig, btConfig?: BtConfig) =>
    request.post<BacktestReport>('/v2/backtest/factor', { config, bt_config: btConfig }),
}

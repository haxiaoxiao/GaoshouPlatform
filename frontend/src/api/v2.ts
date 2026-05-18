import request from './request'
import type { FactorConfig, BtConfig, BacktestReport } from '@/types/factor'

export const backtestV2Api = {
  runFactor: (config: FactorConfig, btConfig?: BtConfig) =>
    request.post<BacktestReport>('/v2/backtest/factor', { config, bt_config: btConfig }),
}

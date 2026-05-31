import request from './request'
import type { FactorConfig, BtConfig, BacktestReport } from '@/types/factor'

export const factorBacktestApi = {
  runFactor: (config: FactorConfig, btConfig?: BtConfig) =>
    request.post<BacktestReport>('/backtest/factor', { config, bt_config: btConfig }),
}

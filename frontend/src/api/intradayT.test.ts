import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./request', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
}))

import request from './request'
import {
  intradayTApi,
  type IntradayTBacktestResult,
  type IntradayTBacktestRequest,
  type IntradayTStrategyParams,
} from './intradayT'

describe('intraday T API client', () => {
  beforeEach(() => {
    vi.mocked(request.get).mockReset()
    vi.mocked(request.post).mockReset()
  })

  it('uses the dedicated capability and coverage endpoints', async () => {
    vi.mocked(request.get).mockResolvedValue({})

    await intradayTApi.capabilities()
    await intradayTApi.coverage(['603629.SH', '688008.SH'], '2026-01-01', '2026-07-14')

    expect(request.get).toHaveBeenNthCalledWith(1, '/intraday-t/capabilities')
    expect(request.get).toHaveBeenNthCalledWith(2, '/intraday-t/coverage', {
      params: {
        symbols: '603629.SH,688008.SH',
        start_date: '2026-01-01',
        end_date: '2026-07-14',
      },
    })
  })

  it('posts a typed minute backtest request without reshaping it', async () => {
    const strategy: IntradayTStrategyParams = {
      max_trade_fraction: 0.25,
      entry_z: 1.75,
      max_entry_z: 2.4,
      exit_z: 0.25,
      realized_vol_window: 10,
      min_realized_vol_bps: 20,
    }
    const payload: IntradayTBacktestRequest = {
      symbols: ['603629.SH', '688008.SH'],
      start_date: '2026-01-01',
      end_date: '2026-07-14',
      initial_capital: 1_000_000,
      base_quantities: { '603629.SH': 2000, '688008.SH': 1000 },
      cash_buffer_fraction: 0.3,
      max_bar_volume_fraction: 0.05,
      strategy,
    }
    vi.mocked(request.post).mockResolvedValue({})

    await intradayTApi.runBacktest(payload)

    expect(request.post).toHaveBeenCalledWith('/intraday-t/backtest', payload)
  })

  it('exposes only paper lifecycle methods', async () => {
    vi.mocked(request.get).mockResolvedValue([])
    vi.mocked(request.post).mockResolvedValue({})

    await intradayTApi.startPaper({ strategy: {} })
    await intradayTApi.paperStatus('it-1')
    await intradayTApi.evaluatePaper('it-1')
    await intradayTApi.startPaperRunner('it-1', 30)
    await intradayTApi.stopPaperRunner('it-1')
    await intradayTApi.paperTrades('it-1')
    await intradayTApi.stopPaper('it-1')
    await intradayTApi.resetPaper('it-1')

    expect(request.post).toHaveBeenNthCalledWith(1, '/intraday-t/paper/start', { strategy: {} })
    expect(request.get).toHaveBeenNthCalledWith(1, '/intraday-t/paper/status', {
      params: { session_id: 'it-1' },
      notifyError: false,
    })
    expect(request.post).toHaveBeenNthCalledWith(2, '/intraday-t/paper/it-1/evaluate')
    expect(request.post).toHaveBeenNthCalledWith(3, '/intraday-t/paper/it-1/runner/start', { interval_seconds: 30 })
    expect(request.post).toHaveBeenNthCalledWith(4, '/intraday-t/paper/it-1/runner/stop')
    expect(request.get).toHaveBeenNthCalledWith(2, '/intraday-t/paper/it-1/trades')
    expect(request.post).toHaveBeenNthCalledWith(5, '/intraday-t/paper/it-1/stop')
    expect(request.post).toHaveBeenNthCalledWith(6, '/intraday-t/paper/it-1/reset')
    expect(intradayTApi).not.toHaveProperty('submitOrder')
  })

  it('types backtest data quality, cross-symbol coverage, and open pairs', () => {
    const period: IntradayTBacktestResult['period'] = {
      start: '2026-07-14T09:31:00',
      end: '2026-07-14T15:00:00',
      trade_days: 1,
      common_trade_days: 1,
      symbol_trade_days: { '603629.SH': 1, '688008.SH': 1 },
      missing_observed_days: { '603629.SH': [], '688008.SH': [] },
      bars: 480,
    }
    const openPairs: IntradayTBacktestResult['metrics']['open_pairs_at_end'] = 0
    const dataQuality: IntradayTBacktestResult['data_quality'] = {
      limit_prices: {
        mode: 'fail_closed',
        expected_symbol_days: 2,
        available_symbol_days: 1,
        missing_symbol_days: ['688008.SH|2026-07-14'],
      },
    }

    expect(dataQuality.limit_prices.mode).toBe('fail_closed')
    expect(period.common_trade_days).toBe(1)
    expect(openPairs).toBe(0)
  })
})

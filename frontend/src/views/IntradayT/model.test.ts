import { describe, expect, it } from 'vitest'

import {
  buildEquitySeries,
  directionLabel,
  formatCurrency,
  formatPercent,
  marketDateString,
  reasonLabel,
  tradeSideLabel,
} from './model'

describe('intraday T view model', () => {
  it('formats signed currency and percent metrics consistently', () => {
    expect(formatCurrency(4911.7625, true)).toBe('+¥4,911.76')
    expect(formatCurrency(-120.5, true)).toBe('-¥120.50')
    expect(formatPercent(0.00491176, true)).toBe('+0.49%')
  })

  it('maps trading directions into concise Chinese labels', () => {
    expect(tradeSideLabel('BUY')).toBe('买入')
    expect(tradeSideLabel('SELL')).toBe('卖出')
    expect(directionLabel('POSITIVE')).toBe('正 T')
    expect(directionLabel('REVERSE')).toBe('反 T')
    expect(reasonLabel('risk_restore')).toBe('风险止损恢复')
  })

  it('formats dates in the China market timezone independent of the browser timezone', () => {
    const instant = new Date('2026-07-18T17:05:00Z')
    expect(marketDateString(instant)).toBe('2026-07-19')
    expect(marketDateString(instant, 'UTC')).toBe('2026-07-18')
  })

  it('converts daily equity points into chart-ready series', () => {
    expect(buildEquitySeries([
      { trade_date: '2026-07-01', equity: 1_001_000, passive_equity: 1_000_200, incremental_pnl: 800 },
      { trade_date: '2026-07-02', equity: 1_003_000, passive_equity: 1_001_000, incremental_pnl: 2000 },
    ])).toEqual({
      dates: ['07-01', '07-02'],
      strategy: [1_001_000, 1_003_000],
      passive: [1_000_200, 1_001_000],
      incremental: [800, 2000],
    })
  })
})

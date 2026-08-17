import { describe, expect, it } from 'vitest'

import { resolveGlobalSearchTarget } from './navigation'

describe('global navigation search', () => {
  it.each([
    ['600000', '/stock/600000.SH'],
    ['000001', '/stock/000001.SZ'],
    ['430047', '/stock/430047.BJ'],
    ['920001', '/stock/920001.BJ'],
    ['300750.sz', '/stock/300750.SZ'],
  ])('normalizes stock symbol %s', (query, target) => {
    expect(resolveGlobalSearchTarget(query)).toBe(target)
  })

  it('resolves exact and partial page names', () => {
    expect(resolveGlobalSearchTarget('系统运维')).toBe('/monitor')
    expect(resolveGlobalSearchTarget('backtest')).toBe('/backtest')
    expect(resolveGlobalSearchTarget('同步')).toBe('/data/sync')
  })

  it('does not navigate for empty or unknown input', () => {
    expect(resolveGlobalSearchTarget('  ')).toBeNull()
    expect(resolveGlobalSearchTarget('not-a-page')).toBeNull()
  })
})

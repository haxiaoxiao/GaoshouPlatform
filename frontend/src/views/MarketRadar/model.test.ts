import { describe, expect, it } from 'vitest'

import {
  BREADTH_BUCKETS,
  buildBreadthChart,
  buildIndexTrend,
  normalizeLimitLadder,
  normalizeSectors,
  mergeRealtimeBreadth,
  isUsableRadarComponent,
  resolveFreshness,
} from './model'

describe('market radar view model', () => {
  it('keeps the fixed ten-bin order and reports flat stocks inside the zero-to-two bin', () => {
    const data = {
      mode: 'percent',
      days: [{
        trade_date: '2026-07-18',
        breadth: {
          status: 'fresh',
          flat_count: 12,
          buckets: Object.fromEntries(BREADTH_BUCKETS.map((bucket, index) => [
            bucket.key,
            { label: bucket.label, value: index + 1 },
          ])),
        },
      }],
    }

    const result = buildBreadthChart(data)

    expect(result.dates).toEqual(['07-18'])
    expect(result.series.map(item => item.key)).toEqual(BREADTH_BUCKETS.map(item => item.key))
    expect(result.series.map(item => item.values[0])).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    expect(result.flatCounts).toEqual([12])
  })

  it('uses null for missing breadth bins instead of inventing zero', () => {
    const result = buildBreadthChart({
      mode: 'percent',
      days: [{ trade_date: '2026-07-18', breadth: { buckets: {} } }],
    })

    expect(result.series.every(item => item.values[0] === null)).toBe(true)
  })

  it('preserves gaps in the all-A median and core-index trend', () => {
    const result = buildIndexTrend({
      days: [
        {
          trade_date: '2026-07-17',
          breakdowns: [{ key: 'all', median_return: -1.2 }],
          indices: {
            '000001.SH': { return_pct: -0.8 },
            '399001.SZ': { return_pct: null, status: 'unavailable' },
          },
        },
        {
          trade_date: '2026-07-18',
          breakdowns: [{ key: 'all', median_return: null }],
          indices: {
            '000001.SH': { return_pct: 0.3 },
            '399001.SZ': { return_pct: -0.5 },
            '000985.SH': { return_pct: -0.2 },
          },
        },
      ],
    })

    expect(result.series.find(item => item.key === 'all')?.values).toEqual([-1.2, null])
    expect(result.series.find(item => item.key === '000001.SH')?.values).toEqual([-0.8, 0.3])
    expect(result.series.find(item => item.key === '399001.SZ')?.values).toEqual([null, -0.5])
    expect(result.series.find(item => item.key === '000985.SH')?.values).toEqual([null, -0.2])
  })

  it('normalizes ladder rows without converting unavailable ratios to zero', () => {
    const result = normalizeLimitLadder({
      status: 'stale',
      trade_date: '2026-07-17',
      source_mode: 'official',
      highest_board: 3,
      promotion_rate: null,
      rows: [{
        symbol: '600001.SH',
        name: '示例股份',
        industry: '电子',
        board_count: 3,
        turnover_ratio: 0.18,
        limit_times: 4,
        seal_amount: 320_000_000,
        first_time: '093100',
        last_time: '145702',
      }],
    })

    expect(result.status).toBe('stale')
    expect(result.tradeDate).toBe('2026-07-17')
    expect(result.promotionRate).toBeNull()
    expect(result.rows[0]).toMatchObject({
      symbol: '600001.SH',
      boardCount: 3,
      turnoverRatio: 0.18,
      limitTimes: 4,
      sealAmount: 320_000_000,
      firstTime: '09:31:00',
      lastTime: '14:57:02',
    })
  })

  it('marks independent sector crowding as unavailable', () => {
    const rows = normalizeSectors({
      status: 'fresh',
      sectors: [{
        industry: '电子',
        median_return: -2.1,
        advance_ratio: 0.18,
        amount_share: 0.12,
        share_z20: 2.8,
        stock_count: 143,
      }],
    })

    expect(rows[0]).toMatchObject({
      industry: '电子',
      medianReturn: -2.1,
      crowdingScore: null,
      emotionScore: null,
    })
  })

  it('keeps freshness labels and source dates explicit', () => {
    expect(resolveFreshness('partial', '2026-07-18')).toEqual({
      status: 'partial',
      label: '部分可用',
      tone: 'warn',
      asOf: '2026-07-18',
    })
    expect(resolveFreshness('unexpected', null)).toEqual({
      status: 'unavailable',
      label: '不可用',
      tone: 'muted',
      asOf: null,
    })
  })

  it('merges the actual intraday compact payload into the chart endpoint history', () => {
    const history = {
      mode: 'percent',
      days: [{ trade_date: '2026-07-17', breadth: { buckets: {} }, breakdowns: [], indices: {} }],
    }
    const metrics = {
      overview: {
        mode: 'push',
        market_median_return_pct: -1.6,
        decline_ratio: 0.72,
        status: 'fresh',
      },
      breadth: {
        status: 'fresh',
        flat_count: 8,
        buckets: { le_neg_8: { percentage: 2.1 } },
      },
      indices: { '000001.SH': { return_pct: -0.8, status: 'fresh' } },
    }

    const result = mergeRealtimeBreadth(history, metrics, '2026-07-18T10:02:00')
    const latest = result.days.at(-1) as Record<string, unknown>

    expect(result.days).toHaveLength(2)
    expect(latest.trade_date).toBe('2026-07-18')
    expect(latest.breadth).toMatchObject({ flat_count: 8 })
    expect(latest.breakdowns).toEqual([{ key: 'all', median_return: -1.6 }])
    expect(latest.indices).toMatchObject({ '000001.SH': { return_pct: -0.8 } })
  })

  it('does not let intraday unavailable placeholders replace daily components', () => {
    expect(isUsableRadarComponent({ status: 'unavailable', reason: 'intraday source not loaded' })).toBe(false)
    expect(isUsableRadarComponent({ status: 'fresh', score: { value: 82 } })).toBe(true)
  })

  it('keeps only the latest 15 days when a full EOD snapshot replaces chart history', () => {
    const days = Array.from({ length: 20 }, (_, index) => ({
      trade_date: `2026-06-${String(index + 1).padStart(2, '0')}`,
      breadth: { status: 'fresh', buckets: {} },
    }))

    const result = mergeRealtimeBreadth(
      { mode: 'percent', days: [] },
      { breadth: { status: 'fresh', days } },
      '2026-06-20T15:20:00',
    )

    expect(result.days).toHaveLength(15)
    expect((result.days[0] as Record<string, unknown>).trade_date).toBe('2026-06-06')
  })
})

import { describe, expect, it } from 'vitest'

import { hasOlderKlinePage, mergeKlinePages } from './klinePagination'

const row = (datetime: string, close: number) => ({
  datetime,
  open: close,
  high: close,
  low: close,
  close,
  volume: 1,
  amount: 1,
})

describe('data workbench K-line pagination', () => {
  it('merges overlapping pages newest first without duplicates', () => {
    expect(mergeKlinePages(
      [row('2026-07-17', 3), row('2026-07-16', 2)],
      [row('2026-07-16', 20), row('2026-07-15', 1)],
    )).toEqual([
      row('2026-07-17', 3),
      row('2026-07-16', 2),
      row('2026-07-15', 1),
    ])
  })

  it('loads an older page only when pagination is incomplete and idle', () => {
    expect(hasOlderKlinePage({ page: 1, totalPages: 2, loading: false })).toBe(true)
    expect(hasOlderKlinePage({ page: 2, totalPages: 2, loading: false })).toBe(false)
    expect(hasOlderKlinePage({ page: 1, totalPages: 2, loading: true })).toBe(false)
  })
})

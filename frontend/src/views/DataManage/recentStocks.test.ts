import { describe, expect, it } from 'vitest'

import {
  MAX_RECENT_STOCKS,
  RECENT_STOCKS_STORAGE_KEY,
  loadRecentStocks,
  rememberRecentStock,
  type RecentStock,
} from './recentStocks'

class MemoryStorage {
  private readonly values = new Map<string, string>()

  getItem(key: string) {
    return this.values.get(key) ?? null
  }

  setItem(key: string, value: string) {
    this.values.set(key, value)
  }
}

describe('data workbench recent stocks', () => {
  it('starts empty and ignores corrupt or invalid persisted values', () => {
    const empty = new MemoryStorage()
    expect(loadRecentStocks(empty)).toEqual([])

    const corrupt = new MemoryStorage()
    corrupt.setItem(RECENT_STOCKS_STORAGE_KEY, '{broken')
    expect(loadRecentStocks(corrupt)).toEqual([])

    corrupt.setItem(RECENT_STOCKS_STORAGE_KEY, JSON.stringify([
      { symbol: '', name: 'invalid' },
      { symbol: '600519.SH', name: '' },
      { symbol: '600519.SH', name: '贵州茅台', industry: '食品饮料' },
    ]))
    expect(loadRecentStocks(corrupt)).toEqual([
      { symbol: '600519.SH', name: '贵州茅台', industry: '食品饮料' },
    ])
  })

  it('moves an explicitly viewed stock to the front and keeps only ten', () => {
    const storage = new MemoryStorage()
    let recent: RecentStock[] = []

    for (let index = 0; index < 11; index += 1) {
      recent = rememberRecentStock({
        symbol: `6005${String(index).padStart(2, '0')}.SH`,
        name: `股票${index}`,
        industry: `行业${index}`,
      }, recent, storage)
    }

    expect(recent).toHaveLength(MAX_RECENT_STOCKS)
    expect(recent[0]?.symbol).toBe('600510.SH')
    expect(recent.at(-1)?.symbol).toBe('600501.SH')

    recent = rememberRecentStock({
      symbol: '600505.SH',
      name: '更新后的股票5',
      theme: '真实查看',
    }, recent, storage)

    expect(recent).toHaveLength(MAX_RECENT_STOCKS)
    expect(recent[0]).toEqual({
      symbol: '600505.SH',
      name: '更新后的股票5',
      theme: '真实查看',
    })
    expect(loadRecentStocks(storage)).toEqual(recent)
  })
})

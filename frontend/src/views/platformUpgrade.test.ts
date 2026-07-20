import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

const root = fileURLToPath(new URL('..', import.meta.url))
const read = (path: string) => readFileSync(`${root}/${path}`, 'utf-8')

describe('platform upgrade UI contracts', () => {
  it('shows provenance on factor, backtest, and live workspaces', () => {
    for (const path of [
      'views/FactorResearch/index.vue',
      'views/StrategyBacktest/index.vue',
      'views/LiveTrading/index.vue',
    ]) {
      expect(read(path)).toContain('<ProvenanceStrip')
    }
  })

  it('keeps the six-node topology reachable and removes black loading masks', () => {
    expect(read('views/SystemMonitor/index.vue')).toContain('overflow-x: auto')
    expect(read('styles/design-system.css')).not.toContain('background: rgba(10, 10, 12, 0.8)')
  })

  it('uses the daily preset display count for composite sync tasks', () => {
    const panel = read('views/DataManage/SyncPanel.vue')
    expect(panel).toContain('preset.display_item_count ?? syncCount + relayCount')
  })

  it('defaults routine synchronization to the full market', () => {
    const panel = read('views/DataManage/SyncPanel.vue')
    expect(panel).toContain("const stockScope = ref<'custom' | 'all'>('all')")
    expect(panel).toContain("symbols: stockScope.value === 'custom' ? parseSymbols(symbolText.value) : undefined")
  })

  it('persists genuine recent stocks without seeded symbols', () => {
    const page = read('views/DataManage/index.vue')
    expect(page).toContain('loadRecentStocks()')
    expect(page).toContain('rememberRecentStock(')
    expect(page).not.toContain("{ symbol: '000001.SZ', name: '平安银行'")
    expect(page).not.toContain("{ symbol: '300750.SZ', name: '宁德时代'")
  })

  it('loads older K-line pages when the chart reaches its left boundary', () => {
    const page = read('views/DataManage/index.vue')
    const chart = read('views/DataManage/KlineChart.vue')
    expect(page).toContain('@request-older="loadOlderKlines"')
    expect(page).toContain('page_size: klinePageSize.value')
    expect(page).toContain("ElMessage.warning('更早行情加载失败，可继续拖动重试')")
    expect(chart).toContain("const emit = defineEmits<{ 'request-older': [] }>()")
    expect(chart).toContain('subscribeVisibleLogicalRangeChange')
    expect(chart).toContain('requestOlderArmed = true')
  })
})

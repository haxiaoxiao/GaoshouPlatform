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
})

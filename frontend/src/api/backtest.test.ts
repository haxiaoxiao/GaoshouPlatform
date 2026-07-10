import { describe, expect, it, vi } from 'vitest'

import * as backtestModule from './backtest'

describe('strategy detail loading', () => {
  it('loads the full strategy by summary id before editing', async () => {
    const loadStrategyDetail = (
      backtestModule as typeof backtestModule & {
        loadStrategyDetail?: (
          summary: { id: number },
          loader: (id: number) => Promise<backtestModule.Strategy>,
        ) => Promise<backtestModule.Strategy>
      }
    ).loadStrategyDetail

    expect(loadStrategyDetail).toBeTypeOf('function')

    const detail: backtestModule.Strategy = {
      id: 43,
      name: 'TSMF',
      code: 'def init(context): pass',
      parameters: null,
      description: null,
      created_at: null,
      updated_at: null,
    }
    const loader = vi.fn().mockResolvedValue(detail)

    await expect(loadStrategyDetail!({ id: 43 }, loader)).resolves.toEqual(detail)
    expect(loader).toHaveBeenCalledOnce()
    expect(loader).toHaveBeenCalledWith(43)
  })
})

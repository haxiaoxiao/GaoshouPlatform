import { describe, expect, it } from 'vitest'

import * as liveTradingModule from './liveTrading'

describe('mobile live trading guard', () => {
  it('forces phone-sized viewports into read-only mode', () => {
    const isMobileTradingReadOnly = (
      liveTradingModule as typeof liveTradingModule & {
        isMobileTradingReadOnly?: (width: number) => boolean
      }
    ).isMobileTradingReadOnly

    expect(isMobileTradingReadOnly).toBeTypeOf('function')
    expect(isMobileTradingReadOnly!(390)).toBe(true)
    expect(isMobileTradingReadOnly!(1024)).toBe(false)
  })

  it('builds a live request with release, idempotency, account, and control token', () => {
    const buildV1LiveOrderRequest = (
      liveTradingModule as typeof liveTradingModule & {
        buildV1LiveOrderRequest?: (...args: unknown[]) => unknown
      }
    ).buildV1LiveOrderRequest

    expect(buildV1LiveOrderRequest).toBeTypeOf('function')
    expect(buildV1LiveOrderRequest!(
      'live',
      [{ strategy_id: 43, symbol: '600519.SH' }],
      {
        releaseId: 'release-1',
        expectedAccountMask: '66***80',
        controlToken: 'control-token',
        idempotencyKey: 'command-1',
      },
    )).toEqual({
      payload: {
        mode: 'live',
        orders: [{ strategy_id: 43, symbol: '600519.SH' }],
        confirm: true,
        release_id: 'release-1',
        expected_account_mask: '66***80',
        idempotency_key: 'command-1',
      },
      headers: { 'X-Gaoshou-Control-Token': 'control-token' },
    })
  })

  it('expires a control session and rejects an account-mask change', () => {
    const isLiveControlSessionActive = (
      liveTradingModule as typeof liveTradingModule & {
        isLiveControlSessionActive?: (...args: unknown[]) => boolean
      }
    ).isLiveControlSessionActive
    const session = { token: 'token', accountMask: '66***80', expiresAt: 1_000 }

    expect(isLiveControlSessionActive).toBeTypeOf('function')
    expect(isLiveControlSessionActive!(session, '66***80', 999)).toBe(true)
    expect(isLiveControlSessionActive!(session, '66***80', 1_001)).toBe(false)
    expect(isLiveControlSessionActive!(session, '77***99', 999)).toBe(false)
  })
})

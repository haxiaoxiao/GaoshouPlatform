import { describe, expect, it, vi } from 'vitest'

import {
  PollingAbortedError,
  PollingTimeoutError,
  pollUntil,
} from './polling'

describe('pollUntil', () => {
  it('returns the first terminal value and reports intermediate values', async () => {
    const values = [1, 2, 3]
    const seen: number[] = []
    const wait = vi.fn(async () => undefined)

    const result = await pollUntil({
      request: async () => values.shift() || 3,
      isTerminal: value => value === 3,
      onValue: value => seen.push(value),
      intervalMs: 100,
      timeoutMs: 1000,
      timeoutMessage: 'timed out',
      wait,
    })

    expect(result).toBe(3)
    expect(seen).toEqual([1, 2, 3])
    expect(wait).toHaveBeenCalledTimes(2)
  })

  it('aborts while waiting without issuing another request', async () => {
    const controller = new AbortController()
    const request = vi.fn(async () => ({ done: false }))
    const pending = pollUntil({
      request,
      isTerminal: value => value.done,
      intervalMs: 10_000,
      timeoutMs: 20_000,
      timeoutMessage: 'timed out',
      signal: controller.signal,
    })

    await Promise.resolve()
    controller.abort()

    await expect(pending).rejects.toBeInstanceOf(PollingAbortedError)
    expect(request).toHaveBeenCalledTimes(1)
  })

  it('uses one total deadline across all attempts', async () => {
    let now = 0

    const pending = pollUntil({
      request: async () => ({ done: false }),
      isTerminal: value => value.done,
      intervalMs: 60,
      timeoutMs: 100,
      timeoutMessage: 'overall timeout',
      now: () => now,
      wait: async milliseconds => {
        now += milliseconds
      },
    })

    await expect(pending).rejects.toEqual(new PollingTimeoutError('overall timeout'))
  })
})

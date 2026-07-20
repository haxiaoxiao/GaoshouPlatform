import { describe, expect, it } from 'vitest'

import { createScopedRequestGate } from './accountRequestGate'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(next => {
    resolve = next
  })
  return { promise, resolve }
}

describe('live trading account request gate', () => {
  it('ignores an old live response after switching to paper mode', async () => {
    const gate = createScopedRequestGate()
    const live = deferred<string>()
    const paper = deferred<string>()
    const applied: string[] = []

    async function load(scope: string, request: Promise<string>) {
      const token = gate.begin(scope)
      const value = await request
      return gate.commit(token, () => applied.push(value))
    }

    const liveLoad = load('live:profile-1', live.promise)
    const paperLoad = load('paper:profile-1', paper.promise)
    paper.resolve('paper account')
    expect(await paperLoad).toBe(true)
    live.resolve('live account')
    expect(await liveLoad).toBe(false)
    expect(applied).toEqual(['paper account'])
  })

  it('invalidates stream tokens when the mode or profile changes', () => {
    const gate = createScopedRequestGate('live:profile-1')
    const streamToken = gate.captureScope()

    gate.setScope('paper:profile-1')

    expect(gate.isScopeCurrent(streamToken)).toBe(false)
  })
})

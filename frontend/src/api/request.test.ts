import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  handleResponse,
  handleResponseError,
} from './request'
import { setRequestErrorNotifier } from './requestNotifications'

describe('API response envelope handling', () => {
  afterEach(() => setRequestErrorNotifier())

  it('rejects a non-zero envelope even when data is absent', () => {
    const notifier = vi.fn()
    setRequestErrorNotifier(notifier)

    expect(() => handleResponse({
      data: { code: 1, message: '参数校验失败' },
      config: {},
    })).toThrow('参数校验失败')
    expect(notifier).toHaveBeenCalledOnce()
    expect(notifier).toHaveBeenCalledWith('参数校验失败')
  })

  it('unwraps successful envelopes, including an explicit missing data field', () => {
    expect(handleResponse({ data: { code: 0, data: { ready: true } }, config: {} }))
      .toEqual({ ready: true })
    expect(handleResponse({ data: { code: 0, message: 'ok' }, config: {} }))
      .toBeUndefined()
  })

  it('returns non-envelope payloads unchanged', () => {
    const payload = { status: 'ready' }

    expect(handleResponse({ data: payload, config: {} })).toBe(payload)
  })

  it('uses an envelope message for non-2xx Axios responses', async () => {
    const notifier = vi.fn()
    setRequestErrorNotifier(notifier)
    const error = Object.assign(new Error('Request failed with status code 400'), {
      config: {},
      response: {
        data: { code: 4001, message: '控制令牌已过期' },
      },
    })

    await expect(handleResponseError(error)).rejects.toThrow('控制令牌已过期')
    expect(notifier).toHaveBeenCalledWith('控制令牌已过期')
  })

  it('suppresses global notifications only when notifyError is false', async () => {
    const notifier = vi.fn()
    setRequestErrorNotifier(notifier)

    expect(() => handleResponse({
      data: { code: 7, message: '后台业务失败' },
      config: { notifyError: false },
    })).toThrow('后台业务失败')

    const error = Object.assign(new Error('网络断开'), {
      config: { notifyError: false },
    })
    await expect(handleResponseError(error)).rejects.toBe(error)
    expect(notifier).not.toHaveBeenCalled()
  })
})

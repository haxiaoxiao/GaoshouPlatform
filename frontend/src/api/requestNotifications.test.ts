import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  notifyRequestError,
  setRequestErrorNotifier,
} from './requestNotifications'

describe('request error notifications', () => {
  afterEach(() => setRequestErrorNotifier())

  it('forwards API errors to the notifier installed by the application shell', () => {
    const notifier = vi.fn()
    setRequestErrorNotifier(notifier)

    notifyRequestError('数据加载失败')

    expect(notifier).toHaveBeenCalledOnce()
    expect(notifier).toHaveBeenCalledWith('数据加载失败')
  })
})

import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./request', () => ({
  default: {
    get: vi.fn(),
  },
}))

import request from './request'
import { runtimeTaskApi } from './runtimeTasks'

describe('runtime task polling requests', () => {
  beforeEach(() => vi.mocked(request.get).mockReset())

  it('does not send duplicate global toasts for background polling failures', () => {
    runtimeTaskApi.list(true)
    runtimeTaskApi.get('task-1')

    expect(request.get).toHaveBeenCalledWith('/system/tasks', {
      params: { include_finished: true },
      notifyError: false,
    })
    expect(request.get).toHaveBeenCalledWith('/system/tasks/task-1', {
      notifyError: false,
    })
  })
})

import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./request', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
}))

import request from './request'
import { sentimentApi } from './sentiment'
import { syncApi } from './sync'

describe('background polling requests', () => {
  beforeEach(() => vi.mocked(request.get).mockReset())

  it('keeps expected transient polling failures out of global notifications', () => {
    syncApi.getStatus()
    sentimentApi.ingestRun('run-1')

    expect(request.get).toHaveBeenCalledWith('/data/sync/status', {
      notifyError: false,
    })
    expect(request.get).toHaveBeenCalledWith('/sentiment/ingest/runs/run-1', {
      notifyError: false,
    })
  })
})

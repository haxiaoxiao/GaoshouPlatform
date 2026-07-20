import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('./request', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
}))

import * as explorerModule from './explorer'
import request from './request'

beforeEach(() => {
  vi.mocked(request.get).mockReset()
  vi.mocked(request.post).mockReset()
})

describe('data explorer row queries', () => {
  it('does not expose the legacy preview query wrapper', () => {
    expect(explorerModule).not.toHaveProperty('previewTable')
  })

  it('sends filters through the structured search endpoint', async () => {
    const query: explorerModule.ExplorerSearchRequest = {
      page: 2,
      page_size: 50,
      filters: [{ column: 'symbol', op: '=', value: '600519.SH' }],
    }
    vi.mocked(request.post).mockResolvedValue({ rows: [] })

    await explorerModule.searchTable('klines_daily', query)

    expect(request.post).toHaveBeenCalledOnce()
    expect(request.post).toHaveBeenCalledWith(
      '/explorer/tables/klines_daily/search',
      query,
    )
  })
})

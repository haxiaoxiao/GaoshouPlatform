/** @vitest-environment jsdom */

import { describe, expect, it } from 'vitest'

import router from './index'

describe('router fallback', () => {
  it('redirects unknown paths to a valid application screen', () => {
    const fallback = router.getRoutes().find(route => route.path === '/:pathMatch(.*)*')

    expect(fallback?.redirect).toBe('/home')
  })
})

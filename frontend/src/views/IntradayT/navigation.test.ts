/** @vitest-environment jsdom */

import { describe, expect, it } from 'vitest'

import { resolveNavItem } from '@/app/navigation'
import router from '@/router'

describe('intraday T navigation', () => {
  it('registers the dedicated route and resolves the most specific navigation item', () => {
    const route = router.getRoutes().find(item => item.path === '/trade/intraday-t')

    expect(route?.name).toBe('IntradayT')
    expect(resolveNavItem('/trade/intraday-t')?.key).toBe('intraday-t')
  })
})

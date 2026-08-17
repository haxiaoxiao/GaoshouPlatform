/** @vitest-environment jsdom */

import { describe, expect, it } from 'vitest'

import { APP_NAV_ITEMS, resolveGlobalSearchTarget } from '@/app/navigation'
import router from '@/router'

describe('market radar navigation', () => {
  it('places the radar between home and data without removing intraday T', () => {
    const paths = APP_NAV_ITEMS.map(item => item.path)

    expect(paths.slice(0, 3)).toEqual(['/home', '/market-radar', '/data'])
    expect(paths).toContain('/trade/intraday-t')
    expect(resolveGlobalSearchTarget('市场雷达')).toBe('/market-radar')
  })

  it('registers the lazy market radar route', () => {
    const route = router.getRoutes().find(item => item.path === '/market-radar')

    expect(route?.name).toBe('MarketRadar')
    expect(route?.meta.title).toBe('市场雷达')
  })
})

import { expect, test } from '@playwright/test'

test.beforeEach(async ({ page }) => {
  page.on('console', message => {
    if (message.type() === 'error') console.error(`[browser console] ${message.text()}`)
  })
  page.on('pageerror', error => console.error(`[browser pageerror] ${error.stack ?? error.message}`))
})

async function expectNoHorizontalOverflow(page: import('@playwright/test').Page) {
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
  }))
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth + 1)
}

test.describe('desktop platform shell', () => {
  test.skip(({ isMobile }) => isMobile)

  test('shows readiness context and keeps every topology node reachable', async ({ page }) => {
    await page.goto('/monitor')
    await expect(page.locator('.status-bar')).toContainText('真实下单关闭')
    await expect(page.locator('.flow-station')).toHaveCount(6)

    const shell = page.locator('.flow-shell')
    const lastStation = page.locator('.flow-station').last()
    await lastStation.scrollIntoViewIfNeeded()
    const [shellBox, stationBox] = await Promise.all([shell.boundingBox(), lastStation.boundingBox()])
    expect(shellBox).not.toBeNull()
    expect(stationBox).not.toBeNull()
    expect(stationBox!.x + stationBox!.width).toBeLessThanOrEqual(shellBox!.x + shellBox!.width + 1)
    await expectNoHorizontalOverflow(page)
  })
})

test.describe('mobile trading shell', () => {
  test.skip(({ isMobile }) => !isMobile)

  test('uses a drawer and enforces read-only trading controls', async ({ page }) => {
    await page.goto('/trade')
    await expect(page.locator('.mobile-menu-button')).toBeVisible()
    await expect(page.locator('.mobile-readonly-banner')).toContainText('移动端只读')
    await expect(page.locator('.action-button--submit')).toBeDisabled()
    await expect(page.locator('.sidebar')).not.toBeInViewport()

    await page.locator('.mobile-menu-button').click()
    await expect(page.locator('.sidebar')).toBeInViewport()
    await page.locator('.mobile-nav-backdrop').click({ position: { x: 380, y: 820 } })
    await expect(page.locator('.sidebar')).not.toBeInViewport()
    await expectNoHorizontalOverflow(page)
  })
})

import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  outputDir: './test-results',
  timeout: 45_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://127.0.0.1:3512',
    channel: 'chrome',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'desktop-chrome',
      use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 1000 } },
    },
    {
      name: 'mobile-chrome',
      use: { ...devices['Desktop Chrome'], viewport: { width: 390, height: 844 }, isMobile: true },
    },
  ],
  webServer: [
    {
      command: 'powershell -NoProfile -ExecutionPolicy Bypass -File ./e2e/start-backend.ps1',
      url: 'http://127.0.0.1:18801/health',
      timeout: 120_000,
      reuseExistingServer: false,
    },
    {
      command: 'powershell -NoProfile -ExecutionPolicy Bypass -File ./e2e/start-frontend.ps1',
      url: 'http://127.0.0.1:3512',
      timeout: 120_000,
      reuseExistingServer: false,
    },
  ],
})

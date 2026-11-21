import { defineConfig, devices } from '@playwright/test'

// The companion Python fixture owns its gateway, database and provider. Do not
// run the shared auth setup or load sessions from another gateway for this test.
export default defineConfig({
  testDir: './features/adaptive-routing',
  testMatch: 'adaptive-routing-runtime.spec.ts',
  forbidOnly: !!process.env.CI,
  workers: 1,
  retries: 0,
  reporter: 'list',
  use: {
    ...devices['Desktop Chrome'],
    storageState: { cookies: [], origins: [] },
    actionTimeout: 10000,
    navigationTimeout: 30000,
    screenshot: 'only-on-failure',
  },
  timeout: 180000,
  expect: { timeout: 10000 },
})

import { expect, test } from '../../core/fixtures/base.fixture'
import type { AdaptiveRoutingConfig } from '../../../../ui/lib/types/adaptiveRouting'

test.use({ skipAutoLogin: true })

const initialConfig: AdaptiveRoutingConfig = {
  direction_selection_enabled: true,
  route_selection_enabled: true,
  append_fallbacks_to_pinned: false,
  reroute_failed_directions: false,
  reroute_target: 'pool',
  prune_failed_fallbacks: false,
  history_enabled: true,
  history_retention_days: 7,
  snapshot_persist_interval_seconds: 300,
}

test.beforeEach(async ({ page }) => {
  // These checks exercise the UI contract without changing a shared gateway.
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/config') return route.fulfill({ json: {
      disable_onboarding: true, is_db_connected: true, is_logs_connected: true,
      client_config: {}, providers: {}, plugins: [], metadata: {},
      auth_config: { is_enabled: true },
    } })
    if (path === '/api/session/is-auth-enabled') return route.fulfill({ json: {
      is_auth_enabled: true, has_valid_token: true, auth_type: 'password',
    } })
    if (path === '/api/version') return route.fulfill({ json: 'v2.5.1' })
    if (path === '/api/providers') return route.fulfill({ json: { providers: [] } })
    if (path === '/api/plugins') return route.fulfill({ json: { plugins: [] } })
    if (path === '/api/adaptive-routing/config') return route.fulfill({ json: initialConfig })
    if (path === '/api/adaptive-routing/status') return route.fulfill({ json: { enabled: false, routes: [] } })
    if (path === '/api/adaptive-routing/history') return route.fulfill({ json: { events: [] } })
    return route.fulfill({ json: {} })
  })
})

test('initial settings save the full profile and a late refresh preserves the next edit', async ({ page, dashboardPage }) => {
  const writes: AdaptiveRoutingConfig[] = []
  let releaseRefresh!: () => void
  const refreshReady = new Promise<void>((resolve) => { releaseRefresh = resolve })
  await page.route('**/api/adaptive-routing/config', async (route) => {
    if (route.request().method() === 'PUT') {
      const saved = route.request().postDataJSON() as AdaptiveRoutingConfig
      writes.push(saved)
      return route.fulfill({ json: saved })
    }
    if (writes.length) await refreshReady
    return route.fulfill({ json: writes[0] ?? initialConfig })
  })
  await page.goto('/workspace/adaptive-routing/settings')
  await expect(page.getByTestId('adaptive-routing-settings')).toBeVisible()
  await page.getByTestId('adaptive-routing-save').click()
  await dashboardPage.waitForSuccessToast('Routing settings saved and applied.')
  expect(writes).toEqual([initialConfig])

  const retention = page.getByTestId('adaptive-routing-history_retention_days')
  await retention.fill('14')
  const refreshed = page.waitForResponse((response) => response.url().endsWith('/api/adaptive-routing/config') && response.request().method() === 'GET')
  releaseRefresh()
  await refreshed
  await expect(retention).toHaveValue('14')
  await expect(page.getByTestId('adaptive-routing-save')).toBeEnabled()
  await page.getByTestId('adaptive-routing-save').click()
  await expect.poll(() => writes.length).toBe(2)
  expect(writes[1]).toEqual({ ...initialConfig, history_retention_days: 14 })
})

test('release respects whole-key and model-scoped holds', async ({ page, dashboardPage }) => {
  const writes: unknown[] = []
  const common = { state: 'held', score: 0, samples: 1, error_rate: 1, latency_ms: 40, inflight: 0, last_seen: new Date().toISOString() }
  await page.route('**/api/adaptive-routing/status', (route) => route.fulfill({ json: { enabled: true, routes: [
    { ...common, provider: 'openai', key_id: 'account-key', key_name: 'Account key', model: 'a', failure_class: 'credential' },
    { ...common, provider: 'openai', key_id: 'limited-key', key_name: 'Model key', model: 'b', failure_class: 'model_access' },
  ] } }))
  await page.route('**/api/adaptive-routing/conditions/clear', (route) => {
    writes.push(route.request().postDataJSON())
    return route.fulfill({ json: {} })
  })
  await page.goto('/workspace/adaptive-routing')
  await page.getByRole('row').filter({ hasText: 'Account key' }).getByTestId('adaptive-routing-release').click()
  await dashboardPage.waitForSuccessToast('Hold released. The next request can use this key.')
  await page.getByRole('row').filter({ hasText: 'Model key' }).getByTestId('adaptive-routing-release').click()
  await expect.poll(() => writes.length).toBe(2)
  expect(writes).toEqual([
    { provider: 'openai', key_id: 'account-key' },
    { provider: 'openai', key_id: 'limited-key', model: 'b' },
  ])
})

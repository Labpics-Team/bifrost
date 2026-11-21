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
    if (path === '/api/adaptive-routing/incidents') return route.fulfill({ json: { incidents: [], total: 0 } })
    return route.fulfill({ json: {} })
  })
})

test('incident pagination keeps the server cursor on refresh and resets only for changed filters', async ({ page }) => {
  const common = {
    scope: 'key_model', provider: 'openai', model: 'test-model', key_id: 'test-key', key_name: 'Test key',
    state: 'held', worst_state: 'held', status: 'mitigated', opened_at: '2026-11-21T10:00:00Z',
    updated_at: '2026-11-21T10:00:01Z', last_seen: '2026-11-21T10:00:01Z', summary: 'Rate limited', action: 'Held the key',
    checks: 1, failed_checks: 0, refused_again: 0,
  }
  const calls: URL[] = []
  await page.route('**/api/adaptive-routing/incidents?*', route => {
    const url = new URL(route.request().url())
    calls.push(url)
    return route.fulfill({ json: url.searchParams.get('cursor') === 'page-two'
      ? { incidents: [{ ...common, id: 'older', key_name: 'Older key' }], total: 2 }
      : { incidents: [{ ...common, id: 'newer', key_name: 'Newer key' }], total: 2, next_cursor: 'page-two' } })
  })
  await page.goto('/workspace/adaptive-routing/incidents')
  await expect(page.getByTestId('adaptive-incidents-table')).toContainText('Newer key')
  await page.getByRole('button', { name: 'Next page', exact: true }).click()
  await expect(page.getByTestId('adaptive-incidents-table')).toContainText('Older key')
  await expect(page.getByTestId('adaptive-incidents-page')).toHaveText('Page 2')
  const refreshed = page.waitForRequest(request => request.url().includes('/incidents?') && new URL(request.url()).searchParams.get('cursor') === 'page-two')
  await page.getByTestId('adaptive-incidents-refresh').click()
  await refreshed
  await expect(page.getByTestId('adaptive-incidents-page')).toHaveText('Page 2')
  await expect(page.getByTestId('adaptive-incidents-table')).toContainText('Older key')
  await page.getByTestId('adaptive-incidents-search').fill('next')
  await expect(page.getByTestId('adaptive-incidents-page')).toHaveText('Page 1')
  await expect.poll(() => calls.at(-1)?.searchParams.get('q')).toBe('next')
  expect(calls.at(-1)?.searchParams.get('cursor')).toBe('')
})

test('resolved incidents retain visible causes independently of generic summaries and request logs', async ({ page }, testInfo) => {
  const incident = {
    id: 'resolved-incident', scope: 'key_model', provider: 'openai', model: 'test-model',
    state: 'healthy', worst_state: 'held', status: 'resolved', failure_class: 'rate_limit',
    opened_at: '2026-11-21T10:00:00Z', updated_at: '2026-11-21T10:01:00Z', last_seen: '2026-11-21T10:01:00Z',
    closed_at: '2026-11-21T10:01:00Z', resolved_at: '2026-11-21T10:01:00Z',
    summary: 'The route recovered.', action: 'Normal selection resumed.', checks: 3, failed_checks: 1, refused_again: 0,
  }
  await page.route('**/api/adaptive-routing/incidents?*', route => route.fulfill({ json: { incidents: [incident], total: 1 } }))
  await page.route('**/api/adaptive-routing/incidents/resolved-incident?*', route => route.fulfill({ json: {
    incident, events: [
      { id: 'event-1', timestamp: incident.opened_at, kind: 'opened', failure_class: 'rate_limit', message: 'A route was held.' },
      { id: 'event-2', timestamp: incident.updated_at, kind: 'observed', failure_class: 'new_provider_failure', message: 'Another failure was observed.' },
    ],
  } }))
  await page.goto('/workspace/adaptive-routing/incidents')
  await expect(page.getByTestId('adaptive-incident-cause')).toHaveText('Rate limit reached')
  await expect(page.getByTestId('adaptive-incident-row')).toContainText('The route recovered.')
  await page.getByTestId('adaptive-incident-open').click()
  const detail = page.getByTestId('adaptive-incident-detail')
  await expect(detail.locator('dd').filter({ hasText: 'Rate limit reached' })).toBeVisible()
  await expect(detail.getByTestId('adaptive-incident-event-cause').first()).toHaveText('Failure class: Rate limit reached')
  await expect(detail.getByTestId('adaptive-incident-event-cause').last()).toHaveText('Failure class: new_provider_failure')
  await expect(detail).toContainText('No request ID was recorded.')
  await page.screenshot({ path: testInfo.outputPath('resolved-incident-causes.png'), fullPage: true })
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

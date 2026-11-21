import { expect, test } from '../../core/fixtures/base.fixture'
import type { AdaptiveRoutingIncident, AdaptiveRoutingIncidentPage } from '../../../../ui/lib/types/adaptiveRouting'

// Only runtime-fixture.py enables this test. Never change a shared gateway.
const fixtureURL = process.env.ADAPTIVE_RUNTIME_FIXTURE_URL
test.use({ skipAutoLogin: true })
test.skip(!fixtureURL, 'Requires the isolated adaptive routing runtime fixture')

test('real requests produce durable incidents and native request logs across restart', async ({ page }, testInfo) => {
  test.setTimeout(180_000)
  const control = await page.request.get(`${fixtureURL}/_fixture/status`)
  const fixture = await control.json()
  expect(fixture.fixture).toBe('adaptive-routing')
  const gateway: string = fixture.gateway
  expect(new URL(gateway).hostname).toBe('127.0.0.1')
  const login = async () => {
    // A browser on Windows can reach a restarted WSL listener slightly later
    // than the fixture's in-WSL readiness probe. Observe from the caller too.
    await expect.poll(async () => {
      try { return (await page.request.get(`${gateway}/health`, { timeout: 1000 })).ok() } catch { return false }
    }).toBeTruthy()
    const response = await page.request.post(`${gateway}/api/session/login`, { data: { username: 'adaptive-test', password: 'adaptive-test-password' } })
    expect(response.ok(), await response.text()).toBeTruthy()
  }
  await login()
  const javascriptErrors: string[] = []
  page.on('pageerror', error => javascriptErrors.push(error.message))
  await page.goto(`${gateway}/workspace/adaptive-routing/settings`)
  await expect(page.getByTestId('adaptive-routing-save')).toBeVisible()
  await page.getByTestId('adaptive-routing-save').click()
  await expect(page.getByText('Routing settings saved and applied.', { exact: true })).toBeVisible()

  const request = (id: string, data: Record<string, unknown> = {}, pin?: string) => page.request.post(`${gateway}/v1/chat/completions`, {
    headers: { 'x-request-id': id, ...(pin ? { 'x-bf-api-key-id': pin } : {}) },
    data: { model: 'openai/test-model', messages: [{ role: 'user', content: 'A controlled routing check' }], ...data },
  })
  const incidents = async () => (await (await page.request.get(`${gateway}/api/adaptive-routing/incidents`)).json()) as AdaptiveRoutingIncidentPage
  const getIncident = async (id: string) => (await (await page.request.get(`${gateway}/api/adaptive-routing/incidents/${id}`)).json()).incident as AdaptiveRoutingIncident
  const refusedId = `adaptive-refused-${Date.now()}`
  await page.request.post(`${fixtureURL}/_fixture/mode`, { data: { failure: 429 } })
  const refusal = await request(refusedId, {}, 'failure-key')
  expect(refusal.status(), await refusal.text()).toBe(429)
  await expect.poll(async () => (await incidents()).incidents.some(row => row.key_id === 'failure-key' && !row.closed_at)).toBeTruthy()
  const incident = (await incidents()).incidents.find(row => row.key_id === 'failure-key' && !row.closed_at)!
  expect(incident.first_request_id).toBe(refusedId)
  expect(incident.scope).toBe('key_model')

  const healthyId = `adaptive-skip-${Date.now()}`
  const healthy = await request(healthyId)
  expect(healthy.ok(), await healthy.text()).toBeTruthy()
  await expect.poll(async () => {
    const response = await page.request.get(`${gateway}/api/logs/${healthyId}`)
    return response.ok() ? (await response.json()).routing_engine_logs : ''
  }).toMatch(/loadbalancing/)

  await page.goto(`${gateway}/workspace/adaptive-routing/incidents?incident=${incident.id}`)
  await expect(page.getByTestId('adaptive-incident-detail')).toBeVisible()
  await expect(page.getByTestId('adaptive-incident-cause').first()).toHaveText('Rate limit reached')
  await expect(page.getByTestId('adaptive-incident-detail').locator('dd').filter({ hasText: 'Rate limit reached' })).toBeVisible()
  await expect(page.getByTestId('adaptive-incident-events')).toContainText(/./)
  await page.screenshot({ path: testInfo.outputPath('durable-incident.png'), fullPage: true })
  await page.getByTestId('adaptive-incident-request').first().click()
  await expect(page.getByRole('tab', { name: /^Routing/ })).toBeVisible()
  await page.getByRole('tab', { name: /^Routing/ }).click()
  await expect(page.getByText('Loadbalancing', { exact: true }).first()).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('native-request-routing.png'), fullPage: true })

  // Kill, not a graceful checkpoint: committed holds and incidents must already be durable.
  const restarted = await page.request.post(`${fixtureURL}/_fixture/restart`, { data: { crash: true }, timeout: 90_000 })
  expect(restarted.ok(), await restarted.text()).toBeTruthy()
  await login()
  const restored = await getIncident(incident.id)
  expect(restored.closed_at).toBeUndefined()
  expect(restored.first_request_id).toBe(refusedId)
  const routeStatus = await (await page.request.get(`${gateway}/api/adaptive-routing/status`)).json()
  expect(routeStatus.persistence.healthy).toBeTruthy()
  expect(routeStatus.routes.some((row: { key_id: string; state: string }) => row.key_id === 'failure-key' && row.state === 'held')).toBeTruthy()

  await page.request.post(`${fixtureURL}/_fixture/mode`, { data: { failure: 0 } })
  await page.goto(`${gateway}/workspace/adaptive-routing`)
  await page.getByRole('row').filter({ hasText: 'Failure key' }).getByTestId('adaptive-routing-release').click()
  await expect(page.getByText('Hold released. The next request can use this key.', { exact: true })).toBeVisible()
  // Pin these real checks so acceptance does not depend on a random traffic share.
  for (let index = 0; index < 8; index++) {
    const response = await request(`adaptive-recovery-${Date.now()}-${index}`, {}, 'failure-key')
    expect(response.ok(), await response.text()).toBeTruthy()
    if ((await getIncident(incident.id)).status === 'resolved') break
  }
  const resolved = await getIncident(incident.id)
  expect(resolved.status).toBe('resolved')
  expect(resolved.resolved_at).toBeTruthy()
  expect(resolved.closed_at).toBeTruthy()
  await page.request.post(`${fixtureURL}/_fixture/restart`, { data: {}, timeout: 90_000 })
  await login()
  expect((await getIncident(incident.id)).resolved_at).toBe(resolved.resolved_at)

  // Native fallback and real SSE retain their actual key and routing explanation.
  await page.request.post(`${fixtureURL}/_fixture/mode`, { data: { failure: 503 } })
  const fallback = await request(`adaptive-fallback-${Date.now()}`, { model: 'openai/unavailable-model', fallbacks: ['backup/test-model'] })
  expect(fallback.ok(), await fallback.text()).toBeTruthy()
  const streamId = `adaptive-stream-${Date.now()}`
  const stream = await request(streamId, { stream: true }, 'healthy-key')
  expect(stream.ok(), await stream.text()).toBeTruthy()
  expect(stream.headers()['content-type']).toContain('text/event-stream')
  expect(await stream.text()).toContain('data: [DONE]')
  await expect.poll(async () => {
    const response = await page.request.get(`${gateway}/api/logs/${streamId}`)
    if (!response.ok()) return null
    const log = await response.json()
    return { key: log.selected_key_id, name: log.selected_key_name, explanation: log.routing_engine_logs }
  }).toEqual({ key: 'healthy-key', name: 'Healthy key', explanation: expect.stringMatching(/explicit/i) })
  await page.goto(`${gateway}/workspace/adaptive-routing/incidents?incident=${incident.id}`)
  await expect(page.getByTestId('adaptive-incident-detail')).toContainText('Resolved')
  await expect(page.getByTestId('adaptive-incident-detail').locator('dd').filter({ hasText: 'Rate limit reached' })).toBeVisible()
  await expect(page.getByTestId('adaptive-incident-event-cause').first()).toContainText('Rate limit reached')
  // Incident history survives the independent retention/deletion of request evidence.
  const deleted = await page.request.delete(`${gateway}/api/logs`, { data: { ids: [refusedId] } })
  expect(deleted.ok(), await deleted.text()).toBeTruthy()
  await page.getByTestId('adaptive-incident-request').first().click()
  await expect(page.getByText('log not found', { exact: false }).first()).toBeVisible()
  await page.goto(`${gateway}/workspace/adaptive-routing/incidents?incident=${incident.id}`)
  await expect(page.getByTestId('adaptive-incident-detail')).toContainText('Resolved')
  await page.setViewportSize({ width: 390, height: 844 })
  await page.screenshot({ path: testInfo.outputPath('incident-mobile.png'), fullPage: true })
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBeTruthy()
  expect(javascriptErrors).toEqual([])
  await testInfo.attach('runtime-receipt', { body: JSON.stringify({ refusedId, healthyId, streamId, incident: resolved, fixture: await (await page.request.get(`${fixtureURL}/_fixture/status`)).json() }, null, 2), contentType: 'application/json' })
})

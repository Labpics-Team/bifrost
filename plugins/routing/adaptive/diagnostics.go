package adaptive

import "time"

// A pending diagnostic retains only the observation's identity, never its
// request context or response. The existing maintenance loop drains it using
// the same revision fencing and single lifecycle writer as an immediate update.
type pendingDiagnostic struct {
	fence         *attempt
	request       string
	at            time.Time
	key, provider bool
}

func (c *Controller) deferDiagnostic(a *attempt, request string, now time.Time, key, provider bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.attemptCurrentLocked(a) {
		return
	}
	if c.pendingDiagnostics == nil {
		c.pendingDiagnostics = make(map[routeID]pendingDiagnostic)
	}
	id := a.route.routeID
	previous := c.pendingDiagnostics[id]
	if previous.fence != nil && c.attemptCurrentLocked(previous.fence) {
		key = key || previous.key
		provider = provider || previous.provider
		if previous.at.After(now) {
			previous.key, previous.provider = key, provider
			c.pendingDiagnostics[id] = previous
			return
		}
	}
	c.pendingDiagnostics[id] = pendingDiagnostic{
		fence:   &attempt{route: a.route, key: a.key, keyEpoch: a.keyEpoch, modelEpoch: a.modelEpoch, providerRevision: a.providerRevision},
		request: request, at: now, key: key, provider: provider,
	}
}

func (c *Controller) flushDiagnostics() {
	c.mu.Lock()
	pending := c.pendingDiagnostics
	c.pendingDiagnostics = nil
	c.mu.Unlock()
	for _, item := range pending {
		c.mu.Lock()
		if !c.attemptCurrentLocked(item.fence) {
			c.mu.Unlock()
			continue
		}
		// The pending update is a reconciliation of current health, not a
		// synthetic attempt or proof that a past observation caused it.
		if last := item.fence.route.LastSeen; last.After(item.at) {
			item.at, item.request = last, ""
		}
		c.mu.Unlock()
		if item.key {
			c.updateHealthIncident(item.fence, item.request, item.at)
		}
		if item.provider {
			c.updateProviderIncident(item.fence, item.request, item.at)
		}
	}
}

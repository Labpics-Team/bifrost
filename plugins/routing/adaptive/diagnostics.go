package adaptive

import (
	"context"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

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
		fence:   &attempt{route: a.route, key: a.key, keyEpoch: a.keyEpoch, modelEpoch: a.modelEpoch, providerRevision: a.providerRevision, probeID: a.probeID, holdID: a.holdID},
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

type healthSummary struct {
	eligible, unavailable int
	degraded, recovering  bool
}

func (s *healthSummary) add(state string, unavailable bool) {
	s.eligible++
	if unavailable {
		s.unavailable++
	}
	s.degraded = s.degraded || state == "degraded"
	s.recovering = s.recovering || state == "recovering"
}

func (s healthSummary) state() string {
	switch {
	case s.eligible == 0:
		return ""
	case s.unavailable == s.eligible:
		return "failed"
	case s.unavailable > 0 || s.degraded:
		return "degraded"
	case s.recovering:
		return "recovering"
	default:
		return "healthy"
	}
}

func (c *Controller) providerHealthStateLocked(provider schemas.ModelProvider, model string, keys []schemas.Key, now time.Time) string {
	var health healthSummary
	for _, key := range keys {
		if key.Enabled != nil && !*key.Enabled || !c.supports(provider, key, model) {
			continue
		}
		id := routeID{provider, model, key.ID}
		state := "unknown"
		if r := c.routes[id]; r != nil {
			state = c.stateLocked(r, now)
		}
		health.add(state, c.weightLocked(id, now) == 0)
	}
	return health.state()
}

// A released whole-key incident can outlive several model observations. Use
// their current health together; one expired model cannot erase another failure.
func (c *Controller) keyHealthStatesLocked(scopes map[routeID]bool, now time.Time) map[routeID]healthSummary {
	states := make(map[routeID]healthSummary)
	for rid, r := range c.routes {
		id := routeID{Provider: rid.Provider, KeyID: rid.KeyID}
		if id.KeyID == "" || !scopes[id] || c.incidents[id] == nil || now.Sub(r.LastSeen) > idleTTL {
			continue
		}
		health := states[id]
		health.add(c.stateLocked(r, now), c.weightLocked(rid, now) == 0)
		states[id] = health
	}
	return states
}

func (c *Controller) elapsedCooldownsLocked(now time.Time) map[routeID]bool {
	scopes := make(map[routeID]bool)
	for id, r := range c.routes {
		if r.FailedUntil.IsZero() || r.FailedUntil.After(now) || now.Sub(r.LastSeen) > idleTTL || c.holdLocked(id, now) != nil {
			continue
		}
		scopes[id] = true
		if id.KeyID != "" {
			scopes[routeID{Provider: id.Provider, KeyID: id.KeyID}] = true
		}
		scopes[routeID{Provider: id.Provider, Model: id.Model}] = true
	}
	return scopes
}

// Reconcile only existing, open observations. Expiry changes admission state,
// never samples, successes, request identities, or the time of real evidence.
func (c *Controller) reconcileCooldowns(ctx context.Context) error {
	c.mu.Lock()
	if !c.config.load().HistoryEnabled || len(c.pendingResets) != 0 {
		c.mu.Unlock()
		return nil
	}
	revision, generation := c.revision, c.resetSequence
	scopes := c.elapsedCooldownsLocked(c.now())
	providers := make(map[schemas.ModelProvider]string)
	needCatalog := make(map[schemas.ModelProvider]bool)
	for id, incident := range c.incidents {
		if !scopes[id] || (incident.State != "failed" && incident.State != "degraded") || incident.ClosedAt != nil {
			continue
		}
		providers[id.Provider] = c.providerRevisions[id.Provider]
		if id.KeyID == "" && c.routes[id] == nil {
			needCatalog[id.Provider] = true
		}
	}
	c.mu.Unlock()
	if len(providers) == 0 {
		return nil
	}
	catalogs := make(map[schemas.ModelProvider][]schemas.Key)
	if c.keys != nil && c.supports != nil {
		for provider := range needCatalog {
			if keys, err := c.keys(ctx, provider); err == nil {
				catalogs[provider] = keys
			}
		}
	}
	return c.transition(ctx, func() transition {
		if c.revision != revision || c.resetSequence != generation || !c.config.load().HistoryEnabled {
			return transition{}
		}
		for provider, identity := range providers {
			if c.providerRevisions[provider] != identity {
				return transition{}
			}
		}
		now := c.now()
		scopes := c.elapsedCooldownsLocked(now)
		keyHealth := c.keyHealthStatesLocked(scopes, now)
		t := transition{}
		for id, source := range c.incidents {
			if !scopes[id] || (source.State != "failed" && source.State != "degraded") || source.ClosedAt != nil || source.ProviderRevision != c.providerRevisions[id.Provider] {
				continue
			}
			state := ""
			if id.KeyID != "" && id.Model == "" {
				key := c.keyStates[id]
				if key == nil || key.revision != source.KeyRevision || c.holdLocked(id, now) != nil {
					continue
				}
				state = keyHealth[id].state()
			} else if r := c.routes[id]; r != nil {
				if source.KeyRevision != r.KeyRevision {
					continue
				}
				state = c.stateLocked(r, now)
			} else if keys, ok := catalogs[id.Provider]; id.KeyID == "" && ok {
				state = c.providerHealthStateLocked(id.Provider, id.Model, keys, now)
			}
			if state == source.State || state != "recovering" && state != "degraded" {
				continue // Elapsed time alone never proves healthy or releases a hold.
			}
			i := *source
			setIncidentLifecycleState(&i, state, now)
			t.incidents = append(t.incidents, &i)
			t.events = append(t.events, incidentEvent(&i, "state_changed", "", "A failure cooldown elapsed; real requests must establish recovery.", now))
		}
		return t
	})
}

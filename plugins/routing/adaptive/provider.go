package adaptive

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

type direction struct {
	provider schemas.ModelProvider
	model    string
	score    float64
	latency  float64
	due      *condition
	recover  *route
	eligible bool
	known    bool
}
type probeContextKey struct{}

// Provider admission has no key identity. Configured weights and native
// affinity select the actual key later; only that key receives an observation.
type providerCheck struct {
	token uint64
	until time.Time
}

func (c *Controller) reserveProviderCheckLocked(ctx *schemas.BifrostContext, id routeID, now time.Time) bool {
	id.KeyID = ""
	if c.providerChecks[id].until.After(now) {
		return false
	}
	if _, exists := c.providerChecks[id]; !exists && len(c.providerChecks) >= maxRoutes {
		c.pruneProviderChecksLocked(now)
		if len(c.providerChecks) >= maxRoutes {
			return false // Never evict a live lease to admit another check.
		}
	}
	if c.providerChecks == nil {
		c.providerChecks = make(map[routeID]providerCheck)
	}
	c.probeSequence++
	c.providerChecks[id] = providerCheck{token: c.probeSequence, until: now.Add(5 * time.Minute)}
	c.setProbeReservationLocked(ctx, probeReservation{routeID: id, token: c.probeSequence, providerOnly: true})
	return true
}

func (c *Controller) finishProviderCheckLocked(reserved probeReservation, now time.Time) {
	if reserved.providerOnly && c.reservationCurrentLocked(reserved, now) {
		c.providerChecks[reserved.routeID] = providerCheck{until: now.Add(recoveryInterval)}
	}
}

func (c *Controller) pruneProviderChecksLocked(now time.Time) {
	for id, check := range c.providerChecks {
		if !check.until.After(now) {
			delete(c.providerChecks, id)
		}
	}
}

func (c *Controller) setProbeReservationLocked(ctx *schemas.BifrostContext, reserved probeReservation) {
	if previous, ok := ctx.Value(probeContextKey{}).(probeReservation); ok && previous.providerOnly {
		c.finishProviderCheckLocked(previous, c.now())
	}
	ctx.SetValue(probeContextKey{}, reserved)
}

func (c *Controller) reserveProbe(ctx *schemas.BifrostContext, d direction) bool {
	if d.due != nil {
		return c.reserveCondition(ctx, routeID{d.provider, d.model, d.due.KeyID}, false)
	}
	if d.recover == nil {
		return false
	}
	if !c.lifeMu.TryLock() {
		return false
	}
	defer c.lifeMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	r := d.recover
	now := c.now()
	if c.routes[r.routeID] != r || !c.recoveryDueLocked(r, now) {
		return false
	}
	if !c.config.load().RouteSelectionEnabled && r.KeyID != "" {
		return c.reserveProviderCheckLocked(ctx, r.routeID, now)
	}
	c.probeSequence++
	// Only older checks are superseded. Ordinary recovery traffic keeps its
	// lifecycle epoch and its real outcomes, even while a check is replaced.
	r.ProbeID = c.probeSequence
	r.ProbeUntil = now.Add(5 * time.Minute)
	c.setProbeReservationLocked(ctx, probeReservation{routeID: r.routeID, token: r.ProbeID})
	return true
}

func (c *Controller) direction(ctx *schemas.BifrostContext, provider schemas.ModelProvider, model, keyID string) direction {
	d := direction{provider: provider, model: model}
	if c.keys == nil || c.supports == nil {
		return d
	}
	keys, err := c.keys(ctx, provider)
	if err != nil {
		return d
	}
	// A successful empty lookup is unavailable; a failed lookup is unknown.
	d.known = true
	for _, key := range keys {
		c.syncKey(provider, key)
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	count, measured := 0, 0
	total, latency := 0.0, 0.0
	for _, key := range keys {
		if key.Enabled != nil && !*key.Enabled || keyID != "" && key.ID != keyID || !c.supports(provider, key, model) {
			continue
		}
		count++
		id := routeID{provider, model, key.ID}
		weight := c.weightLocked(id, now)
		total += weight
		if r := c.routes[id]; r != nil {
			if weight > 0 && r.LatencyMS > 0 {
				latency += r.LatencyMS
				measured++
			}
			if c.recoveryDueLocked(r, now) && (d.recover == nil || r.LastSeen.Before(d.recover.LastSeen)) {
				d.recover = r
			}
		}
		if h := c.holdLocked(id, now); h != nil && !h.HoldUntil.After(now) && !h.ProbeUntil.After(now) {
			d.due = h
		}
	}
	if count == 0 && c.keyless != nil && c.keyless(provider) {
		d.eligible = true
		id := routeID{Provider: provider, Model: model}
		d.score = c.weightLocked(id, now)
		if r := c.routes[id]; r != nil {
			d.latency = r.LatencyMS
			if c.recoveryDueLocked(r, now) {
				d.recover = r
			}
		}
	}
	if count > 0 {
		d.eligible = true
		d.score = total / float64(count)
		// Comparing the inverse first-response latency is relative across the
		// providers serving this same requested model, including aliases.
		if measured > 0 {
			d.latency = latency / float64(measured)
		}
	}
	return d
}

func pinnedKey(ctx *schemas.BifrostContext) bool {
	id, _ := ctx.Value(schemas.BifrostContextKeyAPIKeyID).(string)
	name, _ := ctx.Value(schemas.BifrostContextKeyAPIKeyName).(string)
	rule, _ := ctx.Value(schemas.BifrostContextKeyRoutingPinnedAPIKeyID).(string)
	if strings.TrimSpace(id) != "" || strings.TrimSpace(name) != "" || strings.TrimSpace(rule) != "" {
		return true
	}
	return ctx.Value(schemas.BifrostContextKeyDirectKey) != nil
}

// Route runs after native CEL and virtual-key routing. It does not grant access:
// the model catalog and the already-published governance allowlist define the pool.
func (c *Controller) Route(ctx *schemas.BifrostContext, req *schemas.BifrostRequest) error {
	cfg := c.Config()
	if !c.enabled.Load() || !cfg.DirectionSelectionEnabled || c.providers == nil {
		return nil
	}
	provider, model, fallbacks := req.GetRequestFields()
	if model == "" {
		return nil
	}
	named := provider != ""
	if named && !cfg.AppendFallbacksToPinned && !cfg.RerouteFailedDirections {
		return nil
	}
	if !supportsRequest(ctx) || pinnedKey(ctx) {
		return nil
	}
	allowed, restricted := ctx.Value(schemas.BifrostContextKeyRoutingAllowedProviders).([]schemas.ModelProvider)
	permitted := func(p schemas.ModelProvider) bool { return !restricted || slices.Contains(allowed, p) }
	directions := make([]direction, 0)
	for _, p := range c.providers(model) {
		if permitted(p) {
			d := c.direction(ctx, p, model, "")
			if d.eligible {
				directions = append(directions, d)
			}
		}
	}
	baseline := 0.0
	for _, d := range directions {
		if d.latency > 0 && d.score > 0 && (baseline == 0 || d.latency < baseline) {
			baseline = d.latency
		}
	}
	if baseline > 0 {
		for i := range directions {
			directions[i].score *= latencyFactor(directions[i].latency / baseline)
		}
	}
	// Shuffle only equal scores so a new key/provider starts with a fair share.
	rand.Shuffle(len(directions), func(i, j int) { directions[i], directions[j] = directions[j], directions[i] })
	sort.SliceStable(directions, func(i, j int) bool { return directions[i].score > directions[j].score })
	selected := schemas.Fallback{Provider: provider, Model: model}
	redirected := false
	checking := false
	if !named {
		if len(directions) == 0 {
			return nil
		}
		selected.Provider = directions[0].provider
		// Bifrost ranks providers, but distributes traffic across keys.
		// Due checks below are the exception to the healthiest-provider pick.
		// A due hold, failure cooldown or stale unhealthy observation gets one real recheck,
		// with healthy directions behind it. Only adaptive key selection or
		// a held-key check may reserve a particular credential.
		for _, d := range directions {
			if c.reserveProbe(ctx, d) {
				selected.Provider = d.provider
				checking = true
				break
			}
		}
	} else if current := c.direction(ctx, provider, model, ""); cfg.RerouteFailedDirections && current.known && current.score == 0 {
		checking = c.reserveProbe(ctx, current)
		if !checking {
			if cfg.RerouteTarget == "fallbacks" {
				for _, fb := range fallbacks {
					if permitted(fb.Provider) && c.direction(ctx, fb.Provider, fb.Model, fb.KeyID).score > 0 {
						selected = fb
						redirected = true
						break
					}
				}
			}
			if !redirected {
				for _, d := range directions {
					if d.provider != provider && d.score > 0 {
						selected.Provider = d.provider
						redirected = true
						break
					}
				}
			}
		}
	}
	if named && !redirected && !checking && !cfg.AppendFallbacksToPinned {
		return nil
	}
	chain := make([]schemas.Fallback, 0, len(fallbacks)+len(directions)+1)
	appendUnique := func(fb schemas.Fallback) {
		if fb.Provider == selected.Provider && fb.Model == selected.Model && fb.KeyID == selected.KeyID {
			return
		}
		if !slices.ContainsFunc(chain, func(x schemas.Fallback) bool {
			return x.Provider == fb.Provider && x.Model == fb.Model && x.KeyID == fb.KeyID
		}) {
			chain = append(chain, fb)
		}
	}
	// A due check can add healthy alternatives without changing the native
	// permission to prune a named request's explicitly configured fallbacks.
	prune := cfg.PruneFailedFallbacks && (!named || cfg.AppendFallbacksToPinned || redirected)
	for _, fb := range fallbacks {
		if prune {
			if d := c.direction(ctx, fb.Provider, fb.Model, fb.KeyID); d.known && d.score == 0 {
				continue
			}
		}
		appendUnique(fb)
	}
	if !named || checking || cfg.AppendFallbacksToPinned {
		for _, d := range directions {
			if d.score > 0 {
				appendUnique(schemas.Fallback{Provider: d.provider, Model: model, Inferred: true})
			}
		}
	}
	if redirected {
		appendUnique(schemas.Fallback{Provider: provider, Model: model})
	}
	req.SetProvider(selected.Provider)
	req.SetModel(selected.Model)
	req.SetFallbacks(chain)
	if selected.KeyID != "" {
		ctx.SetValue(schemas.BifrostContextKeyRoutingPinnedAPIKeyID, selected.KeyID)
	}
	ctx.AppendRoutingEngineLog(schemas.RoutingEngineLoadbalancing, schemas.LogLevelInfo, fmt.Sprintf("Adaptive routing selected %s/%s from observed key health; %d fallback(s)", selected.Provider, selected.Model, len(chain)))
	schemas.AppendToContextList(ctx, schemas.BifrostContextKeyRoutingEnginesUsed, schemas.RoutingEngineLoadbalancing)
	return nil
}

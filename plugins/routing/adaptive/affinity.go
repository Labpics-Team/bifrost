package adaptive

import (
	"slices"

	"github.com/maximhq/bifrost/core/schemas"
)

// SessionAffinity decorates the native Bifrost affinity policy with adaptive
// eligibility. A session is a preference, not an explicit caller or rule pin.
func (c *Controller) SessionAffinity(native schemas.SessionAffinity) schemas.SessionAffinity {
	return &adaptiveAffinity{Controller: c, SessionAffinity: native}
}

type adaptiveAffinity struct {
	*Controller
	schemas.SessionAffinity
}

func (a *adaptiveAffinity) ResolveRoute(ctx *schemas.BifrostContext, requested schemas.Route, chain []schemas.Route) []schemas.Route {
	if !a.Enabled() || !supportsRequest(ctx) || requested.Provider != "" || len(chain) < 2 {
		return a.SessionAffinity.ResolveRoute(ctx, requested, chain)
	}
	// The next real request that can check a due provider belongs to that
	// check, not to the previous session binding.
	if lease, ok := ctx.Value(probeContextKey{}).(probeReservation); ok && lease.Provider == chain[0].Provider && lease.Model == chain[0].Model {
		a.mu.Lock()
		current := a.reservationCurrentLocked(lease, a.now())
		a.mu.Unlock()
		if current {
			return chain
		}
	}
	// Only restrict affinity, not the caller's fallback chain. Unknown health
	// is not a failure and stays eligible.
	var eligible []schemas.Route
	for i, route := range chain {
		// Hooks own the primary. Filter only routes affinity could promote,
		// so native affinity sees the same first choice and remembers what
		// this request actually follows.
		if i == 0 {
			continue
		}
		d := a.direction(ctx, route.Provider, route.Model, route.KeyID)
		if d.known && d.score == 0 {
			if eligible == nil {
				eligible = make([]schemas.Route, 0, len(chain)-1)
				eligible = append(eligible, chain[:i]...)
			}
			continue
		}
		if eligible != nil {
			eligible = append(eligible, route)
		}
	}
	if eligible == nil {
		return a.SessionAffinity.ResolveRoute(ctx, requested, chain)
	}
	resolved := a.SessionAffinity.ResolveRoute(ctx, requested, eligible)
	if len(resolved) == 0 || len(eligible) == 0 || resolved[0] == eligible[0] {
		return chain
	}
	// The native policy moved a still-eligible session route to the front.
	// Keep all explicitly configured fallbacks in their original order.
	at := slices.Index(chain, resolved[0])
	if at < 1 {
		return chain
	}
	out := make([]schemas.Route, 0, len(chain))
	out = append(out, chain[at])
	out = append(out, chain[:at]...)
	out = append(out, chain[at+1:]...)
	return out
}

func (a *adaptiveAffinity) ResolveKey(ctx *schemas.BifrostContext, provider schemas.ModelProvider, model string, keys []schemas.Key) (schemas.Key, bool) {
	if !a.Enabled() || !supportsRequest(ctx) {
		return a.SessionAffinity.ResolveKey(ctx, provider, model, keys)
	}
	for _, key := range keys {
		a.syncKey(provider, key)
	}
	now := a.now()
	adaptiveKeys := a.Config().RouteSelectionEnabled
	a.mu.Lock()
	var eligible []schemas.Key
	for i, key := range keys {
		id := routeID{provider, model, key.ID}
		hold := a.holdLocked(id, now)
		r := a.routes[id]
		// A due check must have a chance even if all requests are sticky.
		if hold != nil && !hold.HoldUntil.After(now) && !hold.ProbeUntil.After(now) ||
			adaptiveKeys && a.recoveryDueLocked(r, now) {
			a.mu.Unlock()
			return schemas.Key{}, false
		}
		if lease, ok := ctx.Value(probeContextKey{}).(probeReservation); ok && lease.routeID == id && a.reservationCurrentLocked(lease, now) {
			a.mu.Unlock()
			return schemas.Key{}, false
		}
		if hold != nil || adaptiveKeys && r != nil && !r.FailedUntil.IsZero() {
			if eligible == nil {
				eligible = make([]schemas.Key, 0, len(keys)-1)
				eligible = append(eligible, keys[:i]...)
			}
			continue
		}
		if eligible != nil {
			eligible = append(eligible, key)
		}
	}
	a.mu.Unlock()
	if eligible == nil {
		eligible = keys
	}
	return a.SessionAffinity.ResolveKey(ctx, provider, model, eligible)
}

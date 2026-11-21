package adaptive

import (
	"fmt"
	"math"
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
}
type probeContextKey struct{}

func (c *Controller) reserveProbe(ctx *schemas.BifrostContext, d direction) bool {
	if d.due != nil {
		return c.reserveCondition(ctx, routeID{d.provider, d.model, d.due.KeyID}, false)
	}
	if d.recover == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := d.recover
	now := c.now()
	if c.routes[r.routeID] != r || r.FailedUntil.IsZero() || r.FailedUntil.After(now) || r.ProbeUntil.After(now) {
		return false
	}
	c.probeSequence++
	// A replacement check supersedes earlier outcomes even after its disposable
	// lease is cleared. Attempts and incident writers already fence this epoch.
	r.Epoch++
	r.ProbeID = c.probeSequence
	r.ProbeUntil = now.Add(5 * time.Minute)
	ctx.SetValue(probeContextKey{}, probeReservation{routeID: r.routeID, token: r.ProbeID})
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
	for _, key := range keys {
		c.syncKey(provider, key)
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	total, latency := 0.0, 0.0
	for _, key := range keys {
		if key.Enabled != nil && !*key.Enabled || keyID != "" && key.ID != keyID || !c.supports(provider, key, model) {
			continue
		}
		count++
		id := routeID{provider, model, key.ID}
		total += c.weightLocked(id, now)
		if r := c.routes[id]; r != nil {
			latency += r.LatencyMS
			if !r.FailedUntil.IsZero() && !r.FailedUntil.After(now) && !r.ProbeUntil.After(now) && c.holdLocked(id, now) == nil {
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
			if !r.FailedUntil.IsZero() && !r.FailedUntil.After(now) && !r.ProbeUntil.After(now) {
				d.recover = r
			}
		}
	}
	if count > 0 {
		d.eligible = true
		d.score = total / float64(count)
		// Comparing the inverse first-response latency is relative across the
		// providers serving this same requested model, including aliases.
		d.latency = latency / float64(count)
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
	if pinnedKey(ctx) {
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
			directions[i].score /= math.Sqrt(math.Max(1, directions[i].latency/baseline))
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
		// Keep real traffic on every usable direction, as we do for keys.
		// Always taking the maximum would freeze a recovered provider's last
		// bad score forever behind a healthy provider that keeps serving.
		total := 0.0
		for _, d := range directions {
			total += d.score
		}
		draw := rand.Float64() * total
		for _, d := range directions {
			draw -= d.score
			if draw < 0 {
				selected.Provider = d.provider
				break
			}
		}
		// A due hold or an expired failure cooldown gets one real recheck,
		// with healthy directions behind it. The slot also names the key.
		for _, d := range directions {
			if c.reserveProbe(ctx, d) {
				selected.Provider = d.provider
				checking = true
				break
			}
		}
	} else if current := c.direction(ctx, provider, model, ""); cfg.RerouteFailedDirections && current.score == 0 {
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
		if prune && c.direction(ctx, fb.Provider, fb.Model, fb.KeyID).score == 0 {
			continue
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

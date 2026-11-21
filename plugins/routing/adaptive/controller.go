package adaptive

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/keyselectors"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
)

const (
	maxRoutes = 8192
	idleTTL   = 24 * time.Hour
	decay     = 0.2
)

type routeID struct {
	Provider schemas.ModelProvider `json:"provider"`
	Model    string                `json:"model"`
	KeyID    string                `json:"key_id"`
}

type route struct {
	routeID
	KeyRevision  string               `json:"key_revision"`
	Epoch        uint64               `json:"-"`
	KeyName      string               `json:"key_name"`
	Samples      uint64               `json:"samples"`
	ErrorRate    float64              `json:"error_rate"`
	LatencyMS    float64              `json:"latency_ms"`
	Inflight     int                  `json:"inflight"`
	Consecutive  int                  `json:"consecutive_failures"`
	Recoveries   int                  `json:"recoveries"`
	LastSeen     time.Time            `json:"last_seen"`
	ProbeUntil   time.Time            `json:"-"`
	ProbeID      uint64               `json:"-"`
	Recovering   bool                 `json:"recovering"`
	FailedUntil  time.Time            `json:"failed_until,omitempty"`
	FailureClass schemas.FailureClass `json:"failure_class,omitempty"`
}

type RouteStatus struct {
	Provider     schemas.ModelProvider `json:"provider"`
	Model        string                `json:"model"`
	KeyID        string                `json:"key_id"`
	KeyName      string                `json:"key_name"`
	State        string                `json:"state"`
	Score        float64               `json:"score"`
	Samples      uint64                `json:"samples"`
	ErrorRate    float64               `json:"error_rate"`
	LatencyMS    float64               `json:"latency_ms"`
	Inflight     int                   `json:"inflight"`
	LastSeen     time.Time             `json:"last_seen"`
	HoldUntil    *time.Time            `json:"hold_until,omitempty"`
	FailureClass schemas.FailureClass  `json:"failure_class,omitempty"`
}

// Controller keeps bounded telemetry separately from durable unresolved conditions.
type Controller struct {
	store          configstore.ConfigStore
	keyless        func(schemas.ModelProvider) bool
	providerExists func(schemas.ModelProvider) bool
	// lifeMu serializes durable transitions. mu is never held over database IO.
	lifeMu            sync.Mutex
	conditions        map[routeID]*condition
	keyStates         map[routeID]*keyState
	providerRevisions map[schemas.ModelProvider]string
	pendingResets     map[routeID]uint64
	resetSequence     uint64
	incidents         map[routeID]*Incident
	revision          uint64
	probeSequence     uint64
	persistence       PersistenceStatus
	stateStore        *configstore.AdaptiveRoutingStore
	enabled           atomic.Bool
	startOnce         sync.Once
	stop              context.CancelFunc
	wg                sync.WaitGroup
	config            configValue
	mu                sync.Mutex
	routes            map[routeID]*route
	now               func() time.Time
	keys              func(context.Context, schemas.ModelProvider) ([]schemas.Key, error)
	providers         func(string) []schemas.ModelProvider
	supports          func(schemas.ModelProvider, schemas.Key, string) bool
}

func New(config Config) *Controller {
	c := &Controller{routes: make(map[routeID]*route), conditions: make(map[routeID]*condition), keyStates: make(map[routeID]*keyState), incidents: make(map[routeID]*Incident), persistence: PersistenceStatus{Healthy: true}, now: time.Now}
	c.providerRevisions = make(map[schemas.ModelProvider]string)
	c.pendingResets = make(map[routeID]uint64)
	c.config.store(config)
	c.enabled.Store(true)
	return c
}

// SetCatalog is called once before the server accepts requests. The callbacks
// read the live native catalog and eligible configured key pool.
func (c *Controller) SetCatalog(providers func(string) []schemas.ModelProvider, keys func(context.Context, schemas.ModelProvider) ([]schemas.Key, error), supports func(schemas.ModelProvider, schemas.Key, string) bool, keyless ...func(schemas.ModelProvider) bool) {
	c.providers, c.keys, c.supports = providers, keys, supports
	if len(keyless) > 0 {
		c.keyless = keyless[0]
	}
}

func (c *Controller) Config() Config { return c.config.load() }
func (c *Controller) SetProviderExists(exists func(schemas.ModelProvider) bool) {
	c.providerExists = exists
}
func (c *Controller) Enabled() bool { return c.enabled.Load() }
func (c *Controller) Apply(config Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	c.config.store(config)
	c.enabled.Store(true)
	return nil
}

func (c *Controller) routeLocked(id routeID, name string, now time.Time) *route {
	if r := c.routes[id]; r != nil {
		r.KeyName = name
		return r
	}
	if len(c.routes) >= maxRoutes {
		var oldest *route
		for _, r := range c.routes {
			if r.Inflight == 0 && (oldest == nil || r.LastSeen.Before(oldest.LastSeen)) {
				oldest = r
			}
		}
		if oldest == nil {
			return nil
		}
		delete(c.routes, oldest.routeID)
	}
	r := &route{routeID: id, KeyName: name, LastSeen: now}
	c.routes[id] = r
	return r
}

func (c *Controller) wholeKeyHoldLocked(id routeID, now time.Time) *condition {
	return c.conditions[routeID{Provider: id.Provider, KeyID: id.KeyID}]
}

func (c *Controller) holdLocked(id routeID, now time.Time) *condition {
	if whole := c.wholeKeyHoldLocked(id, now); whole != nil {
		return whole
	}
	return c.conditions[id]
}

func (c *Controller) weightLocked(id routeID, now time.Time) float64 {
	if c.holdLocked(id, now) != nil {
		return 0
	}
	r := c.routes[id]
	if r == nil {
		return 1
	}
	if r.FailedUntil.After(now) {
		return 0
	}
	// Success probability dominates latency and queueing. A usable route keeps
	// a small share of real traffic, so it can recover without synthetic probes.
	w := math.Max(0.05, math.Pow(1-r.ErrorRate, 3))
	w /= float64(1 + r.Inflight)
	if r.Recovering && r.Recoveries < 5 {
		w *= 0.1 + 0.18*float64(r.Recoveries)
	}
	return w
}

// SelectKey receives core's already filtered pool; caller/rule pins and session
// affinity keep their native precedence because those paths bypass this hook.
func (c *Controller) SelectKey(ctx *schemas.BifrostContext, keys []schemas.Key, provider schemas.ModelProvider, model string) (schemas.Key, error) {
	if len(keys) == 0 {
		return schemas.Key{}, fmt.Errorf("no eligible keys for %s/%s", provider, model)
	}
	if !c.enabled.Load() {
		return keyselectors.WeightedRandom(ctx, keys, provider, model)
	}
	for _, key := range keys {
		c.syncKey(provider, key)
	}
	now := c.now()
	c.mu.Lock()
	reserved, hasReservation := ctx.Value(probeContextKey{}).(probeReservation)
	if hasReservation && reserved.Provider == provider && reserved.Model == model {
		valid := false
		if h := c.holdLocked(reserved.routeID, now); h != nil {
			valid = h.ID == reserved.conditionID && h.ProbeID == reserved.token
		} else if r := c.routes[reserved.routeID]; r != nil {
			valid = r.ProbeID == reserved.token
		}
		if valid {
			for _, key := range keys {
				if key.ID == reserved.KeyID {
					c.mu.Unlock()
					return key, nil
				}
			}
		}
	}
	var due []routeID
	for _, key := range keys {
		id := routeID{provider, model, key.ID}
		if h := c.holdLocked(id, now); h != nil && !h.HoldUntil.After(now) && !h.ProbeUntil.After(now) {
			due = append(due, id)
		}
	}
	c.mu.Unlock()
	for _, id := range due {
		if c.reserveCondition(ctx, id, false) {
			for _, key := range keys {
				if key.ID == id.KeyID {
					return key, nil
				}
			}
		}
	}
	c.mu.Lock()
	adaptive := c.config.load().RouteSelectionEnabled
	baseline := 0.0
	if adaptive {
		for _, key := range keys {
			if r := c.routes[routeID{provider, model, key.ID}]; r != nil && r.LatencyMS > 0 && c.weightLocked(r.routeID, now) > 0 && (baseline == 0 || r.LatencyMS < baseline) {
				baseline = r.LatencyMS
			}
		}
	}
	weight := func(key schemas.Key) float64 {
		id := routeID{provider, model, key.ID}
		if c.holdLocked(id, now) != nil {
			return 0
		}
		if !adaptive {
			if key.Weight > 0 && !math.IsInf(key.Weight, 0) && !math.IsNaN(key.Weight) {
				return key.Weight
			}
			return 0
		}
		w := c.weightLocked(id, now)
		if r := c.routes[id]; r != nil && baseline > 0 {
			w /= math.Sqrt(math.Max(1, r.LatencyMS/baseline))
		}
		return w
	}
	total, maxWeight := 0.0, 0.0
	for _, key := range keys {
		w := weight(key)
		total += w
		maxWeight = max(maxWeight, w)
	}
	scale := 1.0
	if math.IsInf(total, 1) {
		scale = 1 / maxWeight
		total = 0
		for _, key := range keys {
			total += weight(key) * scale
		}
	}
	if total == 0 {
		if !adaptive {
			count, selected := 0, 0
			for i, key := range keys {
				if c.holdLocked(routeID{provider, model, key.ID}, now) == nil {
					count++
					if rand.IntN(count) == 0 {
						selected = i
					}
				}
			}
			if count > 0 {
				c.mu.Unlock()
				return keys[selected], nil
			}
		}
		selected := 0
		var earliest time.Time
		for i, key := range keys {
			if h := c.holdLocked(routeID{provider, model, key.ID}, now); h != nil && (earliest.IsZero() || h.HoldUntil.Before(earliest)) {
				earliest = h.HoldUntil
				selected = i
			}
		}
		c.mu.Unlock()
		c.reserveCondition(ctx, routeID{provider, model, keys[selected].ID}, true)
		ctx.AppendRoutingEngineLog(schemas.RoutingEngineLoadbalancing, schemas.LogLevelInfo, "Every eligible key is unavailable; native fail-open sends a real attempt.")
		return keys[selected], nil
	}
	draw := rand.Float64() * total
	selected := len(keys) - 1
	for i, key := range keys {
		draw -= weight(key) * scale
		if draw < 0 {
			selected = i
			break
		}
	}
	key := keys[selected]
	c.mu.Unlock()
	ctx.AppendRoutingEngineLog(schemas.RoutingEngineLoadbalancing, schemas.LogLevelInfo, fmt.Sprintf("Selected key %s for %s/%s by %s; held keys were skipped.", key.Name, provider, model, map[bool]string{true: "observed health, relative latency and load", false: "configured weight"}[adaptive]))
	return key, nil
}

// BeginProviderAttempt observes actual provider calls, including retries and
// streaming teardown. Direct caller credentials never enter managed-key health.
func (c *Controller) BeginProviderAttempt(ctx *schemas.BifrostContext, info schemas.ProviderAttemptInfo) schemas.ProviderAttempt {
	if !c.enabled.Load() || info.Model == "" || ctx.Value(schemas.BifrostContextKeyDirectKey) != nil {
		return nil
	}
	c.mu.Lock()
	currentProvider := info.ProviderRevision == c.providerRevisions[info.Provider]
	c.mu.Unlock()
	if !currentProvider {
		return nil
	}
	keyless := info.KeyID == "" && c.keyless != nil && c.keyless(info.Provider)
	if info.KeyID == "" && !keyless {
		return nil
	}
	key := info.Key
	if key.ID == "" {
		key = schemas.Key{ID: info.KeyID}
		if c.keys != nil {
			if keys, err := c.keys(ctx, info.Provider); err == nil {
				for _, k := range keys {
					if k.ID == info.KeyID {
						key = k
						break
					}
				}
			}
		}
	}
	if !keyless && !c.syncKey(info.Provider, key) {
		return nil
	}
	now := c.now()
	id := routeID{info.Provider, info.Model, info.KeyID}
	c.mu.Lock()
	defer c.mu.Unlock()
	if info.ProviderRevision != c.providerRevisions[info.Provider] {
		return nil
	}
	r := c.routeLocked(id, info.KeyName, now)
	if r == nil {
		return nil
	}
	k := c.keyLocked(id)
	if keyless {
		k.revision = "keyless"
	}
	r.KeyRevision = k.revision
	r.Inflight++
	r.LastSeen = now
	a := &attempt{controller: c, ctx: ctx, route: r, start: now, key: k, keyEpoch: k.epoch, modelEpoch: r.Epoch, providerRevision: info.ProviderRevision}
	if i := c.incidentLocked(routeID{Provider: id.Provider, KeyID: id.KeyID}); i != nil && i.State == "recovering" && r.Samples == 0 {
		r.Recovering = true
	}
	if h := c.holdLocked(id, now); h != nil {
		a.holdID = h.ID
	}
	if lease, ok := ctx.Value(probeContextKey{}).(probeReservation); ok && lease.Provider == info.Provider && lease.Model == info.Model && lease.KeyID == info.KeyID {
		a.probeID = lease.token
	}
	return a
}

type attempt struct {
	controller                    *Controller
	ctx                           *schemas.BifrostContext
	route                         *route
	start                         time.Time
	mu                            sync.Mutex
	done                          bool
	first                         time.Duration
	failure                       schemas.FailureClass
	retryAfter                    int64
	observed                      bool
	hasResponse                   bool
	ignore                        bool
	key                           *keyState
	keyEpoch, modelEpoch, probeID uint64
	holdID                        string
	providerRevision              string
}

func (a *attempt) Observe(result *schemas.BifrostResponse, err *schemas.BifrostError) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.done {
		return
	}
	if result != nil && !a.hasResponse {
		a.first = a.controller.now().Sub(a.start)
		// Native provider latency excludes local injected-tool work. Prefer it
		// when present; the first stream response stays the latency sample.
		if extra := result.GetExtraFields(); extra != nil && extra.Latency > 0 {
			a.first = time.Duration(extra.Latency) * time.Millisecond
		}
		a.hasResponse = true
		a.observed = true
	}
	if err != nil {
		a.failure = bifrost.ClassifyFailure(err)
		a.retryAfter = err.ExtraFields.RetryAfter
		a.observed = true
		a.ignore = err.IsBifrostError && a.failure == schemas.FailureClassUnknown
	}
}

func (a *attempt) End() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.done {
		return
	}
	a.done = true
	now := a.controller.now()
	failure := a.failure
	// A caller's deadline or abandoned stream says nothing about this key.
	// Provider/first-token timeouts arrive as a classified provider error above.
	if a.ctx.Err() != nil {
		failure = schemas.FailureClassCancelled
	}
	if !a.observed || a.ignore {
		failure = schemas.FailureClassCancelled
	}
	latency := a.first
	if !a.hasResponse {
		latency = now.Sub(a.start)
	}
	a.controller.complete(a, failure, a.retryAfter, latency, now)
}

func holdDuration(class schemas.FailureClass, failures int, retryAfter int64) time.Duration {
	var cap time.Duration
	switch class {
	case schemas.FailureClassCredential, schemas.FailureClassRegionBlocked:
		cap = 15 * time.Minute
	case schemas.FailureClassQuota, schemas.FailureClassModelAccess:
		cap = 5 * time.Minute
	case schemas.FailureClassModelGone:
		cap = 30 * time.Minute
	case schemas.FailureClassRateLimit:
		cap = 2 * time.Minute
	default:
		return 30 * time.Second
	}
	if retryAfter > 0 {
		return max(10*time.Second, time.Duration(min(retryAfter, int64((24*time.Hour)/time.Millisecond)))*time.Millisecond)
	}
	return time.Duration(float64(min(30*time.Second*time.Duration(1<<min(max(0, failures-1), 12)), cap)) * (0.9 + rand.Float64()*0.2))
}

func (c *Controller) complete(a *attempt, failure schemas.FailureClass, retryAfter int64, latency time.Duration, now time.Time) {
	r := a.route
	c.mu.Lock()
	r.Inflight--
	if c.routes[r.routeID] != r || c.keyLocked(r.routeID) != a.key || a.key.epoch != a.keyEpoch || r.Epoch != a.modelEpoch || c.providerRevisions[r.Provider] != a.providerRevision {
		c.mu.Unlock()
		return
	}
	r.LastSeen = now
	if r.ProbeID == a.probeID && a.probeID != 0 {
		r.ProbeUntil = time.Time{}
		r.ProbeID = 0
	}
	if failure == schemas.FailureClassCancelled || failure == schemas.FailureClassCallerFault {
		c.mu.Unlock()
		if a.holdID != "" {
			c.completeConditions(a, failure, retryAfter, now)
		}
		return
	}
	before := c.stateLocked(r, now)
	r.Samples++
	sample := 0.0
	if failure != "" {
		sample = 1
	}
	if r.Samples == 1 {
		r.ErrorRate = sample
	} else {
		r.ErrorRate += decay * (sample - r.ErrorRate)
	}
	if failure == "" {
		measured := float64(latency) / float64(time.Millisecond)
		if r.LatencyMS == 0 {
			r.LatencyMS = measured
		} else {
			r.LatencyMS += decay * (measured - r.LatencyMS)
		}
		r.Consecutive = 0
		r.Recoveries++
		r.FailedUntil = time.Time{}
		if r.Recoveries >= 5 {
			r.Recovering = false
			r.FailureClass = ""
		}
	} else {
		r.Consecutive++
		r.Recoveries = 0
		r.FailureClass = failure
		if (!failure.IsPerKey() || r.KeyID == "") && r.Consecutive >= 3 {
			r.FailedUntil = now.Add(30 * time.Second)
			r.Recovering = true
		}
	}
	after := c.stateLocked(r, now)
	incidentNeedsUpdate := false
	scopes := [3]routeID{r.routeID, {Provider: r.Provider, KeyID: r.KeyID}, {Provider: r.Provider, Model: r.Model}}
	for _, scope := range scopes {
		if i := c.incidents[scope]; i != nil {
			incidentNeedsUpdate = incidentNeedsUpdate || i.Status == "waiting_for_traffic"
			i.LastSeen = now
			if request := requestID(a.ctx); request != "" {
				i.LastRequestID = request
			}
		}
	}
	c.mu.Unlock()
	if r.KeyID != "" && (failure.IsPerKey() || a.holdID != "") {
		c.completeConditions(a, failure, retryAfter, now)
	}
	if before != after || incidentNeedsUpdate {
		c.updateHealthIncident(r, requestID(a.ctx), now)
	}
	if r.KeyID != "" && (before != after || incidentNeedsUpdate || failure.IsPerKey() || a.holdID != "") {
		c.updateProviderIncident(r.Provider, r.Model, requestID(a.ctx), now)
	}
	if failure != "" {
		a.ctx.AppendRoutingEngineLog(schemas.RoutingEngineLoadbalancing, schemas.LogLevelInfo, fmt.Sprintf("Attempt on %s/%s key %s failed as %s.", r.Provider, r.Model, r.KeyName, failure))
	}
}

func (c *Controller) stateLocked(r *route, now time.Time) string {
	switch {
	case c.holdLocked(r.routeID, now) != nil:
		return "held"
	case r.FailedUntil.After(now):
		return "failed"
	case r.Recovering && r.Recoveries < 5:
		return "recovering"
	case r.Samples == 0:
		return "unknown"
	case r.ErrorRate > 0.05:
		return "degraded"
	}
	return "healthy"
}

func (c *Controller) statusLocked(r *route, now time.Time) RouteStatus {
	status := RouteStatus{Provider: r.Provider, Model: r.Model, KeyID: r.KeyID, KeyName: r.KeyName, State: c.stateLocked(r, now), Score: 100 * c.weightLocked(r.routeID, now), Samples: r.Samples, ErrorRate: r.ErrorRate, LatencyMS: r.LatencyMS, Inflight: r.Inflight, LastSeen: r.LastSeen, FailureClass: r.FailureClass}
	if h := c.holdLocked(r.routeID, now); h != nil {
		until := h.HoldUntil
		status.HoldUntil = &until
		status.FailureClass = h.HoldClass
	}
	return status
}

func (c *Controller) Status() []RouteStatus {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]RouteStatus, 0, len(c.routes)+len(c.conditions))
	seen := make(map[routeID]bool)
	for id, r := range c.routes {
		result = append(result, c.statusLocked(r, now))
		seen[id] = true
		if c.wholeKeyHoldLocked(id, now) != nil {
			seen[routeID{Provider: id.Provider, KeyID: id.KeyID}] = true
		}
	}
	for id, h := range c.conditions {
		if !seen[id] {
			until := h.HoldUntil
			result = append(result, RouteStatus{Provider: id.Provider, Model: id.Model, KeyID: id.KeyID, KeyName: h.KeyName, State: "held", HoldUntil: &until, FailureClass: h.HoldClass, LastSeen: h.HeldAt})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		if a.Model != b.Model {
			return a.Model < b.Model
		}
		return a.KeyID < b.KeyID
	})
	return result
}

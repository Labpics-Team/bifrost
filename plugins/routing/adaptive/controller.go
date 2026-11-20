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
	maxEvents = 4096
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
	KeyName      string               `json:"key_name"`
	Samples      uint64               `json:"samples"`
	ErrorRate    float64              `json:"error_rate"`
	LatencyMS    float64              `json:"latency_ms"`
	Inflight     int                  `json:"inflight"`
	Consecutive  int                  `json:"consecutive_failures"`
	Recoveries   int                  `json:"recoveries"`
	LastSeen     time.Time            `json:"last_seen"`
	HoldUntil    time.Time            `json:"hold_until,omitempty"`
	ProbeUntil   time.Time            `json:"-"`
	HeldAt       time.Time            `json:"held_at,omitempty"`
	Rung         int                  `json:"rung"`
	Recovering   bool                 `json:"recovering"`
	FailedUntil  time.Time            `json:"failed_until,omitempty"`
	FailureClass schemas.FailureClass `json:"failure_class,omitempty"`
	HoldClass    schemas.FailureClass `json:"hold_class,omitempty"`
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

type Event struct {
	Timestamp time.Time `json:"timestamp"`
	RouteStatus
}

// Controller retains numeric state only, bounded to maxRoutes key/model pairs
// and maxEvents transitions. It never retains prompts or credential values.
type Controller struct {
	store     configstore.ConfigStore
	saveMu    sync.Mutex
	enabled   atomic.Bool
	startOnce sync.Once
	stop      context.CancelFunc
	wg        sync.WaitGroup
	config    configValue
	mu        sync.Mutex
	routes    map[routeID]*route
	events    []Event
	eventNext int
	now       func() time.Time
	keys      func(context.Context, schemas.ModelProvider) ([]schemas.Key, error)
	providers func(string) []schemas.ModelProvider
	supports  func(schemas.ModelProvider, schemas.Key, string) bool
}

func New(config Config) *Controller {
	c := &Controller{routes: make(map[routeID]*route), now: time.Now}
	c.config.store(config)
	c.enabled.Store(true)
	return c
}

// SetCatalog is called once before the server accepts requests. The callbacks
// read the live native catalog and eligible configured key pool.
func (c *Controller) SetCatalog(providers func(string) []schemas.ModelProvider, keys func(context.Context, schemas.ModelProvider) ([]schemas.Key, error), supports func(schemas.ModelProvider, schemas.Key, string) bool) {
	c.providers, c.keys, c.supports = providers, keys, supports
}

func (c *Controller) Config() Config { return c.config.load() }
func (c *Controller) Enabled() bool  { return c.enabled.Load() }
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

func (c *Controller) wholeKeyHoldLocked(id routeID, now time.Time) *route {
	r := c.routes[routeID{Provider: id.Provider, KeyID: id.KeyID}]
	if r != nil && !r.HoldUntil.IsZero() {
		return r
	}
	return nil
}

func (c *Controller) holdLocked(id routeID, now time.Time) *route {
	if whole := c.wholeKeyHoldLocked(id, now); whole != nil {
		return whole
	}
	if r := c.routes[id]; r != nil && !r.HoldUntil.IsZero() {
		return r
	}
	return nil
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
	if !c.enabled.Load() || len(keys) < 2 {
		return keyselectors.WeightedRandom(ctx, keys, provider, model)
	}
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if reserved, ok := ctx.Value(probeContextKey{}).(routeID); ok && reserved.Provider == provider {
		ctx.ClearValue(probeContextKey{})
		for _, key := range keys {
			if key.ID == reserved.KeyID {
				return key, nil
			}
		}
	}
	adaptive := c.config.load().RouteSelectionEnabled
	// The first real request after a hold expires checks it. Reserving here,
	// before the provider starts, prevents a burst of simultaneous checks.
	for _, key := range keys {
		if h := c.holdLocked(routeID{provider, model, key.ID}, now); h != nil && !h.HoldUntil.After(now) && !h.ProbeUntil.After(now) {
			h.ProbeUntil = now.Add(5 * time.Minute)
			return key, nil
		}
	}
	// Compare latency within this model's eligible pool, never against a fixed
	// millisecond target (a long-generation model is not an unhealthy model).
	baseline := 0.0
	if adaptive {
		for _, key := range keys {
			if r := c.routes[routeID{provider, model, key.ID}]; r != nil && r.LatencyMS > 0 && c.weightLocked(r.routeID, now) > 0 {
				if baseline == 0 || r.LatencyMS < baseline {
					baseline = r.LatencyMS
				}
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
	total := 0.0
	maxWeight := 0.0
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
		// Zero static weights mean uniform selection among non-held keys,
		// matching Bifrost's weighted selector.
		if !adaptive {
			count := 0
			selected := 0
			for i, key := range keys {
				if c.holdLocked(routeID{provider, model, key.ID}, now) == nil {
					count++
					if rand.IntN(count) == 0 {
						selected = i
					}
				}
			}
			if count > 0 {
				return keys[selected], nil
			}
		}
		// Every key is held: keep Bifrost's fail-open behavior and let a real
		// request tell us whether the earliest held credential has recovered.
		selected := 0
		var earliest time.Time
		for i, key := range keys {
			id := routeID{provider, model, key.ID}
			r := c.routes[id]
			if whole := c.wholeKeyHoldLocked(id, now); whole != nil {
				r = whole
			}
			if r != nil && (earliest.IsZero() || r.HoldUntil.Before(earliest)) {
				earliest, selected = r.HoldUntil, i
			}
		}
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
	return keys[selected], nil
}

// BeginProviderAttempt observes actual provider calls, including retries and
// streaming teardown. Direct caller credentials never enter managed-key health.
func (c *Controller) BeginProviderAttempt(ctx *schemas.BifrostContext, info schemas.ProviderAttemptInfo) schemas.ProviderAttempt {
	if !c.enabled.Load() || info.KeyID == "" || info.Model == "" || ctx.Value(schemas.BifrostContextKeyDirectKey) != nil {
		return nil
	}
	now := c.now()
	id := routeID{info.Provider, info.Model, info.KeyID}
	c.mu.Lock()
	r := c.routeLocked(id, info.KeyName, now)
	if r != nil {
		r.Inflight++
		r.LastSeen = now
	}
	c.mu.Unlock()
	if r == nil {
		return nil
	}
	return &attempt{controller: c, ctx: ctx, route: r, start: now}
}

type attempt struct {
	controller  *Controller
	ctx         *schemas.BifrostContext
	route       *route
	start       time.Time
	mu          sync.Mutex
	done        bool
	first       time.Duration
	failure     schemas.FailureClass
	retryAfter  int64
	observed    bool
	hasResponse bool
	ignore      bool
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
	a.controller.complete(a.route, failure, a.retryAfter, latency, a.start, now)
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

func (c *Controller) complete(r *route, failure schemas.FailureClass, retryAfter int64, latency time.Duration, started, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r.Inflight--
	// A provider/key edit discarded this generation while the call was in
	// flight. Its old answer must not hold the replacement credential.
	if c.routes[r.routeID] != r {
		return
	}
	r.LastSeen = now
	if failure == schemas.FailureClassCancelled || failure == schemas.FailureClassCallerFault {
		r.ProbeUntil = time.Time{}
		if h := c.holdLocked(r.routeID, now); h != nil && !h.HoldUntil.After(started) {
			h.ProbeUntil = time.Time{}
		}
		return
	}
	r.ProbeUntil = time.Time{}
	before := c.stateLocked(r, now)
	failed := failure != ""
	r.Samples++
	errorSample := 0.0
	if failed {
		errorSample = 1
	}
	if r.Samples == 1 {
		r.ErrorRate = errorSample
	} else {
		r.ErrorRate += decay * (errorSample - r.ErrorRate)
	}
	if !failed {
		if r.LatencyMS == 0 {
			r.LatencyMS = float64(latency) / float64(time.Millisecond)
		} else {
			r.LatencyMS += decay * (float64(latency)/float64(time.Millisecond) - r.LatencyMS)
		}
	}
	// A real check can release a different, earlier refusal (e.g. a 429 proves
	// that a formerly rejected credential now authenticates).
	if h := c.holdLocked(r.routeID, now); h != nil && (failure == "" || !h.HoldUntil.After(started)) {
		if failure == "" || (failure.IsPerKey() && failure != h.HoldClass) {
			h.HoldUntil = time.Time{}
			h.ProbeUntil = time.Time{}
			h.Recovering = true
			h.Recoveries = 0
			r.Recovering = true
			r.Recoveries = 0
		} else if !failure.IsPerKey() {
			h.HoldUntil = now.Add(holdDuration(h.HoldClass, h.Rung, 0))
			h.ProbeUntil = time.Time{}
		}
	}
	if failed {
		r.Consecutive++
		r.Recoveries = 0
		if failure.IsPerKey() {
			h := r
			if failure.CoversAllModels() {
				h = c.routeLocked(routeID{Provider: r.Provider, KeyID: r.KeyID}, r.KeyName, now)
			}
			if h != nil && (h.HoldUntil.IsZero() || !h.HoldUntil.After(started)) {
				if h.HoldClass != failure || now.Sub(h.HeldAt) > 10*time.Minute {
					h.Rung = 0
				}
				h.Rung++
				h.HeldAt = now
				h.HoldUntil = now.Add(holdDuration(failure, h.Rung, retryAfter))
				h.ProbeUntil = time.Time{}
				h.HoldClass = failure
				h.LastSeen = now
			}
		} else if r.Consecutive >= 3 {
			r.FailedUntil = now.Add(30 * time.Second)
			r.Recovering = true
		}
		r.FailureClass = failure
	} else {
		r.Consecutive = 0
		r.Recoveries++
		r.FailedUntil = time.Time{}
		if r.Recoveries >= 5 {
			r.Recovering = false
			r.FailureClass = ""
		}
	}
	if before != c.stateLocked(r, now) && c.config.load().HistoryEnabled {
		event := Event{Timestamp: now, RouteStatus: c.statusLocked(r, now)}
		if len(c.events) < maxEvents {
			c.events = append(c.events, event)
		} else {
			c.events[c.eventNext] = event
			c.eventNext = (c.eventNext + 1) % maxEvents
		}
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
	until, failure := r.HoldUntil, r.FailureClass
	if !until.IsZero() {
		failure = r.HoldClass
	}
	if whole := c.wholeKeyHoldLocked(r.routeID, now); whole != nil {
		until, failure = whole.HoldUntil, whole.HoldClass
	}
	status := RouteStatus{Provider: r.Provider, Model: r.Model, KeyID: r.KeyID, KeyName: r.KeyName,
		State: c.stateLocked(r, now), Score: 100 * c.weightLocked(r.routeID, now), Samples: r.Samples, ErrorRate: r.ErrorRate,
		LatencyMS: r.LatencyMS, Inflight: r.Inflight, LastSeen: r.LastSeen, FailureClass: failure}
	if !until.IsZero() {
		status.HoldUntil = &until
	}
	return status
}

func (c *Controller) Status() []RouteStatus {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]RouteStatus, 0, len(c.routes))
	seen := make(map[routeID]bool)
	for id, r := range c.routes {
		if r.Inflight == 0 && now.Sub(r.LastSeen) > idleTTL {
			delete(c.routes, id)
			continue
		}
		if r.Model != "" {
			result = append(result, c.statusLocked(r, now))
			seen[routeID{Provider: r.Provider, KeyID: r.KeyID}] = true
		}
	}
	// A restored whole-key hold is visible before any model has sent traffic.
	for id, r := range c.routes {
		if id.Model == "" && !seen[id] && !r.HoldUntil.IsZero() {
			result = append(result, c.statusLocked(r, now))
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

func (c *Controller) History() []Event {
	cutoff := c.now().Add(-time.Duration(c.config.load().HistoryRetentionDays) * 24 * time.Hour)
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]Event, 0, len(c.events))
	for _, event := range c.events {
		if event.Timestamp.After(cutoff) {
			result = append(result, event)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp.After(result[j].Timestamp) })
	return result
}

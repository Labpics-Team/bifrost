package adaptive

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
)

// Conditions are decisions with a lifecycle, never entries in the telemetry
// cache. Their identity fences completions from a previous refusal/check.
type condition struct {
	routeID
	ID, KeyRevision, IncidentID, KeyName string
	HoldUntil, HeldAt                    time.Time
	Rung                                 int
	HoldClass                            schemas.FailureClass
	ProbeID                              uint64
	ProbeUntil                           time.Time
}

// A released condition is no longer a hold. Retain only the short
// re-hold ladder window, scoped to the same credential and refusal.
type releasedHold struct {
	class    schemas.FailureClass
	rung     int
	revision string
	at       time.Time
}

const reholdWindow = 10 * time.Minute

func (c *Controller) rememberReleasedLocked(h *condition, now time.Time) {
	// Recent releases are disposable telemetry, unlike unresolved holds.
	// Keep the newest window bounded even if callers use many model names.
	if _, exists := c.releasedHolds[h.routeID]; !exists && len(c.releasedHolds) >= maxRoutes {
		var oldest routeID
		var at time.Time
		for id, previous := range c.releasedHolds {
			if at.IsZero() || previous.at.Before(at) {
				oldest, at = id, previous.at
			}
		}
		delete(c.releasedHolds, oldest)
	}
	c.releasedHolds[h.routeID] = releasedHold{class: h.HoldClass, rung: h.Rung, revision: h.KeyRevision, at: now}
}

func (c *Controller) reholdRungLocked(id routeID, class schemas.FailureClass, revision string, now time.Time) int {
	if previous, ok := c.releasedHolds[id]; ok && previous.class == class && previous.revision == revision && now.Sub(previous.at) < reholdWindow {
		return previous.rung
	}
	return 1
}

func (c *Controller) forgetReleasedLocked(provider schemas.ModelProvider, keyID, model string) {
	for id := range c.releasedHolds {
		if (provider == "" || id.Provider == provider) && (keyID == "" || id.KeyID == keyID) && (model == "" || id.Model == model) {
			delete(c.releasedHolds, id)
		}
	}
}

type keyState struct {
	snapshot         *schemas.Key
	revision         string
	providerRevision string
	epoch            uint64
}
type probeReservation struct {
	routeID
	conditionID  string
	token        uint64
	providerOnly bool
}

func (c *Controller) reservationCurrentLocked(reserved probeReservation, now time.Time) bool {
	if reserved.token == 0 {
		return false
	}
	if reserved.providerOnly {
		return c.providerChecks[reserved.routeID].token == reserved.token
	}
	if h := c.holdLocked(reserved.routeID, now); h != nil {
		return h.ID == reserved.conditionID && h.ProbeID == reserved.token
	}
	r := c.routes[reserved.routeID]
	return reserved.conditionID == "" && r != nil && r.ProbeID == reserved.token && !r.ProbeUntil.IsZero()
}

type transition struct {
	advance   bool
	upsert    []*condition
	remove    []*condition
	incidents []*Incident
	events    []IncidentEvent
	publish   func()
}

func (c *Controller) persistenceResultLocked(err error) {
	c.persistence.Healthy = err == nil && len(c.pendingResets) == 0
	if err != nil {
		c.persistence.LastError = err.Error()
		return
	}
	now := c.now()
	c.persistence.LastSuccessAt = &now
	c.persistence.LastError = ""
	if len(c.pendingResets) != 0 {
		c.persistence.LastError = "Native configuration changed; adaptive reconciliation is pending."
	}
}

func (t transition) durable() bool {
	return t.advance || len(t.upsert)+len(t.remove)+len(t.incidents)+len(t.events) != 0
}

// transition admits observation-only updates without waiting for the durable
// writer. A real transition is rebuilt after serial admission; build may run
// twice and must defer lifecycle publication to publish.
var errLifecycleBusy = errors.New("adaptive lifecycle writer is busy")

func (c *Controller) transition(ctx context.Context, build func() transition) error {
	return c.transitionWithAdmission(ctx, build, true)
}

// Observations must not queue a healthy request behind an unrelated hold
// transaction. A contended diagnostic is retried by the maintenance worker.
func (c *Controller) tryDiagnosticTransition(ctx context.Context, build func() transition) error {
	return c.transitionWithAdmission(ctx, build, false)
}

func (c *Controller) transitionWithAdmission(ctx context.Context, build func() transition, wait bool) error {
	c.mu.Lock()
	t := build()
	if !t.durable() {
		if t.publish != nil {
			t.publish()
		}
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if wait {
		c.lifeMu.Lock()
	} else if !c.lifeMu.TryLock() {
		return errLifecycleBusy
	}
	defer c.lifeMu.Unlock()
	c.mu.Lock()
	t = build()
	if !t.durable() {
		if t.publish != nil {
			t.publish()
		}
		c.mu.Unlock()
		return nil
	}
	revision := c.revision
	c.mu.Unlock()
	m := configstore.AdaptiveMutation{ExpectedRevision: revision, Revision: revision + 1}
	for _, h := range t.remove {
		m.DeleteConditionIDs = append(m.DeleteConditionIDs, h.ID)
	}
	for _, h := range t.upsert {
		m.UpsertConditions = append(m.UpsertConditions, configstore.AdaptiveCondition{ID: h.ID, Provider: string(h.Provider), Model: h.Model, KeyID: h.KeyID, KeyRevision: h.KeyRevision, IncidentID: h.IncidentID, FailureClass: string(h.HoldClass), HoldUntil: h.HoldUntil, HeldAt: h.HeldAt, Rung: h.Rung})
	}
	for _, i := range t.incidents {
		m.UpsertIncidents = append(m.UpsertIncidents, *i)
	}
	m.Events = t.events
	var err error
	if c.stateStore != nil {
		err = c.stateStore.Apply(ctx, m)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.persistenceResultLocked(err)
		return err
	}
	c.revision = m.Revision
	for _, h := range t.remove {
		delete(c.conditions, h.routeID)
		c.bumpScopeLocked(h.routeID)
	}
	for _, h := range t.upsert {
		c.conditions[h.routeID] = h
		c.bumpScopeLocked(h.routeID)
	}
	for _, i := range t.incidents {
		if current := c.incidents[incidentRoute(i)]; current != nil && current.LastSeen.After(i.LastSeen) {
			i.LastSeen = current.LastSeen
			i.LastRequestID = current.LastRequestID
		}
		if i.ClosedAt != nil {
			delete(c.incidents, incidentRoute(i))
		} else {
			c.incidents[incidentRoute(i)] = i
		}
	}
	if t.publish != nil {
		t.publish()
	}
	c.persistenceResultLocked(nil)
	return nil
}

func (c *Controller) bumpScopeLocked(id routeID) {
	if id.Model != "" {
		if r := c.routes[id]; r != nil {
			r.Epoch++
		}
		return
	}
	kid := routeID{Provider: id.Provider, KeyID: id.KeyID}
	if k := c.keyStates[kid]; k != nil {
		k.epoch++
	}
}

func (c *Controller) keyLocked(id routeID) *keyState {
	kid := routeID{Provider: id.Provider, KeyID: id.KeyID}
	k := c.keyStates[kid]
	if k == nil {
		k = &keyState{}
		c.keyStates[kid] = k
	}
	return k
}

func incidentRoute(i *Incident) routeID {
	return routeID{Provider: schemas.ModelProvider(i.Provider), Model: i.Model, KeyID: i.KeyID}
}
func (c *Controller) incidentLocked(id routeID) *Incident {
	if source := c.incidents[id]; source != nil {
		copy := *source
		return &copy
	}
	return nil
}

// Each scope has one mutable incident copy within a lifecycle transaction.
// A change of refusal class can visit that same scope more than once.
func (t *transition) incidentLocked(c *Controller, id routeID) *Incident {
	for _, i := range t.incidents {
		if incidentRoute(i) == id {
			return i
		}
	}
	i := c.incidentLocked(id)
	if i != nil {
		t.incidents = append(t.incidents, i)
	}
	return i
}

func (c *Controller) newIncidentLocked(id routeID, name string, now time.Time, request string) *Incident {
	scope := "key_model"
	if id.Model == "" {
		scope = "key"
	}
	if id.KeyID == "" {
		scope = "provider_model"
	}
	keyRevision := ""
	if key := c.keyStates[routeID{Provider: id.Provider, KeyID: id.KeyID}]; key != nil {
		keyRevision = key.revision
	}
	return &Incident{ID: uuid.NewString(), Scope: scope, Provider: string(id.Provider), ProviderRevision: c.providerRevisions[id.Provider], Model: id.Model, KeyID: id.KeyID, KeyRevision: keyRevision, KeyName: name, OpenedAt: now, UpdatedAt: now, LastSeen: now, FirstRequestID: request, LastRequestID: request}
}

func incidentEvent(i *Incident, kind, request, message string, now time.Time) IncidentEvent {
	return IncidentEvent{ID: uuid.NewString(), IncidentID: i.ID, Timestamp: now, Kind: kind, State: i.State, FailureClass: i.FailureClass, RequestID: request, Message: message}
}

func setIncidentState(i *Incident, state string, now time.Time, request string) {
	i.LastSeen = now
	if request != "" {
		i.LastRequestID = request
		if i.FirstRequestID == "" {
			i.FirstRequestID = request
		}
	}
	setIncidentLifecycleState(i, state, now)
}

// Lifecycle time is separate from the last real observation. A cooldown can
// reopen admission, but cannot supply request evidence or establish recovery.
func setIncidentLifecycleState(i *Incident, state string, now time.Time) {
	i.State = state
	i.UpdatedAt = now
	rank := map[string]int{"unknown": 0, "healthy": 0, "recovering": 1, "degraded": 2, "failed": 3, "held": 4}
	if rank[state] >= rank[i.WorstState] {
		i.WorstState = state
	}
	switch state {
	case "held":
		i.Status = "mitigated"
		i.Summary = "The provider refused this key."
		i.Action = "The key is held until a real request checks it."
		if i.FailureClass == string(schemas.FailureClassCredential) || i.FailureClass == string(schemas.FailureClassQuota) {
			i.Status = "needs_attention"
			i.Action = "Check the credential or quota in the provider settings, or release it to check now."
		}
	case "recovering":
		i.Status = "recovering"
		i.Summary = "The route is earning its traffic back."
		i.Action = "Successful real requests restore its share."
	case "healthy":
		i.Status = "resolved"
		i.Summary = "The route serves normally again."
		i.Action = ""
		i.ResolvedAt = &now
		i.ClosedAt = &now
	case "failed":
		i.Status = "mitigated"
		i.Summary = "The route failed repeatedly."
		i.Action = "Healthy alternatives receive traffic while this route is checked."
	default:
		i.Status = "mitigated"
		i.Summary = "The route is returning errors or responding more slowly than usual."
		i.Action = "Its traffic share is reduced while observations recover."
	}
}

func (c *Controller) reserveCondition(ctx *schemas.BifrostContext, id routeID, force bool) bool {
	// A lease is disposable admission state, not a durable lifecycle transition.
	// Skip an optional check while a transition is in progress; healthy traffic
	// never queues behind that writer. Persist the actual check with its outcome.
	if !c.lifeMu.TryLock() {
		return false
	}
	defer c.lifeMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	h := c.holdLocked(id, now)
	if h == nil || h.ProbeUntil.After(now) || (!force && h.HoldUntil.After(now)) {
		return false
	}
	copy := *h
	c.probeSequence++
	copy.ProbeID = c.probeSequence
	copy.ProbeUntil = now.Add(5 * time.Minute)
	c.conditions[h.routeID] = &copy
	c.setProbeReservationLocked(ctx, probeReservation{routeID: id, conditionID: h.ID, token: copy.ProbeID})
	return true
}

func requestID(ctx *schemas.BifrostContext) string {
	id, _ := ctx.Value(schemas.BifrostContextKeyRequestID).(string)
	return id
}

func (c *Controller) attemptCurrentLocked(a *attempt) bool {
	r := a.route
	return c.routes[r.routeID] == r && c.keyStates[routeID{Provider: r.Provider, KeyID: r.KeyID}] == a.key && a.key.epoch == a.keyEpoch && r.Epoch == a.modelEpoch && c.providerRevisions[r.Provider] == a.providerRevision &&
		(a.probeID == 0 || a.holdID != "" || r.ProbeID == a.probeID)
}

func (c *Controller) releaseCompletedLeaseLocked(a *attempt) {
	if !c.attemptCurrentLocked(a) || a.probeID == 0 {
		return
	}
	h := c.holdLocked(a.route.routeID, c.now())
	if h == nil || h.ID != a.holdID || h.ProbeID != a.probeID {
		return
	}
	copy := *h
	copy.ProbeID = 0
	copy.ProbeUntil = time.Time{}
	c.conditions[h.routeID] = &copy
}

// Only this attempt's committed transition may advance its observation fence.
// A concurrent release/reset must invalidate all later incident updates too.
func followAttemptTransition(t transition, a *attempt) transition {
	publish := t.publish
	t.publish = func() {
		if publish != nil {
			publish()
		}
		a.keyEpoch = a.key.epoch
		a.modelEpoch = a.route.Epoch
	}
	return t
}

func (c *Controller) completeConditions(a *attempt, failure schemas.FailureClass, retryAfter int64, now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.transition(ctx, func() transition {
		r := a.route
		if !c.attemptCurrentLocked(a) {
			return transition{}
		}
		h := c.holdLocked(r.routeID, now)
		if h != nil && (h.ID != a.holdID || (h.ProbeID != 0 && h.ProbeID != a.probeID)) {
			return transition{}
		}
		if failure == schemas.FailureClassCancelled || failure == schemas.FailureClassCallerFault {
			if h == nil || h.ProbeID != a.probeID || a.probeID == 0 {
				return transition{}
			}
			copy := *h
			copy.ProbeID = 0
			copy.ProbeUntil = time.Time{}
			return transition{publish: func() { c.conditions[h.routeID] = &copy }}
		}
		t := transition{}
		request := requestID(a.ctx)
		if h != nil {
			if failure != "" && h.HoldUntil.After(a.start) && a.probeID == 0 {
				return transition{}
			}
			i := t.incidentLocked(c, h.routeID)
			if i == nil {
				return transition{}
			}
			i.Checks++
			t.events = append(t.events, incidentEvent(i, "check_started", request, "A real request checked the held key; its outcome is recorded in this transition.", a.start))
			if failure == "" || failure.IsPerKey() && failure != h.HoldClass {
				t.remove = append(t.remove, h)
				setIncidentState(i, "recovering", now, request)
				t.events = append(t.events, incidentEvent(i, "hold_released", request, "The check showed the previous refusal no longer applies.", now))
				t.publish = func() {
					c.rememberReleasedLocked(h, now)
					r.Recovering = true
					r.Recoveries = 0
				}
			} else {
				copy := *h
				copy.ID = uuid.NewString()
				copy.ProbeID = 0
				copy.ProbeUntil = time.Time{}
				if failure == h.HoldClass {
					copy.Rung++
					i.FailedChecks++
					if a.probeID == 0 {
						i.RefusedAgain++
					}
				}
				copy.HoldUntil = now.Add(holdDuration(copy.HoldClass, copy.Rung, retryAfter))
				copy.HeldAt = now
				setIncidentState(i, "held", now, request)
				t.remove = append(t.remove, h)
				t.upsert = append(t.upsert, &copy)
				t.events = append(t.events, incidentEvent(i, "held_again", request, "The check did not establish recovery; the key remains held.", now))
				return followAttemptTransition(t, a)
			}
		}
		if failure.IsPerKey() {
			id := r.routeID
			if failure.CoversAllModels() {
				id.Model = ""
			}
			i := t.incidentLocked(c, id)
			if i == nil {
				i = c.newIncidentLocked(id, r.KeyName, now, request)
				t.incidents = append(t.incidents, i)
			}
			i.FailureClass = string(failure)
			setIncidentState(i, "held", now, request)
			// A broader hold may mask an older model hold. Replacing that scope
			// deletes its prior identity in the same transaction as the new refusal.
			if previous := c.conditions[id]; previous != nil && previous != h {
				t.remove = append(t.remove, previous)
			}
			rung := c.reholdRungLocked(id, failure, r.KeyRevision, now)
			next := &condition{routeID: id, ID: uuid.NewString(), KeyRevision: r.KeyRevision, IncidentID: i.ID, KeyName: r.KeyName, HoldUntil: now.Add(holdDuration(failure, rung, retryAfter)), HeldAt: now, Rung: rung, HoldClass: failure}
			t.upsert = append(t.upsert, next)
			t.events = append(t.events, incidentEvent(i, "held", request, "The provider refused the key; ordinary traffic skips it.", now))
		}
		return followAttemptTransition(t, a)
	})
	if err != nil {
		c.mu.Lock()
		c.releaseCompletedLeaseLocked(a)
		c.mu.Unlock()
		a.ctx.AppendRoutingEngineLog(schemas.RoutingEngineLoadbalancing, schemas.LogLevelWarn, "Adaptive routing could not commit the condition; the last committed state remains in effect.")
	}
}

func (c *Controller) updateHealthIncident(a *attempt, request string, now time.Time) {
	r := a.route
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.tryDiagnosticTransition(ctx, func() transition {
		if !c.attemptCurrentLocked(a) || c.holdLocked(r.routeID, now) != nil {
			return transition{}
		}
		state := c.stateLocked(r, now)
		// A model refusal can turn into a whole-key refusal (or vice versa).
		// The same real recovery evidence then advances both open observations.
		incidents := make([]*Incident, 0, 2)
		for _, scope := range []routeID{{Provider: r.Provider, KeyID: r.KeyID}, r.routeID} {
			if i := c.incidentLocked(scope); i != nil {
				incidents = append(incidents, i)
			}
		}
		if len(incidents) == 0 {
			if !c.config.load().HistoryEnabled || state == "unknown" || state == "healthy" || len(c.incidents) >= maxOptionalIncidents {
				return transition{}
			}
			incidents = append(incidents, c.newIncidentLocked(r.routeID, r.KeyName, now, request))
		}
		t := transition{}
		for _, i := range incidents {
			scopeState := state
			if i.Model == "" && i.KeyID != "" {
				// This optional lifecycle path runs only for an open whole-key
				// incident. A real outcome for one model cannot clear its siblings.
				scope := incidentRoute(i)
				scopeState = c.keyHealthStatesLocked(map[routeID]bool{scope: true}, now)[scope].state()
				if scopeState == "" {
					continue
				}
			}
			if i.State == scopeState && i.Status != "waiting_for_traffic" {
				i.LastSeen = now
				i.LastRequestID = request
				c.incidents[incidentRoute(i)] = i
				continue
			}
			// Health recovery is evidence about the outcome, not a new cause.
			// Keep each scope's recorded refusal when several scopes recover together.
			if i.FailureClass == "" {
				i.FailureClass = string(r.FailureClass)
			}
			setIncidentState(i, scopeState, now, request)
			t.incidents = append(t.incidents, i)
			t.events = append(t.events, incidentEvent(i, "state_changed", request, fmt.Sprintf("Observed route state changed to %s.", scopeState), now))
		}
		return t
	})
	if err != nil {
		c.deferDiagnostic(a, request, now, true, false)
	}
}

func (c *Controller) updateProviderIncident(a *attempt, request string, now time.Time) {
	if c.keys == nil || c.supports == nil {
		return
	}
	provider, model := a.route.Provider, a.route.Model
	c.mu.Lock()
	if !c.attemptCurrentLocked(a) {
		c.mu.Unlock()
		return
	}
	revision, generation := c.revision, c.resetSequence
	providerRevision := c.providerRevisions[provider]
	c.mu.Unlock()
	keys, err := c.keys(context.Background(), provider)
	if err != nil {
		c.deferDiagnostic(a, request, now, false, true)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	retry := false
	err = c.tryDiagnosticTransition(ctx, func() transition {
		if !c.attemptCurrentLocked(a) {
			return transition{}
		}
		if c.revision != revision || c.resetSequence != generation || c.providerRevisions[provider] != providerRevision {
			retry = true
			return transition{}
		}
		id := routeID{Provider: provider, Model: model}
		i := c.incidentLocked(id)
		if i == nil && (!c.config.load().HistoryEnabled || len(c.incidents) >= maxOptionalIncidents) {
			return transition{}
		}
		state := c.providerHealthStateLocked(provider, model, keys, now)
		if state == "" {
			return transition{}
		}
		if i == nil && state == "healthy" {
			return transition{}
		}
		if i != nil && i.State == state && i.Status != "waiting_for_traffic" {
			return transition{}
		}
		if i == nil {
			i = c.newIncidentLocked(id, "", now, request)
		}
		setIncidentState(i, state, now, request)
		return transition{incidents: []*Incident{i}, events: []IncidentEvent{incidentEvent(i, "state_changed", request, fmt.Sprintf("Provider health for this model changed to %s.", state), now)}}
	})
	if err != nil || retry {
		c.deferDiagnostic(a, request, now, false, true)
	}
}

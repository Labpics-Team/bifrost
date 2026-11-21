package adaptive

import (
	"context"
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

type keyState struct {
	snapshot         *schemas.Key
	revision         string
	providerRevision string
	epoch            uint64
}
type probeReservation struct {
	routeID
	conditionID string
	token       uint64
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
func (c *Controller) transition(ctx context.Context, build func() transition) error {
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
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()
	c.mu.Lock()
	t = build()
	revision := c.revision
	c.mu.Unlock()
	if !t.durable() {
		if t.publish != nil {
			c.mu.Lock()
			t.publish()
			c.mu.Unlock()
		}
		return nil
	}
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

func (c *Controller) newIncidentLocked(id routeID, name string, now time.Time, request string) *Incident {
	scope := "key_model"
	if id.Model == "" {
		scope = "key"
	}
	if id.KeyID == "" {
		scope = "provider_model"
	}
	return &Incident{ID: uuid.NewString(), Scope: scope, Provider: string(id.Provider), ProviderRevision: c.providerRevisions[id.Provider], Model: id.Model, KeyID: id.KeyID, KeyName: name, OpenedAt: now, UpdatedAt: now, LastSeen: now, FirstRequestID: request, LastRequestID: request}
}

func incidentEvent(i *Incident, kind, request, message string, now time.Time) IncidentEvent {
	return IncidentEvent{ID: uuid.NewString(), IncidentID: i.ID, Timestamp: now, Kind: kind, State: i.State, FailureClass: i.FailureClass, RequestID: request, Message: message}
}

func setIncidentState(i *Incident, state string, now time.Time, request string) {
	i.State = state
	i.UpdatedAt = now
	i.LastSeen = now
	if request != "" {
		i.LastRequestID = request
		if i.FirstRequestID == "" {
			i.FirstRequestID = request
		}
	}
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
		i.Summary = "The route is returning errors."
		i.Action = "Its traffic share is reduced while observations recover."
	}
}

func (c *Controller) reserveCondition(ctx *schemas.BifrostContext, id routeID, force bool) bool {
	reserved := false
	writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.transition(writeCtx, func() transition {
		h := c.holdLocked(id, c.now())
		now := c.now()
		if h == nil || h.ProbeUntil.After(now) || (!force && h.HoldUntil.After(now)) {
			return transition{}
		}
		copy := *h
		c.probeSequence++
		copy.ProbeID = c.probeSequence
		copy.ProbeUntil = now.Add(5 * time.Minute)
		i := c.incidentLocked(h.routeID)
		t := transition{}
		if i != nil {
			i.Checks++
			i.UpdatedAt = now
			t.incidents = []*Incident{i}
			t.events = []IncidentEvent{incidentEvent(i, "check_started", requestID(ctx), "A real request is checking the held key.", now)}
		}
		t.publish = func() {
			c.conditions[h.routeID] = &copy
			ctx.SetValue(probeContextKey{}, probeReservation{routeID: id, conditionID: h.ID, token: copy.ProbeID})
			reserved = true
		}
		return t
	})
	if err != nil {
		ctx.AppendRoutingEngineLog(schemas.RoutingEngineLoadbalancing, schemas.LogLevelWarn, "Adaptive routing could not record a held-key check; the existing hold is retained.")
	}
	return reserved
}

func requestID(ctx *schemas.BifrostContext) string {
	id, _ := ctx.Value(schemas.BifrostContextKeyRequestID).(string)
	return id
}

func (c *Controller) completeConditions(a *attempt, failure schemas.FailureClass, retryAfter int64, now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.transition(ctx, func() transition {
		r := a.route
		if c.routes[r.routeID] != r || c.keyLocked(r.routeID) != a.key || a.key.epoch != a.keyEpoch || r.Epoch != a.modelEpoch {
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
			i := c.incidentLocked(h.routeID)
			if i == nil {
				return transition{}
			}
			if failure == "" || failure.IsPerKey() && failure != h.HoldClass {
				t.remove = append(t.remove, h)
				setIncidentState(i, "recovering", now, request)
				t.incidents = append(t.incidents, i)
				t.events = append(t.events, incidentEvent(i, "hold_released", request, "The check showed the previous refusal no longer applies.", now))
				t.publish = func() { r.Recovering = true; r.Recoveries = 0 }
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
				t.incidents = append(t.incidents, i)
				t.events = append(t.events, incidentEvent(i, "held_again", request, "The check did not establish recovery; the key remains held.", now))
				return t
			}
		}
		if failure.IsPerKey() {
			id := r.routeID
			if failure.CoversAllModels() {
				id.Model = ""
			}
			i := c.incidentLocked(id)
			if i == nil {
				i = c.newIncidentLocked(id, r.KeyName, now, request)
			}
			i.FailureClass = string(failure)
			setIncidentState(i, "held", now, request)
			next := &condition{routeID: id, ID: uuid.NewString(), KeyRevision: r.KeyRevision, IncidentID: i.ID, KeyName: r.KeyName, HoldUntil: now.Add(holdDuration(failure, 1, retryAfter)), HeldAt: now, Rung: 1, HoldClass: failure}
			t.upsert = append(t.upsert, next)
			t.incidents = append(t.incidents, i)
			t.events = append(t.events, incidentEvent(i, "held", request, "The provider refused the key; ordinary traffic skips it.", now))
		}
		return t
	})
	if err != nil {
		a.ctx.AppendRoutingEngineLog(schemas.RoutingEngineLoadbalancing, schemas.LogLevelWarn, "Adaptive routing could not commit the condition; the last committed state remains in effect.")
	}
}

func (c *Controller) updateHealthIncident(r *route, request string, now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = c.transition(ctx, func() transition {
		if c.routes[r.routeID] != r || c.holdLocked(r.routeID, now) != nil {
			return transition{}
		}
		state := c.stateLocked(r, now)
		i := c.incidentLocked(routeID{Provider: r.Provider, KeyID: r.KeyID})
		if i == nil {
			i = c.incidentLocked(r.routeID)
		}
		if !c.config.load().HistoryEnabled && i == nil {
			return transition{}
		}
		if i == nil && (state == "unknown" || state == "healthy") {
			return transition{}
		}
		if i != nil && i.State == state && i.Status != "waiting_for_traffic" {
			i.LastSeen = now
			i.LastRequestID = request
			c.incidents[incidentRoute(i)] = i
			return transition{}
		}
		if i == nil {
			i = c.newIncidentLocked(r.routeID, r.KeyName, now, request)
		}
		i.FailureClass = string(r.FailureClass)
		setIncidentState(i, state, now, request)
		return transition{incidents: []*Incident{i}, events: []IncidentEvent{incidentEvent(i, "state_changed", request, fmt.Sprintf("Observed route state changed to %s.", state), now)}}
	})
}

func (c *Controller) updateProviderIncident(provider schemas.ModelProvider, model, request string, now time.Time) {
	if c.keys == nil || c.supports == nil {
		return
	}
	keys, err := c.keys(context.Background(), provider)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = c.transition(ctx, func() transition {
		id := routeID{Provider: provider, Model: model}
		i := c.incidentLocked(id)
		if !c.config.load().HistoryEnabled && i == nil {
			return transition{}
		}
		eligible, unavailable := 0, 0
		errorRate := 0.0
		recovering := false
		for _, key := range keys {
			if key.Enabled != nil && !*key.Enabled || !c.supports(provider, key, model) {
				continue
			}
			eligible++
			rid := routeID{provider, model, key.ID}
			if c.weightLocked(rid, now) == 0 {
				unavailable++
			}
			if r := c.routes[rid]; r != nil {
				errorRate += r.ErrorRate
				recovering = recovering || r.Recovering
			}
		}
		if eligible == 0 {
			return transition{}
		}
		state := "healthy"
		if unavailable == eligible {
			state = "failed"
		} else if recovering {
			state = "recovering"
		} else if errorRate/float64(eligible) > 0.05 {
			state = "degraded"
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
}

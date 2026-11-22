package adaptive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"sort"

	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
)

const configKey = "load_balancer_config"

// NewFromStore follows the native settings contract: a saved dashboard/API
// value wins over the file; the file supplies the initial settings only.
func NewFromStore(ctx context.Context, store configstore.ConfigStore, file *Config) (*Controller, error) {
	config := DefaultConfig()
	enabled := file != nil
	if file != nil {
		config = *file
	}
	if store != nil {
		row, err := store.GetConfig(ctx, configKey)
		if err != nil && !errors.Is(err, configstore.ErrNotFound) {
			return nil, fmt.Errorf("read adaptive routing settings: %w", err)
		}
		if row != nil && row.Value != "" {
			enabled = true
			if err := json.Unmarshal([]byte(row.Value), &config); err != nil {
				return nil, fmt.Errorf("decode adaptive routing settings: %w", err)
			}
		}
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if enabled && store == nil {
		return nil, fmt.Errorf("adaptive routing requires a config store for durable state")
	}
	c := New(config)
	c.store = store
	if store != nil {
		var err error
		c.stateStore, err = configstore.NewAdaptiveRoutingStore(store)
		if err != nil {
			return nil, err
		}
	}
	c.enabled.Store(enabled)
	return c, nil
}

func (c *Controller) Save(ctx context.Context, config Config) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if c.store == nil {
		return fmt.Errorf("config store is unavailable")
	}
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if err = c.store.UpdateConfig(ctx, &tables.TableGovernanceConfig{Key: configKey, Value: string(data)}); err != nil {
		return err
	}
	return c.Apply(config)
}

// Restore restores all unresolved conditions, including ones due for a
// check. The native configuration validates their captured credential revisions.
func (c *Controller) Restore(ctx context.Context) error {
	if c.stateStore == nil {
		return nil
	}
	state, err := c.stateStore.Load(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.revision = state.Revision
	for _, source := range state.OpenIncidents {
		i := source
		c.incidents[incidentRoute(&i)] = &i
		if i.KeyID != "" && i.KeyRevision != "" {
			c.keyStates[routeID{Provider: schemas.ModelProvider(i.Provider), KeyID: i.KeyID}] = &keyState{revision: i.KeyRevision}
		}
	}
	for _, h := range state.Conditions {
		id := routeID{schemas.ModelProvider(h.Provider), h.Model, h.KeyID}
		name := ""
		if i := c.incidents[id]; i != nil {
			name = i.KeyName
		}
		c.conditions[id] = &condition{routeID: id, ID: h.ID, KeyRevision: h.KeyRevision, IncidentID: h.IncidentID, KeyName: name, HoldClass: schemas.FailureClass(h.FailureClass), HoldUntil: h.HoldUntil, HeldAt: h.HeldAt, Rung: h.Rung}
		c.keyStates[routeID{Provider: id.Provider, KeyID: id.KeyID}] = &keyState{revision: h.KeyRevision}
	}
	c.mu.Unlock()
	// A process can stop after native configuration committed but before its
	// adaptive cleanup did. Durable incident identity closes that gap even when
	// no per-key condition or telemetry checkpoint exists.
	changedProviders := map[schemas.ModelProvider]bool{}
	for _, i := range state.OpenIncidents {
		provider := schemas.ModelProvider(i.Provider)
		if i.ProviderRevision != c.providerRevisions[provider] || c.providerExists != nil && !c.providerExists(provider) {
			changedProviders[provider] = true
		}
	}
	for provider := range changedProviders {
		if err := c.ResetProvider(provider); err != nil {
			return err
		}
	}
	// Reconcile with one live snapshot per provider. No saved refusal is rebound
	// to whatever credential happens to exist when a checkpoint is written.
	providers := map[schemas.ModelProvider]map[string]bool{}
	addKey := func(provider schemas.ModelProvider, keyID string) {
		if keyID == "" || changedProviders[provider] {
			return
		}
		if providers[provider] == nil {
			providers[provider] = map[string]bool{}
		}
		providers[provider][keyID] = true
	}
	for _, h := range state.Conditions {
		addKey(schemas.ModelProvider(h.Provider), h.KeyID)
	}
	for _, i := range state.OpenIncidents {
		provider := schemas.ModelProvider(i.Provider)
		addKey(provider, i.KeyID)
		// Older schemas can lack the identity of a released hold. Without a
		// condition, that observation cannot safely be rebound to a live key.
		if i.KeyID != "" && !changedProviders[provider] && c.keyStates[routeID{Provider: provider, KeyID: i.KeyID}] == nil {
			if err := c.ResetKey(provider, i.KeyID); err != nil {
				return err
			}
		}
	}
	for provider, savedKeys := range providers {
		if c.keys == nil {
			return fmt.Errorf("adaptive routing catalog unavailable")
		}
		keys, err := c.keys(ctx, provider)
		if err != nil {
			if c.providerExists != nil && !c.providerExists(provider) {
				if err := c.ResetProvider(provider); err != nil {
					return err
				}
				continue
			}
			return err
		}
		live := map[string]bool{}
		for _, key := range keys {
			live[key.ID] = true
			if !c.syncKey(provider, key) {
				return fmt.Errorf("could not reconcile adaptive credential state")
			}
		}
		for keyID := range savedKeys {
			if !live[keyID] {
				if err := c.ResetKey(provider, keyID); err != nil {
					return err
				}
			}
		}
	}
	if len(state.Checkpoint) > 0 {
		var rows []route
		if err := json.Unmarshal(state.Checkpoint, &rows); err != nil {
			return fmt.Errorf("decode adaptive telemetry: %w", err)
		}
		for _, r := range rows {
			if len(c.routes) >= maxRoutes {
				break
			}
			valid := r.KeyID == "" && c.keyless != nil && c.keyless(r.Provider) && r.KeyRevision == keylessRevision(c.providerRevisions[r.Provider])
			if valid {
				c.keyLocked(r.routeID).revision = r.KeyRevision
			} else {
				if c.keys == nil {
					continue
				}
				keys, err := c.keys(ctx, r.Provider)
				if err != nil {
					if c.providerExists != nil && !c.providerExists(r.Provider) {
						continue
					}
					return err
				}
				for _, key := range keys {
					if key.ID == r.KeyID && credentialRevision(key, c.providerRevisions[r.Provider]) == r.KeyRevision {
						valid = c.syncKey(r.Provider, key)
						break
					}
				}
			}
			if !valid {
				continue
			}
			r.Inflight = 0
			r.ProbeID = 0
			r.ProbeUntil = time.Time{}
			r.Epoch = 0
			c.mu.Lock()
			c.routes[r.routeID] = &r
			c.mu.Unlock()
		}
	}
	c.mu.Lock()
	c.persistenceResultLocked(nil)
	c.mu.Unlock()
	return nil
}

// Checkpoint saves only bounded telemetry. Conditions are committed atomically
// with their lifecycle events when they change.
func (c *Controller) Checkpoint(ctx context.Context) error {
	if !c.Enabled() || c.stateStore == nil {
		return nil
	}
	c.lifeMu.Lock()
	defer c.lifeMu.Unlock()
	c.mu.Lock()
	revision := c.revision
	rows := make([]route, 0, len(c.routes))
	for _, r := range c.routes {
		rows = append(rows, *r)
	}
	c.mu.Unlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].LastSeen.After(rows[j].LastSeen) })
	if len(rows) > 2048 {
		rows = rows[:2048]
	}
	data, err := json.Marshal(rows)
	for err == nil && len(data) > 1<<20 && len(rows) > 0 {
		rows = rows[:len(rows)/2]
		data, err = json.Marshal(rows)
	}
	if err == nil {
		err = c.stateStore.Checkpoint(ctx, revision, data)
	}
	c.mu.Lock()
	c.persistenceResultLocked(err)
	c.mu.Unlock()
	return err
}

func (c *Controller) maintain(ctx context.Context) error {
	c.mu.Lock()
	pending := make(map[routeID]uint64, len(c.pendingResets))
	for id, generation := range c.pendingResets {
		pending[id] = generation
	}
	c.mu.Unlock()
	for id, generation := range pending {
		if err := c.retryReset(ctx, id, generation); err != nil {
			return err
		}
	}
	now := c.now()
	err := c.transition(ctx, func() transition {
		t := transition{}
		observed := make(map[routeID]bool, len(c.routes))
		for id, r := range c.routes {
			if r.Inflight == 0 && now.Sub(r.LastSeen) > idleTTL {
				delete(c.routes, id)
				continue
			}
			observed[id] = true
			observed[routeID{Provider: id.Provider, KeyID: id.KeyID}] = true
			observed[routeID{Provider: id.Provider, Model: id.Model}] = true
		}
		for _, source := range c.incidents {
			i := *source
			if i.WorstState != "held" && (!c.Config().HistoryEnabled || !observed[incidentRoute(&i)]) {
				kind, message := "telemetry_removed", "The route is no longer observed after its telemetry was evicted or became idle."
				if !c.Config().HistoryEnabled {
					kind, message = "history_disabled", "Optional health history was disabled before confirmed recovery."
				}
				i.Status = "no_longer_observed"
				i.ClosedAt = &now
				i.UpdatedAt = now
				i.Summary = message
				i.Action = ""
				t.incidents = append(t.incidents, &i)
				t.events = append(t.events, incidentEvent(&i, kind, "", message, now))
				continue
			}
			if i.Status == "recovering" && now.Sub(i.LastSeen) >= time.Hour {
				i.Status = "waiting_for_traffic"
				i.UpdatedAt = now
				t.incidents = append(t.incidents, &i)
				t.events = append(t.events, incidentEvent(&i, "waiting_for_traffic", "", "No real traffic has established recovery for an hour.", now))
			}
		}
		return t
	})
	if err != nil {
		return err
	}
	if c.stateStore != nil {
		_, err = c.stateStore.PruneIncidents(ctx, now.Add(-time.Duration(c.Config().HistoryRetentionDays)*24*time.Hour), 1000)
	}
	return err
}

func (c *Controller) Start(ctx context.Context, logger schemas.Logger) {
	c.startOnce.Do(func() {
		ctx, c.stop = context.WithCancel(ctx)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			timer := time.NewTimer(time.Second * 5)
			defer timer.Stop()
			lastCheckpoint := time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					err := c.maintain(writeCtx)
					interval := time.Duration(c.Config().SnapshotIntervalSeconds) * time.Second
					if err == nil && time.Since(lastCheckpoint) >= interval {
						err = c.Checkpoint(writeCtx)
						if err == nil {
							lastCheckpoint = time.Now()
						}
					}
					cancel()
					if err != nil && logger != nil {
						logger.Warn("[Routing] adaptive state maintenance failed: %v", err)
					}
					timer.Reset(min(30*time.Second, interval))
				}
			}
		}()
	})
}

func (c *Controller) Close() error {
	if c.stop != nil {
		c.stop()
		c.wg.Wait()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.maintain(ctx); err != nil {
		return err
	}
	return c.Checkpoint(ctx)
}

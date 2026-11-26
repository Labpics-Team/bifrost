package adaptive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

// ConfigHash follows config-file references, whereas this fingerprint covers
// the actual resolved native key snapshot. It is computed once per revision.
func normalizeKey(key schemas.Key) schemas.Key {
	key.Status = ""
	key.ConfigHash = ""
	key.Name = ""
	key.Description = ""
	key.Weight = 0
	// Native SQL materializes omitted defaults. Their explicit form is the same
	// configuration, so reopening config.db must not retire an unresolved hold.
	key.Enabled = withoutDefaultBool(key.Enabled, true)
	key.UseForBatchAPI = withoutDefaultBool(key.UseForBatchAPI, false)
	key.UseAnthropicEndpoints = withoutDefaultBool(key.UseAnthropicEndpoints, false)
	key.UseOpenAIEndpoints = withoutDefaultBool(key.UseOpenAIEndpoints, false)
	return key
}
func withoutDefaultBool(value *bool, defaultValue bool) *bool {
	if value != nil && *value == defaultValue {
		return nil
	}
	return value
}
func keyHash(key schemas.Key) string {
	data, err := json.Marshal(normalizeKey(key))
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func credentialRevision(key schemas.Key, providerRevision string) string {
	if providerRevision == "" {
		return keyHash(key)
	}
	digest := sha256.Sum256([]byte(providerRevision + ":" + keyHash(key)))
	return hex.EncodeToString(digest[:])
}

func keylessRevision(providerRevision string) string {
	if providerRevision == "" {
		return "keyless"
	}
	return "keyless:" + providerRevision
}

// ProviderConfigured runs only when core publishes newly constructed provider
// workers. The identity covers resolved connection/request behavior; UI logging
// and concurrency settings do not change which credential a refusal describes.
func (c *Controller) ProviderConfigured(provider schemas.ModelProvider, config *schemas.ProviderConfig) string {
	identity := *config
	identity.Logger = nil
	identity.ConcurrencyAndBufferSize = schemas.ConcurrencyAndBufferSize{}
	identity.SendBackRawRequest = false
	identity.SendBackRawResponse = false
	identity.StoreRawRequestResponse = false
	identity.IgnoreProviderCost = false
	data, _ := json.Marshal(identity)
	digest := sha256.Sum256(data)
	revision := hex.EncodeToString(digest[:])
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.providerRevisions[provider]
	c.providerRevisions[provider] = revision
	if previous != "" && previous != revision {
		c.markResetLocked(routeID{Provider: provider})
		for id, k := range c.keyStates {
			if id.Provider == provider {
				k.epoch++
			}
		}
	}
	return revision
}

// Freeze nested values once when adopting a revision. Merely keeping native
// pointers would let an in-place secret/alias edit change the comparison's past.
func cloneKey(key schemas.Key) *schemas.Key {
	copy := cloneKeyValue(reflect.ValueOf(key)).Interface().(schemas.Key)
	return &copy
}
func cloneKeyValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		copy := reflect.New(v.Type()).Elem()
		copy.Set(cloneKeyValue(v.Elem()))
		return copy
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}
		copy := reflect.New(v.Type().Elem())
		copy.Elem().Set(cloneKeyValue(v.Elem()))
		return copy
	case reflect.Struct:
		copy := reflect.New(v.Type()).Elem()
		copy.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				copy.Field(i).Set(cloneKeyValue(v.Field(i)))
			}
		}
		return copy
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		copy := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			copy.Index(i).Set(cloneKeyValue(v.Index(i)))
		}
		return copy
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		copy := reflect.MakeMapWithSize(v.Type(), v.Len())
		it := v.MapRange()
		for it.Next() {
			copy.SetMapIndex(it.Key(), cloneKeyValue(it.Value()))
		}
		return copy
	default:
		return v
	}
}

// syncKey only adopts a revision that still belongs to the live catalog. A
// delayed attempt carrying a previous configuration cannot rotate it backwards.
func (c *Controller) syncKey(provider schemas.ModelProvider, key schemas.Key) bool {
	return c.syncKeyState(provider, key) != nil
}

// Return the accepted state itself so admission can reject replacement between
// synchronization and acquiring the attempt's telemetry/probe generations.
func (c *Controller) syncKeyState(provider schemas.ModelProvider, key schemas.Key) *keyState {
	if key.ID == "" {
		return nil
	}
	key = normalizeKey(key)
	id := routeID{Provider: provider, KeyID: key.ID}
	c.mu.Lock()
	current := c.keyStates[id]
	providerRevision := c.providerRevisions[provider]
	same := current != nil && current.snapshot != nil && current.providerRevision == providerRevision && reflect.DeepEqual(current.snapshot, &key)
	c.mu.Unlock()
	if same {
		return current
	}
	revision := credentialRevision(key, providerRevision)
	if revision == "" {
		return nil
	}
	frozen := cloneKey(key)
	var accepted *keyState
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := c.transition(ctx, func() transition {
		if c.providerRevisions[provider] != providerRevision {
			return transition{}
		}
		// Validate after serial admission: a queued old revision cannot replace a
		// newer edit which committed while this caller was waiting.
		if c.keys != nil {
			keys, err := c.keys(context.Background(), provider)
			if err != nil {
				return transition{}
			}
			found := false
			for _, live := range keys {
				normalized := normalizeKey(live)
				if reflect.DeepEqual(&normalized, &key) {
					found = true
					break
				}
			}
			if !found {
				return transition{}
			}
		}
		if k := c.keyStates[id]; k != nil && k.revision == revision {
			return transition{publish: func() { k.snapshot = frozen; k.providerRevision = providerRevision; accepted = k }}
		}
		t := c.retireKeyLocked(provider, key.ID, "The configured credential changed; previous observations no longer apply.")
		t.advance = c.keyStates[id] != nil
		t.publish = func() {
			c.forgetReleasedLocked(provider, key.ID, "")
			accepted = &keyState{revision: revision, snapshot: frozen, providerRevision: providerRevision}
			c.keyStates[id] = accepted
			for rid := range c.routes {
				if rid.Provider == provider && rid.KeyID == key.ID {
					delete(c.routes, rid)
					delete(c.pendingDiagnostics, rid)
				}
			}
		}
		return t
	})
	if err != nil {
		return nil
	}
	return accepted
}

func (c *Controller) retireKeyLocked(provider schemas.ModelProvider, keyID, message string) transition {
	t := transition{}
	now := c.now()
	for id, h := range c.conditions {
		if id.Provider == provider && (keyID == "" || id.KeyID == keyID) {
			t.remove = append(t.remove, h)
		}
	}
	for _, source := range c.incidents {
		if source.Provider != string(provider) || (keyID != "" && source.KeyID != keyID) {
			continue
		}
		i := *source
		i.Status = "no_longer_observed"
		i.UpdatedAt = now
		i.ClosedAt = &now
		i.Summary = message
		i.Action = ""
		t.incidents = append(t.incidents, &i)
		t.events = append(t.events, incidentEvent(&i, "configuration_changed", "", message, now))
	}
	return t
}

func (c *Controller) ResetProvider(provider schemas.ModelProvider) error {
	return c.reset(provider, "")
}
func (c *Controller) ResetKey(provider schemas.ModelProvider, keyID string) error {
	return c.reset(provider, keyID)
}
func (c *Controller) reset(provider schemas.ModelProvider, keyID string) error {
	id := routeID{Provider: provider, KeyID: keyID}
	c.mu.Lock()
	generation := c.markResetLocked(id)
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.retryReset(ctx, id, generation)
}

func (c *Controller) markResetLocked(id routeID) uint64 {
	for scope := range c.providerChecks {
		if scope.Provider == id.Provider {
			delete(c.providerChecks, scope)
		}
	}
	c.resetSequence++
	c.pendingResets[id] = c.resetSequence
	c.persistence.Healthy = false
	c.persistence.LastError = "Native configuration changed; adaptive reconciliation is pending."
	return c.resetSequence
}

func (c *Controller) retryReset(ctx context.Context, scope routeID, generation uint64) error {
	provider, keyID := scope.Provider, scope.KeyID
	return c.transition(ctx, func() transition {
		if c.pendingResets[scope] != generation {
			return transition{}
		}
		t := c.retireKeyLocked(provider, keyID, "The key or provider was edited or removed; its previous observations no longer apply.")
		t.advance = true
		t.publish = func() {
			c.forgetReleasedLocked(provider, keyID, "")
			if c.pendingResets[scope] == generation {
				delete(c.pendingResets, scope)
			}
			for id := range c.routes {
				if id.Provider == provider && (keyID == "" || id.KeyID == keyID) {
					delete(c.routes, id)
					delete(c.pendingDiagnostics, id)
				}
			}
			for id := range c.keyStates {
				if id.Provider == provider && (keyID == "" || id.KeyID == keyID) {
					delete(c.keyStates, id)
				}
			}
			if keyID == "" && c.providerExists != nil && !c.providerExists(provider) {
				delete(c.providerRevisions, provider)
			}
		}
		return t
	})
}

func (c *Controller) Release(ctx context.Context, provider schemas.ModelProvider, keyID, model string) error {
	return c.transition(ctx, func() transition {
		t := transition{}
		now := c.now()
		for id, h := range c.conditions {
			if provider != "" && (id.Provider != provider || id.KeyID != keyID || model != "" && id.Model != model) {
				continue
			}
			t.remove = append(t.remove, h)
			if i := c.incidentLocked(id); i != nil {
				setIncidentState(i, "recovering", now, "")
				t.incidents = append(t.incidents, i)
				t.events = append(t.events, incidentEvent(i, "manual_release", "", "An operator released the hold; real requests must establish recovery.", now))
			}
		}
		t.publish = func() {
			c.forgetReleasedLocked(provider, keyID, model)
			for _, h := range t.remove {
				for id, r := range c.routes {
					if id.Provider == h.Provider && id.KeyID == h.KeyID && (h.Model == "" || id.Model == h.Model) {
						r.Recovering = true
						r.Recoveries = 0
						r.Consecutive = 0
						r.FailedUntil = time.Time{}
					}
				}
			}
		}
		return t
	})
}

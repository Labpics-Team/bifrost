package adaptive

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maximhq/bifrost/core/schemas"

	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
)

const configKey = "load_balancer_config"
const holdsKey = "adaptive_routing_holds"

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
	c := New(config)
	c.store = store
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
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if err = c.store.UpdateConfig(ctx, &tables.TableGovernanceConfig{Key: configKey, Value: string(data)}); err != nil {
		return err
	}
	return c.Apply(config)
}

type storedHold struct {
	routeID
	KeyHash      string               `json:"key_hash"`
	HoldUntil    time.Time            `json:"hold_until"`
	HeldAt       time.Time            `json:"held_at"`
	Rung         int                  `json:"rung"`
	FailureClass schemas.FailureClass `json:"failure_class"`
}

func keyHash(key schemas.Key) string {
	data, err := json.Marshal(key)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// RestoreHolds restores only unexpired refusals, and only for an unchanged
// configured credential. Scores, latency, load and probes are always fresh.
func (c *Controller) RestoreHolds(ctx context.Context) error {
	if !c.Enabled() || c.store == nil || c.keys == nil {
		return nil
	}
	row, err := c.store.GetConfig(ctx, holdsKey)
	if errors.Is(err, configstore.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if row == nil {
		return nil
	}
	var holds []storedHold
	if err := json.Unmarshal([]byte(row.Value), &holds); err != nil {
		return err
	}
	now := c.now()
	for i, h := range holds {
		if i >= maxRoutes {
			break
		}
		if !h.HoldUntil.After(now) || !h.FailureClass.IsPerKey() {
			continue
		}
		keys, err := c.keys(ctx, h.Provider)
		if err != nil {
			continue
		}
		for _, key := range keys {
			if key.ID != h.KeyID || h.KeyHash == "" || keyHash(key) != h.KeyHash {
				continue
			}
			c.mu.Lock()
			if r := c.routeLocked(h.routeID, key.Name, now); r != nil {
				r.HoldUntil = h.HoldUntil
				r.HeldAt = h.HeldAt
				r.Rung = h.Rung
				r.FailureClass = h.FailureClass
				r.HoldClass = h.FailureClass
			}
			c.mu.Unlock()
			break
		}
	}
	return nil
}

func (c *Controller) SaveHolds(ctx context.Context) error {
	if !c.Enabled() || c.store == nil || c.keys == nil {
		return nil
	}
	c.saveMu.Lock()
	defer c.saveMu.Unlock()
	now := c.now()
	c.mu.Lock()
	holds := make([]storedHold, 0)
	for _, r := range c.routes {
		if r.HoldUntil.After(now) && r.HoldClass.IsPerKey() {
			holds = append(holds, storedHold{routeID: r.routeID, HoldUntil: r.HoldUntil, HeldAt: r.HeldAt, Rung: r.Rung, FailureClass: r.HoldClass})
		}
	}
	c.mu.Unlock()
	valid := holds[:0]
	for _, h := range holds {
		keys, err := c.keys(ctx, h.Provider)
		if err != nil {
			continue
		}
		for _, key := range keys {
			if key.ID == h.KeyID {
				h.KeyHash = keyHash(key)
				if h.KeyHash != "" {
					valid = append(valid, h)
				}
				break
			}
		}
	}
	data, err := json.Marshal(valid)
	if err != nil {
		return err
	}
	return c.store.UpdateConfig(ctx, &tables.TableGovernanceConfig{Key: holdsKey, Value: string(data)})
}

func (c *Controller) Start(ctx context.Context, logger schemas.Logger) {
	c.startOnce.Do(func() {
		ctx, c.stop = context.WithCancel(ctx)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			timer := time.NewTimer(time.Duration(c.Config().SnapshotIntervalSeconds) * time.Second)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					if err := c.SaveHolds(writeCtx); err != nil && logger != nil {
						logger.Warn("[Routing] could not persist held keys: %v", err)
					}
					cancel()
					timer.Reset(time.Duration(c.Config().SnapshotIntervalSeconds) * time.Second)
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
	return c.SaveHolds(ctx)
}

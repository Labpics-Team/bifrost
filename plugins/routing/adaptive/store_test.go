package adaptive

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"gorm.io/gorm"
)

type settingsStore struct {
	configstore.ConfigStore
	mu   sync.Mutex
	rows map[string]string
	fail bool
}

func (s *settingsStore) GetConfig(_ context.Context, key string) (*tables.TableGovernanceConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.rows[key]
	if !ok {
		return nil, configstore.ErrNotFound
	}
	return &tables.TableGovernanceConfig{Key: key, Value: v}, nil
}
func (s *settingsStore) UpdateConfig(_ context.Context, row *tables.TableGovernanceConfig, _ ...*gorm.DB) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("database offline")
	}
	s.rows[row.Key] = row.Value
	return nil
}

func TestAdaptiveSettingsRequireExplicitConfiguration(t *testing.T) {
	s := &settingsStore{rows: map[string]string{}}
	c, err := NewFromStore(context.Background(), s, nil)
	if err != nil || c.Enabled() {
		t.Fatalf("existing deployment unexpectedly enabled: %v", err)
	}
	if c.BeginProviderAttempt(testContext(), schemas.ProviderAttemptInfo{Provider: "p", Model: "m", KeyID: "k"}) != nil {
		t.Fatal("unconfigured deployment retained attempt state")
	}
	cfg := DefaultConfig()
	cfg.AppendFallbacksToPinned = true
	s.fail = true
	if c.Save(context.Background(), cfg) == nil || c.Enabled() {
		t.Fatal("failed persistence changed runtime settings")
	}
	s.fail = false
	if err := c.Save(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	file := DefaultConfig()
	loaded, err := NewFromStore(context.Background(), s, &file)
	if err != nil || !loaded.Enabled() || !loaded.Config().AppendFallbacksToPinned {
		t.Fatalf("saved settings not restored: %v", err)
	}
}

func TestAdaptiveHoldsSurviveRestartButNotCredentialRotation(t *testing.T) {
	s := &settingsStore{rows: map[string]string{}}
	c, _ := providerController()
	c.store = s
	keys := []schemas.Key{{ID: "a", Name: "A", Value: *schemas.NewSecretVar("secret-value")}}
	setKeys := func(target *Controller) {
		target.SetCatalog(nil, func(context.Context, schemas.ModelProvider) ([]schemas.Key, error) { return keys, nil }, nil)
	}
	setKeys(c)
	observe(c, "p1", "m", "a", responseError(401))
	if err := c.SaveHolds(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(s.rows[holdsKey], "secret-value") {
		t.Fatal("snapshot contains credential")
	}
	loaded, _ := testController()
	loaded.store = s
	setKeys(loaded)
	if err := loaded.RestoreHolds(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(loaded.routes) != 1 || loaded.routes[routeID{Provider: "p1", KeyID: "a"}] == nil {
		t.Fatalf("whole-key hold was lost: %+v", loaded.routes)
	}
	for _, r := range loaded.routes {
		if r.Samples != 0 || r.LatencyMS != 0 {
			t.Fatal("old scores restored")
		}
	}
	keys[0].Value = *schemas.NewSecretVar("rotated-value")
	fresh, _ := testController()
	fresh.store = s
	setKeys(fresh)
	fresh.RestoreHolds(context.Background())
	if len(fresh.routes) != 0 {
		t.Fatal("old hold affected rotated key")
	}
}

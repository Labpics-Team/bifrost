package server

import (
	"context"
	"path/filepath"
	"testing"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/modelcatalog"
	"github.com/maximhq/bifrost/plugins/routing/adaptive"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
)

func TestAdaptivePersistenceFailureDoesNotAbortNativeConfiguration(t *testing.T) {
	previousLogger := logger
	logger = noopTestLogger{}
	lib.SetLogger(noopTestLogger{})
	defer func() { logger = previousLogger }()
	for _, operation := range []string{"key_update", "key_delete", "provider_reload", "provider_remove"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			store, err := configstore.NewConfigStore(ctx, &configstore.Config{Enabled: true, Type: configstore.ConfigStoreTypeSQLite, Config: &configstore.SQLiteConfig{Path: filepath.Join(t.TempDir(), "config.db")}}, noopTestLogger{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close(ctx)
			settings := adaptive.DefaultConfig()
			controller, err := adaptive.NewFromStore(ctx, store, &settings)
			if err != nil {
				t.Fatal(err)
			}
			key := schemas.Key{ID: "key-1", Name: "First", Value: *schemas.NewSecretVar("fixture")}
			second := schemas.Key{ID: "key-2", Name: "Second", Value: *schemas.NewSecretVar("fixture")}
			provider := schemas.ModelProvider("fixture")
			native := configstore.ProviderConfig{Keys: []schemas.Key{key, second}, CustomProviderConfig: &schemas.CustomProviderConfig{BaseProviderType: schemas.OpenAI, AllowedRequests: &schemas.AllowedRequests{ChatCompletion: true}}}
			if err := store.AddProvider(ctx, provider, native); err != nil {
				t.Fatal(err)
			}
			catalog := modelcatalog.NewTestCatalog(nil)
			catalog.UpsertLive(provider, key.ID, false, []string{"fixture/first"})
			catalog.UpsertLive(provider, second.ID, false, []string{"fixture/second"})
			cfg := &lib.Config{ConfigStore: store, AdaptiveRouter: controller, ModelCatalog: catalog, Providers: map[schemas.ModelProvider]configstore.ProviderConfig{provider: native}}
			server := &BifrostHTTPServer{Config: cfg, Ctx: schemas.NewBifrostContext(ctx, schemas.NoDeadline), Client: &bifrost.Bifrost{}}
			if operation == "provider_remove" {
				server.Client, err = bifrost.Init(ctx, schemas.BifrostConfig{Account: lib.NewBaseAccount(cfg), Logger: noopTestLogger{}, InitialPoolSize: 4})
				if err != nil {
					t.Fatal(err)
				}
				defer server.Client.Shutdown()
			}
			if err := store.DB().Exec("CREATE TRIGGER fail_adaptive_commit BEFORE UPDATE ON adaptive_routing_state BEGIN SELECT RAISE(ABORT, 'adaptive unavailable'); END").Error; err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "key_update":
				key.Enabled = schemas.Ptr(false)
				native.Keys = []schemas.Key{key, second}
				cfg.Providers[provider] = native
				err = server.OnKeyUpdated(ctx, provider, key)
			case "key_delete":
				native.Keys = []schemas.Key{second}
				cfg.Providers[provider] = native
				err = server.OnKeyDeleted(ctx, provider, key.ID)
			case "provider_reload":
				native.Keys = []schemas.Key{second}
				cfg.Providers[provider] = native
				_, err = server.ReloadProvider(ctx, provider, false)
			case "provider_remove":
				// The bare fixture has no governance plugin, whose existing callback
				// can return an error after removal. Check both native removal effects.
				_ = server.RemoveProvider(ctx, provider)
				if cfg.IsProviderConfigured(provider) || server.Client.GetProviderByKey(provider) != nil {
					t.Fatal("adaptive failure prevented native provider removal")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if operation != "provider_remove" {
				models := catalog.GetModelsForProvider(provider)
				if len(models) != 1 || models[0] != "fixture/second" {
					t.Fatalf("native catalog was not refreshed: %v", models)
				}
			}
			if controller.PersistenceStatus().Healthy {
				t.Fatal("adaptive persistence failure was hidden")
			}
		})
	}
}

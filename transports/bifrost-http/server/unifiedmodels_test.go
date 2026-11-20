package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fasthttp/router"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/modelcatalog"
	"github.com/maximhq/bifrost/plugins/governance"
	"github.com/maximhq/bifrost/plugins/routing"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/handlers"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestUnifiedModelCatalogHTTP(t *testing.T) {
	log := bifrost.NewNoOpLogger()
	SetLogger(log)
	handlers.SetLogger(log)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"physical","object":"model","owned_by":"test"}]}`))
	}))
	defer upstream.Close()
	config := &lib.Config{
		ClientConfig: &configstore.ClientConfig{}, ModelCatalog: modelcatalog.NewTestCatalog(nil),
		Providers: map[schemas.ModelProvider]configstore.ProviderConfig{
			schemas.OpenAI: {
				Keys:          []schemas.Key{{ID: "provider-key", Value: *schemas.NewSecretVar("sk-test"), Models: schemas.WhiteList{"*"}, Weight: 1}},
				NetworkConfig: &schemas.NetworkConfig{BaseURL: upstream.URL, AllowPrivateNetwork: true, DefaultRequestTimeoutInSeconds: 5},
			},
		},
	}
	mandatory := true
	gov, err := governance.Init(context.Background(), &governance.Config{IsVkMandatory: &mandatory}, log, nil, &configstore.GovernanceConfig{
		VirtualKeys: []tables.TableVirtualKey{{ID: "reader", Name: "reader", Value: *schemas.NewSecretVar("sk-bf-reader"), IsActive: bifrost.Ptr(true), ProviderConfigs: []tables.TableVirtualKeyProviderConfig{{Provider: "openai", AllowedModels: schemas.WhiteList{"physical"}, AllowAllKeys: true}}}},
	}, nil, nil, nil)
	require.NoError(t, err)
	routes, err := routing.Init(context.Background(), nil, log, nil, gov)
	require.NoError(t, err)
	for name, model := range map[string]string{"Frontier": "physical", "Hidden": "private"} {
		require.NoError(t, routes.GetRuleStore().UpsertRule(context.Background(), &tables.TableRoutingRule{ID: name, Scope: "global", CelExpression: `model == "` + name + `"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr(model), Weight: 1}}}))
	}
	plugins := []schemas.BasePlugin{gov, routes}
	config.BasePlugins.Store(&plugins)
	client, err := bifrost.Init(context.Background(), schemas.BifrostConfig{Account: lib.NewBaseAccount(config), Logger: log, LLMPlugins: []schemas.LLMPlugin{routes, gov}})
	require.NoError(t, err)
	defer client.Shutdown()
	s := &BifrostHTTPServer{Config: config, Client: client, Ctx: schemas.NewBifrostContext(context.Background(), time.Time{})}
	r := router.New()
	handlers.NewInferenceHandler(s, client, config).RegisterRoutes(r)
	handlers.NewIntegrationHandler(client, config, s, nil, nil, nil, nil).RegisterRoutes(r)
	read := func(path, key string) (int, schemas.BifrostListModelsResponse) {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(path)
		if key != "" {
			ctx.Request.Header.Set("x-bf-vk", key)
		}
		r.Handler(ctx)
		var response schemas.BifrostListModelsResponse
		require.NoError(t, json.Unmarshal(ctx.Response.Body(), &response), string(ctx.Response.Body()))
		return ctx.Response.StatusCode(), response
	}
	for _, path := range []string{"/v1/models", "/openai/v1/models"} {
		status, _ := read(path, "")
		require.Equal(t, http.StatusUnauthorized, status, path)
		status, response := read(path, "sk-bf-reader")
		require.Equal(t, http.StatusOK, status, path)
		ids := make([]string, len(response.Data))
		for i, model := range response.Data {
			ids[i] = model.ID
		}
		require.Equal(t, []string{"Frontier", "openai/physical"}, ids, path)
	}
	_, first := read("/v1/models?page_size=1", "sk-bf-reader")
	require.Equal(t, "Frontier", first.Data[0].ID)
	require.NotEmpty(t, first.NextPageToken)
	_, next := read("/v1/models?page_size=1&page_token="+first.NextPageToken, "sk-bf-reader")
	require.Len(t, next.Data, 1)
	require.Equal(t, "openai/physical", next.Data[0].ID)
	require.Empty(t, next.NextPageToken)

	// A rule edit appears without updating a second catalog or restarting.
	require.NoError(t, routes.GetRuleStore().UpsertRule(context.Background(), &tables.TableRoutingRule{ID: "Frontier", Scope: "global", CelExpression: `model == "Base"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical"), Weight: 1}}}))
	_, updated := read("/v1/models", "sk-bf-reader")
	require.Equal(t, "Base", updated.Data[0].ID)

	// Deployments without routing retain the native provider catalog.
	plugins = []schemas.BasePlugin{gov}
	config.BasePlugins.Store(&plugins)
	status, native := read("/v1/models", "sk-bf-reader")
	require.Equal(t, http.StatusOK, status)
	require.Len(t, native.Data, 1)
	require.Equal(t, "openai/physical", native.Data[0].ID)
}

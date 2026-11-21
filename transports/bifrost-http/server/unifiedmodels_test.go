package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func newUnifiedModelCatalog(t testing.TB, providerStatus int) (*BifrostHTTPServer, *routing.RoutingPlugin, *router.Router) {
	t.Helper()
	log := bifrost.NewNoOpLogger()
	SetLogger(log)
	handlers.SetLogger(log)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if providerStatus != http.StatusOK {
			w.WriteHeader(providerStatus)
			_, _ = w.Write([]byte(`{"error":{"message":"discovery unavailable"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"physical","object":"model","owned_by":"test"}]}`))
	}))
	t.Cleanup(upstream.Close)
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
	t.Cleanup(client.Shutdown)
	s := &BifrostHTTPServer{Config: config, Client: client, Ctx: schemas.NewBifrostContext(context.Background(), time.Time{})}
	r := router.New()
	handlers.NewInferenceHandler(s, client, config).RegisterRoutes(r)
	handlers.NewIntegrationHandler(client, config, s, nil, nil, nil, nil).RegisterRoutes(r)
	return s, routes, r
}

func TestUnifiedModelCatalogHTTP(t *testing.T) {
	s, routes, r := newUnifiedModelCatalog(t, http.StatusOK)
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
	plugins := (*s.Config.BasePlugins.Load())[:1]
	s.Config.BasePlugins.Store(&plugins)
	status, native := read("/v1/models", "sk-bf-reader")
	require.Equal(t, http.StatusOK, status)
	require.Len(t, native.Data, 1)
	require.Equal(t, "openai/physical", native.Data[0].ID)
}

func TestUnifiedModelCatalogProviderFailure(t *testing.T) {
	s, _, r := newUnifiedModelCatalog(t, http.StatusServiceUnavailable)
	for _, path := range []string{"/v1/models", "/openai/v1/models"} {
		request := &fasthttp.RequestCtx{}
		request.Request.Header.SetMethod("GET")
		request.Request.SetRequestURI(path)
		request.Request.Header.Set("x-bf-vk", "sk-bf-reader")
		r.Handler(request)
		require.Equal(t, http.StatusOK, request.Response.StatusCode(), string(request.Response.Body()))
		var catalog schemas.BifrostListModelsResponse
		require.NoError(t, json.Unmarshal(request.Response.Body(), &catalog))
		require.Len(t, catalog.Data, 1)
		require.Equal(t, "Frontier", catalog.Data[0].ID)
	}
	httpCtx := &fasthttp.RequestCtx{}
	httpCtx.Request.Header.Set("x-bf-vk", "sk-bf-reader")
	ctx, cancel := lib.ConvertToBifrostContext(httpCtx, s.Config)
	defer cancel()
	response, err := s.ListAllModels(ctx, nil)
	require.Nil(t, err, "%v", err)
	require.Equal(t, []schemas.Model{{ID: "Frontier"}}, response.Data)

	ctx.SetValue(schemas.BifrostContextKeyDirectKey, schemas.Key{Value: *schemas.NewSecretVar("sk-direct")})
	_, err = s.ListAllModels(ctx, nil)
	require.NotNil(t, err, "a direct key must still name its provider")
	require.Equal(t, http.StatusBadRequest, *err.StatusCode)
}

type catalogPolicyPlugin struct {
	status int
	post   bool
}

func (p *catalogPolicyPlugin) GetName() string { return "catalog-policy" }
func (p *catalogPolicyPlugin) Cleanup() error  { return nil }
func (p *catalogPolicyPlugin) PreRequestHook(*schemas.BifrostContext, *schemas.BifrostRequest) error {
	return nil
}
func (p *catalogPolicyPlugin) refusal() *schemas.BifrostError {
	return &schemas.BifrostError{StatusCode: bifrost.Ptr(p.status), Error: &schemas.ErrorField{Message: "catalog policy refuses this caller"}}
}
func (p *catalogPolicyPlugin) PreLLMHook(_ *schemas.BifrostContext, request *schemas.BifrostRequest) (*schemas.BifrostRequest, *schemas.LLMPluginShortCircuit, error) {
	if p.post {
		return request, nil, nil
	}
	return request, &schemas.LLMPluginShortCircuit{Error: p.refusal()}, nil
}
func (p *catalogPolicyPlugin) PostLLMHook(_ *schemas.BifrostContext, response *schemas.BifrostResponse, err *schemas.BifrostError) (*schemas.BifrostResponse, *schemas.BifrostError, error) {
	if p.post {
		refusal := p.refusal()
		if err != nil {
			// A post-hook can retain discovery diagnostics while rejecting the
			// request itself. The returned error is still the policy refusal.
			refusal.ExtraFields.KeyStatuses = err.ExtraFields.KeyStatuses
		}
		return nil, refusal, nil
	}
	return response, err, nil
}

func TestUnifiedModelCatalogPreservesPolicyFailure(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusServiceUnavailable} {
		for _, post := range []bool{false, true} {
			t.Run(strconv.Itoa(status)+"/post="+strconv.FormatBool(post), func(t *testing.T) {
				s, _, r := newUnifiedModelCatalog(t, http.StatusServiceUnavailable)
				require.NoError(t, s.Client.ReloadPlugin(&catalogPolicyPlugin{status: status, post: post}, []schemas.PluginType{schemas.PluginTypeLLM}))
				for _, path := range []string{"/v1/models", "/openai/v1/models"} {
					ctx := &fasthttp.RequestCtx{}
					ctx.Request.Header.SetMethod("GET")
					ctx.Request.SetRequestURI(path)
					ctx.Request.Header.Set("x-bf-vk", "sk-bf-reader")
					r.Handler(ctx)
					require.Equal(t, status, ctx.Response.StatusCode(), string(ctx.Response.Body()))
					require.Contains(t, string(ctx.Response.Body()), "catalog policy refuses this caller")
				}
			})
		}
	}
}

// nativeModelCatalog preserves the pre-change dispatch for the benchmark.
type nativeModelCatalog struct{ *BifrostHTTPServer }

func (n nativeModelCatalog) ListAllModels(ctx *schemas.BifrostContext, req *schemas.BifrostListModelsRequest) (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
	return n.Client.ListAllModels(ctx, req)
}

func BenchmarkModelCatalog(b *testing.B) {
	for _, mode := range []string{"native", "without_routing", "with_routing_names"} {
		b.Run(mode, func(b *testing.B) {
			s, _, r := newUnifiedModelCatalog(b, http.StatusOK)
			if mode == "native" {
				r = router.New()
				handlers.NewInferenceHandler(nativeModelCatalog{s}, s.Client, s.Config).RegisterRoutes(r)
			}
			if mode == "without_routing" {
				plugins := (*s.Config.BasePlugins.Load())[:1]
				s.Config.BasePlugins.Store(&plugins)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ctx := &fasthttp.RequestCtx{}
				ctx.Request.Header.SetMethod("GET")
				ctx.Request.SetRequestURI("/v1/models")
				ctx.Request.Header.Set("x-bf-vk", "sk-bf-reader")
				r.Handler(ctx)
				if ctx.Response.StatusCode() != http.StatusOK || !bytes.Contains(ctx.Response.Body(), []byte(`"openai/physical"`)) {
					b.Fatalf("provider model missing: %s", ctx.Response.Body())
				}
				if bytes.Contains(ctx.Response.Body(), []byte(`"Frontier"`)) != (mode == "with_routing_names") {
					b.Fatalf("unexpected routing model: %s", ctx.Response.Body())
				}
			}
		})
	}
}

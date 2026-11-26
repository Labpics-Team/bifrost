package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/fasthttp/router"
	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/plugins/governance"
	"github.com/maximhq/bifrost/plugins/routing"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/handlers"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestRoutingModelsHTTPAdmission(t *testing.T) {
	log := bifrost.NewNoOpLogger()
	SetLogger(log)
	handlers.SetLogger(log)
	config := &lib.Config{ClientConfig: &configstore.ClientConfig{}}
	newKey := func(id string) tables.TableVirtualKey {
		return tables.TableVirtualKey{ID: id, Name: id, Value: *schemas.NewSecretVar("sk-bf-" + id), IsActive: bifrost.Ptr(true), ProviderConfigs: []tables.TableVirtualKeyProviderConfig{{Provider: "openai", AllowedModels: schemas.WhiteList{"*"}, AllowAllKeys: true}}}
	}
	active, inactive, expired := newKey("active"), newKey("inactive"), newKey("expired")
	rateLimited, budgetExhausted := newKey("rate-limited"), newKey("budget-exhausted")
	now := time.Now()
	rate := tables.TableRateLimit{
		ID: "spent-rate", CreatedAt: now, RequestLastReset: now,
		RequestMaxLimit: bifrost.Ptr(int64(1)), RequestCurrentUsage: 1, RequestResetDuration: bifrost.Ptr("1h"),
	}
	rateLimited.RateLimitID, rateLimited.RateLimit = &rate.ID, &rate
	budget := tables.TableBudget{
		ID: "spent-budget", VirtualKeyID: &budgetExhausted.ID, CreatedAt: now, LastReset: now,
		MaxLimit: 1, CurrentUsage: 1, ResetDuration: "1h",
	}
	budgetExhausted.Budgets = []tables.TableBudget{budget}
	inactive.IsActive = bifrost.Ptr(false)
	expired.ExpiresAt = bifrost.Ptr(time.Now().Add(-time.Hour))
	mandatory, headers := true, []string{"x-catalog-client"}
	gov, err := governance.Init(context.Background(), &governance.Config{IsVkMandatory: &mandatory, RequiredHeaders: &headers}, log, nil, &configstore.GovernanceConfig{
		VirtualKeys: []tables.TableVirtualKey{active, inactive, expired, rateLimited, budgetExhausted},
		RateLimits:  []tables.TableRateLimit{rate}, Budgets: []tables.TableBudget{budget},
	}, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, gov.Cleanup()) })
	routes, err := routing.Init(context.Background(), nil, log, nil, gov)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, routes.Cleanup()) })
	for _, name := range []string{"B", "A"} {
		require.NoError(t, routes.GetRuleStore().UpsertRule(context.Background(), &tables.TableRoutingRule{ID: name, Scope: "global", CelExpression: `model == "` + name + `"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical"), Weight: 1}}}))
	}
	// The same HTTP catalog must exclude declarations that are not public
	// terminal names and targets the caller cannot access.
	for _, rule := range []tables.TableRoutingRule{
		{ID: "denied-provider", Scope: "global", CelExpression: `model == "Denied"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("anthropic"), Model: bifrost.Ptr("physical"), Weight: 1}}},
		{ID: "conditional", Scope: "global", CelExpression: `model == "Conditional" && request_type == "chat_completion"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical"), Weight: 1}}},
		{ID: "disabled", Scope: "global", CelExpression: `model == "Disabled"`, Enabled: bifrost.Ptr(false), Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical"), Weight: 1}}},
		{ID: "chained", Scope: "global", CelExpression: `model == "Chained"`, ChainRule: true, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical"), Weight: 1}}},
	} {
		require.NoError(t, routes.GetRuleStore().UpsertRule(context.Background(), &rule))
	}
	plugins := []schemas.BasePlugin{gov, routes}
	config.BasePlugins.Store(&plugins)
	s := &BifrostHTTPServer{Config: config, Ctx: schemas.NewBifrostContext(context.Background(), time.Time{})}
	httpRouter := router.New()
	// No core client or provider exists: any provider invocation would panic.
	handlers.NewInferenceHandler(s, nil, config).RegisterRoutes(httpRouter)
	read := func(key, query string, sendHeader bool) (int, []byte) {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI("/v1/routing/models" + query)
		if key != "" {
			ctx.Request.Header.Set("x-bf-vk", key)
		}
		if sendHeader {
			ctx.Request.Header.Set("x-catalog-client", "test")
		}
		httpRouter.Handler(ctx)
		return ctx.Response.StatusCode(), append([]byte(nil), ctx.Response.Body()...)
	}
	status, body := read("sk-bf-active", "?page_size=1", true)
	require.Equal(t, 200, status, string(body))
	var result schemas.BifrostListModelsResponse
	require.NoError(t, json.Unmarshal(body, &result))
	require.Equal(t, []schemas.Model{{ID: "A"}}, result.Data)
	require.NotEmpty(t, result.NextPageToken)
	status, body = read("sk-bf-active", "?page_size=1&page_token="+result.NextPageToken, true)
	require.Equal(t, 200, status, string(body))
	result = schemas.BifrostListModelsResponse{}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Equal(t, []schemas.Model{{ID: "B"}}, result.Data)
	require.Empty(t, result.NextPageToken)
	status, body = read("sk-bf-active", "", true)
	require.Equal(t, 200, status, string(body))
	result = schemas.BifrostListModelsResponse{}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Equal(t, []schemas.Model{{ID: "A"}, {ID: "B"}}, result.Data)
	require.Empty(t, result.NextPageToken)
	for _, tc := range []struct {
		key    string
		header bool
		status int
	}{{"", true, 401}, {"sk-bf-missing", true, 401}, {"sk-bf-inactive", true, 403}, {"sk-bf-expired", true, 403}, {"sk-bf-active", false, 400}} {
		status, body := read(tc.key, "", tc.header)
		require.Equal(t, tc.status, status, string(body))
	}
	for _, tc := range []struct {
		key             string
		inferenceStatus int
	}{{"sk-bf-rate-limited", 429}, {"sk-bf-budget-exhausted", 402}} {
		t.Run(tc.key, func(t *testing.T) {
			status, body := read(tc.key, "", true)
			require.Equal(t, 200, status, string(body))
			var catalog schemas.BifrostListModelsResponse
			require.NoError(t, json.Unmarshal(body, &catalog))
			require.Equal(t, []schemas.Model{{ID: "A"}, {ID: "B"}}, catalog.Data)

			// Reusing the caller context after catalog discovery must still enforce
			// inference spending limits; metadata cannot grant an exemption to it.
			httpCtx := &fasthttp.RequestCtx{}
			httpCtx.Request.Header.Set("x-bf-vk", tc.key)
			httpCtx.Request.Header.Set("x-catalog-client", "test")
			caller, cancel := lib.ConvertToBifrostContext(httpCtx, config)
			defer cancel()
			_, catalogErr := s.ListRoutingModels(caller, &schemas.BifrostListModelsRequest{})
			require.Nil(t, catalogErr)
			_, short, hookErr := gov.PreLLMHook(caller, &schemas.BifrostRequest{
				RequestType: schemas.ChatCompletionRequest,
				ChatRequest: &schemas.BifrostChatRequest{Provider: schemas.OpenAI, Model: "physical"},
			})
			require.NoError(t, hookErr)
			require.NotNil(t, short)
			require.NotNil(t, short.Error)
			require.NotNil(t, short.Error.StatusCode)
			require.Equal(t, tc.inferenceStatus, *short.Error.StatusCode)
		})
	}
	require.Equal(t, int64(1), gov.GetGovernanceStore().LoadRateLimit(context.Background(), rate.ID).RequestCurrentUsage)
	require.Equal(t, float64(1), gov.GetGovernanceStore().LoadBudget(context.Background(), budget.ID).CurrentUsage)
	gov.GetGovernanceStore().DeleteVirtualKeyInMemory(context.Background(), active.ID)
	status, body = read("sk-bf-active", "", true)
	require.Equal(t, 401, status, string(body))
}

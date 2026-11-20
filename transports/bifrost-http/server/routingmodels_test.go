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
	inactive.IsActive = bifrost.Ptr(false)
	expired.ExpiresAt = bifrost.Ptr(time.Now().Add(-time.Hour))
	mandatory, headers := true, []string{"x-catalog-client"}
	gov, err := governance.Init(context.Background(), &governance.Config{IsVkMandatory: &mandatory, RequiredHeaders: &headers}, log, nil, &configstore.GovernanceConfig{VirtualKeys: []tables.TableVirtualKey{active, inactive, expired}}, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, gov.Cleanup()) })
	routes, err := routing.Init(context.Background(), nil, log, nil, gov)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, routes.Cleanup()) })
	for _, name := range []string{"B", "A"} {
		require.NoError(t, routes.GetRuleStore().UpsertRule(context.Background(), &tables.TableRoutingRule{ID: name, Scope: "global", CelExpression: `model == "` + name + `"`, Targets: []tables.TableRoutingTarget{{Provider: bifrost.Ptr("openai"), Model: bifrost.Ptr("physical"), Weight: 1}}}))
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
	require.NoError(t, json.Unmarshal(body, &result))
	require.Equal(t, []schemas.Model{{ID: "B"}}, result.Data)
	for _, tc := range []struct {
		key    string
		header bool
		status int
	}{{"", true, 401}, {"sk-bf-missing", true, 401}, {"sk-bf-inactive", true, 403}, {"sk-bf-expired", true, 403}, {"sk-bf-active", false, 400}} {
		status, body := read(tc.key, "", tc.header)
		require.Equal(t, tc.status, status, string(body))
	}
	gov.GetGovernanceStore().DeleteVirtualKeyInMemory(context.Background(), active.ID)
	status, body = read("sk-bf-active", "", true)
	require.Equal(t, 401, status, string(body))
}

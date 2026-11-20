package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/fasthttp/router"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/maximhq/bifrost/framework/vectorstore"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

type cacheAvailabilityConfigStore struct {
	configstore.ConfigStore
	err error
}

func (s *cacheAvailabilityConfigStore) Ping(context.Context) error { return s.err }

type cacheAvailabilityLogsStore struct {
	logstore.LogStore
	err error
}

func (s *cacheAvailabilityLogsStore) Ping(context.Context) error { return s.err }

type cacheAvailabilityVectorStore struct {
	vectorstore.VectorStore
	err   error
	pings int
}

func (s *cacheAvailabilityVectorStore) Ping(context.Context) error { s.pings++; return s.err }

func TestCacheAvailabilityReadinessAndDiagnosticHealth(t *testing.T) {
	vector := &cacheAvailabilityVectorStore{err: errors.New("synthetic unavailable vector store")}
	config := &lib.Config{ClientConfig: &configstore.ClientConfig{}, ConfigStore: &cacheAvailabilityConfigStore{}, LogsStore: &cacheAvailabilityLogsStore{}, VectorStore: vector}
	r := router.New()
	NewHealthHandler(config).RegisterRoutes(r)
	call := func(path string) int {
		var ctx fasthttp.RequestCtx
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(path)
		r.Handler(&ctx)
		return ctx.Response.StatusCode()
	}
	if code := call("/readyz"); code != 200 || vector.pings != 0 {
		t.Fatalf("optional vector store affected readiness: status=%d pings=%d", code, vector.pings)
	}
	if code := call("/health"); code != 503 || vector.pings != 1 {
		t.Fatal("diagnostic health hid the vector-store failure")
	}
	config.ConfigStore = &cacheAvailabilityConfigStore{err: errors.New("synthetic config failure")}
	if code := call("/readyz"); code != 503 {
		t.Fatal("required config store failure did not remove readiness")
	}
	config.ConfigStore = &cacheAvailabilityConfigStore{}
	config.LogsStore = &cacheAvailabilityLogsStore{err: errors.New("synthetic logs failure")}
	config.ClientConfig.DisableDBPingsInHealth = true
	if code := call("/readyz"); code != 503 {
		t.Fatal("diagnostic ping opt-out concealed required store failure")
	}
}

func TestCacheAvailabilityReadinessKeepsAdminAuthentication(t *testing.T) {
	config := &lib.Config{ClientConfig: &configstore.ClientConfig{}}
	middleware := &AuthMiddleware{}
	middleware.UpdateAuthConfig(&configstore.AuthConfig{IsEnabled: true, AdminUserName: schemas.NewSecretVar("synthetic-admin"), AdminPassword: schemas.NewSecretVar("synthetic-password")})
	r := router.New()
	NewHealthHandler(config).RegisterRoutes(r, middleware.APIMiddleware())
	r.GET("/api/config", lib.ChainMiddlewares(func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(200) }, middleware.APIMiddleware()))
	call := func(path string) int {
		var ctx fasthttp.RequestCtx
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(path)
		r.Handler(&ctx)
		return ctx.Response.StatusCode()
	}
	if call("/readyz") != 200 {
		t.Fatal("kubelet readiness requires administrator credentials")
	}
	if call("/api/config") != 401 {
		t.Fatal("readiness registration bypassed administrator authentication")
	}
	for _, path := range []string{"/readyz/api/config", "/readyzX"} {
		if middleware.isAPIRouteWhitelisted(path) {
			t.Fatalf("readiness auth exemption is not exact: %q", path)
		}
	}
}

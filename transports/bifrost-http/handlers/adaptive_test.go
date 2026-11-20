package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/fasthttp/router"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/plugins/routing/adaptive"
	"github.com/valyala/fasthttp"
	"gorm.io/gorm"
)

type adaptiveStore struct {
	configstore.ConfigStore
	value string
	fail  bool
}

func (s *adaptiveStore) GetConfig(_ context.Context, key string) (*tables.TableGovernanceConfig, error) {
	if s.value == "" {
		return nil, configstore.ErrNotFound
	}
	return &tables.TableGovernanceConfig{Key: key, Value: s.value}, nil
}
func (s *adaptiveStore) UpdateConfig(_ context.Context, row *tables.TableGovernanceConfig, _ ...*gorm.DB) error {
	if s.fail {
		return errors.New("offline")
	}
	s.value = row.Value
	return nil
}
func adaptiveRequest(r *router.Router, method, path, body string) *fasthttp.Response {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI(path)
	ctx.Request.SetBodyString(body)
	r.Handler(ctx)
	return &ctx.Response
}

func TestAdaptiveRoutingAPISettingsArePersistentAndExplicit(t *testing.T) {
	s := &adaptiveStore{}
	c, err := adaptive.NewFromStore(context.Background(), s, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := router.New()
	RegisterAdaptiveRoutingRoutes(r, c)
	resp := adaptiveRequest(r, "GET", "/api/adaptive-routing/status", "")
	var status struct{ Enabled bool }
	if err := json.Unmarshal(resp.Body(), &status); err != nil || status.Enabled {
		t.Fatalf("unexpected initial state: %s", resp.Body())
	}
	resp = adaptiveRequest(r, "PUT", "/api/adaptive-routing/config", `{"direction_selection_enabled":true}`)
	if resp.StatusCode() != 400 || c.Enabled() {
		t.Fatal("partial replacement accepted")
	}
	config := adaptive.DefaultConfig()
	data, _ := json.Marshal(config)
	s.fail = true
	resp = adaptiveRequest(r, "PUT", "/api/adaptive-routing/config", string(data))
	if resp.StatusCode() != 500 || c.Enabled() {
		t.Fatal("failed write changed routing")
	}
	s.fail = false
	resp = adaptiveRequest(r, "PUT", "/api/adaptive-routing/config", string(data))
	if resp.StatusCode() != 200 || !c.Enabled() {
		t.Fatalf("settings not enabled: %s", resp.Body())
	}
	loaded, err := adaptive.NewFromStore(context.Background(), s, nil)
	if err != nil || loaded.Config() != config {
		t.Fatalf("settings did not survive reload: %v", err)
	}
}

func TestAdaptiveRoutingAPIRoutesUseAdminMiddleware(t *testing.T) {
	c := adaptive.New(adaptive.DefaultConfig())
	r := router.New()
	deny := func(next fasthttp.RequestHandler) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(401) }
	}
	RegisterAdaptiveRoutingRoutes(r, c, schemas.BifrostHTTPMiddleware(deny))
	for _, test := range []struct{ method, path string }{{"GET", "config"}, {"PUT", "config"}, {"GET", "status"}, {"GET", "history"}, {"POST", "conditions/clear"}} {
		resp := adaptiveRequest(r, test.method, "/api/adaptive-routing/"+test.path, "{}")
		if resp.StatusCode() != 401 {
			t.Fatalf("%s %s bypassed admin middleware", test.method, test.path)
		}
	}
}

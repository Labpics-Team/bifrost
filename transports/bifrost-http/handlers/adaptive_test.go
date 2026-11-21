package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/fasthttp/router"
	bifrost "github.com/maximhq/bifrost/core"
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

func newAdaptiveStore(t *testing.T) *adaptiveStore {
	t.Helper()
	store, err := configstore.NewConfigStore(context.Background(), &configstore.Config{
		Enabled: true, Type: configstore.ConfigStoreTypeSQLite,
		Config: &configstore.SQLiteConfig{Path: filepath.Join(t.TempDir(), "config.db")},
	}, bifrost.NewDefaultLogger(schemas.LogLevelError))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	return &adaptiveStore{ConfigStore: store}
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
	var request fasthttp.Request
	request.Header.SetMethod(method)
	request.SetRequestURI(path)
	request.SetBodyString(body)
	ctx := &fasthttp.RequestCtx{}
	ctx.Init(&request, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, nil)
	r.Handler(ctx)
	return &ctx.Response
}

func TestAdaptiveRoutingAPISettingsArePersistentAndExplicit(t *testing.T) {
	s := newAdaptiveStore(t)
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
	for _, test := range []struct{ method, path string }{{"GET", "config"}, {"PUT", "config"}, {"GET", "status"}, {"GET", "incidents"}, {"GET", "incidents/example"}, {"POST", "conditions/clear"}} {
		resp := adaptiveRequest(r, test.method, "/api/adaptive-routing/"+test.path, "{}")
		if resp.StatusCode() != 401 {
			t.Fatalf("%s %s bypassed admin middleware", test.method, test.path)
		}
	}
}

func TestAdaptiveIncidentQueryPreservesFiltersAndTime(t *testing.T) {
	args := &fasthttp.Args{}
	args.Set("provider", "custom/provider")
	args.Set("model", "model-with/slash")
	args.Set("key_id", "key-id")
	args.Set("q", "Team key")
	args.Set("status", "needs_attention")
	args.Set("scope", "key")
	args.Set("sort", "attention")
	args.Set("cursor", "opaque-token")
	args.Set("from", "2026-11-21T12:00:00.123456789+03:00")
	args.Set("to", "2026-11-21T10:00:00Z")
	query, err := adaptiveIncidentQuery(args)
	if err != nil {
		t.Fatal(err)
	}
	expectedFrom := time.Date(2026, 11, 21, 9, 0, 0, 123456789, time.UTC)
	if query.Provider != "custom/provider" || query.Model != "model-with/slash" || query.KeyID != "key-id" || query.Search != "Team key" ||
		query.Status != "needs_attention" || query.Scope != "key" || query.Sort != "attention" || query.Cursor != "opaque-token" || query.Limit != 25 ||
		!query.From.Equal(expectedFrom) || !query.To.Equal(time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("query changed semantics: %+v", query)
	}
}

func TestAdaptiveIncidentAPIRejectsInvalidFilters(t *testing.T) {
	c := adaptive.New(adaptive.DefaultConfig())
	r := router.New()
	RegisterAdaptiveRoutingRoutes(r, c)
	for _, query := range []string{
		"limit=0", "limit=-1", "limit=101", "limit=abc", "limit=", "sort=random", "status=healthy", "scope=everything", "from=yesterday", "to=invalid",
		"from=2026-11-22T00:00:00Z&to=2026-11-21T00:00:00Z",
	} {
		t.Run(query, func(t *testing.T) {
			response := adaptiveRequest(r, "GET", "/api/adaptive-routing/incidents?"+query, "")
			if response.StatusCode() != 400 {
				t.Fatalf("invalid query accepted: %d %s", response.StatusCode(), response.Body())
			}
		})
	}
	response := adaptiveRequest(r, "GET", "/api/adaptive-routing/incidents/example?limit=101", "")
	if response.StatusCode() != 400 {
		t.Fatal("incident detail accepted unbounded page")
	}
}

func TestAdaptiveIncidentAPIReadsDurableLifecycle(t *testing.T) {
	store := newAdaptiveStore(t)
	durable, err := configstore.NewAdaptiveRoutingStore(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	err = durable.Apply(context.Background(), configstore.AdaptiveMutation{
		Revision:        1,
		UpsertIncidents: []configstore.Incident{{ID: "incident-1", Scope: "key_model", Provider: "openai", Model: "model", KeyID: "key", Status: "recovering", State: "recovering", WorstState: "held", OpenedAt: at, UpdatedAt: at, LastSeen: at, Summary: "Rate limited", Action: "Checking recovery", FirstRequestID: "request-1"}},
		Events:          []configstore.IncidentEvent{{ID: "event-1", IncidentID: "incident-1", Timestamp: at, Kind: "opened", RequestID: "request-1", Message: "Held the key"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := adaptive.NewFromStore(context.Background(), store, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := router.New()
	RegisterAdaptiveRoutingRoutes(r, c)
	response := adaptiveRequest(r, "GET", "/api/adaptive-routing/incidents?status=recovering&provider=openai", "")
	var page adaptive.IncidentPage
	if err := json.Unmarshal(response.Body(), &page); err != nil || response.StatusCode() != 200 || page.Total != 1 || len(page.Incidents) != 1 || page.Incidents[0].ID != "incident-1" {
		t.Fatalf("durable incident not returned: %s, %v", response.Body(), err)
	}
	response = adaptiveRequest(r, "GET", "/api/adaptive-routing/incidents/incident-1", "")
	var detail adaptive.IncidentDetail
	if err := json.Unmarshal(response.Body(), &detail); err != nil || response.StatusCode() != 200 || len(detail.Events) != 1 || detail.Events[0].RequestID != "request-1" {
		t.Fatalf("durable evidence not returned: %s, %v", response.Body(), err)
	}
	for _, test := range []struct {
		path string
		code int
	}{{"incidents/missing", 404}, {"incidents?cursor=invalid", 400}, {"incidents/incident-1?cursor=invalid", 400}} {
		response = adaptiveRequest(r, "GET", "/api/adaptive-routing/"+test.path, "")
		if response.StatusCode() != test.code {
			t.Fatalf("%s: got %d, want %d", test.path, response.StatusCode(), test.code)
		}
	}
}

package lib

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/maximhq/bifrost/framework"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/modelcatalog"
)

// Config tests exercise catalog initialization and persistence, not the size or
// changing contents of the public feeds. Configure a local server through the
// normal catalog URLs so URL validation, HTTP transport and parsing all run.
// Intercepting only the transport leaves the preceding DNS lookup unisolated.
var catalogFixtureURL string

var catalogFixtureFeeds = map[string]*struct {
	body string
	hits atomic.Int64
}{
	"/pricing":     {body: `{"gpt-4o-mini":{"provider":"openai","mode":"chat","input_cost_per_token":0.00000015,"output_cost_per_token":0.0000006}}`},
	"/parameters":  {body: `{"gpt-4o-mini":{"supports_reasoning":false,"supports_sampling_params":true}}`},
	"/mcp-library": {body: `{"servers":[]}`},
}

func catalogFixtureConfig() *framework.FrameworkConfig {
	pricingURL := catalogFixtureURL + "/pricing"
	parametersURL := catalogFixtureURL + "/parameters"
	mcpURL := catalogFixtureURL + "/mcp-library"
	return &framework.FrameworkConfig{Pricing: &modelcatalog.Config{
		PricingURL:         &pricingURL,
		ModelParametersURL: &parametersURL,
		MCPLibraryURL:      &mcpURL,
	}}
}

func TestMain(m *testing.M) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		feed, ok := catalogFixtureFeeds[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		feed.hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(feed.body))
	}))
	catalogFixtureURL = server.URL
	code := m.Run()
	server.Close()
	os.Exit(code)
}

// A fresh boot must fetch every configured feed. Using the seeded database here
// would let an already persisted catalog hide a broken fixture or loader.
func TestLoadConfigServesCatalogFeedsFromFixture(t *testing.T) {
	initTestLogger()
	tempDir := t.TempDir()
	providers := map[string]configstore.ProviderConfig{
		"openai": makeProviderConfigWithNetwork("openai-key-1", "sk-test-123", "https://api.openai.com"),
	}
	data := makeConfigDataWithProvidersAndDir(providers, tempDir)
	data.FrameworkConfig = catalogFixtureConfig()
	createConfigFile(t, tempDir, data)

	before := make(map[string]int64, len(catalogFixtureFeeds))
	for path, feed := range catalogFixtureFeeds {
		before[path] = feed.hits.Load()
	}
	ctx := context.Background()
	config, err := LoadConfig(ctx, tempDir)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	defer config.Close(ctx)
	if config.ModelCatalog == nil {
		t.Fatal("LoadConfig did not initialize the catalog")
	}
	for path, feed := range catalogFixtureFeeds {
		if feed.hits.Load() == before[path] {
			t.Errorf("LoadConfig did not fetch the configured %s feed", path)
		}
	}
}

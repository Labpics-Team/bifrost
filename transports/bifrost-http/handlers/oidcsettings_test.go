package handlers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestOIDCSettings(t *testing.T) {
	SetLogger(&mockLogger{})
	store := newRealOAuth2Store(t)
	cfg := newTestOAuth2Config(store, tables.MCPServerAuthModeHeaders, false)
	h := &ConfigHandler{store: cfg, configManager: setupConfigManager{store: store, validToken: true}}
	conf := &configstore.OIDCConfig{Enabled: true, IssuerURL: "https://id.example", ClientID: "bifrost", ClientSecret: schemas.NewSecretVar("literal-secret"), RedirectURL: "https://ai.example/api/session/oidc/callback", AllowedSubjects: []string{"admin"}}
	save := func(auth any) *fasthttp.RequestCtx {
		body, err := json.Marshal(map[string]any{"client_config": map[string]any{"log_retention_days": 7}, "auth_config": auth})
		require.NoError(t, err)
		ctx := putConfigCtx(string(body))
		h.updateConfig(ctx)
		return ctx
	}
	response := save(&configstore.AuthConfig{IsEnabled: true, OIDC: conf})
	require.Equal(t, 200, response.Response.StatusCode(), string(response.Response.Body()))
	loaded, err := store.GetAuthConfig(context.Background())
	require.NoError(t, err)
	require.True(t, loaded.OIDCEnabled())
	require.False(t, loaded.HasPassword())
	require.Equal(t, "literal-secret", loaded.OIDC.ClientSecret.GetValue())
	get := &fasthttp.RequestCtx{}
	get.Init(&fasthttp.Request{}, nil, nil)
	h.getConfig(get)
	require.Equal(t, 200, get.Response.StatusCode(), string(get.Response.Body()))
	require.NotContains(t, string(get.Response.Body()), "literal-secret")
	var body struct {
		AuthConfig *configstore.AuthConfig `json:"auth_config"`
	}
	require.NoError(t, json.Unmarshal(get.Response.Body(), &body))
	// The exact redacted settings sent back by the UI retain the secret.
	response = save(body.AuthConfig)
	require.Equal(t, 200, response.Response.StatusCode(), string(response.Response.Body()))
	loaded, err = store.GetAuthConfig(context.Background())
	require.NoError(t, err)
	require.Equal(t, "literal-secret", loaded.OIDC.ClientSecret.GetValue())
	require.Empty(t, loaded.AdminPassword.GetValue())
	// Older clients omit OIDC; their auth update must not erase it.
	response = save(map[string]any{"is_enabled": true})
	require.Equal(t, 200, response.Response.StatusCode(), string(response.Response.Body()))
	loaded, err = store.GetAuthConfig(context.Background())
	require.NoError(t, err)
	require.True(t, loaded.OIDCEnabled())
	require.NoError(t, store.CreateSession(context.Background(), &tables.SessionsTable{Token: "active", ExpiresAt: time.Now().Add(time.Hour)}))
	loaded.OIDC.AllowedSubjects = []string{"new-admin"}
	response = save(loaded)
	require.Equal(t, 200, response.Response.StatusCode(), string(response.Response.Body()))
	session, err := store.GetSession(context.Background(), "active")
	require.NoError(t, err)
	require.Nil(t, session)
	response = save(map[string]any{"is_enabled": true, "oidc": nil})
	require.Equal(t, 400, response.Response.StatusCode(), string(response.Response.Body()))
}

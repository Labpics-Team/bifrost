package lib

import (
	"context"
	"testing"

	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/stretchr/testify/require"
)

func TestLoadOIDCOnlyConfig(t *testing.T) {
	initTestLogger()
	store := NewMockConfigStore()
	cfg := &Config{ConfigStore: store}
	data := &ConfigData{Governance: &configstore.GovernanceConfig{AuthConfig: &configstore.AuthConfig{
		IsEnabled: true,
		OIDC:      &configstore.OIDCConfig{Enabled: true, IssuerURL: "https://id.example", ClientID: "bifrost", RedirectURL: "https://ai.example/api/session/oidc/callback", AllowedSubjects: []string{"admin"}},
	}}}
	require.NoError(t, loadClientConfig(context.Background(), cfg, data))
	require.NoError(t, loadAuthConfig(context.Background(), cfg, data))
	require.True(t, cfg.GovernanceConfig.AuthConfig.OIDCEnabled())
	require.False(t, cfg.GovernanceConfig.AuthConfig.HasPassword())
	require.True(t, cfg.ClientConfig.EnforceAuthOnInference)
	// An unchanged restart retains the OIDC-only configuration and sessions.
	store.flushSessionsCalled = false
	require.NoError(t, loadAuthConfig(context.Background(), cfg, data))
	require.False(t, store.flushSessionsCalled)
	data.Governance.AuthConfig.OIDC = data.Governance.AuthConfig.OIDC.Clone()
	data.Governance.AuthConfig.OIDC.AllowedSubjects = []string{"different-admin"}
	require.NoError(t, loadAuthConfig(context.Background(), cfg, data))
	require.True(t, store.flushSessionsCalled)
	store.flushSessionsCalled = false
	data.Governance.AuthConfig = &configstore.AuthConfig{IsEnabled: false, OIDC: data.Governance.AuthConfig.OIDC.Clone()}
	require.NoError(t, loadAuthConfig(context.Background(), cfg, data))
	loaded, err := store.GetAuthConfig(context.Background())
	require.NoError(t, err)
	require.False(t, loaded.IsEnabled, "a disabled OIDC-only file configuration must reach the store")
	require.True(t, store.flushSessionsCalled)
}

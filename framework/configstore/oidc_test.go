package configstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/encrypt"
	"github.com/stretchr/testify/require"
)

func TestOIDCConfig(t *testing.T) {
	conf := &OIDCConfig{Enabled: true, IssuerURL: "https://id.example", ClientID: "bifrost", RedirectURL: "https://ai.example/api/session/oidc/callback", AllowedSubjects: []string{"admin"}, AllowedEmails: []string{"admin@example.com"}}
	require.NoError(t, conf.Validate())
	require.True(t, conf.Allows("admin", "", false))
	require.True(t, conf.Allows("other", "ADMIN@example.com", true))
	require.False(t, conf.Allows("other", "admin@example.com", false))
	require.False(t, conf.Allows("", "admin@example.com", true))
	for _, mutate := range []func(*OIDCConfig){
		func(c *OIDCConfig) { c.IssuerURL = "http://id.example" },
		func(c *OIDCConfig) { c.RedirectURL = "https://ai.example/wrong" },
		func(c *OIDCConfig) { c.ClientID = "" },
		func(c *OIDCConfig) { c.AllowedSubjects = nil; c.AllowedEmails = nil },
		func(c *OIDCConfig) { c.AllowedEmails = []string{"Name <admin@example.com>"} },
	} {
		copy := conf.Clone()
		mutate(copy)
		require.Error(t, copy.Validate())
	}
	t.Setenv("BIFROST_TEST_OIDC_SECRET", "resolved-secret")
	conf.ClientSecret = schemas.NewSecretVar("env.BIFROST_TEST_OIDC_SECRET")
	encoded, err := encodeOIDCConfig(conf)
	require.NoError(t, err)
	require.False(t, strings.Contains(encoded, "resolved-secret"))
	decoded, err := decodeOIDCConfig(encoded)
	require.NoError(t, err)
	require.Equal(t, "resolved-secret", decoded.ClientSecret.GetValue())
	require.True(t, conf.Equals(decoded))
	require.NotEqual(t, "resolved-secret", conf.Redacted().ClientSecret.GetValue())
	decoded.AllowedSubjects[0] = "other"
	require.Equal(t, "admin", conf.AllowedSubjects[0])
}

func TestOIDCConfigStoreRoundTrip(t *testing.T) {
	store := setupRDBTestStore(t)
	require.NoError(t, store.DB().AutoMigrate(&tables.SessionsTable{}))
	conf := &AuthConfig{IsEnabled: true, OIDC: &OIDCConfig{Enabled: true, IssuerURL: "https://id.example", ClientID: "bifrost", ClientSecret: schemas.NewSecretVar("literal-secret"), RedirectURL: "https://ai.example/api/session/oidc/callback", AllowedSubjects: []string{"admin"}}}
	require.NoError(t, store.UpdateAuthConfig(context.Background(), conf))
	loaded, err := store.GetAuthConfig(context.Background())
	require.NoError(t, err)
	require.True(t, loaded.OIDCEnabled())
	require.False(t, loaded.HasPassword())
	require.True(t, conf.OIDC.Equals(loaded.OIDC))
	require.NoError(t, store.CreateSession(context.Background(), &tables.SessionsTable{Token: "unchanged-policy", ExpiresAt: time.Now().Add(time.Hour)}))
	conf.OIDC.AllowedEmails = []string{}
	require.NoError(t, store.UpdateAuthConfig(context.Background(), conf))
	session, err := store.GetSession(context.Background(), "unchanged-policy")
	require.NoError(t, err)
	require.NotNil(t, session, "empty and omitted allowlists describe the same policy")
}

func TestOIDCConfigEncryption(t *testing.T) {
	require.True(t, encrypt.IsEnabled(), "use the package's shared encryption fixture")
	conf := &OIDCConfig{ClientSecret: schemas.NewSecretVar("stored-client-secret")}
	encoded, err := encodeOIDCConfig(conf)
	require.NoError(t, err)
	require.NotContains(t, encoded, "stored-client-secret")
	decoded, err := decodeOIDCConfig(encoded)
	require.NoError(t, err)
	require.Equal(t, "stored-client-secret", decoded.ClientSecret.GetValue())
}

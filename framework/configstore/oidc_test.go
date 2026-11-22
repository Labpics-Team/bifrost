package configstore

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	bifrost "github.com/maximhq/bifrost/core"
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
	require.NotContains(t, encoded, "resolved-secret")
	// Inspect the saved document before SecretVar decoding resolves its ref.
	// Ciphertext alone cannot establish whether a resolved secret was persisted.
	var envelope storedOIDCConfig
	require.NoError(t, json.Unmarshal([]byte(encoded), &envelope))
	raw := envelope.Value
	if envelope.Encrypted {
		raw, err = encrypt.Decrypt(raw)
		require.NoError(t, err)
	}
	require.NotContains(t, raw, "resolved-secret")
	var saved struct {
		Secret struct {
			Value string `json:"value"`
			Ref   string `json:"ref"`
		} `json:"client_secret"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &saved))
	require.Empty(t, saved.Secret.Value)
	require.Equal(t, "env.BIFROST_TEST_OIDC_SECRET", saved.Secret.Ref)
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

func TestOIDCAllowlistPolicyChanges(t *testing.T) {
	store := setupRDBTestStore(t)
	require.NoError(t, store.DB().AutoMigrate(&tables.SessionsTable{}))
	ctx := context.Background()
	conf := &AuthConfig{IsEnabled: true, OIDC: &OIDCConfig{Enabled: true,
		AllowedSubjects: []string{"admin", "operator"},
		AllowedEmails:   []string{"admin@example.com", "Σ@example.com"}}}
	require.NoError(t, store.UpdateAuthConfig(ctx, conf))
	require.NoError(t, store.CreateSessionIfAuthMatches(ctx, &tables.SessionsTable{Token: "before-reorder", ExpiresAt: time.Now().Add(time.Hour)}, conf))
	equivalent := *conf
	equivalent.OIDC = conf.OIDC.Clone()
	equivalent.OIDC.AllowedSubjects = []string{"operator", "admin", "admin"}
	equivalent.OIDC.AllowedEmails = []string{"ς@example.com", "ADMIN@example.com", "admin@example.com"}
	require.NoError(t, store.UpdateAuthConfig(ctx, &equivalent))
	session, err := store.GetSession(ctx, "before-reorder")
	require.NoError(t, err)
	require.NotNil(t, session, "an equivalent allowlist must keep existing sessions")
	require.NoError(t, store.CreateSessionIfAuthMatches(ctx, &tables.SessionsTable{Token: "after-reorder", ExpiresAt: time.Now().Add(time.Hour)}, conf))
	changed := equivalent
	changed.OIDC = equivalent.OIDC.Clone()
	changed.OIDC.AllowedSubjects = []string{"operator", "Admin"}
	require.NoError(t, store.UpdateAuthConfig(ctx, &changed))
	for _, token := range []string{"before-reorder", "after-reorder"} {
		session, err = store.GetSession(ctx, token)
		require.NoError(t, err)
		require.Nil(t, session, "subject case is significant and must revoke the previous policy")
	}
	require.ErrorIs(t, store.CreateSessionIfAuthMatches(ctx, &tables.SessionsTable{Token: "stale"}, conf), ErrAuthConfigChanged)
	removedEmail := conf.OIDC.Clone()
	removedEmail.AllowedEmails = []string{"admin@example.com"}
	require.False(t, conf.OIDC.Equals(removedEmail))
}

func TestOIDCEncryptionOnSQLiteReopen(t *testing.T) {
	require.True(t, encrypt.IsEnabled())
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "config.db")
	open := func() ConfigStore {
		store, err := newSqliteConfigStore(ctx, &SQLiteConfig{Path: path}, bifrost.NewDefaultLogger(schemas.LogLevelError))
		require.NoError(t, err)
		return store
	}
	store := open()
	// Model an existing plaintext installation without changing the shared
	// process encryption fixture used by other package tests.
	conf := &OIDCConfig{Enabled: true, ClientSecret: schemas.NewSecretVar("legacy-secret"), AllowedSubjects: []string{"admin"}}
	raw, err := json.Marshal(conf)
	require.NoError(t, err)
	plain, err := json.Marshal(storedOIDCConfig{Value: string(raw)})
	require.NoError(t, err)
	require.NoError(t, store.(*RDBConfigStore).DB().Create(&tables.TableGovernanceConfig{Key: tables.ConfigOIDCKey, Value: string(plain)}).Error)
	require.NoError(t, store.CreateSession(ctx, &tables.SessionsTable{Token: "existing-session", ExpiresAt: time.Now().Add(time.Hour)}))
	require.NoError(t, store.Close(ctx))
	var firstEncrypted string
	for i := 0; i < 2; i++ {
		store = open()
		var row tables.TableGovernanceConfig
		require.NoError(t, store.(*RDBConfigStore).DB().First(&row, "key = ?", tables.ConfigOIDCKey).Error)
		require.NotContains(t, row.Value, "legacy-secret")
		var envelope storedOIDCConfig
		require.NoError(t, json.Unmarshal([]byte(row.Value), &envelope))
		require.True(t, envelope.Encrypted)
		decoded, err := decodeOIDCConfig(row.Value)
		require.NoError(t, err)
		require.True(t, conf.Equals(decoded))
		session, err := store.GetSession(ctx, "existing-session")
		require.NoError(t, err)
		require.NotNil(t, session, "encryption migration must not revoke sessions")
		if i == 0 {
			firstEncrypted = row.Value
		} else {
			require.Equal(t, firstEncrypted, row.Value, "reopening must not encrypt the envelope twice")
		}
		require.NoError(t, store.Close(ctx))
	}
}

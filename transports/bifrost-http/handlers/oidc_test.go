package handlers

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/fasthttp/router"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/encrypt"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

type oidcTestStore struct {
	configstore.ConfigStore
	auth         *configstore.AuthConfig
	sessions     map[string]*tables.SessionsTable
	beforeCreate func()
}

func (s *oidcTestStore) GetAuthConfig(context.Context) (*configstore.AuthConfig, error) {
	return s.auth, nil
}
func (s *oidcTestStore) GetClientConfig(context.Context) (*configstore.ClientConfig, error) {
	return &configstore.ClientConfig{}, nil
}
func (s *oidcTestStore) CreateSessionIfAuthMatches(_ context.Context, session *tables.SessionsTable, expectedAuth *configstore.AuthConfig) error {
	if s.beforeCreate != nil {
		s.beforeCreate()
		s.beforeCreate = nil
	}
	token := session.Token
	if s.ConfigStore != nil {
		if err := s.ConfigStore.CreateSessionIfAuthMatches(context.Background(), session, expectedAuth); err != nil {
			return err
		}
	} else if expectedAuth == nil || !expectedAuth.IsEnabled || !s.auth.Equals(expectedAuth) {
		return configstore.ErrAuthConfigChanged
	}
	s.sessions[token] = session
	return nil
}
func (s *oidcTestStore) GetSession(_ context.Context, token string) (*tables.SessionsTable, error) {
	if s.ConfigStore != nil {
		return s.ConfigStore.GetSession(context.Background(), token)
	}
	return s.sessions[token], nil
}
func (s *oidcTestStore) DeleteSession(_ context.Context, token string) error {
	delete(s.sessions, token)
	if s.ConfigStore != nil {
		return s.ConfigStore.DeleteSession(context.Background(), token)
	}
	return nil
}

func TestOIDCSignIn(t *testing.T) {
	SetLogger(&mockLogger{})
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	for _, test := range []string{"success", "verified-email", "cookie", "state", "nonce", "audience", "issuer", "expiry", "signature", "unapproved", "unverified-email", "reconfigured", "reconfigured-on-insert", "expired-flow", "provider-error", "token-error", "encrypted-session"} {
		t.Run(test, func(t *testing.T) {
			lib.SetBasePath("/bifrost")
			t.Cleanup(func() { lib.SetBasePath("") })
			var issuer *httptest.Server
			var nonce, challenge string
			issuer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					json.NewEncoder(w).Encode(map[string]any{"issuer": issuer.URL, "authorization_endpoint": issuer.URL + "/authorize", "token_endpoint": issuer.URL + "/token", "jwks_uri": issuer.URL + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"}})
				case "/keys":
					json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "test", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
				case "/token":
					if test == "token-error" {
						http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
						return
					}
					require.NoError(t, r.ParseForm())
					client, secret, _ := r.BasicAuth()
					require.Equal(t, "bifrost", client)
					require.Equal(t, "test-secret", secret)
					require.Equal(t, "https://ai.example/bifrost/api/session/oidc/callback", r.Form.Get("redirect_uri"))
					pkce := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
					require.Equal(t, challenge, base64.RawURLEncoding.EncodeToString(pkce[:]))
					claims := map[string]any{"iss": issuer.URL, "aud": "bifrost", "sub": "admin", "nonce": nonce, "exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(), "email": "admin@example.com", "email_verified": true}
					switch test {
					case "nonce":
						claims["nonce"] = "other"
					case "audience":
						claims["aud"] = "other"
					case "issuer":
						claims["iss"] = "https://other.example"
					case "expiry":
						claims["exp"] = time.Now().Add(-time.Hour).Unix()
					case "unapproved":
						claims["sub"] = "other"
					case "unverified-email":
						claims["email_verified"] = false
					}
					payload, _ := json.Marshal(claims)
					unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"test"}`)) + "." + base64.RawURLEncoding.EncodeToString(payload)
					digest := sha256.Sum256([]byte(unsigned))
					signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
					require.NoError(t, err)
					if test == "signature" {
						signature[0] ^= 1
					}
					json.NewEncoder(w).Encode(map[string]any{"access_token": "access", "token_type": "Bearer", "id_token": unsigned + "." + base64.RawURLEncoding.EncodeToString(signature)})
				default:
					http.NotFound(w, r)
				}
			}))
			defer issuer.Close()
			conf := &configstore.OIDCConfig{Enabled: true, IssuerURL: issuer.URL, ClientID: "bifrost", ClientSecret: schemas.NewSecretVar("test-secret"), RedirectURL: "https://ai.example/bifrost/api/session/oidc/callback", AllowedSubjects: []string{"admin"}}
			if test == "verified-email" || test == "unverified-email" {
				conf.AllowedSubjects = nil
				conf.AllowedEmails = []string{"admin@example.com"}
			}
			store := &oidcTestStore{auth: &configstore.AuthConfig{IsEnabled: true, OIDC: conf}, sessions: map[string]*tables.SessionsTable{}}
			if test == "encrypted-session" {
				encrypt.Init("oidc-test-encryption-passphrase", &mockLogger{})
				t.Cleanup(func() { encrypt.Init("", &mockLogger{}) })
			}
			if test == "encrypted-session" || test == "reconfigured-on-insert" {
				store.ConfigStore = newRealOAuth2Store(t)
				require.NoError(t, store.ConfigStore.UpdateAuthConfig(context.Background(), store.auth))
			}
			if test == "reconfigured-on-insert" {
				store.beforeCreate = func() {
					next := *store.auth
					next.OIDC = conf.Clone()
					next.OIDC.AllowedSubjects = []string{"different-admin"}
					require.NoError(t, store.ConfigStore.UpdateAuthConfig(context.Background(), &next))
				}
			}
			h := NewSessionHandler(store, nil)
			authMiddleware, err := InitAuthMiddleware(store, nil, nil, "")
			require.NoError(t, err)
			routes := router.New()
			h.RegisterRoutes(routes, authMiddleware.APIMiddleware())
			if test == "success" {
				status := &fasthttp.RequestCtx{}
				status.Request.SetRequestURI("/api/session/is-auth-enabled")
				routes.Handler(status)
				require.Equal(t, fasthttp.StatusOK, status.Response.StatusCode())
				var body map[string]any
				require.NoError(t, json.Unmarshal(status.Response.Body(), &body))
				require.Equal(t, "oidc", body["auth_type"])
				require.Equal(t, true, body["oidc_enabled"])
				require.Equal(t, false, body["password_enabled"])
			}
			login := &fasthttp.RequestCtx{}
			destination := "/workspace/logs?model=test"
			if test == "provider-error" {
				destination = "/oauth/consent?client_id=test&response_type=code"
			}
			login.Request.SetRequestURI("https://ai.example/api/session/oidc/login?goto=" + url.QueryEscape(destination))
			routes.Handler(login)
			require.Equal(t, fasthttp.StatusSeeOther, login.Response.StatusCode())
			authorize, err := url.Parse(string(login.Response.Header.Peek("Location")))
			require.NoError(t, err)
			require.Equal(t, "S256", authorize.Query().Get("code_challenge_method"))
			nonce, challenge = authorize.Query().Get("nonce"), authorize.Query().Get("code_challenge")
			state := authorize.Query().Get("state")
			flowCookie := &fasthttp.Cookie{}
			flowCookie.SetKey(oidcFlowCookie)
			require.True(t, login.Response.Header.Cookie(flowCookie))
			require.True(t, flowCookie.Secure())
			require.True(t, flowCookie.HTTPOnly())
			cookie := string(flowCookie.Value())
			if test == "cookie" {
				cookie = "other"
			}
			if test == "state" {
				state = "other"
			}
			if test == "reconfigured" {
				store.auth.OIDC = conf.Clone()
				store.auth.OIDC.AllowedSubjects = []string{"other"}
			}
			if test == "expired-flow" {
				flow := h.oidc.flows[state]
				flow.expires = time.Now().Add(-time.Second)
				h.oidc.flows[state] = flow
			}
			callback := &fasthttp.RequestCtx{}
			callback.Init(&fasthttp.Request{}, nil, nil)
			callback.Request.SetRequestURI("https://ai.example/api/session/oidc/callback?state=" + state + "&code=code")
			callback.Request.Header.Set("X-Forwarded-Proto", "https")
			callback.Request.Header.SetCookie(oidcFlowCookie, cookie)
			callback.QueryArgs().Set("goto", "/workspace/untrusted")
			if test == "provider-error" {
				callback.QueryArgs().Set("error", "access_denied")
			}
			routes.Handler(callback)
			sessionCookie := &fasthttp.Cookie{}
			sessionCookie.SetKey("token")
			if test != "success" && test != "verified-email" && test != "encrypted-session" {
				require.Empty(t, store.sessions)
				require.False(t, callback.Response.Header.Cookie(sessionCookie))
				failureURL, err := url.Parse(string(callback.Response.Header.Peek("Location")))
				require.NoError(t, err)
				require.Equal(t, "/bifrost/login", failureURL.Path)
				require.Equal(t, "failed", failureURL.Query().Get("oidc_error"))
				if test == "cookie" || test == "state" {
					require.Empty(t, failureURL.Query().Get("goto"), "unverified callbacks cannot supply a return destination")
				} else {
					require.Equal(t, destination, failureURL.Query().Get("goto"), "retry must retain the browser-bound flow destination")
				}
				if test == "provider-error" || test == "token-error" {
					retry := &fasthttp.RequestCtx{}
					retry.Request.SetRequestURI("https://ai.example/api/session/oidc/login?goto=" + url.QueryEscape(failureURL.Query().Get("goto")))
					routes.Handler(retry)
					require.Equal(t, fasthttp.StatusSeeOther, retry.Response.StatusCode())
					authorize, err := url.Parse(string(retry.Response.Header.Peek("Location")))
					require.NoError(t, err)
					require.Equal(t, destination, h.oidc.flows[authorize.Query().Get("state")].gotoPath)
				}
				return
			}
			require.Equal(t, "/bifrost/workspace/logs?model=test", string(callback.Response.Header.Peek("Location")))
			require.True(t, callback.Response.Header.Cookie(sessionCookie))
			require.True(t, sessionCookie.Secure())
			token := string(sessionCookie.Value())
			session, err := store.GetSession(context.Background(), token)
			require.NoError(t, err)
			require.NotNil(t, session)
			require.WithinDuration(t, time.Now().Add(time.Hour), session.ExpiresAt, 3*time.Second)
			// A consumed callback never creates a second session.
			callback.Response.Reset()
			routes.Handler(callback)
			require.Len(t, store.sessions, 1)
			logout := &fasthttp.RequestCtx{}
			logout.Request.Header.SetCookie("token", token)
			h.logout(logout)
			require.Equal(t, fasthttp.StatusOK, logout.Response.StatusCode())
			require.Empty(t, store.sessions)
		})
	}
}

func TestOIDCGoto(t *testing.T) {
	for _, path := range []string{"https://evil.test", "//evil.test", "/workspace\\evil", "/workspace%5cevil", "/login", "/workspace\r\nx:y"} {
		require.Equal(t, "/workspace", safeOIDCGoto(path), path)
	}
	require.Equal(t, "/oauth/consent?client_id=x", safeOIDCGoto("/oauth/consent?client_id=x"))
}

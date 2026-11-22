package handlers

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
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
	"golang.org/x/oauth2"
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
	for _, test := range []string{"success", "https", "parallel-tabs", "equivalent-policy", "verified-email", "cookie", "state", "nonce", "audience", "issuer", "expiry", "signature", "unapproved", "unverified-email", "reconfigured", "reconfigured-on-insert", "expired-flow", "provider-error", "token-error", "token-redirect", "keys-redirect", "encrypted-session"} {
		t.Run(test, func(t *testing.T) {
			lib.SetBasePath("/bifrost")
			t.Cleanup(func() { lib.SetBasePath("") })
			var issuer *httptest.Server
			var nonce, challenge string
			var insecureRequests atomic.Int32
			insecure := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				insecureRequests.Add(1)
				http.Error(w, "unexpected plaintext request", http.StatusBadRequest)
			}))
			defer insecure.Close()
			issuer = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					json.NewEncoder(w).Encode(map[string]any{"issuer": issuer.URL, "authorization_endpoint": issuer.URL + "/authorize", "token_endpoint": issuer.URL + "/token", "jwks_uri": issuer.URL + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"}})
				case "/keys":
					if test == "keys-redirect" {
						http.Redirect(w, r, insecure.URL+"/keys", http.StatusTemporaryRedirect)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "test", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
				case "/token":
					if test == "token-redirect" {
						http.Redirect(w, r, insecure.URL+"/token", http.StatusTemporaryRedirect)
						return
					}
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
			if test == "https" || test == "token-redirect" || test == "keys-redirect" {
				issuer.StartTLS()
			} else {
				issuer.Start()
			}
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
			h.oidc.client = issuer.Client()
			h.oidc.client.Timeout = 10 * time.Second
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
			flowCookie.SetKey(oidcFlowCookie + "_" + state)
			require.True(t, login.Response.Header.Cookie(flowCookie))
			require.True(t, flowCookie.Secure())
			require.True(t, flowCookie.HTTPOnly())
			cookie := string(flowCookie.Value())
			if test == "parallel-tabs" {
				firstNonce, firstChallenge := nonce, challenge
				secondLogin := &fasthttp.RequestCtx{}
				secondLogin.Request.SetRequestURI("https://ai.example/api/session/oidc/login")
				secondLogin.Request.Header.SetCookie(string(flowCookie.Key()), cookie)
				routes.Handler(secondLogin)
				require.Equal(t, fasthttp.StatusSeeOther, secondLogin.Response.StatusCode())
				secondAuthorize, err := url.Parse(string(secondLogin.Response.Header.Peek("Location")))
				require.NoError(t, err)
				secondState := secondAuthorize.Query().Get("state")
				nonce, challenge = secondAuthorize.Query().Get("nonce"), secondAuthorize.Query().Get("code_challenge")
				secondCookie := &fasthttp.Cookie{}
				secondCookie.SetKey(oidcFlowCookie + "_" + secondState)
				require.True(t, secondLogin.Response.Header.Cookie(secondCookie))
				require.NotEqual(t, string(flowCookie.Key()), string(secondCookie.Key()))
				secondCallback := &fasthttp.RequestCtx{}
				secondCallback.Init(&fasthttp.Request{}, nil, nil)
				secondCallback.Request.SetRequestURI("https://ai.example/api/session/oidc/callback?state=" + secondState + "&code=code")
				secondCallback.Request.Header.SetCookie(string(flowCookie.Key()), cookie)
				secondCallback.Request.Header.SetCookie(string(secondCookie.Key()), string(secondCookie.Value()))
				routes.Handler(secondCallback)
				require.Equal(t, "/bifrost/workspace", string(secondCallback.Response.Header.Peek("Location")))
				require.True(t, secondCallback.Response.Header.Cookie(secondCookie))
				require.Empty(t, secondCookie.Value(), "callback consumes only its own browser binding")
				require.False(t, secondCallback.Response.Header.Cookie(flowCookie), "the first tab's cookie must remain intact")
				secondSession := &fasthttp.Cookie{}
				secondSession.SetKey("token")
				require.True(t, secondCallback.Response.Header.Cookie(secondSession))
				require.NoError(t, store.DeleteSession(context.Background(), string(secondSession.Value())))
				nonce, challenge = firstNonce, firstChallenge
			}
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
			if test == "equivalent-policy" {
				store.auth.OIDC = conf.Clone()
				store.auth.OIDC.AllowedSubjects = []string{"admin", "admin"}
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
			callback.Request.Header.SetCookie(oidcFlowCookie+"_"+state, cookie)
			callback.QueryArgs().Set("goto", "/workspace/untrusted")
			if test == "provider-error" {
				callback.QueryArgs().Set("error", "access_denied")
			}
			routes.Handler(callback)
			require.Zero(t, insecureRequests.Load(), "code exchange and JWKS redirects must never reach plaintext HTTP")
			sessionCookie := &fasthttp.Cookie{}
			sessionCookie.SetKey("token")
			if test != "success" && test != "https" && test != "parallel-tabs" && test != "equivalent-policy" && test != "verified-email" && test != "encrypted-session" {
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

func TestOIDCDiscoverySharesFailureAndRetries(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	var issuer *httptest.Server
	issuer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"issuer": issuer.URL, "authorization_endpoint": issuer.URL + "/authorize", "token_endpoint": issuer.URL + "/token", "jwks_uri": issuer.URL + "/keys"})
	}))
	defer issuer.Close()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	signIn := newOIDCSignIn()
	const attempts = 12
	errs := make(chan error, attempts)
	var started sync.WaitGroup
	started.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			started.Done()
			_, err := signIn.getProvider(issuer.URL)
			errs <- err
		}()
	}
	started.Wait()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("discovery did not reach the issuer")
	}
	releaseOnce.Do(func() { close(release) })
	for i := 0; i < attempts; i++ {
		require.Error(t, <-errs)
	}
	require.Equal(t, int32(1), calls.Load(), "concurrent failed logins must share one discovery attempt")
	signIn.providerMu.Lock()
	signIn.discovered = time.Now().Add(-6 * time.Second)
	signIn.providerMu.Unlock()
	provider, err := signIn.getProvider(issuer.URL)
	require.NoError(t, err)
	require.NotNil(t, provider)
	cached, err := signIn.getProvider(issuer.URL)
	require.NoError(t, err)
	require.Same(t, provider, cached)
	require.Equal(t, int32(2), calls.Load(), "recovery retries once, then caches the discovered provider")
}

func TestOIDCTokenExchangeRedirects(t *testing.T) {
	t.Run("canonical default port", func(t *testing.T) {
		client := newOIDCSignIn().clientForIssuer("https://id.example")
		original, err := http.NewRequest(http.MethodPost, "https://id.example/token", nil)
		require.NoError(t, err)
		canonical, err := http.NewRequest(http.MethodPost, "https://ID.example:443/final", nil)
		require.NoError(t, err)
		require.NoError(t, client.CheckRedirect(canonical, []*http.Request{original}))
		canonical.URL.Host = "id.example:8443"
		require.Error(t, client.CheckRedirect(canonical, []*http.Request{original}))
	})
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, crossOrigin := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/cross-origin=%t", status, crossOrigin), func(t *testing.T) {
				var received atomic.Int32
				tokenHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					received.Add(1)
					require.NoError(t, r.ParseForm())
					require.Equal(t, "test-code", r.Form.Get("code"))
					require.Equal(t, "test-verifier", r.Form.Get("code_verifier"))
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte(`{"access_token":"test-access","token_type":"Bearer"}`))
				})
				other := httptest.NewTLSServer(tokenHandler)
				defer other.Close()
				issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/token" {
						destination := "/final"
						if crossOrigin {
							destination = other.URL + "/final"
						}
						http.Redirect(w, r, destination, status)
						return
					}
					tokenHandler.ServeHTTP(w, r)
				}))
				defer issuer.Close()
				signIn := newOIDCSignIn()
				signIn.client = issuer.Client()
				conf := oauth2.Config{ClientID: "bifrost", ClientSecret: "test-secret", Endpoint: oauth2.Endpoint{
					TokenURL: issuer.URL + "/token", AuthStyle: oauth2.AuthStyleInParams,
				}}
				ctx := context.WithValue(context.Background(), oauth2.HTTPClient, signIn.clientForIssuer(issuer.URL))
				token, err := conf.Exchange(ctx, "test-code", oauth2.VerifierOption("test-verifier"))
				if crossOrigin {
					require.Error(t, err)
					require.Zero(t, received.Load(), "code, verifier and client secret must remain on the token origin")
				} else {
					require.NoError(t, err)
					require.Equal(t, "test-access", token.AccessToken)
					require.Equal(t, int32(1), received.Load())
				}
				// The issuer may advertise a separate HTTPS token origin directly.
				conf.Endpoint.TokenURL = other.URL + "/final"
				_, err = conf.Exchange(ctx, "test-code", oauth2.VerifierOption("test-verifier"))
				require.NoError(t, err)
			})
		}
	}
}

func TestOIDCDiscoveryRequiresSecureEndpoints(t *testing.T) {
	for _, field := range []string{"authorization_endpoint", "token_endpoint", "jwks_uri", "discovery_redirect"} {
		t.Run(field, func(t *testing.T) {
			var insecureRequests atomic.Int32
			insecure := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				insecureRequests.Add(1)
			}))
			defer insecure.Close()
			var issuer *httptest.Server
			issuer = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if field == "discovery_redirect" {
					http.Redirect(w, r, insecure.URL, http.StatusFound)
					return
				}
				metadata := map[string]any{"issuer": issuer.URL, "authorization_endpoint": issuer.URL + "/authorize", "token_endpoint": issuer.URL + "/token", "jwks_uri": issuer.URL + "/keys"}
				metadata[field] = insecure.URL
				json.NewEncoder(w).Encode(metadata)
			}))
			defer issuer.Close()
			signIn := newOIDCSignIn()
			signIn.client = issuer.Client()
			_, err := signIn.getProvider(issuer.URL)
			require.Error(t, err)
			require.Zero(t, insecureRequests.Load(), "HTTPS issuers must not expose authorization data over HTTP, including loopback")
		})
	}
}

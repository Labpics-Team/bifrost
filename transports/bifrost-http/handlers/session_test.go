package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fasthttp/router"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/encrypt"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

type authChangeDuringSessionStore struct {
	configstore.ConfigStore
	beforeCreate   func()
	attemptedToken string
}

func (s *authChangeDuringSessionStore) CreateSessionIfAuthMatches(ctx context.Context, session *tables.SessionsTable, expectedAuth *configstore.AuthConfig) error {
	s.attemptedToken = session.Token
	if s.beforeCreate != nil {
		s.beforeCreate()
		s.beforeCreate = nil
	}
	return s.ConfigStore.CreateSessionIfAuthMatches(ctx, session, expectedAuth)
}

func TestPasswordLoginUsesCurrentAuthPolicy(t *testing.T) {
	SetLogger(&mockLogger{})
	oldPassword := "OldPassword123!"
	newPassword := "NewPassword123!"
	oldHash, err := encrypt.Hash(oldPassword)
	require.NoError(t, err)
	newHash, err := encrypt.Hash(newPassword)
	require.NoError(t, err)
	for _, change := range []string{"oidc-only", "password", "unchanged"} {
		t.Run(change, func(t *testing.T) {
			store := &authChangeDuringSessionStore{ConfigStore: newRealOAuth2Store(t)}
			original := &configstore.AuthConfig{IsEnabled: true, AdminUserName: schemas.NewSecretVar("admin"), AdminPassword: schemas.NewSecretVar(oldHash)}
			require.NoError(t, store.UpdateAuthConfig(context.Background(), original))
			middleware, err := InitAuthMiddleware(store, nil, nil, "")
			require.NoError(t, err)
			routes := router.New()
			NewSessionHandler(store, nil).RegisterRoutes(routes, middleware.APIMiddleware())
			routes.GET("/api/protected", middleware.APIMiddleware()(func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }))
			store.beforeCreate = func() {
				next := &configstore.AuthConfig{IsEnabled: true, AdminUserName: schemas.NewSecretVar("admin"), AdminPassword: schemas.NewSecretVar(oldHash)}
				if change == "oidc-only" {
					next.AdminUserName, next.AdminPassword = nil, nil
					next.OIDC = &configstore.OIDCConfig{Enabled: true, IssuerURL: "https://id.example", ClientID: "bifrost", RedirectURL: "https://ai.example/api/session/oidc/callback", AllowedSubjects: []string{"admin"}}
				} else if change == "password" {
					next.AdminPassword = schemas.NewSecretVar(newHash)
				}
				require.NoError(t, store.UpdateAuthConfig(context.Background(), next))
				middleware.UpdateAuthConfig(next)
			}
			login := func(password string) *fasthttp.RequestCtx {
				ctx := &fasthttp.RequestCtx{}
				ctx.Init(&fasthttp.Request{}, nil, nil)
				ctx.Request.Header.SetMethod("POST")
				ctx.Request.SetRequestURI("/api/session/login")
				body, err := json.Marshal(map[string]string{"username": "admin", "password": password})
				require.NoError(t, err)
				ctx.Request.SetBody(body)
				routes.Handler(ctx)
				return ctx
			}
			response := login(oldPassword)
			if change == "unchanged" {
				require.Equal(t, fasthttp.StatusOK, response.Response.StatusCode(), string(response.Response.Body()))
			} else {
				assert.Equal(t, fasthttp.StatusUnauthorized, response.Response.StatusCode(), string(response.Response.Body()))
				assert.Empty(t, setCookieHeaders(response), "a policy change must not issue a session cookie")
			}
			protected := &fasthttp.RequestCtx{}
			protected.Init(&fasthttp.Request{}, nil, nil)
			protected.Request.SetRequestURI("/api/protected")
			protected.Request.Header.SetCookie("token", store.attemptedToken)
			routes.Handler(protected)
			if change == "unchanged" {
				require.Equal(t, fasthttp.StatusOK, protected.Response.StatusCode())
			} else {
				assert.Equal(t, fasthttp.StatusUnauthorized, protected.Response.StatusCode(), "the rejected sign-in must leave no usable session")
			}
			if change == "password" {
				response = login(newPassword)
				require.Equal(t, fasthttp.StatusOK, response.Response.StatusCode(), string(response.Response.Body()))
			}
		})
	}
}

// setCookieHeaders returns one Set-Cookie line per response cookie. fasthttp's
// PeekAll(Set-Cookie) joins every cookie into a single value, which would let an
// assertion about one cookie pass on the strength of another.
func setCookieHeaders(ctx *fasthttp.RequestCtx) []string {
	var out []string
	for _, line := range ctx.Response.Header.Cookies() {
		out = append(out, string(line))
	}
	return out
}

// TestSessionCookies_SecureFollowsRequestScheme pins that the session cookies take
// their Secure flag from the same scheme rule as every public URL
// (lib.RequestIsHTTPS): a proxy that only sets x-bf-forwarded-proto, or sends a
// comma-separated X-Forwarded-Proto chain, still gets a Secure cookie.
func TestSessionCookies_SecureFollowsRequestScheme(t *testing.T) {
	for name, headers := range map[string]map[string]string{
		"x-forwarded-proto":    {"X-Forwarded-Proto": "https"},
		"x-forwarded chain":    {"X-Forwarded-Proto": "https, http"},
		"x-bf-forwarded-proto": {"x-bf-forwarded-proto": "https"},
	} {
		t.Run(name, func(t *testing.T) {
			for _, write := range []struct {
				name  string
				write func(ctx *fasthttp.RequestCtx)
			}{
				{"setup session", func(ctx *fasthttp.RequestCtx) { setSetupSessionCookie(ctx, "v", time.Now().Add(time.Hour)) }},
				{"token", func(ctx *fasthttp.RequestCtx) { setSessionTokenCookie(ctx, "v", time.Now().Add(time.Hour)) }},
			} {
				ctx := &fasthttp.RequestCtx{}
				for k, v := range headers {
					ctx.Request.Header.Set(k, v)
				}
				write.write(ctx)
				cookies := setCookieHeaders(ctx)
				require.Len(t, cookies, 1, write.name)
				assert.Contains(t, strings.ToLower(cookies[0]), "; secure", write.name)
			}
		})
	}

	ctx := &fasthttp.RequestCtx{}
	setSessionTokenCookie(ctx, "v", time.Now().Add(time.Hour))
	assert.NotContains(t, strings.ToLower(setCookieHeaders(ctx)[0]), "; secure", "plain http stays non-Secure")
}

// TestSessionCookies_PathStaysRootUnderBasePath pins a deliberate decision: the
// session cookies keep Path=/ even under server.base_path. Requests are accepted
// with or without the prefix (config.schema.json), so a cookie scoped to
// "/bifrost/" would stop riding the unprefixed /api calls that contract allows,
// and fasthttp's SetCookie keys by name, so logout could not expire both paths.
func TestSessionCookies_PathStaysRootUnderBasePath(t *testing.T) {
	lib.SetBasePath("/bifrost")
	t.Cleanup(func() { lib.SetBasePath("") })
	ctx := &fasthttp.RequestCtx{}
	setSessionTokenCookie(ctx, "v", time.Now().Add(time.Hour))
	setSetupSessionCookie(ctx, "v", time.Now().Add(time.Hour))
	cookies := setCookieHeaders(ctx)
	require.Len(t, cookies, 2, "one Set-Cookie line per cookie, so each path is checked on its own")
	for _, c := range cookies {
		assert.Contains(t, strings.ToLower(c), "; path=/;", c)
		assert.NotContains(t, strings.ToLower(c), "path=/bifrost", c)
	}
}

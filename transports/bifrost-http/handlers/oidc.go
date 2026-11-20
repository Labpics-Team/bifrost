package handlers

import (
	"context"
	"crypto/subtle"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
	"github.com/maximhq/bifrost/framework/configstore"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/valyala/fasthttp"
	"golang.org/x/oauth2"
)

const oidcFlowCookie = "bifrost_oidc"

type oidcFlow struct {
	config                            *configstore.OIDCConfig
	provider                          *oidc.Provider
	verifier, nonce, cookie, gotoPath string
	expires                           time.Time
}

// OIDC is used only to establish an ordinary dashboard session. Transient
// authorization requests are single-use and bounded; restarts cancel them.
type oidcSignIn struct {
	mu         sync.Mutex
	flows      map[string]oidcFlow
	providerMu sync.Mutex
	provider   *oidc.Provider
	issuer     string
	discovered time.Time
	client     *http.Client
}

func newOIDCSignIn() *oidcSignIn {
	return &oidcSignIn{flows: make(map[string]oidcFlow), client: &http.Client{Timeout: 10 * time.Second}}
}

func (s *oidcSignIn) getProvider(issuer string) (*oidc.Provider, error) {
	s.providerMu.Lock()
	defer s.providerMu.Unlock()
	if s.provider != nil && s.issuer == issuer && time.Since(s.discovered) < time.Hour {
		return s.provider, nil
	}
	// The HTTP client's timeout also bounds JWKS requests made later by the
	// cached provider; a discovery-only context must not outlive its cancellation.
	provider, err := oidc.NewProvider(oidc.ClientContext(context.Background(), s.client), issuer)
	if err != nil {
		return nil, err
	}
	s.provider, s.issuer, s.discovered = provider, issuer, time.Now()
	return provider, nil
}

func oidcOAuthConfig(c *configstore.OIDCConfig, p *oidc.Provider) oauth2.Config {
	return oauth2.Config{ClientID: c.ClientID, ClientSecret: c.ClientSecret.GetValue(), RedirectURL: c.RedirectURL,
		Endpoint: p.Endpoint(), Scopes: []string{oidc.ScopeOpenID, "email"}}
}

func safeOIDCGoto(path string) string {
	if len(path) > 4096 || strings.ContainsAny(path, "\\\r\n") {
		return "/workspace"
	}
	u, err := url.Parse(path)
	if err != nil || u.IsAbs() || u.Host != "" || strings.ContainsAny(u.Path, "\\\r\n") {
		return "/workspace"
	}
	if u.Path == "/workspace" || strings.HasPrefix(u.Path, "/workspace/") || u.Path == "/oauth/consent" || strings.HasPrefix(u.Path, "/oauth/consent/") {
		return path
	}
	return "/workspace"
}

func setOIDCFlowCookie(ctx *fasthttp.RequestCtx, value string, expires time.Time, secure bool) {
	cookie := fasthttp.AcquireCookie()
	defer fasthttp.ReleaseCookie(cookie)
	cookie.SetKey(oidcFlowCookie)
	cookie.SetValue(value)
	cookie.SetPath("/")
	cookie.SetHTTPOnly(true)
	cookie.SetSameSite(fasthttp.CookieSameSiteLaxMode)
	cookie.SetSecure(secure)
	cookie.SetExpire(expires)
	ctx.Response.Header.SetCookie(cookie)
}

func oidcFailure(ctx *fasthttp.RequestCtx, gotoPath string) {
	location := lib.WithBasePath("/login?oidc_error=failed")
	if gotoPath != "" {
		location += "&goto=" + url.QueryEscape(safeOIDCGoto(gotoPath))
	}
	ctx.Response.Header.Set("Cache-Control", "no-store")
	ctx.Response.Header.Set("Location", location)
	ctx.SetStatusCode(fasthttp.StatusSeeOther)
}

func (h *SessionHandler) oidcLogin(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("Cache-Control", "no-store")
	gotoPath := safeOIDCGoto(string(ctx.QueryArgs().Peek("goto")))
	if h.configStore == nil {
		oidcFailure(ctx, gotoPath)
		return
	}
	auth, err := h.configStore.GetAuthConfig(ctx)
	if err != nil || !auth.OIDCEnabled() || auth.OIDC.Validate() != nil {
		oidcFailure(ctx, gotoPath)
		return
	}
	provider, err := h.oidc.getProvider(auth.OIDC.IssuerURL)
	if err != nil {
		logger.Warn("OIDC discovery failed: %v", err)
		oidcFailure(ctx, gotoPath)
		return
	}
	state := oauth2.GenerateVerifier()
	flow := oidcFlow{config: auth.OIDC.Clone(), provider: provider, verifier: oauth2.GenerateVerifier(),
		nonce: oauth2.GenerateVerifier(), cookie: oauth2.GenerateVerifier(),
		gotoPath: gotoPath, expires: time.Now().Add(10 * time.Minute)}
	h.oidc.mu.Lock()
	for key, pending := range h.oidc.flows {
		if time.Now().After(pending.expires) {
			delete(h.oidc.flows, key)
		}
	}
	if len(h.oidc.flows) >= 4096 {
		h.oidc.mu.Unlock()
		SendError(ctx, fasthttp.StatusTooManyRequests, "Too many pending sign-in requests. Please try again later.")
		return
	}
	h.oidc.flows[state] = flow
	h.oidc.mu.Unlock()
	setOIDCFlowCookie(ctx, flow.cookie, flow.expires, strings.HasPrefix(flow.config.RedirectURL, "https://"))
	conf := oidcOAuthConfig(flow.config, provider)
	markLocationExternal(ctx)
	ctx.Redirect(conf.AuthCodeURL(state, oauth2.S256ChallengeOption(flow.verifier), oidc.Nonce(flow.nonce)), fasthttp.StatusSeeOther)
}

func (h *SessionHandler) oidcCallback(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("Cache-Control", "no-store")
	state, cookie := string(ctx.QueryArgs().Peek("state")), string(ctx.Request.Header.Cookie(oidcFlowCookie))
	h.oidc.mu.Lock()
	flow, found := h.oidc.flows[state]
	valid := found && cookie != "" && subtle.ConstantTimeCompare([]byte(cookie), []byte(flow.cookie)) == 1
	if valid {
		delete(h.oidc.flows, state)
	}
	h.oidc.mu.Unlock()
	if !valid {
		oidcFailure(ctx, "")
		return
	}
	if time.Now().After(flow.expires) {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	setOIDCFlowCookie(ctx, "", time.Now().Add(-time.Hour), strings.HasPrefix(flow.config.RedirectURL, "https://"))
	if h.configStore == nil || len(ctx.QueryArgs().Peek("error")) > 0 || len(ctx.QueryArgs().Peek("code")) == 0 {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	auth, err := h.configStore.GetAuthConfig(ctx)
	if err != nil || !auth.OIDCEnabled() || !auth.OIDC.Equals(flow.config) {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	networkCtx, cancel := context.WithTimeout(oidc.ClientContext(ctx, h.oidc.client), 10*time.Second)
	defer cancel()
	conf := oidcOAuthConfig(flow.config, flow.provider)
	token, err := conf.Exchange(networkCtx, string(ctx.QueryArgs().Peek("code")), oauth2.VerifierOption(flow.verifier))
	if err != nil {
		// Provider errors may contain credentials or codes; never log their body.
		logger.Warn("OIDC code exchange failed")
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	idToken, err := flow.provider.Verifier(&oidc.Config{ClientID: flow.config.ClientID}).Verify(networkCtx, rawIDToken)
	if err != nil || subtle.ConstantTimeCompare([]byte(idTokenNonce(idToken)), []byte(flow.nonce)) != 1 {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	var claims struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
	}
	if err := idToken.Claims(&claims); err != nil || !flow.config.Allows(idToken.Subject, claims.Email, claims.EmailVerified) {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	// Recheck after network I/O and again after insertion. If policy changes
	// after insertion, the normal settings update flushes this session too.
	auth, err = h.configStore.GetAuthConfig(ctx)
	if err != nil || !auth.OIDCEnabled() || !auth.OIDC.Equals(flow.config) {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	expires := time.Now().Add(8 * time.Hour)
	if idToken.Expiry.Before(expires) {
		expires = idToken.Expiry
	}
	sessionToken := uuid.NewString()
	session := &tables.SessionsTable{Token: sessionToken, ExpiresAt: expires, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := h.configStore.CreateSession(ctx, session); err != nil {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	auth, err = h.configStore.GetAuthConfig(ctx)
	if err != nil || !auth.OIDCEnabled() || !auth.OIDC.Equals(flow.config) {
		if err := h.configStore.DeleteSession(ctx, sessionToken); err != nil {
			logger.Error("failed to revoke session after OIDC configuration changed")
		}
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	setSessionTokenCookie(ctx, sessionToken, expires)
	ctx.Response.Header.Set("Location", lib.WithBasePath(flow.gotoPath))
	ctx.SetStatusCode(fasthttp.StatusSeeOther)
}

func idTokenNonce(token *oidc.IDToken) string {
	if token == nil {
		return ""
	}
	return token.Nonce
}

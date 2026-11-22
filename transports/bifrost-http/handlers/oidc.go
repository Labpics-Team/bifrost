package handlers

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net"
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
	mu           sync.Mutex
	flows        map[string]oidcFlow
	providerMu   sync.Mutex
	provider     *oidc.Provider
	issuer       string
	discovered   time.Time
	discoveryErr error
	client       *http.Client
}

func newOIDCSignIn() *oidcSignIn {
	return &oidcSignIn{flows: make(map[string]oidcFlow), client: &http.Client{Timeout: 10 * time.Second}}
}

func (s *oidcSignIn) getProvider(issuer string) (*oidc.Provider, error) {
	s.providerMu.Lock()
	defer s.providerMu.Unlock()
	if s.issuer == issuer {
		if s.provider != nil && time.Since(s.discovered) < time.Hour {
			return s.provider, nil
		}
		// Share a failed attempt with its waiters instead of making each one
		// hold the lock for another network timeout during an issuer outage.
		if s.discoveryErr != nil && time.Since(s.discovered) < 5*time.Second {
			return nil, s.discoveryErr
		}
	}
	// The HTTP client's timeout also bounds JWKS requests made later by the
	// cached provider; a discovery-only context must not outlive its cancellation.
	provider, err := oidc.NewProvider(oidc.ClientContext(context.Background(), s.clientForIssuer(issuer)), issuer)
	if err == nil {
		var metadata struct {
			JWKSURL string `json:"jwks_uri"`
		}
		err = provider.Claims(&metadata)
		if err == nil {
			for _, endpoint := range []string{provider.Endpoint().AuthURL, provider.Endpoint().TokenURL, metadata.JWKSURL} {
				u, parseErr := url.Parse(endpoint)
				if parseErr != nil || !allowedOIDCEndpoint(u, oidcLoopbackHTTP(issuer)) {
					err = fmt.Errorf("OIDC provider endpoints require HTTPS (HTTP is allowed only with a loopback development issuer)")
					break
				}
			}
		}
	}
	if err != nil {
		provider = nil
	}
	s.provider, s.discoveryErr, s.issuer, s.discovered = provider, err, issuer, time.Now()
	return provider, err
}

func oidcLoopbackHTTP(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "http" && allowedOIDCEndpoint(u, true)
}

func allowedOIDCEndpoint(u *url.URL, allowLoopbackHTTP bool) bool {
	if u == nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return allowLoopbackHTTP && u.Scheme == "http" &&
		(strings.EqualFold(u.Hostname(), "localhost") || (ip != nil && ip.IsLoopback()))
}

type oidcTransport struct {
	base              http.RoundTripper
	allowLoopbackHTTP bool
}

func (t oidcTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !allowedOIDCEndpoint(req.URL, t.allowLoopbackHTTP) {
		return nil, fmt.Errorf("OIDC request requires HTTPS")
	}
	return t.base.RoundTrip(req)
}

// The same policy covers discovery, code exchange, JWKS, and every redirect.
// Copy the client so test/proxy transports and its timeout remain intact.
func (s *oidcSignIn) clientForIssuer(issuer string) *http.Client {
	client := *s.client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = oidcTransport{base: base, allowLoopbackHTTP: oidcLoopbackHTTP(issuer)}
	checkRedirect := client.CheckRedirect
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 && via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
			return fmt.Errorf("OIDC redirect cannot downgrade HTTPS")
		}
		if checkRedirect != nil {
			return checkRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		return nil
	}
	return &client
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

func setOIDCFlowCookie(ctx *fasthttp.RequestCtx, state, value string, expires time.Time, secure bool) {
	cookie := fasthttp.AcquireCookie()
	defer fasthttp.ReleaseCookie(cookie)
	cookie.SetKey(oidcFlowCookie + "_" + state)
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
	setOIDCFlowCookie(ctx, state, flow.cookie, flow.expires, strings.HasPrefix(flow.config.RedirectURL, "https://"))
	conf := oidcOAuthConfig(flow.config, provider)
	markLocationExternal(ctx)
	ctx.Redirect(conf.AuthCodeURL(state, oauth2.S256ChallengeOption(flow.verifier), oidc.Nonce(flow.nonce)), fasthttp.StatusSeeOther)
}

func (h *SessionHandler) oidcCallback(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("Cache-Control", "no-store")
	state := string(ctx.QueryArgs().Peek("state"))
	h.oidc.mu.Lock()
	flow, found := h.oidc.flows[state]
	var cookie string
	if found {
		cookie = string(ctx.Request.Header.Cookie(oidcFlowCookie + "_" + state))
	}
	valid := found && cookie != "" && subtle.ConstantTimeCompare([]byte(cookie), []byte(flow.cookie)) == 1
	if valid {
		delete(h.oidc.flows, state)
	}
	h.oidc.mu.Unlock()
	if !valid {
		oidcFailure(ctx, "")
		return
	}
	setOIDCFlowCookie(ctx, state, "", time.Now().Add(-time.Hour), strings.HasPrefix(flow.config.RedirectURL, "https://"))
	if time.Now().After(flow.expires) {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	if h.configStore == nil || len(ctx.QueryArgs().Peek("error")) > 0 || len(ctx.QueryArgs().Peek("code")) == 0 {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	auth, err := h.configStore.GetAuthConfig(ctx)
	if err != nil || !auth.OIDCEnabled() || !auth.OIDC.Equals(flow.config) {
		oidcFailure(ctx, flow.gotoPath)
		return
	}
	networkCtx, cancel := context.WithTimeout(oidc.ClientContext(ctx, h.oidc.clientForIssuer(flow.config.IssuerURL)), 10*time.Second)
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
	expires := time.Now().Add(8 * time.Hour)
	if idToken.Expiry.Before(expires) {
		expires = idToken.Expiry
	}
	sessionToken := uuid.NewString()
	session := &tables.SessionsTable{Token: sessionToken, ExpiresAt: expires, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := h.configStore.CreateSessionIfAuthMatches(ctx, session, auth); err != nil {
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

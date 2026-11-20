package handlers

import (
	"encoding/json"

	"github.com/fasthttp/router"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/plugins/routing/adaptive"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

// RegisterAdaptiveRoutingRoutes uses the same admin authentication chain as
// routing rules. Runtime status contains numbers and key names, never secrets.
func RegisterAdaptiveRoutingRoutes(r *router.Router, c *adaptive.Controller, middlewares ...schemas.BifrostHTTPMiddleware) {
	register := func(method, path string, handler fasthttp.RequestHandler) {
		r.Handle(method, path, lib.ChainMiddlewares(handler, middlewares...))
	}
	register(fasthttp.MethodGet, "/api/adaptive-routing/config", func(ctx *fasthttp.RequestCtx) { SendJSON(ctx, c.Config()) })
	register(fasthttp.MethodPut, "/api/adaptive-routing/config", func(ctx *fasthttp.RequestCtx) {
		var config adaptive.Config
		if err := json.Unmarshal(ctx.PostBody(), &config); err != nil {
			SendError(ctx, 400, "invalid adaptive routing settings")
			return
		}
		if err := config.Validate(); err != nil {
			SendError(ctx, 400, err.Error())
			return
		}
		if err := c.Save(ctx, config); err != nil {
			SendError(ctx, 500, "could not save adaptive routing settings")
			return
		}
		SendJSON(ctx, config)
	})
	register(fasthttp.MethodGet, "/api/adaptive-routing/status", func(ctx *fasthttp.RequestCtx) {
		SendJSON(ctx, map[string]any{"enabled": c.Enabled(), "routes": c.Status()})
	})
	register(fasthttp.MethodGet, "/api/adaptive-routing/history", func(ctx *fasthttp.RequestCtx) { SendJSON(ctx, map[string]any{"events": c.History()}) })
	register(fasthttp.MethodPost, "/api/adaptive-routing/conditions/clear", func(ctx *fasthttp.RequestCtx) {
		var request struct {
			Provider schemas.ModelProvider `json:"provider"`
			KeyID    string                `json:"key_id"`
			Model    string                `json:"model"`
		}
		if len(ctx.PostBody()) > 0 {
			if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
				SendError(ctx, 400, "invalid held-key release")
				return
			}
		}
		if request.Provider != "" && request.KeyID == "" || request.Provider == "" && (request.KeyID != "" || request.Model != "") {
			SendError(ctx, 400, "provider and key_id must be specified together")
			return
		}
		c.Release(request.Provider, request.KeyID, request.Model)
		if err := c.SaveHolds(ctx); err != nil {
			SendError(ctx, 500, "released in memory but could not persist held-key state")
			return
		}
		SendJSON(ctx, map[string]bool{"released": true})
	})
}

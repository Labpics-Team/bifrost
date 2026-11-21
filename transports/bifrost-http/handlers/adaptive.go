package handlers

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

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
		SendJSON(ctx, map[string]any{"enabled": c.Enabled(), "routes": c.Status(), "persistence": c.PersistenceStatus()})
	})
	register(fasthttp.MethodGet, "/api/adaptive-routing/incidents", func(ctx *fasthttp.RequestCtx) {
		query, err := adaptiveIncidentQuery(ctx.QueryArgs())
		if err != nil {
			SendError(ctx, 400, err.Error())
			return
		}
		page, err := c.ListIncidents(ctx, query)
		if errors.Is(err, adaptive.ErrInvalidIncidentQuery) {
			SendError(ctx, 400, "invalid incident cursor or filters")
			return
		}
		if err != nil {
			SendError(ctx, 500, "could not load routing incidents")
			return
		}
		SendJSON(ctx, page)
	})
	register(fasthttp.MethodGet, "/api/adaptive-routing/incidents/{id}", func(ctx *fasthttp.RequestCtx) {
		limit, err := adaptiveIncidentLimit(ctx.QueryArgs())
		if err != nil {
			SendError(ctx, 400, err.Error())
			return
		}
		id, _ := ctx.UserValue("id").(string)
		detail, err := c.GetIncident(ctx, id, string(ctx.QueryArgs().Peek("cursor")), limit)
		if errors.Is(err, adaptive.ErrInvalidIncidentQuery) {
			SendError(ctx, 400, "invalid incident cursor")
			return
		}
		if errors.Is(err, adaptive.ErrIncidentNotFound) {
			SendError(ctx, 404, "routing incident not found")
			return
		}
		if err != nil {
			SendError(ctx, 500, "could not load routing incident")
			return
		}
		SendJSON(ctx, detail)
	})
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
		if err := c.Release(ctx, request.Provider, request.KeyID, request.Model); err != nil {
			SendError(ctx, 500, "could not persist held-key release")
			return
		}
		SendJSON(ctx, map[string]bool{"released": true})
	})
}

// Filtering and ordering belong to the durable store, before pagination.
func adaptiveIncidentQuery(args *fasthttp.Args) (adaptive.IncidentQuery, error) {
	query := adaptive.IncidentQuery{
		Provider: string(args.Peek("provider")), Model: string(args.Peek("model")),
		KeyID: string(args.Peek("key_id")), Status: string(args.Peek("status")),
		Scope: string(args.Peek("scope")), Search: string(args.Peek("q")),
		Sort: string(args.Peek("sort")), Cursor: string(args.Peek("cursor")),
	}
	var err error
	if query.Limit, err = adaptiveIncidentLimit(args); err != nil {
		return query, err
	}
	switch query.Sort {
	case "", "newest", "oldest", "attention", "longest":
	default:
		return query, errors.New("invalid incident sort")
	}
	switch query.Status {
	case "", "needs_attention", "mitigated", "recovering", "resolved", "waiting_for_traffic", "no_longer_observed":
	default:
		return query, errors.New("invalid incident status")
	}
	switch query.Scope {
	case "", "provider_model", "key_model", "key":
	default:
		return query, errors.New("invalid incident scope")
	}
	if value := string(args.Peek("from")); value != "" {
		if query.From, err = time.Parse(time.RFC3339, value); err != nil {
			return query, errors.New("from must be an RFC3339 timestamp")
		}
	}
	if value := string(args.Peek("to")); value != "" {
		if query.To, err = time.Parse(time.RFC3339, value); err != nil {
			return query, errors.New("to must be an RFC3339 timestamp")
		}
	}
	if !query.From.IsZero() && !query.To.IsZero() && query.From.After(query.To) {
		return query, errors.New("from must not be after to")
	}
	return query, nil
}

func adaptiveIncidentLimit(args *fasthttp.Args) (int, error) {
	if !args.Has("limit") {
		return 25, nil
	}
	limit, err := strconv.Atoi(string(args.Peek("limit")))
	if err != nil || limit < 1 || limit > 100 {
		return 0, errors.New("limit must be between 1 and 100")
	}
	return limit, nil
}

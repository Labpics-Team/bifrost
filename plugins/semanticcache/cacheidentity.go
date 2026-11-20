package semanticcache

import "github.com/maximhq/bifrost/core/schemas"

const httpCacheRouteKey schemas.BifrostContextKey = "semantic-cache-http-route"

type cacheHTTPRoute struct {
	method string
	path   string
}

// Only native protocol options participate. Authentication is represented by the
// authenticated grant; trace IDs and cache controls must not partition responses.
// Read the native request snapshots after all HTTP pre-hooks have been applied.
// Only the route is captured earlier: the transport rejects method/path rewrites.
func cacheHTTPIdentity(ctx *schemas.BifrostContext) map[string]string {
	if ctx == nil {
		return nil
	}
	route, ok := ctx.Value(httpCacheRouteKey).(cacheHTTPRoute)
	if !ok {
		return nil
	}
	headers, _ := ctx.Value(schemas.BifrostContextKeyRequestHeaders).(map[string]string)
	query, _ := ctx.Value(schemas.BifrostContextKeyRequestQuery).(map[string]string)
	identity := map[string]string{"method": route.method, "path": route.path}
	for _, name := range []string{"anthropic-version", "anthropic-beta", "openai-version", "openai-beta"} {
		if value := headers[name]; value != "" {
			identity["header:"+name] = value
		}
	}
	for _, name := range []string{"api-version", "alt", "$alt", "fields"} {
		if value := query[name]; value != "" {
			identity["query:"+name] = value
		}
	}
	return identity
}

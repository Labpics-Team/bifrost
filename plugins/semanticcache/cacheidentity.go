package semanticcache

import "github.com/maximhq/bifrost/core/schemas"

const httpCacheIdentityKey schemas.BifrostContextKey = "semantic-cache-http-identity"

// Only native protocol options participate. Authentication is represented by the
// authenticated grant; trace IDs and cache controls must not partition responses.
// The HTTP hook owns this projection so proxies need no cache-specific contract.
func cacheHTTPIdentity(req *schemas.HTTPRequest) map[string]string {
	if req == nil {
		return nil
	}
	identity := map[string]string{"method": req.Method, "path": req.Path}
	for _, name := range []string{"anthropic-version", "anthropic-beta", "openai-version", "openai-beta"} {
		if value := req.CaseInsensitiveHeaderLookup(name); value != "" {
			identity["header:"+name] = value
		}
	}
	for _, name := range []string{"api-version", "alt", "$alt", "fields"} {
		if value := req.CaseInsensitiveQueryLookup(name); value != "" {
			identity["query:"+name] = value
		}
	}
	return identity
}

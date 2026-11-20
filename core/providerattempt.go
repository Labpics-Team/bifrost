package bifrost

import (
	providerUtils "github.com/maximhq/bifrost/core/providers/utils"
	"github.com/maximhq/bifrost/core/schemas"
)

func (bifrost *Bifrost) beginProviderAttempt(ctx *schemas.BifrostContext, provider schemas.ModelProvider, model string, key schemas.Key, req *schemas.BifrostRequest) schemas.ProviderAttempt {
	if bifrost.providerAttemptObserver == nil || model == "" {
		return nil
	}
	// Stored-object lookups can walk other keys inside the provider dispatch;
	// the outer retry key is not evidence about which key served that object.
	if req != nil && isKeyScopedLookupRequest(req) {
		return nil
	}
	return bifrost.providerAttemptObserver.BeginProviderAttempt(ctx, schemas.ProviderAttemptInfo{
		Provider: provider, Model: model, KeyID: key.ID, KeyName: key.Name,
	})
}

// A stream's socket closes before the retry loop settles its first-token
// deadline. Use the same native timeout here so an observer sees that cause,
// rather than the transport's incidental closed-socket error. The handle is
// captured per attempt, since a later retry can already share the context.
func finishProviderAttempt(attempt schemas.ProviderAttempt, abort *providerUtils.AttemptAbort) {
	if attempt == nil {
		return
	}
	if abort.Fired() {
		attempt.Observe(nil, providerUtils.NewFirstTokenTimeoutError(abort.Timeout()))
	}
	attempt.End()
}

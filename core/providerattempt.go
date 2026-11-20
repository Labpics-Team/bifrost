package bifrost

import "github.com/maximhq/bifrost/core/schemas"

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

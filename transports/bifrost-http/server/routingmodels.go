package server

import (
	"sort"

	bifrost "github.com/maximhq/bifrost/core"
	"github.com/maximhq/bifrost/core/schemas"
)

// ListAllModels includes callable routing names in the native model catalog.
// Both HTTP dialects use the same live rules and paginate the combined result.
func (s *BifrostHTTPServer) ListAllModels(ctx *schemas.BifrostContext, req *schemas.BifrostListModelsRequest) (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
	if req == nil {
		req = &schemas.BifrostListModelsRequest{}
	}
	// Without routing, preserve the core catalog and its provider failures.
	if _, err := s.getRoutingPlugin(); err != nil {
		return s.Client.ListAllModels(ctx, req)
	}
	unpaged := *req
	unpaged.PageSize, unpaged.PageToken = 0, ""
	aliases, err := s.ListRoutingModels(ctx, &unpaged)
	if err != nil {
		return nil, err
	}
	response, err := s.Client.ListAllModels(ctx, &unpaged)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(response.Data)+len(aliases.Data))
	for _, model := range response.Data {
		seen[model.ID] = true
	}
	for _, model := range aliases.Data {
		if !seen[model.ID] {
			response.Data = append(response.Data, model)
			seen[model.ID] = true
		}
	}
	sort.Slice(response.Data, func(i, j int) bool { return response.Data[i].ID < response.Data[j].ID })
	return response.ApplyPagination(req.PageSize, req.PageToken), nil
}

// ListRoutingModels admits a catalog read through the same governance hook as
// native list-models. It never invokes a provider or charges inference usage.
func (s *BifrostHTTPServer) ListRoutingModels(ctx *schemas.BifrostContext, req *schemas.BifrostListModelsRequest) (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
	// Like native list-models, this metadata read spends no inference quota.
	// Keep the exemption local; identity and target permissions still apply.
	ctx = schemas.NewBifrostContext(ctx, schemas.NoDeadline)
	defer ctx.Cancel()
	ctx.SetValue(schemas.BifrostContextKeySkipBudgetAndRateLimits, true)

	unavailable := func() (*schemas.BifrostListModelsResponse, *schemas.BifrostError) {
		return nil, &schemas.BifrostError{StatusCode: bifrost.Ptr(503), Error: &schemas.ErrorField{Message: "Routing model catalog unavailable"}}
	}
	governance, err := s.getGovernancePlugin()
	if err != nil {
		return unavailable()
	}
	_, short, err := governance.PreLLMHook(ctx, &schemas.BifrostRequest{RequestType: schemas.ListModelsRequest, ListModelsRequest: req})
	if err != nil {
		return unavailable()
	}
	if short != nil {
		if short.Error != nil {
			return nil, short.Error
		}
		return unavailable()
	}
	access, err := governance.ResolveAccess(ctx)
	if err != nil {
		return unavailable()
	}
	routing, err := s.getRoutingPlugin()
	if err != nil {
		return unavailable()
	}
	models, err := routing.ListModels(ctx, access)
	if err != nil {
		return unavailable()
	}
	return (&schemas.BifrostListModelsResponse{Data: models}).ApplyPagination(req.PageSize, req.PageToken), nil
}

package handlers

import (
	"strconv"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

func (h *CompletionHandler) listRoutingModels(ctx *fasthttp.RequestCtx) {
	bifrostCtx, cancel := lib.ConvertToBifrostContext(ctx, h.config)
	defer cancel()
	if bifrostCtx == nil {
		SendError(ctx, fasthttp.StatusBadRequest, "Failed to convert context")
		return
	}
	if h.modelsManager == nil {
		SendError(ctx, fasthttp.StatusServiceUnavailable, "Routing model catalog unavailable")
		return
	}
	pageSize, _ := strconv.Atoi(string(ctx.QueryArgs().Peek("page_size")))
	resp, err := h.modelsManager.ListRoutingModels(bifrostCtx, &schemas.BifrostListModelsRequest{
		PageSize: pageSize, PageToken: string(ctx.QueryArgs().Peek("page_token")),
	})
	if err != nil {
		forwardProviderHeadersFromContext(ctx, bifrostCtx)
		SendBifrostError(ctx, err)
		return
	}
	SendJSON(ctx, resp)
}

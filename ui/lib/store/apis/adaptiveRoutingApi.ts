import type {
	AdaptiveRoutingConfig,
	AdaptiveRoutingIncidentDetail,
	AdaptiveRoutingIncidentPage,
	AdaptiveRoutingIncidentQuery,
	AdaptiveRoutingStatus,
	ClearAdaptiveRoutingCondition,
} from "@/lib/types/adaptiveRouting";
import { baseApi } from "./baseApi";

export const adaptiveRoutingApi = baseApi.enhanceEndpoints({ addTagTypes: ["AdaptiveRoutingStatus"] }).injectEndpoints({
	endpoints: (builder) => ({
		getAdaptiveRoutingConfig: builder.query<AdaptiveRoutingConfig, void>({
			query: () => "/adaptive-routing/config",
			providesTags: ["LoadBalancerConfig"],
		}),
		updateAdaptiveRoutingConfig: builder.mutation<AdaptiveRoutingConfig, AdaptiveRoutingConfig>({
			query: (body) => ({ url: "/adaptive-routing/config", method: "PUT", body }),
			invalidatesTags: ["LoadBalancerConfig", "AdaptiveRoutingStatus", "LoadBalancerHistory"],
		}),
		getAdaptiveRoutingStatus: builder.query<AdaptiveRoutingStatus, void>({
			query: () => "/adaptive-routing/status",
			providesTags: ["AdaptiveRoutingStatus"],
		}),
		getAdaptiveRoutingIncidents: builder.query<AdaptiveRoutingIncidentPage, AdaptiveRoutingIncidentQuery>({
			query: (params) => ({ url: "/adaptive-routing/incidents", params }),
			providesTags: ["LoadBalancerHistory"],
		}),
		getAdaptiveRoutingIncident: builder.query<AdaptiveRoutingIncidentDetail, { id: string; cursor?: string; limit?: number }>({
			query: ({ id, ...params }) => ({ url: `/adaptive-routing/incidents/${encodeURIComponent(id)}`, params }),
			providesTags: ["LoadBalancerHistory"],
		}),
		clearAdaptiveRoutingCondition: builder.mutation<void, ClearAdaptiveRoutingCondition>({
			query: (body) => ({ url: "/adaptive-routing/conditions/clear", method: "POST", body }),
			invalidatesTags: ["AdaptiveRoutingStatus", "LoadBalancerHistory"],
		}),
	}),
});

export const {
	useGetAdaptiveRoutingConfigQuery,
	useUpdateAdaptiveRoutingConfigMutation,
	useGetAdaptiveRoutingStatusQuery,
	useGetAdaptiveRoutingIncidentsQuery,
	useGetAdaptiveRoutingIncidentQuery,
	useClearAdaptiveRoutingConditionMutation,
} = adaptiveRoutingApi;
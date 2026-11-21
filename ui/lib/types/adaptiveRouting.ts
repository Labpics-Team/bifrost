import type { z } from "zod";
import type { adaptiveRoutingConfigSchema } from "./schemas";

export type AdaptiveRoutingConfig = z.infer<typeof adaptiveRoutingConfigSchema>;
export const ROUTE_STATES = ["unknown", "healthy", "degraded", "held", "failed", "recovering"] as const;
export type RouteState = (typeof ROUTE_STATES)[number];

export interface AdaptiveRouteStatus {
	provider: string;
	model: string;
	key_id: string;
	key_name: string;
	state: RouteState;
	score: number;
	samples: number;
	error_rate: number;
	latency_ms: number;
	inflight: number;
	last_seen: string;
	hold_until?: string;
	failure_class?: string;
}

export interface AdaptiveRoutingStatus {
	enabled: boolean;
	routes: AdaptiveRouteStatus[];
	persistence: { healthy: boolean; last_success_at?: string; last_error?: string };
}

export const INCIDENT_STATUSES = [
	"needs_attention",
	"mitigated",
	"recovering",
	"resolved",
	"waiting_for_traffic",
	"no_longer_observed",
] as const;
export const INCIDENT_SCOPES = ["provider_model", "key_model", "key"] as const;
export const INCIDENT_SORTS = ["newest", "oldest", "attention", "longest"] as const;

export interface AdaptiveRoutingIncident {
	id: string;
	scope: (typeof INCIDENT_SCOPES)[number];
	provider: string;
	model?: string;
	key_id?: string;
	key_name?: string;
	status: (typeof INCIDENT_STATUSES)[number];
	state: RouteState;
	worst_state: RouteState;
	failure_class?: string;
	opened_at: string;
	updated_at: string;
	last_seen: string;
	resolved_at?: string;
	closed_at?: string;
	summary: string;
	action: string;
	// Durably recorded checks with a result, excluding checks interrupted by a crash.
	checks: number;
	failed_checks: number;
	refused_again: number;
	first_request_id?: string;
	last_request_id?: string;
}

export interface AdaptiveRoutingIncidentEvent {
	id: string;
	timestamp: string;
	kind: string;
	state?: RouteState;
	failure_class?: string;
	request_id?: string;
	message: string;
}

export interface AdaptiveRoutingIncidentQuery {
	provider?: string;
	model?: string;
	key_id?: string;
	status?: AdaptiveRoutingIncident["status"];
	scope?: AdaptiveRoutingIncident["scope"];
	q?: string;
	sort?: (typeof INCIDENT_SORTS)[number];
	from?: string;
	to?: string;
	limit?: number;
	cursor?: string;
}

export interface AdaptiveRoutingIncidentPage {
	incidents: AdaptiveRoutingIncident[];
	next_cursor?: string;
	total: number;
}

export interface AdaptiveRoutingIncidentDetail {
	incident: AdaptiveRoutingIncident;
	events: AdaptiveRoutingIncidentEvent[];
	next_cursor?: string;
}

export interface ClearAdaptiveRoutingCondition {
	provider: string;
	key_id: string;
	model?: string;
}

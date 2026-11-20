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
}

export interface AdaptiveRoutingEvent extends AdaptiveRouteStatus {
	timestamp: string;
}

export interface ClearAdaptiveRoutingCondition {
	provider: string;
	key_id: string;
	model?: string;
}
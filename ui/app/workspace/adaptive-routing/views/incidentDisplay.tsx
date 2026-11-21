import { PageTagBadge, type PageTagTone } from "@/components/pageTitle";
import type { AdaptiveRoutingIncident, AdaptiveRoutingIncidentEvent } from "@/lib/types/adaptiveRouting";
import { withBasePath } from "@/lib/utils/basePath";
import { useTranslation } from "react-i18next";

const tones: Record<AdaptiveRoutingIncident["status"], PageTagTone> = {
	needs_attention: "danger",
	mitigated: "warning",
	recovering: "info",
	resolved: "success",
	waiting_for_traffic: "neutral",
	no_longer_observed: "neutral",
};

export function IncidentStatus({ status }: { status: AdaptiveRoutingIncident["status"] }) {
	const { t } = useTranslation("observability");
	return <PageTagBadge tag={{ label: t(`adaptiveRouting.incidentStatuses.${status}`), tone: tones[status] }} />;
}

export function IncidentCause({ cause }: { cause?: string }) {
	const { t } = useTranslation("observability");
	return <>{cause ? t(`adaptiveRouting.failureClasses.${cause}`, { defaultValue: cause }) : "—"}</>;
}

export function IncidentSummary({ incident }: { incident: AdaptiveRoutingIncident }) {
	const { t } = useTranslation("observability");
	if (incident.status === "no_longer_observed") return <>{t("adaptiveRouting.summaries.no_longer_observed")}</>;
	if (incident.status === "waiting_for_traffic") return <>{t("adaptiveRouting.summaries.waiting_for_traffic")}</>;
	const state = incident.state === "unknown" ? "degraded" : incident.state;
	return <>{t(`adaptiveRouting.summaries.${state}`)}</>;
}

export function IncidentAction({ incident }: { incident: AdaptiveRoutingIncident }) {
	const { t } = useTranslation("observability");
	if (incident.closed_at || incident.state === "healthy") return null;
	if (incident.state === "held" && (incident.failure_class === "credential" || incident.failure_class === "quota")) {
		return <>{t("adaptiveRouting.actions.check_credentials")}</>;
	}
	const state = incident.state === "unknown" ? "degraded" : incident.state;
	return <>{t(`adaptiveRouting.actions.${state}`)}</>;
}

export function IncidentEventMessage({ event }: { event: AdaptiveRoutingIncidentEvent }) {
	const { t } = useTranslation("observability");
	// Only known system events are translated. Provider and future event text stays intact.
	switch (event.kind) {
		case "state_changed":
			return (
				<>
					{t("adaptiveRouting.eventMessages.state_changed", {
						state: event.state ? t(`adaptiveRouting.states.${event.state}`) : t("adaptiveRouting.states.unknown"),
					})}
				</>
			);
		case "configuration_changed":
		case "manual_release":
		case "check_started":
		case "hold_released":
		case "held_again":
		case "held":
		case "telemetry_removed":
		case "history_disabled":
		case "waiting_for_traffic":
			return <>{t(`adaptiveRouting.eventMessages.${event.kind}`)}</>;
		default:
			return <>{event.message}</>;
	}
}

export function incidentDuration(incident: AdaptiveRoutingIncident, locale: string, now = Date.now()) {
	const end = incident.closed_at;
	const seconds = Math.max(0, Math.floor(((end ? Date.parse(end) : now) - Date.parse(incident.opened_at)) / 1000));
	if (!Number.isFinite(seconds)) return "—";
	const unit = seconds >= 86400 ? "day" : seconds >= 3600 ? "hour" : seconds >= 60 ? "minute" : "second";
	const divisor = { day: 86400, hour: 3600, minute: 60, second: 1 }[unit];
	return new Intl.NumberFormat(locale, { style: "unit", unit, unitDisplay: "short", maximumFractionDigits: 1 }).format(seconds / divisor);
}

// Native logs fetch selected_log by its actual ID, independently of table pagination.
export function incidentRequestHref(id: string, timestamp: string) {
	const time = Math.floor(Date.parse(timestamp) / 1000);
	const query = new URLSearchParams({ selected_log: id, request_id: id, period: "", polling: "false" });
	if (Number.isFinite(time)) {
		query.set("start_time", String(time - 300));
		query.set("end_time", String(time + 300));
	}
	return withBasePath(`/workspace/logs?${query}`);
}
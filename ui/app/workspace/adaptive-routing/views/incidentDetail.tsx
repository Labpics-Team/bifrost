import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { getProviderLabel } from "@/lib/constants/logs";
import { useLocale } from "@/lib/i18n/useLocale";
import { useGetAdaptiveRoutingIncidentQuery } from "@/lib/store/apis/adaptiveRoutingApi";
import { ChevronLeft, ChevronRight, FileText } from "lucide-react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { IncidentCause, incidentDuration, incidentRequestHref, IncidentStatus } from "./incidentDisplay";
import { RoutingQueryState } from "./routingQueryState";
import { ReleaseHoldButton } from "./releaseHoldButton";

export function IncidentDetail({ id, live, close }: { id: string; live: boolean; close: () => void }) {
	const { t } = useTranslation("observability");
	const locale = useLocale();
	const [cursors, setCursors] = useState([""]);
	const cursor = cursors[cursors.length - 1];
	const query = useGetAdaptiveRoutingIncidentQuery(
		{ id, cursor, limit: 25 },
		{
			pollingInterval: live ? 5000 : 0,
			skipPollingIfUnfocused: true,
		},
	);
	const detail = query.currentData;
	const incident = detail?.incident;
	const date = (value: string) => new Date(value).toLocaleString(locale);
	const requestLinks = incident
		? [
				{ id: incident.first_request_id, time: incident.opened_at, label: "firstRequest" as const },
				...(incident.last_request_id !== incident.first_request_id
					? [{ id: incident.last_request_id, time: incident.last_seen, label: "lastRequest" as const }]
					: []),
			]
		: [];
	return (
		<Sheet
			open
			onOpenChange={(open) => {
				if (!open) close();
			}}
		>
			<SheetContent className="w-[calc(100%-1rem)] gap-0 overflow-hidden p-0" data-testid="adaptive-incident-detail">
				<div className="border-b p-4">
					<SheetHeader headerClassName="mb-2">
						<SheetTitle>{t("adaptiveRouting.incidentDetails")}</SheetTitle>
					</SheetHeader>
					<SheetDescription>
						{incident ? `${getProviderLabel(incident.provider)} · ${incident.model || t("labels.allModels")}` : id}
					</SheetDescription>
				</div>
				<div className="min-h-0 flex-1 space-y-6 overflow-y-auto p-4">
					<RoutingQueryState loading={!detail && query.isFetching} error={query.error} retry={() => void query.refetch()} />
					{incident && (
						<>
							<div className="space-y-3">
								<IncidentStatus status={incident.status} />
								<p className="font-medium">{incident.summary}</p>
								<p className="text-muted-foreground text-sm">{incident.action}</p>
								{!incident.closed_at && incident.state === "held" && incident.key_id && (
									<ReleaseHoldButton
										provider={incident.provider}
										keyId={incident.key_id}
										model={incident.scope === "key_model" ? incident.model : undefined}
									/>
								)}
								<dl className="grid grid-cols-2 gap-4 text-sm">
									{(
										[
											["cause", <IncidentCause key="cause" cause={incident.failure_class} />],
											["scope", t(`adaptiveRouting.incidentScopes.${incident.scope}`)],
											["key", incident.key_name || incident.key_id || "—"],
											["opened", date(incident.opened_at)],
											["duration", incidentDuration(incident, locale)],
											["lastSeen", date(incident.last_seen)],
											["resolved", incident.resolved_at ? date(incident.resolved_at) : "—"],
											["state", t(`adaptiveRouting.states.${incident.state}`)],
											["worstState", t(`adaptiveRouting.states.${incident.worst_state}`)],
										] as const
									).map(([label, value]) => (
										<div key={label} className="min-w-0 space-y-1">
											<dt className="text-muted-foreground">{t(`adaptiveRouting.${label}`)}</dt>
											<dd className="break-words">{value}</dd>
										</div>
									))}
								</dl>
							</div>
							<div className="grid grid-cols-3 divide-x rounded-sm border">
								{(["checks", "failed_checks", "refused_again"] as const).map((key) => (
									<div key={key} className="space-y-2 p-3">
										<div className="text-muted-foreground text-xs">{t(`adaptiveRouting.${key}`)}</div>
										<div className="text-xl font-semibold tabular-nums">{incident[key].toLocaleString(locale)}</div>
									</div>
								))}
							</div>
							<section className="space-y-3">
								<h3 className="text-sm font-semibold">{t("adaptiveRouting.requestEvidence")}</h3>
								<div className="flex flex-wrap gap-2">
									{requestLinks
										.filter((link) => link.id)
										.map((link) => (
											<Button key={link.id} asChild variant="outline" size="sm">
												<a href={incidentRequestHref(link.id!, link.time)} data-testid="adaptive-incident-request">
													<FileText className="size-4" />
													{t(`adaptiveRouting.${link.label}`)}
												</a>
											</Button>
										))}
									{!requestLinks.some((link) => link.id) && (
										<p className="text-muted-foreground text-sm">{t("adaptiveRouting.noRequestEvidence")}</p>
									)}
								</div>
								<p className="text-muted-foreground text-xs">{t("adaptiveRouting.requestRetention")}</p>
							</section>
							<section className="space-y-4">
								<h3 className="text-sm font-semibold">{t("adaptiveRouting.events")}</h3>
								<ol className="space-y-4 border-l pl-4" data-testid="adaptive-incident-events">
									{detail.events.map((event) => (
										<li key={event.id} className="space-y-1">
											<div className="text-muted-foreground text-xs tabular-nums">{date(event.timestamp)}</div>
											<p className="text-sm">{event.message}</p>
											{event.failure_class && (
												<p className="text-muted-foreground text-xs" data-testid="adaptive-incident-event-cause">
													{t("adaptiveRouting.cause")}: <IncidentCause cause={event.failure_class} />
												</p>
											)}
											{event.request_id && (
												<a
													href={incidentRequestHref(event.request_id, event.timestamp)}
													className="text-primary inline-block max-w-full truncate font-mono text-xs underline-offset-4 hover:underline"
													title={event.request_id}
												>
													{event.request_id}
												</a>
											)}
										</li>
									))}
								</ol>
								{!detail.events.length && <p className="text-muted-foreground text-sm">{t("adaptiveRouting.noEvents")}</p>}
								<div className="flex justify-end gap-2">
									<Button
										variant="outline"
										size="icon"
										aria-label={t("labels.previousPage")}
										disabled={cursors.length === 1 || query.isFetching}
										onClick={() => setCursors((pages) => pages.slice(0, -1))}
									>
										<ChevronLeft />
									</Button>
									<Button
										variant="outline"
										size="icon"
										aria-label={t("labels.nextPage")}
										disabled={!detail.next_cursor || query.isFetching}
										onClick={() => setCursors((pages) => [...pages, detail.next_cursor!])}
									>
										<ChevronRight />
									</Button>
								</div>
							</section>
						</>
					)}
				</div>
			</SheetContent>
		</Sheet>
	);
}
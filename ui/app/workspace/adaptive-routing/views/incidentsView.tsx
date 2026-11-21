import PageTitle from "@/components/pageTitle";
import { PIN_SHADOW_RIGHT } from "@/components/table/columnPinning";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getProviderLabel } from "@/lib/constants/logs";
import { useLocale } from "@/lib/i18n/useLocale";
import { parseAsSafeString } from "@/lib/queryParamsParser";
import {
	useGetAdaptiveRoutingConfigQuery,
	useGetAdaptiveRoutingIncidentsQuery,
	useGetAdaptiveRoutingStatusQuery,
} from "@/lib/store/apis/adaptiveRoutingApi";
import { type AdaptiveRoutingIncidentQuery, INCIDENT_SCOPES, INCIDENT_SORTS, INCIDENT_STATUSES } from "@/lib/types/adaptiveRouting";
import { Link } from "@tanstack/react-router";
import { Activity, ChevronLeft, ChevronRight, PanelRightOpen, RefreshCw } from "lucide-react";
import { parseAsBoolean, parseAsStringLiteral, useQueryStates } from "nuqs";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { IncidentDetail } from "./incidentDetail";
import { IncidentAction, IncidentCause, incidentDuration, IncidentStatus, IncidentSummary } from "./incidentDisplay";
import { RoutingQueryState } from "./routingQueryState";

// The cursor stack changes only on navigation. Polling refreshes the same page.
function IncidentTable({
	filters,
	live,
	onSelect,
}: {
	filters: AdaptiveRoutingIncidentQuery;
	live: boolean;
	onSelect: (id: string) => void;
}) {
	const { t } = useTranslation("observability");
	const locale = useLocale();
	const [cursors, setCursors] = useState([""]);
	const query = useGetAdaptiveRoutingIncidentsQuery(
		{ ...filters, limit: 25, cursor: cursors[cursors.length - 1] },
		{
			pollingInterval: live ? 5000 : 0,
			skipPollingIfUnfocused: true,
		},
	);
	const data = query.currentData;
	const date = (value: string) => new Date(value).toLocaleString(locale);
	return (
		<div className="space-y-3">
			<div className="flex items-center justify-between gap-3">
				<p className="text-muted-foreground text-sm" aria-live="polite">
					{data ? t("adaptiveRouting.incidentCount", { formatted: data.total.toLocaleString(locale) }) : ""}
				</p>
				<Button variant="outline" onClick={() => void query.refetch()} disabled={query.isFetching} data-testid="adaptive-incidents-refresh">
					<RefreshCw className="size-4" />
					{t("labels.refresh")}
				</Button>
			</div>
			<RoutingQueryState loading={!data && query.isFetching} error={query.error} retry={() => void query.refetch()} />
			{data && (
				<>
					<div className="overflow-hidden rounded-sm border" data-testid="adaptive-incidents-table">
						<Table>
							<TableHeader>
								<TableRow>
									{(["opened", "duration", "incidentStatus", "where", "cause", "action"] as const).map((key) => (
										<TableHead key={key}>{t(`adaptiveRouting.${key}`)}</TableHead>
									))}
									<TableHead className={`bg-muted sticky right-0 z-10 ${PIN_SHADOW_RIGHT}`}>
										<span className="sr-only">{t("adaptiveRouting.incidentDetails")}</span>
									</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{data.incidents.map((incident) => (
									<TableRow key={incident.id} data-testid="adaptive-incident-row">
										<TableCell className="text-muted-foreground text-xs tabular-nums">
											<div>{date(incident.opened_at)}</div>
											{incident.closed_at && <div className="mt-1">→ {date(incident.closed_at)}</div>}
										</TableCell>
										<TableCell className="tabular-nums">{incidentDuration(incident, locale)}</TableCell>
										<TableCell>
											<IncidentStatus status={incident.status} />
										</TableCell>
										<TableCell>
											<button
												className="max-w-72 cursor-pointer space-y-1 text-left hover:underline"
												onClick={() => onSelect(incident.id)}
												data-testid="adaptive-incident-open"
											>
												<div className="truncate font-medium">
													{getProviderLabel(incident.provider)}
													{incident.model ? ` · ${incident.model}` : ""}
												</div>
												{(incident.key_name || incident.key_id) && (
													<div className="text-muted-foreground truncate text-xs" title={incident.key_id}>
														{incident.key_name || incident.key_id}
													</div>
												)}
												<div className="text-muted-foreground text-xs">{t(`adaptiveRouting.incidentScopes.${incident.scope}`)}</div>
											</button>
										</TableCell>
										<TableCell className="max-w-64 text-sm whitespace-normal">
											<div className="font-medium" data-testid="adaptive-incident-cause">
												<IncidentCause cause={incident.failure_class} />
											</div>
											<div className="text-muted-foreground mt-1 text-xs">
												<IncidentSummary incident={incident} />
											</div>
										</TableCell>
										<TableCell className="text-muted-foreground max-w-64 text-sm whitespace-normal">
											<IncidentAction incident={incident} />
										</TableCell>
										<TableCell className={`bg-card sticky right-0 z-10 ${PIN_SHADOW_RIGHT}`}>
											<Button
												variant="ghost"
												size="icon"
												aria-label={t("adaptiveRouting.incidentDetails")}
												onClick={() => onSelect(incident.id)}
											>
												<PanelRightOpen className="size-4" />
											</Button>
										</TableCell>
									</TableRow>
								))}
								{!data.incidents.length && (
									<TableRow>
										<TableCell colSpan={7} className="text-muted-foreground h-36 text-center whitespace-normal">
											{t("adaptiveRouting.noIncidents")}
										</TableCell>
									</TableRow>
								)}
							</TableBody>
						</Table>
					</div>
				</>
			)}
			{(data || cursors.length > 1) && (
				<div className="flex items-center justify-end gap-3 text-sm">
					<Button
						variant="outline"
						size="icon"
						aria-label={t("labels.previousPage")}
						disabled={cursors.length === 1}
						onClick={() => setCursors((pages) => pages.slice(0, -1))}
					>
						<ChevronLeft />
					</Button>
					<span className="tabular-nums" data-testid="adaptive-incidents-page">
						{t("adaptiveRouting.currentPage", { page: cursors.length.toLocaleString(locale) })}
					</span>
					<Button
						variant="outline"
						size="icon"
						aria-label={t("labels.nextPage")}
						disabled={!data?.next_cursor || query.isFetching}
						onClick={() => data?.next_cursor && setCursors((pages) => [...pages, data.next_cursor!])}
					>
						<ChevronRight />
					</Button>
				</div>
			)}
		</div>
	);
}

export default function IncidentsView() {
	const { t } = useTranslation("observability");
	const { t: shell } = useTranslation("shell");
	const [filters, setFilters] = useQueryStates({
		q: parseAsSafeString.withDefault(""),
		provider: parseAsSafeString.withDefault(""),
		model: parseAsSafeString.withDefault(""),
		key_id: parseAsSafeString.withDefault(""),
		status: parseAsStringLiteral(["", ...INCIDENT_STATUSES]).withDefault(""),
		scope: parseAsStringLiteral(["", ...INCIDENT_SCOPES]).withDefault(""),
		sort: parseAsStringLiteral(INCIDENT_SORTS).withDefault("newest"),
		from: parseAsSafeString.withDefault(""),
		to: parseAsSafeString.withDefault(""),
		live: parseAsBoolean.withDefault(true),
		incident: parseAsSafeString.withDefault(""),
	});
	const config = useGetAdaptiveRoutingConfigQuery();
	const status = useGetAdaptiveRoutingStatusQuery(undefined, { pollingInterval: filters.live ? 5000 : 0, skipPollingIfUnfocused: true });
	const queryFilters: AdaptiveRoutingIncidentQuery = {
		q: filters.q || undefined,
		provider: filters.provider || undefined,
		model: filters.model || undefined,
		key_id: filters.key_id || undefined,
		status: filters.status || undefined,
		scope: filters.scope || undefined,
		sort: filters.sort,
		from: filters.from || undefined,
		to: filters.to || undefined,
	};
	// Native datetime inputs display the user's local time; the API receives RFC3339.
	const localDate = (value: string) => {
		if (!value) return "";
		const date = new Date(value);
		if (!Number.isFinite(date.getTime())) return "";
		return new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
	};
	return (
		<div className="no-padding-parent flex min-w-0 flex-1 flex-col gap-4 p-4">
			<PageTitle
				breadcrumbs={[{ label: shell("nav.adaptiveRouting"), to: "/workspace/adaptive-routing" }, { label: shell("nav.incidents") }]}
			>
				{t("adaptiveRouting.incidentsHelp")}
			</PageTitle>
			<div className="flex flex-wrap items-center gap-2">
				<Button
					variant={filters.live ? "default" : "outline"}
					aria-pressed={filters.live}
					onClick={() => void setFilters({ live: !filters.live })}
				>
					<Activity className="size-4" />
					{t("labels.live")}
				</Button>
				<Input
					value={filters.q}
					onChange={(event) => void setFilters({ q: event.target.value })}
					placeholder={t("adaptiveRouting.search")}
					aria-label={t("adaptiveRouting.search")}
					className="min-w-48 flex-1"
					data-testid="adaptive-incidents-search"
				/>
				<Select value={filters.sort} onValueChange={(sort) => void setFilters({ sort: sort as typeof filters.sort })}>
					<SelectTrigger className="w-52" aria-label={t("adaptiveRouting.sort")}>
						<SelectValue />
					</SelectTrigger>
					<SelectContent>
						{INCIDENT_SORTS.map((sort) => (
							<SelectItem key={sort} value={sort}>
								{t(`adaptiveRouting.incidentSorts.${sort}`)}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
				<Button variant="outline" asChild>
					<Link to="/workspace/adaptive-routing">{shell("nav.adaptiveRoutingDashboard")}</Link>
				</Button>
			</div>
			<div className="flex flex-wrap gap-3 rounded-sm border p-3">
				{(["provider", "model", "key_id"] as const).map((key) => (
					<label key={key} className="min-w-40 flex-1 space-y-1 text-xs">
						<span className="text-muted-foreground">{t(`adaptiveRouting.filterFields.${key}`)}</span>
						<Input
							value={filters[key]}
							onChange={(event) => void setFilters({ [key]: event.target.value })}
							data-testid={`adaptive-incidents-${key}`}
						/>
					</label>
				))}
				<label className="min-w-44 flex-1 space-y-1 text-xs">
					<span className="text-muted-foreground">{t("adaptiveRouting.incidentStatus")}</span>
					<Select
						value={filters.status || "all"}
						onValueChange={(value) => void setFilters({ status: value === "all" ? "" : (value as typeof filters.status) })}
					>
						<SelectTrigger aria-label={t("adaptiveRouting.incidentStatus")}>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="all">{t("adaptiveRouting.allStatuses")}</SelectItem>
							{INCIDENT_STATUSES.map((value) => (
								<SelectItem key={value} value={value}>
									{t(`adaptiveRouting.incidentStatuses.${value}`)}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</label>
				<label className="min-w-40 flex-1 space-y-1 text-xs">
					<span className="text-muted-foreground">{t("adaptiveRouting.scope")}</span>
					<Select
						value={filters.scope || "all"}
						onValueChange={(value) => void setFilters({ scope: value === "all" ? "" : (value as typeof filters.scope) })}
					>
						<SelectTrigger aria-label={t("adaptiveRouting.scope")}>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="all">{t("adaptiveRouting.allScopes")}</SelectItem>
							{INCIDENT_SCOPES.map((value) => (
								<SelectItem key={value} value={value}>
									{t(`adaptiveRouting.incidentScopes.${value}`)}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</label>
				{(["from", "to"] as const).map((key) => (
					<label key={key} className="min-w-48 flex-1 space-y-1 text-xs">
						<span className="text-muted-foreground">{t(`adaptiveRouting.filterFields.${key}`)}</span>
						<Input
							type="datetime-local"
							value={localDate(filters[key])}
							onChange={(event) => void setFilters({ [key]: event.target.value ? new Date(event.target.value).toISOString() : "" })}
							data-testid={`adaptive-incidents-${key}`}
						/>
					</label>
				))}
			</div>
			{status.data?.persistence && !status.data.persistence.healthy && (
				<Alert variant="destructive">
					<AlertDescription>{t("adaptiveRouting.persistenceFailed")}</AlertDescription>
				</Alert>
			)}
			{config.data && !config.data.history_enabled && (
				<Alert>
					<AlertDescription>{t("adaptiveRouting.historyDisabled")}</AlertDescription>
				</Alert>
			)}
			<IncidentTable
				key={JSON.stringify(queryFilters)}
				filters={queryFilters}
				live={filters.live}
				onSelect={(incident) => void setFilters({ incident })}
			/>
			{filters.incident && (
				<IncidentDetail key={filters.incident} id={filters.incident} live={filters.live} close={() => void setFilters({ incident: "" })} />
			)}
		</div>
	);
}
import PageTitle, { PageTagBadge, type PageTagTone } from "@/components/pageTitle";
import { PIN_SHADOW_RIGHT } from "@/components/table/columnPinning";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ProviderSelector } from "@/components/ui/providerSelector";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { getProviderLabel } from "@/lib/constants/logs";
import { useLocale } from "@/lib/i18n/useLocale";
import { parseAsSafeArrayOf, parseAsSafeString } from "@/lib/queryParamsParser";
import { useGetAdaptiveRoutingStatusQuery } from "@/lib/store/apis/adaptiveRoutingApi";
import { type AdaptiveRouteStatus, ROUTE_STATES, type RouteState } from "@/lib/types/adaptiveRouting";
import { withBasePath } from "@/lib/utils/basePath";
import { Link } from "@tanstack/react-router";
import { type ColumnDef, flexRender, getCoreRowModel, getPaginationRowModel, useReactTable } from "@tanstack/react-table";
import { Activity, ChevronLeft, ChevronRight, FileText, RefreshCw, Settings2 } from "lucide-react";
import { createSerializer, parseAsBoolean, parseAsInteger, parseAsString, parseAsStringLiteral, useQueryStates } from "nuqs";
import { useEffect, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { RoutingQueryState } from "./routingQueryState";
import { ReleaseHoldButton } from "./releaseHoldButton";
import { IncidentCause } from "./incidentDisplay";

const stateTones: Record<RouteState, PageTagTone> = {
	unknown: "neutral",
	healthy: "success",
	degraded: "warning",
	held: "danger",
	failed: "danger",
	recovering: "info",
};
const serializeLogs = createSerializer({
	providers: parseAsSafeArrayOf,
	models: parseAsSafeArrayOf,
	selected_key_ids: parseAsSafeArrayOf,
	start_time: parseAsInteger,
	end_time: parseAsInteger,
	period: parseAsString,
});
const emptyRoutes: AdaptiveRouteStatus[] = [];
type RouteRow = AdaptiveRouteStatus;

function logsHref(row?: RouteRow) {
	return withBasePath(
		serializeLogs(
			"/workspace/logs",
			row
				? {
						providers: [row.provider],
						models: row.model ? [row.model] : [],
						selected_key_ids: row.key_id ? [row.key_id] : [],
					}
				: {},
		),
	);
}

function RouteActions({ row }: { row: RouteRow }) {
	const { t } = useTranslation("observability");
	const keyWide = !row.model || row.failure_class === "credential" || row.failure_class === "quota";
	return (
		<div className="flex justify-end gap-2">
			{row.state === "held" && <ReleaseHoldButton provider={row.provider} keyId={row.key_id} model={keyWide ? undefined : row.model} />}
			<Button asChild variant="ghost">
				<a href={logsHref(row)} aria-label={t("adaptiveRouting.viewLogs")} title={t("adaptiveRouting.viewLogs")}>
					<FileText className="size-4" />
				</a>
			</Button>
		</div>
	);
}

export default function AdaptiveRoutingView() {
	const { t } = useTranslation("observability");
	const { t: shell } = useTranslation("shell");
	const { t: common } = useTranslation("common");
	const locale = useLocale();
	const [filters, setFilters] = useQueryStates({
		q: parseAsSafeString.withDefault(""),
		provider: parseAsSafeString.withDefault(""),
		state: parseAsStringLiteral(["all", ...ROUTE_STATES]).withDefault("all"),
		live: parseAsBoolean.withDefault(true),
	});
	const status = useGetAdaptiveRoutingStatusQuery(undefined, {
		pollingInterval: filters.live ? 5000 : 0,
		skipPollingIfUnfocused: true,
	});
	const query = status;
	const routes = status.data?.routes ?? emptyRoutes;
	const source = routes;
	const providers = useMemo(() => [...new Set(source.map((row) => row.provider))], [source]);
	const filtered = useMemo(() => {
		const search = filters.q.trim().toLowerCase();
		return source.filter(
			(row) =>
				(!filters.provider || row.provider === filters.provider) &&
				(filters.state === "all" || row.state === filters.state) &&
				(!search ||
					[row.provider, getProviderLabel(row.provider), row.model, row.key_name, row.key_id].some((value) =>
						value.toLowerCase().includes(search),
					)),
		);
	}, [source, filters.provider, filters.state, filters.q]);
	const columns = useMemo<ColumnDef<RouteRow>[]>(() => {
		const date = (value?: string) => (value && !value.startsWith("0001-") ? new Date(value).toLocaleString(locale) : t("labels.nA"));
		return [
			{
				id: "route",
				header: t("adaptiveRouting.providerKey"),
				cell: ({ row }) => (
					<div className="max-w-64 space-y-1 py-1">
						<div className="truncate font-medium" title={row.original.provider}>
							{getProviderLabel(row.original.provider)}
						</div>
						<div className="text-muted-foreground truncate font-mono text-xs" title={row.original.key_id}>
							{row.original.key_name || row.original.key_id || t("labels.unnamed")}
						</div>
					</div>
				),
			},
			{
				accessorKey: "model",
				header: t("labels.model"),
				cell: ({ row }) => (
					<span className="block max-w-56 truncate" title={row.original.model}>
						{row.original.model || t("labels.allModels")}
					</span>
				),
			},
			{
				id: "state",
				header: t("adaptiveRouting.state"),
				cell: ({ row }) => (
					<div className="space-y-1">
						<PageTagBadge
							tag={{
								label: t(`adaptiveRouting.states.${row.original.state}`),
								tone: stateTones[row.original.state],
							}}
						/>
						{row.original.hold_until && (
							<div className="text-muted-foreground text-xs">{t("adaptiveRouting.holdUntil", { time: date(row.original.hold_until) })}</div>
						)}
					</div>
				),
			},
			{
				accessorKey: "score",
				header: t("adaptiveRouting.score"),
				cell: ({ row }) => (
					<span className="tabular-nums">
						{row.original.samples ? row.original.score.toLocaleString(locale, { maximumFractionDigits: 1 }) : t("labels.nA")}
					</span>
				),
			},
			{
				accessorKey: "error_rate",
				header: t("adaptiveRouting.errorRate"),
				cell: ({ row }) => (
					<span className="tabular-nums">
						{row.original.samples
							? row.original.error_rate.toLocaleString(locale, {
									style: "percent",
									maximumFractionDigits: 1,
								})
							: t("labels.nA")}
					</span>
				),
			},
			...([
				{
					accessorKey: "latency_ms",
					header: t("adaptiveRouting.firstResponse"),
					cell: ({ row }) => (
						<span className="tabular-nums">
							{row.original.samples
								? t("adaptiveRouting.milliseconds", {
										value: row.original.latency_ms.toLocaleString(locale, {
											maximumFractionDigits: 0,
										}),
									})
								: t("labels.nA")}
						</span>
					),
				},
				{
					accessorKey: "inflight",
					header: t("adaptiveRouting.inflight"),
					cell: ({ row }) => row.original.inflight.toLocaleString(locale),
				},
				{
					accessorKey: "samples",
					header: t("adaptiveRouting.samples"),
					cell: ({ row }) => row.original.samples.toLocaleString(locale),
				},
			] satisfies ColumnDef<RouteRow>[]),
			{
				id: "failure",
				header: t("adaptiveRouting.cause"),
				cell: ({ row }) => <IncidentCause cause={row.original.failure_class} />,
			},
			{
				id: "actions",
				header: () => <span className="sr-only">{common("actions")}</span>,
				cell: ({ row }) => <RouteActions row={row.original} />,
			},
		];
	}, [t, common, locale]);
	const table = useReactTable({
		data: filtered,
		columns,
		getCoreRowModel: getCoreRowModel(),
		getPaginationRowModel: getPaginationRowModel(),
		initialState: { pagination: { pageSize: 25 } },
		autoResetPageIndex: false,
		getRowId: (row) => JSON.stringify([row.provider, row.model, row.key_id]),
	});
	useEffect(() => {
		table.setPageIndex(0);
	}, [filters.q, filters.provider, filters.state, table]);
	const lastPageIndex = Math.max(0, table.getPageCount() - 1);
	useEffect(() => {
		table.setPageIndex((pageIndex) => Math.min(pageIndex, lastPageIndex));
	}, [lastPageIndex, table]);
	const canFilter = source.length > 0 || Boolean(filters.q || filters.provider || filters.state !== "all");
	const heldKeys = new Set(routes.filter((row) => row.state === "held").map((row) => JSON.stringify([row.provider, row.key_id]))).size;
	const summary = [
		{ label: "routes", value: routes.length },
		{ label: "healthy", value: routes.filter((row) => row.state === "healthy").length },
		{ label: "heldKeys", value: heldKeys },
		{ label: "inflight", value: routes.reduce((sum, row) => sum + row.inflight, 0) },
	] as const;
	function refresh() {
		void status.refetch();
	}

	return (
		<div className="no-padding-parent flex min-w-0 flex-1 flex-col gap-4 p-4">
			<PageTitle
				title={shell("nav.adaptiveRouting")}
				tags={
					status.data
						? [
								{
									label: t(status.data.enabled ? "adaptiveRouting.configured" : "adaptiveRouting.unconfigured"),
									tone: status.data.enabled ? "success" : "neutral",
								},
							]
						: []
				}
			>
				{t("adaptiveRouting.healthHelp")}
			</PageTitle>
			<div className="flex flex-wrap items-center justify-between gap-3">
				<div className="flex items-center gap-2">
					<Button variant="outline" onClick={refresh} disabled={query.isFetching}>
						<RefreshCw className="size-4" />
						{t("labels.refresh")}
					</Button>
					<Button
						variant={filters.live ? "default" : "outline"}
						aria-pressed={filters.live}
						onClick={() => void setFilters({ live: !filters.live })}
					>
						<Activity className="size-4" />
						{t("labels.live")}
					</Button>
				</div>
				<div className="flex flex-wrap gap-2">
					<Button variant="outline" asChild>
						<Link to="/workspace/adaptive-routing/incidents">{shell("nav.incidents")}</Link>
					</Button>
					<Button variant="outline" asChild>
						<Link to="/workspace/adaptive-routing/settings">
							<Settings2 className="size-4" />
							{shell("nav.adaptiveRoutingSettings")}
						</Link>
					</Button>
				</div>
			</div>
			<RoutingQueryState loading={query.isLoading} error={query.error} retry={refresh} />
			{status.data?.persistence && !status.data.persistence.healthy && (
				<Alert variant="destructive">
					<AlertDescription>{t("adaptiveRouting.persistenceFailed")}</AlertDescription>
				</Alert>
			)}
			{status.data && !status.data.enabled && (
				<Alert>
					<AlertDescription className="flex flex-wrap items-center justify-between gap-3">
						<span>{t("adaptiveRouting.unconfiguredHelp")}</span>
						<Button asChild>
							<Link to="/workspace/adaptive-routing/settings">{shell("nav.adaptiveRoutingSettings")}</Link>
						</Button>
					</AlertDescription>
				</Alert>
			)}
			{status.data && (
				<div className="grid grid-cols-2 divide-x divide-y rounded-sm border sm:grid-cols-4 sm:divide-y-0">
					{summary.map(({ label, value }) => (
						<div key={label} className="space-y-2 p-4">
							<div className="text-muted-foreground text-xs font-medium">{t(`adaptiveRouting.${label}`)}</div>
							<div className="text-2xl font-semibold tabular-nums">{value.toLocaleString(locale)}</div>
						</div>
					))}
				</div>
			)}
			<div className="space-y-1">
				<h2 className="text-sm font-semibold">{t("adaptiveRouting.routeHealth")}</h2>
				<p className="text-muted-foreground text-sm">{t("adaptiveRouting.healthHelp")}</p>
			</div>
			{canFilter && (
				<div className="flex flex-wrap gap-2">
					<Input
						value={filters.q}
						onChange={(event) => void setFilters({ q: event.target.value })}
						placeholder={t("adaptiveRouting.search")}
						aria-label={t("adaptiveRouting.search")}
						className="min-w-48 flex-1"
						data-testid="adaptive-routing-search"
					/>
					<ProviderSelector
						source="values"
						values={providers}
						value={filters.provider}
						onChange={(provider: string) => void setFilters({ provider })}
						allOption={{ value: "", label: t("labels.allProviders") }}
						placeholder={t("labels.allProviders")}
						ariaLabel={t("labels.provider")}
						className="w-48"
					/>
					<Select value={filters.state} onValueChange={(state) => void setFilters({ state: state as typeof filters.state })}>
						<SelectTrigger className="w-44" aria-label={t("adaptiveRouting.state")}>
							<SelectValue />
						</SelectTrigger>
						<SelectContent>
							<SelectItem value="all">{t("adaptiveRouting.allStates")}</SelectItem>
							{ROUTE_STATES.map((state) => (
								<SelectItem key={state} value={state}>
									{t(`adaptiveRouting.states.${state}`)}
								</SelectItem>
							))}
						</SelectContent>
					</Select>
				</div>
			)}
			{query.data && (
				<div className="overflow-hidden rounded-sm border" data-testid="adaptive-routing-table">
					<Table>
						<TableHeader>
							{table.getHeaderGroups().map((group) => (
								<TableRow key={group.id}>
									{group.headers.map((header) => (
										<TableHead
											key={header.id}
											className={header.id === "actions" ? `bg-muted sticky right-0 z-10 ${PIN_SHADOW_RIGHT}` : undefined}
										>
											{flexRender(header.column.columnDef.header, header.getContext())}
										</TableHead>
									))}
								</TableRow>
							))}
						</TableHeader>
						<TableBody>
							{table.getRowModel().rows.length ? (
								table.getRowModel().rows.map((row) => (
									<TableRow key={row.id}>
										{row.getVisibleCells().map((cell) => (
											<TableCell
												key={cell.id}
												className={cell.column.id === "actions" ? `bg-card sticky right-0 z-10 ${PIN_SHADOW_RIGHT}` : undefined}
											>
												{flexRender(cell.column.columnDef.cell, cell.getContext())}
											</TableCell>
										))}
									</TableRow>
								))
							) : (
								<TableRow>
									<TableCell colSpan={columns.length} className="text-muted-foreground h-36 text-center whitespace-normal">
										{t(source.length ? "adaptiveRouting.noMatches" : "adaptiveRouting.noRoutes")}
									</TableCell>
								</TableRow>
							)}
						</TableBody>
					</Table>
				</div>
			)}
			{filtered.length > 0 && (
				<div className="flex items-center justify-between gap-3 text-sm">
					<span className="text-muted-foreground">
						{t("adaptiveRouting.entries", {
							count: filtered.length,
							formatted: filtered.length.toLocaleString(locale),
						})}
					</span>
					<div className="flex items-center gap-3">
						<Button
							variant="outline"
							size="icon"
							aria-label={t("labels.previousPage")}
							disabled={!table.getCanPreviousPage()}
							onClick={() => table.previousPage()}
						>
							<ChevronLeft />
						</Button>
						<span className="tabular-nums">
							{t("adaptiveRouting.page", {
								current: (table.getState().pagination.pageIndex + 1).toLocaleString(locale),
								total: table.getPageCount().toLocaleString(locale),
							})}
						</span>
						<Button
							variant="outline"
							size="icon"
							aria-label={t("labels.nextPage")}
							disabled={!table.getCanNextPage()}
							onClick={() => table.nextPage()}
						>
							<ChevronRight />
						</Button>
					</div>
				</div>
			)}
		</div>
	);
}
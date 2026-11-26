import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Sheet, SheetContent } from "@/components/ui/sheet";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ProviderIconType, RenderProviderIcon } from "@/lib/constants/icons";
import type { ProviderName } from "@/lib/constants/logs";
import { getRequestTypeLabel, RequestTypeColors, Status, StatusBarColors } from "@/lib/constants/logs";
import { getErrorMessage } from "@/lib/store";
import { useGetLogSessionByIdInfiniteQuery, useGetLogSessionSummaryByIdQuery } from "@/lib/store/apis/logsApi";
import { LogEntry, LogSessionSummaryResponse } from "@/lib/types/logs";
import { cn } from "@/lib/utils";
import { format } from "date-fns";
import { dateFnsLocale } from "@/lib/i18n/dateLocale";
import { ArrowDown, ArrowUp, Loader2 } from "lucide-react";
import { useEffect, useId, useMemo, useState } from "react";
import { toast } from "sonner";
import { useTranslation } from "react-i18next";
import { LogMessageCell } from "../views/columns";
import { getLocale } from "@/lib/i18n";

const HIGHLIGHTED_ROW =
	"border-l-2 border-l-sky-500 bg-sky-500/[0.08] shadow-[inset_0_0_0_1px_rgba(56,189,248,0.18)] hover:bg-sky-500/[0.24] hover:shadow-[inset_0_0_0_1px_rgba(56,189,248,0.38)] dark:hover:bg-sky-400/[0.18]";

function formatDurationFromMs(durationMs?: number) {
	if (!durationMs || durationMs <= 0) return "0s";
	const totalSeconds = Math.floor(durationMs / 1000);
	const hours = Math.floor(totalSeconds / 3600);
	const minutes = Math.floor((totalSeconds % 3600) / 60);
	const seconds = totalSeconds % 60;

	if (hours > 0) {
		return `${hours}h ${String(minutes).padStart(2, "0")}m ${String(seconds).padStart(2, "0")}s`;
	}
	if (minutes > 0) {
		return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
	}
	return `${seconds}s`;
}

interface SummaryCard {
	label: string;
	value: string;
	helper?: string;
	size?: "sm";
}

interface SessionDetailsSheetProps {
	sessionId: string | null;
	highlightedLogId?: string | null;
	open: boolean;
	onOpenChange: (open: boolean) => void;
	onLogClick?: (log: LogEntry) => void;
	onFilterByParentRequestId?: (parentRequestId: string) => void;
}

export function SessionDetailsSheet(props: SessionDetailsSheetProps) {
	const { sessionId, open } = props;
	const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");
	const { currentData: sessionSummary } = useGetLogSessionSummaryByIdQuery(sessionId || "", {
		skip: !open || !sessionId,
		pollingInterval: 5000,
		refetchOnMountOrArgChange: true,
	});

	if (!open || !sessionId) return null;

	return (
		<OpenSessionDetailsSheet
			key={sessionId + ":" + sortOrder}
			{...props}
			sessionId={sessionId}
			sortOrder={sortOrder}
			onSortOrderChange={() => setSortOrder((previous) => (previous === "asc" ? "desc" : "asc"))}
			sessionSummary={sessionSummary}
		/>
	);
}

interface OpenSessionDetailsSheetProps extends SessionDetailsSheetProps {
	sessionId: string;
	sortOrder: "asc" | "desc";
	onSortOrderChange: () => void;
	sessionSummary?: LogSessionSummaryResponse;
}

function OpenSessionDetailsSheet({
	sessionId,
	highlightedLogId,
	open,
	onOpenChange,
	onLogClick,
	onFilterByParentRequestId,
	sortOrder,
	onSortOrderChange,
	sessionSummary,
}: OpenSessionDetailsSheetProps) {
	const { t, i18n } = useTranslation("observability");
	const dateLocale = dateFnsLocale(i18n.language);
	// This committed opening owns its pages. Closing or changing session/order
	// unmounts it, so an unfinished Load more cannot suppress the next offset0.
	const openingId = useId();
	const {
		currentData: sessionPages,
		isFetching: loadingSession,
		isError: sessionFailed,
		error: sessionError,
		hasNextPage,
		fetchNextPage,
	} = useGetLogSessionByIdInfiniteQuery({ sessionId, order: sortOrder, openingId });
	const sessionLogs = useMemo(() => {
		const seen = new Map<string, LogEntry>();
		for (const page of sessionPages?.pages ?? []) {
			for (const log of page.logs) seen.set(log.id, log);
		}
		return Array.from(seen.values()).sort((a, b) =>
			sortOrder === "asc"
				? new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
				: new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime(),
		);
	}, [sessionPages, sortOrder]);
	const hasMore = !!sessionPages && hasNextPage;

	useEffect(() => {
		if (open && sessionId && sessionFailed) {
			toast.error(t("logs.session.loadFailed"), { description: getErrorMessage(sessionError) });
		}
	}, [open, sessionId, sessionFailed, sessionError, t]);

	useEffect(() => {
		if (open && sessionId && !loadingSession && !sessionFailed && sessionPages?.pages[0]?.count === 0) {
			onOpenChange(false);
		}
	}, [open, sessionId, loadingSession, sessionFailed, sessionPages, onOpenChange]);

	const summaryCards: SummaryCard[] = useMemo(
		() => [
			{
				label: t("logs.session.logs"),
				value: (sessionSummary?.count || 0).toLocaleString(getLocale()),
				helper:
					sessionSummary && sessionLogs.length < sessionSummary.count
						? t("logs.session.loadedCount", { count: sessionLogs.length.toLocaleString(getLocale()) })
						: undefined,
			},
			{
				label: t("logs.session.totalCost"),
				value: `$${(sessionSummary?.total_cost || 0).toFixed(4)}`,
			},
			{
				label: t("logs.session.totalTokens"),
				value: (sessionSummary?.total_tokens || 0).toLocaleString(getLocale()),
			},
			{
				label: t("logs.session.started"),
				value: sessionSummary?.started_at
					? format(new Date(sessionSummary.started_at), "MMM d, yyyy hh:mm:ss aa", { locale: dateLocale })
					: t("labels.nA"),
				size: "sm",
			},
			{
				label: t("logs.session.latestUpdate"),
				value: sessionSummary?.latest_at
					? format(new Date(sessionSummary.latest_at), "MMM d, yyyy hh:mm:ss aa", { locale: dateLocale })
					: t("labels.nA"),
				size: "sm",
			},
			{
				label: t("logs.session.duration"),
				value: formatDurationFromMs(sessionSummary?.duration_ms),
			},
		],
		[sessionSummary, sessionLogs.length, t, dateLocale],
	);

	return (
		<Sheet open={open} onOpenChange={onOpenChange}>
			<SheetContent className="flex w-full flex-col gap-4 overflow-x-hidden p-4 sm:max-w-[60%] md:p-8">
				<div className="flex items-center justify-between gap-4">
					<div>
						<div className="text-lg font-medium">{t("labels.session")}</div>
						{sessionId && onFilterByParentRequestId ? (
							<Tooltip>
								<TooltipTrigger asChild>
									<button
										type="button"
										className="text-primary hover:text-primary/80 cursor-pointer text-left font-mono text-sm break-all underline-offset-2 hover:underline"
										onClick={() => onFilterByParentRequestId(sessionId)}
									>
										{sessionId}
									</button>
								</TooltipTrigger>
								<TooltipContent sideOffset={6}>{t("logs.session.filterThis")}</TooltipContent>
							</Tooltip>
						) : (
							<code className="text-sm break-all">{sessionId}</code>
						)}
					</div>
					<div className="flex items-center gap-3">
						<Button variant="outline" size="sm" data-testid="session-details-sort-btn" onClick={onSortOrderChange}>
							{sortOrder === "asc" ? <ArrowUp className="mr-2 h-4 w-4" /> : <ArrowDown className="mr-2 h-4 w-4" />}
							{sortOrder === "asc" ? t("logs.session.earliestFirst") : t("logs.session.latestFirst")}
						</Button>
					</div>
				</div>

				<div className="grid shrink-0 grid-cols-1 gap-4 sm:grid-cols-3">
					{summaryCards.map((card) => (
						<Card key={card.label} className="py-4 shadow-none">
							<CardContent className="px-4">
								<div className="text-muted-foreground text-xs">{card.label}</div>
								<div
									className={
										card.size === "sm"
											? "font-mono text-sm leading-5 break-words sm:text-base"
											: "truncate font-mono text-xl font-medium sm:text-2xl"
									}
								>
									{card.helper ? (
										<div className="flex items-baseline gap-2">
											<span>{card.value}</span>
											<span className="text-muted-foreground text-sm">{card.helper}</span>
										</div>
									) : (
										card.value
									)}
								</div>
							</CardContent>
						</Card>
					))}
				</div>

				<div className="min-h-0 flex-1 overflow-hidden rounded-sm border">
					<Table containerClassName="h-full overflow-auto">
						<TableHeader className="sticky top-0 z-10 bg-[#f9f9f9] dark:bg-[#27272a]">
							<TableRow>
								<TableHead className="w-2"></TableHead>
								<TableHead>{t("labels.time")}</TableHead>
								<TableHead>{t("labels.type")}</TableHead>
								<TableHead>{t("labels.message")}</TableHead>
								<TableHead>{t("labels.provider")}</TableHead>
								<TableHead>{t("labels.model")}</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{loadingSession && sessionLogs.length === 0 ? (
								<TableRow>
									<TableCell colSpan={6} className="h-24 text-center">
										<div className="flex items-center justify-center gap-2">
											<Loader2 className="h-4 w-4 animate-spin" />
											{t("logs.session.loading")}
										</div>
									</TableCell>
								</TableRow>
							) : sessionLogs.length ? (
								sessionLogs.map((log) => (
									<TableRow
										key={log.id}
										className={cn("cursor-pointer transition-colors", log.id === highlightedLogId ? HIGHLIGHTED_ROW : "hover:bg-muted/40")}
										onClick={() => {
											if (open && sessionId) onLogClick?.(log);
										}}
									>
										<TableCell>
											<div className={`h-6 w-1 rounded-sm ${StatusBarColors[log.status as Status]}`} />
										</TableCell>
										<TableCell className="relative text-xs">
											{log.id === highlightedLogId ? (
												<div className="bg-background pointer-events-none absolute -top-1.5 left-1 z-10 rounded-full border border-sky-400/45 px-1.5 py-0 text-[9px] leading-tight font-semibold tracking-wide text-sky-600 uppercase dark:text-sky-300">
													{t("logs.session.current")}
												</div>
											) : null}
											{format(new Date(log.timestamp), "yyyy-MM-dd hh:mm:ss aa (XXX)", { locale: dateLocale })}
										</TableCell>
										<TableCell>
											<Badge variant="outline" className={`${RequestTypeColors[log.object as keyof typeof RequestTypeColors]} text-xs`}>
												{getRequestTypeLabel(log.object, t)}
											</Badge>
										</TableCell>
										<TableCell className="max-w-[360px]">
											<LogMessageCell log={log} contentClassName="max-w-[360px]" />
										</TableCell>
										<TableCell>
											<Badge variant="secondary" className="font-mono text-xs uppercase">
												<RenderProviderIcon provider={log.provider as ProviderIconType} size="sm" />
												{log.provider as ProviderName}
											</Badge>
										</TableCell>
										<TableCell className="max-w-[140px] truncate font-mono text-xs">{log.model || t("labels.nA")}</TableCell>
									</TableRow>
								))
							) : (
								<TableRow>
									<TableCell colSpan={6} className="text-muted-foreground h-24 text-center">
										{t("logs.session.empty")}
									</TableCell>
								</TableRow>
							)}
						</TableBody>
					</Table>
				</div>

				{hasMore ? (
					<div className="flex justify-center">
						<Button variant="outline" data-testid="session-details-load-more-btn" onClick={() => fetchNextPage()} disabled={loadingSession}>
							{loadingSession ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
							{t("logs.session.loadMore")}
						</Button>
					</div>
				) : null}
			</SheetContent>
		</Sheet>
	);
}
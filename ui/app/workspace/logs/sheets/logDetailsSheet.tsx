import { SheetNavigationButtons } from "@/components/sheetNavigationButtons";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { useSheetNavigation } from "@/hooks/useSheetNavigation";
import { getErrorMessage } from "@/lib/store/apis/baseApi";
import { useGetLogByIdQuery } from "@/lib/store/apis/logsApi";
import { useGetPromptQuery } from "@/lib/store/apis/promptsApi";
import type { LogEntry } from "@/lib/types/logs";
import { AlertCircle, Loader2 } from "lucide-react";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { LogDetailView } from "./logDetailView";

interface LogDetailSheetProps {
	logId: string | null;
	log: LogEntry | null;
	open: boolean;
	onOpenChange: (open: boolean) => void;
	handleDelete?: (log: LogEntry) => void;
	onNavigate?: (direction: "prev" | "next") => void;
	hasPrev?: boolean;
	hasNext?: boolean;
	canReveal?: boolean;
	onViewSession?: (sessionId: string, logId: string) => void;
	onFilterByParentRequestId?: (parentRequestId: string) => void;
	onFilterBySessionId?: (sessionId: string) => void;
	onOpenLog?: (logId: string) => void;
}

export function LogDetailSheet({
	logId,
	log,
	open,
	onOpenChange,
	handleDelete,
	onNavigate,
	hasPrev = false,
	hasNext = false,
	canReveal = false,
	onViewSession,
	onFilterByParentRequestId,
	onFilterBySessionId,
	onOpenLog,
}: LogDetailSheetProps) {
	const { t } = useTranslation("observability");
	const [pollingInterval, setPollingInterval] = useState(0);
	const matchingLog = log?.id === logId ? log : null;
	const {
		currentData: fullLog,
		isLoading,
		isError,
		error,
	} = useGetLogByIdQuery(matchingLog ? { id: matchingLog.id, timestamp: matchingLog.timestamp } : (logId ?? ""), {
		skip: !open || !logId,
		pollingInterval,
	});

	const shouldPoll = isError || fullLog?.status === "processing";

	// A successful current-ID cache remains usable during polling and after its error.
	const displayLog = fullLog?.id === logId ? fullLog : matchingLog;
	const isFullDataReady = displayLog != null && (isError || (fullLog?.id === logId && !isLoading));
	// The matching list row lets prompt and detail requests start in parallel.
	const selectedPromptId = displayLog?.selected_prompt_id;
	const { currentData: selectedPromptData } = useGetPromptQuery(selectedPromptId ?? "", {
		skip: !open || !selectedPromptId,
	});

	useEffect(() => {
		setPollingInterval(shouldPoll ? 2000 : 0);
	}, [shouldPoll]);

	// Keyboard navigation: arrow up/down to navigate between logs
	const { prev: prevKeys, next: nextKeys } = useSheetNavigation({
		enabled: open,
		hasPrev,
		hasNext,
		onNavigate: (direction) => onNavigate?.(direction),
	});

	if (!logId) return null;

	const resolvedSelectedPromptName = selectedPromptData?.prompt?.name ?? displayLog?.selected_prompt_name ?? "";

	return (
		<Sheet open={open} onOpenChange={onOpenChange}>
			<SheetContent className="border-secondary flex w-full flex-col gap-2 overflow-x-hidden border p-0 sm:max-w-[60%] 2xl:max-w-[40%]">
				{!isFullDataReady || !displayLog ? (
					<>
						<SheetHeader headerClassName="mb-0 px-4 pt-4 md:px-8 md:pt-8">
							<div className="flex min-w-0 flex-col gap-1">
								<SheetTitle>{isError ? t("logs.detail.requestDetails") : t("logs.detail.loadingDetails")}</SheetTitle>
								<p className="text-muted-foreground font-mono text-xs break-all">{logId}</p>
							</div>
						</SheetHeader>
						{isError ? (
							<div className="px-4 md:px-8">
								<Alert variant="destructive">
									<AlertCircle className="h-4 w-4" />
									<AlertDescription>{getErrorMessage(error)}</AlertDescription>
								</Alert>
							</div>
						) : (
							<div className="flex min-h-0 flex-1 items-center justify-center">
								<Loader2 className="text-muted-foreground h-6 w-6 animate-spin" />
							</div>
						)}
					</>
				) : (
					<LogDetailView
						log={displayLog}
						resolvedSelectedPromptName={resolvedSelectedPromptName}
						handleDelete={handleDelete}
						canReveal={canReveal}
						onClose={() => onOpenChange(false)}
						onFilterByParentRequestId={onFilterByParentRequestId}
						onFilterBySessionId={onFilterBySessionId}
						onOpenLog={onOpenLog}
						headerAction={
							<>
								{displayLog.parent_request_id && onViewSession ? (
									<Button
										variant="outline"
										size="sm"
										data-testid="session-button-view"
										onClick={() => onViewSession(displayLog.parent_request_id as string, displayLog.id)}
									>
										{t("logs.detail.viewSession")}
									</Button>
								) : null}
								<SheetNavigationButtons
									hasPrev={hasPrev}
									hasNext={hasNext}
									onNavigate={(dir) => onNavigate?.(dir)}
									prevKeys={prevKeys}
									nextKeys={nextKeys}
									entityLabel={t("logs.detail.entityLog")}
								/>
							</>
						}
					/>
				)}
			</SheetContent>
		</Sheet>
	);
}
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { getErrorMessage } from "@/lib/store/apis/baseApi";
import { useTranslation } from "react-i18next";

export function RoutingQueryState({ loading, error, retry }: { loading: boolean; error?: unknown; retry: () => void }) {
	const { t } = useTranslation("common");
	if (loading)
		return (
			<div className="text-muted-foreground p-8 text-center text-sm" role="status">
				{t("loading")}
			</div>
		);
	if (!error) return null;
	return (
		<Alert variant="destructive">
			<AlertDescription className="flex flex-wrap items-center justify-between gap-3">
				<span>{getErrorMessage(error)}</span>
				<Button variant="outline" onClick={retry}>
					{t("retry")}
				</Button>
			</AlertDescription>
		</Alert>
	);
}
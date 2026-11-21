import { Button } from "@/components/ui/button";
import { useClearAdaptiveRoutingConditionMutation } from "@/lib/store/apis/adaptiveRoutingApi";
import { getErrorMessage } from "@/lib/store/apis/baseApi";
import { RbacOperation, RbacResource, useRbac } from "@enterprise/lib";
import { Unlock } from "lucide-react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";

export function ReleaseHoldButton({ provider, keyId, model }: { provider: string; keyId: string; model?: string }) {
	const { t } = useTranslation("observability");
	const canUpdate = useRbac(RbacResource.AdaptiveRouter, RbacOperation.Update);
	const [release, { isLoading }] = useClearAdaptiveRoutingConditionMutation();
	async function clearHold() {
		try {
			await release({ provider, key_id: keyId, ...(model ? { model } : {}) }).unwrap();
			toast.success(t("adaptiveRouting.released"));
		} catch (error) {
			toast.error(getErrorMessage(error));
		}
	}
	if (!canUpdate) return null;
	return (
		<Button
			variant="outline"
			onClick={() => void clearHold()}
			disabled={isLoading}
			data-testid="adaptive-routing-release"
			title={t(model ? "adaptiveRouting.releaseModel" : "adaptiveRouting.releaseKey")}
		>
			<Unlock className="size-3.5" />
			{t("adaptiveRouting.release")}
		</Button>
	);
}
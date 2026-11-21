import PageTitle from "@/components/pageTitle";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Form, FormControl, FormDescription, FormField, FormItem, FormLabel, FormMessage } from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import {
	useGetAdaptiveRoutingConfigQuery,
	useGetAdaptiveRoutingStatusQuery,
	useUpdateAdaptiveRoutingConfigMutation,
} from "@/lib/store/apis/adaptiveRoutingApi";
import { getErrorMessage } from "@/lib/store/apis/baseApi";
import type { AdaptiveRoutingConfig } from "@/lib/types/adaptiveRouting";
import { adaptiveRoutingConfigSchema } from "@/lib/types/schemas";
import { RbacOperation, RbacResource, useRbac } from "@enterprise/lib";
import { zodResolver } from "@hookform/resolvers/zod";
import { Link } from "@tanstack/react-router";
import { useForm } from "react-hook-form";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { RoutingQueryState } from "./routingQueryState";

const toggles = [
	{
		name: "direction_selection_enabled",
		label: "providerSelection",
		help: "providerSelectionHelp",
	},
	{ name: "route_selection_enabled", label: "keySelection", help: "keySelectionHelp" },
	{ name: "append_fallbacks_to_pinned", label: "appendFallbacks", help: "appendFallbacksHelp" },
	{ name: "reroute_failed_directions", label: "redirect", help: "redirectHelp" },
	{ name: "prune_failed_fallbacks", label: "pruneFallbacks", help: "pruneFallbacksHelp" },
	{ name: "history_enabled", label: "history", help: "historyHelp" },
] as const;

export default function AdaptiveRoutingSettingsView() {
	const { t: shell } = useTranslation("shell");
	const config = useGetAdaptiveRoutingConfigQuery();
	const status = useGetAdaptiveRoutingStatusQuery();
	return (
		<div className="mx-auto w-full max-w-4xl space-y-4">
			<PageTitle
				breadcrumbs={[
					{ label: shell("nav.adaptiveRouting"), to: "/workspace/adaptive-routing" },
					{ label: shell("nav.adaptiveRoutingSettings") },
				]}
			/>
			<RoutingQueryState
				loading={config.isLoading || status.isLoading}
				error={config.error || status.error}
				retry={() => {
					void config.refetch();
					void status.refetch();
				}}
			/>
			{config.data && status.data && <AdaptiveRoutingSettingsForm config={config.data} configured={status.data.enabled} />}
		</div>
	);
}

function AdaptiveRoutingSettingsForm({ config, configured }: { config: AdaptiveRoutingConfig; configured: boolean }) {
	const { t } = useTranslation("observability");
	const { t: shell } = useTranslation("shell");
	const { t: common } = useTranslation("common");
	const canUpdate = useRbac(RbacResource.AdaptiveRouter, RbacOperation.Update);
	const [save, { isLoading: isSaving }] = useUpdateAdaptiveRoutingConfigMutation();
	const form = useForm<AdaptiveRoutingConfig>({
		resolver: zodResolver(adaptiveRoutingConfigSchema),
		defaultValues: config,
	});
	const directionEnabled = form.watch("direction_selection_enabled");
	const redirectEnabled = form.watch("reroute_failed_directions");

	async function onSubmit(values: AdaptiveRoutingConfig) {
		try {
			const saved = await save(values).unwrap();
			form.reset(saved);
			toast.success(t("adaptiveRouting.saved"));
		} catch (error) {
			form.setError("root", { message: getErrorMessage(error) });
		}
	}

	return (
		<Form {...form}>
			<form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4" data-testid="adaptive-routing-settings">
				{!configured && (
					<Alert>
						<AlertDescription>{t("adaptiveRouting.unconfiguredHelp")}</AlertDescription>
					</Alert>
				)}
				<fieldset disabled={!canUpdate || isSaving} className="space-y-4">
					{toggles.map(({ name, label, help }) => (
						<div key={name} className="rounded-sm border p-4">
							<FormField
								control={form.control}
								name={name}
								render={({ field }) => (
									<FormItem className="flex items-center justify-between gap-6">
										<div className="space-y-1">
											<FormLabel>{t(`adaptiveRouting.${label}`)}</FormLabel>
											<FormDescription>{t(`adaptiveRouting.${help}`)}</FormDescription>
										</div>
										<FormControl>
											<Switch
												checked={field.value ?? false}
												onCheckedChange={field.onChange}
												disabled={
													!canUpdate ||
													isSaving ||
													(["append_fallbacks_to_pinned", "reroute_failed_directions", "prune_failed_fallbacks"].includes(name) &&
														!directionEnabled)
												}
												data-testid={`adaptive-routing-${name}`}
											/>
										</FormControl>
									</FormItem>
								)}
							/>
							{name === "reroute_failed_directions" && (
								<FormField
									control={form.control}
									name="reroute_target"
									render={({ field }) => (
										<FormItem className="mt-4 space-y-2 border-t pt-4">
											<FormLabel>{t("adaptiveRouting.redirectTo")}</FormLabel>
											<Select
												value={field.value}
												onValueChange={field.onChange}
												disabled={!canUpdate || isSaving || !directionEnabled || !redirectEnabled}
											>
												<FormControl>
													<SelectTrigger className="w-full sm:w-80">
														<SelectValue />
													</SelectTrigger>
												</FormControl>
												<SelectContent>
													<SelectItem value="pool">{t("adaptiveRouting.pool")}</SelectItem>
													<SelectItem value="fallbacks">{t("adaptiveRouting.fallbacks")}</SelectItem>
												</SelectContent>
											</Select>
											<FormMessage />
										</FormItem>
									)}
								/>
							)}
						</div>
					))}
					{(
						[
							{
								name: "history_retention_days",
								label: "retention",
								help: "retentionHelp",
								min: 1,
								max: 30,
							},
							{
								name: "snapshot_persist_interval_seconds",
								label: "snapshot",
								help: "snapshotHelp",
								min: 5,
								max: 86400,
							},
						] as const
					).map(({ name, label, help, min, max }) => (
						<FormField
							key={name}
							control={form.control}
							name={name}
							render={({ field }) => (
								<FormItem className="rounded-sm border p-4">
									<div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-center">
										<div className="space-y-1">
											<FormLabel>{t(`adaptiveRouting.${label}`)}</FormLabel>
											<FormDescription>{t(`adaptiveRouting.${help}`)}</FormDescription>
										</div>
										<FormControl>
											<Input
												{...field}
												value={Number.isNaN(field.value) ? "" : (field.value ?? "")}
												onChange={(event) => field.onChange(event.target.valueAsNumber)}
												type="number"
												step={1}
												min={min}
												max={max}
												className="w-32 shrink-0"
												data-testid={`adaptive-routing-${name}`}
											/>
										</FormControl>
									</div>
									<FormMessage />
								</FormItem>
							)}
						/>
					))}
				</fieldset>
				{form.formState.errors.root && (
					<Alert variant="destructive">
						<AlertDescription>{form.formState.errors.root.message}</AlertDescription>
					</Alert>
				)}
				<div className="flex items-center justify-between gap-3 pb-4">
					<Button asChild variant="outline">
						<Link to="/workspace/adaptive-routing">{shell("nav.adaptiveRoutingDashboard")}</Link>
					</Button>
					<Button
						type="submit"
						disabled={!canUpdate || isSaving || (configured && !form.formState.isDirty)}
						data-testid="adaptive-routing-save"
					>
						{isSaving ? common("loading") : common("save")}
					</Button>
				</div>
			</form>
		</Form>
	);
}
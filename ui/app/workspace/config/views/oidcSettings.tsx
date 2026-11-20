import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { SecretVarInput } from "@/components/ui/secretVarInput";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import type { OIDCConfig } from "@/lib/types/config";
import { useTranslation } from "react-i18next";

const emptyConfig: OIDCConfig = { enabled: false, issuer_url: "", client_id: "", redirect_url: "" };

export default function OIDCSettings({
	value,
	disabled,
	onChange,
	errors = {},
}: {
	value?: OIDCConfig | null;
	disabled: boolean;
	onChange: (value: OIDCConfig) => void;
	errors?: Partial<Record<keyof OIDCConfig, string[]>>;
}) {
	const { t } = useTranslation("config");
	const config = value ?? emptyConfig;
	const update = (patch: Partial<OIDCConfig>) => onChange({ ...config, ...patch });
	return (
		<div className="space-y-4 rounded-sm border p-4" data-testid="oidc-settings">
			<div className="flex items-center justify-between gap-4">
				<div className="space-y-1">
					<Label htmlFor="oidc-enabled">{t("security.oidc.title")}</Label>
					<p className="text-muted-foreground text-sm">{t("security.oidc.description")}</p>
				</div>
				<Switch id="oidc-enabled" checked={config.enabled} disabled={disabled} onCheckedChange={(enabled) => update({ enabled })} />
			</div>
			{config.enabled && (
				<fieldset disabled={disabled} className="space-y-4">
					<div className="grid gap-4 sm:grid-cols-2">
						<div className="space-y-2">
							<Label htmlFor="oidc-issuer">{t("security.oidc.issuer")}</Label>
							<Input
								id="oidc-issuer"
								type="url"
								value={config.issuer_url}
								aria-invalid={!!errors.issuer_url}
								aria-describedby={errors.issuer_url ? "oidc-issuer-error" : undefined}
								onChange={(e) => update({ issuer_url: e.target.value })}
							/>
							{errors.issuer_url && (
								<p id="oidc-issuer-error" role="alert" className="text-destructive text-sm">
									{errors.issuer_url[0]}
								</p>
							)}
						</div>
						<div className="space-y-2">
							<Label htmlFor="oidc-client">{t("security.oidc.clientId")}</Label>
							<Input
								id="oidc-client"
								value={config.client_id}
								aria-invalid={!!errors.client_id}
								aria-describedby={errors.client_id ? "oidc-client-error" : undefined}
								onChange={(e) => update({ client_id: e.target.value })}
							/>
							{errors.client_id && (
								<p id="oidc-client-error" role="alert" className="text-destructive text-sm">
									{errors.client_id[0]}
								</p>
							)}
						</div>
					</div>
					<div className="space-y-2">
						<Label htmlFor="oidc-secret">{t("security.oidc.clientSecret")}</Label>
						<SecretVarInput
							id="oidc-secret"
							type="password"
							value={config.client_secret}
							disabled={disabled}
							onChange={(client_secret) => update({ client_secret })}
						/>
						<p className="text-muted-foreground text-xs">{t("security.oidc.secretHelp")}</p>
					</div>
					<div className="space-y-2">
						<Label htmlFor="oidc-redirect">{t("security.oidc.redirect")}</Label>
						<Input
							id="oidc-redirect"
							type="url"
							value={config.redirect_url}
							aria-invalid={!!errors.redirect_url}
							aria-describedby={errors.redirect_url ? "oidc-redirect-error" : undefined}
							onChange={(e) => update({ redirect_url: e.target.value })}
						/>
						{errors.redirect_url && (
							<p id="oidc-redirect-error" role="alert" className="text-destructive text-sm">
								{errors.redirect_url[0]}
							</p>
						)}
						<p className="text-muted-foreground text-xs">{t("security.oidc.redirectHelp")}</p>
					</div>
					<div className="grid gap-4 sm:grid-cols-2">
						<div className="space-y-2">
							<Label htmlFor="oidc-subjects">{t("security.oidc.subjects")}</Label>
							<Textarea
								id="oidc-subjects"
								aria-invalid={!!errors.allowed_subjects}
								aria-describedby={errors.allowed_subjects ? "oidc-subjects-error" : undefined}
								value={(config.allowed_subjects ?? []).join("\n")}
								onChange={(e) => update({ allowed_subjects: e.target.value.split("\n") })}
								onBlur={() => update({ allowed_subjects: config.allowed_subjects?.map((v) => v.trim()).filter(Boolean) })}
							/>
							{errors.allowed_subjects && (
								<p id="oidc-subjects-error" role="alert" className="text-destructive text-sm">
									{errors.allowed_subjects[0]}
								</p>
							)}
						</div>
						<div className="space-y-2">
							<Label htmlFor="oidc-emails">{t("security.oidc.emails")}</Label>
							<Textarea
								id="oidc-emails"
								aria-invalid={!!errors.allowed_emails}
								aria-describedby={errors.allowed_emails ? "oidc-emails-error" : undefined}
								value={(config.allowed_emails ?? []).join("\n")}
								onChange={(e) => update({ allowed_emails: e.target.value.split("\n") })}
								onBlur={() => update({ allowed_emails: config.allowed_emails?.map((v) => v.trim()).filter(Boolean) })}
							/>
							{errors.allowed_emails && (
								<p id="oidc-emails-error" role="alert" className="text-destructive text-sm">
									{errors.allowed_emails[0]}
								</p>
							)}
						</div>
					</div>
					<p className="text-muted-foreground text-xs">{t("security.oidc.allowHelp")}</p>
				</fieldset>
			)}
		</div>
	);
}
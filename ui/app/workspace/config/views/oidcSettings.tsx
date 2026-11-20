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
}: {
	value?: OIDCConfig | null;
	disabled: boolean;
	onChange: (value: OIDCConfig) => void;
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
							<Input id="oidc-issuer" type="url" value={config.issuer_url} onChange={(e) => update({ issuer_url: e.target.value })} />
						</div>
						<div className="space-y-2">
							<Label htmlFor="oidc-client">{t("security.oidc.clientId")}</Label>
							<Input id="oidc-client" value={config.client_id} onChange={(e) => update({ client_id: e.target.value })} />
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
						<Input id="oidc-redirect" type="url" value={config.redirect_url} onChange={(e) => update({ redirect_url: e.target.value })} />
						<p className="text-muted-foreground text-xs">{t("security.oidc.redirectHelp")}</p>
					</div>
					<div className="grid gap-4 sm:grid-cols-2">
						<div className="space-y-2">
							<Label htmlFor="oidc-subjects">{t("security.oidc.subjects")}</Label>
							<Textarea
								id="oidc-subjects"
								value={(config.allowed_subjects ?? []).join("\n")}
								onChange={(e) => update({ allowed_subjects: e.target.value.split("\n") })}
								onBlur={() => update({ allowed_subjects: config.allowed_subjects?.map((v) => v.trim()).filter(Boolean) })}
							/>
						</div>
						<div className="space-y-2">
							<Label htmlFor="oidc-emails">{t("security.oidc.emails")}</Label>
							<Textarea
								id="oidc-emails"
								value={(config.allowed_emails ?? []).join("\n")}
								onChange={(e) => update({ allowed_emails: e.target.value.split("\n") })}
								onBlur={() => update({ allowed_emails: config.allowed_emails?.map((v) => v.trim()).filter(Boolean) })}
							/>
						</div>
					</div>
					<p className="text-muted-foreground text-xs">{t("security.oidc.allowHelp")}</p>
				</fieldset>
			)}
		</div>
	);
}
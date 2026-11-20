import { describe, expect, it } from "vitest";

import { authProofOfControlSchema, oidcConfigSchema } from "./schemas";

describe("authProofOfControlSchema", () => {
	// The server verifies current_password against the stored hash byte for byte (and the
	// password policy allows spaces), so the value must reach the request exactly as typed.
	it("keeps current_password byte-exact, including surrounding whitespace", () => {
		const result = authProofOfControlSchema.safeParse({ current_password: " pass word ", setup_token: "" });
		expect(result.success).toBe(true);
		expect(result.data?.current_password).toBe(" pass word ");
	});

	it("still treats a whitespace-only current_password as missing", () => {
		const result = authProofOfControlSchema.safeParse({ current_password: "   ", setup_token: "" });
		expect(result.success).toBe(false);
		expect(result.error?.issues.map((issue) => issue.path.join("."))).toContain("current_password");
	});

	it("accepts a setup token on its own and trims it, as the server does", () => {
		const result = authProofOfControlSchema.safeParse({ current_password: "", setup_token: "  tok  " });
		expect(result.success).toBe(true);
		expect(result.data?.setup_token).toBe("tok");
	});

	it("requires one of the two", () => {
		const result = authProofOfControlSchema.safeParse({ current_password: "", setup_token: "" });
		expect(result.success).toBe(false);
	});
});

describe("oidcConfigSchema", () => {
	const config = {
		enabled: true,
		issuer_url: "https://id.example",
		client_id: "bifrost",
		redirect_url: "https://ai.example/api/session/oidc/callback",
		allowed_subjects: ["admin"],
	};
	it("accepts a configured issuer and loopback development", () => {
		expect(oidcConfigSchema.safeParse(config).success).toBe(true);
		expect(oidcConfigSchema.safeParse({ ...config, issuer_url: "http://localhost:9000" }).success).toBe(true);
	});
	it.each([
		["issuer_url", "http://id.example"],
		["issuer_url", "https://id.example?tenant=a"],
		["client_id", ""],
		["redirect_url", "https://ai.example/wrong-callback"],
		["allowed_subjects", []],
		["allowed_emails", ["invalid-email"]],
	])("reports the invalid %s field", (field, value) => {
		const result = oidcConfigSchema.safeParse({ ...config, [field]: value });
		expect(result.success).toBe(false);
		expect(result.error?.issues.map((issue) => issue.path[0])).toContain(field);
	});
	it("allows OIDC to be disabled before the form is complete", () => {
		expect(oidcConfigSchema.safeParse({ enabled: false, issuer_url: "", client_id: "", redirect_url: "" }).success).toBe(true);
	});
});
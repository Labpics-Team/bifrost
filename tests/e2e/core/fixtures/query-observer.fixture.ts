import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { extname, resolve, sep } from "node:path";
import { promisify } from "node:util";
import { test as base, expect } from "./base.fixture";

type QueryReceipt = { requestId: string; status: "fulfilled" | "rejected" };
type QueryObserver = {
  capture(sessionId: string, endpointName?: string): Promise<string>;
  settled(requestId: string): Promise<QueryReceipt>;
};

declare global {
  interface Window {
    __bifrostQueryObserver: {
      capture(sessionId: string, endpointName?: string): string;
      settled(requestId: string): Promise<QueryReceipt>;
    };
  }
}

const contentTypes: Record<string, string> = {
  ".html": "text/html", ".js": "application/javascript", ".css": "text/css",
  ".json": "application/json", ".svg": "image/svg+xml", ".woff2": "font/woff2",
};

// Only tests requesting queryObserver use this build; ordinary tests and
// production builds have no observer entry or window API.
export const test = base.extend<{ queryObserver: QueryObserver }, { queryObservedBuild: string }>({
  queryObservedBuild: [async ({}, use) => {
    const outDir = await mkdtemp(resolve(tmpdir(), "bifrost-query-observer-"));
    const uiRoot = resolve(process.env.E2E_QUERY_UI_ROOT || resolve(__dirname, "../../../../ui"));
    try {
      await promisify(execFile)(process.execPath, [resolve(__dirname, "query-observer.build.mjs"), uiRoot, outDir], {
        cwd: uiRoot, timeout: 60000, maxBuffer: 1024 * 1024,
      });
      await use(outDir);
    } finally {
      await rm(outDir, { recursive: true, force: true });
    }
  }, { scope: "worker", timeout: 60000 }],
  queryObserver: async ({ page, queryObservedBuild }, use, testInfo) => {
    await page.route((url) => !url.pathname.startsWith("/api/"), async (route) => {
      const name = route.request().isNavigationRequest() ? "index.html" : new URL(route.request().url()).pathname.slice(1);
      const file = resolve(queryObservedBuild, name);
      if (!file.startsWith(queryObservedBuild + sep)) return route.fallback();
      let body: Buffer;
      try { body = await readFile(file); } catch { return route.fallback(); }
      await route.fulfill({ body, contentType: contentTypes[extname(file)] || "application/octet-stream" });
    });
    await use({
      capture: (sessionId, endpointName) => page.evaluate(
        ({ sessionId, endpointName }) => window.__bifrostQueryObserver.capture(sessionId, endpointName),
        { sessionId, endpointName },
      ),
      settled: async (requestId) => {
        const receipt = await page.evaluate((id) => window.__bifrostQueryObserver.settled(id), requestId);
        await testInfo.attach("processed-query-" + requestId, { body: JSON.stringify(receipt), contentType: "application/json" });
        return receipt;
      },
    });
  },
});

export { expect };

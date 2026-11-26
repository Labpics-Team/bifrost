import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const [uiRoot, outDir] = process.argv.slice(2);
if (!uiRoot || !outDir) throw new Error("Expected UI source root and output directory");
const requireUI = createRequire(resolve(uiRoot, "package.json"));
const { build } = await import(pathToFileURL(requireUI.resolve("vite")).href);
const observer = await readFile(new URL("./query-observer.browser.mjs", import.meta.url), "utf8");
const entry = "/__e2e-query-observer.js";
const virtualEntry = "\0bifrost-e2e-query-observer";
await build({
  root: uiRoot,
  configFile: resolve(uiRoot, "vite.config.mts"),
  logLevel: "error",
  build: { outDir, emptyOutDir: true, copyPublicDir: false },
  plugins: [{
    name: "e2e-query-observer",
    // Both entries participate in ONE build graph. Rebundling the observer
    // separately would instantiate another store and invalidate the oracle.
    transformIndexHtml: {
      order: "pre",
      handler: () => [{ tag: "script", attrs: { type: "module", src: entry }, injectTo: "head" }],
    },
    resolveId: (id) => id === entry ? virtualEntry : undefined,
    load: (id) => id === virtualEntry ? observer : undefined,
  }],
});

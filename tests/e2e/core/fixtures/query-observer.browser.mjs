// Included only by query-observer.build.mjs, never by the production entry.
// These imports resolve to the same modules used by the application's Provider.
import { flushSync } from "react-dom";
import { store } from "@/lib/store/store";
import { logsApi } from "@/lib/store/apis/logsApi";

const captured = new Map();

window.__bifrostQueryObserver = Object.freeze({
  capture(sessionId, endpointName = "getLogSessionById") {
    const matches = Object.entries(store.getState()[logsApi.reducerPath].queries).filter(([, query]) =>
      query?.status === "pending" && query.endpointName === endpointName &&
      (typeof query.originalArgs === "string" ? query.originalArgs : query.originalArgs?.sessionId) === sessionId
    );
    if (matches.length !== 1) throw new Error("Expected one pending query in the application's store");
    const [cacheKey, query] = matches[0];
    // Observes an existing request; it must not initiate or substitute a request.
    const running = store.dispatch(logsApi.util.getRunningQueryThunk(endpointName, query.originalArgs));
    if (!running || running.requestId !== query.requestId) throw new Error("Pending query has no matching running request");
    const { requestId } = running;
    const notified = new Promise((resolve) => {
      const unsubscribe = store.subscribe(() => {
        const current = store.getState()[logsApi.reducerPath].queries[cacheKey];
        if (current?.requestId === requestId && current.status === "pending") return;
        unsubscribe();
        resolve();
      });
    });
    captured.set(requestId, Promise.all([running, notified]).then(([result]) => {
      if (result.status !== "fulfilled" && result.status !== "rejected") throw new Error("Query did not reach a terminal state");
      // The public store notification has reached React subscribers. Flush their
      // pending work before asserting the DOM; no action or UI state is injected.
      flushSync(() => {});
      return { requestId, status: result.status };
    }));
    return requestId;
  },
  async settled(requestId) {
    const pending = captured.get(requestId);
    if (!pending) throw new Error("Query was not captured while pending");
    const receipt = await pending;
    captured.delete(requestId);
    return receipt;
  },
});

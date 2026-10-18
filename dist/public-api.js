import { createRequire } from "node:module";
var __require = /* @__PURE__ */ createRequire(import.meta.url);

// src/public-api.ts
var key = Symbol.for("omo.agent-coordination.api.v1");
function getCoordinationBroker(host = globalThis) {
  const globals = host;
  const ready = globals[Symbol.for("omo.agent-coordination.api-ready.v1")];
  const broker = globals[key] ?? (typeof ready === "function" ? ready() : undefined);
  if (!broker || typeof broker !== "object")
    return { ok: false, reason: "coordination_unavailable" };
  if (broker.version !== 1)
    return { ok: false, reason: "api_version_unsupported" };
  if (!Array.isArray(broker.features) || typeof broker.getClient !== "function") {
    return { ok: false, reason: "coordination_unavailable" };
  }
  return { ok: true, value: broker };
}
function getCoordinationClient(ctx, host = globalThis) {
  const broker = getCoordinationBroker(host);
  return broker.ok ? broker.value.getClient(ctx) : broker;
}
export {
  getCoordinationBroker,
  getCoordinationClient
};

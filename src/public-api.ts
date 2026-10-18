import type { CoordinationBroker, CoordinationClient, Result } from "./contracts.ts";
export type {
  AskInput, AskPolicyInput, AskResult, CoordinationBroker, CoordinationClient, Feature,
  InspectProjection, NotifyInput, Peer, RequestReceipt, Result, Roster, TargetRef,
} from "./contracts.ts";

const key = Symbol.for("omo.agent-coordination.api.v1");

/** Pure discovery: importing this helper never imports the extension, SDK, or capture owner. */
export function getCoordinationBroker(
  host: object = globalThis,
): Result<CoordinationBroker> {
  const globals = host as Record<symbol, unknown>;
  // Discovery may publish the already-captured owner's lazy broker. Importing this
  // module does nothing; this callback never creates a capture or calls a model.
  const ready = globals[Symbol.for("omo.agent-coordination.api-ready.v1")];
  const broker = (globals[key] ?? (typeof ready === "function" ? ready() : undefined)) as CoordinationBroker | undefined;
  if (!broker || typeof broker !== "object") return { ok: false, reason: "coordination_unavailable" };
  if (broker.version !== 1) return { ok: false, reason: "api_version_unsupported" };
  if (!Array.isArray(broker.features) || typeof broker.getClient !== "function") {
    return { ok: false, reason: "coordination_unavailable" };
  }
  return { ok: true, value: broker };
}

export function getCoordinationClient(
  ctx: unknown, host: object = globalThis,
): Result<CoordinationClient> {
  const broker = getCoordinationBroker(host);
  return broker.ok ? broker.value.getClient(ctx) : broker;
}

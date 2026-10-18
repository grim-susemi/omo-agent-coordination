import { expect, test } from "bun:test";
import { API_PORTS } from "../src/contracts.ts";
import { getCoordinationBroker, getCoordinationClient } from "../src/public-api.ts";
import { runChannelCase } from "../qa/cases/channel-native.ts";

test("the pure helper refuses missing and incompatible brokers without loading a runtime", () => {
  const key = Symbol.for("omo.agent-coordination.api.v1");
  const host: Record<symbol, unknown> = {};
  expect(getCoordinationBroker(host)).toEqual({ ok: false, reason: "coordination_unavailable" });
  expect(getCoordinationClient({}, host)).toEqual({ ok: false, reason: "coordination_unavailable" });
  host[key] = { version: 2, features: [], getClient() { throw new Error("must not call"); } };
  expect(getCoordinationBroker(host)).toEqual({ ok: false, reason: "api_version_unsupported" });
  host[key] = { version: 1, features: [], getClient: null };
  expect(getCoordinationBroker(host)).toEqual({ ok: false, reason: "coordination_unavailable" });
});

test("the built product API export imports without a second capture or provider request", async () => {
  const global = globalThis as any;
  const captureKey = Symbol.for("omo.agent-coordination.capture.v1");
  const nativeKey = Symbol.for("omo.agent-coordination.native.v1");
  const providerKey = Symbol.for("coordination.channel.qa.provider-count");
  const owner = global[captureKey], native = global[nativeKey];
  const captures = owner?.capture.list().length ?? 0;
  const providers = global[providerKey] ?? 0;
  const specifier = "piomo-agent-coordination-mod/api";
  const resolved = import.meta.resolve(specifier);
  expect(resolved).toBe(new URL("../dist/public-api.js", import.meta.url).href);
  const helper = await import(specifier);
  expect(Object.keys(helper).sort()).toEqual(["getCoordinationBroker", "getCoordinationClient"]);
  expect(helper.getCoordinationBroker({})).toEqual({ ok: false, reason: "coordination_unavailable" });
  expect(global[captureKey]).toBe(owner);
  expect(global[nativeKey]).toBe(native);
  expect(owner?.capture.list().length ?? 0).toBe(captures);
  expect(global[providerKey] ?? 0).toBe(providers);
  console.log(JSON.stringify({ product_api_import: {
    specifier, resolved, capture_delta: 0, provider_delta: 0, native_owner_unchanged: true,
  } }));
});

test("an actual loader consumer gets only the seven scoped ports, with unwired features absent", async () => {
  const result = await runChannelCase("channel-api");
  expect(result.passed).toBe(true);
  expect(result.api).toMatchObject({
    broker_keys: ["features", "getClient", "version"],
    client_ports: [...API_PORTS].sort(), version: 1, features: [],
    features_frozen: true, pure_import_capture_delta: 0, pure_import_provider_delta: 0,
    pure_import_hook_unchanged: true,
    foreign_context_refused: true, one_shot_refused: true, stale_client_refused: true,
    unwired_ports_refused: true, roster_changes_only: true,
    dispose_preserves_other_client: true, foreign_broker_preserved: true,
    built_api_imported: true,
  });
}, 120_000);

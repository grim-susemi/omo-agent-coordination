import type { EngineBridge, OwnedSnapshot } from "./engine-bridge.ts";
import type { NativeReplayCarrier, SettlementRecord } from "./task-settlement.ts";
import type { TargetRef } from "./contracts.ts";

const active = new WeakSet<EngineBridge>();
export const repairActive = (bridge: EngineBridge) => active.has(bridge);

function proven(row: OwnedSnapshot) {
  const witness = row.outcome, identity = row.identity;
  return row.status === "running" && row.admission !== "failed" &&
    !["persisted_only", "rpc_detached"].includes(row.residency) &&
    witness?.status === "completed" && witness.result_sha256 !== null &&
    witness.task_id === identity.task_id && witness.run_epoch === identity.run_epoch &&
    witness.handle_id === identity.handle_id && witness.generation === identity.generation &&
    witness.owner_session_id === identity.owner_session_id && witness.owner_root_id === identity.owner_root_id;
}

/** Events select candidates; only the bridge's fresh same-owner/epoch outcome and
 * locked native replay fence authorize mutation. No timer, provider or cancel path.
 * Native admission failures keep their original admission/rollback path: a row's
 * age or lack of a handle is never evidence to manufacture a terminal outcome.
 */
export function createAutoReconcile(options: { bridge: EngineBridge; enabled: boolean; attempts?: Set<string> }) {
  const { bridge } = options;
  const subscriptions = new Map<TargetRef, () => void>();
  const attempted = options.attempts ?? new Set<string>();
  const key = (row: OwnedSnapshot) => JSON.stringify([row.identity.owner_session_id, row.identity.owner_root_id,
    row.identity.task_id, row.identity.run_epoch, row.identity.generation]);
  let disposed = false;
  if (options.enabled) active.add(bridge);
  const reconcile = (ref: TargetRef) => {
    if (disposed || !options.enabled) return;
    const current = bridge.inspectOwned(ref);
    if (!current.ok || !proven(current.value) || attempted.has(key(current.value))) return;
    // Mark before native replay: its store mutation can re-enter an event handler.
    // A degraded publication remains visible, not an automatic retry loop.
    const identity = key(current.value);
    attempted.add(identity);
    const result = bridge.reconcileOwnedTerminal(ref);
    if (!result.ok) attempted.delete(identity);
  };
  const observe = (allowRepair = true) => {
    if (disposed || !options.enabled) return;
    const listed = bridge.listOwned();
    if (!listed.ok) return;
    for (const row of listed.value) {
      const ref = row.identity.target_ref;
      if (proven(row)) { if (allowRepair) reconcile(ref); continue; }
      if (row.status !== "running" || row.admission === "failed" ||
          ["persisted_only", "rpc_detached"].includes(row.residency) ||
          !row.identity.handle_id || subscriptions.has(ref) || attempted.has(key(row))) continue;
      subscriptions.set(ref, () => {});
      const subscribed = bridge.subscribeOutcome(ref, () => {
        subscriptions.get(ref)?.(); subscriptions.delete(ref);
        reconcile(ref);
      });
      if (subscribed.ok) subscriptions.set(ref, subscribed.value);
      else subscriptions.delete(ref);
    }
  };
  return {
    observe,
    dispose() {
      if (disposed) return;
      disposed = true; active.delete(bridge);
      for (const stop of subscriptions.values()) stop();
      subscriptions.clear();
      if (!options.attempts) attempted.clear();
    },
  };
}

type Store = Parameters<typeof attachAutoReconcile>[0]["store"];
type Observer = NonNullable<NativeReplayCarrier["terminalObservers"]> extends WeakMap<Store, infer T> ? T : never;
const subscriptions = new WeakMap<Store, {
  handlers: Set<ReturnType<typeof createAutoReconcile>>;
  attempts: Set<string>;
  observer: Observer;
  previous: Observer;
  stop(): void;
  schedule(): void;
}>();

/** The adapter observer runs before the native store write, while its original
 * handle still exists. Retain the existing settlement observer and failure path.
 * Store events are coalesced after native cleanup so replay never runs inside a store lock.
 */
export function attachAutoReconcile(options: {
  bridge: EngineBridge;
  carrier: NativeReplayCarrier;
  store: { load(id: string): SettlementRecord | null };
  onMutation(listener: () => void): () => void;
}) {
  const { carrier, store } = options;
  let shared = subscriptions.get(store);
  if (!shared) {
    const previous = carrier.terminalObservers?.get(store);
    if (!previous || !carrier.replays?.has(store)) return undefined;
    const handlers = new Set<ReturnType<typeof createAutoReconcile>>();
    let queued = false;
    const schedule = () => {
      if (queued || !handlers.size) return;
      queued = true;
      setImmediate(() => {
        queued = false;
        for (const handler of handlers) handler.observe();
      });
    };
    const observer: Observer = (...args) => {
      const receipt = previous(...args);
      for (const handler of handlers) handler.observe(false);
      schedule();
      return { failed(error) { receipt.failed(error); schedule(); } };
    };
    carrier.terminalObservers!.set(store, observer);
    shared = { handlers, observer, previous, attempts: new Set(), schedule, stop: options.onMutation(schedule) };
    subscriptions.set(store, shared);
  }
  const registry = shared;
  const auto = createAutoReconcile({ bridge: options.bridge, enabled: true, attempts: registry.attempts });
  registry.handlers.add(auto);
  let disposed = false;
  auto.observe(); // Actual owner attachment/rebinding, not a periodic scan.
  return {
    dispose() {
      if (disposed) return;
      disposed = true; auto.dispose(); registry.handlers.delete(auto);
      if (registry.handlers.size) return;
      registry.stop(); registry.attempts.clear(); subscriptions.delete(store);
      if (carrier.terminalObservers?.get(store) === registry.observer) carrier.terminalObservers.set(store, registry.previous);
    },
  };
}

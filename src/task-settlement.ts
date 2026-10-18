import { createHash } from "node:crypto";

const FENCE = Symbol.for("omo.agent-coordination.terminal-fence.v1");
const hash = (text: string) => createHash("sha256").update(text).digest("hex");
export interface SettlementRecord {
  task_id: string;
  parent_session_id: string;
  root_session_id: string;
  child_session_id?: string;
  host_pid?: number;
  residency_claim?: unknown;
  residency_state: string;
  status: string;
  final_response?: string;
  notification: { run_epoch: number; notified_epoch: number };
}
interface Transition {
  type: string;
  timestamp: string;
  final_response?: string;
  [FENCE]?: (record: SettlementRecord) => boolean;
}
interface Handle { waitForOutcome(): Promise<unknown> }
interface Store { load(id: string): SettlementRecord | null }
export interface NativeReplayCarrier {
  terminalObservers?: WeakMap<Store, (id: string, record: SettlementRecord, transition: Transition) =>
    { failed(error: unknown): void }>;
  replays?: WeakMap<Store, (id: string, record: SettlementRecord, timestamp: string, transition: Transition) => void>;
}
export interface SettlementReceipt {
  readonly task_id: string;
  readonly run_epoch: number;
  readonly state: "settled" | "degraded";
  readonly result_sha256: string;
  readonly result_preserved: boolean;
  readonly publication: "published" | "unpublished";
  readonly notification: "accepted" | "unaccepted";
  readonly waiter: "native_canonical" | "native_storage_failure";
  readonly slot: "released" | "held" | "parked";
  readonly replayed: boolean;
  readonly error?: { readonly phase: "publication" | "notification" | "slot"; readonly code: string };
}
interface Attempt {
  owned: SettlementRecord;
  transition: Transition;
  handle: WeakRef<Handle>;
  error?: unknown;
}
interface Registry {
  users: number;
  attempts: Map<string, Attempt>;
  observer: NonNullable<NativeReplayCarrier["terminalObservers"]> extends WeakMap<Store, infer T> ? T : never;
}
const registries = new WeakMap<Store, Registry>();

/** Retain only the transition selected by this store's original native outcome
 * tracker. Replays call its original persistTerminal closure, including its
 * notification store chain, waiter settlement, and storage-failure cleanup.
 * This module neither constructs a transition nor writes a task record.
 */
export function attachTaskSettlement(options: {
  carrier: NativeReplayCarrier;
  store: Store;
  liveHandle(id: string): Handle | undefined;
}) {
  const { carrier, store } = options;
  if (!carrier.terminalObservers || !carrier.replays?.has(store)) return undefined;
  let registry = registries.get(store);
  if (!registry) {
    const attempts = new Map<string, Attempt>();
    const observer: Registry["observer"] = (id, owned, transition) => {
      let attempt = attempts.get(id);
      if (!transition[FENCE]) {
        const handle = options.liveHandle(id);
        if (!handle) return { failed() {} };
        attempt = { owned, transition, handle: new WeakRef(handle) };
        attempts.set(id, attempt);
      }
      const current = attempt;
      return { failed(error) { if (current) current.error = error; } };
    };
    registry = { users: 0, attempts, observer };
    carrier.terminalObservers.set(store, observer);
    registries.set(store, registry);
  }
  const shared = registry;
  shared.users++;
  let disposed = false;
  return {
    reconcile(input: {
      task_id: string; run_epoch: number; handle: Handle;
      result_sha256: string; guard(record: SettlementRecord): boolean;
      slot(): "released" | "held" | "parked";
    }): SettlementReceipt | undefined {
      if (disposed) return undefined;
      const attempt = shared.attempts.get(input.task_id);
      if (!attempt || attempt.owned.notification.run_epoch !== input.run_epoch ||
          attempt.handle.deref() !== input.handle || attempt.transition.type !== "complete" ||
          !attempt.transition.final_response || hash(attempt.transition.final_response) !== input.result_sha256) {
        return undefined;
      }
      let fresh = store.load(input.task_id);
      if (!fresh || !input.guard(fresh)) return undefined;
      let replayed = false;
      if (fresh.status === "running") {
        const replay = carrier.replays!.get(store);
        if (!replay) return undefined;
        replayed = true;
        replay(input.task_id, attempt.owned, attempt.transition.timestamp,
          { ...attempt.transition, [FENCE]: input.guard });
        fresh = store.load(input.task_id);
        if (!fresh || !input.guard(fresh)) return undefined;
      }
      const preserved = fresh.status === "completed" && fresh.final_response !== undefined &&
        hash(fresh.final_response) === input.result_sha256;
      const notified = fresh.notification.notified_epoch >= input.run_epoch;
      const slot = input.slot();
      const code = attempt.error instanceof Error && "code" in attempt.error
        ? String(attempt.error.code) : "storage_failure";
      return Object.freeze({
        task_id: input.task_id, run_epoch: input.run_epoch,
        state: preserved && notified && slot === "released" ? "settled" : "degraded",
        result_sha256: input.result_sha256, result_preserved: true,
        publication: preserved ? "published" : "unpublished",
        notification: notified ? "accepted" : "unaccepted",
        waiter: preserved ? "native_canonical" : "native_storage_failure",
        slot, replayed,
        ...(!preserved ? { error: { phase: "publication" as const, code } } :
          !notified ? { error: { phase: "notification" as const, code: "not_accepted" } } :
          slot !== "released" ? { error: { phase: "slot" as const, code: "not_released" } } : {}),
      });
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      if (--shared.users === 0) {
        if (carrier.terminalObservers?.get(store) === shared.observer) carrier.terminalObservers.delete(store);
        shared.attempts.clear();
        registries.delete(store);
      }
    },
  };
}

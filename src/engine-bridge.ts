import { createHash, randomUUID } from "node:crypto";
import type { DenialReason, NativeState, TargetRef } from "./contracts.ts";
import { attachTaskSettlement } from "./task-settlement.ts";
import { attachAutoReconcile } from "./auto-reconcile.ts";
import type { NativeReplayCarrier, SettlementReceipt, SettlementRecord } from "./task-settlement.ts";

const CARRIER = Symbol.for("omo.agent-coordination.native.v1");
const BARREL = Symbol.for("omo.senpi-task.senpiBarrel");
const BASELINES = new Set([
  "36d98c7ab3cc07349b60adc8bc7365a85e7573653ebab574d0114c2571d53dcd",
  "d41bc807b6f91768db84b7f8f76092107aebf9934888cc749174d88a1c00ac6b",
]);
const normalize = (id: string) => id.replace(/^senpi:/, "");
type RefusalReason = "unsupported_runtime" | "connection_failed" | "scope_denied" |
  "invalid_target" | "stale_epoch" | "stale_generation" | "runtime_mismatch" | "repair_not_enabled";
export type BridgeResult<T> = { readonly ok: true; readonly value: T } |
  { readonly ok: false; readonly reason: RefusalReason; readonly connection?: string };

interface SessionManager { getSessionId(): string }
interface NativeRecord {
  task_id: string;
  parent_session_id: string;
  root_session_id: string;
  child_session_id?: string;
  status: NativeState;
  residency_state: string;
  model: string;
  agent_type?: string;
  name?: string;
  created_at: string;
  started_at?: string;
  terminal_at?: string;
  failure_kind?: string;
  pid?: number;
  host_session?: object;
  tool_allow?: readonly string[];
  tool_deny?: readonly string[];
  updated_at: string;
  notification: { run_epoch: number; notified_epoch: number };
}
interface NativeOutcome {
  status: "completed" | "error" | "cancelled" | "interrupted";
  finalResponse?: string;
}
interface NativeHandle {
  readonly kind: "in-process" | "rpc";
  readonly sessionId?: string;
  waitForOutcome(): Promise<NativeOutcome>;
}
interface NativeStore {
  load(id: string): NativeRecord | null;
  list(): { records: readonly NativeRecord[];
    diagnostics: readonly { type: string; path: string }[] };
}
interface OutcomePorts {
  store: NativeStore;
  liveHandle(id: string): NativeHandle | undefined;
  tryLoad(id: string): NativeRecord | null;
  releaseSlot(id: string, model: string, epoch: number): void;
  settleWaiters(id: string, terminal?: NativeRecord): void;
  forget(id: string, options: { path: "park" }): void;
}
interface NativeEngine {
  onStoreMutation?(listener: () => void): () => void;
  manager: {
    list(scope: { scope: "all" }): readonly { record: NativeRecord }[];
    getResidentHandle(id: string): NativeHandle | undefined;
    concurrency: { leaseState(id: string, epoch: number): "held" | "parked" | undefined };
  };
  runtime: { sessionId(): string | undefined };
  ancestry?: { rootSessionId?: string };
  lifecycle: object;
  notifier: object;
}
export interface ActualRuntime {
  readonly sessionId: string;
}
interface SessionPrototype {
  bindExtensions(this: ActualRuntime, ...args: unknown[]): unknown;
}
interface BirthHook {
  original: SessionPrototype["bindExtensions"];
  wrapped: SessionPrototype["bindExtensions"];
  users: Set<symbol>;
}
const birthHooks = new WeakMap<SessionPrototype, BirthHook>();
export interface SenpiNamespace {
  AgentSession: { new (...args: never[]): ActualRuntime; prototype: SessionPrototype };
}
interface Carrier extends NativeReplayCarrier {
  version: number;
  baseline_sha256: string;
  engines: Set<WeakRef<NativeEngine>>;
  links: WeakMap<NativeEngine, { store: NativeStore; active: boolean }>;
  ports: WeakMap<NativeStore, OutcomePorts>;
  contexts: WeakMap<SessionManager, WeakRef<NativeEngine>>;
  generations: WeakMap<SessionManager, number>;
  runtimeOwners: WeakMap<NativeHandle, { task_id: string; runtime: WeakRef<ActualRuntime> }>;
  module?: SenpiNamespace;
  moduleListeners: Set<(module: SenpiNamespace) => void>;
}
export interface OwnedIdentity {
  readonly target_ref: TargetRef;
  readonly task_id: string;
  readonly run_epoch: number;
  readonly owner_session_id: string;
  readonly owner_root_id: string;
  readonly canonical_session_id: string | null;
  readonly session_id: string | null;
  readonly handle_id: string | null;
  readonly runtime_instance_id: string | null;
  readonly generation: number;
}
export interface OutcomeWitness {
  readonly owner_session_id: string;
  readonly owner_root_id: string;
  readonly generation: number;
  readonly task_id: string;
  readonly run_epoch: number;
  readonly handle_id: string;
  readonly status: NativeOutcome["status"];
  readonly result_sha256: string | null;
  readonly source: "native ManagedChildHandle.waitForOutcome";
  readonly observed_at: string;
}
export interface OwnedSnapshot {
  readonly identity: OwnedIdentity;
  readonly status: NativeState;
  readonly residency: string;
  readonly role: string;
  readonly name: string;
  readonly last_transition_at: string | null;
  readonly admission: "queued" | "admitted" | "failed" | "unknown";
  readonly collaboration: { readonly allowed: boolean; readonly denial_reason?: DenialReason };
  readonly notification: { readonly run_epoch: number; readonly notified_epoch: number };
  readonly slot: "held" | "parked" | "not_acquired" | "released";
  readonly outcome: OutcomeWitness | null;
  readonly source: "native owner engine/store/outcome ports";
  readonly observed_at: string;
}
export interface EnumerationIssue {
  readonly identity?: OwnedIdentity;
  readonly reason: RefusalReason | "record_unreadable" | "enumeration_failed";
}
export interface OwnedEnumeration {
  readonly owned: number;
  readonly unreadable: number;
  readonly coverage: "complete" | "partial" | "unknown";
  readonly issues: readonly EnumerationIssue[];
}
export interface EngineBridge {
  listOwned(): BridgeResult<readonly OwnedSnapshot[]> & { readonly enumeration?: OwnedEnumeration };
  inspectOwned(target: TargetRef): BridgeResult<OwnedSnapshot>;
  bindActualRuntime(target: TargetRef, runtime: ActualRuntime): BridgeResult<OwnedIdentity>;
  subscribeOutcome(target: TargetRef, listener: (witness: OutcomeWitness) => void): BridgeResult<() => void>;
  reconcileOwnedTerminal(target: TargetRef): BridgeResult<SettlementReceipt>;
  dispose(): void;
}

/** Connect only through a native-captured SessionManager capability, never an ID supplied as authority.
 * No SDK is imported here. The loader and lazy barrel namespaces are passed through the same hook.
 */
export function connectEngineBridge(options: {
  context: { sessionManager: SessionManager };
  generation: number;
  loaderNamespace: SenpiNamespace;
  onModuleReady?: (namespace: SenpiNamespace) => void;
  enabled?: boolean;
}): BridgeResult<EngineBridge> {
  const root = globalThis as unknown as Record<symbol, unknown>;
  const carrier = root[CARRIER] as Carrier | undefined;
  if (options.enabled === false || process.env.OMO_AGENT_COORDINATION === "0") {
    return { ok: false, reason: "unsupported_runtime", connection: "mode_disabled" };
  }
  if (!carrier || carrier.version !== 2 || !BASELINES.has(carrier.baseline_sha256)) {
    return { ok: false, reason: "unsupported_runtime", connection: "native adapter carrier" };
  }
  const manager = options.context.sessionManager;
  const engineRef = carrier.contexts.get(manager);
  const engine = engineRef?.deref();
  if (!engineRef || !engine) return { ok: false, reason: "scope_denied", connection: "native session_start binding" };
  const link = carrier.links.get(engine);
  const ports = link && carrier.ports.get(link.store);
  if (!link?.active || !ports || ports.store !== link.store ||
      typeof ports.releaseSlot !== "function" || typeof ports.settleWaiters !== "function" ||
      !engine.lifecycle || !engine.notifier) {
    return { ok: false, reason: "connection_failed", connection: "same-engine store/lifecycle/outcome/notification" };
  }
  const owner = engine.runtime.sessionId();
  const ownerRoot = engine.ancestry?.rootSessionId ?? owner;
  if (!owner || !ownerRoot || normalize(manager.getSessionId()) !== normalize(owner)) {
    return { ok: false, reason: "scope_denied", connection: "live owner session" };
  }
  const previousGeneration = carrier.generations.get(manager);
  if (previousGeneration !== undefined && options.generation < previousGeneration) {
    return { ok: false, reason: "stale_generation" };
  }
  const hooks = new Map<SessionPrototype, BirthHook>();
  const hookUser = Symbol();
  let readinessError: string | undefined;
  let disposed = false;
  const ready = (namespace: SenpiNamespace) => {
    try {
      const AgentSession = namespace?.AgentSession;
      const proto = AgentSession?.prototype;
      if (!proto || typeof proto.bindExtensions !== "function") throw new Error("AgentSession.bindExtensions");
      if (hooks.has(proto)) return;
      let hook = birthHooks.get(proto);
      if (!hook) {
        const original = proto.bindExtensions;
        const users = new Set<symbol>();
        const wrapped = async function (this: ActualRuntime, ...args: unknown[]) {
          return await Reflect.apply(original, this, args);
        };
        hook = { original, wrapped, users };
        proto.bindExtensions = wrapped;
        birthHooks.set(proto, hook);
      }
      hook.users.add(hookUser);
      hooks.set(proto, hook);
      options.onModuleReady?.(namespace);
    } catch (error) { readinessError = String(error); }
  };
  carrier.moduleListeners.add(ready);
  ready(options.loaderNamespace);
  const barrel = root[BARREL] as { module?: SenpiNamespace } | undefined;
  if (barrel?.module) ready(barrel.module);
  if (carrier.module) ready(carrier.module);

  type Binding = {
    identity: OwnedIdentity; handle?: WeakRef<NativeHandle>; runtime?: WeakRef<ActualRuntime>;
    witness?: OutcomeWitness;
  };
  const bindings = new Map<TargetRef, Binding>();
  const stops = new Set<() => void>();
  const settlement = process.env.OMO_AGENT_COORDINATION_REPAIR === "0" ? undefined :
    attachTaskSettlement({ carrier, store: link.store, liveHandle: ports.liveHandle });
  let automatic: ReturnType<typeof attachAutoReconcile>;
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    carrier.moduleListeners.delete(ready);
    for (const [proto, hook] of hooks) {
      hook.users.delete(hookUser);
      if (hook.users.size === 0 && proto.bindExtensions === hook.wrapped) {
        proto.bindExtensions = hook.original;
        birthHooks.delete(proto);
      }
    }
    hooks.clear();
    automatic?.dispose();
    settlement?.dispose();
    bindings.clear();
    for (const stop of [...stops]) stop();
  };
  if (readinessError) {
    dispose();
    return { ok: false, reason: "connection_failed", connection: readinessError };
  }
  carrier.generations.set(manager, options.generation);

  // These closures retain weak engine/handle references only; owner objects stay scoped to this client.
  const current = (): BridgeResult<NativeEngine> => {
    if (disposed || carrier.generations.get(manager) !== options.generation) {
      return { ok: false, reason: "stale_generation" };
    }
    if (readinessError) return { ok: false, reason: "connection_failed", connection: readinessError };
    const live = engineRef.deref();
    if (!live || !carrier.links.get(live)?.active ||
        normalize(live.runtime.sessionId() ?? "") !== normalize(owner) ||
        normalize(manager.getSessionId()) !== normalize(owner) ||
        normalize(live.ancestry?.rootSessionId ?? live.runtime.sessionId() ?? "") !== normalize(ownerRoot)) {
      return { ok: false, reason: "scope_denied" };
    }
    return { ok: true, value: live };
  };
  const owned = (target: TargetRef, terminalHandle?: NativeHandle): BridgeResult<{
    binding: Binding; record: NativeRecord; engine: NativeEngine;
  }> => {
    const active = current();
    if (!active.ok) return active;
    const binding = bindings.get(target);
    if (!binding) return { ok: false, reason: "invalid_target" };
    const store = carrier.links.get(active.value)!.store;
    let fresh: NativeRecord | null;
    try { fresh = store.load(binding.identity.task_id); }
    catch { return { ok: false, reason: "connection_failed", connection: "native record read" }; }
    if (!fresh || normalize(fresh.root_session_id) !== normalize(ownerRoot)) {
      return { ok: false, reason: "scope_denied" };
    }
    if (fresh.notification.run_epoch !== binding.identity.run_epoch) {
      return { ok: false, reason: "stale_epoch" };
    }
    const handle = binding.handle?.deref();
    const runtime = binding.runtime?.deref();
    const resident = active.value.manager.getResidentHandle(fresh.task_id);
    const live = carrier.ports.get(store)?.liveHandle(fresh.task_id);
    // The original native outcome promise can resolve after publication failure
    // has released residency. Its exact bound handle remains the provenance of
    // a historical fact, not authority to bind a replacement live runtime.
    const released = binding.handle !== undefined && resident === undefined && live === undefined &&
      (binding.witness !== undefined || (terminalHandle !== undefined && terminalHandle === handle));
    // Session IDs corroborate the exact native handle/runtime; either metadata
    // field may arrive late. Learn one value without making absence a conflict,
    // and never let a later ID replace a value learned by this binding.
    const canonicalId = fresh.child_session_id;
    const handleId = handle?.sessionId;
    const sessionIds = [binding.identity.session_id,
      canonicalId === undefined ? null : normalize(canonicalId),
      handleId === undefined ? null : normalize(handleId),
      runtime ? normalize(runtime.sessionId) : null];
    const sessionId = sessionIds.find((id) => id !== null) ?? null;
    if ((binding.handle !== undefined && handle === undefined && !released) ||
        (!released && (resident !== handle || live !== handle)) ||
        sessionIds.some((id) => id !== null && id !== sessionId) ||
        (handle && handleId === undefined && handle.kind !== "rpc") ||
        (!released && binding.runtime !== undefined && (!runtime || !handle ||
          carrier.runtimeOwners.get(handle)?.runtime.deref() !== runtime))) {
      return { ok: false, reason: "runtime_mismatch" };
    }
    if (binding.identity.session_id !== sessionId ||
        (binding.identity.canonical_session_id === null && canonicalId !== undefined)) {
      binding.identity = { ...binding.identity, session_id: sessionId,
        canonical_session_id: binding.identity.canonical_session_id ?? canonicalId ?? null };
    }
    return { ok: true, value: { binding, record: fresh, engine: active.value } };
  };
  const snapshot = (binding: Binding, record: NativeRecord, native: NativeEngine): OwnedSnapshot => {
    const role = record.agent_type ?? "unknown";
    const assignmentDenied = record.tool_deny?.some((tool) =>
      ["coordination_ask", "coordination_notify", "*"].includes(tool));
    const denial = record.tool_deny?.includes("__coordination_independent_review__") ? "independent_review" :
      assignmentDenied ? "assignment_denied" : role === "plan-reviewer" ? "one_shot" :
      ["omo-native-gate-reviewer", "omo-native-code-reviewer", "omo-native-qa-executor"].includes(role)
        ? "independent_review" : undefined;
    return {
      identity: { ...binding.identity }, status: record.status, residency: record.residency_state, role,
      name: record.name ?? record.task_id,
      last_transition_at: record.status === "pending" ? record.created_at ?? null :
        record.status === "running" ? record.started_at ?? null : record.terminal_at ?? null,
      admission: record.failure_kind && !record.child_session_id && record.pid === undefined &&
        !record.host_session ? "failed" : record.status === "pending" ? "queued" :
        record.child_session_id || record.pid !== undefined || record.host_session ? "admitted" : "unknown",
      collaboration: denial ? { allowed: false, denial_reason: denial } : { allowed: true },
      notification: { ...record.notification },
      slot: native.manager.concurrency.leaseState(record.task_id, record.notification.run_epoch) ??
        (record.status === "pending" ? "not_acquired" : "released"),
      outcome: binding.witness ?? null, source: "native owner engine/store/outcome ports",
      observed_at: new Date().toISOString(),
    };
  };
  const bridge: EngineBridge = {
    listOwned() {
      const active = current();
      if (!active.ok) return active;
      const native = active.value;
      const snapshots: OwnedSnapshot[] = [];
      const issues: EnumerationIssue[] = [];
      const known = new Map([...bindings.values()].map((b) => [b.identity.task_id, b.identity]));
      let listed: readonly { record: NativeRecord }[];
      try { listed = native.manager.list({ scope: "all" }); }
      catch {
        return { ok: true, value: [], enumeration: { owned: known.size, unreadable: known.size,
          coverage: "unknown", issues: known.size ? [...known.values()].map((identity) =>
            ({ identity: { ...identity }, reason: "enumeration_failed" as const })) :
            [{ reason: "enumeration_failed" }] } };
      }
      const seen = new Set<string>();
      for (const { record } of listed) {
        if (normalize(record.root_session_id) !== normalize(ownerRoot)) continue;
        seen.add(record.task_id);
        const handle = native.manager.getResidentHandle(record.task_id);
        const sameEpoch = [...bindings.values()].filter((b) => b.identity.task_id === record.task_id &&
          b.identity.run_epoch === record.notification.run_epoch);
        let binding = (handle === undefined ? sameEpoch.find((b) => b.witness) : undefined) ??
          sameEpoch.find((b) => b.handle?.deref() === handle);
        if (!binding) {
          const target_ref = `coordination:${randomUUID()}` as TargetRef;
          binding = { ...(handle ? { handle: new WeakRef(handle) } : {}), identity: {
            target_ref, task_id: record.task_id, run_epoch: record.notification.run_epoch,
            owner_session_id: owner, owner_root_id: ownerRoot,
            canonical_session_id: record.child_session_id ?? null,
            session_id: record.child_session_id ? normalize(record.child_session_id) : null,
            handle_id: handle ? randomUUID() : null, runtime_instance_id: null, generation: options.generation,
          } };
          bindings.set(target_ref, binding);
        }
        const valid = owned(binding.identity.target_ref);
        if (!valid.ok) {
          issues.push({ identity: { ...binding.identity }, reason: valid.reason });
          continue;
        }
        try { snapshots.push(snapshot(binding, valid.value.record, native)); }
        catch { issues.push({ identity: { ...binding.identity }, reason: "record_unreadable" }); }
      }
      try {
        const diagnostics = carrier.links.get(native)!.store.list().diagnostics;
        for (const diagnostic of diagnostics) {
          if (diagnostic.type !== "parse_error") continue;
          const taskId = /(?:^|[\\/])(st_[a-z0-9]+)\.json$/i.exec(diagnostic.path)?.[1];
          const identity = taskId ? known.get(taskId) : undefined;
          if (taskId && seen.has(taskId)) continue;
          if (taskId) seen.add(taskId);
          issues.push({ ...(identity ? { identity: { ...identity } } : {}), reason: "record_unreadable" });
        }
      } catch { issues.push({ reason: "enumeration_failed" }); }
      // A previously owned row can disappear between the list and individual reads.
      // Preserve its issued identity, never substitute its old state as fresh truth.
      for (const [taskId, identity] of known) {
        if (seen.has(taskId)) continue;
        try {
          const fresh = carrier.links.get(native)!.store.load(taskId);
          if (!fresh || normalize(fresh.root_session_id) !== normalize(ownerRoot)) continue;
        } catch {
          issues.push({ identity: { ...identity }, reason: "record_unreadable" });
          continue;
        }
        issues.push({ identity: { ...identity }, reason: "record_unreadable" });
      }
      return { ok: true, value: snapshots, enumeration: {
        owned: snapshots.length + issues.filter((issue) => issue.identity).length,
        unreadable: issues.length, coverage: issues.length ? "partial" : "complete", issues,
      } };
    },
    inspectOwned(target) {
      const valid = owned(target);
      return valid.ok ? { ok: true, value: snapshot(valid.value.binding, valid.value.record, valid.value.engine) } : valid;
    },
    bindActualRuntime(target, runtime) {
      const valid = owned(target);
      if (!valid.ok) return valid;
      const handle = valid.value.binding.handle?.deref();
      const owner = handle && carrier.runtimeOwners.get(handle);
      if (!handle || valid.value.engine.manager.getResidentHandle(valid.value.record.task_id) !== handle ||
          !owner || owner.task_id !== valid.value.record.task_id || owner.runtime.deref() !== runtime ||
          (valid.value.binding.identity.session_id !== null &&
            normalize(runtime.sessionId) !== valid.value.binding.identity.session_id)) {
        return { ok: false, reason: "runtime_mismatch" };
      }
      const binding = valid.value.binding;
      if (binding.runtime?.deref() && binding.runtime.deref() !== runtime) {
        return { ok: false, reason: "runtime_mismatch" };
      }
      binding.runtime = new WeakRef(runtime);
      binding.identity = { ...binding.identity, session_id: binding.identity.session_id ?? normalize(runtime.sessionId),
        runtime_instance_id: binding.identity.runtime_instance_id ?? randomUUID() };
      return { ok: true, value: { ...binding.identity } };
    },
    subscribeOutcome(target, listener) {
      const valid = owned(target);
      if (!valid.ok) return valid;
      let active = true;
      const stop = () => { active = false; stops.delete(stop); };
      stops.add(stop);
      const handle = valid.value.binding.handle?.deref();
      if (!handle) {
        stop();
        return { ok: false, reason: "unsupported_runtime", connection: "task has no live handle" };
      }
      const provenance = Object.freeze({ ...valid.value.binding.identity });
      void handle.waitForOutcome().then((outcome) => {
        if (!active) return;
        const checked = owned(target, handle);
        if (!checked.ok) { stop(); return; }
        const witness: OutcomeWitness = checked.value.binding.witness ?? Object.freeze({
          owner_session_id: provenance.owner_session_id, owner_root_id: provenance.owner_root_id,
          generation: provenance.generation, task_id: provenance.task_id, run_epoch: provenance.run_epoch,
          handle_id: provenance.handle_id!, status: outcome.status,
          result_sha256: outcome.finalResponse === undefined ? null :
            createHash("sha256").update(outcome.finalResponse).digest("hex"),
          source: "native ManagedChildHandle.waitForOutcome", observed_at: new Date().toISOString(),
        });
        checked.value.binding.witness = witness;
        stop();
        listener(witness);
      }, () => { stop(); });
      return { ok: true, value: stop };
    },
    reconcileOwnedTerminal(target) {
      const valid = owned(target);
      if (!valid.ok) return valid;
      const { binding, record, engine: native } = valid.value;
      const handle = binding.handle?.deref();
      const witness = binding.witness;
      if (!settlement || !handle || witness?.status !== "completed" || !witness.result_sha256) {
        return { ok: false, reason: "repair_not_enabled" };
      }
      let refusal: RefusalReason | undefined;
      const guard = (fresh: SettlementRecord) => {
        const active = current();
        if (!active.ok) { refusal = active.reason; return false; }
        if (fresh.notification.run_epoch !== binding.identity.run_epoch) {
          refusal = "stale_epoch"; return false;
        }
        const resident = native.manager.getResidentHandle(record.task_id);
        const live = ports.liveHandle(record.task_id);
        if (fresh.task_id !== record.task_id || fresh.parent_session_id !== record.parent_session_id ||
            normalize(fresh.root_session_id) !== normalize(ownerRoot) ||
            fresh.host_pid !== (record as NativeRecord & { host_pid?: number }).host_pid ||
            fresh.residency_claim !== (record as NativeRecord & { residency_claim?: unknown }).residency_claim ||
            ["persisted_only", "rpc_detached"].includes(fresh.residency_state) ||
            !["running", "completed"].includes(fresh.status) ||
            (resident !== undefined && resident !== handle) || (live !== undefined && live !== handle) ||
            (fresh.child_session_id !== undefined && normalize(fresh.child_session_id) !== binding.identity.session_id)) {
          refusal = "scope_denied"; return false;
        }
        return true;
      };
      const receipt = settlement.reconcile({ task_id: record.task_id, run_epoch: binding.identity.run_epoch,
        handle, result_sha256: witness.result_sha256, guard,
        slot: () => native.manager.concurrency.leaseState(record.task_id, binding.identity.run_epoch) ?? "released" });
      return receipt ? { ok: true, value: receipt } : { ok: false, reason: refusal ?? "repair_not_enabled" };
    },
    dispose,
  };
  if (settlement && typeof engine.onStoreMutation === "function") {
    automatic = attachAutoReconcile({ bridge, carrier, store: link.store,
      onMutation: (listener) => engine.onStoreMutation!(listener) });
  }
  return { ok: true, value: Object.freeze(bridge) };
}

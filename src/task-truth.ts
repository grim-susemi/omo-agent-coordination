import { randomUUID } from "node:crypto";
import type { Asks, DenialReason, Fact, Peer, Roster, TargetBinding, TargetInspection, TargetRef,
  TreeInspection } from "./contracts.ts";
import type { BridgeResult, EngineBridge, EnumerationIssue, OwnedIdentity,
  OwnedSnapshot } from "./engine-bridge.ts";
import type { CapturedRuntime, RuntimeCapture, RuntimeSnapshot } from "./runtime-capture.ts";

type Policy = { readonly allowed: boolean; readonly denial_reason?: DenialReason };
export function peerPolicy(role: string, native: Policy, asks: Asks = "allowed"):
  Pick<Peer, "asks" | "ask_capability" | "notify_capability" | "denial_reason"> {
  const denial = native.denial_reason ?? (role === "plan-reviewer" ? "one_shot" :
    ["omo-native-gate-reviewer", "omo-native-code-reviewer", "omo-native-qa-executor"].includes(role)
      ? "independent_review" : !native.allowed ? "assignment_denied" :
        asks === "denied" ? "target_excluded" : undefined);
  return denial ? { asks: "denied", ask_capability: false, notify_capability: false, denial_reason: denial }
    : { asks: "allowed", ask_capability: true, notify_capability: true };
}
const terminal = (status: string) => ["completed", "error", "cancelled", "interrupted", "lost"].includes(status);
function fact<T>(value: T | "unknown", source: string, at: string, evidence_ref?: string): Fact<T> {
  return { value, source, observed_at: at, ...(evidence_ref ? { evidence_ref } : {}) };
}
function activityOf(runtime: RuntimeSnapshot): string {
  const activity = runtime.activity;
  if (activity.isBashRunning) return "bash";
  if (activity.isCompacting) return "compaction";
  if (activity.hasActiveWakeSource) return "deferred_wake";
  if (activity.hasSessionWork) return "session_work";
  const results = new Set<string>();
  const calls = new Set<string>();
  for (const message of runtime.messages) {
    if (!message || typeof message !== "object") continue;
    const row = message as { role?: string; toolCallId?: string; content?: unknown };
    if (row.role === "toolResult" && row.toolCallId) results.add(row.toolCallId);
    if (row.role !== "assistant" || !Array.isArray(row.content)) continue;
    for (const part of row.content) {
      if (part?.type === "toolCall" && typeof part.id === "string") calls.add(part.id);
    }
  }
  if (activity.isStreaming && [...calls].some((id) => !results.has(id))) return "blocked_tool";
  return activity.isStreaming ? "streaming" : "idle";
}

/** Only these scalar facts leave a process child; no prompt or transcript. */
export interface RuntimeFacts {
  readonly session_id: string;
  readonly runtime_instance_id: string;
  readonly revision: number;
  readonly observed_at: string;
  readonly activity: string;
}
export const runtimeFacts = (runtime: RuntimeSnapshot): RuntimeFacts => ({
  session_id: runtime.session_id, runtime_instance_id: runtime.runtime_instance_id,
  revision: runtime.revision, observed_at: runtime.observed_at, activity: activityOf(runtime),
});
export interface RemoteRuntimeObservation {
  readonly binding: TargetBinding;
  readonly facts: RuntimeFacts;
  current(): boolean;
}

/** Input facts are current owner snapshots, not model text or a persisted transcript. */
export function projectTask(snapshot: OwnedSnapshot, runtime: RuntimeSnapshot | RuntimeFacts | null,
  reachable: boolean | "unknown" = runtime ? true : "unknown", asks: Asks = "allowed"): TargetInspection {
  const at = snapshot.observed_at;
  const id = snapshot.identity;
  const evidence = `${id.task_id}/epoch/${id.run_epoch}`;
  const witness = snapshot.outcome;
  const outcome = witness && witness.task_id === id.task_id && witness.run_epoch === id.run_epoch &&
    witness.handle_id === id.handle_id && witness.owner_session_id === id.owner_session_id &&
    witness.owner_root_id === id.owner_root_id && witness.generation === id.generation ? witness : null;
  const settled = terminal(snapshot.status);
  const mismatch = outcome !== null && outcome.status !== snapshot.status;
  const remote = runtime && typeof runtime.activity === "string";
  const activity = runtime ? remote ? runtime.activity as string : activityOf(runtime as RuntimeSnapshot) : "unknown";
  const runtimeSource = remote ? "authenticated native process runtime facts" : "exact native-owned runtime capture";
  const unpublished = outcome !== null && !settled;
  const diagnostic: TargetInspection["diagnostic_class"] = unpublished ? "completed_unpublished" :
    snapshot.admission === "failed" ? "never_started" :
      snapshot.status === "pending" ? "queued" :
        reachable === false || snapshot.residency === "rpc_detached" ? "transport_unreachable" :
          activity === "blocked_tool" ? "live_blocked_tool" :
            ["bash", "compaction", "deferred_wake", "session_work"].includes(activity) ? "external_wait" :
              runtime || settled ? "normal" : "unknown";
  return {
    target_ref: id.target_ref, live: settled || outcome || snapshot.admission === "failed" ? false :
      reachable === true ? true : "unknown",
    status: snapshot.status, run_epoch: id.run_epoch, last_transition_at: snapshot.last_transition_at,
    record_consistent: mismatch ? false : settled || snapshot.status === "pending" || runtime ? true : "unknown",
    asks: peerPolicy(snapshot.role, snapshot.collaboration, asks).asks,
    recorded_state: fact(snapshot.status, snapshot.source, at, evidence),
    runtime_reachability: fact(reachable, runtimeSource, runtime?.observed_at ?? at),
    activity: fact(activity, remote ? runtimeSource : "actual AgentSession.activitySnapshot and unfinished tool pairs", runtime?.observed_at ?? at),
    progress: fact(runtime ? { evidence_ref: `${runtime.runtime_instance_id}/revision/${runtime.revision}`,
      at: runtime.observed_at } : "unknown", remote ? runtimeSource : "capture-local live context revision", runtime?.observed_at ?? at),
    work_outcome: fact(outcome?.status ?? "unknown", outcome?.source ?? "native outcome not observed",
      outcome?.observed_at ?? at, outcome ? evidence : undefined),
    publication: fact(unpublished ? "unpublished" : settled ? snapshot.status : "not_terminal",
      "fresh native owner store", at, evidence),
    notification: fact(snapshot.notification.notified_epoch === id.run_epoch ? "accepted" : "not_accepted",
      "native notification epoch bookkeeping", at, evidence),
    slot: fact(snapshot.slot, "native owner concurrency leaseState", at, evidence),
    residency: fact(snapshot.residency as TargetInspection["residency"]["value"], snapshot.source, at, evidence),
    diagnostic_class: diagnostic,
  };
}

export interface InspectionTree extends TreeInspection {
  readonly errors: readonly EnumerationIssue[];
}
export function createTaskTruth(options: {
  bridge: EngineBridge; capture: RuntimeCapture; owner: CapturedRuntime; generation: number;
  asks?: (target: TargetRef) => Asks;
}) {
  const root = (() => {
    const actual = options.owner.runtime();
    return actual ? new WeakRef(actual) : undefined;
  })();
  const session = root?.deref()?.sessionId;
  let rootRef = `coordination:${randomUUID()}` as TargetRef;
  let rootBranch: string | null = null;
  const listeners = new Set<(roster: Roster) => void>();
  const subscriptions = new Map<TargetRef, () => void>();
  const known = new Map<TargetRef, Peer>();
  let disposed = false;
  let revision = 0;
  let signature = "";
  let roster: Roster = { revision, peers: [] };
  let refreshing = false;
  let readRemote: ((ref: TargetRef) => Promise<RemoteRuntimeObservation | null>) | undefined;
  const remoteObservations = new Map<TargetRef, RemoteRuntimeObservation>();
  const remoteChanges = new Map<TargetRef, { change: number; revision: number }>();
  const rootInspection = (): TargetInspection => {
    const read = options.owner.read();
    const runtime = read.ok ? read.value : null;
    const at = runtime?.observed_at ?? new Date().toISOString();
    const unknown = <T>() => fact<T>("unknown", "root has no native task record", at);
    return {
      target_ref: rootRef, live: runtime ? true : "unknown", status: "unknown", run_epoch: null,
      last_transition_at: null, record_consistent: "unknown", asks: "allowed",
      recorded_state: unknown(), runtime_reachability: fact(runtime ? true : "unknown",
        "bound owner AgentSession capture", at),
      activity: fact(runtime ? activityOf(runtime) : "unknown", "actual owner activitySnapshot", at),
      progress: fact(runtime ? { at, evidence_ref: `${runtime.runtime_instance_id}/revision/${runtime.revision}` } :
        "unknown", "capture-local live owner context revision", at),
      work_outcome: unknown(), publication: unknown(), notification: unknown(), slot: unknown(), residency: unknown(),
      diagnostic_class: runtime ? "normal" : "unknown",
    };
  };
  const unreadable = (identity: OwnedIdentity): TargetInspection => {
    const at = new Date().toISOString();
    const unknown = <T>() => fact<T>("unknown", "current owner record unreadable", at);
    return { target_ref: identity.target_ref, live: "unknown", status: "unknown",
      run_epoch: identity.run_epoch, last_transition_at: null, record_consistent: "unknown",
      asks: "denied", recorded_state: unknown(), runtime_reachability: unknown(), activity: unknown(),
      progress: unknown(), work_outcome: unknown(), publication: unknown(), notification: unknown(),
      slot: unknown(), residency: unknown(), diagnostic_class: "unknown" };
  };
  const project = (snapshot: OwnedSnapshot): TargetInspection => {
    const ref = snapshot.identity.target_ref;
    if (snapshot.identity.handle_id && !subscriptions.has(ref)) {
      const subscribed = options.bridge.subscribeOutcome(ref, () => {});
      if (subscribed.ok) subscriptions.set(ref, subscribed.value);
    }
    let runtime: RuntimeSnapshot | RuntimeFacts | null = null;
    for (const capture of options.capture.list()) {
      if (capture.runtime()?.sessionId.replace(/^senpi:/, "") !== snapshot.identity.session_id) continue;
      // Matching strings are discovery only. This call still requires the exact
      // runtime object recorded by the original native runner and managed handle.
      if (!options.capture.bindOwned(options.bridge, ref, capture).ok) continue;
      const read = capture.read();
      if (read.ok) runtime = read.value;
      break;
    }
    const fresh = options.bridge.inspectOwned(ref);
    if (!fresh.ok) return unreadable(snapshot.identity);
    const remote = remoteObservations.get(ref);
    if (!runtime && remote?.current()) {
      const id = fresh.value.identity, binding = remote.binding, facts = remote.facts;
      if (binding.kind === "worker" && binding.task_id === id.task_id && binding.run_epoch === id.run_epoch &&
          binding.generation === id.generation && binding.generation === options.generation &&
          binding.owner_session_id.replace(/^senpi:/, "") === id.owner_session_id.replace(/^senpi:/, "") &&
          binding.owner_root_id.replace(/^senpi:/, "") === id.owner_root_id.replace(/^senpi:/, "") &&
          // Native RPC session metadata may arrive late. The channel's current()
          // still proves the original live handle and its RPC-authenticated ID.
          (id.session_id === null || binding.session_id.replace(/^senpi:/, "") === id.session_id.replace(/^senpi:/, "")) &&
          facts.session_id.replace(/^senpi:/, "") === binding.session_id.replace(/^senpi:/, "") &&
          facts.runtime_instance_id === binding.runtime_instance_id) runtime = facts;
    }
    return projectTask(fresh.value, runtime, runtime ? true :
      fresh.value.residency === "rpc_detached" ? false : "unknown", options.asks?.(ref));
  };
  const refresh = (): BridgeResult<InspectionTree> => {
    if (disposed || options.capture.generation !== options.generation) return { ok: false, reason: "stale_generation" };
    const actual = root?.deref();
    if (!actual || options.owner.runtime() !== actual || actual.sessionId !== session) {
      return { ok: false, reason: "scope_denied" };
    }
    const leaf = actual.sessionManager.getLeafId();
    if (rootBranch === null) rootBranch = leaf;
    else if (leaf !== rootBranch) {
      const entries = Reflect.apply(Reflect.get(actual.sessionManager, "getBranch"), actual.sessionManager, []) as { id: string }[];
      if (!entries.some((entry) => entry.id === rootBranch)) {
        rootRef = `coordination:${randomUUID()}` as TargetRef;
        rootBranch = leaf;
      }
    }
    const listed = options.bridge.listOwned();
    if (!listed.ok) return listed;
    const targets: TargetInspection[] = [rootInspection()];
    const peers: Peer[] = [{ target_ref: rootRef, name: "root", role: "root",
      asks: "allowed", ask_capability: true, notify_capability: true }];
    const errors: EnumerationIssue[] = [...(listed.enumeration?.issues ?? [])];
    let failedReads = 0;
    for (const snapshot of listed.value) {
      const projected = project(snapshot);
      targets.push(projected);
      if (projected.recorded_state.value === "unknown") {
        failedReads++;
        errors.push({ identity: snapshot.identity, reason: "record_unreadable" });
      }
      const peer: Peer = { target_ref: snapshot.identity.target_ref, name: snapshot.name, role: snapshot.role,
        ...peerPolicy(snapshot.role, snapshot.collaboration, options.asks?.(snapshot.identity.target_ref)) };
      known.set(peer.target_ref, peer);
      peers.push(peer);
    }
    for (const issue of listed.enumeration?.issues ?? []) {
      if (!issue.identity) continue;
      targets.push(unreadable(issue.identity));
      const previous = known.get(issue.identity.target_ref);
      if (previous) peers.push({ ...previous, asks: "denied", ask_capability: false, notify_capability: false });
    }
    peers.sort((a, b) => a.target_ref.localeCompare(b.target_ref));
    const nextSignature = JSON.stringify(peers);
    if (nextSignature !== signature) {
      signature = nextSignature;
      roster = Object.freeze({ revision: ++revision, peers: Object.freeze(peers.map((peer) => Object.freeze(peer))) });
      for (const listener of listeners) listener(roster);
    }
    const current = new Set(peers.map((peer) => peer.target_ref));
    for (const [ref, stop] of subscriptions) {
      if (!current.has(ref)) { stop(); subscriptions.delete(ref); }
    }
    const unread = (listed.enumeration?.unreadable ?? 0) + failedReads;
    return { ok: true, value: { ...roster, owned: 1 + (listed.enumeration?.owned ?? listed.value.length),
      visited: 1 + listed.value.length - failedReads, unreadable: unread,
      coverage: listed.enumeration?.coverage === "unknown" ? "unknown" : unread ? "partial" : "complete",
      targets, errors } };
  };
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    stopCapture();
    for (const stop of subscriptions.values()) stop();
    subscriptions.clear();
    listeners.clear();
    known.clear();
    remoteObservations.clear(); remoteChanges.clear(); readRemote = undefined;
  };
  const stopCapture = options.capture.onRuntime((capture, event) => {
    if (capture === options.owner && event === "closed") { dispose(); return; }
    if (refreshing || disposed || event === "changed") return;
    refreshing = true;
    try { refresh(); } finally { refreshing = false; }
  });
  return Object.freeze({
    async inspect(target?: TargetRef): Promise<BridgeResult<InspectionTree | TargetInspection>> {
      const tree = refresh();
      if (!tree.ok) return tree;
      const reader = readRemote;
      if (reader) for (const row of tree.value.targets) {
        if (target !== undefined && row.target_ref !== target) continue;
        remoteObservations.delete(row.target_ref);
        const version = remoteChanges.get(row.target_ref) ?? { change: 0, revision: -1 };
        const observed = await reader(row.target_ref);
        const current = remoteChanges.get(row.target_ref) ?? version;
        if (observed && version.change === current.change && observed.facts.revision >= current.revision) {
          remoteObservations.set(row.target_ref, observed);
          remoteChanges.set(row.target_ref, { change: current.change, revision: observed.facts.revision });
        }
      }
      // A resolved native outcome is observed through its existing promise, not
      // inferred from a model stop, a quiet session, or the record's age.
      await Promise.resolve();
      const fresh = refresh();
      if (!fresh.ok || target === undefined) return fresh;
      if (target === rootRef) return { ok: true, value: rootInspection() };
      const snapshot = options.bridge.inspectOwned(target);
      return snapshot.ok ? { ok: true, value: project(snapshot.value) } : snapshot;
    },
    peers(): BridgeResult<Roster> {
      const tree = refresh();
      return tree.ok ? { ok: true, value: roster } : tree;
    },
    onPeersChanged(listener: (next: Roster) => void) {
      listeners.add(listener);
      return () => { listeners.delete(listener); };
    },
    remoteRuntimeReader(read: (ref: TargetRef) => Promise<RemoteRuntimeObservation | null>) {
      readRemote = read;
      return () => { if (readRemote === read) { readRemote = undefined; remoteObservations.clear(); } };
    },
    observeRemote(observed: RemoteRuntimeObservation) {
      const ref = observed.binding.target_ref, version = remoteChanges.get(ref) ?? { change: 0, revision: -1 };
      if (!observed.current() || observed.facts.revision < version.revision) return;
      remoteObservations.set(ref, observed);
      remoteChanges.set(ref, { change: version.change + 1, revision: observed.facts.revision });
    },
    invalidateRemote(ref: TargetRef) {
      const version = remoteChanges.get(ref) ?? { change: 0, revision: -1 };
      remoteObservations.delete(ref);
      remoteChanges.set(ref, { ...version, change: version.change + 1 });
    },
    rosterAfter(consumedRevision: number): BridgeResult<Roster | null> {
      const tree = refresh();
      return tree.ok ? { ok: true, value: roster.revision === consumedRevision ? null : roster } : tree;
    },
    dispose,
  });
}
export type TaskTruth = ReturnType<typeof createTaskTruth>;

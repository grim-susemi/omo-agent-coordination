import { createHash, randomUUID } from "node:crypto";
import type { EngineBridge, OwnedIdentity, SenpiNamespace } from "./engine-bridge.ts";
import type { TargetRef } from "./contracts.ts";

export interface LiveSession {
  readonly sessionId: string;
  readonly sessionManager: { getSessionId(): string; getLeafId(): string | null };
  readonly model: { provider: string; id: string; [key: string]: unknown } | undefined;
  readonly modelRuntime: object;
  readonly systemPrompt: string;
  readonly messages: readonly unknown[];
  readonly activitySnapshot: {
    isStreaming: boolean; isBashRunning: boolean; isCompacting: boolean;
    hasSessionWork: boolean; hasActiveWakeSource: boolean;
  };
  readonly _disposed?: boolean;
  subscribe(listener: (event: { type: string }) => void): () => void;
  dispose(): void;
}
type Failure = "unsupported_runtime" | "capture_missing" | "stale_generation" |
  "target_not_live" | "runtime_mismatch" | "scope_denied" | "stale_epoch" | "invalid_target" |
  "connection_failed" | "repair_not_enabled";
export type CaptureResult<T> = { readonly ok: true; readonly value: T } |
  { readonly ok: false; readonly reason: Failure; readonly connection?: string };
export interface RuntimeSnapshot {
  readonly session_id: string;
  readonly runtime_instance_id: string;
  readonly generation: number;
  readonly branch: string | null;
  /** Capture-local monotonic revision, not a native persistence revision. */
  readonly revision: number;
  readonly observed_at: string;
  readonly model: NonNullable<LiveSession["model"]>;
  readonly model_runtime: object;
  readonly system_prompt: string;
  readonly messages: readonly unknown[];
  readonly activity: LiveSession["activitySnapshot"];
}
/** Live scalar facts plus the message array by reference: no clone and no hash. */
export interface RuntimeObservation {
  readonly session_id: string;
  readonly runtime_instance_id: string;
  readonly generation: number;
  readonly branch: string | null;
  readonly revision: number;
  readonly observed_at: string;
  readonly messages: readonly unknown[];
  readonly activity: LiveSession["activitySnapshot"];
}
export interface CapturedRuntime {
  readonly runtime_instance_id: string;
  runtime(): LiveSession | undefined;
  /** Same verdict as `read().ok` without cloning and hashing the conversation. */
  alive(): boolean;
  /** Same liveness verdict as `read()`, but returns the messages by reference. */
  observe(): CaptureResult<RuntimeObservation>;
  read(): CaptureResult<RuntimeSnapshot>;
}
interface Prototype {
  bindExtensions(this: LiveSession, ...args: unknown[]): unknown;
  dispose(this: LiveSession, ...args: unknown[]): unknown;
}
interface Hook {
  original: Prototype["bindExtensions"];
  wrapped: Prototype["bindExtensions"];
  originalDispose: Prototype["dispose"];
  wrappedDispose: Prototype["dispose"];
  listeners: Map<symbol, { bound(runtime: LiveSession): void; closed(runtime: LiveSession): void }>;
}
const HOOKS = Symbol.for("omo.agent-coordination.capture-hooks.v1");
// Verified compatible: the AgentSession surfaces captured below (class, sessionId,
// sessionManager.getLeafId, subscribe, model, modelRuntime, systemPrompt, messages,
// activitySnapshot, _disposed, bindExtensions, dispose) are unchanged between these builds.
const SUPPORTED_SENPI = new Set(["2026.10.19", "2026.10.19-2"]);
const supportedVersion = (version: string | undefined) => version !== undefined && SUPPORTED_SENPI.has(version);
const global = globalThis as unknown as Record<symbol, unknown>;
const hooks = (global[HOOKS] ??= new WeakMap<Prototype, Hook>()) as WeakMap<Prototype, Hook>;
// Reload re-evaluates this module and does not call bindExtensions again.
// Keep only weak references to runtimes admitted by a successful native bind.
const BOUND_RUNTIMES = Symbol.for("omo.agent-coordination.bound-runtimes.v1");
const boundRuntimes = (global[BOUND_RUNTIMES] ??= new Set<WeakRef<LiveSession>>()) as Set<WeakRef<LiveSession>>;

/** A birth observation is not ownership. Only bindOwned's native bridge may authorize a worker.
 * No transcript, credentials, queued draft, provider request, or session setter is used here.
 */
export function createRuntimeCapture(options: {
  loaderNamespace: SenpiNamespace & { VERSION?: string };
  /** Exact SDK module published by the pinned native carrier before child construction. */
  nativeNamespace?: () => SenpiNamespace | undefined;
  generation: number;
  enabled?: boolean;
}) {
  const token = Symbol();
  const records = new WeakMap<LiveSession, CapturedRuntime>();
  const stops = new WeakMap<LiveSession, () => void>();
  const live = new Set<WeakRef<LiveSession>>();
  const listeners = new Set<(capture: CapturedRuntime, event: "bound" | "changed" | "closed") => void>();
  const seen = new WeakSet<object>();
  let disposed = false;
  let refusal: string | undefined;
  const attached = new Map<Prototype, Hook>();
  const classes = new Set<SenpiNamespace["AgentSession"]>();
  const emit = (capture: CapturedRuntime, event: "bound" | "changed" | "closed") => {
    for (const listener of listeners) listener(capture, event);
  };
  const close = (runtime: LiveSession) => {
    const record = records.get(runtime);
    if (!record) return;
    stops.get(runtime)?.();
    stops.delete(runtime);
    records.delete(runtime);
    emit(record, "closed");
  };
  const bound = (runtime: LiveSession) => {
    if (disposed || seen.has(runtime)) return;
    if (![...classes].some(AgentSession => runtime instanceof AgentSession) ||
        typeof runtime.sessionId !== "string" || !runtime.sessionManager ||
        typeof runtime.sessionManager.getLeafId !== "function" ||
        typeof runtime.subscribe !== "function" || !runtime.modelRuntime ||
        typeof runtime.systemPrompt !== "string" || !Array.isArray(runtime.messages) ||
        !runtime.activitySnapshot || runtime._disposed) {
      refusal = "runtime_shape";
      return;
    }
    let remembered = false;
    for (const ref of boundRuntimes) {
      const actual = ref.deref();
      if (!actual || actual._disposed) boundRuntimes.delete(ref);
      else if (actual === runtime) remembered = true;
    }
    if (!remembered) boundRuntimes.add(new WeakRef(runtime));
    seen.add(runtime);
    const ref = new WeakRef(runtime);
    const instance = randomUUID();
    let revision = 0;
    let fingerprint: string | undefined;
    let closed = false;
    const capture: CapturedRuntime = Object.freeze({
      runtime_instance_id: instance,
      runtime: () => disposed || closed ? undefined : ref.deref(),
      alive(): boolean {
        if (disposed || refusal) return false;
        const actual = ref.deref();
        return !closed && !!actual && !actual._disposed && !!actual.model;
      },
      observe(): CaptureResult<RuntimeObservation> {
        if (disposed) return { ok: false, reason: "stale_generation" };
        if (refusal) return { ok: false, reason: "unsupported_runtime", connection: refusal };
        const actual = ref.deref();
        if (closed || !actual || actual._disposed) return { ok: false, reason: "target_not_live" };
        if (!actual.model) return { ok: false, reason: "unsupported_runtime", connection: "model_unavailable" };
        return { ok: true, value: {
          session_id: actual.sessionId, runtime_instance_id: instance, generation: options.generation,
          branch: actual.sessionManager.getLeafId(), revision, observed_at: new Date().toISOString(),
          messages: actual.messages, activity: { ...actual.activitySnapshot },
        } };
      },
      read(): CaptureResult<RuntimeSnapshot> {
        if (disposed) return { ok: false, reason: "stale_generation" };
        if (refusal) return { ok: false, reason: "unsupported_runtime", connection: refusal };
        const actual = ref.deref();
        if (closed || !actual || actual._disposed) return { ok: false, reason: "target_not_live" };
        if (!actual.model) return { ok: false, reason: "unsupported_runtime", connection: "model_unavailable" };
        const model = structuredClone(actual.model);
        const messages = structuredClone(actual.messages);
        const prompt = actual.systemPrompt;
        const branch = actual.sessionManager.getLeafId();
        const next = createHash("sha256").update(JSON.stringify([model, messages, prompt, branch])).digest("hex");
        if (fingerprint !== undefined && next !== fingerprint) revision++;
        fingerprint = next;
        return { ok: true, value: {
          session_id: actual.sessionId, runtime_instance_id: instance, generation: options.generation,
          branch, revision, observed_at: new Date().toISOString(), model,
          model_runtime: actual.modelRuntime, system_prompt: prompt, messages,
          activity: { ...actual.activitySnapshot },
        } };
      },
    });
    records.set(runtime, capture);
    live.add(ref);
    const unsubscribe = runtime.subscribe(() => {
      if (disposed || closed) return;
      revision++;
      emit(capture, "changed");
    });
    stops.set(runtime, () => { closed = true; unsubscribe(); });
    emit(capture, "bound");
  };
  const moduleReady = (namespace: SenpiNamespace & { VERSION?: string }): CaptureResult<void> => {
    if (disposed) return { ok: false, reason: "stale_generation" };
    const proto = namespace?.AgentSession?.prototype as unknown as Prototype | undefined;
    const reason = options.enabled === false || process.env.OMO_AGENT_COORDINATION === "0" ? "mode_disabled" :
      !supportedVersion(options.loaderNamespace.VERSION) || !supportedVersion(namespace.VERSION) ? "version" :
      !proto || typeof proto.bindExtensions !== "function" || typeof proto.dispose !== "function" ? "shape" :
      namespace.AgentSession !== options.loaderNamespace.AgentSession &&
        namespace.AgentSession !== options.nativeNamespace?.()?.AgentSession ? "class_identity" : undefined;
    if (reason) {
      refusal = reason;
      return { ok: false, reason: "unsupported_runtime", connection: reason };
    }
    if (attached.has(proto!)) return { ok: true, value: undefined };
    let hook = hooks.get(proto!);
    if (!hook) {
      const original = proto!.bindExtensions;
      const originalDispose = proto!.dispose;
      const subscribers: Hook["listeners"] = new Map();
      const wrapped = async function (this: LiveSession, ...args: unknown[]) {
        const result = await Reflect.apply(original, this, args);
        // A rejected native binding, including a forged receiver, is never captured.
        for (const listener of subscribers.values()) listener.bound(this);
        return result;
      };
      const wrappedDispose = function (this: LiveSession, ...args: unknown[]) {
        try { return Reflect.apply(originalDispose, this, args); }
        finally { for (const listener of subscribers.values()) listener.closed(this); }
      };
      hook = { original, wrapped, originalDispose, wrappedDispose, listeners: subscribers };
      proto!.bindExtensions = wrapped;
      proto!.dispose = wrappedDispose;
      hooks.set(proto!, hook);
    }
    hook.listeners.set(token, { bound, closed: close });
    classes.add(namespace.AgentSession);
    attached.set(proto!, hook);
    return { ok: true, value: undefined };
  };
  const status = moduleReady(options.loaderNamespace);
  return Object.freeze({
    status, generation: options.generation, moduleReady,
    adopt(sessionManager: object): void {
      if (disposed || refusal) return;
      for (const ref of boundRuntimes) {
        const runtime = ref.deref();
        if (!runtime || runtime._disposed) { boundRuntimes.delete(ref); continue; }
        if (runtime.sessionManager === sessionManager) bound(runtime);
      }
    },
    list(): readonly CapturedRuntime[] {
      const result: CapturedRuntime[] = [];
      for (const ref of live) {
        const runtime = ref.deref();
        const record = runtime && records.get(runtime);
        if (!runtime || !record) { live.delete(ref); continue; }
        result.push(record);
      }
      return result;
    },
    forRuntime(runtime: LiveSession): CaptureResult<CapturedRuntime> {
      if (disposed) return { ok: false, reason: "stale_generation" };
      const record = records.get(runtime);
      return record ? { ok: true, value: record } : { ok: false, reason: "capture_missing" };
    },
    bindOwned(bridge: EngineBridge, target: TargetRef, capture: CapturedRuntime): CaptureResult<OwnedIdentity> {
      if (disposed) return { ok: false, reason: "stale_generation" };
      if (refusal) return { ok: false, reason: "unsupported_runtime", connection: refusal };
      const runtime = capture.runtime();
      if (!runtime || records.get(runtime) !== capture) return { ok: false, reason: "capture_missing" };
      return bridge.bindActualRuntime(target, runtime);
    },
    onRuntime(listener: (capture: CapturedRuntime, event: "bound" | "changed" | "closed") => void) {
      listeners.add(listener);
      return () => { listeners.delete(listener); };
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      for (const ref of live) { const runtime = ref.deref(); if (runtime) close(runtime); }
      live.clear();
      listeners.clear();
      for (const [prototype, hook] of attached) {
        hook.listeners.delete(token);
        if (hook.listeners.size === 0) {
          if (prototype.bindExtensions === hook.wrapped) prototype.bindExtensions = hook.original;
          if (prototype.dispose === hook.wrappedDispose) prototype.dispose = hook.originalDispose;
          // Keep an inert hook discoverable while another mod's wrapper still chains to it.
          if (prototype.bindExtensions === hook.original && prototype.dispose === hook.originalDispose) hooks.delete(prototype);
        }
      }
      attached.clear();
      classes.clear();
    },
  });
}
export type RuntimeCapture = ReturnType<typeof createRuntimeCapture>;

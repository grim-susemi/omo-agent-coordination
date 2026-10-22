import { expect, test } from "bun:test";
import { runCaptureCase } from "../qa/cases/capture-native.ts";
import { createRuntimeCapture } from "../src/runtime-capture.ts";

test("a fresh module generation adopts only live natively bound runtimes for the exact manager", async () => {
  // Given separately evaluated module instances and native receiver validation.
  const moduleUrl = "../src/runtime-capture.ts?gen=reload-test";
  const generationB: typeof import("../src/runtime-capture.ts") = await import(moduleUrl);
  expect(generationB.createRuntimeCapture).not.toBe(createRuntimeCapture);
  const nativeInstances = new WeakSet<object>();
  class Session {
    sessionId = crypto.randomUUID();
    sessionManager = { getSessionId: () => this.sessionId, getLeafId: () => null };
    model = { provider: "capture", id: "offline" };
    modelRuntime = {};
    systemPrompt = "";
    messages: unknown[] = [];
    activitySnapshot = { isStreaming: false, isBashRunning: false, isCompacting: false,
      hasSessionWork: false, hasActiveWakeSource: false };
    _disposed = false;
    constructor() { nativeInstances.add(this); }
    async bindExtensions() {
      if (!nativeInstances.has(this)) throw new TypeError("Invalid native receiver");
    }
    dispose() { this._disposed = true; }
    subscribe() { return () => {}; }
  }
  const loaderNamespace = { AgentSession: Session, VERSION: "2026.10.19-2" };
  const a = createRuntimeCapture({ loaderNamespace, generation: 1 });
  const root = new Session();
  const closed = new Session();
  const unbound = new Session();
  const forged = Object.assign(Object.create(Session.prototype), new Session());
  try {
    await root.bindExtensions();
    await closed.bindExtensions();
    await expect(Reflect.apply(Session.prototype.bindExtensions, forged, [])).rejects.toThrow("Invalid native receiver");
    closed.dispose();
    const old = a.forRuntime(root);
    a.dispose();
    const next = generationB.createRuntimeCapture({ loaderNamespace, generation: 2 });
    let bounds = 0;
    next.onRuntime((_capture, event) => { if (event === "bound") bounds++; });
    try {
      // When reload supplies the original manager, without rebinding the runtime.
      next.adopt({ ...root.sessionManager });
      next.adopt(closed.sessionManager);
      next.adopt(unbound.sessionManager);
      next.adopt(forged.sessionManager);
      expect(next.list()).toHaveLength(0);
      next.adopt(root.sessionManager);
      next.adopt(root.sessionManager);
      // Then exactly one fresh capture is emitted and old captures remain stale.
      expect(bounds).toBe(1);
      expect(next.forRuntime(root).ok).toBe(true);
      expect(next.list()).toHaveLength(1);
      expect(next.forRuntime(closed).ok).toBe(false);
      expect(next.forRuntime(unbound).ok).toBe(false);
      expect(next.forRuntime(forged).ok).toBe(false);
      if (old.ok) expect(old.value.read()).toEqual({ ok: false, reason: "stale_generation" });
    } finally { next.dispose(); }
  } finally { a.dispose(); root.dispose(); }
});

test("reload recaptures the same root and reconnects API v1 on its next turn", async () => {
  const result = await runCaptureCase("capture-reload");
  expect(result.passed).toBe(true);
  expect(result.reload).toMatchObject({
    captured: true, bridge: true, inspect: { ok: true },
    next_turn_api_version: 1, next_turn_connected: true,
  });
}, 120_000);

test("capture accepts the exact lazy native SDK class alongside the bundled loader, not a foreign class", async () => {
  const sessionClass = () => class {
    sessionId = crypto.randomUUID();
    sessionManager = { getSessionId: () => this.sessionId, getLeafId: () => null };
    model = { provider: "capture", id: "offline" };
    modelRuntime = {};
    systemPrompt = "";
    messages: unknown[] = [];
    activitySnapshot = { isStreaming: false, isBashRunning: false, isCompacting: false,
      hasSessionWork: false, hasActiveWakeSource: false };
    async bindExtensions() {}
    dispose() {}
    subscribe() { return () => {}; }
  };
  const loader = { AgentSession: sessionClass(), VERSION: "2026.10.19" };
  const native = { AgentSession: sessionClass(), VERSION: "2026.10.19" };
  const foreign = { AgentSession: sessionClass(), VERSION: "2026.10.19" };
  let attested: typeof native | undefined;
  const options = { loaderNamespace: loader, generation: 1, nativeNamespace: () => attested };
  const originalLoader = loader.AgentSession.prototype.bindExtensions;
  const originalNative = native.AgentSession.prototype.bindExtensions;
  const capture = createRuntimeCapture(options);
  const root = new loader.AgentSession();
  await root.bindExtensions();
  expect(capture.forRuntime(root).ok).toBe(true);
  // R13 publishes the actual lazy import before child construction.
  attested = native;
  try {
    expect(capture.moduleReady(native)).toEqual({ ok: true, value: undefined });
    const child = new native.AgentSession();
    await child.bindExtensions();
    expect(capture.list()).toHaveLength(2);
    const childCapture = capture.forRuntime(child);
    expect(childCapture.ok).toBe(true);
    if (childCapture.ok) {
      expect(childCapture.value.read().ok).toBe(true);
      expect(childCapture.value.alive()).toBe(true);
    }
    child.dispose();
    if (childCapture.ok) {
      expect(childCapture.value.alive()).toBe(false);
      expect(childCapture.value.read().ok).toBe(false);
    }
    expect(capture.forRuntime(child)).toEqual({ ok: false, reason: "capture_missing" });
  } finally { capture.dispose(); }
  expect(loader.AgentSession.prototype.bindExtensions).toBe(originalLoader);
  expect(native.AgentSession.prototype.bindExtensions).toBe(originalNative);
  const isolated = createRuntimeCapture(options);
  const foreignBind = foreign.AgentSession.prototype.bindExtensions;
  try {
    expect(isolated.moduleReady(foreign)).toEqual({
      ok: false, reason: "unsupported_runtime", connection: "class_identity",
    });
    expect(foreign.AgentSession.prototype.bindExtensions).toBe(foreignBind);
    const stranger = new foreign.AgentSession();
    await stranger.bindExtensions();
    expect(isolated.forRuntime(stranger)).toEqual({ ok: false, reason: "capture_missing" });
  } finally { isolated.dispose(); }
});

test("the real loader captures simultaneous parent and native in-process child with live context", async () => {
  const result = await runCaptureCase("capture-in-process");
  expect(result.passed).toBe(true);
  expect(result.identities).toMatchObject({
    parent_and_child_distinct: true, native_runtime_same_object: true,
    run_epoch: 0, module_ready_before_child: true, same_AgentSession: true,
  });
  expect(result.context_changes).toMatchObject({
    model: true, system_prompt: true, messages: true, revision: true,
    snapshot_detached: true, activity: true,
  });
  expect(result.adversarial).toMatchObject({
    duplicate_capture_prevented: true, fabricated_runtime_refused: true,
    stale_generation_refused: true, prior_wrapper_preserved: true,
  });
  expect(result.lifecycle).toEqual({ native_dispose_observed: true, closed_runtime_refused: true });
  expect(result.cleanup).toMatchObject({
    sessions_disposed: true, prototype_restored: true, module_subscriptions: 0,
  });
}, 120_000);

test("a native process child loads the same single ESM extension and captures its local runtime", async () => {
  const result = await runCaptureCase("capture-process");
  expect(result.passed).toBe(true);
  expect(result.identities).toMatchObject({
    parent_and_child_distinct: true, separate_process: true, run_epoch: 0,
    child_model: "coordination-capture/offline", native_pid_matches: true, native_rpc_branch_matches: true,
  });
  expect(result.extension).toMatchObject({
    native_package_inheritance: true, entry_hash_matches: true, single_esm_js: true,
  });
  expect(result.native_worker_completed).toBe(true);
}, 120_000);

test("the 2026.10.19-2 build is admitted by the same capture gate", async () => {
  const Session = class {
    sessionId = crypto.randomUUID();
    sessionManager = { getSessionId: () => this.sessionId, getLeafId: () => null };
    model = { provider: "capture", id: "offline" };
    modelRuntime = {};
    systemPrompt = "";
    messages: unknown[] = [];
    activitySnapshot = { isStreaming: false, isBashRunning: false, isCompacting: false,
      hasSessionWork: false, hasActiveWakeSource: false };
    async bindExtensions() {}
    dispose() {}
    subscribe() { return () => {}; }
  };
  const capture = createRuntimeCapture({
    loaderNamespace: { AgentSession: Session, VERSION: "2026.10.19-2" }, generation: 1,
  });
  try {
    expect(capture.status).toEqual({ ok: true, value: undefined });
    const session = new Session();
    await session.bindExtensions();
    expect(capture.forRuntime(session).ok).toBe(true);
  } finally { capture.dispose(); }
});

test("version, shape, and class mismatch refuse only capture and preserve native worker completion", async () => {
  const result = await runCaptureCase("capture-unsupported");
  expect(result.passed).toBe(true);
  expect(result.refusals).toEqual(["version", "shape", "class_identity"]);
  expect(result.native_worker_completed).toBe(true);
  expect(result.original_prototype_unchanged).toBe(true);
}, 120_000);

import { expect, test } from "bun:test";
import { runCaptureCase } from "../qa/cases/capture-native.ts";
import { createRuntimeCapture } from "../src/runtime-capture.ts";

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

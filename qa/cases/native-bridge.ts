import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { attempt, bundle, isolatedEnv, product, senpi, sha256 } from "../command.mjs";
import { commandReceipt, hashes, taskEvidence } from "./receipts.mjs";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";
import { connectEngineBridge } from "../../src/engine-bridge.ts";
import type { BridgeResult, EngineBridge, OutcomeWitness, SenpiNamespace } from "../../src/engine-bridge.ts";
import type { TargetRef } from "../../src/contracts.ts";

type Json = Record<string, unknown>;
type Scenario = "engine-bridge" | "adversarial-native" | "bridge-drift" | "foreign-owner" |
  "mode-absent" | "mode-disabled" | "original";
function value<T>(result: BridgeResult<T>): T {
  assert(result.ok, JSON.stringify(result));
  return result.value;
}
async function bounded<T>(promise: Promise<T>, name: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`BRIDGE_EVENT_DEADLINE: ${name}`)), 20_000);
  });
  try { return await Promise.race([promise, deadline]); } finally { clearTimeout(timer!); }
}
function event(bus: EventEmitter, name: string, predicate: (data: Json) => boolean = () => true) {
  let listener: (data: Json) => void;
  const pending = new Promise<Json>((resolve) => {
    listener = (data) => {
      if (predicate(data)) { bus.off(name, listener); resolve(data); }
    };
    bus.on(name, listener);
  });
  return bounded(pending, name).finally(() => bus.off(name, listener));
}

export async function runBridgeCase(scenario: Scenario): Promise<Json> {
  const artifact = path.join(taskEvidence, `${scenario}-${randomUUID()}.json`);
  if (process.versions.bun) {
    const receipt = await commandReceipt([
      "node", fileURLToPath(import.meta.url), "--scenario", scenario, "--result", artifact,
    ]);
    assert.equal(receipt.exit, 0, `NATIVE_BRIDGE_FAILED: ${receipt.receiptPath}`);
    return { ...JSON.parse(fs.readFileSync(artifact, "utf8")), command_receipt: receipt.receiptPath };
  }
  return nativeCase(scenario, artifact);
}

async function nativeCase(scenario: Scenario, artifact: string): Promise<Json> {
  const sandbox = path.join(taskEvidence, "runs", `${scenario}-${randomUUID()}`);
  const env = isolatedEnv(sandbox);
  const oldEnv = { ...process.env };
  const oldCwd = process.cwd();
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true });
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  process.chdir(cwd);
  const bus = new EventEmitter();
  const bridges: EngineBridge[] = [];
  const sessions: {
    sessionId: string; _disposed?: boolean; waitForIdle(): Promise<void>; dispose(): void;
    extensionRunner?: { emit(event: Json): Promise<unknown>; createContext(): { sessionManager: { getSessionId(): string } };
      getToolDefinition(name: string): { execute(...args: unknown[]): Promise<{ details: { task_id: string } }> } };
  }[] = [];
  const captured = new Map<object, { restore(): void }>();
  const report: Json = { scenario, sandbox, artifact, started_at: new Date().toISOString(),
    source_sha256: hashes(), installed_sha256_before: sha256(bundle), provider_calls: 0,
    fixture_identity: "actual DefaultResourceLoader + AgentSession + registered native task",
    repair_enabled: false };
  let manifest: ReturnType<typeof buildAdapter> | undefined;
  let hooksAtBirth = 0;
  let expectedRestore: unknown;
  try {
    const output = path.join(sandbox, "candidate/omo-task.js");
    manifest = buildAdapter({ output });
    report.adapter_manifest = output + ".manifest.json";
    let loadedBundle = output;
    if (scenario === "bridge-drift") {
      const driftInput = path.join(sandbox, "drift-input.js");
      fs.writeFileSync(driftInput, fs.readFileSync(bundle, "utf8") + "\n// isolated byte drift\n");
      const refusedOutput = path.join(sandbox, "refused/omo-task.js");
      assert.throws(() => buildAdapter({ input: driftInput, output: refusedOutput }), /BUNDLE_DRIFT_REFUSED/);
      assert.equal(fs.existsSync(refusedOutput), false);
      report.drift_refused = true;
      report.refused_candidate_absent = true;
      loadedBundle = bundle;
    }
    if (scenario === "original") loadedBundle = bundle;
    if (scenario === "mode-disabled") process.env.OMO_AGENT_COORDINATION = "0";
    process.env.COORDINATION_CANDIDATE_BUNDLE = loadedBundle;
    fs.writeFileSync(path.join(cwd, ".omo/omo.json"), JSON.stringify({
      agents: { explore: { model: "coordination-qa/offline" } },
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: "in-process",
        default_concurrency: 1, global_concurrency: 1, host_shard_prewarm: "off" },
    }));
    const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const { AssistantMessageEventStream } = await import(pathToFileURL(
      "C:/Users/USER/.bun/install/global/node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js").href);
    const auth = AuthStorage.inMemory();
    const registry = ModelRegistry.inMemory(auth);
    let childCalls = 0;
    let ownerCalls = 0;
    registry.registerProvider("coordination-qa", {
      api: "openai-completions", apiKey: "offline-not-a-credential", baseUrl: "http://offline.invalid",
      models: [{ id: "offline", name: "Bridge offline gate", reasoning: false, input: ["text"],
        contextWindow: 262144, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
      streamSimple: (_model: unknown, context: { messages: unknown[] }) => {
        const stream = new AssistantMessageEventStream();
        const isChild = JSON.stringify(context.messages).includes("BRIDGE_FIXTURE_TASK");
        const n = isChild ? childCalls++ : ownerCalls++;
        const finish = () => {
          const message = { role: "assistant", api: "openai-completions", provider: "coordination-qa",
            model: "offline", content: [{ type: "text", text: isChild ? `bridge-result-${n}` : "owner received completion" }],
            stopReason: "stop", timestamp: Date.now(),
            usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
          stream.push({ type: "start", partial: message });
          stream.push({ type: "done", reason: "stop", message });
          stream.end(message);
        };
        if (isChild) {
          bus.once(`release-${n}`, finish);
          bus.emit("provider-start", { n });
        } else queueMicrotask(finish);
        return stream;
      },
    });
    const loaderFor = () => new sdk.DefaultResourceLoader({
      cwd, agentDir: env.OMO_CODING_AGENT_DIR, noSkills: true, noPromptTemplates: true,
      noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory({
        packages: ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg"],
        extensions: [path.join(product, "qa/cases/fixture-extension.js")],
      }),
    });
    const createRoot = async () => {
      const loader = loaderFor();
      await loader.reload();
      assert.deepEqual(loader.getExtensions().errors, [], "ACTUAL_EXTENSION_LOADER_FAILED");
      const root = (await sdk.createAgentSession({
        cwd, agentDir: env.OMO_CODING_AGENT_DIR, authStorage: auth, modelRegistry: registry,
        model: registry.find("coordination-qa", "offline"), resourceLoader: loader,
        sessionManager: sdk.SessionManager.inMemory(cwd),
      })).session;
      sessions.push(root);
      await root.bindExtensions({ mode: "print" });
      const state = (globalThis as unknown as Record<symbol, {
        registered: boolean; context: { sessionManager: { getSessionId(): string } };
      }>)[Symbol.for("coordination.qa.bridge.fixture")];
      assert(state.registered, "NATIVE_COMPONENT_NOT_REGISTERED_BY_LOADER");
      assert(state.context, "NATIVE_ROOT_SESSION_START_NOT_OBSERVED");
      assert.equal(state.context.sessionManager, root.extensionRunner.createContext().sessionManager);
      return { root, context: state.context, loader };
    };
    const first = await createRoot();
    const beforeHook = sdk.AgentSession.prototype.bindExtensions;
    expectedRestore = beforeHook;
    const holder = (globalThis as unknown as Record<symbol, { module?: unknown }>)[
      Symbol.for("omo.senpi-task.senpiBarrel")];
    const descriptor = holder && Object.getOwnPropertyDescriptor(holder, "module");
    const moduleReady = (namespace: SenpiNamespace) => {
      const proto = namespace.AgentSession.prototype;
      if (captured.has(proto)) return;
      report.namespace_hook_ready_before_child = childCalls === 0;
      const previous = proto.bindExtensions;
      proto.bindExtensions = async function (this: typeof sessions[number], ...args: unknown[]) {
        const result = await Reflect.apply(previous, this, args);
        sessions.push(this);
        hooksAtBirth++;
        return result;
      };
      captured.set(proto, { restore() { proto.bindExtensions = previous; } });
    };
    const enabled = ["engine-bridge", "adversarial-native", "foreign-owner"].includes(scenario);
    let bridge: EngineBridge | undefined;
    if (enabled) {
      bridge = value(connectEngineBridge({
        context: first.context, generation: 1, loaderNamespace: sdk, onModuleReady: moduleReady,
      }));
      bridges.push(bridge);
      assert.deepEqual(Object.keys(bridge).sort(), [
        "listOwned", "inspectOwned", "bindActualRuntime", "subscribeOutcome", "reconcileOwnedTerminal", "dispose",
      ].sort());
      const failedReadiness = connectEngineBridge({
        context: first.context, generation: 1, loaderNamespace: sdk,
        onModuleReady() { throw new Error("FIXTURE_MODULE_READY_REFUSED"); },
      });
      assert.equal(failedReadiness.ok, false);
      assert(!failedReadiness.ok && failedReadiness.reason === "connection_failed");
      report.failed_connection_reported = failedReadiness;
    } else if (scenario !== "mode-absent") {
      const rejected = connectEngineBridge({ context: first.context, generation: 1, loaderNamespace: sdk });
      assert.equal(rejected.ok, false, "UNSUPPORTED_CONNECTION_MUST_NOT_REPORT_SUCCESS");
      report.connection_refusal = rejected;
    }
    const rawCarrier = (globalThis as unknown as Record<symbol, {
      contexts: WeakMap<object, WeakRef<{
        manager: { get(id: string): { status: string; final_response?: string; notification: { run_epoch: number; notified_epoch: number } };
          getResidentHandle(id: string): unknown; waitFor(id: string): Promise<unknown> };
        lifecycle: object; notifier: object;
      }>>;
      links: WeakMap<object, { store: { load(id: string): unknown; stateDir: string }; active: boolean }>;
      ports: WeakMap<object, { store: object; liveHandle(id: string): unknown }>;
      runtimeOwners: WeakMap<object, { task_id: string; runtime: WeakRef<object> }>;
      module?: unknown; moduleListeners: Set<unknown>;
    }>)[Symbol.for("omo.agent-coordination.native.v1")];
    const native = await import(pathToFileURL(loadedBundle).href);
    const reader = native.createTaskRecordStore({
      project_dir: cwd, task: { state_dir: path.join(sandbox, "senpi-task") },
    });
    assert(path.resolve(reader.stateDir).startsWith(path.resolve(sandbox) + path.sep));
    report.resolved_task_store = reader.stateDir;
    report.loaded_bundle_sha256 = sha256(loadedBundle);
    report.native_loader_paths = first.loader.getExtensions().extensions.map((extension: { path: string }) => extension.path);
    const start = async (root: typeof first.root, n: number) => {
      const provider = event(bus, "provider-start", (data) => data.n === n);
      const tool = root.extensionRunner.getToolDefinition("task");
      assert(tool, "ACTUAL_REGISTERED_TASK_TOOL_MISSING");
      const launched = await tool.execute(randomUUID(), {
        subagent_type: "explore", model: "coordination-qa/offline",
        prompt: "BRIDGE_FIXTURE_TASK: Return the offline result.", description: "Bridge owned proof",
        run_in_background: true,
      }, undefined, undefined, root.extensionRunner.createContext());
      await provider;
      return launched.details.task_id as string;
    };
    const id = await start(first.root, 0);
    const recordPath = path.join(reader.stateDir, "tasks", `${id}.json`);
    report.task_id = id;
    report.hooks_at_first_birth = hooksAtBirth;
    let target: TargetRef | undefined;
    let outcome: Promise<OutcomeWitness> | undefined;
    const engine = rawCarrier?.contexts.get(first.context.sessionManager)?.deref();
    if (bridge) {
      assert(engine, "BRIDGE_NOT_BOUND_TO_NATIVE_ENGINE");
      const store = rawCarrier.links.get(engine)!.store;
      const ports = rawCarrier.ports.get(store)!;
      assert.equal(ports.store, store);
      assert.equal(ports.liveHandle(id), engine.manager.getResidentHandle(id));
      const running = value(bridge.listOwned());
      assert.equal(running.length, 1);
      target = running[0]!.identity.target_ref;
      assert.equal(running[0]!.slot, "held");
      assert.equal(running[0]!.identity.owner_root_id.replace(/^senpi:/, ""), first.root.sessionId);
      const actual = sessions.find((session) => session.sessionId === running[0]!.identity.session_id);
      assert(actual, "ACTUAL_CHILD_BIRTH_NOT_CAPTURED");
      const handle = engine.manager.getResidentHandle(id);
      assert(typeof handle === "object" && handle !== null);
      const nativeRuntimeOwner = rawCarrier.runtimeOwners.get(handle);
      assert.equal(nativeRuntimeOwner?.task_id, id);
      assert.equal(nativeRuntimeOwner?.runtime.deref(), actual, "NATIVE_HANDLE_RUNTIME_CAPTURE_MISMATCH");
      report.native_runtime_owner = { same_actual_session_object: true, task_id: id,
        same_manager_outcome_handle: ports.liveHandle(id) === handle,
        source: "R8 eF session parameter -> runner handle -> R6/R8 BB ManagedChildHandle" };
      const beforeBinding = sha256(recordPath);
      const failedBindings = [];
      for (const [receiver, forged] of [
        ["plain_object", { sessionId: actual.sessionId, _disposed: true }],
        ["prototype_only", Object.defineProperty(Object.create(sdk.AgentSession.prototype),
          "sessionId", { value: actual.sessionId })],
      ] as const) {
        const failed = Reflect.apply(sdk.AgentSession.prototype.bindExtensions, forged, [{ mode: "print" }]);
        const rejected = assert.rejects(Promise.resolve(failed), TypeError);
        const whilePending = bridge.bindActualRuntime(target, forged);
        await rejected;
        const afterRejection = bridge.bindActualRuntime(target, forged);
        assert.deepEqual(afterRejection, { ok: false, reason: "runtime_mismatch" },
          "FAILED_NATIVE_BINDING_TRUSTED_FABRICATED_RUNTIME");
        assert.deepEqual(whilePending, { ok: false, reason: "runtime_mismatch" },
          "PENDING_NATIVE_BINDING_TRUSTED_FABRICATED_RUNTIME");
        assert.equal(value(bridge.inspectOwned(target)).identity.runtime_instance_id, null);
        assert.equal(sha256(recordPath), beforeBinding, "FAILED_BINDING_MUTATED_NATIVE_RECORD");
        failedBindings.push({ receiver, native_instance: forged instanceof sdk.AgentSession,
          native_rejected: true, while_pending: whilePending, after_rejection: afterRejection,
          runtime_unbound: true, mutation_count: 0 });
      }
      report.failed_native_bindings = failedBindings;
      // A genuine, successfully bound SDK instance can share the real child's
      // SessionManager and ID without being the session owned by its runner.
      const decoyLoader = new sdk.DefaultResourceLoader({
        cwd, agentDir: env.OMO_CODING_AGENT_DIR, noSkills: true, noPromptTemplates: true,
        noThemes: true, noContextFiles: true, settingsManager: sdk.SettingsManager.inMemory({}),
      });
      await decoyLoader.reload();
      const decoy = (await sdk.createAgentSession({
        cwd, agentDir: env.OMO_CODING_AGENT_DIR, authStorage: auth, modelRegistry: registry,
        model: registry.find("coordination-qa", "offline"), resourceLoader: decoyLoader,
        sessionManager: Reflect.get(actual, "sessionManager"),
      })).session;
      sessions.push(decoy);
      await decoy.bindExtensions({ mode: "print" });
      assert.notEqual(decoy, actual);
      assert.equal(decoy.sessionId, actual.sessionId);
      assert.equal(decoy.sessionManager, Reflect.get(actual, "sessionManager"));
      assert(decoy instanceof sdk.AgentSession);
      const isolatedBinding = value(connectEngineBridge({
        context: first.context, generation: 1, loaderNamespace: sdk,
      }));
      bridges.push(isolatedBinding);
      const decoyTarget = value(isolatedBinding.listOwned())
        .find((snapshot) => snapshot.identity.task_id === id)!.identity.target_ref;
      const beforeDecoy = sha256(recordPath);
      const decoyResult = isolatedBinding.bindActualRuntime(decoyTarget, decoy);
      report.unrelated_native_runtime = {
        distinct_object: decoy !== actual, same_session_id: decoy.sessionId === actual.sessionId,
        same_session_manager: decoy.sessionManager === Reflect.get(actual, "sessionManager"),
        native_instance: decoy instanceof sdk.AgentSession, native_binding_fulfilled: true,
        target_child_runtime: false, binding_result: decoyResult,
        native_record_unchanged: sha256(recordPath) === beforeDecoy,
      };
      assert.deepEqual(decoyResult, { ok: false, reason: "runtime_mismatch" },
        "UNRELATED_NATIVE_RUNTIME_TRUSTED");
      assert.equal(value(isolatedBinding.inspectOwned(decoyTarget)).identity.runtime_instance_id, null);
      const actualResult = isolatedBinding.bindActualRuntime(decoyTarget, actual);
      assert(actualResult.ok, "NATIVE_OWNER_RUNTIME_REFUSED");
      assert(actualResult.value.runtime_instance_id);
      assert.deepEqual(isolatedBinding.bindActualRuntime(decoyTarget, decoy), decoyResult);
      assert.equal(sha256(recordPath), beforeDecoy, "DECOY_BINDING_MUTATED_NATIVE_RECORD");
      report.actual_native_runtime = { binding_result: actualResult,
        stable_instance_id: value(isolatedBinding.bindActualRuntime(decoyTarget, actual))
          .runtime_instance_id === actualResult.value.runtime_instance_id };
      isolatedBinding.dispose();
      assert.equal(value(bridge.bindActualRuntime(target, actual)).runtime_instance_id !== null, true);
      assert.deepEqual(bridge.bindActualRuntime(target, { sessionId: actual.sessionId }), {
        ok: false, reason: "runtime_mismatch",
      });
      report.running = value(bridge.inspectOwned(target));
      report.same_native_objects = { manager_handle: true, outcome_store: true,
        lifecycle: typeof engine.lifecycle === "object", notifier: typeof engine.notifier === "object" };
      const before = sha256(recordPath);
      const wrongOwner = connectEngineBridge({
        context: { sessionManager: { getSessionId: () => first.root.sessionId } },
        generation: 1, loaderNamespace: sdk,
      });
      assert.equal(wrongOwner.ok, false);
      assert.deepEqual(bridge.inspectOwned("unknown" as TargetRef), { ok: false, reason: "invalid_target" });
      assert.deepEqual(bridge.reconcileOwnedTerminal(target), { ok: false, reason: "repair_not_enabled" });
      assert.equal(sha256(recordPath), before, "REFUSAL_MUTATED_NATIVE_RECORD");
      report.unknown_owner_refusal = wrongOwner;
      report.refusal_mutation_count = 0;
      let staleOutcomeEvents = 0;
      value(bridge.subscribeOutcome(target, () => { staleOutcomeEvents++; }));
      const late = value(connectEngineBridge({ context: first.context, generation: 2, loaderNamespace: sdk }));
      bridges.push(late);
      assert.deepEqual(bridge.inspectOwned(target), { ok: false, reason: "stale_generation" });
      const next = value(late.listOwned())[0]!;
      assert.equal(value(late.bindActualRuntime(next.identity.target_ref, actual)).runtime_instance_id !== null, true);
      // The stale listener is removed explicitly; it must never be credited as a terminal witness.
      bridge.dispose();
      bridge = late;
      target = next.identity.target_ref;
      outcome = bounded(new Promise<OutcomeWitness>((resolve) => {
        value(late.subscribeOutcome(target!, resolve));
      }), "current generation outcome");
      report.late_attach_same_handle = next.identity.task_id === id;
      report.stale_generation_refused = true;
      report.stale_outcome_events = () => staleOutcomeEvents;
    }
    if (scenario === "foreign-owner") {
      const second = await createRoot();
      const other = value(connectEngineBridge({ context: second.context, generation: 1, loaderNamespace: sdk }));
      bridges.push(other);
      const otherId = await start(second.root, 1);
      const foreign = value(other.listOwned()).find((snapshot) => snapshot.identity.task_id === otherId)!;
      const otherPath = path.join(reader.stateDir, "tasks", `${otherId}.json`);
      const before = [sha256(recordPath), sha256(otherPath)];
      assert.equal(value(bridge!.listOwned()).some((s) => s.identity.task_id === otherId), false);
      assert.deepEqual(bridge!.inspectOwned(foreign.identity.target_ref), { ok: false, reason: "invalid_target" });
      assert.deepEqual(bridge!.reconcileOwnedTerminal(foreign.identity.target_ref), { ok: false, reason: "invalid_target" });
      assert.deepEqual([sha256(recordPath), sha256(otherPath)], before);
      const otherEngine = rawCarrier.contexts.get(second.context.sessionManager)!.deref()!;
      const complete = bounded(otherEngine.manager.waitFor(otherId), "foreign original completion");
      bus.emit("release-1");
      await complete;
      assert.equal(reader.load(otherId).final_response, "bridge-result-1");
      report.foreign_owner = { refused: true, mutation_count: 0, original_task_unharmed: true,
        distinct_native_engines: otherEngine !== engine, task_id: otherId };
    }
    let queuedId: string | undefined;
    let queuedProvider: Promise<Json> | undefined;
    if (scenario === "engine-bridge") {
      queuedProvider = event(bus, "provider-start", (data) => data.n === 1);
      const queued = await first.root.extensionRunner.getToolDefinition("task").execute(randomUUID(), {
        subagent_type: "explore", model: "coordination-qa/offline",
        prompt: "BRIDGE_FIXTURE_TASK: Queued snapshot control.", description: "Queued bridge proof",
        run_in_background: true,
      }, undefined, undefined, first.root.extensionRunner.createContext());
      queuedId = queued.details.task_id;
      const snapshot = value(bridge!.listOwned()).find((s) => s.identity.task_id === queuedId)!;
      assert.equal(snapshot.status, "pending");
      assert.equal(snapshot.identity.handle_id, null);
      assert.equal(snapshot.identity.session_id, null);
      assert.equal(bridge!.subscribeOutcome(snapshot.identity.target_ref, () => {}).ok, false);
      report.queued_snapshot = snapshot;
    }
    // For baseline/no-mode cases observe the actual store terminal edge before releasing the child.
    const terminalLedger = (globalThis as unknown as Record<symbol, {
      subscribe(listener: (edge: { record: { task_id: string; status: string } }) => void): () => void;
    }>)[Symbol.for("omo.task.terminalObservers")];
    let detach: () => void = () => {};
    const completed = bounded(new Promise<void>((resolve) => {
      detach = terminalLedger.subscribe(({ record }) => {
        if (record.task_id === id && record.status === "completed") { detach(); resolve(); }
      });
    }), "original native completion");
    bus.emit("release-0");
    await completed;
    if (outcome) report.outcome = await outcome;
    if (typeof report.stale_outcome_events === "function") {
      report.stale_outcome_events = report.stale_outcome_events();
      assert.equal(report.stale_outcome_events, 0);
    }
    if (queuedId && queuedProvider) {
      await queuedProvider;
      const done = bounded(engine!.manager.waitFor(queuedId), "queued native completion");
      bus.emit("release-1");
      await done;
      report.queued_promoted_normally = reader.load(queuedId).status === "completed";
    }
    const terminal = reader.load(id);
    assert.equal(terminal.status, "completed");
    assert.equal(terminal.final_response, "bridge-result-0");
    assert.equal(terminal.notification.notified_epoch, terminal.notification.run_epoch);
    if (bridge && target) {
      report.terminal = value(bridge.inspectOwned(target));
      assert.equal((report.terminal as { slot: string }).slot, "released");
      // Native revival changes the epoch on the same handle. An old target can never repair it.
      const provider = event(bus, "provider-start", (data) => data.n === childCalls - 1);
      const oldEpoch = terminal.notification.run_epoch;
      const send = first.root.extensionRunner.getToolDefinition("task_send");
      await send.execute(randomUUID(), { to: id, message: "BRIDGE_FIXTURE_TASK: Second epoch." },
        undefined, undefined, first.root.extensionRunner.createContext());
      await provider;
      const revived = reader.load(id);
      assert.equal(revived.notification.run_epoch, oldEpoch + 1);
      const before = sha256(recordPath);
      assert.deepEqual(bridge.inspectOwned(target), { ok: false, reason: "stale_epoch" });
      assert.deepEqual(bridge.bindActualRuntime(target, sessions.find((session) =>
        session.sessionId === terminal.child_session_id)!), { ok: false, reason: "stale_epoch" });
      assert.deepEqual(bridge.reconcileOwnedTerminal(target), { ok: false, reason: "stale_epoch" });
      assert.equal(sha256(recordPath), before);
      const renewed = value(bridge.listOwned()).find((snapshot) => snapshot.identity.task_id === id)!;
      assert.equal(renewed.identity.run_epoch, oldEpoch + 1);
      const renewedRuntime = sessions.find((session) => session.sessionId === renewed.identity.session_id);
      assert(renewedRuntime);
      const renewedBinding = value(bridge.bindActualRuntime(renewed.identity.target_ref, renewedRuntime));
      assert.equal(renewedBinding.run_epoch, oldEpoch + 1);
      report.current_epoch_runtime_bound = true;
      const finish = bounded(engine!.manager.waitFor(id), "native revived completion");
      bus.emit(`release-${childCalls - 1}`);
      await finish;
      report.stale_epoch_refused = true;
    }
    // Under concurrency one, a second real child can only start after the native slot is returned.
    const nextNumber = childCalls;
    const nextId = await start(first.root, nextNumber);
    let stopNext: () => void = () => {};
    const nextDone = bounded(new Promise<void>((resolve) => {
      stopNext = terminalLedger.subscribe(({ record }) => {
        if (record.task_id === nextId && record.status === "completed") { stopNext(); resolve(); }
      });
    }), "native slot reuse");
    bus.emit(`release-${nextNumber}`);
    await nextDone;
    assert.equal(reader.load(nextId).final_response, `bridge-result-${nextNumber}`);
    await bounded(first.root.waitForIdle(), "owner native notification drain");
    const afterDescriptor = holder && Object.getOwnPropertyDescriptor(holder, "module");
    assert.deepEqual(afterDescriptor, descriptor, "HOLDER_MODULE_DESCRIPTOR_CHANGED");
    report.barrel = { same_namespace: rawCarrier?.module === sdk,
      same_AgentSession: (holder?.module as SenpiNamespace | undefined)?.AgentSession === sdk.AgentSession,
      descriptor_preserved: true };
    report.original_task_unharmed = true;
    report.normal_completion = { status: terminal.status, final_response: terminal.final_response,
      run_epoch: terminal.notification.run_epoch, notified_epoch: terminal.notification.notified_epoch,
      slot_reused: true };
    report.child_provider_calls = childCalls;
    report.owner_provider_calls = ownerCalls;
    report.provider_calls = childCalls + ownerCalls;
    if (!enabled) assert.equal(sdk.AgentSession.prototype.bindExtensions, beforeHook);
    report.mode_absent_noop = !enabled;
    const removal = removeAdapter(manifest);
    assert.equal(removal.restored_sha256, sha256(bundle));
    report.removal = removal;
    const removeDrift = { ...manifest, output: path.join(sandbox, "removal-drift.js") };
    fs.writeFileSync(removeDrift.output, "not the candidate");
    assert.throws(() => removeAdapter(removeDrift), /REMOVAL_DRIFT_REFUSED/);
    report.removal_drift_refused = true;
    report.passed = true;
    return report;
  } catch (error) {
    report.passed = false;
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    throw error;
  } finally {
    // Drain gates in failures too; never strand an owned offline provider.
    for (let n = 0; n < 8; n++) bus.emit(`release-${n}`);
    for (const capture of captured.values()) capture.restore();
    for (const bridge of bridges) bridge.dispose();
    for (const session of [...new Set(sessions)].reverse()) {
      if (session._disposed) continue;
      await bounded(session.waitForIdle(), "cleanup session drain");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    }
    if (manifest && sha256(manifest.output) === manifest.candidate_sha256) {
      report.failure_removal = removeAdapter(manifest);
    }
    const carrier = (globalThis as unknown as Record<symbol, { moduleListeners: Set<unknown> }>)[
      Symbol.for("omo.agent-coordination.native.v1")];
    const sdkAgain = await import(pathToFileURL(senpi + "/dist/index.js").href);
    report.cleanup = {
      sessions_disposed: [...new Set(sessions)].every((session) => session._disposed === true),
      module_subscriptions: carrier?.moduleListeners.size ?? 0,
      prototype_restored: expectedRestore === undefined || sdkAgain.AgentSession.prototype.bindExtensions === expectedRestore,
      provider_gates: bus.eventNames().length, candidate_only: true,
    };
    assert.equal(report.installed_sha256_before, sha256(bundle), "INSTALLED_BUNDLE_CHANGED");
    report.installed_sha256_after = sha256(bundle);
    report.finished_at = new Date().toISOString();
    fs.mkdirSync(path.dirname(artifact), { recursive: true });
    fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
    process.chdir(oldCwd);
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, oldEnv);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const scenario = process.argv[process.argv.indexOf("--scenario") + 1] as Scenario;
  const result = process.argv[process.argv.indexOf("--result") + 1]!;
  const rel = path.relative(taskEvidence, path.resolve(result));
  assert(!path.isAbsolute(rel) && !rel.startsWith(".."), "RESULT_OUTSIDE_TASK_EVIDENCE");
  const report = await nativeCase(scenario, result);
  console.log(JSON.stringify({ scenario, passed: report.passed, artifact: result }));
}

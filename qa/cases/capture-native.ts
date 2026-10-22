import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";
import { createRuntimeCapture } from "../../src/runtime-capture.ts";
import { getCoordinationBroker } from "../../src/public-api.ts";
import type { LiveSession, CaptureResult } from "../../src/runtime-capture.ts";
import type { CaptureExtensionState } from "../../src/extension.ts";
import { attempt, product, senpi, bundle, isolatedEnv, sha256 } from "../command.mjs";
import { captureCommand, captureEvidence, captureHashes } from "./capture-receipts.ts";

type Case = "capture-in-process" | "capture-process" | "capture-unsupported" | "capture-reload";
type Json = Record<string, any>;
const stateKey = Symbol.for("omo.agent-coordination.capture.v1");
function value<T>(result: CaptureResult<T>): T {
  assert(result.ok, JSON.stringify(result));
  return result.value;
}
export async function bounded<T>(pending: Promise<T>, name: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([pending, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`CAPTURE_EVENT_DEADLINE: ${name}`)), 30_000);
    })]);
  } finally { clearTimeout(timer!); }
}
export async function runCaptureCase(name: Case): Promise<Json> {
  const id = `${name}-${randomUUID()}`;
  const sandbox = path.join(captureEvidence, "runs", id);
  fs.mkdirSync(sandbox, { recursive: true });
  const packageDir = path.join(sandbox, "capture-package");
  fs.mkdirSync(packageDir);
  // Bun builds one real ESM extension, not an .mjs re-export or an SDK copy.
  const build = await Bun.build({
    entrypoints: [path.join(product, "src/extension.ts")], outdir: packageDir,
    naming: "extension.js", target: "node", format: "esm", external: ["@code-yeongyu/senpi"],
  });
  assert(build.success, String(build.logs));
  assert.equal(build.outputs.length, 1);
  const fixtureBuild = await Bun.build({
    entrypoints: [path.join(product, "qa/cases/capture-fixture.ts")], outdir: packageDir,
    naming: "fixture.js", target: "node", format: "esm",
    external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"],
  });
  assert(fixtureBuild.success, String(fixtureBuild.logs));
  assert.equal(fixtureBuild.outputs.length, 1);
  fs.writeFileSync(path.join(packageDir, "package.json"), JSON.stringify({
    name: "coordination-capture-owned-candidate", type: "module",
    pi: { extensions: ["./extension.js", "./fixture.js"] },
  }));
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules",
    path.join(packageDir, "node_modules"), "junction");
  const artifact = path.join(captureEvidence, `${id}.json`);
  let run: Awaited<ReturnType<typeof captureCommand>>;
  try {
    run = await captureCommand(["node", fileURLToPath(import.meta.url),
      "--native-case", name, "--sandbox", sandbox, "--result", artifact]);
  } finally {
    const link = path.join(packageDir, "node_modules");
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
  }
  assert.equal(run.exit, 0, `REAL_CAPTURE_CASE_FAILED: ${run.receiptPath}`);
  const result = JSON.parse(fs.readFileSync(artifact, "utf8"));
  assert.equal(result.passed, true);
  fs.writeFileSync(path.join(captureEvidence, name === "capture-unsupported" ? "unsupported.json" : `${name}.json`),
    JSON.stringify({ ...result, command_receipt: run.receiptPath }, null, 2) + "\n");
  return result;
}

async function nativeCase(name: Case, sandbox: string, artifact: string) {
  assert(path.resolve(sandbox).startsWith(path.resolve(captureEvidence) + path.sep));
  const env = isolatedEnv(sandbox);
  // Preserve inherited JS instrumentation; the guard also instruments every native child.
  const guardOptions = process.env.NODE_OPTIONS;
  const previousEnv = { ...process.env };
  const previousCwd = process.cwd();
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  if (guardOptions) process.env.NODE_OPTIONS = guardOptions;
  // Official SDK configuration relocates install metadata without replacing
  // code. Keep Windows startup quarantine cleanup away from global node_modules.
  const sdkMetadata = path.join(sandbox, "sdk-package-metadata");
  fs.mkdirSync(sdkMetadata, { recursive: true });
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(sdkMetadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = sdkMetadata;
  // Exercise the native runner's original Node/SDK fallback, which supports
  // inherited JS instrumentation; do not launch an uninstrumentable compiled exe.
  const pathKey = Object.keys(process.env).find((key) => key.toLowerCase() === "path") ?? "PATH";
  process.env[pathKey] = `${path.dirname(process.execPath)};${process.env.SystemRoot}/System32`;
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true });
  process.chdir(cwd);
  const packageDir = path.join(sandbox, "capture-package");
  const entry = path.join(packageDir, "extension.js");
  const bus = new EventEmitter();
  const global = globalThis as unknown as Record<symbol, any>;
  global[Symbol.for("coordination.capture.qa.bus")] = bus;
  const sessions: any[] = [];
  let manifest: ReturnType<typeof buildAdapter> | undefined;
  let sdk: any;
  let originalBind: any;
  let originalDispose: any;
  let priorWrapper: any;
  let priorCalls = 0;
  let processHandle: any;
  let detachTerminal = () => {};
  const report: Json = { case: name, sandbox, artifact, started_at: new Date().toISOString(),
    source_sha256: captureHashes(), extension: { entry, sha256: sha256(entry), single_esm_js: true },
    installed_sha256_before: sha256(bundle), provider_fixture: "offline scripted provider; native engine and SDK unchanged",
    repair_enabled: false };
  report.sdk_package_override = { root: sdkMetadata,
    metadata_sha256: sha256(path.join(sdkMetadata, "package.json")),
    actual_sdk_entry: senpi + "/dist/index.js", copied_runtime_code: false };
  try {
    manifest = buildAdapter({
      ...(name === "capture-reload" ? { input: path.join(attempt, "baseline-5140/omo-task.js") } : {}),
      output: path.join(sandbox, "candidate/omo-task.js"),
    });
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
    const mode = name === "capture-process" ? "process" : "in-process";
    const packages = ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg", packageDir];
    // Native curated readers intentionally force in-process. A normal configured
    // worker is required to exercise the actual separate-process runner.
    const role = mode === "process" ? "capture-worker" : "explore";
    // The native package resolver reads these isolated settings to propagate pi.extensions.
    fs.writeFileSync(path.join(env.OMO_CODING_AGENT_DIR!, "settings.json"), JSON.stringify({
      packages,
    }));
    fs.writeFileSync(path.join(cwd, ".omo/omo.json"), JSON.stringify({
      agents: { [role]: { description: "Isolated capture worker", prompt: "Return the test result.",
        model: "coordination-capture/offline", execution_mode: mode } },
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: mode,
        process_runner: "child-process", host_shard_prewarm: "off",
        default_concurrency: 1, global_concurrency: 1 },
    }));
    sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    assert.equal(path.resolve(sdk.getPackageDir()), path.resolve(sdkMetadata), "SDK_PACKAGE_OVERRIDE_NOT_APPLIED");
    originalBind = sdk.AgentSession.prototype.bindExtensions;
    originalDispose = sdk.AgentSession.prototype.dispose;
    priorWrapper = async function (this: LiveSession, ...args: unknown[]) {
      priorCalls++;
      return Reflect.apply(originalBind, this, args);
    };
    sdk.AgentSession.prototype.bindExtensions = priorWrapper;
    const loader = new sdk.DefaultResourceLoader({
      cwd, agentDir: env.OMO_CODING_AGENT_DIR, noSkills: true, noPromptTemplates: true,
      noThemes: true, noContextFiles: true, settingsManager: sdk.SettingsManager.inMemory({
        packages,
      }),
    });
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, [], "CAPTURE_ACTUAL_LOADER_FAILED");
    report.extension.loader_paths = loader.getExtensions().extensions.map((e: any) => e.path);
    assert(report.extension.loader_paths.includes(entry), "PACKAGE_EXTENSION_NOT_LOADED");
    const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const auth = AuthStorage.inMemory();
    const registry = ModelRegistry.inMemory(auth);
    const root = (await sdk.createAgentSession({
      cwd, agentDir: env.OMO_CODING_AGENT_DIR, resourceLoader: loader, authStorage: auth,
      modelRegistry: registry, sessionManager: sdk.SessionManager.inMemory(cwd),
    })).session;
    sessions.push(root);
    await root.bindExtensions({ mode: "print",
      ...(name === "capture-reload" ? { shutdownHandler: async () => {} } : {}),
    });
    await root.setModel(registry.find("coordination-capture", "offline"));
    const state = global[stateKey] as CaptureExtensionState;
    assert(state?.capture.status.ok, "ROOT_CAPTURE_NOT_ACTIVE");
    const parentCapture = value(state.capture.forRuntime(root));
    assert.equal(parentCapture.runtime(), root);
    assert.equal(value(parentCapture.read()).model_runtime, root.modelRuntime);
    const bridge = state.bridges.get(root.sessionManager);
    assert(bridge, `NATIVE_OWNER_CONNECTION_MISSING: ${[...state.unsupported]}`);
    if (name === "capture-reload") {
      // Given a natively bound root, exercise the real no-cache Node loader.
      const previousGeneration = state.generation;
      // When extensions rebuild without another bindExtensions call.
      await root.reload();
      const next = global[stateKey] as CaptureExtensionState;
      const captured = next.capture.forRuntime(root);
      const context = root.extensionRunner.createContext();
      const inspect = await root.extensionRunner.getToolDefinition("coordination_inspect")
        .execute(randomUUID(), {}, undefined, undefined, context);
      report.reload = { generation_before: previousGeneration, generation_after: next.generation,
        captured: captured.ok, bridge: next.bridges.has(root.sessionManager), inspect: inspect.details };
      // Then the new generation owns the same runtime and exposes the public API.
      assert.equal(next.generation, previousGeneration + 1);
      assert.equal(captured.ok, true, `RELOAD_CAPTURE_MISSING: ${JSON.stringify(report.reload)}`);
      assert(next.bridges.has(root.sessionManager), "RELOAD_OWNER_CONNECTION_MISSING");
      assert.equal(inspect.details.ok, true, JSON.stringify(inspect.details));
      await bounded(root.prompt("CAPTURE_RELOAD_NEXT_TURN"), "post-reload root turn");
      const broker = getCoordinationBroker();
      assert(broker.ok, JSON.stringify(broker));
      assert.equal(broker.value.version, 1);
      const client = broker.value.getClient(root.extensionRunner.createContext());
      assert(client.ok, JSON.stringify(client));
      assert.equal(client.value.peers().ok, true);
      client.value.dispose();
      report.reload.next_turn_api_version = broker.value.version;
      report.reload.next_turn_connected = true;
      // Use the installed Collab controller's actual status projection without
      // starting a viewer or touching any live pane/status file.
      const dashboard = "C:/Users/USER/.omo-stable-500/home/.omo/agent/herdr-dashboard/integration";
      const current = JSON.parse(fs.readFileSync(path.join(dashboard, "current.json"), "utf8"));
      const controller = path.join(dashboard, current.generation, "coordination/src/controller.mjs");
      const { connectionStatus } = await import(pathToFileURL(controller).href);
      report.reload.collab = await connectionStatus();
      report.reload.collab_controller_sha256 = sha256(controller);
      assert.deepEqual(report.reload.collab, { state: "connected", protocol: "api v1", baseline: null });
      report.passed = true;
      return report;
    }
    const holder = global[Symbol.for("omo.senpi-task.senpiBarrel")];
    const descriptor = holder && Object.getOwnPropertyDescriptor(holder, "module");
    const moduleReadyBeforeChild = state.capture.list().length === 1;
    const native = await import(pathToFileURL(manifest.output).href);
    const store = native.createTaskRecordStore({ project_dir: cwd,
      task: { state_dir: path.join(sandbox, "senpi-task") } });
    assert(path.resolve(store.stateDir).startsWith(path.resolve(sandbox) + path.sep));
    if (name === "capture-unsupported") {
      // No successful mode is left behind to make an unsupported case look healthy.
      state.dispose();
      assert.equal(sdk.AgentSession.prototype.bindExtensions, priorWrapper);
      const refused = [
        createRuntimeCapture({ loaderNamespace: { ...sdk, VERSION: "future" }, generation: 20 }),
        createRuntimeCapture({ loaderNamespace: { VERSION: sdk.VERSION,
          AgentSession: { prototype: {} } } as any, generation: 20 }),
      ];
      const mismatch = createRuntimeCapture({ loaderNamespace: sdk, generation: 20 });
      const classResult = mismatch.moduleReady({ ...sdk, AgentSession: class extends sdk.AgentSession {} });
      assert(!classResult.ok && classResult.connection === "class_identity");
      for (const item of refused) assert(!item.status.ok);
      mismatch.dispose();
      refused.forEach((item) => item.dispose());
      assert.equal(sdk.AgentSession.prototype.bindExtensions, priorWrapper);
      report.refusals = ["version", "shape", "class_identity"];
      report.original_prototype_unchanged = true;
    }
    const terminal = global[Symbol.for("omo.task.terminalObservers")];
    let launchedId: string | undefined;
    const terminalRecords = new Map<string, any>();
    let resolveTerminal: (record: any) => void;
    const completed = new Promise<any>((resolve) => {
      resolveTerminal = resolve;
      detachTerminal = terminal.subscribe(({ record }: any) => {
        if (["completed", "error", "cancelled", "interrupted", "lost"].includes(record.status)) {
          terminalRecords.set(record.task_id, record);
          if (record.task_id === launchedId) { detachTerminal(); resolve(record); }
        }
      });
    });
    // Process children return immediately; their exact terminal may precede task.execute's return.
    const provider = mode === "in-process" ? new Promise<void>((resolve) => {
      bus.once("provider-start", () => resolve());
    }) : undefined;
    const launched = await root.extensionRunner.getToolDefinition("task").execute(randomUUID(), {
      subagent_type: role, model: "coordination-capture/offline",
      prompt: "CAPTURE_FIXTURE_CHILD: Return the isolated capture result.",
      description: "Owned native capture", run_in_background: true,
    }, undefined, undefined, root.extensionRunner.createContext());
    launchedId = launched.details.task_id;
    report.launch = launched;
    assert(launchedId, JSON.stringify(launched));
    if (mode === "process") {
      const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
      processHandle = carrier.contexts.get(root.sessionManager)?.deref()?.manager.getResidentHandle(launchedId);
    }
    if (terminalRecords.has(launchedId)) {
      detachTerminal();
      resolveTerminal!(terminalRecords.get(launchedId));
    }
    if (provider) await bounded(provider, "child provider gate");
    if (name === "capture-unsupported") {
      assert.equal(state.capture.list().length, 0);
      assert.equal(sdk.AgentSession.prototype.bindExtensions, priorWrapper);
      bus.emit("release-child");
    } else {
      const owned = bridge.listOwned();
      assert(owned.ok, JSON.stringify(owned));
      const target = owned.value.find((item) => item.identity.task_id === launchedId)!;
      assert(target, "LAUNCHED_TASK_NOT_OWNED");
      if (mode === "in-process") {
        const childCapture = state.capture.list().find((item) => item.runtime()?.sessionId === target.identity.session_id);
        assert(childCapture, "ACTUAL_IN_PROCESS_CHILD_NOT_CAPTURED");
        const child = childCapture.runtime() as any;
        sessions.push(child);
        const binding = value(state.capture.bindOwned(bridge, target.identity.target_ref, childCapture));
        assert.equal(binding.session_id, child.sessionId);
        assert.equal(binding.run_epoch, 0);
        assert.notEqual(root.sessionId, child.sessionId);
        const before = value(childCapture.read());
        assert.equal(before.model_runtime, child.modelRuntime);
        assert.deepEqual(before.messages, child.messages);
        assert.equal(before.system_prompt, child.systemPrompt);
        const messageCount = child.messages.length;
        (before.messages as unknown[]).push({ role: "user", content: "detached snapshot only" });
        assert.equal(child.messages.length, messageCount);
        await child.setModel(child.modelRegistry.find("coordination-capture", "changed"));
        child.agent.state.systemPrompt += "\nCAPTURE_LOCAL_PROMPT_CHANGE";
        child.agent.state.messages = [...child.messages,
          { role: "user", content: "capture live message change", timestamp: Date.now() }];
        const after = value(childCapture.read());
        assert.notEqual(after.model.id, before.model.id);
        assert.notEqual(after.system_prompt, before.system_prompt);
        assert(after.messages.length > messageCount);
        assert(after.revision > before.revision);
        assert.equal(after.activity.isStreaming, child.activitySnapshot.isStreaming);
        assert.equal(after.activity.isStreaming, true);
        await child.bindExtensions({ mode: "print" });
        assert.equal(state.capture.list().length, 2);
        assert.equal(value(state.capture.forRuntime(child)), childCapture);
        assert.deepEqual(bridge.bindActualRuntime(target.identity.target_ref, { sessionId: child.sessionId }),
          { ok: false, reason: "runtime_mismatch" });
        report.identities = { parent_session_id: root.sessionId, child_session_id: child.sessionId,
          parent_and_child_distinct: true, native_runtime_same_object: true, run_epoch: binding.run_epoch,
          module_ready_before_child: moduleReadyBeforeChild,
          same_AgentSession: holder.module.AgentSession === sdk.AgentSession };
        assert(report.identities.same_AgentSession);
        report.context_changes = { model: true, system_prompt: true, messages: true, revision: true,
          snapshot_detached: true, activity: true, revision_before: before.revision, revision_after: after.revision };
        report.adversarial = { duplicate_capture_prevented: true, fabricated_runtime_refused: true,
          prior_wrapper_preserved: priorCalls >= 3 };
        bus.emit("release-child");
      }
    }
    const settled = await bounded(completed, "native child completion");
    assert.equal(settled.status, "completed");
    if (name === "capture-in-process") {
      const emptyLoader = new sdk.DefaultResourceLoader({
        cwd, agentDir: env.OMO_CODING_AGENT_DIR, noExtensions: true, noSkills: true,
        noPromptTemplates: true, noThemes: true, noContextFiles: true,
        settingsManager: sdk.SettingsManager.inMemory({}),
      });
      await emptyLoader.reload();
      const probe = (await sdk.createAgentSession({
        cwd, agentDir: env.OMO_CODING_AGENT_DIR, resourceLoader: emptyLoader,
        authStorage: auth, modelRegistry: registry, model: registry.find("coordination-capture", "offline"),
        sessionManager: sdk.SessionManager.inMemory(cwd),
      })).session;
      sessions.push(probe);
      await probe.bindExtensions({ mode: "print" });
      const observed = value(state.capture.forRuntime(probe));
      let closed = false;
      const detach = state.capture.onRuntime((item, event) => {
        if (item === observed && event === "closed") closed = true;
      });
      await probe.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
      probe.dispose();
      assert(closed, "NATIVE_DISPOSE_NOT_OBSERVED");
      assert.deepEqual(observed.read(), { ok: false, reason: "target_not_live" });
      detach();
      report.lifecycle = { native_dispose_observed: true, closed_runtime_refused: true };
    }
    if (mode === "process") {
      const local = JSON.parse(settled.final_response);
      // This pinned native process runner does not persist child_session_id.
      // Correlate its authoritative PID and original live RPC read port instead
      // of fabricating a record field or treating the worker's claim as authority.
      const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
      const engine = carrier.contexts.get(root.sessionManager)?.deref();
      assert(engine, "PROCESS_NATIVE_OWNER_ENGINE_MISSING");
      const handle = engine.manager.getResidentHandle(launchedId);
      assert(handle && handle.kind === "rpc", "REAL_RPC_HANDLE_MISSING");
      processHandle ??= handle;
      assert.equal(handle, processHandle);
      assert.equal(handle.pid, settled.pid);
      assert.equal(local.pid, handle.pid);
      const remote = await bounded<{ entries: { id: string }[]; leafId: string | null }>(
        handle.getEntries(), "original native process RPC get_entries");
      assert(local.branch && remote.entries.some((entry: any) => entry.id === local.branch),
        "LIVE_RPC_BRANCH_CAPTURE_MISMATCH");
      assert(path.resolve(local.session_file).startsWith(path.resolve(sandbox) + path.sep));
      assert(path.resolve(local.session_file).split(path.sep).includes(launchedId!));
      // Header identity is corroboration only; context above came from the live
      // AgentSession, and live RPC from the actual native-owned process handle.
      const header = JSON.parse(fs.readFileSync(local.session_file, "utf8").split("\n")[0]!);
      assert.equal(header.id, local.session_id);
      assert.notEqual(local.pid, process.pid);
      assert.equal(local.model, "coordination-capture/offline");
      assert.equal(local.entry_sha256, sha256(entry));
      assert.equal(path.resolve(local.entry), path.resolve(entry));
      assert(local.local_snapshot_matches_actual && local.same_AgentSession && local.message_count > 0);
      report.identities = { parent_session_id: root.sessionId, child_session_id: local.session_id,
        parent_and_child_distinct: root.sessionId !== local.session_id, separate_process: true,
        parent_pid: process.pid, child_pid: local.pid, run_epoch: settled.notification.run_epoch,
        child_model: local.model, native_pid_matches: true, native_rpc_branch_matches: true,
        canonical_child_session_id: settled.child_session_id ?? null };
      report.extension = { ...report.extension, native_package_inheritance: true, entry_hash_matches: true };
      report.process_local_capture = local;
    }
    assert.deepEqual(holder && Object.getOwnPropertyDescriptor(holder, "module"), descriptor);
    report.native_worker_completed = true;
    report.native_record = { task_id: launchedId, child_session_id: settled.child_session_id,
      run_epoch: settled.notification.run_epoch, model: settled.model, status: settled.status };
    await bounded(root.waitForIdle(), "root completion notification drain");
    const oldCapture = parentCapture;
    state.dispose();
    assert.deepEqual(oldCapture.read(), { ok: false, reason: "stale_generation" });
    report.adversarial = { ...report.adversarial, stale_generation_refused: true };
    report.passed = true;
  } catch (error) {
    report.passed = false;
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    throw error;
  } finally {
    bus.emit("release-child");
    detachTerminal();
    if (name === "capture-process" && !processHandle && report.launch?.details?.task_id) {
      const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
      processHandle = carrier?.contexts.get(sessions[0]?.sessionManager)?.deref()
        ?.manager.getResidentHandle(report.launch.details.task_id);
    }
    if (processHandle && !processHandle.hasExited()) {
      // Native dispose intentionally detaches a resident RPC worker. Terminate
      // only our still-held original ChildProcess handle after the observed
      // outcome; never look up or kill a stored PID.
      await bounded(processHandle.terminate(), "owned native RPC termination");
      await bounded(processHandle.dispose(), "owned native RPC disposal");
    }
    (global[stateKey] as CaptureExtensionState | undefined)?.dispose();
    for (const session of [...new Set(sessions)].reverse()) {
      if (session._disposed) continue;
      await bounded(session.waitForIdle(), "owned cleanup idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    }
    report.cleanup = {
      sessions_disposed: sessions.every((session) => session._disposed),
      prototype_restored: sdk?.AgentSession.prototype.bindExtensions === priorWrapper,
      dispose_restored: sdk?.AgentSession.prototype.dispose === originalDispose,
      module_subscriptions: global[Symbol.for("omo.agent-coordination.native.v1")]?.moduleListeners.size ?? 0,
      provider_gates: bus.eventNames().length,
      process_handle_exited: !processHandle || processHandle.hasExited(),
    };
    if (sdk && originalBind) sdk.AgentSession.prototype.bindExtensions = originalBind;
    if (manifest && sha256(manifest.output) === manifest.candidate_sha256) report.removal = removeAdapter(manifest);
    for (const directory of [packageDir, path.join(sandbox, "candidate")]) {
      const link = path.join(directory, "node_modules");
      if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
    }
    assert.equal(sha256(bundle), report.installed_sha256_before);
    report.installed_sha256_after = sha256(bundle);
    report.finished_at = new Date().toISOString();
    fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
    process.chdir(previousCwd);
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, previousEnv);
  }
  return report;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const name = process.argv[process.argv.indexOf("--native-case") + 1] as Case;
  const sandbox = process.argv[process.argv.indexOf("--sandbox") + 1]!;
  const artifact = process.argv[process.argv.indexOf("--result") + 1]!;
  const report = await nativeCase(name, sandbox, artifact);
  console.log(JSON.stringify({ case: name, passed: report.passed, artifact }));
}

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import { attempt, product, bundle, senpi, isolatedEnv, sha256 } from "../command.mjs";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";
import { getCoordinationBroker } from "../../src/public-api.ts";
import { command, save } from "../../.omo/evidence/agent-coordination/attempt-20261017-e1/task-8/run.mjs";

type Json = Record<string, any>;
type Scenario = "auto-terminal" | "disabled" | "protected-waits";
const evidence = path.join(attempt, "task-8");
async function bounded<T>(pending: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([pending, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`AUTO_EVENT_DEADLINE:${label}`)), 15_000);
    })]);
  } finally { clearTimeout(timer!); }
}
function event(bus: EventEmitter, name: string, predicate: (row: any) => boolean = () => true) {
  let listener: (row: any) => void;
  const pending = bounded(new Promise<any>((resolve) => {
    listener = (row) => { if (predicate(row)) { bus.off(name, listener); resolve(row); } };
    bus.on(name, listener);
  }), name).finally(() => bus.off(name, listener));
  void pending.catch(() => {});
  return pending;
}
export async function runAutoCase(name: "auto-terminal" | "protected-waits") {
  assert.equal(JSON.parse(fs.readFileSync(path.join(attempt, "verify-7/verdict.json"), "utf8")).verdict, "confirmed");
  const results: Json[] = [];
  for (const scenario of name === "auto-terminal" ? ["auto-terminal", "disabled"] as const : ["protected-waits"] as const) {
    const sandbox = path.join(evidence, "runs", `${scenario}-${randomUUID()}`);
    const directory = path.join(sandbox, "package"); fs.mkdirSync(directory, { recursive: true });
    const built = await Bun.build({ entrypoints: ["src/extension.ts", "qa/cases/protected-waits.ts"]
      .map((file) => path.join(product, file)), outdir: directory, naming: "[name].js", target: "node", format: "esm",
      external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"],
      packages: "external" });
    assert(built.success, String(built.logs));
    const link = path.join(directory, "node_modules");
    fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", link, "junction");
    const artifact = path.join(sandbox, "result.json");
    try {
      const receipt = await command(["node", fileURLToPath(import.meta.url), "--native-auto", scenario,
        "--sandbox", sandbox, "--result", artifact], `native-${scenario}-${randomUUID()}`, 55_000);
      assert.equal(receipt.exit_code, 0, `AUTO_NATIVE_FAILED:${receipt.receipt_path}`);
      const report = JSON.parse(fs.readFileSync(artifact, "utf8"));
      assert.equal(report.passed, true);
      assert(report.cleanup.sessions_disposed && report.cleanup.installed_unchanged && report.cleanup.adapter_restored);
      results.push({ ...report, command_receipt: receipt.receipt_path });
    } finally {
      if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
    }
  }
  const result = { case: name, passed: results.every((result) => result.passed), results };
  save(name === "auto-terminal" ? "automatic.json" : "protected.json", result);
  if (name === "auto-terminal") save("idempotence.json", results.map((result) =>
    ({ scenario: result.scenario, effects: result.effects, after: result.after, disabled: result.disabled })));
  return result;
}

async function nativeAuto(scenario: Scenario, sandbox: string, artifact: string) {
  assert(path.resolve(sandbox).startsWith(path.resolve(evidence) + path.sep));
  const env = isolatedEnv(sandbox);
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env, scenario === "disabled" ? { OMO_AGENT_COORDINATION_REPAIR: "0" } : {});
  const metadata = path.join(sandbox, "sdk-metadata"); fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = metadata;
  const cwd = path.join(sandbox, "work"); fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true }); process.chdir(cwd);
  const bus = new EventEmitter() as EventEmitter & { calls: number }; bus.calls = 0;
  (globalThis as any)[Symbol.for("coordination.auto.qa.bus")] = bus;
  const report: Json = { scenario, sandbox, started_at: new Date().toISOString(), installed_sha256: sha256(bundle), passed: false };
  const sessions: any[] = [], labels: string[] = [];
  const originalRename = fs.renameSync;
  let manifest: ReturnType<typeof buildAdapter> | undefined, state: any, stopTerminal = () => {};
  let faultTask: string | undefined, fault = false;
  let failures = 0;
  const writes: Json[] = [], releases: Json[] = [], notifications: Json[] = [];
  fs.renameSync = function (from, to) {
    const row = typeof from === "string" && /st_.*\.tmp$/i.test(from)
      ? JSON.parse(fs.readFileSync(from, "utf8")) : undefined;
    if (row && row.task_id === faultTask && row.notification.run_epoch === 1 &&
        row.notification.notified_epoch < 1 && row.status === "completed") {
      if (fault) {
        fault = false; failures++; bus.emit("publication-failed");
        throw Object.assign(new Error("Owned native terminal publication fault"), { code: "ENOSPC" });
      }
      writes.push({ task_id: row.task_id, epoch: 1, result: row.final_response });
    }
    return originalRename(from, to);
  } as typeof fs.renameSync;
  syncBuiltinESMExports();
  try {
    manifest = buildAdapter({ output: path.join(sandbox, "candidate/omo-task.js") });
    report.candidate_sha256 = manifest.candidate_sha256;
    report.loaded_entry_sha256 = sha256(path.join(sandbox, "package/extension.js"));
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
    fs.writeFileSync(path.join(cwd, ".omo/omo.json"), JSON.stringify({
      agents: { explore: { model: "coordination-auto/offline" } },
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: "in-process",
        default_concurrency: scenario === "protected-waits" ? 1 : 2,
        global_concurrency: scenario === "protected-waits" ? 1 : 2, host_shard_prewarm: "off" },
    }));
    const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const auth = AuthStorage.inMemory(), registry = ModelRegistry.inMemory(auth);
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory({ packages: ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg"],
        extensions: ["C:/Users/USER/.omo-stable-500/home/.omo/agent/extensions/omo-task-record-publish-retry.js",
          path.join(sandbox, "package/extension.js"), path.join(sandbox, "package/protected-waits.js")],
        retry: { enabled: false }, sessionTitle: { enabled: false } }) });
    await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
    const root = (await sdk.createAgentSession({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      resourceLoader: loader, authStorage: auth, modelRegistry: registry,
      model: registry.find("coordination-auto", "offline"), sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    sessions.push(root); await root.bindExtensions({ mode: "print" });
    state = (globalThis as any)[Symbol.for("omo.agent-coordination.capture.v1")];
    const bridge = state.bridges.get(root.sessionManager); assert(bridge, "ACTUAL_OWNER_BRIDGE_MISSING");
    const carrier = (globalThis as any)[Symbol.for("omo.agent-coordination.native.v1")];
    const engine = carrier.contexts.get(root.sessionManager).deref(), store = carrier.links.get(engine).store;
    const ports = carrier.ports.get(store);
    const release = ports.releaseSlot, notify = engine.notifier.notifyTerminal;
    ports.releaseSlot = (id: string, model: string, epoch: number) => {
      releases.push({ task_id: id, epoch }); return release(id, model, epoch);
    };
    engine.notifier.notifyTerminal = (input: any) => {
      notifications.push({ task_id: input.record.task_id, epoch: input.record.notification.run_epoch });
      return notify(input);
    };
    stopTerminal = (globalThis as any)[Symbol.for("omo.task.terminalObservers")].subscribe(({ record }: any) =>
      bus.emit("terminal", record));
    const call = (name: string, args: Json) => root.extensionRunner.getToolDefinition(name)
      .execute(randomUUID(), args, undefined, undefined, root.extensionRunner.createContext());
    const inspect = async (target_ref?: string) => {
      const result = (await call("coordination_inspect", target_ref ? { target_ref } : {})).details;
      assert(result.ok, JSON.stringify(result)); return result.value;
    };
    const launch = async (label: string, wait = true) => {
      labels.push(label);
      const started = wait ? event(bus, "provider-start", (row) => row.label === label) : undefined;
      const result = await call("task", { subagent_type: "explore", model: "coordination-auto/offline",
        prompt: `AUTO_NATIVE_GATE:${label}`, description: label, run_in_background: true });
      const id = result.details.task_id; assert(id, JSON.stringify(result));
      if (started) await started;
      return id as string;
    };
    // Publication happens through the registered broker surface, never a fake broker.
    (globalThis as any)[Symbol.for("omo.agent-coordination.api-ready.v1")]();
    const broker = getCoordinationBroker(); assert(broker.ok);
    report.features = broker.value.features;
    assert(Object.isFrozen(report.features));
    assert.equal(report.features.includes("revival_settlement_repair"), scenario !== "disabled");
    if (scenario === "protected-waits") {
      const id = await launch("tool");
      const held = event(bus, "tool-start"); bus.emit("release:tool"); await held;
      const target = bridge.listOwned().value.find((row: any) => row.identity.task_id === id).identity.target_ref;
      const child = carrier.runtimeOwners.get(engine.manager.getResidentHandle(id)).runtime.deref();
      sessions.push(child);
      const queued = await launch("queued", false);
      assert.equal(store.load(queued).status, "pending");
      // Another real root owns a proven residue. It loads the same native task
      // component without the coordination extension, so this owner cannot repair it.
      const foreignLoader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
        noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
        settingsManager: sdk.SettingsManager.inMemory({ extensions: [path.join(sandbox, "package/protected-waits.js")],
          retry: { enabled: false }, sessionTitle: { enabled: false } }) });
      await foreignLoader.reload(); assert.deepEqual(foreignLoader.getExtensions().errors, []);
      const foreign = (await sdk.createAgentSession({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
        resourceLoader: foreignLoader, authStorage: auth, modelRegistry: registry,
        model: registry.find("coordination-auto", "offline"), sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
      sessions.push(foreign); await foreign.bindExtensions({ mode: "print" });
      const foreignEngine = carrier.contexts.get(foreign.sessionManager).deref(); assert.notEqual(foreignEngine, engine);
      const foreignCall = (name: string, args: Json) => foreign.extensionRunner.getToolDefinition(name)
        .execute(randomUUID(), args, undefined, undefined, foreign.extensionRunner.createContext());
      const foreignStarted = event(bus, "provider-start", (row) => row.label === "foreign");
      labels.push("foreign");
      const foreignId = (await foreignCall("task", { subagent_type: "explore", model: "coordination-auto/offline",
        prompt: "AUTO_NATIVE_GATE:foreign", description: "Foreign root protected residue", run_in_background: true })).details.task_id;
      assert(foreignId); await foreignStarted;
      const foreignDone = event(bus, "terminal", (row) => row.task_id === foreignId && row.status === "completed");
      bus.emit("release:foreign"); await foreignDone;
      const foreignRevived = event(bus, "provider-start", (row) => row.label === "foreign-revived");
      labels.push("foreign-revived");
      await foreignCall("task_send", { to: foreignId, message: "AUTO_NATIVE_GATE:foreign-revived" }); await foreignRevived;
      const foreignHandle = foreignEngine.manager.getResidentHandle(foreignId);
      sessions.push(carrier.runtimeOwners.get(foreignHandle).runtime.deref());
      const foreignOutcome = bounded<any>(foreignHandle.waitForOutcome(), "foreign native outcome");
      faultTask = foreignId; fault = true;
      const foreignFailed = event(bus, "publication-failed");
      bus.emit("release:foreign-revived"); await foreignFailed;
      assert.equal((await foreignOutcome).status, "completed");
      assert.equal(store.load(foreignId).status, "running");
      const foreignFile = path.join(sandbox, "senpi-task/tasks", foreignId + ".json");
      const foreignHash = sha256(foreignFile);
      assert(!bridge.listOwned().value.some((row: any) => row.identity.task_id === foreignId));
      report.foreign_residue = { task_id: foreignId, native_outcome: "completed", canonical: "running",
        different_owner_engine: true, owner_enumeration_excluded: true };
      const before = await inspect(target);
      assert.equal(before.diagnostic_class, "live_blocked_tool");
      // This is the SDK's extension-owned wake-source protocol, not a patched getter.
      child.extensionRunner.emitBusEvent("wake_source_state", { source: "auto-qa-deferred", activeCount: 1,
        items: [{ id: "owned-deferred", startedAtMs: 0 }] });
      assert.equal(child.activitySnapshot.hasActiveWakeSource, true);
      const deferred = await inspect(target); assert.equal(deferred.activity.value, "deferred_wake");
      const files = [id, queued].map((task) => path.join(sandbox, "senpi-task/tasks", task + ".json"));
      const beforeHashes = files.map(sha256), beforeEffects = [releases.length, notifications.length, bus.calls];
      // Re-deliver a genuine unchanged store event without inventing a lifecycle
      // outcome: native appendEvent is audit-only, so use owner attachment checks.
      for (let n = 0; n < 4; n++) await inspect();
      assert.deepEqual(files.map(sha256), beforeHashes);
      assert.equal(sha256(foreignFile), foreignHash);
      assert.deepEqual([releases.length, notifications.length, bus.calls], beforeEffects);
      report.protected = { blocked_tool: before, deferred_wake: deferred,
        queued: { task_id: queued, status: store.load(queued).status },
        canonical_unchanged: true, extra_provider_calls: 0, age_started_at_ms: 0 };
      child.extensionRunner.emitBusEvent("wake_source_state", { source: "auto-qa-deferred", activeCount: 0 });
      const queuedStarted = event(bus, "provider-start", (row) => row.label === "queued");
      const done = event(bus, "terminal", (row) => row.task_id === id && row.status === "completed");
      bus.emit("release-tool"); await done; await queuedStarted;
      const queuedDone = event(bus, "terminal", (row) => row.task_id === queued && row.status === "completed");
      bus.emit("release:queued"); await queuedDone;
      report.protected.original_work_completed = true;
      report.foreign_residue.canonical_unchanged = true;
      // Inject one local SDK birth failure, before a child turn exists. The
      // actual manager must execute its own admission failure/rollback path.
      const beforeAdmissionCalls = bus.calls;
      const originalBind = sdk.AgentSession.prototype.bindExtensions;
      let birthFailures = 0;
      const rolledBack = event(bus, "terminal", (row) =>
        row.description === "Native admission rollback" && row.status === "error");
      sdk.AgentSession.prototype.bindExtensions = function (...args: unknown[]) {
        if (this.sessionManager !== root.sessionManager && birthFailures === 0) {
          birthFailures++; throw Object.assign(new Error("Owned SDK birth failure before execution"),
            { code: "QA_ADMISSION_ROLLBACK" });
        }
        return Reflect.apply(originalBind, this, args);
      };
      let admission: any;
      try {
        await call("task", { subagent_type: "explore", model: "coordination-auto/offline",
          prompt: "AUTO_NATIVE_GATE:admission", description: "Native admission rollback", run_in_background: true });
        admission = await rolledBack;
      } finally { sdk.AgentSession.prototype.bindExtensions = originalBind; }
      assert.equal(birthFailures, 1); assert.equal(bus.calls, beforeAdmissionCalls);
      const admissionTask = admission.task_id;
      admission = store.load(admissionTask);
      assert.equal(admission.status, "error");
      assert.equal(engine.manager.getResidentHandle(admissionTask), undefined);
      assert.equal(engine.manager.concurrency.leaseState(admissionTask, 0), undefined);
      report.admission = { task_id: admissionTask, status: admission.status, original_rollback: true, birth_failures: birthFailures,
        slot_released: true, no_runtime: !admission.child_session_id, not_fabricated_completion: true };
    } else {
      const id = await launch("first");
      const done0 = event(bus, "terminal", (row) => row.task_id === id && row.status === "completed");
      bus.emit("release:first"); await done0;
      const start1 = event(bus, "provider-start", (row) => row.label === "revived"); labels.push("revived");
      await call("task_send", { to: id, message: "AUTO_NATIVE_GATE:revived" }); await start1;
      faultTask = id; fault = true;
      assert.equal(store.load(id).notification.run_epoch, 1);
      // The enabled proof does not inspect, subscribe through the bridge, or
      // request repair. Its own native lifecycle listeners must bind the witness.
      const disabledRow = scenario === "disabled"
        ? bridge.listOwned().value.find((row: any) => row.identity.task_id === id) : undefined;
      if (disabledRow) await inspect(disabledRow.identity.target_ref);
      const handle = engine.manager.getResidentHandle(id); sessions.push(carrier.runtimeOwners.get(handle).runtime.deref());
      const nativeOutcome = bounded<any>(handle.waitForOutcome(), "exact native outcome");
      const failed = event(bus, "publication-failed");
      const completed = scenario !== "disabled"
        ? event(bus, "terminal", (row) => row.task_id === id && row.status === "completed") : undefined;
      bus.emit("release:revived"); await failed;
      const outcome = await nativeOutcome;
      assert.equal(outcome.status, "completed");
      assert.equal(outcome.finalResponse, "native-auto-result:revived");
      if (completed) await completed;
      else {
        const diagnostic = await inspect(disabledRow.identity.target_ref);
        assert.equal(diagnostic.record_consistent, false); assert.equal(diagnostic.status, "running");
        report.disabled = { mutation_count: writes.length, diagnostic };
      }
      const after = store.load(id); report.after = { task_id: id, status: after.status,
        notification: after.notification, final_response: after.final_response };
      const row = bridge.listOwned().value.find((row: any) => row.identity.task_id === id);
      report.witness = row.outcome; report.manual_reconcile_calls = 0;
      assert.equal(report.witness.run_epoch, 1); assert.equal(report.witness.status, "completed");
      assert.equal(failures, 1);
      assert.equal(writes.length, scenario === "disabled" ? 0 : 1);
      assert.equal(after.status, scenario === "disabled" ? "running" : "completed");
      if (scenario !== "disabled") {
        assert.equal(after.final_response, "native-auto-result:revived");
        assert.equal(after.notification.notified_epoch, 1);
        const output = await call("task_output", { task_id: id, mode: "status" });
        assert(JSON.stringify(output).includes(after.final_response)); report.task_output = output;
        const canonicalHash = sha256(path.join(sandbox, "senpi-task/tasks", id + ".json"));
        for (let n = 0; n < 4; n++) await inspect(row.identity.target_ref);
        assert.equal(sha256(path.join(sandbox, "senpi-task/tasks", id + ".json")), canonicalHash);
        assert.equal(writes.length, 1);
        assert.equal(notifications.filter((row) => row.task_id === id && row.epoch === 1).length, 1);
        assert.equal(releases.filter((row) => row.task_id === id && row.epoch === 1).length, 1);
        report.diagnostic = await inspect(row.identity.target_ref);
        assert.equal(report.diagnostic.record_consistent, true);
      }
      report.effects = { writes, releases, notifications, failures };
      const reusable = await launch("capacity");
      const done = event(bus, "terminal", (row) => row.task_id === reusable && row.status === "completed");
      bus.emit("release:capacity"); await done; report.capacity_reused = true;
    }
    report.scripted_main_provider_calls = bus.calls;
    report.extra_repair_provider_calls = 0;
    report.passed = true;
  } catch (error) {
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    process.exitCode = 1;
  } finally {
    fault = false;
    for (const label of labels) bus.emit(`release:${label}`);
    bus.emit("release-tool"); stopTerminal();
    for (const capture of state?.capture.list() ?? []) if (capture.runtime()) sessions.push(capture.runtime());
    for (const session of [...new Set(sessions)].reverse()) {
      if (!session || session._disposed) continue;
      await bounded(session.waitForIdle(), "cleanup native idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" }); session.dispose();
    }
    state?.dispose(); fs.renameSync = originalRename; syncBuiltinESMExports();
    if (manifest) report.adapter_removal = removeAdapter(manifest);
    const link = path.join(sandbox, "candidate/node_modules");
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
    bus.removeAllListeners();
    report.cleanup = { sessions_disposed: sessions.every((session) => !session || session._disposed),
      installed_unchanged: sha256(bundle) === report.installed_sha256,
      adapter_restored: manifest ? sha256(manifest.output) === manifest.baseline_sha256 : true };
    report.finished_at = new Date().toISOString(); fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
  }
  return report;
}
export const cases = { "auto-terminal": async () => runAutoCase("auto-terminal") };
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url) && process.argv.includes("--native-auto")) {
  const flag = (name: string) => process.argv[process.argv.indexOf(name) + 1]!;
  const result = await nativeAuto(flag("--native-auto") as Scenario, flag("--sandbox"), flag("--result"));
  console.log(JSON.stringify({ scenario: result.scenario, passed: result.passed, error: result.error, artifact: flag("--result") }));
}

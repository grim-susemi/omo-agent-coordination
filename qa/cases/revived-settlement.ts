import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import { syncBuiltinESMExports } from "node:module";
import { randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { attempt, product, bundle, senpi, sha256, isolatedEnv } from "../command.mjs";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";

type Json = Record<string, any>;
type Scenario = "revived-settlement" | "retry-success" | "permanent" | "nontarget";
export const settlementEvidence = path.join(attempt, "task-7");
export function settlementHashes() {
  const files = ["package.json", "tsconfig.json", "bun.lock"];
  for (const directory of ["src", "test", "qa", "scripts"]) {
    for (const file of fs.readdirSync(path.join(product, directory), { recursive: true, withFileTypes: true })) {
      if (file.isFile()) files.push(path.relative(product, path.join(file.parentPath, file.name)).replaceAll("\\", "/"));
    }
  }
  return Object.fromEntries(files.sort().map((file) => [file, sha256(path.join(product, file))]));
}
export async function settlementCommand(argv: string[], options: {
  cwd?: string; timeoutMs?: number; onSpawn?: (child: any) => void; onStdout?: (bytes: Buffer) => void;
} = {}) {
  fs.mkdirSync(settlementEvidence, { recursive: true });
  const id = randomUUID(), raw = path.join(settlementEvidence, `command-${id}.raw.txt`);
  const receiptPath = path.join(settlementEvidence, `command-${id}.json`);
  const receipt: Json = { argv, cwd: options.cwd ?? product, started_at: new Date().toISOString(),
    exit_code: null, timeout_ms: options.timeoutMs ?? 55_000, source_sha256: settlementHashes(),
    module_sha256: { [bundle]: sha256(bundle), [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js") },
    raw_output_path: raw, fixture_identity: settlementEvidence };
  const save = () => fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  const fd = fs.openSync(raw, "wx");
  const child = spawn(argv[0]!, argv.slice(1), { cwd: receipt.cwd,
    env: isolatedEnv(path.join(settlementEvidence, "commands", id)), windowsHide: true,
    stdio: [options.onSpawn ? "pipe" : "ignore", "pipe", "pipe"] });
  receipt.pid = child.pid; save(); options.onSpawn?.(child);
  child.stdout!.on("data", (bytes: Buffer) => options.onStdout?.(bytes));
  for (const stream of [child.stdout, child.stderr]) stream!.on("data", (bytes: Buffer) => fs.writeSync(fd, bytes));
  const timer = setTimeout(() => { receipt.timed_out = true; child.kill(); }, receipt.timeout_ms);
  try {
    receipt.exit_code = await new Promise<number>((resolve) => {
      child.once("error", (error) => { receipt.error = String(error); resolve(127); });
      child.once("close", (code, signal) => { receipt.signal = signal; resolve(code ?? 1); });
    });
  } finally { clearTimeout(timer); fs.closeSync(fd); }
  receipt.finished_at = new Date().toISOString(); save();
  return { exit: receipt.exit_code as number, receiptPath, raw };
}
function save(file: string, data: unknown) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(data, null, 2) + "\n");
}
export async function runSettlementCase(scenario: Scenario): Promise<Json> {
  const sandbox = path.join(settlementEvidence, "runs", `${scenario}-${randomUUID()}`);
  const directory = path.join(sandbox, "package");
  fs.mkdirSync(directory, { recursive: true });
  const build = await Bun.build({ entrypoints: [path.join(product, "src/extension.ts")],
    outdir: directory, naming: "extension.js", target: "node", format: "esm",
    external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"] });
  assert(build.success, String(build.logs));
  const link = path.join(directory, "node_modules");
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", link, "junction");
  const artifact = path.join(sandbox, "result.json");
  try {
    const receipt = await settlementCommand(["node", fileURLToPath(import.meta.url),
      "--native-settlement", scenario, "--sandbox", sandbox, "--result", artifact]);
    assert.equal(receipt.exit, 0, `SETTLEMENT_NATIVE_FAILED:${receipt.receiptPath}`);
    const result = JSON.parse(fs.readFileSync(artifact, "utf8"));
    assert.equal(result.passed, true);
    assert(result.cleanup.sessions_disposed && result.cleanup.reader_exited && result.cleanup.installed_unchanged);
    const output = { ...result, command_receipt: receipt.receiptPath };
    save(path.join(settlementEvidence, `${scenario}.json`), output);
    if (scenario === "revived-settlement") save(path.join(settlementEvidence, "settlement.json"), output);
    return output;
  } finally {
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
  }
}
/** Preserve todo 1's complete event schedule and assertions. Only its imported
 * bundle/evidence bindings and explicit post-reader reconciliation are wired
 * to this candidate in a disposable generated host, never in installed code.
 */
export async function runRegressionCase(name: "normal-baseline" | "revived-stale-record",
  schedule = name === "revived-stale-record" ? "r2-closest-reader" : "after-send"): Promise<Json> {
  const directory = path.join(settlementEvidence, "regression-hosts", randomUUID());
  fs.mkdirSync(directory, { recursive: true });
  const entry = path.join(directory, "extension.js");
  const built = await Bun.build({ entrypoints: [path.join(product, "src/extension.ts")],
    outdir: directory, naming: "extension.js", target: "node", format: "esm",
    external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"] });
  assert(built.success, String(built.logs));
  const link = path.join(directory, "node_modules");
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", link, "junction");
  const shim = path.join(directory, "command.mjs");
  const candidateEnabled = process.env.COORDINATION_SETTLEMENT_TEST_DISABLED !== "1";
  fs.writeFileSync(shim, `
import fs from "node:fs";
import path from "node:path";
import {bundle as installed,senpi,isolatedEnv,sha256} from ${JSON.stringify(pathToFileURL(path.join(product, "qa/command.mjs")).href)};
import {buildAdapter,removeAdapter} from ${JSON.stringify(pathToFileURL(path.join(product, "scripts/build-adapter.mjs")).href)};
import {settlementCommand,settlementHashes} from ${JSON.stringify(import.meta.url)};
export const attempt=${JSON.stringify(settlementEvidence)},evidence=attempt,sourceHashes=settlementHashes,command=settlementCommand;
export const settlementEnabled=${JSON.stringify(candidateEnabled)},candidateExtension=${JSON.stringify(pathToFileURL(entry).href)};
const manifest=settlementEnabled?buildAdapter({output:${JSON.stringify(path.join(directory, "candidate/omo-task.js"))}}):undefined;
export const bundle=manifest?.output??installed;
export {senpi,isolatedEnv,sha256};
export function cleanupCandidate(){if(manifest){removeAdapter(manifest);const link=path.join(path.dirname(manifest.output),"node_modules");if(fs.existsSync(link))fs.unlinkSync(link)}}
`);
  const original = fs.readFileSync(path.join(product, "qa/harness.ts"), "utf8");
  let source = original;
  const replace = (before: string, after: string) => {
    assert.equal(source.split(before).length - 1, 1, `REGRESSION_WIRING_ANCHOR:${before}`);
    source = source.replace(before, after);
  };
  replace('from "./command.mjs";', `from ${JSON.stringify(pathToFileURL(shim).href)};
import { settlementEnabled, candidateExtension, cleanupCandidate } from ${JSON.stringify(pathToFileURL(shim).href)};
import { connectEngineBridge } from ${JSON.stringify(pathToFileURL(path.join(product, "src/engine-bridge.ts")).href)};`);
  replace("  let readerDone: Promise<number> | undefined;", `  let readerDone: Promise<number> | undefined;
  let settlementBridge: any;
  let settlementRef: any;
  let settlementObserved: Promise<unknown> | undefined;`);
  replace('    await dispatch("session_start", { type: "session_start" });', `
    if (settlementEnabled) await (await import(candidateExtension)).default(pi);
    await dispatch("session_start", { type: "session_start" });
    if (settlementEnabled) {
      const connection = connectEngineBridge({ context: ctx, generation: 1, loaderNamespace: sdk });
      assert(connection.ok, JSON.stringify(connection)); settlementBridge = connection.value;
    }`);
  replace("      await nextProvider;", `      await nextProvider;
      if (settlementBridge) {
        const listed = settlementBridge.listOwned(); assert(listed.ok);
        settlementRef = listed.value.find((row: any) => row.identity.task_id === taskId).identity.target_ref;
        settlementObserved = new Promise((resolve) => {
          const subscribed = settlementBridge.subscribeOutcome(settlementRef, resolve); assert(subscribed.ok);
        });
      }`);
  replace('path.join(path.dirname(fileURLToPath(import.meta.url)), "reader.ps1")',
    JSON.stringify(path.join(product, "qa/reader.ps1")));
  replace("        report.record_after_reader_release = store.load(taskId);", `        if (settlementBridge) {
          await settlementObserved;
          const repaired = settlementBridge.reconcileOwnedTerminal(settlementRef);
          assert(repaired.ok, JSON.stringify(repaired));
          assert.equal(repaired.value.state, "settled");
          report.candidate_settlement = repaired.value;
        }
        report.record_after_reader_release = store.load(taskId);`);
  replace("      observations.restore();", `      settlementBridge?.dispose();
      (globalThis as any)[Symbol.for("omo.agent-coordination.capture.v1")]?.dispose();
      observations.restore();
      cleanupCandidate();`);
  source = `function sanitizeReport(value: any) {
    const clone = JSON.parse(JSON.stringify(value, (key, item) =>
      ["spawn_spec", "instructions", "systemPrompt", "prompt"].includes(key) ? undefined : item));
    for (const key of Object.keys(value)) delete value[key];
    Object.assign(value, clone);
  }\n` + source;
  const reportWrite = 'fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\\n");';
  assert.equal(source.split(reportWrite).length - 1, 2);
  source = source.replaceAll(reportWrite, "sanitizeReport(report); " + reportWrite);
  const host = path.join(directory, "harness.ts"), artifact = path.join(directory, "result.json");
  fs.writeFileSync(host, source);
  save(path.join(directory, "wiring.json"), { original_harness_sha256: sha256(path.join(product, "qa/harness.ts")),
    generated_harness_sha256: sha256(host), candidate_enabled: candidateEnabled, assertions_changed: false });
  try {
    const receipt = await settlementCommand(["node", host, "--native-case", name, "--schedule", schedule, "--result", artifact]);
    assert.equal(receipt.exit, 0, `REGRESSION_NATIVE_FAILED:${receipt.receiptPath}`);
    return { ...JSON.parse(fs.readFileSync(artifact, "utf8")), command_receipt: receipt.receiptPath,
      candidate_enabled: candidateEnabled };
  } finally {
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
  }
}
function bounded<T>(pending: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([pending, new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`SETTLEMENT_EVENT_DEADLINE:${label}`)), 20_000);
  })]).finally(() => clearTimeout(timer!));
}
function event(bus: EventEmitter, name: string, predicate = (_row: any) => true) {
  let listener: (row: any) => void;
  const pending = new Promise<any>((resolve) => {
    listener = (row) => { if (predicate(row)) { bus.off(name, listener); resolve(row); } };
    bus.on(name, listener);
  });
  return bounded(pending, name).finally(() => bus.off(name, listener!));
}
const lifecycle = (record: any) => ({
  task_id: record.task_id, status: record.status, residency_state: record.residency_state,
  notification: { ...record.notification }, final_response: record.final_response, error_message: record.error_message,
});
async function nativeSettlement(scenario: Scenario, sandbox: string, artifact: string) {
  assert(path.resolve(sandbox).startsWith(path.resolve(settlementEvidence) + path.sep));
  const env = isolatedEnv(sandbox);
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  const metadata = path.join(sandbox, "sdk-metadata"); fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = metadata;
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true }); process.chdir(cwd);
  const bus = new EventEmitter(), sessions: any[] = [];
  const originalRename = fs.renameSync;
  const report: Json = { scenario, started_at: new Date().toISOString(), source_sha256: settlementHashes(),
    installed_sha256: sha256(bundle), real_provider_calls: 0, passed: false, pid: process.pid };
  let manifest: any, state: any, root: any, reader: any, readerDone: Promise<number> | undefined;
  let stopLedger = () => {}, recordPath = "", readerReleased = false, faultsEnabled = false;
  const renames: Json[] = [], releases: Json[] = [], notifications: Json[] = [];
  const releaseReader = () => {
    if (!readerReleased && reader?.stdin?.writable) { readerReleased = true; reader.stdin.end("RELEASE\n"); }
  };
  fs.renameSync = function (from: any, to: any) {
    if (path.resolve(String(to)) !== path.resolve(recordPath)) return originalRename(from, to);
    const next = JSON.parse(fs.readFileSync(from, "utf8"));
    const row: Json = { status: next.status, run_epoch: next.notification.run_epoch };
    renames.push(row);
    try {
      if (faultsEnabled && scenario === "nontarget" && next.status === "completed" && next.notification.run_epoch === 1) {
        throw Object.assign(new Error("Owned native publication ENOSPC"), { code: "ENOSPC" });
      }
      const result = originalRename(from, to); row.applied = true; return result;
    } catch (error) {
      row.applied = false; row.code = (error as any).code;
      if (scenario === "retry-success" && row.run_epoch === 1) releaseReader();
      throw error;
    }
  } as typeof fs.renameSync;
  syncBuiltinESMExports();
  try {
    manifest = buildAdapter({ output: path.join(sandbox, "candidate/omo-task.js") });
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
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
    const auth = AuthStorage.inMemory(), registry = ModelRegistry.inMemory(auth);
    let calls = 0;
    registry.registerProvider("coordination-qa", {
      api: "openai-completions", apiKey: "offline-not-a-credential", baseUrl: "http://offline.invalid",
      models: [{ id: "offline", name: "Task7 native gate", reasoning: false, input: ["text"],
        contextWindow: 262144, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
      streamSimple: (_model: any, context: any) => {
        const stream = new AssistantMessageEventStream();
        const child = JSON.stringify(context.messages).includes("SETTLEMENT_NATIVE_GATE");
        const n = child ? calls++ : -1;
        const finish = () => {
          const message = { role: "assistant", api: "openai-completions", provider: "coordination-qa",
            model: "offline", content: [{ type: "text", text: `native-settlement-result-${n}` }],
            stopReason: "stop", timestamp: Date.now(), usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0,
              totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
          stream.push({ type: "start", partial: message }); stream.push({ type: "done", reason: "stop", message });
          stream.end(message);
        };
        if (child) { bus.once(`release-${n}`, finish); bus.emit("provider-start", { n }); }
        else queueMicrotask(finish);
        return stream;
      },
    });
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory({
        packages: ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg"],
        extensions: ["C:/Users/USER/.omo-stable-500/home/.omo/agent/extensions/omo-task-record-publish-retry.js",
          path.join(sandbox, "package/extension.js"), path.join(product, "qa/cases/fixture-extension.js")],
        retry: { enabled: false }, sessionTitle: { enabled: false },
      }) });
    await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
    root = (await sdk.createAgentSession({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      resourceLoader: loader, authStorage: auth, modelRegistry: registry,
      model: registry.find("coordination-qa", "offline"), sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    sessions.push(root); await root.bindExtensions({ mode: "print" });
    state = (globalThis as any)[Symbol.for("omo.agent-coordination.capture.v1")];
    const bridge = state.bridges.get(root.sessionManager); assert(bridge, "ACTUAL_CANDIDATE_BRIDGE_MISSING");
    const context = root.extensionRunner.createContext();
    const carrier = (globalThis as any)[Symbol.for("omo.agent-coordination.native.v1")];
    const engine = carrier.contexts.get(context.sessionManager).deref();
    const ports = carrier.ports.get(carrier.links.get(engine).store);
    const originalRelease = ports.releaseSlot, originalNotify = engine.notifier.notifyTerminal;
    ports.releaseSlot = (id: string, model: string, epoch: number) => {
      releases.push({ task_id: id, epoch }); return originalRelease(id, model, epoch);
    };
    engine.notifier.notifyTerminal = (input: any) => {
      const result = originalNotify(input);
      notifications.push({ task_id: input.record.task_id, epoch: input.record.notification.run_epoch, result });
      return result;
    };
    const call = (tool: string, args: Json) => root.extensionRunner.getToolDefinition(tool)
      .execute(randomUUID(), args, undefined, undefined, context);
    stopLedger = (globalThis as any)[Symbol.for("omo.task.terminalObservers")].subscribe(({ record }: any) =>
      bus.emit("native-terminal", record));
    const firstProvider = event(bus, "provider-start", (row) => row.n === 0);
    const started = await call("task", { subagent_type: "explore", model: "coordination-qa/offline",
      prompt: "SETTLEMENT_NATIVE_GATE Return the gated result.", description: "Native settlement QA", run_in_background: true });
    const taskId = started.details.task_id; assert(taskId); await firstProvider;
    recordPath = path.join(sandbox, "senpi-task/tasks", `${taskId}.json`);
    const handle0 = engine.manager.getResidentHandle(taskId);
    sessions.push(carrier.runtimeOwners.get(handle0).runtime.deref());
    const first = bridge.listOwned(); assert(first.ok);
    const oldRef = first.value.find((row: any) => row.identity.task_id === taskId).identity.target_ref;
    const firstTerminal = event(bus, "native-terminal", (row) => row.task_id === taskId && row.status === "completed");
    bus.emit("release-0"); await firstTerminal; await bounded(root.waitForIdle(), "epoch0 owner idle");
    report.baseline = lifecycle(ports.store.load(taskId)); assert.equal(report.baseline.status, "completed");
    const nextProvider = event(bus, "provider-start", (row) => row.n === 1);
    await call("task_send", { to: taskId, message: "SETTLEMENT_NATIVE_GATE Return epoch1 result." });
    await nextProvider;
    assert.deepEqual(bridge.reconcileOwnedTerminal(oldRef), { ok: false, reason: "stale_epoch" });
    const next = bridge.listOwned(); assert(next.ok);
    const current = next.value.find((row: any) => row.identity.task_id === taskId);
    assert.equal(current.identity.run_epoch, 1);
    const handle = engine.manager.getResidentHandle(taskId);
    const outcome = bounded<any>(handle.waitForOutcome(), "epoch1 native outcome");
    const witness = bounded<any>(new Promise((resolve) => {
      const subscribed = bridge.subscribeOutcome(current.identity.target_ref, resolve); assert(subscribed.ok);
    }), "epoch1 bridge witness");
    const nativeWaiter = bounded<any>(engine.manager.waitFor(taskId), "original native waiter");
    if (scenario !== "nontarget") {
      const ready = event(bus, "reader-ready");
      let readerOutput = "";
      const receipt = settlementCommand([
        path.join(process.env.SystemRoot!, "System32/WindowsPowerShell/v1.0/powershell.exe"),
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", path.join(product, "qa/reader.ps1"),
        "-Path", recordPath, "-AttemptDir", attempt,
      ], { cwd, timeoutMs: 20_000, onSpawn(child) { reader = child; },
        onStdout(bytes) {
          readerOutput += bytes.toString();
          if (readerOutput.includes("READER_READY")) bus.emit("reader-ready");
        } });
      readerDone = receipt.then((result) => { report.reader_receipt = result.receiptPath; return result.exit; });
      await ready;
    }
    faultsEnabled = true;
    bus.emit("release-1");
    report.native_outcome = await outcome; report.witness = await witness;
    report.original_waiter = lifecycle(await nativeWaiter);
    assert.equal(report.native_outcome.finalResponse, "native-settlement-result-1");
    assert.equal(report.witness.run_epoch, 1);
    report.before = lifecycle(ports.store.load(taskId));
    if (scenario === "retry-success") {
      assert.equal(report.before.status, "completed");
      assert(renames.some((row) => row.run_epoch === 1 && row.applied === false));
      assert(renames.some((row) => row.run_epoch === 1 && row.status === "completed" && row.applied));
    } else {
      assert.equal(report.before.status, "running"); assert.equal(report.original_waiter.status, "error");
      assert.equal(engine.manager.getResidentHandle(taskId), undefined);
    }
    if (scenario === "revived-settlement") releaseReader();
    if (readerReleased) assert.equal(await readerDone, 0);
    const repairedWaiter = bounded<any>(engine.manager.waitFor(taskId), "replay native waiter");
    const repaired = bridge.reconcileOwnedTerminal(current.identity.target_ref);
    assert(repaired.ok, JSON.stringify(repaired)); report.settlement = repaired.value;
    if (scenario === "permanent" || scenario === "nontarget") {
      assert.equal(repaired.value.state, "degraded");
      assert.equal(repaired.value.publication, "unpublished");
      assert.equal(repaired.value.result_preserved, true); assert.equal(repaired.value.slot, "released");
      assert.equal(repaired.value.notification, "unaccepted");
      assert.equal((await repairedWaiter).status, "error");
      assert.equal(ports.store.load(taskId).notification.notified_epoch, 0);
      if (scenario === "nontarget") {
        assert.equal(repaired.value.error.code, "ENOSPC");
        assert.equal(renames.filter((row) => row.code === "ENOSPC").length, 2, "NON_TARGET_ERROR_WAS_RETRIED");
      }
    } else {
      assert.equal(repaired.value.state, "settled");
      assert.equal(repaired.value.publication, "published");
      assert.equal(repaired.value.notification, "accepted");
      assert.equal((await repairedWaiter).status, "completed");
      const output = await call("task_output", { task_id: taskId, mode: "status" });
      assert(JSON.stringify(output).includes("native-settlement-result-1"), "TASK_OUTPUT_RESULT_MISSING");
      report.task_output = output;
      const repeated = bridge.reconcileOwnedTerminal(current.identity.target_ref);
      assert(repeated.ok); assert.equal(repeated.value.replayed, false);
      report.repeated = repeated.value;
      assert.equal(notifications.filter((row) => row.task_id === taskId && row.epoch === 1).length, 1);
      assert.equal(releases.filter((row) => row.task_id === taskId && row.epoch === 1).length, 1);
      report.prior_epoch_protected = true;
    }
    report.after = lifecycle(ports.store.load(taskId)); report.renames = renames;
    report.releases = releases; report.notifications = notifications;
    const reuseRun = calls;
    const freeProvider = event(bus, "provider-start", (row) => row.n === reuseRun);
    const reused = await call("task", { subagent_type: "explore", model: "coordination-qa/offline",
      prompt: "SETTLEMENT_NATIVE_GATE Return the free slot result.", description: "Actual slot reuse", run_in_background: true });
    await freeProvider;
    const reuseId = reused.details.task_id; assert(reuseId);
    sessions.push(carrier.runtimeOwners.get(engine.manager.getResidentHandle(reuseId)).runtime.deref());
    const reusedTerminal = event(bus, "native-terminal", (row) => row.task_id === reuseId && row.status === "completed");
    bus.emit(`release-${reuseRun}`); await reusedTerminal;
    report.capacity_reused = true; report.passed = true;
  } catch (error) {
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    process.exitCode = 1;
  } finally {
    releaseReader(); if (readerDone) assert.equal(await readerDone, 0);
    for (let n = 0; n < 8; n++) bus.emit(`release-${n}`);
    stopLedger();
    for (const capture of state?.capture.list() ?? []) if (capture.runtime()) sessions.push(capture.runtime());
    for (const session of [...new Set(sessions)].reverse()) {
      if (!session || session._disposed) continue;
      await bounded(session.waitForIdle(), "native cleanup idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" }); session.dispose();
    }
    state?.dispose(); fs.renameSync = originalRename; syncBuiltinESMExports();
    if (manifest) report.adapter_removal = removeAdapter(manifest);
    const link = path.join(sandbox, "candidate/node_modules");
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
    bus.removeAllListeners();
    report.cleanup = { reader_exited: !readerDone || readerReleased, sessions_disposed: sessions.every((s) => !s || s._disposed),
      provider_listeners: bus.eventNames().length, installed_unchanged: sha256(bundle) === report.installed_sha256,
      adapter_restored: manifest ? sha256(manifest.output) === manifest.baseline_sha256 : true };
    report.finished_at = new Date().toISOString(); save(artifact, report);
  }
  return report;
}

export const cases = { "revived-settlement": async () => {
  const result = await runSettlementCase("revived-settlement");
  return { case: "revived-settlement", passed: result.passed, artifact: path.join(settlementEvidence, "settlement.json") };
} };
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const flag = (name: string) => process.argv[process.argv.indexOf(name) + 1]!;
  console.log(JSON.stringify(await nativeSettlement(flag("--native-settlement") as Scenario, flag("--sandbox"), flag("--result"))));
}

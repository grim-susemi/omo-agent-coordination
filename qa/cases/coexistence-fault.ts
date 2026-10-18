import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { stripVTControlCharacters } from "node:util";
import { randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import questionFixture from "./question-fixture.ts";
import { install } from "../../scripts/install.mjs";
import { installationFixture } from "./install-roundtrip.ts";
import { task9, runCommand, save, hashes } from "./install-receipts.ts";
import { product, senpi, sha256 } from "../command.mjs";
import { getCoordinationBroker, getCoordinationClient } from "../../src/public-api.ts";
import type { TargetRef } from "../../src/contracts.ts";

const summaryPath = "C:/Users/USER/.omo-stable-500/home/.omo/agent/extensions/omo-compaction-model.ts";
const retryPath = "C:/Users/USER/.omo-stable-500/home/.omo/agent/task-record-publish-retry/extension.mjs";
const fallbackPath = "C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg";
const herdrPath = "C:/Users/USER/orca/projects/herdr-agent-sidebar-diagnosis/repair/herdr-senpi.mjs";
const captureKey = Symbol.for("omo.agent-coordination.capture.v1");
const ok = <T>(result: { ok: true; value: T } | { ok: false; reason: string }): T => {
  assert(result.ok, JSON.stringify(result)); return result.value;
};
async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`COEXISTENCE_DEADLINE:${label}`)), 12_000);
    })]);
  } finally { clearTimeout(timer!); }
}
function event(bus: EventEmitter, name: string, predicate: (data: any) => boolean = () => true) {
  let listener: (data: any) => void;
  const promise = new Promise<any>((resolve) => {
    listener = (data) => { if (predicate(data)) { bus.off(name, listener); resolve(data); } };
    bus.on(name, listener);
  });
  return bounded(promise, name).finally(() => bus.off(name, listener));
}

/** Reuse the established offline model only. Engine, store, runner, runtime,
 * extension factories and all four coexistence mods are their real modules. */
export default async function coexistenceFixture(pi: any) {
  await questionFixture(pi);
  pi.on("session_before_compact", () => {
    (globalThis as any)[Symbol.for("coordination.question.qa.bus")].emit("user-compact-hook");
  });
}

export async function runCoexistence(): Promise<Record<string, any>> {
  const orders: Record<string, any>[] = [];
  for (const order of ["package-first", "extension-last"]) {
    const fixture = installationFixture();
    const installed = await install({ agentDir: fixture.agentDir, adapterTarget: fixture.target });
    const fixtureDir = path.join(fixture.directory, "fixture");
    const built = await Bun.build({
      entrypoints: [fileURLToPath(import.meta.url)], outdir: fixtureDir, naming: "fixture.js",
      target: "node", format: "esm", external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"],
    });
    assert(built.success, String(built.logs));
    const packageDir = installed.package_dir;
    const entry = path.join(packageDir, "dist/extension.js");
    const settings = {
      // The first order also declares the entry as an explicit CLI resource,
      // exercising the native loader's precedence and package deduplication.
      packages: order === "package-first" ? [packageDir, fallbackPath] : [fallbackPath],
      extensions: [summaryPath, retryPath, herdrPath,
        ...(order === "extension-last" ? [path.join(fallbackPath, "omo-task-no-fallback.js"), entry] : []),
        path.join(fixtureDir, "fixture.js")],
      compaction: { enabled: false, model: "coordination-question/offline-alt" },
      retry: { enabled: false }, sessionTitle: { enabled: false },
    };
    const settingsFile = path.join(fixture.agentDir, "settings.json");
    const installedSettings = fs.readFileSync(settingsFile);
    fs.writeFileSync(settingsFile, JSON.stringify(settings));
    const modules = path.join(fixtureDir, "node_modules");
    fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", modules, "junction");
    const nativeModules = path.join(path.dirname(fixture.target), "node_modules");
    fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", nativeModules, "junction");
    const artifact = path.join(fixture.directory, "coexistence.json");
    try {
      const command = await runCommand(["node", fileURLToPath(import.meta.url), "--coexistence-native",
        order, fixture.agentDir, fixture.target, artifact], `coexistence-${order}`);
      assert.equal(command.code, 0, command.output);
      const report = JSON.parse(fs.readFileSync(artifact, "utf8"));
      assert(report.passed, JSON.stringify(report));
      orders.push({ ...report, command_receipt: command.receipt });
    } finally {
      fs.unlinkSync(modules);
      fs.unlinkSync(nativeModules);
      fs.writeFileSync(settingsFile, installedSettings);
      await install({ agentDir: fixture.agentDir, action: "uninstall" });
    }
  }
  const report = { case: "coexistence-fault", passed: true, source_sha256: hashes(), orders,
    actual_mod_sha256: Object.fromEntries([summaryPath, retryPath, herdrPath,
      path.join(fallbackPath, "omo-task-no-fallback.js")].map((file) => [file, sha256(file)])) };
  save("coexistence", report);
  return report;
}

async function nativeCase(order: string, agentDir: string, target: string, artifact: string) {
  assert(path.resolve(agentDir).startsWith(path.resolve(task9) + path.sep));
  process.env.COORDINATION_CANDIDATE_BUNDLE = target;
  const cwd = path.join(path.dirname(agentDir), "work"); fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true });
  process.chdir(cwd);
  fs.writeFileSync(path.join(cwd, ".omo/omo.json"), JSON.stringify({
    agents: { explore: { model: "coordination-question/offline", execution_mode: "in-process" } },
    task: { state_dir: path.join(path.dirname(agentDir), "senpi-task"), default_execution_mode: "in-process",
      default_concurrency: 1, global_concurrency: 1, host_shard_prewarm: "off" },
  }));
  const global = globalThis as any, bus = new EventEmitter();
  global[Symbol.for("coordination.question.qa.bus")] = bus;
  const counts = { main: 0, side: 0, payloads: [], main_tools: {} };
  global[Symbol.for("coordination.question.qa.counts")] = counts;
  const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
  const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
  const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
  const nativeBind = sdk.AgentSession.prototype.bindExtensions;
  let priorCalls = 0;
  const prior = async function (this: any, ...args: any[]) {
    priorCalls++; return Reflect.apply(nativeBind, this, args);
  };
  sdk.AgentSession.prototype.bindExtensions = prior;
  const writes = fs.writeFileSync, renames = fs.renameSync;
  let priorWrites = 0;
  fs.writeFileSync = ((...args: any[]) => { priorWrites++; return Reflect.apply(writes, fs, args); }) as typeof fs.writeFileSync;
  const loader = new sdk.DefaultResourceLoader({ cwd, agentDir, noSkills: true, noPromptTemplates: true,
    noThemes: true, noContextFiles: true, settingsManager: sdk.SettingsManager.create(cwd, agentDir),
    additionalExtensionPaths: order === "package-first" ? [path.join(agentDir, "packages/piomo-agent-coordination-mod/dist/extension.js")] : [] });
  const sessions: any[] = [];
  let root: any, child: any, stopTerminal = () => {};
  const report: Record<string, any> = { order, passed: false, frames: [], reloads: 0 };
  try {
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, [], "COEXISTENCE_ACTUAL_LOADER_FAILED");
    report.loader_paths = loader.getExtensions().extensions.map((e: any) => e.path);
    for (const required of [summaryPath, retryPath, herdrPath, path.join(fallbackPath, "omo-task-no-fallback.js")]) {
      assert(report.loader_paths.some((file: string) => path.resolve(file) === path.resolve(required)), `ACTUAL_MOD_MISSING:${required}`);
    }
    const entryIndex = report.loader_paths.findIndex((file: string) => file.endsWith("dist\\extension.js") || file.endsWith("dist/extension.js"));
    assert.equal(report.loader_paths.filter((file: string) => path.resolve(file) ===
      path.resolve(agentDir, "packages/piomo-agent-coordination-mod/dist/extension.js")).length, 1);
    const summaryIndex = report.loader_paths.findIndex((file: string) => path.resolve(file) === path.resolve(summaryPath));
    assert(order === "package-first" ? entryIndex < summaryIndex : entryIndex > summaryIndex, "LOAD_ORDER_NOT_OBSERVED");
    const auth = AuthStorage.inMemory(), registry = ModelRegistry.inMemory(auth);
    root = (await sdk.createAgentSession({ cwd, agentDir, resourceLoader: loader, authStorage: auth,
      modelRegistry: registry, settingsManager: sdk.SettingsManager.create(cwd, agentDir),
      sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    sessions.push(root);
    await root.bindExtensions({ mode: "print" });
    await root.setModel(registry.find("coordination-question", "offline"));
    root.setSessionThinkingLevel("high");
    const state = global[captureKey], runner = root.extensionRunner, ctx = runner.createContext();
    assert(state.capture.status.ok, JSON.stringify([...state.unsupported]));
    const holder = global[Symbol.for("omo.senpi-task.senpiBarrel")];
    const descriptor = Object.getOwnPropertyDescriptor(holder, "module");
    const guardWrite = fs.writeFileSync, guardRename = fs.renameSync;
    assert((guardWrite as any)[Symbol.for("omo.custom.taskNoFallback.v1")] === true);
    assert((guardRename as any)[Symbol.for("omo.custom.taskRecordPublishRetry.v1")] === true);
    assert.equal(root.settingsManager.getRetryFallbackSettings().modelFallback, false);
    const execute = (tool: string, args: any, signal?: AbortSignal) =>
      runner.getToolDefinition(tool).execute(randomUUID(), args, signal, undefined, ctx);
    const mainStarted = event(bus, "main-latch", (data) => data.label === "coexist");
    const completed = new Map<string, any>();
    stopTerminal = global[Symbol.for("omo.task.terminalObservers")].subscribe(({ record }: any) => {
      if (["completed", "error", "cancelled", "interrupted", "lost"].includes(record.status)) {
        completed.set(record.task_id, record); bus.emit("terminal", record);
      }
    });
    const launched = await execute("task", { subagent_type: "explore", model: "coordination-question/offline",
      prompt: "QUESTION_NATIVE_CHILD:coexist", description: "Owned coexistence latch", run_in_background: true });
    const id = launched.details?.task_id; assert(id, JSON.stringify(launched));
    await mainStarted;
    const bridge = state.bridges.get(root.sessionManager);
    const row = ok<any[]>(bridge.listOwned()).find((row) => row.identity.task_id === id);
    assert(row);
    const ref = row.identity.target_ref as TargetRef;
    child = state.capture.list().map((row: any) => row.runtime()).find((runtime: any) => runtime?.sessionId.replace(/^senpi:/, "") === row.identity.session_id);
    assert(child); sessions.push(child);
    child.setSessionThinkingLevel("high");
    const client = ok(getCoordinationClient(ctx));
    const inspect = await execute("coordination_inspect", { target_ref: ref });
    assert(inspect.details.ok);
    const answer = await execute("coordination_ask", { target_ref: ref, question: "State your current evidence.", request_id: "one-answer" });
    assert(answer.details.ok && answer.details.value.reply === "peer-nonce", JSON.stringify(answer));
    const note = await execute("coordination_notify", { target_ref: ref, message: "Owned pending note", request_id: "owned-note" });
    assert(note.details.ok && note.details.value.incorporation === "queued_for_next_turn");
    await child.sendCustomMessage({ customType: "foreign-mod-note", content: "foreign queued message",
      display: false, details: { owner: "not-coordination" } }, { deliverAs: "nextTurn", triggerTurn: false });
    assert(child._pendingNextTurnMessages.some((message: any) => message.customType === "omo-agent-coordination-peer-v1"));
    const themeModule = await import(pathToFileURL(senpi + "/dist/modes/interactive/theme/theme.js").href);
    sdk.initTheme("dark");
    fs.mkdirSync(path.join(task9, "frames"), { recursive: true });
    for (const width of [32, 80]) for (const [name, result] of [["inspect", inspect], ["answer", answer], ["notify", note]] as const) {
      const definition = state.tools.find((tool: any) => tool.name === `coordination_${name === "answer" ? "ask" : name}`);
      const component = definition.renderResult(result, { expanded: true, isPartial: false }, themeModule.theme);
      const lines: string[] = component.render(width);
      assert(lines.length > 0 && lines.every((line) => sdk.visibleWidth(line) <= width), `RENDER_OVERFLOW:${width}:${name}`);
      assert(lines.map(stripVTControlCharacters).join("").replace(/\s/g, "")
        .includes(name === "answer" ? "peer-nonce" : name === "notify" ? "queued_for_next_turn" : "record_consistent"),
      `RENDER_CONTENT_LOST:${width}:${name}`);
      const frame = path.join(task9, "frames", `${order}-${width}-${name}.ansi.txt`);
      fs.writeFileSync(frame, lines.join("\n") + "\n"); report.frames.push(frame);
    }
    // Exercise the real summary mod's override/fallback dispatcher and actual
    // runner. Only the builtin's expensive summarization is event-controlled.
    const compact = runner.extensions.find((extension: any) => extension.path === "<builtin:compaction>");
    assert(compact, "BUILTIN_COMPACTION_MISSING");
    const handlers = compact.handlers.get("session_before_compact");
    const originalHandlers = [...handlers];
    const picked: string[] = []; let userCalls = 0;
    const onUser = () => { userCalls++; }; bus.on("user-compact-hook", onUser);
    handlers.splice(0, handlers.length, async () => {
      const model = runner.getCompactionModelFn(); picked.push(model.id);
      return picked.length === 1 ? { cancel: true, reason: "controlled summary failure" } : { compaction: { summary: "native fallback succeeded" } };
    });
    try {
      const fallback = await runner.emit({ type: "session_before_compact", reason: "manual",
        requestId: "coexist-summary", signal: new AbortController().signal });
      assert(fallback?.compaction, JSON.stringify(fallback));
      assert.deepEqual(picked, ["offline-alt", "offline"]);
      assert.equal(userCalls, 1, "USER_COMPACTION_HOOK_REPLAYED");
      report.summary_fallback = true;
    } finally { handlers.splice(0, handlers.length, ...originalHandlers); bus.off("user-compact-hook", onUser); }
    const pendingStart = event(bus, "side-start", (data) => data.nonce === "pending-reload");
    const pending = client.ask({ target_ref: ref, question: "CONTROL:gate NONCE:pending-reload",
      request_id: "pending-before-dispose", depth: "deep" });
    await Promise.race([pendingStart, pending.then((result) => {
      throw new Error(`SIDE_REQUEST_ENDED_BEFORE_GATE:${JSON.stringify(result)}`);
    })]);
    const aborted = event(bus, "side-aborted", (data) => data.nonce === "pending-reload");
    const sideCount = counts.side;
    await state.dispose(); await aborted;
    const cancelled = await bounded(pending, "pending side cancellation");
    assert(!cancelled.ok || cancelled.value.state === "cancelled", JSON.stringify(cancelled));
    assert.equal(child.isStreaming, true, "COORDINATION_DISPOSAL_ABORTED_MAIN");
    assert(child._pendingNextTurnMessages.some((message: any) => message.customType === "foreign-mod-note"));
    assert(!child._pendingNextTurnMessages.some((message: any) => message.customType === "omo-agent-coordination-peer-v1"));
    assert.deepEqual(client.peers(), { ok: false, reason: "stale_generation" });
    assert.deepEqual(getCoordinationBroker(), { ok: false, reason: "coordination_unavailable" });
    assert.deepEqual(Object.getOwnPropertyDescriptor(holder, "module"), descriptor);
    assert.equal(fs.writeFileSync, guardWrite); assert.equal(fs.renameSync, guardRename);
    assert.equal(sdk.AgentSession.prototype.bindExtensions, prior);
    bus.emit("release-side:pending-reload");
    const done = event(bus, "terminal", (record) => record.task_id === id);
    bus.emit("release-main:coexist");
    const terminal = completed.get(id) ?? await done;
    assert.equal(terminal.status, "completed");
    assert.equal(terminal.final_response, "native-main-result:coexist");
    await bounded(root.waitForIdle(), "completion notification");
    assert.equal(counts.side, sideCount);
    for (let index = 0; index < 2; index++) {
      await bounded(root.reload(), "actual SDK reload");
      report.reloads++;
      const next = global[captureKey]; assert(next.generation > state.generation);
      const missing = await root.extensionRunner.getToolDefinition("coordination_inspect").execute(
        randomUUID(), {}, undefined, undefined, root.extensionRunner.createContext());
      assert.equal(missing.details.connection, "capture_missing", JSON.stringify(missing));
      assert.equal(next.capture.list().length, 0, "OLD_RUNTIME_WAS_AUTOMATICALLY_RESTARTED_OR_CAPTURED");
      await root.bindExtensions({ mode: "print" });
      assert.equal(next.capture.list().length, 1);
      assert.equal(counts.side, sideCount, "RELOAD_REPLAYED_PAID_REQUEST");
    }
    process.env.OMO_AGENT_COORDINATION = "0";
    await bounded(root.reload(), "disabled reload");
    assert.equal(global[captureKey].capture.status.ok, false);
    assert.equal(global[captureKey].capture.status.connection, "mode_disabled");
    assert.equal(root.extensionRunner.getToolDefinition("coordination_ask"), undefined);
    assert.equal(sdk.AgentSession.prototype.bindExtensions, prior);
    assert.deepEqual(getCoordinationBroker(), { ok: false, reason: "coordination_unavailable" });
    delete process.env.OMO_AGENT_COORDINATION;
    report.descriptor_preserved = true; report.native_completed = true; report.stale_client_refused = true;
    report.queue_foreign_preserved = true; report.side_replayed = false; report.capture_missing = true;
    report.prior_wrapper_calls = priorCalls; assert(priorCalls >= 4); assert(priorWrites > 0);
    report.prior_fs_wrapper_calls = priorWrites; report.side_provider_calls = counts.side;
    report.cancelled_side = cancelled; report.main_outcome = terminal.status;
    report.passed = true;
  } catch (error) {
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    throw error;
  } finally {
    bus.emit("release-side:pending-reload"); bus.emit("release-main:coexist"); stopTerminal();
    await global[captureKey]?.dispose();
    for (const session of [...new Set(sessions)].reverse()) if (!session._disposed) {
      await bounded(session.waitForIdle(), "cleanup native idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    }
    report.cleanup = { sessions_disposed: sessions.every((session) => session._disposed),
      capture_restored: sdk.AgentSession.prototype.bindExtensions === prior,
      module_listeners: global[Symbol.for("omo.agent-coordination.native.v1")]?.moduleListeners.size ?? 0,
      pending_provider_gates: bus.listenerCount("release-side:pending-reload") + bus.listenerCount("release-main:coexist") };
    sdk.AgentSession.prototype.bindExtensions = nativeBind;
    fs.writeFileSync = writes; fs.renameSync = renames;
    fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
  }
}
export const cases = { "coexistence-fault": runCoexistence };
if (path.resolve(process.argv[1] ?? "") === fileURLToPath(import.meta.url) && process.argv.includes("--coexistence-native")) {
  const index = process.argv.indexOf("--coexistence-native");
  await nativeCase(process.argv[index + 1]!, process.argv[index + 2]!, process.argv[index + 3]!, process.argv[index + 4]!);
}

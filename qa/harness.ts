import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { syncBuiltinESMExports } from "node:module";
import { fileURLToPath, pathToFileURL } from "node:url";
import { randomUUID, createHash } from "node:crypto";
import type { ChildProcess } from "node:child_process";
import { attempt, evidence, bundle, senpi, isolatedEnv, sourceHashes, sha256, command } from "./command.mjs";

export const MISMATCH = "CURRENT_EPOCH_OUTCOME_COMPLETED_CANONICAL_RUNNING";
type CaseName = "normal-baseline" | "revived-stale-record";
type Json = Record<string, unknown>;
type NativeTool = {
  name: string;
  execute(id: string, args: Json, signal: AbortSignal | undefined, update: unknown, ctx: unknown): Promise<unknown>;
};
function inside(root: string, file: string): boolean {
  const rel = path.relative(path.resolve(root), path.resolve(file));
  return rel === "" || (!path.isAbsolute(rel) && rel !== ".." && !rel.startsWith(".." + path.sep));
}
function waitFor(bus: EventEmitter, event: string, predicate: (data: Json) => boolean = () => true) {
  let timer: ReturnType<typeof setTimeout>;
  let handler: (data: Json) => void;
  const promise = new Promise<Json>((resolve, reject) => {
    handler = (data) => {
      if (!predicate(data)) return;
      clearTimeout(timer);
      bus.off(event, handler);
      resolve(data);
    };
    bus.on(event, handler);
    timer = setTimeout(() => {
      bus.off(event, handler);
      reject(new Error(`SETUP_EVENT_DEADLINE: ${event}`));
    }, 15_000);
  });
  return promise;
}
function observeSandbox(sandbox: string, bus: EventEmitter) {
  const operations: Json[] = [];
  const originals = {
    writeFileSync: fs.writeFileSync, renameSync: fs.renameSync,
    openSync: fs.openSync, writeSync: fs.writeSync,
  };
  const logFds = new Map<number, string>();
  const renameAttempts = new Map<string, number>();
  const taskPath = (file: unknown): file is string => typeof file === "string" &&
    inside(sandbox, file) && /[\\/]tasks[\\/]st_[a-z0-9]+\.json(?:\.\d+\.[a-f0-9]+\.tmp)?$/i.test(file);
  const parsed = (text: string) => {
    try { return JSON.parse(text) as Json; } catch { return {}; }
  };
  const snap = (file: string) => {
    if (!fs.existsSync(file)) return {};
    const bytes = fs.readFileSync(file);
    const record = parsed(bytes.toString());
    return { status: record.status, residency: record.residency_state,
      epoch: (record.notification as Json | undefined)?.run_epoch,
      hash: createHash("sha256").update(bytes).digest("hex") };
  };
  fs.writeFileSync = ((...args: Parameters<typeof fs.writeFileSync>) => {
    const [file, data] = args;
    if (!taskPath(file)) return Reflect.apply(originals.writeFileSync, fs, args);
    const operation: Json = { op: "write", path: file, time: new Date().toISOString(),
      stack: new Error().stack, ...parsed(String(data)), hash: createHash("sha256").update(String(data)).digest("hex") };
    // Store only lifecycle fields, not task prompts, credentials, or transcripts.
    const record = parsed(String(data));
    delete operation.prompt;
    for (const key of Object.keys(record)) delete operation[key];
    Object.assign(operation, { status: record.status, epoch: (record.notification as Json | undefined)?.run_epoch });
    try {
      const result = Reflect.apply(originals.writeFileSync, fs, args);
      operation.error = null;
      operations.push(operation);
      bus.emit("record-write", operation);
      return result;
    } catch (error) {
      operations.push({ ...operation, error: String(error), code: (error as NodeJS.ErrnoException).code });
      throw error;
    }
  }) as typeof fs.writeFileSync;
  fs.renameSync = ((src, dest) => {
    if (!taskPath(String(src)) || !taskPath(String(dest))) return originals.renameSync(src, dest);
    const key = String(src);
    const count = (renameAttempts.get(key) ?? 0) + 1;
    renameAttempts.set(key, count);
    const operation: Json = { op: "rename", src, path: dest, time: new Date().toISOString(),
      retry_count: count - 1, stack: new Error().stack, ...snap(String(src)), previous: snap(String(dest)) };
    try {
      const result = originals.renameSync(src, dest);
      operation.error = null;
      operations.push(operation);
      bus.emit("record-rename", operation);
      return result;
    } catch (error) {
      operations.push({ ...operation, error: String(error), code: (error as NodeJS.ErrnoException).code });
      throw error;
    }
  }) as typeof fs.renameSync;
  fs.openSync = ((...args: Parameters<typeof fs.openSync>) => {
    const fd = Reflect.apply(originals.openSync, fs, args);
    if (typeof args[0] === "string" && inside(sandbox, args[0]) && /[\\/]logs[\\/].*\.jsonl$/.test(args[0])) {
      logFds.set(fd, args[0]);
    }
    return fd;
  }) as typeof fs.openSync;
  fs.writeSync = ((...args: Parameters<typeof fs.writeSync>) => {
    const result = Reflect.apply(originals.writeSync, fs, args);
    if (logFds.has(args[0])) {
      const text = typeof args[1] === "string" ? args[1] : Buffer.from(args[1]).toString();
      for (const line of text.trim().split("\n")) {
        const event = parsed(line);
        bus.emit("native-event", { ...event, task_id: path.basename(logFds.get(args[0])!, ".jsonl") });
      }
    }
    return result;
  }) as typeof fs.writeSync;
  syncBuiltinESMExports();
  return { operations, restore() { Object.assign(fs, originals); syncBuiltinESMExports(); } };
}

/** Native installed component, engine, manager, store, runner, handle, and SDK session.
 * Only the extension host and an offline provider are controlled by the test.
 */
export async function runCase(name: CaseName, schedule = name === "revived-stale-record" ? "r2-closest-reader" : "after-send"): Promise<Json & { artifact: string }> {
  assert(["normal-baseline", "revived-stale-record"].includes(name));
  // Bun snapshots named fs imports; Node's syncBuiltinESMExports makes the native
  // bundle's write/rename observation reliable without changing its installed bytes.
  if (process.versions.bun && schedule !== "r2-closest-bun") {
    const resultPath = path.join(evidence, `native-result-${randomUUID()}.json`);
    const exit = await command(["node", fileURLToPath(import.meta.url), "--native-case", name, "--schedule", schedule, "--result", resultPath]);
    assert.equal(exit, 0, "NATIVE_HOST_RUN_FAILED: see nested command receipt");
    return JSON.parse(fs.readFileSync(resultPath, "utf8")) as Json & { artifact: string };
  }
  const sandbox = path.join(attempt, "runs", `${name}-${randomUUID()}`);
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true });
  const previousCwd = process.cwd();
  const previousEnv = { ...process.env };
  const env = isolatedEnv(sandbox);
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  process.chdir(cwd);
  const bus = new EventEmitter();
  const observations = observeSandbox(sandbox, bus);
  let shutdown: (() => Promise<void>) | undefined;
  let restoreSessionCapture: (() => Promise<void>) | undefined;
  let reader: ChildProcess | undefined;
  let readerDone: Promise<number> | undefined;
  const report: Json = { case: name, schedule, sandbox, cwd, started_at: new Date().toISOString(),
    source_sha256: sourceHashes(), module_sha256: { [bundle]: sha256(bundle) },
    isolation: { home: env.HOME, agentDir: env.OMO_CODING_AGENT_DIR, tmp: env.TMP,
      endpoints: env.OMO_RPC_DIR }, provider_calls: 0 };
  const artifact = path.join(evidence, `${name}-${path.basename(sandbox)}.json`);
  try {
    const native = await import(pathToFileURL(bundle).href);
    const closest = schedule.startsWith("r2-closest");
    const storeConfig = { project_dir: cwd, ...(closest ? { task: { state_dir: path.join(cwd, ".omo/senpi-task") } } : {}) };
    const stateDir = native.resolveStateDir(storeConfig);
    assert(inside(attempt, stateDir), `TASK_STORE_ESCAPED_ATTEMPT: ${stateDir}`);
    assert(inside(sandbox, stateDir));
    report.resolved_task_store = stateDir;
    report.task_store_asserted_before_run = true;
    const store = native.createTaskRecordStore(storeConfig);
    assert.equal(store.stateDir, stateDir);
    fs.writeFileSync(path.join(cwd, ".omo", "omo.json"), JSON.stringify({
      agents: { explore: { model: "coordination-qa/offline" } },
      task: { default_execution_mode: "in-process", global_concurrency: schedule.startsWith("r2-closest") ? 2 : 1,
        default_concurrency: schedule.startsWith("r2-closest") ? 2 : 1, host_shard_prewarm: "off",
        ...(closest ? { state_dir: stateDir } : {}) },
    }));

    const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    type CapturedSession = { sessionId: string; readonly _disposed?: boolean;
      extensionRunner?: { hasHandlers(event: string): boolean; emit(event: unknown): Promise<unknown> };
      getLastAssistantText(): string | undefined;
      subscribe(fn: (event: { type: string; message?: { role?: string; stopReason?: string } }) => void): () => void;
      dispose(): void };
    const sessions: CapturedSession[] = [];
    const nativeSessionEvents: Json[] = [];
    const stopObserving: (() => void)[] = [];
    const bindExtensions = sdk.AgentSession.prototype.bindExtensions;
    sdk.AgentSession.prototype.bindExtensions = function (this: CapturedSession, ...args: unknown[]) {
      if (!sessions.includes(this)) {
        sessions.push(this);
        stopObserving.push(this.subscribe((event) => {
          if (event.type === "agent_end" || event.type === "message_end") {
            const observation = { type: event.type, session_id: this.sessionId,
              stop_reason: event.message?.stopReason, role: event.message?.role,
              observed_at: new Date().toISOString(), final_text: this.getLastAssistantText() };
            nativeSessionEvents.push(observation);
            bus.emit("native-session-event", observation);
          }
        }));
      }
      return Reflect.apply(bindExtensions, this, args);
    };
    restoreSessionCapture = async () => {
      sdk.AgentSession.prototype.bindExtensions = bindExtensions;
      stopObserving.forEach((unsubscribe) => unsubscribe());
      for (const session of sessions) {
        if (session._disposed) continue;
        if (session.extensionRunner?.hasHandlers("session_shutdown")) {
          await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
        }
        session.dispose();
      }
    };
    if (schedule.startsWith("r2-closest")) {
      const fallbackPackage = "C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg";
      const retryExtension = "C:/Users/USER/.omo-stable-500/home/.omo/agent/extensions/omo-task-record-publish-retry.js";
      // Fresh in-memory settings contain only the two read-only local code paths.
      // Native package discovery determines their order; no live settings/auth are copied.
      const loader = new sdk.DefaultResourceLoader({
        cwd, agentDir: env.OMO_CODING_AGENT_DIR, noSkills: true, noPromptTemplates: true,
        noThemes: true, noContextFiles: true,
        settingsManager: sdk.SettingsManager.inMemory({
          packages: [fallbackPackage], extensions: [retryExtension],
        }),
      });
      await loader.reload();
      const loaded = loader.getExtensions();
      assert.equal(loaded.errors.length, 0, "SETUP_INSTALLED_MOD_LOADER_ERROR");
      report.native_mod_load_order = loaded.extensions.map((extension: { path: string }) => extension.path)
        .filter((file: string) => file.includes("task-no-fallback") || file.includes("publish-retry"));
      report.mod_load_order_source = "native DefaultResourceLoader with package and extension entry types matching current stable settings";
      report.r2_conditions = {
        runner: "in-process", revival: "task_send to completed resident explore child",
        parent_busy: true, companion_completion_overlap: true,
        historical_mod_order: "R2 does not record runtime load order or prove retry was loaded; current stable order is tested",
        differences: ["offline provider instead of ollama/deepseek-v4.1-flash high",
          "event-controlled short transcript instead of the original investigation",
          "historical filesystem sharing and exact parent schedule were not recorded"],
      };
    }
    if (schedule === "installed-mods") {
      const retryPath = "C:/Users/USER/.omo-stable-500/home/.omo/agent/task-record-publish-retry/extension.mjs";
      const fallbackPath = "C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg/omo-task-no-fallback.js";
      const retry = await import(pathToFileURL(retryPath).href);
      const fallback = await import(pathToFileURL(fallbackPath).href);
      retry.install();
      fallback.installRecordGuard();
      fallback.installBarrelGuard();
      fallback.installSettingsGuard(sdk.SettingsManager);
      report.installed_mods = { [retryPath]: sha256(retryPath), [fallbackPath]: sha256(fallbackPath) };
    }
    const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const { AssistantMessageEventStream } = await import(pathToFileURL(
      "C:/Users/USER/.bun/install/global/node_modules/@earendil-works/pi-ai/dist/utils/event-stream.js").href);
    const registry = ModelRegistry.inMemory(AuthStorage.inMemory());
    let streamNumber = 0;
    registry.registerProvider("coordination-qa", {
      api: "openai-completions", apiKey: "offline-not-a-credential", baseUrl: "http://offline.invalid",
      models: [{ id: "offline", name: "Offline event gate", reasoning: false,
        input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 262144, maxTokens: 4096 }],
      streamSimple: () => {
        const stream = new AssistantMessageEventStream();
        const n = streamNumber++;
        report.provider_calls = streamNumber;
        bus.once(`release-${n}`, () => {
          const message = { role: "assistant", api: "openai-completions", provider: "coordination-qa",
            model: "offline", content: [{ type: "text", text: `native-result-${n}` }], stopReason: "stop",
            timestamp: Date.now(), usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0,
              totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
          stream.push({ type: "start", partial: message });
          stream.push({ type: "done", reason: "stop", message });
          stream.end(message);
        });
        bus.emit("provider-start", { n });
        return stream;
      },
    });
    const model = registry.find("coordination-qa", "offline");
    assert(model, "SETUP_OFFLINE_MODEL_UNAVAILABLE");
    const tools = new Map<string, NativeTool>();
    const handlers = new Map<string, ((payload: unknown, ctx: unknown) => unknown)[]>();
    const notifications: unknown[] = [];
    const owner = "coordination-qa-owner";
    let parentBusy = false;
    const ctx = { cwd, agentDir: env.OMO_CODING_AGENT_DIR, mode: "print", hasUI: false,
      modelRegistry: registry, model, modelRuntime: registry.modelRuntime,
      isIdle: () => !parentBusy, isProjectTrusted: () => true, hasPendingMessages: () => false,
      sessionManager: { getSessionId: () => owner, getSessionFile: () => undefined, getEntries: () => [] },
      ui: { setStatus() {}, setWidget() {}, notify() {} } };
    const pi = {
      cwd, on(event: string, fn: (payload: unknown, ctx: unknown) => unknown) {
        handlers.set(event, [...(handlers.get(event) ?? []), fn]);
      },
      registerTool(tool: NativeTool) { tools.set(tool.name, tool); },
      registerFlag() {}, getFlag() { return undefined; }, registerCommand() {},
      registerMessageRenderer() {}, registerRemovedToolHint() {},
      getSessionContext() { return undefined; },
      sendMessage(message: unknown) { notifications.push(message); bus.emit("notification", { message }); },
      sendUserMessage() {},
      events: { on(event: string, listener: (data: Json) => void) {
        bus.on(event, listener); return () => bus.off(event, listener);
      }, emit(event: string, data: Json) { bus.emit(event, data); } },
    };
    const dispatch = async (event: string, payload: unknown) => {
      for (const handler of handlers.get(event) ?? []) await handler(payload, ctx);
    };
    await native.createTaskComponent({ resolveCwd: () => cwd }).register(pi, {
      config: { getFlag() { return undefined; } },
      logger: { info() {}, warn(...args: unknown[]) { console.error(...args); }, error() {}, debug() {} },
      getCapturedTools: () => [...tools.values()],
    });
    const ledger = (globalThis as unknown as Record<symbol, {
      subscribe(fn: (edge: { record: { status: string; task_id: string; notification: { run_epoch: number } } }) => void): () => void;
    }>)[Symbol.for("omo.task.terminalObservers")];
    assert(ledger, "SETUP_NATIVE_TERMINAL_LEDGER_MISSING");
    const detachTerminal = ledger.subscribe((edge) => bus.emit("native-terminal", {
      status: edge.record.status, epoch: edge.record.notification.run_epoch, task_id: edge.record.task_id,
    }));
    assert(tools.has("task") && tools.has("task_send"), "SETUP_NATIVE_TASK_TOOLS_NOT_REGISTERED");
    shutdown = async () => {
      detachTerminal();
      await dispatch("session_shutdown", { type: "session_shutdown", reason: "quit" });
    };
    await dispatch("session_start", { type: "session_start" });
    const call = async (name: string, args: Json) => tools.get(name)!.execute(randomUUID(), args, undefined, undefined, ctx);
    const firstProvider = waitFor(bus, "provider-start", (data) => data.n === 0);
    const started = await call("task", { subagent_type: "explore", model: "coordination-qa/offline",
      prompt: "Return the offline fixture result.", description: "Isolated baseline", run_in_background: true });
    report.spawn = started;
    await firstProvider;
    const records = store.list().records;
    assert.equal(records.length, 1, "SETUP_EXPECTED_ONE_NATIVE_TASK");
    const task = records[0];
    const taskId = task.task_id;
    report.task_id = taskId;
    report.epoch0_running = task;
    const terminal = (epoch: number, id?: string) => waitFor(bus, "native-terminal", (op) =>
      op.status === "completed" && op.epoch === epoch && (id === undefined || op.task_id === id));
    const firstTerminal = terminal(0);
    bus.emit("release-0");
    await firstTerminal;
    const baseline = store.load(taskId);
    report.epoch0_terminal = baseline;
    assert.equal(baseline.status, "completed", "BASELINE_RECORD_NOT_COMPLETED");
    assert.equal(baseline.notification.run_epoch, 0);
    assert.equal(baseline.notification.notified_epoch, 0, "BASELINE_NOTIFICATION_EPOCH_MISMATCH");
    assert.equal(baseline.final_response, "native-result-0");
    report.native_barrel = {
      path: senpi + "/dist/index.js",
      same_namespace: (globalThis as unknown as Record<symbol, { module?: unknown }>)[
        Symbol.for("omo.senpi-task.senpiBarrel")]?.module === sdk,
    };
    report.native_runtime = { executable: process.execPath, node: process.version, bun: process.versions.bun ?? null };
    if (name === "revived-stale-record") {
      let siblingId: string | undefined;
      if (closest) {
        const siblingProvider = waitFor(bus, "provider-start", (data) => data.n === 1);
        const sibling = await call("task", { subagent_type: "explore", model: "coordination-qa/offline",
          prompt: "Return the companion fixture result.", description: "R2 companion", run_in_background: true });
        await siblingProvider;
        siblingId = (sibling as { details: { task_id: string } }).details.task_id;
        report.sibling_running = store.load(siblingId);
        parentBusy = true;
        await dispatch("before_agent_start", { type: "before_agent_start" });
      }
      const runNumber = closest ? 2 : 1;
      const nextProvider = waitFor(bus, "provider-start", (data) => data.n === runNumber);
      const readerCase = schedule === "r2-closest-reader";
      const releaseCase = schedule === "r2-closest-release";
      const nextTerminal = readerCase || releaseCase ? undefined : terminal(1, taskId);
      if (schedule === "early") {
        bus.once("provider-start", () => bus.emit("release-1"));
      }
      const revived = await call("task_send", { to: taskId, message: "Return the next offline fixture result." });
      report.revive = revived;
      await nextProvider;
      const running = store.load(taskId);
      report.epoch1_running = running;
      assert.equal(running.notification.run_epoch, 1, "SETUP_REVIVAL_DID_NOT_ADVANCE_EPOCH");
      if (readerCase) {
        const ready = waitFor(bus, "reader-ready");
        let output = "";
        readerDone = command([
          path.join(process.env.SystemRoot!, "System32/WindowsPowerShell/v1.0/powershell.exe"),
          "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
          path.join(path.dirname(fileURLToPath(import.meta.url)), "reader.ps1"),
          "-Path", path.join(stateDir, "tasks", `${taskId}.json`), "-AttemptDir", attempt,
        ], {
          cwd, timeoutMs: 30_000,
          onSpawn(child) { reader = child; },
          onStdout(data) {
            output += data.toString();
            if (output.includes("READER_READY")) bus.emit("reader-ready", { output });
          },
        });
        report.reader_ready = await ready;
      }
      if (releaseCase) {
        const released = waitFor(bus, "record-rename", (operation) =>
          operation.epoch === 1 && operation.residency === "persisted_only");
        const settling = dispatch("session_shutdown", { type: "session_shutdown", reason: "reload" });
        report.residency_release = await released;
        bus.emit(`release-${runNumber}`);
        bus.emit("release-1");
        await settling;
        shutdown = undefined;
      } else
      if (schedule !== "early") {
        assert.equal(running.status, "running", "SETUP_REVIVAL_NOT_RUNNING");
        const companionTerminal = siblingId === undefined ? undefined : terminal(0, siblingId);
        const terminalAttempt = readerCase ? waitFor(bus, "native-event", (event) =>
          event.task_id === taskId && (event.payload as Json | undefined)?.status === "completed") : undefined;
        bus.emit(`release-${runNumber}`);
        if (siblingId !== undefined) bus.emit("release-1");
        if (terminalAttempt !== undefined) report.terminal_attempt = await terminalAttempt;
        if (companionTerminal !== undefined) await companionTerminal;
      }
      if (nextTerminal !== undefined) await nextTerminal;
      const current = store.load(taskId);
      report.current_record = current;
      const primarySession = sessions.find((session) => session.sessionId === running.child_session_id) ?? sessions[0];
      const preserved = primarySession?.getLastAssistantText();
      report.captured_mismatch = current.status === "running" && current.notification.run_epoch === 1 &&
        preserved === `native-result-${runNumber}` && !releaseCase;
      report.mismatch = report.captured_mismatch ? MISMATCH : null;
      report.preserved_result = preserved;
      report.work_outcome = { status: releaseCase ? "late-output-after-release" : "completed", run_epoch: 1,
        source: readerCase ? "native complete transition attempt plus actual SDK session output"
          : releaseCase ? "actual SDK session events; ownership was released first" : "native task terminal observer",
        result: preserved };
      report.notification_consistent = current.notification.notified_epoch === 1;
      if (readerCase) {
        report.retry_observation = observations.operations.filter((operation) =>
          operation.op === "rename" && operation.epoch === 1 && operation.status === "completed");
        reader!.stdin!.end("RELEASE\n");
        report.reader_exit = await readerDone;
        reader = undefined;
        readerDone = undefined;
        report.record_after_reader_release = store.load(taskId);
        if (report.captured_mismatch) {
          report.followup_after_release = await call("task_send", { to: taskId, message: "Post-publication resident check." });
        }
      }
      if (closest && !releaseCase) {
        report.sibling_terminal = store.load(siblingId);
        parentBusy = false;
        await dispatch("agent_end", { type: "agent_end" });
        const laneProvider = waitFor(bus, "provider-start", (data) => data.n === 3);
        const secondLaneProvider = waitFor(bus, "provider-start", (data) => data.n === 4);
        const lane = await call("task", { subagent_type: "explore", model: "coordination-qa/offline",
          prompt: "Return the lane control.", description: "R2 lane control", run_in_background: true });
        const secondLane = await call("task", { subagent_type: "explore", model: "coordination-qa/offline",
          prompt: "Return the second lane control.", description: "R2 second lane control", run_in_background: true });
        await laneProvider;
        await secondLaneProvider;
        const laneId = (lane as { details: { task_id: string } }).details.task_id;
        const secondLaneId = (secondLane as { details: { task_id: string } }).details.task_id;
        const laneTerminal = terminal(0, laneId);
        const secondLaneTerminal = terminal(0, secondLaneId);
        bus.emit("release-3");
        bus.emit("release-4");
        await laneTerminal;
        await secondLaneTerminal;
        report.capacity_consistent = true;
        report.slots_reused = 2;
      }
    } else {
      // A second child must actually start under global_concurrency:1 after epoch0 releases its lane.
      const capacityProvider = waitFor(bus, "provider-start", (data) => data.n === 1);
      const next = await call("task", { subagent_type: "explore", model: "coordination-qa/offline",
        prompt: "Return the lane reuse result.", description: "Native capacity control", run_in_background: true });
      report.capacity_spawn = next;
      await capacityProvider;
      const capacityTerminal = terminal(0);
      bus.emit("release-1");
      await capacityTerminal;
      report.capacity_consistent = true;
      report.current_record = baseline;
      report.captured_mismatch = false;
    }
    report.notifications = notifications;
    report.operations = observations.operations;
    report.native_session_events = nativeSessionEvents;
    report.fs_observation = observations.operations.length > 0 ? "sandbox native fs observed"
      : "unavailable: Bun named fs imports do not follow syncBuiltinESMExports; see Node variants";
    report.completed_at = new Date().toISOString();
    fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
    if (name === "normal-baseline") {
      fs.writeFileSync(path.join(evidence, "baseline.json"), JSON.stringify({ ...report, artifact }, null, 2) + "\n");
    }
    return { ...report, artifact };
  } catch (error) {
    report.setup_error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    report.operations = observations.operations;
    fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
    throw error;
  } finally {
    try {
      if (reader?.stdin?.writable) reader.stdin.end("RELEASE\n");
      if (readerDone !== undefined) await readerDone;
      await shutdown?.();
    } finally {
      await restoreSessionCapture?.();
      observations.restore();
      process.chdir(previousCwd);
      for (const key of Object.keys(process.env)) delete process.env[key];
      Object.assign(process.env, previousEnv);
      fs.writeFileSync(artifact + ".cleanup.json", JSON.stringify({
        reader_exited: readerDone === undefined, native_shutdown_awaited: true,
        prototype_and_fs_observers_restored: true, cwd_and_environment_restored: true,
        active_resources: process.getActiveResourcesInfo(),
      }, null, 2) + "\n");
    }
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const nativeFlag = process.argv.indexOf("--native-case");
  const flag = process.argv.indexOf("--case");
  const name = nativeFlag !== -1 ? process.argv[nativeFlag + 1]
    : flag === -1 ? "normal-baseline" : process.argv[flag + 1];
  const scheduleFlag = process.argv.indexOf("--schedule");
  if (nativeFlag === -1 && !["normal-baseline", "revived-stale-record"].includes(name ?? "")) {
    // Keep the original two isolated regressions intact; additive cases use
    // the same module discovery/attempt validation as the owned QA entry.
    await import("./owned.ts");
  } else {
  const result = await runCase(name as CaseName, scheduleFlag === -1 ? undefined : process.argv[scheduleFlag + 1]);
  const resultFlag = process.argv.indexOf("--result");
  if (resultFlag !== -1) {
    const resultPath = process.argv[resultFlag + 1]!;
    assert(inside(attempt, resultPath));
    fs.writeFileSync(resultPath, JSON.stringify(result, null, 2) + "\n");
  } else if (name === "revived-stale-record" && scheduleFlag === -1) {
    assert.equal(result.captured_mismatch, true, "EXPECTED_CURRENT_EPOCH_MISMATCH_NOT_CAPTURED");
  }
  console.log(JSON.stringify({ case: name, captured_mismatch: result.captured_mismatch, artifact: result.artifact }));
  }
}

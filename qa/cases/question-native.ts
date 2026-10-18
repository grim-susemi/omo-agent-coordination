import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { randomUUID, createHash } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { syncBuiltinESMExports } from "node:module";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";
import { product, senpi, bundle, isolatedEnv, sha256 } from "../command.mjs";
import { getCoordinationBroker, getCoordinationClient } from "../../src/public-api.ts";
import { questionCommand, questionEvidence, questionHashes, questionReceipt } from "./question-receipts.ts";
import type { CoordinationClient, TargetRef } from "../../src/contracts.ts";
import { inspectQuestionModels, realQuestion } from "./question-real.ts";
import { nativeQuestionTarget } from "../../src/peer-questions.ts";
import { verifyQuestionPolicyReplay } from "./question-policy-replay.ts";
import { disposableModelConfig, realModelProfile } from "../model-config.ts";

export type QuestionCase = "busy-question" | "question-races" | "deep-abort" | "ask-policy" |
  "inspect-api" | "public-api-consumer" | "max-reasoning-nonce" | "deep-ask-long" | "question-process" |
  "question-model-inventory";
type Json = Record<string, any>;
const captureKey = Symbol.for("omo.agent-coordination.capture.v1");
const ok = <T>(result: { ok: true; value: T } | { ok: false; reason: string }): T => {
  assert(result.ok, JSON.stringify(result)); return result.value;
};
async function bounded<T>(promise: Promise<T>, label: string, ms = 30_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try { return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`QUESTION_EVENT_DEADLINE:${label}`)), ms);
  })]); } finally { clearTimeout(timer!); }
}
function event(bus: EventEmitter, name: string, predicate: (data: any) => boolean = () => true) {
  let listener: (data: any) => void;
  const promise = new Promise<any>((resolve) => {
    listener = (data) => { if (predicate(data)) { bus.off(name, listener); resolve(data); } };
    bus.on(name, listener);
  });
  return bounded(promise, name).finally(() => bus.off(name, listener));
}
const fingerprint = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");
export async function runQuestionCase(name: QuestionCase) {
  const sandbox = path.join(questionEvidence, "runs", `${name}-${randomUUID()}`);
  const directory = path.join(sandbox, "question-package");
  fs.mkdirSync(directory, { recursive: true });
  const build = await Bun.build({
    entrypoints: ["src/extension.ts", "qa/cases/question-fixture.ts", "qa/cases/question-consumer.ts"]
      .map((file) => path.join(product, file)), outdir: directory, naming: "[name].js", target: "node", format: "esm",
    external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"],
  });
  assert(build.success, String(build.logs));
  const api = await Bun.build({ entrypoints: [path.join(product, "src/public-api.ts")], outdir: path.join(directory, "dist"),
    naming: "public-api.js", target: "node", format: "esm" });
  assert(api.success, String(api.logs));
  fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({
    name: "coordination-question-owned-candidate", type: "module", exports: { "./api": "./dist/public-api.js" },
    pi: { extensions: ["./extension.js", "./question-fixture.js", "./question-consumer.js"] },
  }));
  const link = path.join(directory, "node_modules");
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", link, "junction");
  const artifact = path.join(questionEvidence, `${name}-${randomUUID()}.json`);
  try {
    const result = await questionCommand(["node", fileURLToPath(import.meta.url), "--native-case", name,
      "--sandbox", sandbox, "--result", artifact], name === "deep-ask-long" ? 600_000 : 180_000);
    assert.equal(result.exit, 0, `REAL_QUESTION_CASE_FAILED:${result.receiptPath}`);
    const report = JSON.parse(fs.readFileSync(artifact, "utf8"));
    assert.equal(report.passed, true, JSON.stringify(report.error));
    questionReceipt(name === "busy-question" ? "busy" : name === "question-races" ? "races" : name,
      { ...report, command_receipt: result.receiptPath });
    return report;
  } finally {
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
  }
}

async function nativeCase(name: QuestionCase, sandbox: string, artifact: string) {
  assert(path.resolve(sandbox).startsWith(path.resolve(questionEvidence) + path.sep));
  const oldEnv = { ...process.env }, oldCwd = process.cwd();
  const env = isolatedEnv(sandbox), guard = process.env.NODE_OPTIONS;
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  if (guard) process.env.NODE_OPTIONS = guard;
  const metadata = path.join(sandbox, "sdk-metadata");
  fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = metadata;
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true }); process.chdir(cwd);
  const bus = new EventEmitter(), global = globalThis as any;
  const counts = { main: 0, side: 0, payloads: [] as any[], main_tools: {} as Record<string, string[]> };
  global[Symbol.for("coordination.question.qa.bus")] = bus;
  global[Symbol.for("coordination.question.qa.counts")] = counts;
  const sessions: any[] = [], clients: CoordinationClient[] = [], labels: string[] = [];
  const report: Json = { case: name, sandbox, source_sha256: questionHashes(),
    module_sha256_before: { [bundle]: sha256(bundle), [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js") },
    started_at: new Date().toISOString(), repair_enabled: false, passed: false };
  let manifest: ReturnType<typeof buildAdapter> | undefined, state: any;
  let modelConfig: ReturnType<typeof disposableModelConfig> | undefined;
  let processHandle: any, processGate: string | undefined;
  let stopTerminal = () => {};
  try {
    manifest = buildAdapter({ output: path.join(sandbox, "candidate/omo-task.js") });
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
    const directory = path.join(sandbox, "question-package");
    const packages = ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg", directory];
    if (name === "question-process") {
      fs.writeFileSync(path.join(env.OMO_CODING_AGENT_DIR!, "settings.json"), JSON.stringify({ packages }));
      const pathKey = Object.keys(process.env).find((key) => key.toLowerCase() === "path") ?? "PATH";
      process.env[pathKey] = `${path.dirname(process.execPath)};${process.env.SystemRoot}/System32`;
    }
    fs.writeFileSync(path.join(cwd, ".omo/omo.json"), JSON.stringify({
      agents: Object.fromEntries(["question-worker", "explore", "oracle", "omo-native-code-reviewer", "question-denied", "question-process-worker"]
        .map((role) => [role, { description: "Owned disposable question QA", prompt: "Run only the controlled fixture.",
          model: "coordination-question/offline", execution_mode: role === "question-process-worker" ? "process" : "in-process",
          ...(role === "question-denied" ? { tools: { read: true, coordination_ask: false, coordination_notify: false } } : {}) }])),
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: "in-process",
        default_concurrency: 8, global_concurrency: 8, host_shard_prewarm: "off", process_runner: "child-process" },
    }));
    const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    const { AuthStorage, ReadOnlyAuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory({ packages, retry: { enabled: false }, sessionTitle: { enabled: false } }) });
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, [], "QUESTION_NATIVE_LOADER_FAILED");
    const auth = AuthStorage.inMemory();
    const realModel = name === "max-reasoning-nonce" || name === "deep-ask-long" || name === "question-model-inventory";
    const { ModelRuntime } = await import(pathToFileURL(senpi + "/dist/core/model-runtime.js").href);
    const { InMemoryCodingAgentModelsStore } = await import(pathToFileURL(senpi + "/dist/core/models-store.js").href);
    const modelsStore = new InMemoryCodingAgentModelsStore();
    const nativeCatalog = "C:/Users/USER/.omo-stable-500/home/.omo/agent/models-store.json";
    if (realModel && fs.existsSync(nativeCatalog)) {
      const cached = JSON.parse(fs.readFileSync(nativeCatalog, "utf8"));
      for (const [provider, entry] of Object.entries(cached)) await modelsStore.write(provider, entry);
      report.native_catalog = { path: nativeCatalog, sha256: sha256(nativeCatalog),
        access: "read-only file read; native in-memory catalog; no profile lock or cache writes" };
    }
    let registry: any;
    if (realModel) {
      modelConfig = disposableModelConfig();
      const runtime = await ModelRuntime.create({ agentDir: env.OMO_CODING_AGENT_DIR,
        credentials: new ReadOnlyAuthStorage(realModelProfile + "/auth.json"),
        modelsPath: modelConfig.modelsPath, allowModelNetwork: false, modelsStore });
      modelConfig.observe(runtime.modelsPath);
      registry = new ModelRegistry(runtime, auth);
    } else registry = ModelRegistry.inMemory(auth);
    const root = (await sdk.createAgentSession({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      resourceLoader: loader, authStorage: auth, modelRegistry: registry,
      sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    sessions.push(root);
    await root.bindExtensions({ mode: "print" });
    await root.setModel(registry.find("coordination-question", "offline"));
    root.setSessionThinkingLevel("high");
    state = global[captureKey];
    assert(state.capture.status.ok);
    const bridge = state.bridges.get(root.sessionManager), truth = state.inspections.get(root.sessionManager);
    assert(bridge && truth, "QUESTION_OWNER_BINDING_MISSING");
    const rootCtx = root.extensionRunner.createContext();
    const execute = (tool: string, input: any, ctx = rootCtx, signal?: AbortSignal, id: string = randomUUID()) =>
      root.extensionRunner.getToolDefinition(tool).execute(id, input, signal, undefined, ctx);
    const toolAsk = async (ref: TargetRef, id: string, question = "NONCE:tool-nonce", extra: Json = {}, ctx = rootCtx, signal?: AbortSignal) =>
      (await execute("coordination_ask", { target_ref: ref, request_id: id, question, ...extra }, ctx, signal, id)).details;
    const completed = new Map<string, any>();
    stopTerminal = global[Symbol.for("omo.task.terminalObservers")].subscribe(({ record }: any) => {
      if (["completed", "error", "cancelled", "interrupted", "lost"].includes(record.status)) {
        completed.set(record.task_id, record); bus.emit("native-terminal", record);
      }
    });
    const settled = (id: string) => completed.has(id) ? Promise.resolve(completed.get(id))
      : event(bus, "native-terminal", (record) => record.task_id === id);
    const launch = async (label: string, role = "question-worker") => {
      const started = event(bus, "main-latch", (data) => data.label === label);
      labels.push(label);
      const result = await execute("task", { subagent_type: role, model: "coordination-question/offline",
        prompt: `QUESTION_NATIVE_CHILD:${label}`, description: `Owned question ${label}`, run_in_background: true });
      const id = result.details?.task_id;
      assert(id, JSON.stringify(result));
      await started;
      const snapshot = ok<any[]>(bridge.listOwned()).find((row) => row.identity.task_id === id);
      assert(snapshot, "QUESTION_NATIVE_TARGET_MISSING");
      if (role === "question-denied") assert.equal(snapshot.collaboration.allowed, false,
        "NATIVE_ASSIGNMENT_DENIAL_WAS_NOT_CONFIGURED");
      const capture = state.capture.list().find((row: any) => row.runtime()?.sessionId.replace(/^senpi:/, "") === snapshot.identity.session_id);
      assert(capture && state.capture.bindOwned(bridge, snapshot.identity.target_ref, capture).ok);
      const runtime = capture.runtime();
      sessions.push(runtime);
      runtime.setSessionThinkingLevel("high");
      return { id, ref: snapshot.identity.target_ref as TargetRef, runtime, ctx: runtime.extensionRunner.createContext() };
    };
    const a = await launch("a", "explore"), b = await launch("b", "oracle");
    assert(["coordination_inspect", "coordination_ask", "coordination_notify"].every((tool) =>
      a.runtime.getActiveToolNames().includes(tool)), "READONLY_NATIVE_COLLABORATION_NOT_EXPOSED");
    report.first_request_capability = { first_request_tool_names: counts.main_tools.a,
      later_active_tool_names: a.runtime.getActiveToolNames() };
    assert(["coordination_inspect", "coordination_ask", "coordination_notify"].every((tool) =>
      counts.main_tools.a?.includes(tool)), "READONLY_NATIVE_FIRST_REQUEST_MISSING_COLLABORATION");
    const client = ok(getCoordinationClient(rootCtx)); clients.push(client);
    const rootRef = ok(client.peers()).peers.find((row) => row.role === "root")!.target_ref;
    const before = { history: fingerprint(a.runtime.messages), thinking: a.runtime.thinkingLevel,
      model: fingerprint(a.runtime.model), main_calls: counts.main, side_calls: counts.side };
    const consumer = async () => (await execute("question_api_consumer", { target_ref: a.ref })).details;

    if (name === "question-process") {
      processGate = path.join(sandbox, "process-gate"); fs.mkdirSync(processGate);
      process.env.COORDINATION_QUESTION_PROCESS_GATE = processGate;
      let watcher: fs.FSWatcher | undefined;
      const ready = new Promise<void>((resolve, reject) => {
        watcher = fs.watch(processGate!, (_event, file) => {
          if (String(file) === "child-ready") { watcher?.close(); resolve(); }
        });
        watcher.on("error", reject);
      });
      let id: string;
      try {
        const launched = await execute("task", { subagent_type: "question-process-worker", execution_mode: "process",
          model: "coordination-question/offline", prompt: "QUESTION_NATIVE_CHILD:rpc",
          description: "Ordinary managed process without fixture bootstrap", run_in_background: true });
        id = launched.details.task_id; assert(id);
        const native = global[Symbol.for("omo.agent-coordination.native.v1")].contexts.get(root.sessionManager).deref();
        processHandle = native.manager.getResidentHandle(id);
        await bounded(ready, "ordinary process registered tools");
      } finally { watcher?.close(); }
      const snapshot = ok<any[]>(bridge.listOwned()).find((row) => row.identity.task_id === id);
      assert(snapshot, "PROCESS_NATIVE_BINDING_MISSING");
      const engine = global[Symbol.for("omo.agent-coordination.native.v1")].contexts.get(root.sessionManager).deref();
      processHandle = engine.manager.getResidentHandle(id);
      assert(processHandle && processHandle.kind === "rpc");
      const proof = JSON.parse(fs.readFileSync(path.join(processGate, "child-proof.json"), "utf8"));
      assert.equal(proof.pid, processHandle.pid);
      assert.equal(proof.fixture_handshake_calls, 0);
      const branch = await bounded<any>(processHandle.getEntries(), "original native RPC branch proof");
      assert(branch.entries.some((entry: any) => entry.id === proof.branch));
      if (processHandle.sessionId) assert.equal(proof.session_id.replace(/^senpi:/, ""),
        processHandle.sessionId.replace(/^senpi:/, ""));
      report.child_extension_probe = { pid: proof.pid, argv: proof.argv,
        loaded_extensions: proof.loaded_extensions, extension_errors: proof.extension_errors,
        capture_entry_url: proof.capture_entry_url };
      assert.equal(proof.extension_errors.length, 0);
      assert(proof.loaded_extensions.some((extension: Json) =>
        extension.resolved_path.replaceAll("\\", "/").endsWith("/question-package/extension.js")),
      "RPC_CHILD_COORDINATION_PACKAGE_NOT_LOADED");
      const result = await toolAsk(snapshot.identity.target_ref, "actual-process-question", "NONCE:actual-process-nonce");
      const duplicate = await client.ask({ target_ref: snapshot.identity.target_ref,
        request_id: "actual-process-question", question: "NONCE:actual-process-nonce", depth: "quick" });
      assert.deepEqual(duplicate, result, "PROCESS_TOOL_API_DEDUPLICATION_CHANGED_RESULT");
      const after = JSON.parse(fs.readFileSync(path.join(processGate, "side-provider-count.json"), "utf8"));
      const outbound = proof.outbound;
      report.process_connection = { native_pid: processHandle.pid, capture_pid: proof.pid,
        native_branch_matches: true, native_session_matches: true, fixture_handshake_calls: 0,
        production_bootstrap: true,
        actual_named_pipe_identity_roundtrip: true, registered_question_result: result,
        side_provider_calls: after.side_provider_calls, tool_api_shared_ledger: true, outbound, expected_state: "replied",
        native_status: ok<any>(bridge.inspectOwned(snapshot.identity.target_ref)).status };
      fs.writeFileSync(path.join(processGate, "release-main"), "release owned native process\n");
      assert.equal((await bounded<any>(processHandle.waitForOutcome(), "process original outcome")).status, "completed");
      report.process_connection.native_main_completed = true;
      assert(result.ok && result.value?.state === "replied", "CONNECTED_NATIVE_PROCESS_QUESTION_HANDLER_NOT_WIRED");
      assert.equal(result.value.reply, "actual-process-nonce");
      assert.equal(after.side_provider_calls, 1, "PROCESS_QUESTION_NOT_EXACTLY_ONE_PROVIDER_REQUEST");
    } else if (name === "busy-question") {
      const reviewer = await launch("reviewer", "omo-native-code-reviewer");
      const denied = await launch("denied", "question-denied");
      const excludedBefore = counts.side;
      for (const target of [reviewer, denied]) {
        assert.deepEqual(await toolAsk(target.ref, `excluded:${target.id}`), { ok: false, reason: "scope_denied" });
        assert.deepEqual(getCoordinationClient(target.ctx), { ok: false, reason: "scope_denied" });
        assert(!target.runtime.getActiveToolNames().includes("coordination_ask"));
        assert.equal(target.runtime._pendingNextTurnMessages.length, 0);
      }
      const oneShot = await execute("task", { subagent_type: "plan-reviewer", model: "coordination-question/offline",
        prompt: "No cooperation permitted.", description: "Owned native one-shot admission probe", run_in_background: true });
      assert.equal(oneShot.details.status, "denied");
      assert.equal(counts.side, excludedBefore);
      before.main_calls = counts.main;
      const reply = ok<any>(await toolAsk(a.ref, "busy", "NONCE:busy-nonce"));
      assert.equal(reply.state, "replied", JSON.stringify(reply));
      assert.equal(reply.reply, "busy-nonce");
      assert.equal(fingerprint(a.runtime.messages), before.history);
      assert.equal(a.runtime.thinkingLevel, before.thinking);
      assert.equal(fingerprint(a.runtime.model), before.model);
      assert.equal(counts.main, before.main_calls);
      assert.equal(counts.side, before.side_calls + 1);
      const childClient = ok(getCoordinationClient(a.ctx)); clients.push(childClient);
      assert.equal(ok<any>(await childClient.ask({ target_ref: b.ref, request_id: "child-sibling",
        question: "NONCE:sibling-nonce" })).reply, "sibling-nonce");
      const rootLatch = event(bus, "main-latch", (data) => data.label === "root");
      labels.push("root");
      const rootMain = root.prompt("QUESTION_NATIVE_CHILD:root");
      await rootLatch;
      const rootBusyHistory = fingerprint(root.messages), rootBusyCalls = counts.main;
      assert.equal(ok<any>(await childClient.ask({ target_ref: rootRef, request_id: "child-root",
        question: "NONCE:root-nonce" })).reply, "root-nonce");
      assert.equal(fingerprint(root.messages), rootBusyHistory);
      assert.equal(counts.main, rootBusyCalls);
      const noteBefore = counts.side;
      const note = ok<any>((await execute("coordination_notify", { target_ref: a.ref,
        request_id: "note", message: "note-nonce" })).details);
      assert.equal(note.incorporation, "queued_for_next_turn");
      assert.equal(counts.side, noteBefore);
      const customBefore = a.runtime.messages.filter((message: any) => message.customType === "omo-agent-coordination-peer-v1").length;
      assert.equal(customBefore, 0);
      const rootNote = ok<any>(await client.notify({ target_ref: rootRef, request_id: "root-note", message: "natural-root-note" }));
      assert.equal(rootNote.incorporation, "queued_for_next_turn");
      bus.emit("release-main:root");
      await bounded(rootMain, "original busy root completes normally");
      assert.equal(root.messages.filter((message: any) => message.customType === "omo-agent-coordination-peer-v1").length, 0);
      await bounded(root.prompt("Normal next root turn."), "natural root prompt");
      const rootMessages = root.messages.filter((message: any) => message.customType === "omo-agent-coordination-peer-v1");
      assert.equal(rootMessages.filter((message: any) => String(message.content).includes("natural-root-note")).length, 1);
      assert.equal(rootMessages.filter((message: any) => String(message.content).includes("root-nonce")).length, 1);
      bus.emit("release-main:a");
      const done = await settled(a.id);
      assert.equal(done.status, "completed");
      await bounded(a.runtime.waitForIdle(), "a idle");
      assert.equal(a.runtime.messages.filter((message: any) => message.customType === "omo-agent-coordination-peer-v1").length, 0);
      report.busy = { reply, immediate_main_history_changes: 0, main_provider_calls_during_side: 0,
        main_thinking_unchanged: true, main_model_unchanged: true, native_main_completed: true,
        readonly_child_to_sibling: true, readonly_child_to_root: true, lead_relay_calls: 0,
        root_note_consumed_once: true, root_question_consumed_once: true, busy_root_original_result_preserved: true,
        note_to_terminal_worker_not_revived: true };
    } else if (name === "deep-abort") {
      const controller = new AbortController();
      const started = event(bus, "side-start", (data) => data.nonce === "abort-nonce");
      const aborted = event(bus, "side-aborted", (data) => data.nonce === "abort-nonce");
      const pending = client.ask({ target_ref: a.ref, request_id: "deep-abort", depth: "deep",
        question: "NONCE:abort-nonce CONTROL:gate", signal: controller.signal });
      await started;
      assert.deepEqual(await client.ask({ target_ref: a.ref, request_id: "busy-deep", depth: "deep", question: "busy" }),
        { ok: false, reason: "target_busy" });
      controller.abort();
      await aborted;
      const result = await pending;
      bus.emit("release-side:abort-nonce");
      assert.equal((result as any).value?.state ?? (result as any).reason, "cancelled", JSON.stringify(result));
      const service = state.questions.get(root.sessionManager);
      assert.equal(service.diagnostics().active, 0);
      assert.equal(service.diagnostics().pending, 0);
      assert.equal(fingerprint(a.runtime.messages), before.history);
      report.deep_abort = { result, side_stream_aborted: true, late_output_discarded: true,
        remaining_active: 0, remaining_pending: 0, main_unchanged: true };
    } else if (name === "ask-policy") {
      report.policy_replay = await verifyQuestionPolicyReplay({ client, context: rootCtx,
        service: state.questions.get(root.sessionManager), target: a, counts, toolAsk });
      const policyClient = ok(getCoordinationClient(rootCtx)); clients.push(policyClient);
      ok(policyClient.setAskPolicy({ target_ref: a.ref, policy: "deny_peer_asks" }));
      const sibling = ok(getCoordinationClient(b.ctx)); clients.push(sibling);
      const count = counts.side, queued = a.runtime._pendingNextTurnMessages.length;
      assert.deepEqual(await sibling.ask({ target_ref: a.ref, question: "denied", request_id: "denied" }),
        { ok: false, reason: "target_excluded" });
      assert.deepEqual(await sibling.notify({ target_ref: a.ref, message: "denied", request_id: "denied-note" }),
        { ok: false, reason: "target_excluded" });
      assert.equal(counts.side, count); assert.equal(a.runtime._pendingNextTurnMessages.length, queued);
      const protectedClient = ok(getCoordinationClient(a.ctx)); clients.push(protectedClient);
      assert.equal(ok<any>(await protectedClient.ask({ target_ref: rootRef, question: "NONCE:outbound", request_id: "outbound" })).reply, "outbound");
      assert.deepEqual(sibling.setAskPolicy({ target_ref: a.ref, policy: "default" }), { ok: false, reason: "policy_not_owner" });
      assert.equal(ok(await client.inspect(a.ref)).asks, "denied");
      policyClient.dispose();
      assert.equal(ok(await client.inspect(a.ref)).asks, "allowed");
      report.ask_policy = { provider_calls_on_denial: 0, queue_additions_on_denial: 0, outbound_allowed: true,
        child_policy_denied: true, dispose_cleared: true,
        policy_receipts: state.questions.get(root.sessionManager).policyReceipts() };
    } else if (name === "inspect-api") {
      const count = counts.side;
      const live = ok(await client.inspect(a.ref)); assert.equal(live.live, true); assert.equal(live.record_consistent, true);
      bus.emit("release-main:a"); await settled(a.id);
      const finished = ok(await client.inspect(a.ref));
      assert.equal(finished.live, false); assert.equal(finished.record_consistent, true);
      const features = ok(getCoordinationBroker()).features;
      assert.deepEqual(features, ["ask_depth_deep", "ask_policy", "inspect"]);
      assert(Object.isFrozen(features)); assert.equal(counts.side, count);
      const restarting = event(bus, "main-latch", (data) => data.label === "stuck");
      labels.push("stuck");
      await execute("task_send", { to: a.id, message: "QUESTION_NATIVE_CHILD:stuck" });
      await restarting;
      const fresh = ok<any[]>(bridge.listOwned()).find((row) => row.identity.task_id === a.id);
      assert.equal(fresh.identity.run_epoch, 1);
      const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
      const engine = carrier.contexts.get(root.sessionManager).deref();
      const handle = engine.manager.getResidentHandle(a.id);
      const outcome = handle.waitForOutcome();
      const witness = new Promise<void>((resolve) => {
        const subscribed = bridge.subscribeOutcome(fresh.identity.target_ref, () => { subscribed.value?.(); resolve(); });
        assert(subscribed.ok);
      });
      const recordFile = path.join(sandbox, "senpi-task/tasks", a.id + ".json");
      const rename = fs.renameSync;
      let refused = 0;
      fs.renameSync = function (from: any, to: any) {
        if (path.resolve(String(to)) === path.resolve(recordFile)) {
          const next = JSON.parse(fs.readFileSync(from, "utf8"));
          if (next.status === "completed" && next.notification.run_epoch === 1) {
            refused++;
            const error = new Error("Owned API inspection publication-refusal probe") as NodeJS.ErrnoException;
            error.code = "EPERM"; throw error;
          }
        }
        return Reflect.apply(rename, fs, [from, to]);
      } as typeof fs.renameSync;
      syncBuiltinESMExports();
      let stuck: any;
      try {
        bus.emit("release-main:stuck");
        assert.equal((await bounded<any>(outcome, "actual stuck outcome")).status, "completed");
        await bounded(witness, "actual bridge witness");
        await bounded(root.waitForIdle(), "stuck publication drain");
        const recordHash = sha256(recordFile), providerCount = counts.side;
        stuck = ok(await client.inspect(fresh.identity.target_ref));
        assert(refused > 0);
        assert.equal(stuck.live, false); assert.equal(stuck.status, "running");
        assert.equal(stuck.record_consistent, false);
        assert.equal(sha256(recordFile), recordHash); assert.equal(counts.side, providerCount);
      } finally { fs.renameSync = rename; syncBuiltinESMExports(); }
      report.inspect_api = { live, settled: finished, stuck, refused_publication_attempts: refused,
        features, provider_calls: 0, repair_enabled: false };
    } else if (name === "public-api-consumer") {
      report.public_api_consumer = await consumer();
      assert.equal(report.public_api_consumer.passed, true);
    } else if (name === "question-races") {
      const cases: any[] = [];
      for (const control of ["empty", "tool", "error", "quota"]) {
        const count = counts.side;
        const result = ok<any>(await toolAsk(a.ref, control, `NONCE:${control} CONTROL:${control}`));
        assert.equal(result.state, "failed", JSON.stringify(result));
        assert.equal(counts.side, count + 1);
        assert.equal(fingerprint(a.runtime.messages), before.history);
        cases.push({ control, state: result.state, reason: result.terminal_reason, provider_calls: 1 });
      }
      const deepInvalid = await toolAsk(a.ref, "invalid-deep", "invalid", { depth: "deep", timeout_ms: 1 });
      assert.deepEqual(deepInvalid, { ok: false, reason: "invalid_argument" });
      const modelStart = event(bus, "side-start", (data) => data.nonce === "model-race");
      const modelAbort = event(bus, "side-aborted", (data) => data.nonce === "model-race");
      const changing = toolAsk(a.ref, "model-race", "NONCE:model-race CONTROL:gate", { depth: "deep" });
      await modelStart;
      await a.runtime.setModel(registry.find("coordination-question", "offline-alt"));
      await modelAbort;
      const changed = ok<any>(await changing);
      assert.equal(changed.state, "cancelled"); assert.equal(changed.terminal_reason, "stale_epoch");
      bus.emit("release-side:model-race");
      await a.runtime.setModel(registry.find("coordination-question", "offline"));
      const timeoutStart = event(bus, "side-start", (data) => data.nonce === "timeout-race");
      const timeoutAbort = event(bus, "side-aborted", (data) => data.nonce === "timeout-race");
      const timing = toolAsk(a.ref, "timeout-race", "NONCE:timeout-race CONTROL:gate", { timeout_ms: 2000 });
      await timeoutStart; await timeoutAbort;
      const timed = ok<any>(await timing); assert.equal(timed.state, "timed_out");
      bus.emit("release-side:timeout-race");
      const completionStart = event(bus, "side-start", (data) => data.nonce === "completion-race");
      const completionAbort = event(bus, "side-aborted", (data) => data.nonce === "completion-race");
      const completing = toolAsk(b.ref, "completion-race", "NONCE:completion-race CONTROL:gate", { depth: "deep" });
      await completionStart;
      const done = settled(b.id); bus.emit("release-main:b");
      await done; await completionAbort;
      const completedResult = ok<any>(await completing);
      assert.equal(completedResult.state, "cancelled");
      assert.equal(completedResult.ended_by, "target_settled");
      bus.emit("release-side:completion-race");
      const beforeSettled = counts.side;
      assert.deepEqual(await toolAsk(b.ref, "settled-before-delivery"), { ok: false, reason: "target_settled" });
      assert.equal(counts.side, beforeSettled);
      report.races = { cases, model_change: changed, timeout: timed, completion: completedResult,
        settled_before_delivery_provider_calls: 0, no_retry_fallback_or_main_cancel: true, invalid_deep_timeout: deepInvalid };
    } else if (name === "question-model-inventory") {
      report.model_inventory = inspectQuestionModels(registry);
      report.real_provider_requests = 0;
      const renderNativeDeep = async (model: Json) => {
        await root.setModel(registry.find(model.provider, model.id)); root.setSessionThinkingLevel("high");
        const captured = ok<any>(state.capture.forRuntime(root));
        const bound = ok<any>(state.questions.get(root.sessionManager).bindContext(rootCtx));
        let proof: Json | undefined;
        const dry = nativeQuestionTarget({ capture: captured, binding: bound,
          validate: () => {
            const fresh = state.questions.get(root.sessionManager).bindContext(rootCtx);
            return fresh.ok && fresh.value.runtime_instance_id === bound.runtime_instance_id
              ? { ok: true, value: undefined } : { ok: false, reason: "stale_generation" };
          },
          subscribe: (listener) => state.capture.onRuntime((capture: any, event: string) => {
            if (capture === captured) listener(event === "closed" ? "target_not_live" : undefined);
          }),
          onPayload(_snapshot, payload) {
            proof = { model, keys: Object.keys(payload), reasoning: payload.reasoning,
              reasoning_effort: payload.reasoning_effort,
              max_output_tokens: payload.max_output_tokens ?? payload.max_completion_tokens ?? payload.max_tokens ?? null,
              output_token_field_absent:
                !("max_tokens" in payload) && !("max_output_tokens" in payload) && !("max_completion_tokens" in payload),
              stopped_in_onPayload_before_native_transport: true, physical_provider_requests: 0 };
            throw new Error("QA_NATIVE_BUILDER_STOP_BEFORE_TRANSPORT");
          },
        });
        const prepared = ok<any>(await dry.prepare(ok<any>(dry.read()),
          "Native request-body construction only. This request must not be sent.", [], "deep", new AbortController().signal));
        await assert.rejects(prepared.run(), /provider_failure/);
        assert(proof?.stopped_in_onPayload_before_native_transport);
        return proof;
      };
      const codex = report.model_inventory.find((model: Json) =>
        model.auth_configured && model.deep_supported && model.api === "openai-codex-responses");
      if (codex) {
        report.selected_codex_builder_model = codex;
        await root.setModel(registry.find(codex.provider, codex.id)); root.setSessionThinkingLevel("high");
        const refusal = ok<any>(await toolAsk(rootRef, "native-codex-cap-refusal", "Return a short policy-check nonce."));
        assert.equal(refusal.state, "failed"); assert.equal(refusal.terminal_reason, "reasoning_policy_unsupported");
        assert.equal(refusal.logical_provider_requests, 0); assert.equal(refusal.physical_provider_attempts, 0);
        report.native_codex_zero_call_refusal = refusal;
        report.native_codex_deep_builder = await renderNativeDeep(codex);
      }
      const preferredDeep = report.model_inventory.find((model: Json) => model.auth_configured && model.deep_supported);
      if (preferredDeep && preferredDeep.id !== codex?.id) {
        report.native_preferred_deep_builder = await renderNativeDeep(preferredDeep);
      }
    } else {
      report.real_model = await realQuestion({ name, root, registry, rootRef, childContext: a.ctx, toolAsk });
      if (!report.real_model.passed) {
        report.unavailable = report.real_model.unavailable;
        throw new Error(report.real_model.reason ?? "REAL_MODEL_CASE_FAILED");
      }
    }
    report.provider_counts = counts;
    report.native_provider_failures = [...state.questions.get(root.sessionManager).providerFailures()];
    report.registered_tools = ["coordination_inspect", "coordination_ask", "coordination_notify", "read"];
    report.entry_sha256 = sha256(path.join(directory, "extension.js"));
    report.passed = true;
  } catch (error) {
    report.native_provider_failures = [...(state?.questions.get(sessions[0]?.sessionManager)?.providerFailures() ?? [])];
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    throw error;
  } finally {
    try {
    for (const label of labels) bus.emit(`release-main:${label}`);
    bus.emit("release-side:abort-nonce");
    stopTerminal();
    for (const client of clients) client.dispose();
    if (processGate && !fs.existsSync(path.join(processGate, "release-main"))) {
      fs.writeFileSync(path.join(processGate, "release-main"), "release owned native process during cleanup\n");
    }
    if (processHandle) {
      if (!processHandle.hasExited()) await bounded(processHandle.terminate(), "exact owned process terminate");
      await bounded(processHandle.dispose(), "exact owned process dispose");
    }
    for (const capture of state?.capture.list() ?? []) if (capture.runtime()) sessions.push(capture.runtime());
    for (const session of [...new Set(sessions)].reverse()) {
      if (session._disposed) continue;
      await bounded(session.waitForIdle(), "cleanup idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    }
    state?.dispose();
    if (manifest && sha256(manifest.output) === manifest.candidate_sha256) report.removal = removeAdapter(manifest);
    const link = path.join(sandbox, "candidate/node_modules");
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
    bus.removeAllListeners();
    report.cleanup = { sessions_disposed: sessions.every((session) => session._disposed),
      provider_listeners: bus.eventNames().length, installed_unchanged: sha256(bundle) === report.module_sha256_before[bundle] };
    } finally {
    if (modelConfig) report.model_config = modelConfig.dispose();
    report.ended_at = new Date().toISOString();
    fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
    process.chdir(oldCwd);
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, oldEnv);
    }
  }
  return report;
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const flag = (key: string) => process.argv[process.argv.indexOf(key) + 1]!;
  const result = await nativeCase(flag("--native-case") as QuestionCase, flag("--sandbox"), flag("--result"));
  console.log(JSON.stringify({ case: result.case, passed: result.passed }));
}

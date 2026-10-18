import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { buildAdapter, removeAdapter } from "../scripts/build-adapter.mjs";
import { attempt, product, bundle, senpi, isolatedEnv, sha256 } from "./command.mjs";
import { getCoordinationBroker, getCoordinationClient } from "../src/public-api.ts";
import type { CoordinationClient, Result, TargetRef } from "../src/contracts.ts";
import { disposableModelConfig, realModelProfile } from "./model-config.ts";

export type FinalCase = "acceptance" | "max-reasoning-nonce" | "deep-ask-long" | "deep-abort" | "ask-policy" | "inspect-api";
type CaseName = "collaboration-preview" | "preview-denied" | FinalCase;
type Json = Record<string, any>;
const selectedCase = process.argv[process.argv.indexOf(process.argv.includes("--native-case") ? "--native-case" : "--case") + 1];
export const finalRun = ["acceptance", "max-reasoning-nonce", "deep-ask-long", "deep-abort", "ask-policy", "inspect-api"].includes(selectedCase ?? "");
export const liveEvidence = path.join(attempt, ["collaboration-preview", "preview-denied"].includes(selectedCase ?? "") ? "task-11" : "task-10",
  ...(["2", "3"].includes(process.env.COORDINATION_QA_ATTEMPT ?? "") ? [`attempt-${process.env.COORDINATION_QA_ATTEMPT}`] : []));
const captureKey = Symbol.for("omo.agent-coordination.capture.v1");
const queueType = "omo-agent-coordination-peer-v1";
const value = <T>(result: Result<T>): T => { assert(result.ok, JSON.stringify(result)); return result.value; };
const fingerprint = (data: unknown) => createHash("sha256").update(JSON.stringify(data)).digest("hex");
const save = (file: string, data: unknown) => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(data, null, 2) + "\n");
};
export function liveHashes(): Record<string, string> {
  const files = ["package.json", "tsconfig.json", "bun.lock"];
  for (const dir of ["src", "test", "qa", "scripts"]) {
    for (const entry of fs.readdirSync(path.join(product, dir), { recursive: true, withFileTypes: true })) {
      if (entry.isFile()) files.push(path.relative(product, path.join(entry.parentPath, entry.name)).replaceAll("\\", "/"));
    }
  }
  return Object.fromEntries(files.sort().map((file) => [file, sha256(path.join(product, file))]));
}
const modules = () => Object.fromEntries([bundle, senpi + "/dist/core/agent-session.js",
  senpi + "/dist/core/model-runtime.js"].map((file) => [file, sha256(file)]));
async function bounded<T>(pending: Promise<T>, label: string, ms = 30_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([pending, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`PREVIEW_EVENT_DEADLINE:${label}`)), ms);
    })]);
  } finally { clearTimeout(timer!); }
}
function event(bus: EventEmitter, name: string, predicate: (data: any) => boolean = () => true) {
  let listener: (data: any) => void;
  const pending = new Promise<any>((resolve) => {
    listener = (data) => { if (predicate(data)) { bus.off(name, listener); resolve(data); } };
    bus.on(name, listener);
  });
  return bounded(pending, name).finally(() => bus.off(name, listener));
}

/** A subprocess gives each run a fresh real loader, broker, SDK and native engine.
 * Only the model used to control main-tool latches is scripted; real-model proofs
 * below always use the target's native ModelRuntime and physical provider route. */
export async function runLiveCase(name: CaseName, repairDisabled = false) {
  const final = name !== "collaboration-preview" && name !== "preview-denied";
  assert.equal(JSON.parse(fs.readFileSync(path.join(attempt, final ? "verify-9/verdict.json" : "verify-6/verdict.json"), "utf8")).verdict, "confirmed");
  fs.mkdirSync(liveEvidence, { recursive: true });
  const sandbox = path.join(liveEvidence, "runs", `${name}${repairDisabled ? "-repair-disabled" : ""}-${randomUUID()}`);
  const directory = path.join(sandbox, "question-package");
  fs.mkdirSync(directory, { recursive: true });
  const built = await Bun.build({
    entrypoints: ["src/extension.ts", ...(final ? ["qa/cases/acceptance-fixture.ts", "qa/cases/acceptance-consumer.ts"]
      : ["qa/cases/question-fixture.ts", "qa/cases/question-consumer.ts"]),
      ...(name === "preview-denied" ? ["qa/cases/preview-denied.ts"] : [])]
      .map((file) => path.join(product, file)),
    outdir: directory, naming: "[name].js", target: "node", format: "esm",
    external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"],
  });
  assert(built.success, String(built.logs));
  const api = await Bun.build({ entrypoints: [path.join(product, "src/public-api.ts")],
    outdir: path.join(directory, "dist"), naming: "public-api.js", target: "node", format: "esm" });
  assert(api.success, String(api.logs));
  save(path.join(directory, "package.json"), { name: "coordination-question-owned-candidate", type: "module",
    exports: { "./api": "./dist/public-api.js" }, pi: { extensions: ["./extension.js",
      ...(final ? ["./acceptance-fixture.js", "./acceptance-consumer.js"] : ["./question-fixture.js", "./question-consumer.js"]),
      ...(name === "preview-denied" ? ["./preview-denied.js"] : [])] } });
  const junction = path.join(directory, "node_modules");
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", junction, "junction");
  const artifact = path.join(sandbox, "result.json");
  const argv = ["node", fileURLToPath(import.meta.url), "--native-case", name, "--sandbox", sandbox, "--result", artifact,
    ...(repairDisabled ? ["--repair-disabled"] : [])];
  const raw = path.join(sandbox, "raw.txt"), receiptPath = path.join(sandbox, "command.json");
  const receipt: Json = { argv, cwd: product, started_at: new Date().toISOString(), exit_code: null,
    source_sha256: liveHashes(), module_sha256: modules(), raw_output_path: raw,
    timeout_ms: name === "collaboration-preview" || name === "deep-ask-long" ? 660_000
      : name === "max-reasoning-nonce" ? 90_000 : 55_000, fixture_identity: sandbox };
  const fd = fs.openSync(raw, "wx");
  try {
    const child = spawn(argv[0]!, argv.slice(1), { cwd: product,
      env: { ...isolatedEnv(path.join(sandbox, "launcher")),
        COORDINATION_QA_ATTEMPT: process.env.COORDINATION_QA_ATTEMPT,
        ...(!final || repairDisabled ? { OMO_AGENT_COORDINATION_REPAIR: "0" } : {}) },
      stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
    receipt.pid = child.pid; save(receiptPath, receipt);
    for (const stream of [child.stdout, child.stderr]) stream.on("data", (bytes) => fs.writeSync(fd, bytes));
    const deadline = setTimeout(() => { receipt.timed_out = true; child.kill(); }, receipt.timeout_ms);
    try {
      receipt.exit_code = await new Promise<number>((resolve) => {
        child.once("error", (error) => { receipt.error = String(error); resolve(127); });
        child.once("close", (code, signal) => { receipt.signal = signal; resolve(code ?? 1); });
      });
    } finally { clearTimeout(deadline); }
    receipt.finished_at = new Date().toISOString(); save(receiptPath, receipt);
    assert.equal(receipt.exit_code, 0, `PREVIEW_NATIVE_FAILED:${receiptPath}`);
    const report = JSON.parse(fs.readFileSync(artifact, "utf8"));
    assert.equal(report.passed, true);
    assert(report.cleanup.sessions_disposed && report.cleanup.installed_unchanged);
    assert.equal(report.cleanup.provider_listeners, 0);
    assert.deepEqual(report.source_sha256, liveHashes(), "SOURCE_CHANGED_DURING_PREVIEW");
    const result = { ...report, command_receipt: receiptPath, qa_exit: 0 };
    save(path.join(liveEvidence, final ? name + (repairDisabled ? "-repair-disabled"
      : name === "max-reasoning-nonce" || name === "deep-ask-long" ? "-case" : "") + ".json"
      : name === "collaboration-preview" ? "preview.json" : "denied.json"), result);
    if (name === "collaboration-preview") save(path.join(liveEvidence, "ready.json"), {
      checkpoint: "collaboration-preview-v1", ...result, repair_enabled: false, passed: true,
    });
    return { case: name, passed: true, artifact, command_receipt: receiptPath };
  } finally {
    fs.closeSync(fd);
    if (fs.existsSync(junction)) { assert(fs.lstatSync(junction).isSymbolicLink()); fs.unlinkSync(junction); }
  }
}

export async function realProof(name: "max-reasoning-nonce" | "deep-ask-long", root: any, registry: any,
  rootRef: TargetRef, childContext: any, ask: (...args: any[]) => Promise<any>) {
  const current = liveHashes();
  for (const source of finalRun ? [] : [path.join(liveEvidence, name + ".json"), path.join(attempt, "task-6", name + ".json")]) {
    if (!fs.existsSync(source)) continue;
    const prior = JSON.parse(fs.readFileSync(source, "utf8"));
    if (prior.passed && fingerprint(Object.entries(prior.source_sha256).sort()) === fingerprint(Object.entries(current).sort())) {
      const proof = prior.real_model ?? prior;
      assert.equal(proof.receipt.state, "replied");
      assert.equal(proof.receipt.physical_provider_attempts, 1);
      return { evidence_path: source, evidence_sha256: sha256(source), reused: true, proof };
    }
  }
  const model = registry.find("ollama", "deepseek-v4.1-flash");
  assert(model && registry.hasConfiguredAuth(model), "BLOCKED: owner-assigned Ollama target is not configured");
  assert.equal(model.api, "openai-completions");
  assert.equal(new URL(model.baseUrl).origin + new URL(model.baseUrl).pathname.replace(/\/$/, ""), "https://ollama.com/v1");
  const nonce = randomUUID().replaceAll("-", "");
  const admission = path.join(liveEvidence, name + "-single-request.json");
  const fd = fs.openSync(admission, "wx");
  const claim: Json = { case: name, source_sha256: current, admitted_at: new Date().toISOString(),
    request_id: `${name}:${nonce}`, model: "ollama/deepseek-v4.1-flash",
    logical_request_limit: 1, retries: 0, fallback: 0, credentials_copied: false };
  fs.writeFileSync(fd, JSON.stringify(claim, null, 2) + "\n"); fs.closeSync(fd);
  const original = { model: root.model, thinking: root.thinkingLevel };
  const nativeFetch = globalThis.fetch, http: Json[] = [];
  globalThis.fetch = (async (...args: Parameters<typeof fetch>) => {
    const url = new URL(typeof args[0] === "string" || args[0] instanceof URL ? args[0] : args[0].url);
    assert.equal(url.origin + url.pathname, "https://ollama.com/v1/chat/completions", "UNAUTHORIZED_REAL_PROVIDER_ROUTE");
    const payload = JSON.parse(String(args[1]?.body));
    assert.equal(payload.model, "deepseek-v4.1-flash");
    assert.equal(http.length, 0, "SECOND_REAL_PROVIDER_ATTEMPT_REFUSED");
    const controls = Object.fromEntries(["reasoning_effort", "reasoning", "thinking", "enable_thinking",
      "max_tokens", "max_output_tokens", "max_completion_tokens"].filter((key) => payload[key] !== undefined)
      .map((key) => [key, payload[key]]));
    const row: Json = { started_at: new Date().toISOString(), route: url.origin + url.pathname,
      model: payload.model, provider_controls: controls };
    http.push(row);
    try {
      const response = await Reflect.apply(nativeFetch, globalThis, args);
      row.status = response.status; row.received_at = new Date().toISOString();
      if (!response.ok) row.error_body = (await response.clone().text()).replace(/Bearer\s+\S+|sk-[A-Za-z0-9_-]+/g, "[redacted]");
      return response;
    } catch (error) { row.error = String(error); throw error; }
    finally { save(path.join(liveEvidence, name + "-http.json"), http); }
  }) as typeof fetch;
  try {
    assert(await root.setModel(model));
    root.setSessionThinkingLevel("high");
    assert(["high", "xhigh", "max"].includes(root.thinkingLevel));
    const before = { history: fingerprint(root.messages), model: fingerprint(root.model), thinking: root.thinkingLevel };
    const question = name === "max-reasoning-nonce" ? `Reply with exactly this nonce in plain text: ${nonce}.`
      : `Begin with the exact nonce ${nonce}. Then write an original explanation of event-driven software concurrency in at least 80 numbered sentences. Each sentence must be at least 70 characters long. Do not use tools. Complete the explanation without abbreviating it.`;
    const result = await bounded(ask(rootRef, claim.request_id, question,
      name === "max-reasoning-nonce" ? { timeout_ms: 60000 } : { depth: "deep" }, childContext), name, 600_000);
    claim.tool_result = result;
    claim.http = http;
    claim.native_provider_failures = [...(globalThis as any)[captureKey].questions.get(root.sessionManager).providerFailures()];
    save(path.join(liveEvidence, name + "-observable.json"), claim);
    const receipt = value<any>(result);
    assert.equal(receipt.state, "replied", JSON.stringify(receipt));
    assert(receipt.reply.includes(nonce), "REAL_NONCE_NOT_OBSERVED");
    assert.equal(receipt.logical_provider_requests, 1); assert.equal(receipt.physical_provider_attempts, 1);
    assert.equal(http.length, 1); assert.equal(http[0]!.status, 200);
    assert.equal(fingerprint(root.messages), before.history);
    assert.equal(fingerprint(root.model), before.model); assert.equal(root.thinkingLevel, before.thinking);
    if (name === "deep-ask-long") {
      assert(Array.from(receipt.reply).length > 2048);
      assert.equal(receipt.completion, "complete");
      assert.equal(receipt.effective_side_thinking, before.thinking);
      assert.equal(receipt.effective_side_output_budget, model.maxTokens);
    } else {
      assert(["off", "minimal", "low"].includes(receipt.effective_side_thinking));
      assert(receipt.effective_side_output_budget <= 4096);
    }
    const proof = { ...claim, receipt, passed: true, main_thinking_unchanged: true, main_model_unchanged: true,
      immediate_history_unchanged: true, reply_characters: Array.from(receipt.reply).length };
    const destination = path.join(liveEvidence, name + ".json");
    save(destination, proof);
    return { evidence_path: destination, evidence_sha256: sha256(destination), reused: false, proof };
  } finally {
    globalThis.fetch = nativeFetch;
    await root.setModel(original.model); root.setSessionThinkingLevel(original.thinking);
  }
}

async function nativeCase(name: CaseName, sandbox: string, artifact: string) {
  assert(path.resolve(sandbox).startsWith(path.resolve(liveEvidence) + path.sep));
  const oldEnv = { ...process.env }, oldCwd = process.cwd();
  const env = isolatedEnv(sandbox);
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env, { OMO_AGENT_COORDINATION_REPAIR: "0" });
  const metadata = path.join(sandbox, "sdk-metadata");
  fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = metadata;
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true }); process.chdir(cwd);
  if (name === "preview-denied") {
    const plan = path.join(cwd, ".omo/plans/preview.md");
    fs.mkdirSync(path.dirname(plan), { recursive: true });
    fs.writeFileSync(plan, "# Disposable collaboration policy QA\n\nReview this isolated fixture only.\n");
    process.env.COORDINATION_PREVIEW_PLAN = plan;
  }
  const bus = new EventEmitter(), global = globalThis as any;
  const counts = { main: 0, side: 0, payloads: [] as any[], main_tools: {} as Record<string, string[]> };
  global[Symbol.for("coordination.question.qa.bus")] = bus;
  global[Symbol.for("coordination.question.qa.counts")] = counts;
  const sessions: any[] = [], clients: CoordinationClient[] = [], labels: string[] = [], mains: Promise<unknown>[] = [];
  const report: Json = { case: name, source_sha256: liveHashes(), module_sha256_before: modules(),
    started_at: new Date().toISOString(), sandbox, pid: process.pid, argv: process.argv,
    repair_enabled: false, repair_env: process.env.OMO_AGENT_COORDINATION_REPAIR, passed: false };
  let manifest: ReturnType<typeof buildAdapter> | undefined, state: any;
  let modelConfig: ReturnType<typeof disposableModelConfig> | undefined;
  let stopTerminal = () => {};
  try {
    manifest = buildAdapter({ output: path.join(sandbox, "candidate/omo-task.js") });
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
    const directory = path.join(sandbox, "question-package");
    const packages = ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg", directory];
    save(path.join(cwd, ".omo/omo.json"), {
      agents: Object.fromEntries(["explore", "oracle", "omo-native-code-reviewer", "plan-reviewer"].map((role) =>
        [role, { description: "Disposable collaboration checkpoint", prompt: "Run the controlled fixture.",
          model: "coordination-question/offline", execution_mode: "in-process" }])),
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: "in-process",
        default_concurrency: 4, global_concurrency: 4, host_shard_prewarm: "off" },
    });
    const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    const { AuthStorage, ReadOnlyAuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory({ packages, retry: { enabled: false }, sessionTitle: { enabled: false } }) });
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, []);
    const auth = AuthStorage.inMemory();
    let registry: any;
    if (name === "collaboration-preview") {
      const { ModelRuntime } = await import(pathToFileURL(senpi + "/dist/core/model-runtime.js").href);
      const { InMemoryCodingAgentModelsStore } = await import(pathToFileURL(senpi + "/dist/core/models-store.js").href);
      const modelsStore = new InMemoryCodingAgentModelsStore();
      const profile = realModelProfile;
      for (const [provider, entry] of Object.entries(JSON.parse(fs.readFileSync(profile + "/models-store.json", "utf8")))) {
        await modelsStore.write(provider, entry);
      }
      modelConfig = disposableModelConfig();
      const runtime = await ModelRuntime.create({ agentDir: env.OMO_CODING_AGENT_DIR,
        credentials: new ReadOnlyAuthStorage(profile + "/auth.json"), modelsPath: modelConfig.modelsPath,
        allowModelNetwork: false, modelsStore });
      modelConfig.observe(runtime.modelsPath);
      registry = new ModelRegistry(runtime, auth);
    } else registry = ModelRegistry.inMemory(auth);
    const root = (await sdk.createAgentSession({ cwd, agentDir: env.OMO_CODING_AGENT_DIR, resourceLoader: loader,
      authStorage: auth, modelRegistry: registry, sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    sessions.push(root);
    await root.bindExtensions({ mode: "print" });
    await root.setModel(registry.find("coordination-question", "offline")); root.setSessionThinkingLevel("high");
    state = global[captureKey];
    assert(state.capture.status.ok);
    const bridge = state.bridges.get(root.sessionManager);
    assert(bridge, "PREVIEW_NATIVE_OWNER_BRIDGE_MISSING");
    const rootCtx = root.extensionRunner.createContext();
    const execute = (tool: string, input: any, ctx = rootCtx, id: string = randomUUID()) =>
      root.extensionRunner.getToolDefinition(tool).execute(id, input, undefined, undefined, ctx);
    const ask = async (ref: TargetRef, id: string, question: string, extra: Json = {}, ctx = rootCtx) =>
      (await execute("coordination_ask", { target_ref: ref, request_id: id, question, ...extra }, ctx, id)).details;
    const completed = new Map<string, any>();
    stopTerminal = global[Symbol.for("omo.task.terminalObservers")].subscribe(({ record }: any) => {
      if (["completed", "error", "cancelled", "interrupted", "lost"].includes(record.status)) {
        completed.set(record.task_id, record); bus.emit("native-terminal", record);
      }
    });
    const settled = (id: string) => completed.has(id) ? Promise.resolve(completed.get(id))
      : event(bus, "native-terminal", (record) => record.task_id === id);
    const launch = async (label: string, role: string, model = "coordination-question/offline") => {
      const started = event(bus, "main-latch", (data) => data.label === label); labels.push(label);
      const launched = await execute("task", { subagent_type: role, model,
        prompt: `QUESTION_NATIVE_CHILD:${label}`, description: `Owned preview ${label}`, run_in_background: true });
      const id = launched.details?.task_id; assert(id, JSON.stringify(launched));
      await started;
      const snapshot = value<any[]>(bridge.listOwned()).find((row) => row.identity.task_id === id); assert(snapshot);
      const capture = state.capture.list().find((entry: any) =>
        entry.runtime()?.sessionId.replace(/^senpi:/, "") === snapshot.identity.session_id);
      assert(capture && state.capture.bindOwned(bridge, snapshot.identity.target_ref, capture).ok);
      const runtime = capture.runtime(); sessions.push(runtime);
      return { id, label, ref: snapshot.identity.target_ref as TargetRef, runtime, ctx: runtime.extensionRunner.createContext() };
    };
    const a = await launch("a", "explore"), b = await launch("b", "oracle");
    const extraTargets: Awaited<ReturnType<typeof launch>>[] = [];
    const client = value(getCoordinationClient(rootCtx)); clients.push(client);
    const child = value(getCoordinationClient(a.ctx)); clients.push(child);
    const rootRef = value(client.peers()).peers.find((peer) => peer.role === "root")!.target_ref;
    const broker = value(getCoordinationBroker());
    assert(Object.isFrozen(broker.features));
    assert.deepEqual(broker.features, ["ask_depth_deep", "ask_policy", "inspect"]);
    report.features = broker.features;
    report.loader = { extensions: loader.getExtensions().extensions.map((extension: any) =>
      ({ path: extension.path, resolved_path: extension.resolvedPath })),
      entry_sha256: sha256(path.join(directory, "extension.js")), candidate_sha256: manifest.candidate_sha256 };
    const captures = state.capture.list();
    assert.equal(captures.length, 3);
    assert.equal(new Set(captures.map((capture: any) => capture.runtime())).size, 3);
    assert.equal(global[Symbol.for("omo.senpi-task.senpiBarrel")].module.AgentSession, sdk.AgentSession);
    report.capture = { owner_count: 1, distinct_native_sessions: 3, actual_sdk_class_identity: true };
    const before = { history: fingerprint(a.runtime.messages), model: fingerprint(a.runtime.model),
      thinking: a.runtime.thinkingLevel, main: counts.main, side: counts.side };
    const frame = (label: string, data: unknown) => save(path.join(liveEvidence, "frames", `${name}-${path.basename(sandbox)}-${label}.json`), data);
    if (name === "collaboration-preview") {
      for (const runtime of [a.runtime, b.runtime]) assert(["coordination_inspect", "coordination_ask", "coordination_notify"]
        .every((tool) => runtime.getActiveToolNames().includes(tool)));
      assert(["coordination_inspect", "coordination_ask", "coordination_notify"].every((tool) => counts.main_tools.a!.includes(tool)));
      const inspectCount = counts.side;
      const inspect = value<any>((await execute("coordination_inspect", {})).details);
      assert.equal(inspect.coverage, "complete"); assert.equal(inspect.owned, 3); assert.equal(inspect.visited, 3);
      assert.equal(inspect.unreadable, 0); assert.equal(counts.side, inspectCount); frame("inspect", inspect);
      const reply = value<any>(await ask(a.ref, "preview-tool-nonce", "NONCE:preview-tool-nonce"));
      assert.equal(reply.state, "replied"); assert.equal(reply.reply, "preview-tool-nonce");
      assert.deepEqual(await client.ask({ target_ref: a.ref, request_id: "preview-tool-nonce", question: "NONCE:preview-tool-nonce" }),
        { ok: true, value: reply });
      assert.equal(counts.side, before.side + 1); frame("ask", reply);
      const sibling = value(await child.ask({ target_ref: b.ref, request_id: "preview-sibling", question: "NONCE:preview-sibling" }));
      assert.equal(sibling.reply, "preview-sibling");
      assert.equal(fingerprint(a.runtime.messages), before.history); assert.equal(fingerprint(a.runtime.model), before.model);
      assert.equal(a.runtime.thinkingLevel, before.thinking); assert.equal(counts.main, before.main);
      const rootStarted = event(bus, "main-latch", (data) => data.label === "root"); labels.push("root");
      const main = root.prompt("QUESTION_NATIVE_CHILD:root"); mains.push(main); await rootStarted;
      const rootHistory = fingerprint(root.messages), rootMain = counts.main;
      const toRoot = value<any>(await ask(rootRef, "preview-child-root", "NONCE:preview-child-root", {}, a.ctx));
      assert.equal(toRoot.reply, "preview-child-root");
      const count = counts.side;
      const note = value<any>((await execute("coordination_notify", { target_ref: rootRef,
        request_id: "preview-note", message: "preview-next-natural-turn-note" }, a.ctx)).details);
      assert.equal(note.incorporation, "queued_for_next_turn"); assert.equal(counts.side, count);
      assert.equal(fingerprint(root.messages), rootHistory); assert.equal(counts.main, rootMain);
      // A fresh consumer target avoids the scripted provider's first-NONCE
      // parser mistaking an earlier approved pending exchange for this question.
      const consumerTarget = await launch("consumer", "oracle"); extraTargets.push(consumerTarget);
      const consumer = (await execute("question_api_consumer", { target_ref: consumerTarget.ref })).details;
      assert.equal(consumer.passed, true); assert.equal(consumer.capture_delta, 0); assert.equal(consumer.provider_delta, 0);
      assert.equal(consumer.subscribed_revisions.length, 1); assert(consumer.hook_unchanged);
      const inspected = value(await client.inspect(consumerTarget.ref)); assert.equal(inspected.live, true);
      assert.equal(inspected.asks, "allowed", "CONSUMER_DISPOSE_DID_NOT_CLEAR_POLICY");
      const apiNote = value(await client.notify({ target_ref: consumerTarget.ref,
        request_id: "consumer-note", message: "consumer-note-nonce" }));
      const deniedPolicy = value(client.setAskPolicy({ target_ref: consumerTarget.ref, policy: "deny_peer_asks" }));
      assert.equal(deniedPolicy.asks, "denied");
      const resetPolicy = value(client.setAskPolicy({ target_ref: consumerTarget.ref, policy: "default" }));
      assert.equal(resetPolicy.asks, "allowed");
      bus.emit("release-main:root"); await bounded(main, "busy root native result");
      assert.equal(root.getLastAssistantText(), "native-main-result:root");
      assert.equal(root.messages.filter((message: any) => message.customType === queueType).length, 0);
      const rootEnd = new Promise<void>((resolve) => {
        const stop = root.subscribe((event: any) => { if (event.type === "agent_end") { stop(); resolve(); } });
      });
      await bounded(root.prompt("Normal next root turn."), "normal next root prompt");
      await bounded(rootEnd, "native next-turn agent_end");
      const peerMessages = () => root.messages.filter((message: any) => message.customType === queueType);
      assert.equal(peerMessages().filter((message: any) => String(message.content).includes("preview-next-natural-turn-note")).length, 1);
      assert.equal(peerMessages().filter((message: any) => String(message.content).includes("preview-child-root")).length, 1);
      assert.equal(note.incorporation, "incorporated");
      const consumedCount = peerMessages().length;
      await bounded(root.prompt("Another normal root turn."), "second natural root turn");
      assert.equal(peerMessages().length, consumedCount);
      report.tools = { coordination_inspect: inspect, coordination_ask: reply, coordination_notify: note };
      report.api = { consumer, peers: value(client.peers()), ask: consumer.reply, notify: apiNote,
        inspect: inspected, setAskPolicy: { denied: deniedPolicy, reset: resetPolicy },
        onPeersChanged: consumer.subscribed_revisions, dispose: { stale_client_refused: consumer.stale_client_refused },
        readonly_to_sibling: sibling, readonly_to_root: toRoot,
        tool_api_duplicate_provider_calls: 1, roster_changes_only: true, policy_dispose_cleared: true };
      report.main = { immediate_history_changes: 0, forced_turns: 0, aborts: 0, lead_relay_calls: 0,
        busy_root_result: root.getLastAssistantText(), original_busy_result_observed: true,
        note_consumed_once: true, question_consumed_once: true, main_thinking_unchanged: true };
      frame("notify", note); frame("api", report.api);
      report.real_models = {};
      for (const realName of ["max-reasoning-nonce", "deep-ask-long"] as const) {
        report.real_models[realName] = await realProof(realName, root, registry, rootRef, a.ctx, ask);
      }
    } else {
      const reviewer = await launch("reviewer", "omo-native-code-reviewer");
      extraTargets.push(reviewer);
      const oneShotCount = counts.side;
      const oneShot = await execute("task", { subagent_type: "plan-reviewer", model: "coordination-question/offline",
        prompt: "Independent one-shot admission probe.", description: "Owned protected review", run_in_background: true });
      assert.equal(oneShot.details.status, "denied"); assert.equal(counts.side, oneShotCount);
      // Open the native gate through ordinary disposable-session input and a
      // model-issued, actually executed plan write, never injected tracker state.
      const originalModel = root.model;
      await root.setModel(registry.find("coordination-preview-review", "offline"));
      assert.equal(root.model.provider, "coordination-preview-review");
      const readObserved = new Promise<any>((resolve) => {
        const stop = root.subscribe((event: any) => {
          if (event.type === "tool_execution_end" && event.toolName === "write" && !event.isError) {
            stop(); resolve(event);
          }
        });
      });
      await bounded(root.prompt("Please run ulw-plan and write the disposable preview plan."), "native plan request");
      const planRead = await bounded(readObserved, "native plan artifact write");
      await root.setModel(originalModel);
      assert.equal(root.model, originalModel);
      const planReviewer = await launch("planreviewer", "plan-reviewer", "coordination-preview-review/offline");
      extraTargets.push(planReviewer);
      const reviewerPeer = value(client.peers()).peers.find((peer) => peer.target_ref === planReviewer.ref)!;
      assert(reviewerPeer);
      assert.equal(reviewerPeer.role, "plan-reviewer");
      assert.equal(reviewerPeer.denial_reason, "one_shot");
      assert.equal(reviewerPeer.ask_capability, false); assert.equal(reviewerPeer.notify_capability, false);
      const reviewerBefore = { provider: counts.side, queue: planReviewer.runtime._pendingNextTurnMessages.length,
        history: fingerprint(planReviewer.runtime.messages), main: counts.main };
      const reviewerAsk = await ask(planReviewer.ref, "one-shot-ask", "Denied one-shot question");
      const reviewerNotify = (await execute("coordination_notify", { target_ref: planReviewer.ref,
        request_id: "one-shot-notify", message: "Denied one-shot note" })).details;
      const reviewerApiAsk = await client.ask({ target_ref: planReviewer.ref, request_id: "one-shot-api-ask", question: "Denied" });
      const reviewerApiNotify = await client.notify({ target_ref: planReviewer.ref, request_id: "one-shot-api-note", message: "Denied" });
      for (const result of [reviewerAsk, reviewerNotify, reviewerApiAsk, reviewerApiNotify]) {
        assert.deepEqual(result, { ok: false, reason: "scope_denied" });
      }
      assert.equal(counts.side, reviewerBefore.provider);
      assert.equal(planReviewer.runtime._pendingNextTurnMessages.length, reviewerBefore.queue);
      assert.equal(fingerprint(planReviewer.runtime.messages), reviewerBefore.history);
      assert.equal(counts.main, reviewerBefore.main);
      const reviewerPolicy = { verified: true, task_id: planReviewer.id, target_ref: planReviewer.ref,
        native_snapshot: value(bridge.inspectOwned(planReviewer.ref)), peer: reviewerPeer,
        coordination_ask: reviewerAsk, coordination_notify: reviewerNotify,
        api_ask: reviewerApiAsk, api_notify: reviewerApiNotify,
        provider_calls_delta: counts.side + counts.main - reviewerBefore.provider - reviewerBefore.main,
        queue_delta: planReviewer.runtime._pendingNextTurnMessages.length - reviewerBefore.queue,
        main_history_preserved: true, native_gate: { supplementary_admission_denial: oneShot.details,
          plan_write_tool_call_id: planRead.toolCallId, request_source: "ordinary root.prompt input" } };
      const denialBefore = counts.side, queueBefore = reviewer.runtime._pendingNextTurnMessages.length;
      const independent = await ask(reviewer.ref, "independent-denied", "Denied independent question");
      assert.deepEqual(independent, { ok: false, reason: "scope_denied" });
      const independentNote = await client.notify({ target_ref: reviewer.ref, request_id: "independent-note", message: "Denied" });
      assert.deepEqual(independentNote, { ok: false, reason: "scope_denied" });
      assert.deepEqual(getCoordinationClient(reviewer.ctx), { ok: false, reason: "scope_denied" });
      assert.equal(reviewer.runtime._pendingNextTurnMessages.length, queueBefore);
      const policy = value(client.setAskPolicy({ target_ref: b.ref, policy: "deny_peer_asks" }));
      const pending = b.runtime._pendingNextTurnMessages.length;
      const policyAsk = await ask(b.ref, "policy-denied", "Denied policy question");
      const policyNote = (await execute("coordination_notify", { target_ref: b.ref,
        request_id: "policy-note", message: "Denied" })).details;
      const apiAsk = await child.ask({ target_ref: b.ref, request_id: "policy-api", question: "Denied" });
      const apiNote = await child.notify({ target_ref: b.ref, request_id: "policy-api-note", message: "Denied" });
      for (const denied of [policyAsk, policyNote, apiAsk, apiNote]) assert.deepEqual(denied, { ok: false, reason: "target_excluded" });
      assert.equal(value(await client.inspect(b.ref)).asks, "denied");
      assert.equal(b.runtime._pendingNextTurnMessages.length, pending);
      assert.deepEqual(child.setAskPolicy({ target_ref: b.ref, policy: "default" }), { ok: false, reason: "policy_not_owner" });
      assert.equal(counts.side, denialBefore);
      assert.equal(fingerprint(a.runtime.messages), before.history);
      state.dispose();
      const oldTarget = await ask(a.ref, "old-target", "Denied stale target");
      assert.deepEqual(oldTarget, { ok: false, reason: "stale_generation" });
      assert.deepEqual(await client.inspect(a.ref), { ok: false, reason: "stale_generation" });
      const unavailable = getCoordinationClient(rootCtx);
      assert.deepEqual(unavailable, { ok: false, reason: "coordination_unavailable" });
      assert.equal(counts.side, denialBefore);
      report.denials = { plan_reviewer: reviewerPolicy, independent_review: independent, independent_notify: independentNote,
        policy, tool_ask: policyAsk, tool_notify: policyNote, api_ask: apiAsk, api_notify: apiNote,
        old_target: { target_ref: a.ref, result: oldTarget }, api_absent: unavailable,
        provider_calls: counts.side - denialBefore, queue_additions: 0, main_history_preserved: true };
      frame("denials", report.denials);
    }
    report.native_main_results = [];
    for (const target of [a, b, ...extraTargets]) {
      const done = settled(target.id);
      bus.emit(`release-main:${target.label}`);
      assert.equal((await done).status, "completed");
      await bounded(target.runtime.waitForIdle(), "original readonly main idle");
      assert.equal(target.runtime.getLastAssistantText(), `native-main-result:${target.label}`);
      report.native_main_results.push({ task_id: target.id, status: "completed", result: target.runtime.getLastAssistantText() });
    }
    report.provider_counts = counts; report.passed = true;
  } catch (error) {
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    throw error;
  } finally {
    try {
    for (const label of labels) bus.emit(`release-main:${label}`);
    for (const main of mains) await bounded(main, "cleanup root main");
    stopTerminal();
    for (const client of clients) client.dispose();
    for (const capture of state?.capture.list() ?? []) if (capture.runtime()) sessions.push(capture.runtime());
    for (const session of [...new Set(sessions)].reverse()) {
      if (session._disposed) continue;
      await bounded(session.waitForIdle(), "cleanup native idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    }
    state?.dispose();
    if (manifest && sha256(manifest.output) === manifest.candidate_sha256) report.removal = removeAdapter(manifest);
    const junction = path.join(sandbox, "candidate/node_modules");
    if (fs.existsSync(junction)) { assert(fs.lstatSync(junction).isSymbolicLink()); fs.unlinkSync(junction); }
    bus.removeAllListeners();
    report.cleanup = { sessions_disposed: sessions.every((session) => session._disposed),
      provider_listeners: bus.eventNames().length, installed_unchanged: fingerprint(modules()) === fingerprint(report.module_sha256_before),
      adapter_restored: manifest ? sha256(manifest.output) === manifest.baseline_sha256 : true };
    } finally {
    if (modelConfig) report.model_config = modelConfig.dispose();
    report.ended_at = new Date().toISOString(); save(artifact, report);
    process.chdir(oldCwd);
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, oldEnv);
    }
  }
  return report;
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const flag = (name: string) => process.argv[process.argv.indexOf(name) + 1]!;
  const name = flag("--native-case") as CaseName;
  const report = finalRun
    ? await (await import("./cases/acceptance-native.ts")).nativeAcceptance(name as FinalCase, flag("--sandbox"), flag("--result"),
      { liveEvidence, liveHashes, realProof })
    : await nativeCase(name, flag("--sandbox"), flag("--result"));
  console.log(JSON.stringify({ case: name, passed: report.passed, result: flag("--result") }));
}

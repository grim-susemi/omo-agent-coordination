import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { createHash, randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { syncBuiltinESMExports } from "node:module";
import childProcess from "node:child_process";
import { createConnection } from "node:net";
import { connectRemoteRuntime } from "../../src/peer-channel.ts";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";
import { bundle, senpi, isolatedEnv, sha256 } from "../command.mjs";
import { getCoordinationBroker, getCoordinationClient } from "../../src/public-api.ts";
import type { FinalCase, realProof } from "../live.ts";
import type { CoordinationClient } from "../../src/contracts.ts";
import { disposableModelConfig, realModelProfile } from "../model-config.ts";

type Json = Record<string, any>;
const captureKey = Symbol.for("omo.agent-coordination.capture.v1");
const queueType = "omo-agent-coordination-peer-v1";
const value = (result: any): any => { assert(result.ok, JSON.stringify(result)); return result.value; };
const fingerprint = (data: any) => createHash("sha256").update(JSON.stringify(data)).digest("hex");
const save = (file: string, data: any) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(data, null, 2) + "\n"); };
async function bounded<T>(promise: Promise<T>, label: string, ms = 15_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try { return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`ACCEPTANCE_EVENT_DEADLINE:${label}`)), ms);
  })]); } finally { clearTimeout(timer!); }
}
function event(bus: EventEmitter, name: string, predicate: (data: any) => boolean = () => true) {
  let listener: (data: any) => void;
  const pending = bounded(new Promise<any>(resolve => {
    listener = data => { if (predicate(data)) { bus.off(name, listener); resolve(data); } };
    bus.on(name, listener);
  }), name).finally(() => bus.off(name, listener));
  void pending.catch(() => {});
  return pending;
}
export async function nativeAcceptance(name: FinalCase, sandbox: string, artifact: string, helpers: {
  liveEvidence: string; liveHashes(): Record<string, string>; realProof: typeof realProof;
}) {
  const { liveEvidence, liveHashes, realProof } = helpers;
  assert(path.resolve(sandbox).startsWith(path.resolve(liveEvidence) + path.sep));
  const env = isolatedEnv(sandbox);
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  const repairEnabled = !process.argv.includes("--repair-disabled");
  if (!repairEnabled) process.env.OMO_AGENT_COORDINATION_REPAIR = "0";
  const metadata = path.join(sandbox, "sdk-metadata"); fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = metadata;
  const cwd = path.join(sandbox, "work"); fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true }); process.chdir(cwd);
  const global = globalThis as any, bus = new EventEmitter();
  const counts: Json = { main: 0, side: 0, payloads: [], emitted: [], main_tools: {}, events: [] };
  global[Symbol.for("coordination.acceptance.bus")] = bus;
  global[Symbol.for("coordination.acceptance.counts")] = counts;
  const sessions: any[] = [], clients: CoordinationClient[] = [], labels: string[] = [];
  const activePrompts: Promise<any>[] = [], emitted: Json[] = [];
  const beforeModules = { [bundle]: sha256(bundle), [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js") };
  const report: Json = { case: name, source_sha256: liveHashes(), pid: process.pid, argv: process.argv,
    started_at: new Date().toISOString(), repair_enabled: repairEnabled, passed: false };
  let state: any, manifest: any, stopTerminal = () => {}, processHandle: any, processGate: string | undefined;
  let modelConfig: ReturnType<typeof disposableModelConfig> | undefined;
  let restoreUi = () => {};
  const originalRename = fs.renameSync;
  const originalLink = fs.linkSync, bootstrapTickets = new Map<string, Json>();
  fs.linkSync = function (from, to) {
    const result = originalLink(from, to);
    if (String(to).replaceAll("\\", "/").includes("/agent-coordination/bootstrap/") && String(to).endsWith(".json")) {
      bootstrapTickets.set(path.basename(String(to), ".json"), JSON.parse(fs.readFileSync(to, "utf8")));
    }
    return result;
  } as typeof fs.linkSync;
  const originalSpawn = childProcess.spawn, spawned: Json[] = [];
  let processSpawnHook: ((row: Json) => void) | undefined;
  // Exact ownership: the task manager gives its own RPC child
  // <state_dir>/children/<task_id>/sessions/<task_id>, so this is checkable against the task record.
  const ownedRpcTask = (row: Json, options: any): string | undefined => {
    if (!row.argv.includes("--mode") || !row.argv.includes("rpc")) return undefined;
    const childEnv = options?.env as Record<string, string | undefined> | undefined;
    if (childEnv?.OMO_SENPI_TASK_RPC_CHILD !== "1") return undefined;
    const sessionDir = childEnv?.SENPI_CODING_AGENT_SESSION_DIR;
    if (typeof sessionDir !== "string") return undefined;
    const dir = path.resolve(sessionDir), children = path.resolve(path.join(sandbox, "senpi-task", "children"));
    if (!dir.toLowerCase().startsWith(children.toLowerCase() + path.sep)) return undefined;
    // The native layout is children/<task_id>/sessions/<task_id>; the middle segment is never
    // the task id, so both task segments must agree and the middle one must be "sessions".
    const segments = path.relative(children, dir).split(path.sep);
    const [task, sessions, session] = segments;
    if (segments.length !== 3 || sessions !== "sessions" || task !== session || !task) return undefined;
    return /^st_[a-z0-9]+$/.test(task) ? task : undefined;
  };
  childProcess.spawn = function (this: typeof childProcess, ...args: any[]) {
    const child = Reflect.apply(originalSpawn, this, args as any);
    const row: Json = { executable: args[0], argv: [args[0], ...(Array.isArray(args[1]) ? args[1] : [])],
      pid: child.pid, observed_at: new Date().toISOString(), cwd: args[2]?.cwd };
    const owner = ownedRpcTask(row, args[2]);
    if (owner) row.owner_task_id = owner;
    spawned.push(row); save(path.join(sandbox, "spawned.json"), spawned);
    processSpawnHook?.(row);
    return child;
  } as typeof childProcess.spawn;
  syncBuiltinESMExports();
  const nativeResults = new Map<string, any>();
  try {
    manifest = buildAdapter({ output: path.join(sandbox, "candidate/omo-task.js") });
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
    const directory = path.join(sandbox, "question-package");
    const packages = ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg", directory];
    save(path.join(env.OMO_CODING_AGENT_DIR!, "settings.json"), { packages });
    report.process_route = "unmodified native default executable selection";
    save(path.join(cwd, ".omo/omo.json"), {
      agents: Object.fromEntries(["explore", "oracle", "acceptance-process", "omo-native-code-reviewer"].map(role =>
        [role, { description: "Owned final integration fixture", prompt: "Run the controlled acceptance only.",
          model: "coordination-acceptance/offline", execution_mode: role === "acceptance-process" ? "process" : "in-process" }])),
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: "in-process",
        default_concurrency: 6, global_concurrency: 6, host_shard_prewarm: "off", process_runner: "child-process" },
    });
    const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    const theme = await import(pathToFileURL(senpi + "/dist/modes/interactive/theme/theme.js").href);
    sdk.initTheme("dark");
    const { AuthStorage, ReadOnlyAuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const auth = AuthStorage.inMemory();
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory({ packages, retry: { enabled: false }, sessionTitle: { enabled: false } }) });
    await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
    const real = name === "max-reasoning-nonce" || name === "deep-ask-long";
    let registry: any;
    if (real) {
      const { ModelRuntime } = await import(pathToFileURL(senpi + "/dist/core/model-runtime.js").href);
      const { InMemoryCodingAgentModelsStore } = await import(pathToFileURL(senpi + "/dist/core/models-store.js").href);
      const modelsStore = new InMemoryCodingAgentModelsStore(), profile = realModelProfile;
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
    sessions.push(root); await root.bindExtensions({ mode: "print" });
    await root.setModel(registry.find("coordination-acceptance", "offline")); root.setSessionThinkingLevel("high");
    state = global[captureKey]; assert(state.capture.status.ok);
    const bridge = state.bridges.get(root.sessionManager); assert(bridge);
    const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
    const engine = carrier.contexts.get(root.sessionManager).deref(), store = carrier.links.get(engine).store;
    const ports = carrier.ports.get(store), releases: Json[] = [], notifications: Json[] = [];
    const oldRelease = ports.releaseSlot, oldNotify = engine.notifier.notifyTerminal;
    ports.releaseSlot = (id: string, model: string, epoch: number) => { releases.push({ id, epoch }); return oldRelease(id, model, epoch); };
    engine.notifier.notifyTerminal = (input: any) => {
      notifications.push({ id: input.record.task_id, epoch: input.record.notification.run_epoch }); return oldNotify(input);
    };
    const rootCtx = root.extensionRunner.createContext();
    const uiEvents: Json[] = [];
    const originalUiNotify = rootCtx.ui.notify;
    rootCtx.ui.notify = (message: string, kind: string) => {
      uiEvents.push({ message, kind });
      return originalUiNotify.call(rootCtx.ui, message, kind);
    };
    restoreUi = () => { rootCtx.ui.notify = originalUiNotify; };
    const execute = (name: string, args: Json, ctx = rootCtx, signal?: AbortSignal) =>
      root.extensionRunner.getToolDefinition(name).execute(randomUUID(), args, signal, undefined, ctx);
    const ask = async (ref: string, id: string, question: string, extra: Json = {}, ctx = rootCtx) =>
      (await execute("coordination_ask", { target_ref: ref, request_id: id, question, ...extra }, ctx)).details;
    stopTerminal = global[Symbol.for("omo.task.terminalObservers")].subscribe(({ record }: any) => {
      if (["completed", "error", "cancelled", "interrupted", "lost"].includes(record.status)) {
        nativeResults.set(`${record.task_id}:${record.notification.run_epoch}`, record); bus.emit("terminal", record);
      }
    });
    const terminal = (id: string, epoch = 0) => nativeResults.has(`${id}:${epoch}`)
      ? Promise.resolve(nativeResults.get(`${id}:${epoch}`)) : event(bus, "terminal", row => row.task_id === id && row.notification.run_epoch === epoch);
    const launch = async (label: string, role = "explore") => {
      const started = event(bus, "latch", row => row.label === label); labels.push(label);
      const launched = await execute("task", { subagent_type: role, model: "coordination-acceptance/offline",
        prompt: `ACCEPTANCE_HOLD:${label}`, description: `Final integration ${label}`, run_in_background: true });
      const id = launched.details?.task_id; assert(id, JSON.stringify(launched)); await started;
      const row = value(bridge.listOwned()).find((row: any) => row.identity.task_id === id); assert(row);
      const handle = engine.manager.getResidentHandle(id), runtime = carrier.runtimeOwners.get(handle).runtime.deref();
      const capture = value(state.capture.forRuntime(runtime)); assert(state.capture.bindOwned(bridge, row.identity.target_ref, capture).ok);
      sessions.push(runtime);
      return { id, ref: row.identity.target_ref, runtime, label, ctx: runtime.extensionRunner.createContext() };
    };
    const modelCall = async (runtime: any, name: string, args: Json) => {
      const id = `model-${randomUUID()}`;
      let stop = () => {};
      const observed = new Promise<any>(resolve => {
        stop = runtime.subscribe((event: any) => { if (event.type === "tool_execution_end" && event.toolCallId === id) { stop(); resolve(event); } });
      });
      const spec = { id, name, args };
      const prompt = runtime.prompt(`ACCEPTANCE_CALL:${Buffer.from(JSON.stringify(spec)).toString("base64")}`);
      activePrompts.push(prompt);
      try {
        const event = await bounded(observed, `model-issued ${name}`);
        await bounded(prompt, `model issuer natural completion ${name}`);
        assert.equal(event.toolName, name);
        assert(counts.emitted.some((entry: Json) => entry.id === id && entry.name === name));
        emitted.push({ ...spec, result: event.result, isError: event.isError, session_id: runtime.sessionId });
        if (name.startsWith("coordination_")) for (const width of [32, 80]) {
          const definition = state.tools.find((tool: any) => tool.name === name);
          const lines = definition.renderResult(event.result, { expanded: true, isPartial: false }, theme.theme).render(width);
          assert(lines.length && lines.every((line: string) => sdk.visibleWidth(line) <= width));
          const frame = path.join(liveEvidence, "raw-frames", `${path.basename(sandbox)}-${id}-${width}.ansi.txt`);
          fs.mkdirSync(path.dirname(frame), { recursive: true }); fs.writeFileSync(frame, lines.join("\n") + "\n");
          (report.raw_frames ??= []).push(frame);
        }
        return event.result;
      } finally { stop(); }
    };
    const a = await launch("a"), b = await launch("b", "oracle");
    const client = value(getCoordinationClient(rootCtx)) as CoordinationClient; clients.push(client);
    const child = value(getCoordinationClient(a.ctx)) as CoordinationClient; clients.push(child);
    const rootRef = value(client.peers()).peers.find((peer: any) => peer.role === "root").target_ref;
    global[Symbol.for("coordination.acceptance.root_ref")] = rootRef;
    global[Symbol.for("coordination.acceptance.peer_refs")] = [rootRef, a.ref, b.ref];
    const features = value(getCoordinationBroker()).features;
    assert.deepEqual(features, ["ask_depth_deep", "ask_policy", "inspect", ...(repairEnabled ? ["revival_settlement_repair"] : [])]);
    assert(Object.isFrozen(features));
    report.features = features;
    report.loader = { extensions: loader.getExtensions().extensions.map((entry: any) => entry.resolvedPath),
      entry_sha256: sha256(path.join(directory, "extension.js")), native_bundle_sha256: manifest.candidate_sha256,
      actual_class_identity: global[Symbol.for("omo.senpi-task.senpiBarrel")].module.AgentSession === sdk.AgentSession };
    assert(report.loader.actual_class_identity);
    const before = { history: fingerprint(a.runtime.messages), model: fingerprint(a.runtime.model), thinking: a.runtime.thinkingLevel };
    if (real) {
      report.real_model = await realProof(name, root, registry, rootRef, a.ctx, ask);
    } else if (name === "deep-abort") {
      const controller = new AbortController();
      const started = event(bus, "side-start", row => row.nonce === "abort-nonce");
      const aborted = event(bus, "side-aborted", row => row.nonce === "abort-nonce");
      const pending = execute("coordination_ask", { target_ref: a.ref, question: "NONCE:abort-nonce CONTROL:gate",
        depth: "deep", request_id: "final-deep-abort" }, rootCtx, controller.signal);
      await started;
      assert.deepEqual(await client.ask({ target_ref: a.ref, depth: "deep", question: "busy", request_id: "busy" }),
        { ok: false, reason: "target_busy" });
      controller.abort(); await aborted;
      const cancelled = value((await pending).details);
      assert.equal(cancelled.state, "cancelled"); assert.equal(cancelled.ended_by, "asker_aborted");
      bus.emit("side-release:abort-nonce");
      const service = state.questions.get(root.sessionManager);
      assert.equal(service.diagnostics().active, 0); assert.equal(service.diagnostics().pending, 0);
      assert.equal(fingerprint(a.runtime.messages), before.history);
      assert.equal(a.runtime._pendingNextTurnMessages.length, 0);
      assert.equal(value(await client.ask({ target_ref: a.ref, question: "NONCE:after-abort", request_id: "after-abort" })).reply, "after-abort");
      report.deep_abort = { cancelled, late_output_discarded: true, stream_aborted: true, capacity_reused: true,
        pending_after_abort: 0, active_after_abort: 0, main_unchanged: true };
    } else if (name === "ask-policy") {
      const policy = value(getCoordinationClient(rootCtx)) as CoordinationClient; clients.push(policy);
      value(policy.setAskPolicy({ target_ref: a.ref, policy: "deny_peer_asks" }));
      const sibling = value(getCoordinationClient(b.ctx)) as CoordinationClient; clients.push(sibling);
      const count = counts.side, pending = a.runtime._pendingNextTurnMessages.length;
      const denied = await sibling.ask({ target_ref: a.ref, question: "denied", request_id: "denied" });
      const note = await sibling.notify({ target_ref: a.ref, message: "denied", request_id: "denied-note" });
      assert.deepEqual(denied, { ok: false, reason: "target_excluded" }); assert.deepEqual(note, denied);
      assert.equal(counts.side, count); assert.equal(a.runtime._pendingNextTurnMessages.length, pending);
      assert.equal(value(await child.ask({ target_ref: rootRef, question: "NONCE:policy-outbound", request_id: "outbound" })).reply, "policy-outbound");
      assert.deepEqual(sibling.setAskPolicy({ target_ref: a.ref, policy: "default" }), { ok: false, reason: "policy_not_owner" });
      assert.equal(value(await client.inspect(a.ref)).asks, "denied"); policy.dispose();
      assert.equal(value(await client.inspect(a.ref)).asks, "allowed");
      report.ask_policy = { denied, note, provider_calls_on_denial: 0, queue_additions_on_denial: 0,
        own_outbound_allowed: true, dispose_cleared: true, receipts: state.questions.get(root.sessionManager).policyReceipts() };
    } else if (name === "inspect-api") {
      const count = counts.side;
      const live = value(await client.inspect(a.ref)); assert.equal(live.live, true); assert.equal(live.record_consistent, true);
      const done = terminal(a.id); bus.emit("release:a"); await done; await bounded(a.runtime.waitForIdle(), "settled idle");
      const settled = value(await client.inspect(a.ref)); assert.equal(settled.live, false); assert.equal(settled.record_consistent, true);
      const started = event(bus, "latch", row => row.label === "stuck"); labels.push("stuck");
      await execute("task_send", { to: a.id, message: "ACCEPTANCE_HOLD:stuck" }); await started;
      const row = value(bridge.listOwned()).find((row: any) => row.identity.task_id === a.id);
      const revivedLive = value(await client.inspect(row.identity.target_ref));
      assert.equal(revivedLive.live, true); assert.equal(revivedLive.run_epoch, 1);
      const handle = engine.manager.getResidentHandle(a.id), outcome = handle.waitForOutcome();
      let failures = 0;
      const publicationFailed = event(bus, "stuck-publication-failed", row => row.failures >= (repairEnabled ? 2 : 1));
      const file = path.join(sandbox, "senpi-task/tasks", a.id + ".json");
      fs.renameSync = function(from: any, to: any) {
        if (path.resolve(String(to)) === path.resolve(file)) {
          const record = JSON.parse(fs.readFileSync(from, "utf8"));
          if (record.status === "completed" && record.notification.run_epoch === 1) {
            failures++; bus.emit("stuck-publication-failed", { failures });
            throw Object.assign(new Error("Owned final stuck inspection probe"), { code: "ENOSPC" });
          }
        }
        return originalRename(from, to);
      } as typeof fs.renameSync;
      syncBuiltinESMExports();
      bus.emit("release:stuck"); assert.equal((await bounded<any>(outcome, "stuck outcome")).status, "completed");
      await publicationFailed;
      await bounded(root.waitForIdle(), "stuck publication");
      const stuck = value(await client.inspect(row.identity.target_ref));
      assert(failures > 0); assert.equal(stuck.live, false); assert.equal(stuck.record_consistent, false);
      const recordHash = sha256(file); await client.inspect(row.identity.target_ref); assert.equal(sha256(file), recordHash);
      fs.renameSync = originalRename; syncBuiltinESMExports();
      assert.equal(counts.side, count);
      report.inspect_api = { live, settled, revived_live: revivedLive, stuck, refused_publications: failures, provider_calls: 0, features };
    } else {
      const inspect = value((await modelCall(root, "coordination_inspect", {})).details);
      assert.equal(counts.roster_context[emitted.at(-1)!.id], true, "AUTOMATIC_ROSTER_MISSING_FROM_FIRST_MODEL_CONTEXT");
      assert.equal(inspect.coverage, "complete"); assert.equal(inspect.unreadable, 0); assert.equal(inspect.owned, 3);
      const reviewLabel = "independent-execution", reviewStarted = event(bus, "latch", row => row.label === reviewLabel);
      labels.push(reviewLabel);
      const reviewSpec = { parent_session_id: engine.runtime.sessionId(),
        root_session_id: engine.ancestry?.rootSessionId ?? engine.runtime.sessionId(), depth: 1,
        subagent_type: "explore", model: "coordination-acceptance/offline", run_in_background: true,
        prompt: `ACCEPTANCE_HOLD:${reviewLabel}`, description: "Owner-designated ordinary-role independent execution" };
      carrier.independentReview(engine, reviewSpec);
      const reviewLaunch = await engine.manager.start(reviewSpec);
      assert.equal(reviewLaunch.kind, "started"); await reviewStarted;
      const reviewRecord = store.load(reviewLaunch.task_id);
      assert.equal(reviewRecord.agent_type, "explore"); assert.equal(reviewRecord.model, "coordination-acceptance/offline");
      assert(reviewRecord.tool_deny.includes("__coordination_independent_review__"));
      const reviewRow = value(bridge.listOwned()).find((row: any) => row.identity.task_id === reviewLaunch.task_id);
      assert.equal(reviewRow.collaboration.denial_reason, "independent_review");
      const reviewHandle = engine.manager.getResidentHandle(reviewLaunch.task_id);
      const reviewRuntime = carrier.runtimeOwners.get(reviewHandle).runtime.deref();
      sessions.push(reviewRuntime);
      const reviewCtx = reviewRuntime.extensionRunner.createContext();
      const reviewClient = value(getCoordinationClient(reviewCtx)) as CoordinationClient; clients.push(reviewClient);
      assert.equal(value(client.peers()).peers.find((peer: any) => peer.target_ref === reviewRow.identity.target_ref).denial_reason,
        "independent_review");
      const reviewRef = reviewRow.identity.target_ref, reviewCalls = counts.side;
      const queueCount = () => [root, a.runtime, b.runtime, reviewRuntime]
        .reduce((count, runtime) => count + runtime._pendingNextTurnMessages.length, 0);
      const reviewQueue = queueCount(), reviewRefusals: Json[] = [];
      for (const [ctx, target_ref, direction] of [[rootCtx, reviewRef, "target"], [reviewCtx, a.ref, "caller"]] as const) {
        for (const operation of ["coordination_ask", "coordination_notify"]) {
          const result = (await execute(operation, { target_ref, request_id: `review-tool-${direction}-${operation}`,
            ...(operation === "coordination_ask" ? { question: "must not run" } : { message: "must not queue" }) }, ctx)).details;
          assert.deepEqual(result, { ok: false, reason: "scope_denied" });
          reviewRefusals.push({ surface: "tool", direction, operation, result });
        }
      }
      for (const [api, target_ref, direction] of [[client, reviewRef, "target"], [reviewClient, a.ref, "caller"]] as const) {
        const askResult = await api.ask({ target_ref, request_id: `review-api-${direction}`, question: "must not run" });
        const notifyResult = await api.notify({ target_ref, request_id: `review-api-note-${direction}`, message: "must not queue" });
        assert.deepEqual(askResult, { ok: false, reason: "scope_denied" }); assert.deepEqual(notifyResult, askResult);
        reviewRefusals.push({ surface: "api", direction, ask: askResult, notify: notifyResult });
      }
      const defaultPolicy = client.setAskPolicy({ target_ref: reviewRef, policy: "default" });
      const allowPolicy = client.setAskPolicy({ target_ref: reviewRef, policy: "deny_peer_asks", allow_from: [rootRef, a.ref] });
      assert.deepEqual(defaultPolicy, { ok: false, reason: "scope_denied" }); assert.deepEqual(allowPolicy, defaultPolicy);
      value(client.setAskPolicy({ target_ref: a.ref, policy: "deny_peer_asks", allow_from: [reviewRef] }));
      assert.deepEqual(await reviewClient.ask({ target_ref: a.ref, request_id: "review-allow-from", question: "must not run" }),
        { ok: false, reason: "scope_denied" });
      assert.deepEqual(await reviewClient.notify({ target_ref: a.ref, request_id: "review-allow-from-note", message: "must not queue" }),
        { ok: false, reason: "scope_denied" });
      value(client.setAskPolicy({ target_ref: a.ref, policy: "default" }));
      assert.equal(counts.side, reviewCalls); assert.equal(queueCount(), reviewQueue);
      assert.equal(value(await reviewClient.inspect(reviewRef)).asks, "denied");
      const reviewDone = terminal(reviewLaunch.task_id); bus.emit(`release:${reviewLabel}`); await reviewDone;
      await bounded(reviewRuntime.waitForIdle(), "ordinary-role independent execution main");
      const controlStarted = event(bus, "latch", row => row.label === reviewLabel);
      // Reusing the exact prompt, role, model and spec does not reuse the
      // consumed owner registration. Only the designated execution was denied.
      const controlLaunch = await engine.manager.start(reviewSpec);
      assert.equal(controlLaunch.kind, "started"); await controlStarted;
      const controlRecord = store.load(controlLaunch.task_id);
      assert(!controlRecord.tool_deny?.includes("__coordination_independent_review__"));
      const controlRow = value(bridge.listOwned()).find((row: any) => row.identity.task_id === controlLaunch.task_id);
      assert(controlRow.collaboration.allowed);
      const controlHandle = engine.manager.getResidentHandle(controlLaunch.task_id);
      const controlRuntime = carrier.runtimeOwners.get(controlHandle).runtime.deref(); sessions.push(controlRuntime);
      const controlClient = value(getCoordinationClient(controlRuntime.extensionRunner.createContext())) as CoordinationClient;
      clients.push(controlClient);
      assert.equal(value(await controlClient.notify({ target_ref: a.ref, request_id: "ordinary-review-label-control",
        message: "ordinary-review-label-control" })).state, "delivered");
      const controlDone = terminal(controlLaunch.task_id); bus.emit(`release:${reviewLabel}`); await controlDone;
      await bounded(controlRuntime.waitForIdle(), "same-prompt ordinary execution control");
      report.independent_review = { task_id: reviewLaunch.task_id, role: reviewRecord.agent_type,
        model: reviewRecord.model, native_tool_deny: reviewRecord.tool_deny,
        owner_registered: true, ordinary_role_control_allowed: true, refusals: reviewRefusals,
        same_spec_unregistered_control: { task_id: controlLaunch.task_id, tool_deny: controlRecord.tool_deny,
          collaboration_allowed: controlRow.collaboration.allowed },
        policy_cannot_reopen: true, allow_from_cannot_reopen: true, provider_calls: 0, queue_additions: 0,
        main_result: reviewRuntime.getLastAssistantText() };
      const sideBefore = counts.side, mainBefore = counts.main;
      const reply = value((await modelCall(root, "coordination_ask", { target_ref: a.ref,
        request_id: "model-busy", question: "NONCE:busy-nonce" })).details);
      assert.equal(reply.reply, "busy-nonce"); assert.equal(counts.side, sideBefore + 1);
      assert.deepEqual(await client.ask({ target_ref: a.ref, request_id: "model-busy", question: "NONCE:busy-nonce" }),
        { ok: true, value: reply });
      assert.equal(counts.side, sideBefore + 1);
      const noteBefore = counts.side;
      const note = value((await modelCall(root, "coordination_notify", { target_ref: a.ref,
        request_id: "model-note", message: "evidence-correction: use observed native outcome, not the prior peer claim" })).details);
      assert.equal(counts.side, noteBefore); assert.equal(note.incorporation, "queued_for_next_turn");
      assert.equal(fingerprint(a.runtime.messages), before.history); assert.equal(fingerprint(a.runtime.model), before.model);
      assert.equal(a.runtime.thinkingLevel, before.thinking);
      const pendingStart = counts.payloads.length;
      const corrected = value(await child.ask({ target_ref: b.ref, request_id: "direct-sibling", question: "NONCE:sibling-nonce" }));
      assert.equal(corrected.reply, "sibling-nonce");
      const correction = value(await client.ask({ target_ref: a.ref, request_id: "followup", question: "NONCE:corrected-nonce" }));
      assert.equal(correction.reply, "corrected-nonce");
      assert(counts.payloads.slice(pendingStart).some((row: Json) => row.pending_peer_context));
      assert.equal(fingerprint(a.runtime.messages), before.history);
      const consumer = value((await modelCall(root, "coordination_inspect", { target_ref: b.ref })).details);
      assert(consumer);
      const consumed = (await modelCall(root, "acceptance_api_consumer", { target_ref: b.ref })).details;
      assert(consumed.passed); assert.equal(value(await client.inspect(b.ref)).asks, "allowed");
      const rootHistoryStart = counts.side;
      const rootNote = value(await child.notify({ target_ref: rootRef, message: "root-next-turn-once", request_id: "root-note" }));
      assert.equal(counts.side, rootHistoryStart); assert.equal(rootNote.incorporation, "queued_for_next_turn");
      const oldCount = root.messages.filter((message: any) => message.customType === queueType).length;
      await bounded(root.prompt("First normal root turn after peer note."), "first natural turn");
      const peerMessages = root.messages.filter((message: any) => message.customType === queueType);
      assert.equal(peerMessages.length, oldCount + 1); assert.equal(rootNote.incorporation, "incorporated");
      await bounded(root.prompt("Second normal root turn."), "second natural turn");
      assert.equal(root.messages.filter((message: any) => message.customType === queueType).length, peerMessages.length);
      const rootLatch = event(bus, "latch", row => row.label === "root"); labels.push("root");
      const rootMain = root.prompt("ACCEPTANCE_HOLD:root"); activePrompts.push(rootMain); await rootLatch;
      const rootHistory = fingerprint(root.messages), busyMain = counts.main;
      const toRoot = value(await child.ask({ target_ref: rootRef, question: "NONCE:direct-root", request_id: "direct-root" }));
      assert.equal(toRoot.reply, "direct-root"); assert.equal(fingerprint(root.messages), rootHistory); assert.equal(counts.main, busyMain);
      processGate = path.join(sandbox, "process-gate"); fs.mkdirSync(processGate);
      process.env.COORDINATION_ACCEPTANCE_GATE = processGate;
      let watch: fs.FSWatcher | undefined;
      // Both subscriptions exist before the trigger. The unchanged 30000 ms bound is armed only by
      // the exact owned --mode rpc spawn, never by --list-models, another child, or execute() return.
      let readyAt: string | undefined;
      const ready = new Promise<void>((resolve, reject) => {
        watch = fs.watch(processGate!, (_kind, file) => {
          if (String(file) === "ready.json") { watch?.close(); readyAt = new Date().toISOString(); resolve(); }
          if (String(file) === "provider-error.json") {
            watch?.close(); reject(new Error(`PROCESS_PROVIDER_ERROR:${path.join(processGate!, "provider-error.json")}`));
          }
        });
        watch.on("error", reject);
      });
      void ready.catch(() => {});
      let readiness: Promise<void> | undefined, readinessTimer: ReturnType<typeof setTimeout> | undefined;
      let readinessArmedAt: string | undefined, ownedRpcSpawn: Json | undefined;
      // Pre-registered before the trigger: the actual spawn signal, not execute()'s return,
      // is the observation this block awaits.
      let resolveOwnedSpawn!: (row: Json) => void;
      const ownedSpawnObserved = new Promise<Json>(resolve => { resolveOwnedSpawn = resolve; });
      const armReadiness = () => {
        if (readiness) return;
        readinessArmedAt = new Date().toISOString();
        readiness = Promise.race([ready, new Promise<never>((_resolve, reject) => {
          readinessTimer = setTimeout(() => reject(new Error("ACCEPTANCE_EVENT_DEADLINE:actual process model tool calls")), 30_000);
        })]);
        void readiness.finally(() => { if (readinessTimer) clearTimeout(readinessTimer); }).catch(() => {});
      };
      processSpawnHook = (row: Json) => {
        if (!row.owner_task_id) return;
        assert(!ownedRpcSpawn, `DUPLICATE_OWNED_RPC_SPAWN:${ownedRpcSpawn?.pid},${row.pid}`);
        ownedRpcSpawn = row; armReadiness(); resolveOwnedSpawn(row);
      };
      let processId: string;
      try {
        const launched = await execute("task", { subagent_type: "acceptance-process", model: "coordination-acceptance/offline",
          execution_mode: "process", prompt: "ACCEPTANCE_HOLD:rpc", description: "Final process registered-tool proof", run_in_background: true });
        processId = launched.details.task_id; assert(processId);
        // Await the pre-registered actual spawn observation; execute()'s return proves nothing.
        const spawnRow = await ownedSpawnObserved;
        assert.equal(spawnRow.owner_task_id, processId, "OWNED_RPC_SPAWN_TASK_MISMATCH");
        assert(Date.parse(readinessArmedAt!) >= Date.parse(spawnRow.observed_at), "RPC_READINESS_ARMED_BEFORE_SPAWN");
        processHandle = engine.manager.getResidentHandle(processId); await readiness!;
        report.rpc_readiness = { owner_task_id: spawnRow.owner_task_id, spawn_pid: spawnRow.pid,
          spawned_at: spawnRow.observed_at, armed_at: readinessArmedAt, ready_at: readyAt, bound_ms: 30_000,
          elapsed_ms: readyAt && readinessArmedAt ? Date.parse(readyAt) - Date.parse(readinessArmedAt) : null };
      } finally {
        watch?.close(); processSpawnHook = undefined;
        if (readinessTimer) clearTimeout(readinessTimer);
      }
      processHandle = engine.manager.getResidentHandle(processId); assert(processHandle, "ACTUAL_PROCESS_HANDLE_NOT_RESIDENT");
      const proof = JSON.parse(fs.readFileSync(path.join(processGate, "ready.json"), "utf8"));
      assert.notEqual(proof.pid, process.pid); assert.equal(processHandle.kind, "rpc");
      const rpcBranch = await bounded<any>(processHandle.getEntries(), "actual default RPC branch");
      assert(rpcBranch.entries.some((entry: any) => entry.id === proof.branch));
      if (processHandle.sessionId) assert.equal(proof.session_id.replace(/^senpi:/, ""), processHandle.sessionId.replace(/^senpi:/, ""));
      const nativeSpawn = spawned.find(row => row.pid === processHandle.pid && row.argv.includes("rpc"));
      assert(nativeSpawn, "NATIVE_DEFAULT_RPC_ARGV_NOT_OBSERVED");
      assert(proof.loaded.some((file: string) => file.replaceAll("\\", "/").endsWith("/question-package/extension.js")));
      for (const name of ["coordination_inspect", "coordination_ask", "coordination_notify"]) {
        assert(proof.model_issued.some((row: Json) => row.name === name));
        assert(proof.tool_events.some((row: Json) => row.tool === name && row.details?.ok), `PROCESS_TOOL_RESULT_MISSING:${name}`);
      }
      const rpcRef = value(bridge.listOwned()).find((row: any) => row.identity.task_id === processId).identity.target_ref;
      const channel = value(await state.questions.get(root.sessionManager).transport());
      const ownInspection = proof.tool_events.find((row: Json) => row.id === "process-model-inspect")
        .details.value.targets.find((row: Json) => row.recorded_state.evidence_ref === `${processId}/epoch/0`);
      const inspectedProcess = value((await execute("coordination_inspect", { target_ref: rpcRef })).details);
      const redactedFacts = value(await channel.invokeTarget(rpcRef, "coordination.question.facts", null));
      report.process_inspection = { self: ownInspection, parent: inspectedProcess,
        native_identity: value(bridge.inspectOwned(rpcRef)).identity,
        authenticated_binding: value(channel.resolveTarget(rpcRef)).binding,
        transport_fields: Object.keys(redactedFacts), facts: redactedFacts, provider_calls: 0 };
      assert.equal(ownInspection.live, true); assert.equal(ownInspection.runtime_reachability.value, true);
      assert.equal(ownInspection.record_consistent, true); assert.notEqual(ownInspection.activity.value, "unknown");
      assert.notEqual(ownInspection.progress.value, "unknown");
      assert.equal(inspectedProcess.live, true); assert.equal(inspectedProcess.runtime_reachability.value, true);
      assert.equal(inspectedProcess.record_consistent, true); assert.equal(inspectedProcess.activity.value, "blocked_tool");
      assert.notEqual(inspectedProcess.progress.value, "unknown");
      assert.equal(value(await client.inspect(rpcRef)).live, true);
      assert.deepEqual(Object.keys(redactedFacts).sort(),
        ["activity", "observed_at", "revision", "runtime_instance_id", "session_id"]);
      const siblingUi = uiEvents.filter(row => row.message.startsWith(`Peer ${processId} -> ${b.id}: `));
      assert.equal(siblingUi.length, 2);
      assert(siblingUi.some(row => row.message.endsWith(": replied")));
      assert(siblingUi.some(row => row.message.endsWith(": delivered")));
      assert.equal(proof.sibling_duplicate_replied, true);
      assert.equal(fingerprint(root.messages), rootHistory); assert.equal(counts.main, busyMain);
      assert(!JSON.stringify([root.messages, root._pendingNextTurnMessages]).includes("process-sibling-nonce"));
      assert(!JSON.stringify([root.messages, root._pendingNextTurnMessages]).includes("process-sibling-note"));
      report.process_sibling_display = { events: siblingUi, ask_events: 1, notify_events: 1,
        duplicate_events: 0, root_history_unchanged: true, root_context_additions: 0, forced_turns: 0 };
      const refuseClaim = async (ref: string, fields: Json) => {
        const invitation = channel.inviteRemote(), foreign = createConnection(invitation.address);
        try {
          await bounded(new Promise<void>((resolve, reject) => {
            foreign.once("connect", resolve); foreign.once("error", reject);
          }), "adversarial claimant connection");
          foreign.write(JSON.stringify({ type: "request", id: randomUUID(), value: { kind: "hello", hello: {
            token: invitation.token, generation: invitation.generation, pid: proof.pid,
            session_id: proof.session_id, branch: proof.branch, runtime_instance_id: proof.runtime_instance_id, ...fields,
          } } }) + "\n");
          return await channel.bindRemote(invitation, ref);
        } finally { foreign.destroy(); }
      };
      const beforeAttack = counts.side;
      const rejectedProcess = await refuseClaim(rpcRef, { pid: process.pid });
      assert.deepEqual(rejectedProcess, { ok: false, reason: "scope_denied" });
      const nativeIdentity = await processHandle[Symbol.for("omo.agent-coordination.rpc-identity.v1")]();
      assert.equal(nativeIdentity.session_id.replace(/^senpi:/, ""), proof.session_id.replace(/^senpi:/, ""));
      const otherSession = await refuseClaim(rpcRef, { session_id: root.sessionId });
      assert.deepEqual(otherSession, { ok: false, reason: "scope_denied" });
      const consumedTicket = bootstrapTickets.get(processId);
      assert(consumedTicket, "NATIVE_BOOTSTRAP_TICKET_NOT_OBSERVED");
      const secondClaimant = await connectRemoteRuntime({ invitation: consumedTicket as any,
        capture: value(state.capture.forRuntime(root)), context: rootCtx });
      assert.deepEqual(secondClaimant, { ok: false, reason: "scope_denied" });
      assert.equal(counts.side, beforeAttack);
      const attackProviderDelta = counts.side - beforeAttack;
      const rpcReply = value(await ask(rpcRef, "root-process", "NONCE:root-process-nonce"));
      assert.equal(rpcReply.reply, "root-process-nonce");
      bus.emit("release:root"); await bounded(rootMain, "busy root original result");
      assert.equal(root.getLastAssistantText(), "acceptance-main-result:root");
      assert.equal(root.messages.filter((message: any) => message.customType === queueType &&
        String(message.content).includes("NONCE:direct-root")).length, 0);
      await bounded(root.prompt("Natural root turn after the busy peer exchange."), "question next-turn consumption");
      const incorporated = root.messages.filter((message: any) => message.customType === queueType &&
        String(message.content).includes("NONCE:direct-root"));
      assert.equal(incorporated.length, 1); assert.equal(toRoot.incorporation, "incorporated");
      assert.equal(incorporated[0].role, "custom");
      assert.equal(incorporated[0].details.generation, state.generation);
      assert.equal(typeof incorporated[0].details.exchange_id, "string");
      const stableRoster = value(client.peers()).revision, stableMessages = root.messages.filter((message: any) => message.customType === queueType).length;
      const stableProjection = counts.roster_observations.at(-1);
      assert.equal(stableProjection.sections, 1);
      await bounded(root.prompt("Another natural root turn with no roster change."), "no repeated peer injection");
      assert.equal(value(client.peers()).revision, stableRoster);
      assert.equal(root.messages.filter((message: any) => message.customType === queueType).length, stableMessages);
      assert.deepEqual(counts.roster_observations.at(-1), stableProjection);
      assert.equal(state.rosterRevision(root).consumed, stableRoster);
      assert(!JSON.stringify(root.messages).includes("<omo-agent-coordination-roster-v1>"));
      // A genuinely completed native task is revived by task_send. One native
      // publication failure leaves an outcome witness for automatic repair.
      const done0 = terminal(a.id); bus.emit("release:a"); await done0; await bounded(a.runtime.waitForIdle(), "epoch0 idle");
      assert.equal(store.load(a.id).status, "completed");
      const start1 = event(bus, "latch", row => row.label === "revived"); labels.push("revived");
      await execute("task_send", { to: a.id, message: "ACCEPTANCE_HOLD:revived" }); await start1;
      const handle = engine.manager.getResidentHandle(a.id), outcome = handle.waitForOutcome();
      const terminal1 = terminal(a.id, 1); let failures = 0;
      fs.renameSync = function(from: any, to: any) {
        if (String(to).endsWith(a.id + ".json") && failures === 0) {
          const record = JSON.parse(fs.readFileSync(from, "utf8"));
          if (record.status === "completed" && record.notification.run_epoch === 1 && record.notification.notified_epoch < 1) {
            failures++; throw Object.assign(new Error("Final acceptance native publication fault"), { code: "ENOSPC" });
          }
        }
        return originalRename(from, to);
      } as typeof fs.renameSync;
      syncBuiltinESMExports();
      bus.emit("release:revived"); assert.equal((await bounded<any>(outcome, "revived outcome")).status, "completed");
      assert.equal((await terminal1).status, "completed"); fs.renameSync = originalRename; syncBuiltinESMExports();
      const repaired = store.load(a.id); assert.equal(failures, 1); assert.equal(repaired.status, "completed");
      assert.equal(repaired.notification.notified_epoch, 1); assert.equal(repaired.final_response, "acceptance-main-result:revived");
      assert.equal(releases.filter(row => row.id === a.id && row.epoch === 1).length, 1);
      assert.equal(notifications.filter(row => row.id === a.id && row.epoch === 1).length, 1);
      const fresh = value(bridge.listOwned()).find((row: any) => row.identity.task_id === a.id);
      const staleProcess = await refuseClaim(a.ref, {});
      assert.deepEqual(staleProcess, { ok: false, reason: "stale_epoch" });
      report.bootstrap_regressions = { positive_default_descendant: true, fixture_bootstrap_calls: proof.fixture_bootstrap_calls,
        non_descendant: rejectedProcess, different_session: otherSession, earlier_epoch: staleProcess,
        second_claimant: secondClaimant, provider_calls_on_refusal: attackProviderDelta };
      assert.equal(value(await client.inspect(fresh.identity.target_ref)).record_consistent, true);
      const output = await execute("task_output", { task_id: a.id, mode: "status" });
      assert(JSON.stringify(output).includes(repaired.final_response));
      const calls = counts.side;
      assert.deepEqual(await ask(fresh.identity.target_ref, "terminal-refusal", "no revival"), { ok: false, reason: "target_settled" });
      assert.equal(counts.side, calls);
      report.repair = { failures, record: { task_id: a.id, status: repaired.status, notification: repaired.notification,
        final_response: repaired.final_response }, output, manual_reconcile_calls: 0, releases, notifications };
      const failuresObserved: Json[] = [];
      for (const control of ["empty", "quota"]) {
        const beforeCount = counts.side;
        const failed = value(await ask(b.ref, `final-${control}`, `NONCE:${control}-nonce CONTROL:${control}`));
        assert.equal(failed.state, "failed"); assert.equal(counts.side, beforeCount + 1);
        failuresObserved.push({ control, receipt: failed, provider_calls: counts.side - beforeCount });
      }
      const raceStarted = event(bus, "side-start", row => row.nonce === "completion-race");
      const raceAborted = event(bus, "side-aborted", row => row.nonce === "completion-race");
      const race = client.ask({ target_ref: b.ref, request_id: "final-completion-race", depth: "deep",
        question: "NONCE:completion-race CONTROL:gate" });
      await raceStarted;
      const bDone = terminal(b.id); bus.emit("release:b"); await bDone; await raceAborted;
      const raced = value(await race); assert.equal(raced.state, "cancelled"); assert.equal(raced.ended_by, "target_settled");
      bus.emit("side-release:completion-race");
      report.failure_paths = { provider_failures: failuresObserved, completion_race: raced,
        no_retry_or_fallback: true, native_main_results_preserved: true };
      report.tools = { inspect, reply, note, model_issued_events: emitted };
      report.api = consumed;
      report.context_consumption = { next_turn_note_once: true, next_turn_question_once: true,
        unchanged_roster_reinjection: 0, pending_peer_context_seen: true,
        automatic_roster_observed_before_discovery: true, stable_roster_projection: stableProjection,
        persistent_roster_history_entries: 0,
        incorporated_peer_message: incorporated[0],
        root_note: rootNote, immediately_changed_main_history: 0, side_answers_are_user_approval: false,
        correction_exchange: { original: reply, notification: note, corrected: correction } };
      report.collaboration_counts = { direct_sibling: corrected, direct_root: toRoot, lead_relay_calls: 0,
        main_steers: 0, main_aborts: 0, forced_turns: 0, notify_provider_calls: 0, duplicate_provider_calls: 1,
        main_calls_before: mainBefore, main_calls_after: counts.main, physical_scripted_side_requests: counts.side };
      const processCalls = counts.side;
      await channel.dispose();
      const disconnected = value((await execute("coordination_inspect", { target_ref: rpcRef })).details);
      assert.equal(disconnected.live, "unknown"); assert.equal(disconnected.record_consistent, "unknown");
      for (const key of ["runtime_reachability", "activity", "progress"]) assert.equal(disconnected[key].value, "unknown");
      assert.equal(value(await client.inspect(rpcRef)).live, "unknown");
      assert.equal(store.load(processId).status, "running"); assert(!processHandle.hasExited());
      assert.equal(counts.side, processCalls);
      report.process_inspection.disconnected = disconnected;
      report.process_inspection.native_main_preserved_on_disconnect = true;
      fs.writeFileSync(path.join(processGate, "release"), "release owned process\n");
      assert.equal((await bounded<any>(processHandle.waitForOutcome(), "original process completion")).status, "completed");
      report.process = { ...proof, manager_pid: processHandle.pid, runtime_pid: proof.pid, native_spawn: nativeSpawn,
        non_descendant_refused: rejectedProcess, provider_calls_on_refusal: attackProviderDelta,
        native_session_identity: nativeIdentity.session_id,
        actual_native_rpc_branch_matches: true, parent_to_process: rpcReply, original_main_completed: true,
        task_output: await execute("task_output", { task_id: processId, mode: "status" }) };
      const beforeDispose = counts.side;
      await state.dispose();
      assert.deepEqual(getCoordinationClient(rootCtx), { ok: false, reason: "coordination_unavailable" });
      assert.deepEqual(await client.inspect(fresh.identity.target_ref), { ok: false, reason: "stale_generation" });
      assert.equal(counts.side, beforeDispose);
      report.failure_paths.mode_disabled = { unavailable: true, stale_client_refused: true, replayed_provider_requests: 0 };
    }
    for (const target of [a, b]) {
      const record = store.load(target.id);
      if (record.status === "running" && !(name === "inspect-api" && target.id === a.id)) {
        const done = terminal(target.id, record.notification.run_epoch); bus.emit(`release:${target.label}`);
        assert.equal((await done).status, "completed");
      }
      await bounded(target.runtime.waitForIdle(), "original worker idle");
    }
    report.counts = { main: counts.main, side: counts.side, payloads: counts.payloads, emitted: counts.emitted };
    report.passed = true;
  } catch (error) {
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    process.exitCode = 1;
  } finally {
    try {
    restoreUi();
    fs.linkSync = originalLink;
    fs.renameSync = originalRename; syncBuiltinESMExports();
    childProcess.spawn = originalSpawn; syncBuiltinESMExports(); report.spawned = spawned;
    for (const label of labels) bus.emit(`release:${label}`);
    bus.emit("side-release:abort-nonce");
    if (processGate && !fs.existsSync(path.join(processGate, "release"))) fs.writeFileSync(path.join(processGate, "release"), "cleanup owned process\n");
    if (processHandle) {
      if (!processHandle.hasExited()) await bounded(processHandle.terminate(), "exact owned process terminate");
      await bounded(processHandle.dispose(), "exact owned process dispose");
    }
    for (const prompt of activePrompts) await bounded(prompt, "cleanup model prompt");
    stopTerminal(); for (const client of clients) client.dispose();
    for (const capture of state?.capture.list() ?? []) if (capture.runtime()) sessions.push(capture.runtime());
    for (const session of [...new Set(sessions)].reverse()) {
      if (session._disposed) continue;
      await bounded(session.waitForIdle(), "cleanup idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" }); session.dispose();
    }
    await state?.dispose();
    if (manifest) report.removal = removeAdapter(manifest);
    const junction = path.join(sandbox, "candidate/node_modules");
    if (fs.existsSync(junction)) { assert(fs.lstatSync(junction).isSymbolicLink()); fs.unlinkSync(junction); }
    bus.removeAllListeners();
    report.cleanup = { sessions_disposed: sessions.every(session => session._disposed), provider_listeners: bus.eventNames().length,
      installed_unchanged: Object.entries(beforeModules).every(([file, hash]) => sha256(file) === hash),
      adapter_restored: manifest ? sha256(manifest.output) === manifest.baseline_sha256 : true };
    } finally {
    if (modelConfig) report.model_config = modelConfig.dispose();
    report.finished_at = new Date().toISOString(); save(artifact, report);
    }
  }
  return report;
}

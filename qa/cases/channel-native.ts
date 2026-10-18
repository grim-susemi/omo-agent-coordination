import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { createConnection } from "node:net";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";
import { connectRemoteRuntime, createPeerChannel, publishCoordinationBroker } from "../../src/peer-channel.ts";
import type { PeerChannel, PeerHandler, RemoteInvitation } from "../../src/peer-channel.ts";
import type { TargetRef } from "../../src/contracts.ts";
import { getCoordinationClient } from "../../src/public-api.ts";
import { product, senpi, bundle, isolatedEnv, sha256 } from "../command.mjs";
import { channelCommand, channelEvidence, channelHashes } from "./channel-receipts.ts";

type Case = "channel-in-process" | "channel-process" | "channel-disconnected" | "channel-api";
type Json = Record<string, any>;
const captureKey = Symbol.for("omo.agent-coordination.capture.v1");
const ownerKey = Symbol.for("coordination.channel.qa.owner");
const ok = <T>(result: { ok: true; value: T } | { ok: false; reason: string }): T => {
  assert(result.ok, JSON.stringify(result)); return result.value;
};
async function bounded<T>(pending: Promise<T>, name: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([pending, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`CHANNEL_QA_DEADLINE: ${name}`)), 25_000);
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

export async function runChannelCase(name: Case): Promise<Json> {
  const id = `${name}-${randomUUID()}`;
  const sandbox = path.join(channelEvidence, "runs", id);
  const packageDir = path.join(sandbox, "channel-package");
  fs.mkdirSync(packageDir, { recursive: true });
  const built = await Bun.build({
    entrypoints: ["src/extension.ts", "qa/cases/channel-fixture.ts", "qa/cases/channel-consumer.ts"]
      .map((file) => path.join(product, file)),
    outdir: packageDir, naming: "[name].js", target: "node", format: "esm",
    external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"],
  });
  assert(built.success, String(built.logs));
  assert.equal(built.outputs.length, 3);
  const apiBuild = await Bun.build({
    entrypoints: [path.join(product, "src/public-api.ts")], outdir: path.join(packageDir, "dist"),
    naming: "public-api.js", target: "node", format: "esm",
  });
  assert(apiBuild.success, String(apiBuild.logs));
  assert.equal(apiBuild.outputs.length, 1);
  fs.writeFileSync(path.join(packageDir, "package.json"), JSON.stringify({
    name: "coordination-channel-owned-candidate", type: "module",
    exports: { "./api": "./dist/public-api.js" },
    pi: { extensions: ["./extension.js", "./channel-fixture.js", "./channel-consumer.js"] },
  }));
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", path.join(packageDir, "node_modules"), "junction");
  const artifact = path.join(channelEvidence, `${id}.json`);
  let run: Awaited<ReturnType<typeof channelCommand>>;
  try {
    run = await channelCommand(["node", fileURLToPath(import.meta.url),
      "--native-case", name, "--sandbox", sandbox, "--result", artifact]);
  } finally {
    const link = path.join(packageDir, "node_modules");
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
  }
  assert.equal(run.exit, 0, `REAL_CHANNEL_CASE_FAILED: ${run.receiptPath}`);
  const result = JSON.parse(fs.readFileSync(artifact, "utf8"));
  assert.equal(result.passed, true);
  const named = name === "channel-process" ? "roundtrip" : name === "channel-api" ? "public-api" : "isolation";
  fs.writeFileSync(path.join(channelEvidence, `${named}.json`),
    JSON.stringify({ ...result, command_receipt: run.receiptPath }, null, 2) + "\n");
  return result;
}

async function nativeCase(name: Case, sandbox: string, artifact: string): Promise<Json> {
  assert(path.resolve(sandbox).startsWith(path.resolve(channelEvidence) + path.sep));
  const env = isolatedEnv(sandbox);
  const oldEnv = { ...process.env }, oldCwd = process.cwd();
  const guard = process.env.NODE_OPTIONS;
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  if (guard) process.env.NODE_OPTIONS = guard;
  const metadata = path.join(sandbox, "sdk-metadata");
  fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = metadata;
  const pathKey = Object.keys(process.env).find((key) => key.toLowerCase() === "path") ?? "PATH";
  process.env[pathKey] = `${path.dirname(process.execPath)};${process.env.SystemRoot}/System32`;
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true }); process.chdir(cwd);
  const packageDir = path.join(sandbox, "channel-package");
  const bus = new EventEmitter();
  const global = globalThis as any;
  global[Symbol.for("coordination.channel.qa.bus")] = bus;
  const sessions: any[] = [], processHandles: any[] = [], released = new Set<string>();
  const mainLabels: string[] = [];
  let channel: PeerChannel | undefined;
  let publication: ReturnType<typeof publishCoordinationBroker> | undefined;
  let manifest: ReturnType<typeof buildAdapter> | undefined;
  let stopTerminal = () => {};
  const report: Json = {
    case: name, sandbox, artifact, start_time: new Date().toISOString(),
    source_sha256: channelHashes(), installed_sha256_before: sha256(bundle),
    provider_fixture: "offline event-gated provider; actual native engine, managed handle and AgentSession",
    repair_enabled: false,
  };
  try {
    manifest = buildAdapter({ output: path.join(sandbox, "candidate/omo-task.js") });
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
    const packages = ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg", packageDir];
    fs.writeFileSync(path.join(env.OMO_CODING_AGENT_DIR!, "settings.json"), JSON.stringify({ packages }));
    fs.writeFileSync(path.join(cwd, ".omo/omo.json"), JSON.stringify({
      agents: Object.fromEntries(["channel-worker", "channel-process-worker", "omo-native-code-reviewer"].map((role) => [role, {
        description: "Isolated channel QA", prompt: "Return the controlled test result.",
        model: "coordination-channel/offline", execution_mode: role === "channel-process-worker" ? "process" : "in-process",
      }])),
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: "in-process",
        process_runner: "child-process", host_shard_prewarm: "off", default_concurrency: 4, global_concurrency: 4 },
    }));
    const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory({ packages }) });
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, [], "REAL_CHANNEL_LOADER_FAILED");
    const auth = AuthStorage.inMemory(), registry = ModelRegistry.inMemory(auth);
    const root = (await sdk.createAgentSession({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      resourceLoader: loader, authStorage: auth, modelRegistry: registry,
      sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    sessions.push(root);
    await root.bindExtensions({ mode: "print" });
    await root.setModel(registry.find("coordination-channel", "offline"));
    const state = global[captureKey];
    assert(state.capture.status.ok);
    const rootCapture = ok<any>(state.capture.forRuntime(root));
    const bridge = state.bridges.get(root.sessionManager);
    assert(bridge, `CHANNEL_NATIVE_BRIDGE_MISSING:${[...state.unsupported]}`);
    const rootCtx = root.extensionRunner.createContext();
    const identityCalls = new Map<string, number>();
    const identity: PeerHandler = (call) => {
      const capture = state.capture.list().find((item: any) => item.runtime_instance_id === call.target.runtime_instance_id);
      assert(capture, "CHANNEL_HANDLER_NOT_TARGET_LOCAL");
      const actual = capture.runtime();
      assert(actual instanceof sdk.AgentSession);
      const key = `${call.caller.target_ref}:${call.request_id}`;
      identityCalls.set(key, (identityCalls.get(key) ?? 0) + 1);
      return { ok: true, value: {
        session_id: actual.sessionId, target_ref: call.target.target_ref,
        caller_ref: call.caller.target_ref, body: call.payload,
      } };
    };
    channel = ok(await createPeerChannel({ context: rootCtx, root: rootCapture,
      capture: state.capture, bridge, roster: state.inspections?.get(root.sessionManager), handlers: {
        identity,
        gate: (call) => new Promise((resolve) => {
          bus.once(`release-peer:${String(call.payload)}`, () => resolve({ ok: true, value: "late answer" }));
          bus.emit(`peer-start:${String(call.payload)}`);
        }),
        remoteProof: (call) => {
          bus.emit("remote-proof", call.payload);
          return { ok: true, value: "proof observed" };
        },
      } }));
    global[ownerKey] = { channel };
    publication = publishCoordinationBroker(channel, { deepEnabled: true, repairEnabled: true });
    const native = await import(pathToFileURL(manifest.output).href);
    const store = native.createTaskRecordStore({ project_dir: cwd, task: { state_dir: path.join(sandbox, "senpi-task") } });
    const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
    const engine = carrier.contexts.get(root.sessionManager).deref();
    const completed = new Map<string, any>();
    stopTerminal = global[Symbol.for("omo.task.terminalObservers")].subscribe(({ record }: any) => {
      if (["completed", "error", "cancelled", "interrupted", "lost"].includes(record.status)) {
        completed.set(`${record.task_id}:${record.notification.run_epoch}`, record);
        bus.emit("native-terminal", record);
      }
    });
    const terminal = (id: string, epoch = 0) => {
      const cached = completed.get(`${id}:${epoch}`);
      return cached ? Promise.resolve(cached) : event(bus, "native-terminal",
        (record) => record.task_id === id && record.notification.run_epoch === epoch);
    };
    const execute = (tool: string, input: any, ctx = rootCtx, signal?: AbortSignal) =>
      root.extensionRunner.getToolDefinition(tool).execute(randomUUID(), input, signal, undefined, ctx);
    const probe = async (target_ref: TargetRef, request_id: string, payload: unknown,
      operation = "identity", ctx = rootCtx, signal?: AbortSignal) =>
      (await execute("channel_probe", { target_ref, request_id, payload, operation }, ctx, signal)).details;
    const launch = async (label: string, mode = "in-process", role = "channel-worker", invitation?: RemoteInvitation) => {
      const provider = mode === "in-process" && label !== "denied"
        ? event(bus, "provider-start", (data) => data.label === label) : undefined;
      mainLabels.push(label);
      const launched = await execute("task", { subagent_type: role, model: "coordination-channel/offline",
        execution_mode: mode, prompt: `CHANNEL_FIXTURE_CHILD:${label} Return the isolated result.`,
        description: `Owned channel ${label}`, run_in_background: true });
      const id = launched.details?.task_id;
      assert(id, JSON.stringify(launched));
      if (provider) await provider;
      const hello = invitation ? await channel!.remoteReady(invitation) : undefined;
      const listed = bridge.listOwned();
      const snapshot = ok<any[]>(listed).find((item) => item.identity.task_id === id);
      if (!snapshot && mode === "process") {
        const actual = engine.manager.getResidentHandle(id);
        if (actual) processHandles.push(actual);
        const record = store.load(id);
        const issue = listed.enumeration?.issues.find((row: any) => row.identity?.task_id === id);
        const fresh = actual ? await bounded<any>(actual.getEntries(), "blocked native RPC evidence") : undefined;
        report.bridge_blocker = {
          point: "EngineBridge.listOwned/inspectOwned before peer grant",
          task_id: id, native_status: record?.status, native_run_epoch: record?.notification?.run_epoch,
          canonical_child_session_id: record?.child_session_id ?? null,
          native_handle_kind: actual?.kind, native_handle_pid: actual?.pid,
          native_handle_session_id: actual?.sessionId ?? null,
          native_handle_alive: actual ? !actual.hasExited() : false,
          announced_pid_matches_native_handle: hello?.pid === actual?.pid,
          announced_branch_matches_fresh_rpc: !!fresh?.entries.some((entry: any) => entry.id === hello?.branch),
          enumeration: listed.enumeration,
          inspect_refusal: issue?.identity ? bridge.inspectOwned(issue.identity.target_ref) : null,
          source: "src/engine-bridge.ts owned(): undefined RPC sessionId normalizes to empty string, but binding session_id is null",
          bypass_attempted: false,
        };
      }
      assert(snapshot, "AUTHORITATIVE_RPC_TARGET_REFUSED");
      channel!.refresh();
      return { id, ref: snapshot.identity.target_ref as TargetRef };
    };
    const oneShotAdmission = async () => {
      const countKey = Symbol.for("coordination.channel.qa.provider-count");
      const before = global[countKey];
      const denied = await execute("task", { subagent_type: "plan-reviewer", model: "coordination-channel/offline",
        prompt: "CHANNEL_FIXTURE_CHILD:denied", description: "Verify native one-shot admission",
        run_in_background: true });
      assert.equal(denied.details.status, "denied");
      assert.equal(denied.details.task_id, "");
      assert.equal(global[countKey], before);
      report.one_shot = { native_admission_denied: true, endpoint_issued: false, provider_calls: 0 };
    };
    const sibling = await launch("sibling");
    const siblingSnapshot = ok<any>(bridge.inspectOwned(sibling.ref));
    const siblingCapture = state.capture.list().find((item: any) =>
      item.runtime()?.sessionId === siblingSnapshot.identity.session_id);
    assert(siblingCapture);
    const siblingRuntime = siblingCapture.runtime();
    sessions.push(siblingRuntime);
    const siblingCtx = siblingRuntime.extensionRunner.createContext();
    if (name === "channel-process" || name === "channel-disconnected") {
      const invitation = channel.inviteRemote();
      const gates = path.join(sandbox, "gates");
      fs.mkdirSync(gates);
      process.env.COORDINATION_CHANNEL_INVITATION = JSON.stringify(invitation);
      process.env.COORDINATION_CHANNEL_GATE = gates;
      process.env.COORDINATION_CHANNEL_SIBLING = sibling.ref;
      const child = await launch("process", "process", "channel-process-worker", invitation);
      const handle = engine.manager.getResidentHandle(child.id);
      assert.equal(handle.kind, "rpc", "NATIVE_PROCESS_POLICY_NOT_APPLIED");
      processHandles.push(handle);
      const proofEvent = event(bus, "remote-proof");
      const binding = await channel.bindRemote(invitation, child.ref);
      assert(binding.ok, JSON.stringify(binding));
      const proof = await proofEvent;
      assert.equal(proof.pid, handle.pid);
      if (name === "channel-process") {
        const large = "deep:\uD83D\uDE80\uD55C\uAE00\u00E9\n".repeat(18_000);
        const response = await probe(child.ref, "large-response", "large");
        assert(response.ok, JSON.stringify(response));
        assert.equal(Buffer.compare(Buffer.from(response.value.body), Buffer.from(large)), 0);
        const repeated = await probe(child.ref, "large-response", "large");
        assert.deepEqual(repeated, response);
        assert.equal(response.value.calls, 1);
        const wrong = { ...invitation, token: "not-the-native-invitation" };
        assert.deepEqual(await channel.bindRemote(wrong, child.ref), { ok: false, reason: "scope_denied" });
        assert.deepEqual(await channel.bindRemote(invitation, child.ref), { ok: false, reason: "scope_denied" });
        const reconnect = await connectRemoteRuntime({ invitation, capture: rootCapture, context: rootCtx });
        assert.deepEqual(reconnect, { ok: false, reason: "scope_denied" });
        const frames = channel.diagnostics().frames;
        assert(frames > 1);
        report.process = { native_pid_matches: true, native_rpc_branch_matches: true,
          root_to_child: true, child_to_root: proof.child_to_root, child_to_sibling: proof.child_to_sibling,
          separate_process: handle.pid !== process.pid, split_frames: true, byte_identical: true,
          reconnect_replayed: false, handler_calls: response.value.calls,
          reply_bytes: Buffer.byteLength(large), reply_sha256: sha256Text(large), received_chunk_frames: frames };
      } else {
        const before = store.load(child.id);
        const beforeHash = sha256Text(JSON.stringify(before));
        assert.equal(before.status, "running");
        const disconnected = await probe(child.ref, "disconnect", null, "disconnect");
        assert.deepEqual(disconnected, { ok: false, reason: "host_failure", reachability: "unreachable" });
        const unreachable = await probe(child.ref, "after-disconnect", null);
        assert.deepEqual(unreachable, disconnected);
        const after = store.load(child.id);
        assert.equal(after.status, "running");
        assert.equal(sha256Text(JSON.stringify(after)), beforeHash);
        assert.equal(handle.hasExited(), false);
        report.disconnected = { result: unreachable, native_status_before: before.status,
          native_status_after: after.status, native_handle_alive: true, native_record_unchanged: true };
      }
      const ended = terminal(child.id);
      fs.writeFileSync(path.join(gates, `release-${handle.pid}`), "release owned offline provider\n");
      const result = await ended;
      assert.equal(result.status, "completed");
      if (report.disconnected) report.disconnected.worker_completed_normally = true;
      report.native_process = { task_id: child.id, pid: handle.pid, epoch: result.notification.run_epoch,
        child_session_id: result.child_session_id ?? null, status: result.status };
    } else {
      const child = await launch("local");
      const childSnapshot = ok<any>(bridge.inspectOwned(child.ref));
      const childCapture = state.capture.list().find((item: any) =>
        item.runtime()?.sessionId === childSnapshot.identity.session_id);
      const childRuntime = childCapture.runtime();
      sessions.push(childRuntime);
      const childCtx = childRuntime.extensionRunner.createContext();
      assert.equal(ok<any>(state.capture.bindOwned(bridge, child.ref, childCapture)).session_id, childRuntime.sessionId);
      if (name === "channel-api") {
        const consumer = (await execute("channel_api_probe", { target_ref: child.ref })).details;
        const first = ok(getCoordinationClient(rootCtx));
        const second = ok(getCoordinationClient(rootCtx));
        const changes: number[] = [];
        first.onPeersChanged((roster) => { changes.push(roster.revision); });
        first.peers(); second.peers();
        assert.equal(changes.length, 0);
        await oneShotAdmission();
        const denied = await launch("denied", "in-process", "omo-native-code-reviewer");
        first.peers();
        assert(changes.length > 0);
        assert.equal(new Set(changes).size, changes.length);
        assert(changes.every((value, index) => index === 0 || value > changes[index - 1]!));
        const stable = changes.length;
        first.peers(); first.peers();
        assert.equal(changes.length, stable);
        assert.deepEqual(await first.ask({ target_ref: denied.ref, request_id: "denied", question: "no call" }),
          { ok: false, reason: "scope_denied" });
        first.dispose();
        assert(second.peers().ok);
        assert.deepEqual(publication.broker.getClient({ sessionManager: { getSessionId: () => root.sessionId } }),
          { ok: false, reason: "scope_denied" });
        const brokerKeys = Object.keys(publication.broker).sort();
        const features = [...publication.broker.features];
        const frozen = Object.isFrozen(publication.broker.features);
        const key = Symbol.for("omo.agent-coordination.api.v1");
        const foreignBroker = { unrelated: true };
        global[key] = foreignBroker;
        publication.dispose();
        assert.equal(global[key], foreignBroker);
        assert.deepEqual(second.peers(), { ok: false, reason: "stale_generation" });
        delete global[key];
        report.api = { broker_keys: brokerKeys, client_ports: consumer.ports, version: 1, features,
          features_frozen: frozen, ...consumer,
          foreign_context_refused: true, one_shot_refused: true, stale_client_refused: true,
          roster_changes_only: true, roster_revision_events: changes,
          dispose_preserves_other_client: true, foreign_broker_preserved: true };
        await terminal(denied.id);
      } else {
        const payload = { nonce: randomUUID(), text: "local identity" };
        const result = await probe(child.ref, "duplicate", payload);
        assert.equal(ok<any>(result).session_id, childRuntime.sessionId);
        assert.deepEqual(await probe(child.ref, "duplicate", payload), result);
        assert.deepEqual(await probe(child.ref, "duplicate", { text: payload.text, nonce: payload.nonce }), result);
        assert.equal(identityCalls.get(`${channel.root_ref}:duplicate`), 1);
        assert.deepEqual(await probe(child.ref, "duplicate", { changed: true }), { ok: false, reason: "conflict" });
        assert.deepEqual(await probe(child.ref, "oversize", "\uD55C".repeat(65_536)), { ok: false, reason: "invalid_argument" });
        assert.deepEqual(await probe("coordination:unknown" as TargetRef, "unknown", null), { ok: false, reason: "invalid_target" });
        assert.deepEqual(await probe(child.ref, "foreign", null, "identity",
          { ...rootCtx, sessionManager: { getSessionId: () => root.sessionId } }), { ok: false, reason: "scope_denied" });
        const invitation = channel.inviteRemote();
        const raw = createConnection(invitation.address);
        const closed = new Promise<void>((resolve) => { raw.once("close", () => resolve()); });
        raw.on("error", () => {});
        await bounded(new Promise<void>((resolve) => { raw.once("connect", resolve); }), "malformed client connect");
        raw.write(Buffer.alloc(65_537, 120));
        await bounded(closed, "oversize wire rejected");
        const forged = await connectRemoteRuntime({ invitation: { ...invitation, generation: 999 },
          capture: rootCapture, context: rootCtx });
        assert.deepEqual(forged, { ok: false, reason: "scope_denied" });
        assert.equal(ok<any>(await probe(channel.root_ref, "child-root", null, "identity", childCtx)).session_id, root.sessionId);
        assert.equal(ok<any>(await probe(sibling.ref, "child-sibling", null, "identity", childCtx)).session_id, siblingRuntime.sessionId);
        await oneShotAdmission();
        const denied = await launch("denied", "in-process", "omo-native-code-reviewer");
        assert.deepEqual(await probe(denied.ref, "one-shot", null), { ok: false, reason: "scope_denied" });
        await terminal(denied.id);
        const abort = new AbortController();
        const abortStarted = event(bus, "peer-start:abort");
        const aborted = probe(child.ref, "aborted", "abort", "gate", rootCtx, abort.signal);
        await abortStarted; abort.abort();
        assert.deepEqual(await aborted, { ok: false, reason: "asker_aborted" });
        bus.emit("release-peer:abort");
        const lateStarted = event(bus, "peer-start:late");
        const late = probe(child.ref, "late", "late", "gate");
        await lateStarted;
        const firstDone = terminal(child.id);
        bus.emit("release-main:local"); released.add("local");
        assert.equal((await firstDone).status, "completed");
        assert.deepEqual(await probe(child.ref, "duplicate", payload), result);
        const nextProvider = event(bus, "provider-start", (data) => data.label === "local");
        await execute("task_send", { to: child.id, message: "CHANNEL_FIXTURE_CHILD:local native next epoch" });
        await nextProvider;
        assert.equal(store.load(child.id).notification.run_epoch, 1);
        bus.emit("release-peer:late");
        assert.deepEqual(await late, { ok: false, reason: "stale_epoch" });
        assert.deepEqual(await probe(child.ref, "stale", null), { ok: false, reason: "stale_epoch" });
        const secondDone = terminal(child.id, 1);
        bus.emit("release-main:local");
        assert.equal((await secondDone).status, "completed");
        report.local = { native_runtime_same_object: true,
          root_has_no_task_id: !("task_id" in ok<any>(channel.bindContext(rootCtx))),
          root_to_child: true, child_to_root: true, child_to_sibling: true,
          duplicate_handler_calls: 1, conflicting_payload_refused: true,
          oversize_refused: true, unknown_refused: true, foreign_context_refused: true,
          one_shot_refused: true, stale_epoch_refused: true, late_reply_discarded: true,
          aborted_request_refused: true, malformed_wire_refused: true, wrong_generation_refused: true };
      }
    }
    const siblingDone = terminal(sibling.id);
    bus.emit("release-main:sibling"); released.add("sibling");
    assert.equal((await siblingDone).status, "completed");
    if (name === "channel-api") {
      const localId = ok<any[]>(bridge.listOwned()).find((item) =>
        item.status === "running" && item.identity.session_id !== siblingRuntime.sessionId)?.identity.task_id;
      if (localId && store.load(localId).status === "running") {
        const done = terminal(localId);
        bus.emit("release-main:local"); released.add("local"); await done;
      }
    }
    await bounded(root.waitForIdle(), "native completion notification drain");
    report.registered_tools = ["task", "task_send", "channel_probe", "channel_api_probe"];
    report.loader_paths = loader.getExtensions().extensions.map((item: any) => item.path);
    report.entry_sha256 = sha256(path.join(packageDir, "extension.js"));
    report.api_sha256 = sha256(path.join(packageDir, "dist/public-api.js"));
    report.passed = true;
  } catch (error) {
    report.passed = false;
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    throw error;
  } finally {
    for (const label of mainLabels) bus.emit(`release-main:${label}`);
    bus.emit("release-peer:abort"); bus.emit("release-peer:late");
    for (const capture of global[captureKey]?.capture.list() ?? []) {
      const runtime = capture.runtime();
      if (runtime) sessions.push(runtime);
    }
    publication?.dispose();
    stopTerminal();
    for (const handle of processHandles) {
      if (!handle.hasExited()) await bounded(handle.terminate(), "original owned process termination");
      await bounded(handle.dispose(), "original owned process disposal");
    }
    for (const session of [...new Set(sessions)].reverse()) {
      if (session._disposed) continue;
      await bounded(session.waitForIdle(), "owned cleanup idle");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" }); session.dispose();
    }
    await channel?.dispose();
    global[captureKey]?.dispose();
    if (manifest && sha256(manifest.output) === manifest.candidate_sha256) report.removal = removeAdapter(manifest);
    for (const directory of [packageDir, path.join(sandbox, "candidate")]) {
      const link = path.join(directory, "node_modules");
      if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
    }
    bus.removeAllListeners();
    report.cleanup = { sessions_disposed: sessions.every((session) => session._disposed),
      ...(channel?.diagnostics() ?? { listeners: 0, endpoints: 0, pending_requests: 0 }),
      process_handles_exited: processHandles.every((handle) => handle.hasExited()),
      dependency_junctions: 0, installed_unchanged: sha256(bundle) === report.installed_sha256_before,
      module_subscriptions: global[Symbol.for("omo.agent-coordination.native.v1")]?.moduleListeners.size ?? 0,
      provider_gates: bus.eventNames().length };
    assert.equal(report.cleanup.installed_unchanged, true);
    report.end_time = new Date().toISOString();
    fs.writeFileSync(artifact, JSON.stringify(report, null, 2) + "\n");
    delete global[ownerKey];
    process.chdir(oldCwd);
    for (const key of Object.keys(process.env)) delete process.env[key];
    Object.assign(process.env, oldEnv);
  }
  return report;
}
function sha256Text(text: string) { return createHash("sha256").update(text).digest("hex"); }

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const name = process.argv[process.argv.indexOf("--native-case") + 1] as Case;
  const sandbox = process.argv[process.argv.indexOf("--sandbox") + 1]!;
  const artifact = process.argv[process.argv.indexOf("--result") + 1]!;
  const report = await nativeCase(name, sandbox, artifact);
  console.log(JSON.stringify({ case: name, passed: report.passed, artifact }));
}

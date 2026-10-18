import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { syncBuiltinESMExports } from "node:module";
import { randomUUID } from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { attempt, product, bundle, senpi, isolatedEnv, sha256 } from "../command.mjs";
import { buildAdapter, removeAdapter } from "../../scripts/build-adapter.mjs";
import { inspectCommand, inspectEvidence, inspectHashes } from "./inspect-receipts.ts";
import type { CaptureExtensionState } from "../../src/extension.ts";
import type { InspectionTree } from "../../src/task-truth.ts";
import type { TargetInspection, TargetRef } from "../../src/contracts.ts";
import { connectEngineBridge } from "../../src/engine-bridge.ts";
import type { OwnedSnapshot } from "../../src/engine-bridge.ts";

type Case = "inspect-owned-tree" | "inspect-partial" | "inspect-rpc";
type Json = Record<string, any>;
async function bounded<T>(promise: Promise<T>, name: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`INSPECT_EVENT_DEADLINE: ${name}`)), 30_000);
    })]);
  } finally { clearTimeout(timer!); }
}
export async function runInspectCase(name: Case): Promise<Json> {
  const sandbox = path.join(inspectEvidence, "runs", `${name}-${randomUUID()}`);
  const directory = path.join(sandbox, "package");
  fs.mkdirSync(directory, { recursive: true });
  for (const [file, output] of [["src/extension.ts", "extension.js"],
    ["qa/cases/inspect-fixture.ts", "fixture.js"]]) {
    const build = await Bun.build({ entrypoints: [path.join(product, file!)], outdir: directory,
      naming: output, target: "node", format: "esm",
      external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"] });
    assert(build.success, String(build.logs));
    assert.equal(build.outputs.length, 1);
  }
  fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({
    name: "coordination-inspect-owned-candidate", type: "module",
    pi: { extensions: ["./extension.js", "./fixture.js"] },
  }));
  const artifact = path.join(inspectEvidence, `${name}-${randomUUID()}.json`);
  const link = path.join(directory, "node_modules");
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", link, "junction");
  let receipt: Awaited<ReturnType<typeof inspectCommand>>;
  try {
    receipt = await inspectCommand(["node", fileURLToPath(import.meta.url),
      "--native-case", name, "--sandbox", sandbox, "--result", artifact]);
  } finally {
    if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
  }
  assert.equal(receipt.exit, 0, `REAL_INSPECTION_FAILED: ${receipt.receiptPath}`);
  const result = JSON.parse(fs.readFileSync(artifact, "utf8"));
  assert.equal(result.passed, true);
  fs.writeFileSync(path.join(inspectEvidence, name === "inspect-partial" ? "partial.json" :
    name === "inspect-rpc" ? "rpc.json" : "owned-tree.json"),
    JSON.stringify({ ...result, command_receipt: receipt.receiptPath }, null, 2) + "\n");
  return result;
}

async function nativeCase(name: Case, sandbox: string, artifact: string): Promise<Json> {
  assert(path.resolve(sandbox).startsWith(path.resolve(inspectEvidence) + path.sep));
  const previousEnv = { ...process.env };
  const previousCwd = process.cwd();
  const env = isolatedEnv(sandbox);
  const guard = process.env.NODE_OPTIONS;
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, env);
  if (guard) process.env.NODE_OPTIONS = guard;
  const metadata = path.join(sandbox, "sdk-package-metadata");
  fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) process.env[key] = metadata;
  const cwd = path.join(sandbox, "work");
  fs.mkdirSync(path.join(cwd, ".omo"), { recursive: true });
  process.chdir(cwd);
  const bus = Object.assign(new EventEmitter(), { calls: 0 });
  const global = globalThis as unknown as Record<symbol, any>;
  global[Symbol.for("coordination.inspect.qa.bus")] = bus;
  const report: Json = { case: name, sandbox, artifact, started_at: new Date().toISOString(),
    source_sha256: inspectHashes(), module_sha256_before: { [bundle]: sha256(bundle),
      [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js") },
    fixture_identity: "actual DefaultResourceLoader + AgentSession + registered native task + coordination_inspect",
    repair_enabled: false };
  let state: CaptureExtensionState | undefined;
  let manifest: ReturnType<typeof buildAdapter> | undefined;
  let sdk: any;
  let root: any;
  let originalBind: any;
  let originalDispose: any;
  let restoreRead = () => {};
  let processHandle: any;
  let processTaskId: string | undefined;
  let stopRpcReady = () => {};
  const gates = path.join(sandbox, "rpc-gates");
  const sessions: any[] = [];
  const pendingStarts = new Set<() => void>();
  const labels = ["INSPECT_CHILD_A", "INSPECT_CHILD_B", "INSPECT_CHILD_C", "INSPECT_CHILD_D", "INSPECT_CHILD_E"];
  try {
    manifest = buildAdapter({ output: path.join(sandbox, "candidate/omo-task.js") });
    process.env.COORDINATION_CANDIDATE_BUNDLE = manifest.output;
    const directory = path.join(sandbox, "package");
    const packages = ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg", directory];
    if (name === "inspect-rpc") {
      fs.mkdirSync(gates);
      process.env.COORDINATION_INSPECT_GATES = gates;
      const pathKey = Object.keys(process.env).find((key) => key.toLowerCase() === "path") ?? "PATH";
      process.env[pathKey] = `${path.dirname(process.execPath)};${process.env.SystemRoot}/System32`;
      fs.writeFileSync(path.join(env.OMO_CODING_AGENT_DIR!, "settings.json"), JSON.stringify({ packages }));
    }
    fs.writeFileSync(path.join(cwd, ".omo/omo.json"), JSON.stringify({
      agents: {
        oracle: { description: "Isolated readonly oracle fixture", prompt: "Return the native result.",
          model: "coordination-inspect/offline", tools: { edit: false, write: false, bash: false } },
        "inspect-worker": { description: "Owned inspection worker", prompt: "Return the native result.",
          model: "coordination-inspect/offline" },
        "inspect-denied": { description: "Explicit collaboration denial", prompt: "Return the native result.",
          model: "coordination-inspect/offline",
          tools: { coordination_ask: false, coordination_notify: false } },
        "inspect-process": { description: "Actual native RPC ownership regression",
          prompt: "Return the native result.", model: "coordination-inspect/offline", execution_mode: "process" },
      },
      task: { state_dir: path.join(sandbox, "senpi-task"), default_execution_mode: "in-process",
        default_concurrency: 8, global_concurrency: 8, max_depth: 2, host_shard_prewarm: "off",
        process_runner: "child-process" },
    }));
    sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
    originalBind = sdk.AgentSession.prototype.bindExtensions;
    originalDispose = sdk.AgentSession.prototype.dispose;
    const loader = new sdk.DefaultResourceLoader({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      settingsManager: sdk.SettingsManager.inMemory(name === "inspect-rpc" ? { packages } : {
        packages: ["C:/Users/USER/.omo-stable-500/home/.omo/agent/task-no-fallback-pkg"],
        extensions: [path.join(directory, "extension.js"), path.join(directory, "fixture.js")],
      }) });
    await loader.reload();
    assert.deepEqual(loader.getExtensions().errors, [], "REAL_INSPECT_LOADER_FAILED");
    const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const auth = AuthStorage.inMemory();
    const registry = ModelRegistry.inMemory(auth);
    root = (await sdk.createAgentSession({ cwd, agentDir: env.OMO_CODING_AGENT_DIR,
      resourceLoader: loader, authStorage: auth, modelRegistry: registry,
      sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    sessions.push(root);
    await root.bindExtensions({ mode: "print" });
    await root.setModel(registry.find("coordination-inspect", "offline"));
    state = global[Symbol.for("omo.agent-coordination.capture.v1")] as CaptureExtensionState;
    const truth = state.inspections.get(root.sessionManager);
    const bridge = state.bridges.get(root.sessionManager);
    assert(truth && bridge, `REAL_BOUND_INSPECTOR_MISSING: ${[...state.unsupported]}`);
    const tool = root.extensionRunner.getToolDefinition("coordination_inspect");
    assert(tool, "COORDINATION_INSPECT_NOT_REGISTERED");
    const inspect = async (target_ref?: TargetRef, context = root.extensionRunner.createContext()) => {
      const result = await tool.execute(randomUUID(), target_ref ? { target_ref } : {}, undefined, undefined, context);
      return result.details;
    };
    const started = (label: string) => {
      let listener: (event: { label: string }) => void;
      let cancel: () => void;
      const pending = new Promise<void>((resolve, reject) => {
        listener = (event) => { if (event.label === label) resolve(); };
        cancel = () => {
          bus.off("provider-start", listener);
          reject(new Error("INSPECT_START_CANCELLED"));
        };
        pendingStarts.add(cancel);
        bus.on("provider-start", listener);
      });
      return bounded(pending, label).finally(() => {
        pendingStarts.delete(cancel!);
        bus.off("provider-start", listener!);
      });
    };
    const spawnTask = async (parent: any, label: string, role: string) => {
      const provider = started(label);
      void provider.catch(() => {});
      const task = parent.extensionRunner.getToolDefinition("task");
      assert(task, "ACTUAL_REGISTERED_TASK_MISSING");
      const result = await task.execute(randomUUID(), { subagent_type: role,
        model: "coordination-inspect/offline", prompt: `INSPECT_NATIVE_GATE:${label} Return the isolated native result.`,
        description: label, run_in_background: true }, undefined, undefined, parent.extensionRunner.createContext());
      assert(result.details.task_id, JSON.stringify(result));
      await provider;
      return result.details.task_id as string;
    };
    if (name === "inspect-rpc") {
      const ready = bounded(new Promise<void>((resolve) => {
        const watcher = fs.watch(gates, (_event, file) => {
          if (String(file) === "rpc-start.json" && fs.existsSync(path.join(gates, "rpc-start.json"))) {
            watcher.close();
            resolve();
          }
        });
        stopRpcReady = () => watcher.close();
      }), "actual RPC provider start");
      void ready.catch(() => {});
      const launched = await root.extensionRunner.getToolDefinition("task").execute(randomUUID(), {
        subagent_type: "inspect-process", model: "coordination-inspect/offline",
        prompt: "INSPECT_NATIVE_GATE:INSPECT_CHILD_F Return the isolated native RPC result.",
        description: "Session-less native RPC bridge proof", run_in_background: true,
      }, undefined, undefined, root.extensionRunner.createContext());
      processTaskId = launched.details.task_id;
      assert(processTaskId, JSON.stringify(launched));
      await ready;
      const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
      const engine = carrier.contexts.get(root.sessionManager).deref();
      processHandle = engine.manager.getResidentHandle(processTaskId);
      assert(processHandle && processHandle.kind === "rpc", "EXACT_NATIVE_RPC_HANDLE_MISSING");
      const local = JSON.parse(fs.readFileSync(path.join(gates, "rpc-start.json"), "utf8"));
      assert.equal(local.pid, processHandle.pid);
      assert.notEqual(local.pid, process.pid);
      const remote: any = await bounded(processHandle.getEntries(), "native live RPC branch");
      assert(remote.entries.some((entry: any) => entry.id === local.branch));
      const store = carrier.links.get(engine).store;
      const record = store.load(processTaskId);
      assert(record && record.notification.run_epoch === 0);
      const recordFile = path.join(sandbox, "senpi-task", "tasks", `${processTaskId}.json`);
      const hashesBefore = sha256(recordFile);
      const handleDescriptor = Object.getOwnPropertyDescriptor(processHandle, "sessionId");
      const recordDescriptor = Object.getOwnPropertyDescriptor(record, "child_session_id");
      let handleSession: string | undefined;
      let recordSession: string | undefined;
      let handleReads = 0;
      let recordReads = 0;
      // Control only optional metadata on the original native objects. The
      // provider gate keeps their task/epoch/handle stable; handshake timing
      // cannot choose which case runs. No canonical file is rewritten.
      Object.defineProperty(processHandle, "sessionId", { configurable: true,
        get() { handleReads++; return handleSession; } });
      Object.defineProperty(record, "child_session_id", { configurable: true,
        get() { recordReads++; return recordSession; } });
      let ref: TargetRef;
      const matrix: Json[] = [];
      try {
        const listed = bridge.listOwned();
        assert(listed.ok);
        const row = listed.value.find((item) => item.identity.task_id === processTaskId);
        assert(row, `SESSIONLESS_NATIVE_RPC_OMITTED: ${JSON.stringify(listed)}`);
        assert.equal(row.identity.session_id, null);
        assert.equal(listed.enumeration?.coverage, "complete");
        ref = row.identity.target_ref;
        report.rpc = { task_id: processTaskId, listed: true, handle_kind: processHandle.kind,
          native_pid: local.pid, actual_child_session_id: local.session_id,
          native_pid_matches: true, native_rpc_branch_matches: true, metadata_controlled: true, matrix };
        const cases: { name: string; handle?: string; record?: string; accepted: boolean }[] = [
          { name: "both_absent", accepted: true },
          { name: "handle_only", handle: local.session_id, accepted: true },
          { name: "absent_after_learning", accepted: true },
          { name: "record_only", record: `senpi:${local.session_id}`, accepted: true },
          { name: "both_equal", handle: local.session_id, record: `senpi:${local.session_id}`, accepted: true },
          { name: "both_different", handle: local.session_id, record: "conflicting-native-session", accepted: false },
          { name: "changed_handle_only", handle: "conflicting-native-session", accepted: false },
          { name: "changed_record_only", record: "conflicting-native-session", accepted: false },
          { name: "restored_equal", handle: `senpi:${local.session_id}`, record: local.session_id, accepted: true },
        ];
        let learned = false;
        for (const entry of cases) {
          handleSession = entry.handle;
          recordSession = entry.record;
          const readsBefore = [handleReads, recordReads];
          const direct = bridge.inspectOwned(ref);
          assert.equal(direct.ok, entry.accepted, `RPC_METADATA_${entry.name}: ${JSON.stringify(direct)}`);
          assert(handleReads > readsBefore[0]! && recordReads > readsBefore[1]!,
            `RPC_METADATA_STATE_NOT_READ: ${entry.name}`);
          assert.equal(engine.manager.getResidentHandle(processTaskId), processHandle);
          assert.equal(store.load(processTaskId), record, "NATIVE_RECORD_OBJECT_CHANGED");
          const current = bridge.listOwned();
          assert(current.ok);
          const currentRow: OwnedSnapshot | undefined = current.value.find((item) => item.identity.task_id === processTaskId);
          if (entry.accepted) {
            learned ||= entry.handle !== undefined || entry.record !== undefined;
            assert(direct.ok && currentRow);
            assert.equal(currentRow.identity.target_ref, ref);
            assert.equal(currentRow.identity.handle_id, row.identity.handle_id);
            assert.equal(currentRow.identity.run_epoch, row.identity.run_epoch);
            assert.equal(direct.value.identity.session_id, learned ? local.session_id : null);
            assert.equal(current.enumeration?.coverage, "complete");
            const projected = await inspect(ref);
            assert(projected.ok && projected.value.status === "running");
            const tree = await inspect();
            assert(tree.ok && tree.value.coverage === "complete");
            assert.equal(tree.value.owned, 2);
            assert.equal(tree.value.visited, 2);
            assert.equal(tree.value.unreadable, 0);
            assert(tree.value.peers.some((peer: any) => peer.target_ref === ref));
            if (entry.name === "handle_only") report.rpc.counterexample_rpc = {
              passed: true, exact_original_handle: true, canonical_child_session_id: null,
              populated_handle_session_id: handleSession, actual_child_session_id: local.session_id,
              target_ref_unchanged: true, handle_id_unchanged: true, run_epoch_unchanged: true,
              direct, registered_inspection: tree,
            };
          } else {
            assert.deepEqual(direct, { ok: false, reason: "runtime_mismatch" });
            assert.equal(currentRow, undefined);
            assert(current.enumeration?.issues.some((issue) =>
              issue.identity?.target_ref === ref && issue.reason === "runtime_mismatch"));
          }
          // Fresh bindings cover record-first learning independently of the
          // stable ref above, as well as initial two-present conflicts.
          if (["both_absent", "handle_only", "record_only", "both_equal", "both_different"].includes(entry.name)) {
            const fresh = connectEngineBridge({ context: root.extensionRunner.createContext(),
              generation: state.generation, loaderNamespace: sdk });
            assert(fresh.ok);
            try {
              const initial = fresh.value.listOwned();
              assert(initial.ok);
              const initialRow = initial.value.find((item) => item.identity.task_id === processTaskId);
              assert.equal(!!initialRow, entry.accepted, `RPC_INITIAL_METADATA_${entry.name}`);
              if (initialRow) assert.equal(initialRow.identity.session_id,
                entry.name === "both_absent" ? null : local.session_id);
              else assert(initial.enumeration?.issues.some((issue) => issue.reason === "runtime_mismatch"));
            } finally { fresh.value.dispose(); }
          }
          assert.equal(sha256(recordFile), hashesBefore, `RPC_METADATA_RECORD_WRITE: ${entry.name}`);
          matrix.push({ name: entry.name, handle_session_id: entry.handle ?? null,
            canonical_child_session_id: entry.record ?? null, accepted: entry.accepted,
            observed: true, record_unchanged: true });
        }
        report.rpc = { ...report.rpc, metadata_states_verified: matrix.length,
          conflicting_session_refused: true, record_unchanged_on_refusal: true,
          learned_session_stable: true, optional_absence_not_conflict: true };
      } finally {
        if (handleDescriptor) Object.defineProperty(processHandle, "sessionId", handleDescriptor);
        else delete processHandle.sessionId;
        if (recordDescriptor) Object.defineProperty(record, "child_session_id", recordDescriptor);
        else delete record.child_session_id;
      }
      assert.equal(sha256(recordFile), hashesBefore);
      assert(bridge.inspectOwned(ref!).ok);
      const projection = await inspect(ref!);
      assert(projection.ok);
      assert.equal(projection.value.status, "running");
      assert.equal(projection.value.work_outcome.value, "unknown");
      const terminal = bounded<{ status: string }>(engine.manager.waitFor(processTaskId), "actual RPC native terminal");
      fs.writeFileSync(path.join(gates, "release-rpc"), "release owned RPC provider\n");
      const completed = await terminal;
      assert.equal(completed.status, "completed");
      await bounded(root.waitForIdle(), "RPC owner notification drain");
      report.rpc = { ...report.rpc, passed: true, owned: true,
        session_id: local.session_id, projection: projection.value, native_completed: true };
      report.passed = true;
      return report;
    }
    const a = await spawnTask(root, labels[0]!, "inspect-worker");
    const first = await inspect();
    assert(first.ok, JSON.stringify(first));
    const snapshotA = bridge.listOwned();
    assert(snapshotA.ok);
    const identityA = snapshotA.value.find((item) => item.identity.task_id === a)!.identity;
    const actualA = state.capture.list().find((capture) => capture.runtime()?.sessionId === identityA.session_id)?.runtime();
    assert(actualA, "REAL_CHILD_RUNTIME_NOT_CAPTURED");
    const carrier = global[Symbol.for("omo.agent-coordination.native.v1")];
    const engine = carrier.contexts.get(root.sessionManager).deref();
    const b = await spawnTask(root, labels[1]!, "explore");
    // This pinned fixture suppresses native extension definitions in children.
    // Use the same actual owner manager to admit a prefixed descendant; the
    // inspector itself still runs exclusively through its registered SDK tool.
    const descendantProvider = started(labels[2]!);
    void descendantProvider.catch(() => {});
    const descendant = await engine.manager.start({
      parent_session_id: actualA.sessionId, root_session_id: `senpi:${root.sessionId}`,
      depth: 2, subagent_type: "librarian", model: "coordination-inspect/offline",
      prompt: `INSPECT_NATIVE_GATE:${labels[2]} Return the isolated native result.`,
      description: labels[2], run_in_background: true,
    });
    assert.equal(descendant.kind, "started", JSON.stringify(descendant));
    await descendantProvider;
    const c = descendant.task_id as string;
    const d = await spawnTask(root, labels[3]!, "oracle");
    const e = await spawnTask(root, labels[4]!, "inspect-denied");
    const ids = [a, b, c, d, e];
    const native = await import(pathToFileURL(manifest.output).href);
    const store = native.createTaskRecordStore({ project_dir: cwd,
      task: { state_dir: path.join(sandbox, "senpi-task") } });
    assert(path.resolve(store.stateDir).startsWith(path.resolve(sandbox) + path.sep));
    const recordFiles = ids.map((id) => path.join(store.stateDir, "tasks", `${id}.json`));
    const before = recordFiles.map(sha256);
    const callsBefore = bus.calls;
    const result = await inspect();
    assert(result.ok, JSON.stringify(result));
    const tree = result.value as InspectionTree;
    assert.equal(tree.coverage, "complete");
    assert.equal(tree.owned, 6);
    assert.equal(tree.visited, 6);
    assert.equal(tree.unreadable, 0);
    assert.equal(tree.targets.length, 6);
    assert.equal(tree.peers.length, 6);
    const raw = ids.map((id) => store.load(id));
    assert(raw.some((record) => record.root_session_id.startsWith("senpi:") ||
      record.parent_session_id.startsWith("senpi:")), "PREFIXED_NATIVE_ID_NOT_EXERCISED");
    assert.equal(raw[2].parent_session_id.replace(/^senpi:/, ""), actualA.sessionId);
    assert.equal(new Set(tree.targets.map((target) => target.target_ref)).size, 6);
    assert(tree.targets.every((target) => typeof target.record_consistent === "boolean" ||
      target.record_consistent === "unknown"));
    const listed = bridge.listOwned();
    assert(listed.ok);
    const refFor = (id: string) => listed.value.find((item) => item.identity.task_id === id)!.identity.target_ref;
    for (const id of [a, b, c, d]) {
      assert.equal(tree.peers.find((peer) => peer.target_ref === refFor(id))!.asks, "allowed");
      assert.equal((await inspect(refFor(id))).value.live, true);
    }
    assert.equal(tree.peers.find((peer) => peer.target_ref === refFor(e))!.denial_reason, "assignment_denied");
    let rosterEvents = 0;
    const detach = truth.onPeersChanged(() => { rosterEvents++; });
    assert.equal((await inspect()).value.revision, tree.revision);
    const repeated = truth.rosterAfter(tree.revision);
    assert(repeated.ok && repeated.value === null);
    assert.equal(rosterEvents, 0, "UNCHANGED_ROSTER_REPEATED_DISCOVERY");
    assert.deepEqual(await inspect("coordination:foreign" as TargetRef), { ok: false, reason: "invalid_target" });
    const forgedContext = { sessionManager: { getSessionId: () => root.sessionId } };
    assert.equal((await inspect(undefined, forgedContext)).ok, false);
    assert.deepEqual(recordFiles.map(sha256), before, "INSPECTION_MUTATED_RECORDS");
    assert.equal(bus.calls, callsBefore, "INSPECTION_CALLED_PROVIDER");
    report.owned_tree = tree;
    report.identities = raw.map((record, index) => ({ task_id: ids[index],
      parent_session_id: record.parent_session_id, root_session_id: record.root_session_id,
      child_session_id: record.child_session_id, run_epoch: record.notification.run_epoch,
      target_ref: refFor(ids[index]!) }));
    report.read_only = { record_hashes_unchanged: true, additional_provider_calls: 0,
      unchanged_roster_events: rosterEvents, repeated_roster_suppressed: true };
    if (name === "inspect-partial") {
      const original = fs.readFileSync;
      const originalStat = fs.statSync;
      const denied = path.resolve(recordFiles[1]!);
      let denials = 0;
      const refuse = (file: unknown) => {
        if (typeof file === "string" && path.resolve(file) === denied) {
          denials++;
          const error = new Error("Owned QA record read denied") as NodeJS.ErrnoException;
          error.code = "EACCES";
          throw error;
        }
      };
      fs.readFileSync = function (file: any, ...args: any[]) {
        refuse(file);
        return Reflect.apply(original, fs, [file, ...args]);
      } as typeof fs.readFileSync;
      // The native store validates its parsed-record cache with statSync on
      // every read. Refuse that actual read boundary too, rather than relying
      // on the cache happening to miss or on a timing delay.
      fs.statSync = function (file: any, ...args: any[]) {
        refuse(file);
        return Reflect.apply(originalStat, fs, [file, ...args]);
      } as typeof fs.statSync;
      syncBuiltinESMExports();
      restoreRead = () => { fs.readFileSync = original; fs.statSync = originalStat; syncBuiltinESMExports(); };
      const partialResult = await inspect();
      assert(partialResult.ok, JSON.stringify(partialResult));
      const partial = partialResult.value as InspectionTree;
      assert(denials > 0, "READ_FAULT_NOT_OBSERVED");
      assert.equal(partial.coverage, "partial");
      assert.equal(partial.unreadable, 1);
      assert.equal(partial.owned, 6);
      assert.equal(partial.visited, 5);
      assert.equal(partial.targets.length, 6);
      assert(partial.errors.some((error) => error.identity?.task_id === b));
      const unknown = partial.targets.find((target) => target.target_ref === refFor(b))!;
      assert.equal(unknown.record_consistent, "unknown");
      assert.equal(unknown.live, "unknown");
      assert.equal(unknown.status, "unknown");
      assert.equal((await inspect(refFor(b))).ok, false);
      restoreRead();
      restoreRead = () => {};
      assert.deepEqual(recordFiles.map(sha256), before);
      assert.equal(bus.calls, callsBefore);
      report.partial = partial;
      report.partial_fault = { api: "actual fs.statSync/readFileSync(EACCES) in native store",
        denied_reads: denials, restored: true, record_writes: 0, no_ghost_zero_claim: true };
    }
    detach();
    const completions = ids.map((id) => bounded(engine.manager.waitFor(id), `native terminal ${id}`));
    for (const label of labels) bus.emit(`release-${label}`);
    await Promise.all(completions);
    await bounded(root.waitForIdle(), "owner notification drain");
    const settled = await inspect();
    assert(settled.ok);
    for (const id of ids) {
      const terminal = (await inspect(refFor(id))).value as TargetInspection;
      assert.equal(terminal.status, "completed");
      assert.equal(terminal.record_consistent, true);
      assert.equal(terminal.slot.value, "released");
      assert.equal(terminal.live, false);
    }
    report.completed = settled.value;
    if (name === "inspect-partial") {
      // Reproduce the verifier's actual epoch-1 counterexample, not a
      // constructed outcome: native task_send revives the original child.
      const revivedProvider = started(labels[0]!);
      void revivedProvider.catch(() => {});
      const send = root.extensionRunner.getToolDefinition("task_send");
      assert(send, "NATIVE_REVIVAL_TOOL_MISSING");
      await send.execute(randomUUID(), { to: a, message: "Return the next isolated native result." },
        undefined, undefined, root.extensionRunner.createContext());
      await revivedProvider;
      assert.deepEqual(bridge.inspectOwned(refFor(a)), { ok: false, reason: "stale_epoch" });
      const renewedTree = await inspect();
      assert(renewedTree.ok);
      const renewedList = bridge.listOwned();
      assert(renewedList.ok);
      const renewed = renewedList.value.find((row) => row.identity.task_id === a)!;
      assert.equal(renewed.identity.run_epoch, 1);
      const renewedRef = renewed.identity.target_ref;
      const handle = engine.manager.getResidentHandle(a);
      assert(handle, "ACTUAL_NATIVE_MANAGED_HANDLE_MISSING");
      const outcome = bounded<{ status: string; finalResponse?: string }>(
        handle.waitForOutcome(), "actual epoch-1 native outcome");
      let bridgeObserved: any = null;
      const subscribed = bridge.subscribeOutcome(renewedRef, (witness) => { bridgeObserved = witness; });
      assert(subscribed.ok, "NATIVE_OUTCOME_SUBSCRIPTION_REFUSED");
      const originalRename = fs.renameSync;
      let refused = 0;
      fs.renameSync = function (from: any, to: any) {
        if (path.resolve(String(to)) === path.resolve(recordFiles[0]!)) {
          const pending = JSON.parse(fs.readFileSync(from, "utf8"));
          if (pending.status === "completed" && pending.notification.run_epoch === 1) {
            refused++;
            const error = new Error("Owned QA native terminal publication refusal") as NodeJS.ErrnoException;
            error.code = "EPERM";
            throw error;
          }
        }
        return Reflect.apply(originalRename, fs, [from, to]);
      } as typeof fs.renameSync;
      syncBuiltinESMExports();
      try {
        bus.emit(`release-${labels[0]}`);
        const nativeOutcome = await outcome;
        await bounded(root.waitForIdle(), "publication refusal owner drain");
        const recordHash = sha256(recordFiles[0]!);
        const providerCalls = bus.calls;
        const direct = await inspect(renewedRef);
        const treeAfter = await inspect();
        assert(treeAfter.ok);
        const listedAfter = bridge.listOwned();
        assert(listedAfter.ok);
        const retained = listedAfter.value.find((row) => row.identity.task_id === a)!;
        report.native_residue_probe = { passed: false, task_id: a, run_epoch: 1,
          refused_terminal_writes: refused, native_handle_outcome: nativeOutcome,
          bridge_observed: bridgeObserved, canonical: { status: store.load(a).status,
            run_epoch: store.load(a).notification.run_epoch }, direct, tree_after_outcome: treeAfter,
          retained_identity: retained?.identity, resident_handle_released: !engine.manager.getResidentHandle(a) };
        assert(refused > 0, "NATIVE_TERMINAL_REFUSAL_NOT_OBSERVED");
        assert.equal(nativeOutcome.status, "completed");
        assert.equal(store.load(a).status, "running");
        assert.equal(engine.manager.getResidentHandle(a), undefined);
        assert(direct.ok, JSON.stringify(report.native_residue_probe));
        assert.equal(direct.value.work_outcome.value, "completed");
        assert.equal(direct.value.record_consistent, false);
        assert.equal(direct.value.diagnostic_class, "completed_unpublished");
        assert.equal(direct.value.publication.value, "unpublished");
        assert.equal(direct.value.live, false);
        assert.equal(direct.value.slot.value, "released");
        assert.equal(retained.identity.target_ref, renewedRef, "RESIDENCY_RELEASE_LOST_ISSUED_IDENTITY");
        assert.equal(retained.outcome?.handle_id, renewed.identity.handle_id);
        assert.equal(bridgeObserved?.handle_id, renewed.identity.handle_id);
        assert(Object.isFrozen(bridgeObserved), "NATIVE_TERMINAL_WITNESS_MUTABLE");
        assert.equal(treeAfter.value.targets.find((target: TargetInspection) => target.target_ref === renewedRef)
          .work_outcome.value, "completed");
        assert.equal(sha256(recordFiles[0]!), recordHash, "INSPECTION_REPAIRED_NATIVE_RESIDUE");
        assert.equal(bus.calls, providerCalls, "RESIDUE_INSPECTION_CALLED_PROVIDER");
        report.native_residue_probe.passed = true;
        report.native_residue_probe.read_only = { record_hash_unchanged: true, provider_calls: 0 };
      } finally {
        fs.renameSync = originalRename;
        syncBuiltinESMExports();
        subscribed.value();
      }
      const nextGeneration = connectEngineBridge({ context: root.extensionRunner.createContext(),
        generation: state.generation + 1, loaderNamespace: sdk });
      assert(nextGeneration.ok);
      try {
        assert.deepEqual(bridge.inspectOwned(renewedRef), { ok: false, reason: "stale_generation" });
        const next = nextGeneration.value.listOwned();
        assert(next.ok);
        assert.equal(next.value.find((row) => row.identity.task_id === a)?.outcome, null);
        report.native_residue_probe.stale_epoch_refused = true;
        report.native_residue_probe.stale_generation_refused = true;
      } finally { nextGeneration.value.dispose(); }
    }
    report.extension = { entry: path.join(directory, "extension.js"),
      sha256: sha256(path.join(directory, "extension.js")),
      loader_paths: loader.getExtensions().extensions.map((entry: any) => entry.path) };
    report.classification = { live_running: true, normal_completed_consistent: true,
      independent_fields: true, provider_calls_for_inspect: 0 };
    report.passed = true;
  } catch (error) {
    report.passed = false;
    report.error = error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
    throw error;
  } finally {
    restoreRead();
    stopRpcReady();
    if (name === "inspect-rpc" && fs.existsSync(gates)) {
      fs.writeFileSync(path.join(gates, "release-rpc"), "cleanup owned RPC provider\n");
      processHandle ??= global[Symbol.for("omo.agent-coordination.native.v1")]?.contexts
        .get(root?.sessionManager)?.deref()?.manager.getResidentHandle(processTaskId);
      if (processHandle && !processHandle.hasExited()) {
        await bounded(processHandle.waitForOutcome(), "owned RPC outcome cleanup");
        await bounded(processHandle.terminate(), "owned RPC termination");
        await bounded(processHandle.dispose(), "owned RPC disposal");
      }
    }
    for (const cancel of pendingStarts) cancel();
    for (const label of labels) bus.emit(`release-${label}`);
    for (const capture of state?.capture.list() ?? []) {
      const runtime = capture.runtime();
      if (runtime) sessions.push(runtime);
    }
    state?.dispose();
    for (const session of [...new Set(sessions)].reverse()) {
      if (session._disposed) continue;
      await bounded(session.waitForIdle(), "owned session cleanup");
      await session.extensionRunner?.emit({ type: "session_shutdown", reason: "quit" });
      session.dispose();
    }
    if (manifest && sha256(manifest.output) === manifest.candidate_sha256) report.removal = removeAdapter(manifest);
    for (const directory of ["candidate", "package"]) {
      const link = path.join(sandbox, directory, "node_modules");
      if (fs.existsSync(link)) { assert(fs.lstatSync(link).isSymbolicLink()); fs.unlinkSync(link); }
    }
    report.cleanup = { sessions_disposed: [...new Set(sessions)].every((session) => session._disposed),
      prototype_restored: sdk?.AgentSession.prototype.bindExtensions === originalBind,
      dispose_restored: sdk?.AgentSession.prototype.dispose === originalDispose,
      module_subscriptions: global[Symbol.for("omo.agent-coordination.native.v1")]?.moduleListeners.size ?? 0,
      provider_gates: bus.eventNames().length, owned_only: true };
    report.cleanup.process_handle_exited = !processHandle || processHandle.hasExited();
    assert(report.cleanup.sessions_disposed && report.cleanup.prototype_restored &&
      report.cleanup.dispose_restored && report.cleanup.module_subscriptions === 0 &&
      report.cleanup.provider_gates === 0, "INSPECT_CLEANUP_INCOMPLETE");
    assert(Object.entries(report.module_sha256_before).every(([file, hash]) => sha256(file) === hash));
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
  const result = process.argv[process.argv.indexOf("--result") + 1]!;
  const report = await nativeCase(name, sandbox, result);
  console.log(JSON.stringify({ case: name, passed: report.passed, artifact: result }));
}

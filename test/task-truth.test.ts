import { expect, test } from "bun:test";
import { createTaskTruth, projectTask, peerPolicy, runtimeFacts } from "../src/task-truth.ts";
import type { RemoteRuntimeObservation } from "../src/task-truth.ts";
import type { OwnedSnapshot } from "../src/engine-bridge.ts";
import type { RuntimeSnapshot } from "../src/runtime-capture.ts";
import type { TargetRef } from "../src/contracts.ts";
import { runInspectCase } from "../qa/cases/inspect-native.ts";
import type { RuntimeCapture, CapturedRuntime } from "../src/runtime-capture.ts";
import type { EngineBridge } from "../src/engine-bridge.ts";

const at = "2026-10-17T00:00:00.000Z";
function record(changes: Partial<OwnedSnapshot> = {}): OwnedSnapshot {
  return {
    identity: { target_ref: "test-only-fact" as TargetRef, task_id: "st_facts", run_epoch: 1,
      owner_session_id: "owner", owner_root_id: "owner", canonical_session_id: "senpi:child",
      session_id: "child", handle_id: "handle", runtime_instance_id: "runtime", generation: 1 },
    name: "Fact data", status: "running", residency: "resident", role: "explore",
    collaboration: { allowed: true }, notification: { run_epoch: 1, notified_epoch: 0 },
    slot: "held", outcome: null, last_transition_at: at, admission: "admitted",
    source: "native owner engine/store/outcome ports", observed_at: at, ...changes,
  };
}
function runtime(activity: Partial<RuntimeSnapshot["activity"]> = {}): RuntimeSnapshot {
  return { session_id: "child", runtime_instance_id: "runtime", generation: 1, branch: "branch",
    revision: 3, observed_at: at, model: { provider: "offline", id: "facts" }, model_runtime: {},
    system_prompt: "NOT_FOR_INSPECTION", messages: [],
    activity: { isStreaming: false, isBashRunning: false, isCompacting: false,
      hasSessionWork: false, hasActiveWakeSource: false, ...activity } };
}

test("canonical truth and outcome are independent in a revived unpublished run", () => {
  const outcome: NonNullable<OwnedSnapshot["outcome"]> = {
    owner_session_id: "owner", owner_root_id: "owner", generation: 1,
    task_id: "st_facts", run_epoch: 1, handle_id: "handle", status: "completed",
    result_sha256: "result", source: "native ManagedChildHandle.waitForOutcome", observed_at: at,
  };
  const projected = projectTask(record({ outcome, slot: "released" }), runtime());
  expect(projected.status).toBe("running");
  expect(projected.work_outcome.value).toBe("completed");
  expect(projected.publication.value).toBe("unpublished");
  expect(projected.notification.value).toBe("not_accepted");
  expect(projected.record_consistent).toBe(false);
  expect(projected.diagnostic_class).toBe("completed_unpublished");
  expect(projected.run_epoch).toBe(1);
  expect(projected.last_transition_at).toBe(at);
  expect(JSON.stringify(projected)).not.toContain("NOT_FOR_INSPECTION");
});

test("stale handle or epoch witnesses cannot terminalize a current run", () => {
  const stale = { owner_session_id: "owner", owner_root_id: "owner", generation: 1,
    task_id: "st_facts", run_epoch: 0, handle_id: "handle", status: "completed",
    result_sha256: "old", source: "native ManagedChildHandle.waitForOutcome", observed_at: at } as const;
  expect(projectTask(record({ outcome: stale }), runtime()).record_consistent).toBe(true);
  expect(projectTask(record({ outcome: { ...stale, run_epoch: 1, handle_id: "other" } }), runtime())
    .work_outcome.value).toBe("unknown");
  for (const changed of [{ generation: 0 }, { owner_session_id: "foreign" }, { owner_root_id: "foreign" }]) {
    expect(projectTask(record({ outcome: { ...stale, run_epoch: 1, ...changed } }), runtime())
      .work_outcome.value).toBe("unknown");
  }
});

test("normal queued and proven admission failure are distinct without inventing a runtime", () => {
  const queued = projectTask(record({ status: "pending", admission: "queued", slot: "not_acquired",
    identity: { ...record().identity, handle_id: null, session_id: null } }), null);
  const failed = projectTask(record({ status: "error", admission: "failed", slot: "released" }), null);
  expect(queued.diagnostic_class).toBe("queued");
  expect(queued.record_consistent).toBe(true);
  expect(queued.live).toBe("unknown");
  expect(failed.diagnostic_class).toBe("never_started");
  expect(failed.record_consistent).toBe(true);
  expect(failed.live).toBe(false);
  expect(projectTask(record({ status: "running", admission: "unknown" }), null).diagnostic_class)
    .toBe("unknown");
});

test("blocked tool, bash, compaction and deferred wake preserve their independent activity", () => {
  const blocked = runtime({ isStreaming: true });
  const tool = { role: "assistant", content: [{ type: "toolCall", id: "call", name: "read",
    arguments: { secret: "DO_NOT_EXPOSE" } }] };
  const projection = projectTask(record(), { ...blocked, messages: [tool] });
  expect(projection.diagnostic_class).toBe("live_blocked_tool");
  expect(projection.live).toBe(true);
  expect(JSON.stringify(projection)).not.toContain("DO_NOT_EXPOSE");
  for (const [flag, label] of [["isBashRunning", "bash"], ["isCompacting", "compaction"],
    ["hasActiveWakeSource", "deferred_wake"], ["hasSessionWork", "session_work"]] as const) {
    const result = projectTask(record(), runtime({ [flag]: true }));
    expect(result.activity.value).toBe(label);
    expect(result.diagnostic_class).toBe("external_wait");
    expect(result.record_consistent).toBe(true);
  }
});

test("transport refusal, completed publication, and missing evidence never conflate death", () => {
  const unreachable = projectTask(record({ residency: "rpc_detached" }), null, false);
  expect(unreachable.diagnostic_class).toBe("transport_unreachable");
  expect(unreachable.live).toBe("unknown");
  expect(unreachable.record_consistent).toBe("unknown");
  const completed = projectTask(record({ status: "completed", slot: "released",
    notification: { run_epoch: 1, notified_epoch: 1 } }), runtime());
  expect(completed.record_consistent).toBe(true);
  expect(completed.publication.value).toBe("completed");
  expect(completed.live).toBe(false);
  expect(completed.runtime_reachability.value).toBe(true);
});

test("age and model success prose cannot establish completion or death", () => {
  const current = record();
  const silent = runtime();
  expect(projectTask(current, silent).diagnostic_class).toBe("normal");
  const old = projectTask({ ...current, observed_at: "2000-01-01T00:00:00.000Z" },
    { ...silent, messages: [{ role: "assistant", content: [{ type: "text", text: "SUCCESS completed" }] }] });
  expect(old.record_consistent).toBe(true);
  expect(old.work_outcome.value).toBe("unknown");
  expect(old.live).toBe(true);
});

test("trusted role policy permits readonly peers and explicit assignment denial wins", () => {
  for (const role of ["oracle", "explore", "librarian", "plan-consultant", "worker", "root"]) {
    expect(peerPolicy(role, { allowed: true })).toEqual({ asks: "allowed",
      ask_capability: true, notify_capability: true });
  }
  for (const [role, reason] of [["plan-reviewer", "one_shot"],
    ["omo-native-gate-reviewer", "independent_review"],
    ["omo-native-code-reviewer", "independent_review"],
    ["omo-native-qa-executor", "independent_review"]] as const) {
    expect(peerPolicy(role, { allowed: true }).denial_reason).toBe(reason);
  }
  expect(peerPolicy("oracle", { allowed: false, denial_reason: "assignment_denied" }))
    .toMatchObject({ asks: "denied", ask_capability: false, notify_capability: false,
      denial_reason: "assignment_denied" });
  expect(peerPolicy("new-native-one-shot", { allowed: false, denial_reason: "one_shot" }).asks)
    .toBe("denied");
  expect(peerPolicy("worker", { allowed: true }, "denied").denial_reason).toBe("target_excluded");
  expect(peerPolicy("plan-reviewer", { allowed: true }, "allowed").asks).toBe("denied");
});

test("redacted process observations reject stale identity, epoch, generation, events and disconnects", async () => {
  const snapshot = record();
  const rootRuntime = { sessionId: "owner", sessionManager: { getLeafId: () => "root-branch" } };
  const rootRead = { ...runtime(), session_id: "owner", branch: "root-branch" };
  const owner = { runtime: () => rootRuntime, read: () => ({ ok: true, value: rootRead }) } as unknown as CapturedRuntime;
  const bridge = { listOwned: () => ({ ok: true, value: [snapshot] }),
    inspectOwned: () => ({ ok: true, value: snapshot }),
    subscribeOutcome: () => ({ ok: true, value: () => {} }) } as unknown as EngineBridge;
  const capture = { generation: 1, list: () => [], onRuntime: () => () => {} } as unknown as RuntimeCapture;
  const truth = createTaskTruth({ owner, bridge, capture, generation: 1 });
  const valid: RemoteRuntimeObservation = {
    binding: { ...snapshot.identity, kind: "worker", canonical_session_id: "senpi:child",
      session_id: "child", runtime_instance_id: "runtime" },
    facts: runtimeFacts(runtime({ isBashRunning: true })), current: () => true,
  };
  let observation: RemoteRuntimeObservation | null = valid;
  let invalidateDuringRead = false;
  let publishDuringRead = false;
  let publishStaleDuringRead = false;
  truth.remoteRuntimeReader(async ref => {
    if (invalidateDuringRead) truth.invalidateRemote(ref);
    if (publishDuringRead) truth.observeRemote({ ...valid,
      facts: { ...valid.facts, revision: 4, activity: "compaction" } });
    if (publishStaleDuringRead) truth.observeRemote({ ...valid,
      facts: { ...valid.facts, revision: 2, activity: "streaming" } });
    return observation;
  });
  const inspect = async () => {
    const row = await truth.inspect(snapshot.identity.target_ref);
    if (!row.ok || !("live" in row.value)) throw new Error("INSPECTION_FAILED");
    return row.value;
  };
  try {
    const fresh = await inspect();
    expect(fresh).toMatchObject({ live: true, record_consistent: true,
      activity: { value: "bash", observed_at: at }, runtime_reachability: { value: true } });
    expect(JSON.stringify(fresh)).not.toContain("NOT_FOR_INSPECTION");
    for (const binding of [{ ...valid.binding, run_epoch: 0 }, { ...valid.binding, generation: 0 },
      { ...valid.binding, owner_session_id: "foreign" }, { ...valid.binding, owner_root_id: "foreign" },
      { ...valid.binding, session_id: "foreign" }]) {
      observation = { ...valid, binding };
      expect((await inspect()).live).toBe("unknown");
    }
    observation = { ...valid, facts: { ...valid.facts, runtime_instance_id: "foreign" } };
    expect((await inspect()).activity.value).toBe("unknown");
    observation = valid; await inspect();
    observation = { ...valid, facts: { ...valid.facts, revision: 2 } };
    expect((await inspect()).runtime_reachability.value).toBe("unknown");
    observation = valid; invalidateDuringRead = true;
    expect((await inspect()).runtime_reachability.value).toBe("unknown");
    invalidateDuringRead = false; publishDuringRead = true;
    expect((await inspect()).activity.value).toBe("compaction");
    publishDuringRead = false; observation = { ...valid, facts: { ...valid.facts, revision: 5 } }; await inspect();
    publishStaleDuringRead = true; observation = null;
    expect((await inspect()).runtime_reachability.value).toBe("unknown");
    publishStaleDuringRead = false; observation = null;
    expect(await inspect()).toMatchObject({ live: "unknown", record_consistent: "unknown",
      runtime_reachability: { value: "unknown" }, activity: { value: "unknown" }, progress: { value: "unknown" } });
  } finally { truth.dispose(); }
});

test("the real loader's registered inspector visits prefixed owned descendants without writes or model calls", async () => {
  const result = await runInspectCase("inspect-owned-tree");
  expect(result.passed).toBe(true);
  expect(result.owned_tree).toMatchObject({ coverage: "complete", owned: 6, visited: 6, unreadable: 0 });
  expect(result.read_only).toMatchObject({ record_hashes_unchanged: true,
    additional_provider_calls: 0, repeated_roster_suppressed: true });
  expect(result.cleanup).toMatchObject({ sessions_disposed: true, prototype_restored: true,
    dispose_restored: true, module_subscriptions: 0, provider_gates: 0 });
}, 120_000);

test("the original native RPC identity survives late optional metadata and rejects conflicting IDs", async () => {
  const result = await runInspectCase("inspect-rpc");
  expect(result.passed).toBe(true);
  expect(result.rpc).toMatchObject({ passed: true, owned: true, listed: true, handle_kind: "rpc",
    session_id: result.rpc.actual_child_session_id, metadata_controlled: true, metadata_states_verified: 9,
    native_pid_matches: true, native_rpc_branch_matches: true, conflicting_session_refused: true,
    record_unchanged_on_refusal: true, native_completed: true,
    learned_session_stable: true, optional_absence_not_conflict: true });
  expect(result.rpc.matrix[0]).toEqual({ name: "both_absent", handle_session_id: null,
    canonical_child_session_id: null, accepted: true, observed: true, record_unchanged: true });
  expect(result.rpc.matrix.map((row: { name: string; accepted: boolean }) => [row.name, row.accepted])).toEqual([
    ["both_absent", true], ["handle_only", true], ["absent_after_learning", true], ["record_only", true],
    ["both_equal", true], ["both_different", false], ["changed_handle_only", false],
    ["changed_record_only", false], ["restored_equal", true],
  ]);
  expect(result.rpc.counterexample_rpc).toMatchObject({ passed: true, exact_original_handle: true,
    canonical_child_session_id: null, populated_handle_session_id: result.rpc.actual_child_session_id,
    target_ref_unchanged: true, handle_id_unchanged: true, run_epoch_unchanged: true,
    direct: { ok: true }, registered_inspection: { ok: true,
      value: { owned: 2, visited: 2, unreadable: 0, coverage: "complete" } } });
  expect(result.cleanup).toMatchObject({ sessions_disposed: true, prototype_restored: true,
    module_subscriptions: 0, provider_gates: 0, process_handle_exited: true });
}, 120_000);

test("real native-store refusal preserves partial coverage and the actual epoch-1 completed-unpublished outcome", async () => {
  const result = await runInspectCase("inspect-partial");
  expect(result.passed).toBe(true);
  expect(result.partial).toMatchObject({ coverage: "partial", owned: 6, visited: 5, unreadable: 1 });
  expect(result.partial_fault).toMatchObject({ restored: true, record_writes: 0, no_ghost_zero_claim: true });
  expect(result.native_residue_probe).toMatchObject({ passed: true, run_epoch: 1,
    canonical: { status: "running", run_epoch: 1 }, native_handle_outcome: { status: "completed" },
    resident_handle_released: true, stale_epoch_refused: true, stale_generation_refused: true,
    direct: { ok: true, value: { work_outcome: { value: "completed" }, record_consistent: false,
      diagnostic_class: "completed_unpublished", publication: { value: "unpublished" } } },
    read_only: { record_hash_unchanged: true, provider_calls: 0 } });
  expect(result.native_residue_probe.refused_terminal_writes).toBeGreaterThan(0);
  expect(result.cleanup).toMatchObject({ sessions_disposed: true, prototype_restored: true,
    module_subscriptions: 0, provider_gates: 0 });
}, 120_000);

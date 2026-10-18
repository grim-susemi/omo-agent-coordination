import { expect, test } from "bun:test";
import { runBridgeCase } from "../qa/cases/native-bridge.ts";

test("the actual registered native task binds owner, epoch, handle, runtime, and settlement receipts", async () => {
  const result = await runBridgeCase("engine-bridge");
  expect(result.passed).toBe(true);
  expect(result.namespace_hook_ready_before_child).toBe(true);
  expect(result.stale_epoch_refused).toBe(true);
  expect(result.stale_generation_refused).toBe(true);
  expect(result.failed_connection_reported).toMatchObject({ ok: false, reason: "connection_failed" });
  expect(result.failed_native_bindings).toEqual([
    { receiver: "plain_object", native_instance: false, native_rejected: true,
      while_pending: { ok: false, reason: "runtime_mismatch" },
      after_rejection: { ok: false, reason: "runtime_mismatch" },
      runtime_unbound: true, mutation_count: 0 },
    { receiver: "prototype_only", native_instance: true, native_rejected: true,
      while_pending: { ok: false, reason: "runtime_mismatch" },
      after_rejection: { ok: false, reason: "runtime_mismatch" },
      runtime_unbound: true, mutation_count: 0 },
  ]);
  expect(result.refusal_mutation_count).toBe(0);
  expect(result.cleanup).toMatchObject({ sessions_disposed: true, module_subscriptions: 0, prototype_restored: true });
}, 120_000);

test("a genuine decoy sharing the child SessionManager is refused before the actual runtime binds", async () => {
  const result = await runBridgeCase("adversarial-native");
  expect(result.passed).toBe(true);
  expect(result.unrelated_native_runtime).toEqual({
    distinct_object: true, same_session_id: true, same_session_manager: true,
    native_instance: true, native_binding_fulfilled: true, target_child_runtime: false,
    binding_result: { ok: false, reason: "runtime_mismatch" }, native_record_unchanged: true,
  });
  expect(result.actual_native_runtime).toMatchObject({
    binding_result: { ok: true, value: { task_id: result.task_id, run_epoch: 0 } },
    stable_instance_id: true,
  });
  expect(result.native_runtime_owner).toMatchObject({
    same_actual_session_object: true, same_manager_outcome_handle: true, task_id: result.task_id,
  });
  expect(result.current_epoch_runtime_bound).toBe(true);
  expect(result.failed_native_bindings).toHaveLength(2);
  expect(result.cleanup).toMatchObject({
    sessions_disposed: true, module_subscriptions: 0, prototype_restored: true, provider_gates: 0,
  });
}, 120_000);

test("a drifted bundle refuses the adapter while the installed original task still completes", async () => {
  const result = await runBridgeCase("bridge-drift");
  expect(result.drift_refused).toBe(true);
  expect(result.original_task_unharmed).toBe(true);
  expect(result.refused_candidate_absent).toBe(true);
}, 120_000);

test("a real foreign owner cannot inspect or repair the other engine's task", async () => {
  const result = await runBridgeCase("foreign-owner");
  expect(result.foreign_owner).toMatchObject({
    refused: true, mutation_count: 0, original_task_unharmed: true, distinct_native_engines: true,
  });
}, 120_000);

test("unregistered and disabled modes preserve the original completion, notification, and capacity path", async () => {
  const original = await runBridgeCase("original");
  for (const scenario of ["mode-absent", "mode-disabled"] as const) {
    const result = await runBridgeCase(scenario);
    expect(result.normal_completion).toEqual(original.normal_completion);
    expect(result.mode_absent_noop).toBe(true);
    expect(result.cleanup).toMatchObject({ module_subscriptions: 0, prototype_restored: true });
  }
}, 120_000);

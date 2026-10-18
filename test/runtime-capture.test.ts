import { expect, test } from "bun:test";
import { runCaptureCase } from "../qa/cases/capture-native.ts";

test("the real loader captures simultaneous parent and native in-process child with live context", async () => {
  const result = await runCaptureCase("capture-in-process");
  expect(result.passed).toBe(true);
  expect(result.identities).toMatchObject({
    parent_and_child_distinct: true, native_runtime_same_object: true,
    run_epoch: 0, module_ready_before_child: true, same_AgentSession: true,
  });
  expect(result.context_changes).toMatchObject({
    model: true, system_prompt: true, messages: true, revision: true,
    snapshot_detached: true, activity: true,
  });
  expect(result.adversarial).toMatchObject({
    duplicate_capture_prevented: true, fabricated_runtime_refused: true,
    stale_generation_refused: true, prior_wrapper_preserved: true,
  });
  expect(result.lifecycle).toEqual({ native_dispose_observed: true, closed_runtime_refused: true });
  expect(result.cleanup).toMatchObject({
    sessions_disposed: true, prototype_restored: true, module_subscriptions: 0,
  });
}, 120_000);

test("a native process child loads the same single ESM extension and captures its local runtime", async () => {
  const result = await runCaptureCase("capture-process");
  expect(result.passed).toBe(true);
  expect(result.identities).toMatchObject({
    parent_and_child_distinct: true, separate_process: true, run_epoch: 0,
    child_model: "coordination-capture/offline", native_pid_matches: true, native_rpc_branch_matches: true,
  });
  expect(result.extension).toMatchObject({
    native_package_inheritance: true, entry_hash_matches: true, single_esm_js: true,
  });
  expect(result.native_worker_completed).toBe(true);
}, 120_000);

test("version, shape, and class mismatch refuse only capture and preserve native worker completion", async () => {
  const result = await runCaptureCase("capture-unsupported");
  expect(result.passed).toBe(true);
  expect(result.refusals).toEqual(["version", "shape", "class_identity"]);
  expect(result.native_worker_completed).toBe(true);
  expect(result.original_prototype_unchanged).toBe(true);
}, 120_000);

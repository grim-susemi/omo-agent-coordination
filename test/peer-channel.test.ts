import { expect, test } from "bun:test";
import { runChannelCase } from "../qa/cases/channel-native.ts";

test("native local endpoints fence ownership, epochs, duplicates, cancellation, and late replies", async () => {
  const result = await runChannelCase("channel-in-process");
  expect(result.passed).toBe(true);
  expect(result.local).toMatchObject({
    native_runtime_same_object: true, root_has_no_task_id: true,
    root_to_child: true, child_to_root: true, child_to_sibling: true,
    duplicate_handler_calls: 1, conflicting_payload_refused: true,
    oversize_refused: true, unknown_refused: true, foreign_context_refused: true,
    one_shot_refused: true, stale_epoch_refused: true, late_reply_discarded: true,
    aborted_request_refused: true,
    malformed_wire_refused: true, wrong_generation_refused: true,
  });
  expect(result.cleanup).toMatchObject({
    sessions_disposed: true, listeners: 0, endpoints: 0, pending_requests: 0,
    dependency_junctions: 0, installed_unchanged: true,
  });
}, 120_000);

test("native process channels return large replies byte-identically without replay on reconnect", async () => {
  const result = await runChannelCase("channel-process");
  expect(result.passed).toBe(true);
  expect(result.process).toMatchObject({
    native_pid_matches: true, native_rpc_branch_matches: true,
    root_to_child: true, child_to_root: true, child_to_sibling: true,
    separate_process: true, split_frames: true, byte_identical: true,
    reconnect_replayed: false, handler_calls: 1,
  });
  expect(result.process.reply_bytes).toBeGreaterThan(65_536);
  expect(result.cleanup).toMatchObject({
    sessions_disposed: true, listeners: 0, endpoints: 0, pending_requests: 0,
    process_handles_exited: true, dependency_junctions: 0, installed_unchanged: true,
  });
}, 120_000);

test("a disconnected native worker is unreachable, not dead or completed", async () => {
  const result = await runChannelCase("channel-disconnected");
  expect(result.passed).toBe(true);
  expect(result.disconnected).toMatchObject({
    result: { ok: false, reason: "host_failure", reachability: "unreachable" },
    native_status_before: "running", native_status_after: "running",
    native_handle_alive: true, native_record_unchanged: true,
    worker_completed_normally: true,
  });
}, 120_000);

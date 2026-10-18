import { describe, expect, test } from "bun:test";
import {
  API_PORTS, ASKS, DEPTHS, ENDED_BY, FEATURES, INSPECT_FIELDS, LIMITS, REQUEST_STATES,
  TERMINAL_REASONS, TERMINAL_STATES, isRecordConsistent, isRequestState, isTargetBinding,
} from "../src/contracts.ts";
import type { TargetRef, ToolAskInput } from "../src/contracts.ts";

describe("Coordination API v1 machine contract", () => {
  test("request and terminal states are distinct and immutable", () => {
    expect(REQUEST_STATES).toEqual(["accepted", "delivered", "replied", "failed", "timed_out", "cancelled"]);
    expect(TERMINAL_STATES).toEqual(["replied", "failed", "timed_out", "cancelled"]);
    expect(Object.isFrozen(REQUEST_STATES)).toBe(true);
    expect(isRequestState("completed")).toBe(false);
    expect(isRequestState("delivered")).toBe(true);
    expect(new Set(TERMINAL_REASONS).size).toBe(TERMINAL_REASONS.length);
  });
  test("amendment 2 has explicit depth, ending, policy, and feature values", () => {
    const rejectsDeepTimeout: {
      target_ref: TargetRef; question: string; depth: "deep"; timeout_ms: number;
    } extends ToolAskInput ? false : true = true;
    expect(rejectsDeepTimeout).toBe(true);
    expect(DEPTHS).toEqual(["quick", "deep"]);
    expect(ENDED_BY).toEqual(["reply", "target_settled", "target_not_live", "asker_aborted",
      "owner_cancel", "generation_change", "provider_failure", "host_failure"]);
    expect(FEATURES).toEqual(["ask_depth_deep", "ask_policy", "inspect", "revival_settlement_repair"]);
    for (const reason of ["target_excluded", "policy_not_owner", "invalid_target", "host_failure",
      "reasoning_policy_unsupported", "stale_generation", "api_version_unsupported"] as const) {
      expect(TERMINAL_REASONS).toContain(reason);
    }
    expect(Object.isFrozen(FEATURES)).toBe(true);
    expect(ASKS).toEqual(["allowed", "denied"]);
  });
  test("inspect preserves uncertainty and has all six projection fields", () => {
    expect(INSPECT_FIELDS).toEqual(["live", "status", "run_epoch", "last_transition_at", "record_consistent", "asks"]);
    expect([true, false, "unknown"].every(isRecordConsistent)).toBe(true);
    expect([null, undefined, 0, "false"].some(isRecordConsistent)).toBe(false);
    expect(API_PORTS).toHaveLength(7);
  });
  test("worker and root bindings carry different identity fields", () => {
    const common = { target_ref: "opaque-1", owner_session_id: "owner", owner_root_id: "root",
      canonical_session_id: "senpi:child", session_id: "child", runtime_instance_id: "instance", generation: 0 };
    expect(isTargetBinding({ ...common, kind: "worker", task_id: "st_test", run_epoch: 1 })).toBe(true);
    expect(isTargetBinding({ ...common, kind: "worker", task_id: "st_test" })).toBe(false);
    expect(isTargetBinding({ ...common, kind: "worker", task_id: "st_test", run_epoch: -1 })).toBe(false);
    expect(isTargetBinding({ ...common, kind: "root", branch: "branch" })).toBe(true);
    expect(isTargetBinding({ ...common, kind: "root", task_id: "invented" })).toBe(false);
  });
  test("byte, concurrency, and quick limits retain the approved numbers", () => {
    expect(LIMITS.question_utf8_bytes).toBe(16384);
    expect(LIMITS.message_utf8_bytes).toBe(4096);
    expect(LIMITS.target_concurrency).toBe(1);
    expect(LIMITS.global_concurrency).toBe(4);
    expect(LIMITS.quick_timeout_default_ms).toBe(30000);
    expect(LIMITS.quick_timeout_max_ms).toBe(60000);
    expect(LIMITS.quick_output_tokens).toBe(4096);
    expect(Object.isFrozen(LIMITS)).toBe(true);
  });
});

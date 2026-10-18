import { expect, test } from "bun:test";
import fs from "node:fs";
import { runLiveCase } from "../qa/live.ts";

test("the default senpi.exe descendant binds once and refuses foreign, stale and duplicate bootstrap claims", async () => {
  const run = await runLiveCase("acceptance");
  const report = JSON.parse(fs.readFileSync(run.artifact, "utf8"));
  expect(report.passed).toBe(true);
  expect(report.process.native_spawn.executable).toMatch(/senpi\.exe$/i);
  expect(report.process.runtime_pid).not.toBe(report.process.manager_pid);
  expect(report.process.actual_native_rpc_branch_matches).toBe(true);
  expect(report.bootstrap_regressions).toEqual({
    positive_default_descendant: true,
    fixture_bootstrap_calls: 0,
    non_descendant: { ok: false, reason: "scope_denied" },
    different_session: { ok: false, reason: "scope_denied" },
    earlier_epoch: { ok: false, reason: "stale_epoch" },
    second_claimant: { ok: false, reason: "scope_denied" },
    provider_calls_on_refusal: 0,
  });
  expect(report.cleanup).toMatchObject({ sessions_disposed: true, provider_listeners: 0,
    installed_unchanged: true, adapter_restored: true });
  expect(report.context_consumption).toMatchObject({ automatic_roster_observed_before_discovery: true,
    unchanged_roster_reinjection: 0, persistent_roster_history_entries: 0,
    stable_roster_projection: { sections: 1 } });
  expect(report.process_inspection.self).toMatchObject({ live: true, record_consistent: true,
    runtime_reachability: { value: true } });
  expect(report.process_inspection.parent).toMatchObject({ live: true, record_consistent: true,
    activity: { value: "blocked_tool" }, runtime_reachability: { value: true } });
  expect(report.process_inspection.parent.progress.value).not.toBe("unknown");
  expect(report.process_inspection.disconnected).toMatchObject({ live: "unknown", record_consistent: "unknown",
    activity: { value: "unknown" }, progress: { value: "unknown" }, runtime_reachability: { value: "unknown" } });
  expect(report.process_inspection.native_main_preserved_on_disconnect).toBe(true);
  expect(report.independent_review).toMatchObject({ role: "explore", owner_registered: true,
    ordinary_role_control_allowed: true, policy_cannot_reopen: true, allow_from_cannot_reopen: true,
    provider_calls: 0, queue_additions: 0 });
  expect(report.process_sibling_display).toMatchObject({ ask_events: 1, notify_events: 1,
    duplicate_events: 0, root_history_unchanged: true, root_context_additions: 0, forced_turns: 0 });
}, 60_000);

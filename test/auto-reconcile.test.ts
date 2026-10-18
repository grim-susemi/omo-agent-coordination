import { expect, test } from "bun:test";
import { attachAutoReconcile, createAutoReconcile, repairActive } from "../src/auto-reconcile.ts";
import type { EngineBridge, OutcomeWitness, OwnedSnapshot } from "../src/engine-bridge.ts";
import type { TargetRef } from "../src/contracts.ts";
import { projectTask } from "../src/task-truth.ts";
import { publishQuestionBroker } from "../src/peer-questions.ts";
import type { NativeReplayCarrier } from "../src/task-settlement.ts";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { ACTIVITY_FILE, bindActivityDir } from "../src/activity-log.ts";

// Boundary doubles test event selection only. The owned cases separately load
// the actual SDK, engine, store, native runner and original replay closure.
function fixture(enabled = true) {
  const ref = "coordination:test" as TargetRef;
  let row: OwnedSnapshot = {
    identity: { target_ref: ref, task_id: "task", run_epoch: 1, owner_session_id: "owner",
      owner_root_id: "owner", canonical_session_id: "child", session_id: "child", handle_id: "handle",
      runtime_instance_id: null, generation: 1 },
    status: "running", residency: "resident", role: "explore", name: "worker", last_transition_at: "2000-01-01",
    admission: "admitted", collaboration: { allowed: true }, notification: { run_epoch: 1, notified_epoch: 0 },
    slot: "released", outcome: null, source: "native owner engine/store/outcome ports", observed_at: "2026-10-17",
  };
  const witness: OutcomeWitness = { ...row.identity, handle_id: "handle", status: "completed",
    result_sha256: "result-link", source: "native ManagedChildHandle.waitForOutcome", observed_at: "2026-10-17" };
  const listeners = new Set<(witness: OutcomeWitness) => void>();
  let mutations = 0;
  const bridge: EngineBridge = {
    listOwned: () => ({ ok: true, value: [row] }),
    inspectOwned: () => ({ ok: true, value: row }),
    bindActualRuntime: () => ({ ok: false, reason: "runtime_mismatch" }),
    subscribeOutcome(_ref, listener) {
      listeners.add(listener);
      return { ok: true, value: () => { listeners.delete(listener); } };
    },
    reconcileOwnedTerminal() {
      mutations++;
      row = { ...row, status: "completed", notification: { run_epoch: 1, notified_epoch: 1 } };
      return { ok: true, value: { task_id: "task", run_epoch: 1, state: "settled", result_sha256: "result-link",
        result_preserved: true, publication: "published", notification: "accepted", waiter: "native_canonical",
        slot: "released", replayed: true } };
    },
    dispose() {},
  };
  const auto = createAutoReconcile({ bridge, enabled });
  return { bridge, auto, ref, witness, row: () => row, mutations: () => mutations,
    listeners: () => listeners.size,
    change: (change: Partial<OwnedSnapshot>) => { row = { ...row, ...change }; },
    finish(w = witness) {
      row = { ...row, outcome: w };
      for (const listener of [...listeners]) listener(w);
    },
  };
}

test("native completion and repeated events repair once and retain the current result link", () => {
  const f = fixture();
  f.auto.observe(); f.auto.observe();
  expect(f.listeners()).toBe(1);
  f.finish();
  for (let i = 0; i < 5; i++) { f.auto.observe(); f.finish(); }
  expect(f.mutations()).toBe(1);
  expect(f.row().outcome?.result_sha256).toBe("result-link");
  f.auto.dispose(); expect(f.listeners()).toBe(0);
});

test("live, pending, detached and unproven never-started work stays read-only regardless of age", () => {
  for (const age of ["1970-01-01", "2026-10-17", "2099-01-01"]) {
    for (const change of [
      {}, { status: "pending" as const, admission: "queued" as const },
      { residency: "rpc_detached" }, { residency: "persisted_only" }, { admission: "failed" as const },
    ]) {
      const f = fixture(); f.change({ ...change, last_transition_at: age });
      f.auto.observe();
      if (Object.keys(change).length) f.finish();
      f.auto.observe(); expect(f.mutations()).toBe(0); f.auto.dispose();
    }
  }
});

test("foreign, prior-epoch, wrong-handle and stale-generation outcomes never authorize repair", () => {
  for (const change of [{ owner_root_id: "foreign" }, { owner_session_id: "foreign" }, { run_epoch: 0 },
    { handle_id: "other" }, { generation: 0 }, { task_id: "other" }]) {
    const f = fixture(); f.auto.observe(); f.finish({ ...f.witness, ...change });
    f.auto.observe(); expect(f.mutations()).toBe(0); f.auto.dispose();
  }
});

test("repair-disabled generation leaves inspection mismatch intact and has no subscriptions or mutations", () => {
  const f = fixture(false);
  f.auto.observe(); f.finish(); f.auto.observe();
  expect(f.listeners()).toBe(0); expect(f.mutations()).toBe(0);
  expect(projectTask(f.row(), null).record_consistent).toBe(false);
  expect(repairActive(f.bridge)).toBe(false); f.auto.dispose();
});

test("a queued outcome is discarded after disposal or an owner/epoch transition", () => {
  const f = fixture(); f.auto.observe(); f.auto.dispose(); f.finish();
  expect(f.mutations()).toBe(0);
  const changed = fixture(); changed.auto.observe();
  changed.change({ identity: { ...changed.row().identity, run_epoch: 2 } });
  changed.finish(); expect(changed.mutations()).toBe(0); changed.auto.dispose();
});

test("broker feature is frozen at publication and reflects an actually connected repair handler", () => {
  const f = fixture();
  expect(repairActive(f.bridge)).toBe(true);
  const enabled = publishQuestionBroker(new Set(), {}, repairActive(f.bridge))!;
  expect(enabled.broker.features).toContain("revival_settlement_repair");
  expect(Object.isFrozen(enabled.broker.features)).toBe(true);
  f.auto.dispose(); expect(repairActive(f.bridge)).toBe(false);
  expect(enabled.broker.features).toContain("revival_settlement_repair");
  const disabled = publishQuestionBroker(new Set(), {}, false)!;
  expect(disabled.broker.features).not.toContain("revival_settlement_repair");
  enabled.dispose(); disabled.dispose();
});

test("same-store reattachment shares native events and preserves settlement observer in either disposal order", () => {
  for (const order of [[0, 1], [1, 0]]) {
    const a = fixture(false), b = fixture(false);
    const store = { load: () => null };
    const carrier: NativeReplayCarrier = { terminalObservers: new WeakMap(), replays: new WeakMap() };
    const original = () => ({ failed() {} }); carrier.terminalObservers!.set(store, original);
    carrier.replays!.set(store, () => {});
    let listeners = 0;
    const controllers = [a, b].map((f) => attachAutoReconcile({
      bridge: f.bridge, carrier, store, onMutation() { listeners++; return () => { listeners--; }; },
    })!);
    expect(listeners).toBe(1);
    controllers[order[0]!]!.dispose(); expect(listeners).toBe(1);
    controllers[order[1]!]!.dispose(); expect(listeners).toBe(0);
    expect(carrier.terminalObservers!.get(store)).toBe(original);
    expect(a.listeners() + b.listeners()).toBe(0);
  }
});

test("a rejected early replay is eligible again when the native terminal event establishes its transition", () => {
  const f = fixture(); const original = f.bridge.reconcileOwnedTerminal;
  f.bridge.reconcileOwnedTerminal = () => ({ ok: false, reason: "repair_not_enabled" });
  f.auto.observe(); f.finish(); expect(f.mutations()).toBe(0);
  f.bridge.reconcileOwnedTerminal = original;
  f.auto.observe(); f.auto.observe(); expect(f.mutations()).toBe(1); f.auto.dispose();
});

const activity = (dir: string) => fs.existsSync(path.join(dir, ACTIVITY_FILE))
  ? fs.readFileSync(path.join(dir, ACTIVITY_FILE), "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line)) : [];

test("a successful repair appends exactly one repair activity line, flushed once the owner dir is bound", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "coordination-repair-"));
  try {
    const f = fixture();
    f.auto.observe(); f.finish();
    expect(f.mutations()).toBe(1);
    expect(activity(dir)).toHaveLength(0);
    bindActivityDir(f.bridge, dir);
    for (let i = 0; i < 3; i++) { f.auto.observe(); f.finish(); }
    const lines = activity(dir);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ v: 1, kind: "repair", phase: "done", from: "auto-repair", to: null, ok: true,
      summary: "settled worker run 1" });
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

test("a failed repair or no proven candidate appends no repair line", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "coordination-repair-"));
  try {
    const failed = fixture(); bindActivityDir(failed.bridge, dir);
    failed.bridge.reconcileOwnedTerminal = () => ({ ok: false, reason: "repair_not_enabled" });
    failed.auto.observe(); failed.finish();
    const none = fixture(); bindActivityDir(none.bridge, dir);
    none.auto.observe(); none.auto.observe();
    expect(none.mutations()).toBe(0);
    expect(activity(dir)).toHaveLength(0);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { attachTaskSettlement } from "../src/task-settlement.ts";
import type { NativeReplayCarrier, SettlementRecord } from "../src/task-settlement.ts";
import { runSettlementCase } from "../qa/cases/revived-settlement.ts";

test("native epoch1 replay preserves result, settles new waiters, notifies once, and returns capacity", async () => {
  const result = await runSettlementCase("revived-settlement");
  expect(result.passed).toBe(true);
  expect(result.settlement).toMatchObject({ state: "settled", result_preserved: true,
    publication: "published", notification: "accepted", waiter: "native_canonical", slot: "released", replayed: true });
  expect(result.original_waiter.status).toBe("error");
  expect(result.repeated.replayed).toBe(false);
  expect(result.prior_epoch_protected).toBe(true);
  expect(result.capacity_reused).toBe(true);
  expect(result.after.notification).toEqual({ run_epoch: 1, notified_epoch: 1 });
}, 45_000);

// Pure boundary tests exercise the replay fence independently of SDK timing.
// Real authority, notification, retry, and capacity proofs are above.
function boundary() {
  let record: SettlementRecord = { task_id: "task", parent_session_id: "owner", root_session_id: "owner",
    residency_state: "resident", status: "running", notification: { run_epoch: 1, notified_epoch: 0 } };
  const store = { load: () => record };
  const carrier: NativeReplayCarrier = { terminalObservers: new WeakMap(), replays: new WeakMap() };
  const handle = { waitForOutcome: () => Promise.resolve({ status: "completed", finalResponse: "result" }) };
  let beforeLock = () => {};
  let writes = 0;
  carrier.replays!.set(store, (_id, _owned, _timestamp, transition) => {
    beforeLock();
    const fence = Reflect.get(transition, Symbol.for("omo.agent-coordination.terminal-fence.v1"));
    if (!fence(record)) return;
    writes++;
    record = { ...record, status: "completed", final_response: transition.final_response };
  });
  const connection = attachTaskSettlement({ carrier, store, liveHandle: () => handle })!;
  carrier.terminalObservers!.get(store)!("task", record,
    { type: "complete", timestamp: "2026-10-17T00:00:00Z", final_response: "result" });
  const input = { task_id: "task", run_epoch: 1, handle,
    result_sha256: createHash("sha256").update("result").digest("hex"),
    guard: (fresh: SettlementRecord) => fresh.notification.run_epoch === 1 && fresh.root_session_id === "owner",
    slot: () => "released" as const };
  return { carrier, store, connection, input, writes: () => writes,
    change: (fn: (fresh: SettlementRecord) => SettlementRecord) => { beforeLock = () => { record = fn(record); }; } };
}

test("the native locked-boundary fence rejects an epoch or owner change after the precheck", () => {
  for (const changed of [
    (fresh: SettlementRecord) => ({ ...fresh, notification: { ...fresh.notification, run_epoch: 2 } }),
    (fresh: SettlementRecord) => ({ ...fresh, root_session_id: "other-owner" }),
  ]) {
    const fixture = boundary();
    fixture.change(changed);
    expect(fixture.connection.reconcile(fixture.input)).toBeUndefined();
    expect(fixture.writes()).toBe(0);
    fixture.connection.dispose();
    expect(fixture.carrier.terminalObservers!.has(fixture.store)).toBe(false);
  }
});

test("publication without notification acceptance is degraded and repeated inspection does not notify", () => {
  const fixture = boundary();
  const receipt = fixture.connection.reconcile(fixture.input)!;
  expect(receipt).toMatchObject({ state: "degraded", publication: "published",
    notification: "unaccepted", error: { phase: "notification", code: "not_accepted" } });
  expect(fixture.connection.reconcile(fixture.input)!.replayed).toBe(false);
  expect(fixture.writes()).toBe(1);
  fixture.connection.dispose();
});

test("a native storage failure retains the outcome without claiming publication or notification", () => {
  const fixture = boundary();
  fixture.carrier.replays!.set(fixture.store, (id, owned, _timestamp, transition) => {
    fixture.carrier.terminalObservers!.get(fixture.store)!(id, owned, transition)
      .failed(Object.assign(new Error("native storage fault"), { code: "ENOSPC" }));
  });
  expect(fixture.connection.reconcile(fixture.input)).toMatchObject({
    state: "degraded", result_preserved: true, publication: "unpublished",
    notification: "unaccepted", waiter: "native_storage_failure", slot: "released",
    error: { phase: "publication", code: "ENOSPC" },
  });
  expect(fixture.writes()).toBe(0);
  fixture.connection.dispose();
});

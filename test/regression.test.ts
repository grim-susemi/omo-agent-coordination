import { expect, test } from "bun:test";
import { MISMATCH } from "../qa/harness.ts";
import { runRegressionCase as runCase } from "../qa/cases/revived-settlement.ts";

test("native epoch0 completion: canonical record, notification, and capacity agree", async () => {
  const result = await runCase("normal-baseline");
  expect(result.capacity_consistent).toBe(true);
}, 45_000);

test("R2 st_01a1469c: task_send revival publishes epoch1 while parent and companion are busy", async () => {
  const result = await runCase("revived-stale-record", "r2-closest");
  expect(result.captured_mismatch, MISMATCH).toBe(false);
  expect((result.current_record as { status: string }).status).toBe("completed");
  expect(result.notification_consistent).toBe(true);
  expect(result.capacity_consistent).toBe(true);
}, 45_000);

test("CURRENT_EPOCH_OUTCOME_COMPLETED_CANONICAL_RUNNING: R2 revival must publish after the Windows reader releases", async () => {
  const result = await runCase("revived-stale-record", "r2-closest-reader");
  expect(result.preserved_result).toBe("native-result-2");
  expect((result.record_after_reader_release as { status: string }).status, MISMATCH).toBe("completed");
}, 60_000);

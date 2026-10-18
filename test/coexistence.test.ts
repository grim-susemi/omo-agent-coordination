import { expect, test } from "bun:test";
import { runCoexistence } from "../qa/cases/coexistence-fault.ts";

test("actual summary, publication retry, no-fallback and Herdr coexist in both loader orders", async () => {
  const report = await runCoexistence();
  expect(report.passed).toBe(true);
  expect(report.orders.length).toBe(2);
  for (const order of report.orders) {
    expect(order.summary_fallback).toBe(true);
    expect(order.descriptor_preserved).toBe(true);
    expect(order.native_completed).toBe(true);
    expect(order.reloads).toBe(2);
    expect(order.stale_client_refused).toBe(true);
    expect(order.queue_foreign_preserved).toBe(true);
    expect(order.side_replayed).toBe(false);
    expect(order.capture_missing).toBe(true);
  }
}, 60_000);

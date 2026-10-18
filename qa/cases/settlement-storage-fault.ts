import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { runSettlementCase, settlementEvidence } from "./revived-settlement.ts";

export async function runStorageFaultCase() {
  const results = [];
  for (const scenario of ["retry-success", "permanent", "nontarget"] as const) {
    const result = await runSettlementCase(scenario);
    assert.equal(result.passed, true);
    results.push(result);
  }
  const report = { case: "settlement-storage-fault", passed: true, results };
  fs.writeFileSync(path.join(settlementEvidence, "storage-fault.json"), JSON.stringify(report, null, 2) + "\n");
  return report;
}
export const cases = { "settlement-storage-fault": runStorageFaultCase };

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { install } from "../../scripts/install.mjs";
import { installationFixture, snapshot } from "./install-roundtrip.ts";
import { runCommand, save, hashes } from "./install-receipts.ts";

export async function runDrift(): Promise<Record<string, any>> {
  const refusals: { name: string; unchanged: boolean; exit_code: number; receipt: string }[] = [];
  const edits: [string, string, "missing" | "corrupt", "uninstall" | "rollback"][] = [
    ["missing-receipt", ".agent-coordination-install/receipt.json", "missing", "uninstall"],
    ["corrupt-receipt", ".agent-coordination-install/receipt.json", "corrupt", "rollback"],
    ["missing-seal", ".agent-coordination-install/receipt.sha256", "missing", "uninstall"],
    ["missing-settings-backup", ".agent-coordination-install/settings.original", "missing", "rollback"],
    ["corrupt-settings-backup", ".agent-coordination-install/settings.original", "corrupt", "uninstall"],
    ["missing-adapter-backup", ".agent-coordination-install/adapter/omo-task.js.baseline", "missing", "uninstall"],
    ["corrupt-adapter-backup", ".agent-coordination-install/adapter/omo-task.js.baseline", "corrupt", "rollback"],
    ["corrupt-adapter-manifest", ".agent-coordination-install/adapter/omo-task.js.manifest.json", "corrupt", "uninstall"],
    ["installed-entry-drift", "packages/piomo-agent-coordination-mod/dist/extension.js", "corrupt", "uninstall"],
    ["settings-drift", "settings.json", "corrupt", "rollback"],
    ["native-target-drift", "native/omo-task.js", "corrupt", "uninstall"],
  ];
  const source = installationFixture();
  fs.appendFileSync(source.target, "\n// upstream drift\n");
  const sourceBefore = snapshot(source.agentDir);
  const sourceResult = await runCommand(["bun", "scripts/install.mjs", "install",
    "--agent-dir", source.agentDir, "--adapter-target", source.target], "source-drift");
  assert.notEqual(sourceResult.code, 0);
  assert.deepEqual(snapshot(source.agentDir), sourceBefore);
  refusals.push({ name: "source-drift", unchanged: true, exit_code: sourceResult.code, receipt: sourceResult.receipt });
  for (const [name, relative, edit, action] of edits) {
    const fixture = installationFixture();
    await install({ agentDir: fixture.agentDir, adapterTarget: fixture.target });
    const file = path.join(fixture.agentDir, relative);
    if (edit === "missing") fs.unlinkSync(file);
    else fs.appendFileSync(file, "\nCORRUPTED\n");
    const before = snapshot(fixture.agentDir);
    const result = await runCommand(["bun", "scripts/install.mjs", action, "--agent-dir", fixture.agentDir], name);
    assert.notEqual(result.code, 0, `REMOVAL_REPORTED_SUCCESS:${name}:${result.output}`);
    assert.deepEqual(snapshot(fixture.agentDir), before, `REMOVAL_MUTATED_BEFORE_REFUSAL:${name}`);
    assert(fs.existsSync(path.join(fixture.agentDir, "packages/piomo-agent-coordination-mod/dist/extension.js")));
    refusals.push({ name, unchanged: true, exit_code: result.code, receipt: result.receipt });
  }
  const report = { case: "install-drift", passed: true, source_sha256: hashes(), refusals,
    cleanup: { intentionally_corrupted_fixtures_preserved_for_evidence: true, owned_child_processes_running: 0 },
    owner_adversarial: "Missing/hash-mismatched receipts and original backups refuse non-zero before deleting or overwriting any remaining fixture file." };
  save("install-drift", report);
  return report;
}
export const cases = { "install-drift": runDrift };

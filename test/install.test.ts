import { expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { installationFixture, runInstallCase, snapshot } from "../qa/cases/install-roundtrip.ts";
import { install } from "../scripts/install.mjs";

test("managed package dry-run, actual loader import and uninstall restore original bytes", async () => {
  const report = await runInstallCase("install-roundtrip");
  expect(report.passed).toBe(true);
  expect(report.settings_restored).toBe(true);
  expect(report.adapter_restored).toBe(true);
  expect(report.pure_helper).toBe(true);
}, 60_000);

test("source, installed-file, missing receipt and backup drift refuse before any mutation", async () => {
  const report = await runInstallCase("install-drift");
  expect(report.passed).toBe(true);
  expect(report.refusals.length).toBeGreaterThanOrEqual(8);
  expect(report.refusals.every((r: { unchanged: boolean; exit_code: number }) => r.unchanged && r.exit_code !== 0)).toBe(true);
}, 60_000);

test("installer rejects symlink escapes and a missing explicit disposable agent directory", async () => {
  await expect(install({})).rejects.toThrow("AGENT_DIR_REQUIRED");
  const fixture = installationFixture();
  expect(fixture.agentDir.replaceAll("\\", "/")).toContain("/task-9/");
  const before = snapshot(fixture.agentDir);
  const link = path.join(fixture.directory, "linked-agent");
  fs.symlinkSync(fixture.agentDir, link, "junction");
  try {
    await expect(install({ agentDir: link, dryRun: true })).rejects.toThrow("SYMLINK_REFUSED");
    await expect(install({ agentDir: link })).rejects.toThrow("SYMLINK_REFUSED");
    expect(snapshot(fixture.agentDir)).toEqual(before);
  } finally {
    fs.unlinkSync(link);
  }
});

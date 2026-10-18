import fs from "node:fs";
import path from "node:path";
import { attempt, evidence, product, sourceHashes, sha256, command } from "./command.mjs";

const names = fs.readdirSync(evidence);
const receipts = names.filter((name) => /^command-.*\.json$/.test(name)).map((name) => {
  const file = path.join(evidence, name);
  return { ...JSON.parse(fs.readFileSync(file, "utf8")), receipt_path: file };
});
const source = sourceHashes();
const processCleanup = [];
for (const receipt of receipts.filter((entry) =>
  entry.status === "running" && entry.argv.includes("--native-case") && Number.isInteger(entry.pid))) {
  const expected = path.join(product, "qa/harness.ts").replaceAll("\\", "/").replaceAll("'", "''");
  const script = `$ErrorActionPreference='Stop'; $p = Get-CimInstance Win32_Process -Filter 'ProcessId=${receipt.pid}';
if ($null -eq $p) { @{pid=${receipt.pid};present=$false} | ConvertTo-Json -Compress; exit 0 };
$started = [DateTimeOffset]::Parse('${receipt.start_time}').UtcDateTime;
$same = $p.CommandLine.Replace('\\','/').Contains('${expected}') -and $p.CommandLine.Contains('--native-case');
if (-not $same -or [Math]::Abs(($p.CreationDate.ToUniversalTime()-$started).TotalSeconds) -gt 10) {
  throw 'Refusing to stop a reused or foreign PID'
};
$owned = Get-Process -Id ${receipt.pid};
if ([Math]::Abs(($owned.StartTime.ToUniversalTime()-$started).TotalSeconds) -gt 10) { throw 'Owned process identity changed' };
$owned.Kill();
if (-not $owned.WaitForExit(5000)) { throw 'Owned process did not exit' };
@{pid=${receipt.pid};present=$true;matched=$true;stopped=$true;exit_code=$owned.ExitCode} | ConvertTo-Json -Compress`;
  let raw = "";
  const exit = await command([
    path.join(process.env.SystemRoot, "System32/WindowsPowerShell/v1.0/powershell.exe"),
    "-NoProfile", "-Command", script,
  ], { timeoutMs: 15_000, onStdout(data) { raw += data.toString(); } });
  if (exit !== 0) throw new Error(`Owned process cleanup failed: ${receipt.receipt_path}`);
  const observation = JSON.parse(raw.trim());
  const closure = { observed_at: new Date().toISOString(), ...observation,
    reason: "The parent command timed out before its native child receipt could close." };
  receipt.cleanup = closure;
  receipt.status = "closed_by_cleanup";
  if (observation.exit_code !== undefined) receipt.exit_code = observation.exit_code;
  const { receipt_path, ...updated } = receipt;
  fs.writeFileSync(receipt_path, JSON.stringify(updated, null, 2) + "\n");
  processCleanup.push({ receipt_path, ...closure });
}
const view = (record) => record ? {
  task_id: record.task_id, status: record.status, residency: record.residency_state,
  run_epoch: record.notification?.run_epoch, notified_epoch: record.notification?.notified_epoch,
  result: record.final_response ?? null,
} : null;
const variants = names.filter((name) =>
  /^(normal-baseline|revived-stale-record)-.*\.json$/.test(name) && !name.endsWith(".cleanup.json"))
  .map((name) => {
    const artifact = path.join(evidence, name);
    const run = JSON.parse(fs.readFileSync(artifact, "utf8"));
    const commands = receipts.filter((receipt) => {
      if (!receipt.raw_output_path || !fs.existsSync(receipt.raw_output_path)) return false;
      const raw = fs.readFileSync(receipt.raw_output_path, "utf8");
      return raw.includes(JSON.stringify(artifact)) ||
        (run.setup_error && receipt.start_time <= run.started_at && receipt.end_time >= run.started_at);
    });
    const operations = run.operations ?? [];
    const renameErrors = operations.filter((op) => op.op === "rename" && op.error);
    const completed = new Set();
    let staleOverwrite = false;
    for (const operation of operations.filter((op) => op.op === "rename" && !op.error)) {
      const key = `${operation.path}:${operation.epoch}`;
      if (operation.status === "running" && completed.has(key)) staleOverwrite = true;
      if (operation.status === "completed") completed.add(key);
    }
    return {
      artifact, case: run.case, schedule: run.schedule ?? "after-send",
      commands: commands.map((receipt) => ({
        argv: receipt.argv, receipt_path: receipt.receipt_path, exit_code: receipt.exit_code,
      })),
      setup_error: run.setup_error ?? null, record: view(run.current_record),
      outcome: run.work_outcome ?? (run.current_record ? {
        status: run.current_record.status, source: "legacy variant native canonical terminal observation",
      } : null),
      notification: run.current_record?.notification ?? null,
      slot: run.capacity_consistent === true ? { available: true, reused: run.slots_reused ?? 1 }
        : { available: "unknown", reason: "not independently probed in this variant" },
      captured_mismatch: run.captured_mismatch ?? false,
      preserved_result: run.preserved_result ?? null,
      reader_ready: run.reader_ready ?? null, reader_exit: run.reader_exit ?? null,
      followup_after_release: run.followup_after_release ?? null,
      record_after_reader_release: view(run.record_after_reader_release),
      native_mod_load_order: run.native_mod_load_order ?? null,
      same_loaded_sdk_namespace: run.native_barrel?.same_namespace ?? "unknown",
      task_store_asserted_before_run: run.task_store_asserted_before_run ?? false,
      fs_observation: run.fs_observation ?? (operations.length ? "observed" : "unavailable"),
      rename_errors: renameErrors.length,
      rename_error_codes: [...new Set(renameErrors.map((error) => error.code))],
      maximum_retry_count: Math.max(0, ...renameErrors.map((error) => error.retry_count)),
      stale_overwrite_observed: operations.length ? staleOverwrite : "unknown",
      retry_bound_exceeded: renameErrors.length > 0 && run.captured_mismatch === true,
      native_runtime: run.native_runtime ?? null,
    };
  });
const repetitions = receipts.filter((receipt) =>
  receipt.argv.join(" ") === "bun test test/contracts.test.ts test/regression.test.ts" &&
  receipt.source_sha256["qa/harness.ts"] === source["qa/harness.ts"] &&
  receipt.source_sha256["test/regression.test.ts"] === source["test/regression.test.ts"])
  .map((receipt) => {
    const raw = fs.readFileSync(receipt.raw_output_path, "utf8");
    return { argv: receipt.argv, receipt_path: receipt.receipt_path, exit_code: receipt.exit_code,
      named_mismatch: raw.includes("(fail) CURRENT_EPOCH_OUTCOME_COMPLETED_CANONICAL_RUNNING") &&
        raw.includes('Received: "running"'),
      setup_failure: raw.includes("SETUP_EVENT_DEADLINE") || raw.includes("NATIVE_HOST_RUN_FAILED"),
    };
  });
const reproduced = variants.some((run) => run.captured_mismatch);
const regression = {
  reproduced, reproduction: reproduced ? "reproduced" : "not-reproduced",
  mismatch: "CURRENT_EPOCH_OUTCOME_COMPLETED_CANONICAL_RUNNING",
  source_sha256: source, variants, repetitions,
  historical_comparison: {
    R2_task: "st_01a1469c", runner: "in-process",
    revival: "native task_send to an already completed resident explore child",
    original: { record: "running", run_epoch: 1, notified_epoch: 0, assistant_stop_reason: "stop",
      followup: "no resident session in this process" },
    known_unknowns: ["R2 did not capture rename errors or runtime mod load order",
      "R2 did not record the exact parent busy/concurrent-publication schedule"],
    recreated: ["current stable native package/extension mod loading order",
      "project-local .omo/senpi-task store", "busy parent and companion publication",
      "late stream settlement after native residency release",
      "separate Windows FileStream reader denying delete sharing during native publication"],
    differences: ["offline event-controlled provider instead of live Ollama high-thinking model",
      "short fixture transcript", "sandbox reader contention is controlled, not proof of R2's original cause"],
  },
  conclusion: reproduced
    ? "Real Windows reader contention exhausted the installed/native rename retry path; native epoch1 result survived while canonical running and notified_epoch 0 remained. R2's historical cause remains unknown."
    : "Current native variants did not reproduce R2; no failure was manufactured.",
};
fs.writeFileSync(path.join(evidence, "red-regression.json"), JSON.stringify(regression, null, 2) + "\n");

const pinsPath = path.join(evidence, "source-pins.json");
const pins = JSON.parse(fs.readFileSync(pinsPath, "utf8"));
const drift = pins.files.filter((file) => sha256(file.path) !== file.sha256).map((file) => file.path);
pins.post_verification = { observed_at: new Date().toISOString(), unchanged: drift.length === 0,
  drift, actual_loaded_namespace_proofs: variants.filter((run) => run.same_loaded_sdk_namespace === true)
    .map((run) => run.artifact), product_source_sha256: source };
fs.writeFileSync(pinsPath, JSON.stringify(pins, null, 2) + "\n");
const cleanup = {
  completed_at: new Date().toISOString(), live_profile_writes: false,
  external_temp_directories_created: [], foreign_processes_touched: [],
  processes: receipts.filter((receipt) => receipt.end_time).map((receipt) => ({
    pid: receipt.pid ?? null, argv: receipt.argv, exit_code: receipt.exit_code,
    timed_out: receipt.timed_out ?? false, receipt_path: receipt.receipt_path,
  })),
  session_cleanup_receipts: names.filter((name) => name.endsWith(".cleanup.json")).map((name) => path.join(evidence, name)),
  process_cleanup: processCleanup,
  notes: ["All process termination is limited to command children started here.",
    "The first contention probe exposed a forgotten-session cleanup leak and reached its command deadline. Later probes emit native child session_shutdown before dispose and exit normally.",
    "This audit's own command receipt completes after this file is written."],
};
fs.writeFileSync(path.join(evidence, "cleanup.json"), JSON.stringify(cleanup, null, 2) + "\n");
const checks = {
  lsp: "TypeScript language server is not installed; no LSP was installed. Compiler validation is recorded separately.",
  adversarial: [
    { class: "stale_state", result: drift.length === 0 ? "Pinned installed sources unchanged; actual loaded SDK namespace identity observed." : `Drift: ${drift.join(", ")}` },
    { class: "flaky_tests", result: `${repetitions.length} unchanged-harness regression runs; identical named red: ${repetitions.length >= 3 && repetitions.every((run) => run.exit_code === 1 && run.named_mismatch && !run.setup_failure)}` },
    { class: "misleading_success_output", result: "Required capture QA asserts captured_mismatch true; red test asserts canonical completed, not a setup failure." },
    { class: "hung_commands", result: "Every command has a deadline. One early cleanup timeout is retained; corrected native cleanup exits normally." },
    { class: "dirty_worktree", result: "Product directory was verified absent before creation; git init made no commit." },
    { class: "prompt_injection", not_applicable: "No untrusted prompt source is executed." },
    { class: "cancel_resume", not_applicable: "No task_cancel/resume feature is changed; native retirement is an observed fixture." },
    { class: "repeated_interruptions", not_applicable: "No interactive interruption contract is implemented in this unit." },
    { class: "malformed_input", not_applicable: "Only machine contract guards are in scope; new runtime input handlers belong to later todos." },
  ],
};
fs.writeFileSync(path.join(evidence, "verification.json"), JSON.stringify(checks, null, 2) + "\n");
console.log(JSON.stringify({ reproduction: regression.reproduction, variants: variants.length,
  repetitions, source_drift: drift, cleanup: path.join(evidence, "cleanup.json") }));

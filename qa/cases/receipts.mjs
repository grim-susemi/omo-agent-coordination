import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { attempt, bundle, isolatedEnv, product, senpi, sha256 } from "../command.mjs";

export const taskEvidence = path.join(attempt, "task-3");
export function hashes() {
  const files = ["package.json", "tsconfig.json", "src/contracts.ts", "src/engine-bridge.ts",
    "test/contracts.test.ts", "test/regression.test.ts", "test/engine-bridge.test.ts",
    "qa/owned.ts", "qa/harness.ts", "scripts/build-adapter.mjs"];
  for (const file of fs.readdirSync(path.join(product, "qa/cases"))) files.push(`qa/cases/${file}`);
  return Object.fromEntries(files.filter((file) => fs.existsSync(path.join(product, file)))
    .map((file) => [file, sha256(path.join(product, file))]));
}

export async function commandReceipt(argv, timeoutMs = 120_000) {
  fs.mkdirSync(taskEvidence, { recursive: true });
  const id = `${new Date().toISOString().replaceAll(/[:.]/g, "-")}-${randomUUID()}`;
  const outputPath = path.join(taskEvidence, `command-${id}.raw.txt`);
  const receiptPath = path.join(taskEvidence, `command-${id}.json`);
  const receipt = {
    argv, cwd: product, started_at: new Date().toISOString(), finished_at: null,
    exit_code: null, timeout_ms: timeoutMs, fixture_identity: "attempt-20261017-e1/task-3",
    source_sha256: hashes(), module_sha256: {
      [bundle]: sha256(bundle), [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js"),
    }, raw_output_path: outputPath, status: "running",
  };
  const resolved = argv[0] === "bun" ? ["C:/Users/USER/.bun/bin/bun.exe", ...argv.slice(1)] :
    argv[0] === "bunx" ? ["C:/Users/USER/.bun/bin/bun.exe", "x", "--no-install", ...argv.slice(1)] : argv;
  receipt.resolved_argv = resolved;
  const save = () => fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  save();
  const fd = fs.openSync(outputPath, "wx");
  const child = spawn(resolved[0], resolved.slice(1), {
    cwd: product, env: isolatedEnv(path.join(taskEvidence, "commands", id)),
    stdio: ["ignore", "pipe", "pipe"], windowsHide: true,
  });
  receipt.pid = child.pid;
  save();
  for (const stream of [child.stdout, child.stderr]) stream.on("data", (bytes) => {
    fs.writeSync(fd, bytes);
    process.stdout.write(bytes);
  });
  const deadline = setTimeout(() => { receipt.timed_out = true; child.kill(); }, timeoutMs);
  const exit = await new Promise((resolve) => {
    child.once("error", (error) => { receipt.error = String(error); resolve(127); });
    child.once("close", (code, signal) => { receipt.signal = signal; resolve(code ?? 1); });
  });
  clearTimeout(deadline);
  fs.closeSync(fd);
  receipt.exit_code = exit;
  receipt.finished_at = new Date().toISOString();
  receipt.status = "finished";
  save();
  console.log(JSON.stringify({ receipt: receiptPath, exit_code: exit }));
  return { exit, receiptPath, outputPath };
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(import.meta.filename)) {
  const repeat = process.argv[2] === "--repeat" ? Number(process.argv[3]) : 1;
  if (!Number.isSafeInteger(repeat) || repeat < 1 || repeat > 3) throw new Error("repeat must be 1..3");
  const argv = process.argv.slice(process.argv[2] === "--repeat" ? 4 : 2);
  const exits = [];
  for (let index = 0; index < repeat; index++) exits.push((await commandReceipt(argv)).exit);
  console.log(JSON.stringify({ repeat_exit_codes: exits }));
  process.exitCode = exits.find((exit) => exit !== 0) ?? 0;
}

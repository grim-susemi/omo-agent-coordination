import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { attempt, product, bundle, senpi, isolatedEnv, sha256, priorPreload } from "../command.mjs";

export const inspectEvidence = path.join(attempt, "task-4");
export function inspectHashes() {
  const files = ["package.json", "tsconfig.json", "bun.lock", "scripts/build-adapter.mjs", "qa/owned.ts"];
  for (const directory of ["src", "test", "qa/cases"]) {
    for (const name of fs.readdirSync(path.join(product, directory))) {
      if (fs.statSync(path.join(product, directory, name)).isFile()) files.push(`${directory}/${name}`);
    }
  }
  return Object.fromEntries(files.sort().map((file) => [file, sha256(path.join(product, file))]));
}
export async function inspectCommand(argv: string[], cwd = product, preload?: string) {
  preload = preload?.replaceAll("\\", "/");
  const id = `${new Date().toISOString().replaceAll(/[:.]/g, "-")}-${randomUUID()}`;
  const raw = path.join(inspectEvidence, `command-${id}.raw.txt`);
  const receiptPath = path.join(inspectEvidence, `command-${id}.json`);
  fs.mkdirSync(inspectEvidence, { recursive: true });
  const resolved = argv[0] === "bunx" ? ["C:/Users/USER/.bun/bin/bun.exe", "x", "--no-install", ...argv.slice(1)]
    : argv[0] === "bun" ? ["C:/Users/USER/.bun/bin/bun.exe", ...argv.slice(1)] : [...argv];
  if (preload && argv[0] === "bun") {
    if (argv[1] === "run" && argv[2] === "qa:owned") {
      resolved.splice(1, resolved.length - 1, "run", "--preload", preload, "qa/owned.ts", ...argv.slice(3));
    } else resolved.splice(2, 0, "--preload", preload);
  }
  const receipt = { argv, resolved_argv: resolved, cwd, started_at: new Date().toISOString(),
    finished_at: null as string | null, exit_code: null as number | null,
    timeout_ms: 240_000, timed_out: false, source_sha256: inspectHashes(),
    module_sha256: { [bundle]: sha256(bundle),
      [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js") },
    raw_output_path: raw, fixture_identity: "task-4 actual native loader/owner fixtures",
    pid: undefined as number | undefined, error: undefined as string | undefined };
  const save = () => fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  const env = isolatedEnv(path.join(inspectEvidence, "commands", id));
  // Keep containment, but never inherit another suite's task-local allowlist.
  const guard = preload ?? priorPreload("task-2/write-guard.cjs", "task-4/write-guard.cjs");
  env.NODE_OPTIONS = `--require="${guard}"`;
  const fd = fs.openSync(raw, "wx");
  save();
  const child = spawn(resolved[0]!, resolved.slice(1), {
    cwd, env, windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
  });
  receipt.pid = child.pid;
  save();
  for (const stream of [child.stdout, child.stderr]) stream.on("data", (bytes) => {
    fs.writeSync(fd, bytes);
    process.stdout.write(bytes);
  });
  const timer = setTimeout(() => { receipt.timed_out = true; child.kill(); }, receipt.timeout_ms);
  const code = await new Promise<number>((resolve) => {
    child.once("error", (error) => { receipt.error = String(error); });
    child.once("close", (exit) => resolve(exit ?? 127));
  });
  clearTimeout(timer);
  fs.closeSync(fd);
  receipt.exit_code = code;
  receipt.finished_at = new Date().toISOString();
  save();
  console.log(JSON.stringify({ receipt: receiptPath, exit_code: code }));
  return { exit: code, receiptPath, raw };
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  process.exitCode = (await inspectCommand(args)).exit;
}

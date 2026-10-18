import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import { attempt, product, senpi, bundle, isolatedEnv, sha256 } from "../command.mjs";

export const channelEvidence = path.join(attempt, "task-5");
export function channelHashes() {
  const files = ["package.json", "tsconfig.json", "bun.lock"];
  for (const directory of ["src", "test", "qa", "scripts"]) {
    for (const entry of fs.readdirSync(path.join(product, directory), { recursive: true, withFileTypes: true })) {
      if (entry.isFile()) files.push(path.relative(product, path.join(entry.parentPath, entry.name)).replaceAll("\\", "/"));
    }
  }
  return Object.fromEntries(files.map((file) => [file, sha256(path.join(product, file))]));
}
export async function channelCommand(argv: string[], timeoutMs = 120_000) {
  fs.mkdirSync(channelEvidence, { recursive: true });
  const id = `${new Date().toISOString().replaceAll(/[:.]/g, "-")}-${randomUUID()}`;
  const raw = path.join(channelEvidence, `command-${id}.raw.txt`);
  const receiptPath = path.join(channelEvidence, `command-${id}.json`);
  const guard = path.join(channelEvidence, "write-guard.cjs").replaceAll("\\", "/");
  let resolved = argv[0] === "bun" ? ["C:/Users/USER/.bun/bin/bun.exe", ...argv.slice(1)] :
    argv[0] === "bunx" ? ["C:/Users/USER/.bun/bin/bun.exe", "x", "--no-install", ...argv.slice(1)] : argv;
  if (argv[0] === "bun" && ["test", "run"].includes(argv[1]!)) {
    if (argv[1] === "run" && argv[2] === "qa:owned") {
      resolved = [resolved[0]!, "run", "--preload", guard, "qa/owned.ts", ...argv.slice(3)];
    } else resolved.splice(2, 0, "--preload", guard);
  }
  const receipt = {
    argv, resolved_argv: resolved, cwd: product, start_time: new Date().toISOString(),
    end_time: null as string | null, exit_code: null as number | null, timeout_ms: timeoutMs,
    source_sha256: channelHashes(), module_sha256: {
      [bundle]: sha256(bundle), [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js"),
    }, raw_output_path: raw, fixture_identity: "attempt-20261017-e1/task-5",
    pid: undefined as number | undefined, timed_out: false, error: undefined as string | undefined,
  };
  const save = () => fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  const fd = fs.openSync(raw, "wx");
  save();
  const env = isolatedEnv(path.join(channelEvidence, "commands", id));
  env.NODE_OPTIONS = `--require="${guard}"`;
  const child = spawn(resolved[0]!, resolved.slice(1), {
    cwd: product, env, windowsHide: true, stdio: ["ignore", "pipe", "pipe"],
  });
  receipt.pid = child.pid;
  save();
  for (const stream of [child.stdout, child.stderr]) stream.on("data", (bytes) => {
    fs.writeSync(fd, bytes);
    process.stdout.write(bytes);
  });
  const deadline = setTimeout(() => { receipt.timed_out = true; child.kill(); }, timeoutMs);
  const exit = await new Promise<number>((resolve) => {
    child.once("error", (error) => { receipt.error = String(error); resolve(127); });
    child.once("close", (code) => resolve(code ?? 1));
  });
  clearTimeout(deadline);
  fs.closeSync(fd);
  receipt.exit_code = exit;
  receipt.end_time = new Date().toISOString();
  save();
  console.log(JSON.stringify({ receipt: receiptPath, exit_code: exit }));
  return { exit, receiptPath, raw };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const repeat = args[0] === "--repeat" ? Number(args[1]) : 1;
  if (!Number.isSafeInteger(repeat) || repeat < 1 || repeat > 3) throw new Error("invalid repeat");
  const argv = args.slice(args[0] === "--repeat" ? 2 : 0);
  for (let i = 0; i < repeat; i++) {
    const { exit } = await channelCommand(argv);
    if (exit !== 0) process.exitCode = exit;
  }
}

import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { attempt, product, bundle, senpi, sha256, isolatedEnv } from "../command.mjs";

export const task9 = path.join(attempt, "task-9");
export function hashes(): Record<string, string> {
  const files = ["package.json", "README.md", "INSTALL.md", "LICENSE"];
  for (const directory of ["src", "test", "qa", "scripts"]) {
    for (const file of fs.readdirSync(path.join(product, directory), { recursive: true })) {
      const name = `${directory}/${String(file).replaceAll("\\", "/")}`;
      if (/\.(?:ts|mjs|js)$/.test(name) && fs.statSync(path.join(product, name)).isFile()) files.push(name);
    }
  }
  return Object.fromEntries(files.filter((file) => fs.existsSync(path.join(product, file)))
    .map((file) => [file, sha256(path.join(product, file))]));
}
export async function runCommand(argv: string[], label: string, extra: Record<string, string> = {}) {
  const id = `${label}-${randomUUID()}`;
  const directory = path.join(task9, "commands", id);
  const env = isolatedEnv(directory);
  const metadata = path.join(directory, "sdk-metadata");
  fs.mkdirSync(metadata);
  fs.copyFileSync(path.join(senpi, "package.json"), path.join(metadata, "package.json"));
  for (const key of ["SENPI_PACKAGE_DIR", "PI_PACKAGE_DIR", "OMO_PACKAGE_DIR"]) env[key] = metadata;
  if (process.env.NODE_OPTIONS) env.NODE_OPTIONS = process.env.NODE_OPTIONS;
  Object.assign(env, extra);
  const resolved = argv[0] === "bun" ? ["C:/Users/USER/.bun/bin/bun.exe", ...argv.slice(1)] : argv;
  const raw = path.join(task9, `${id}.raw.txt`), receiptPath = path.join(task9, `${id}.json`);
  const receipt: Record<string, any> = { argv, resolved_argv: resolved, started_at: new Date().toISOString(),
    finished_at: null, exit_code: null, cwd: product, timeout_ms: 55_000,
    source_sha256: hashes(), module_sha256: {
      [bundle]: sha256(bundle), [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js"),
    }, raw_output_path: raw, fixture_identity: id };
  const fd = fs.openSync(raw, "wx");
  const child = spawn(resolved[0]!, resolved.slice(1), { cwd: product, env, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  receipt.pid = child.pid;
  fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  let output = "";
  for (const stream of [child.stdout!, child.stderr!]) stream.on("data", (data: Buffer) => {
    fs.writeSync(fd, data); output += data.toString();
  });
  const timer = setTimeout(() => { receipt.timed_out = true; child.kill(); }, 55_000);
  const code = await new Promise<number>((resolve) => {
    child.once("error", (error) => { receipt.error = String(error); resolve(127); });
    child.once("close", (code, signal) => { receipt.signal = signal; resolve(code ?? 1); });
  });
  clearTimeout(timer); fs.closeSync(fd);
  receipt.exit_code = code; receipt.finished_at = new Date().toISOString();
  fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  return { code, output, receipt: receiptPath };
}
export function save(name: string, value: unknown) {
  fs.mkdirSync(task9, { recursive: true });
  fs.writeFileSync(path.join(task9, `${name}.json`), JSON.stringify(value, null, 2) + "\n");
}

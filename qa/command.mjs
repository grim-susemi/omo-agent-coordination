import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const product = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const attempt = path.join(product, ".omo/evidence/agent-coordination/attempt-5138-rebuild-e1");
export const priorAttempt = path.join(product, ".omo/evidence/agent-coordination/attempt-20261017-e1");
export const evidence = path.join(attempt, "task-1");
export const senpi = "C:/Users/USER/.bun/install/global/node_modules/@code-yeongyu/senpi";
export const omo = "C:/Users/USER/.bun/install/global/node_modules/omo-ai";
export const installedBundle = path.join(omo, "plugin/extensions/omo-task.js");
// QA always builds from preserved baseline bytes, even after the live carrier is installed.
export const bundle = path.join(attempt, "baseline-5140/omo-task.js");
export const sha256 = (file) => createHash("sha256").update(fs.readFileSync(file)).digest("hex");
export function priorPreload(relative, outputRelative = relative) {
  const source = path.join(priorAttempt, relative);
  const output = path.join(attempt, outputRelative);
  // Legacy guards log beside __filename. Read their source, but execute with
  // the current output filename so neither logs nor sandbox writes touch prior evidence.
  fs.mkdirSync(path.dirname(output), { recursive: true });
  if (!fs.existsSync(output)) fs.writeFileSync(output,
    `const Module = require("node:module");\nconst fixture = new Module(__filename, module);\n` +
    `fixture.filename = __filename;\nfixture.paths = module.paths;\n` +
    `fixture._compile(require("node:fs").readFileSync(${JSON.stringify(source)}, "utf8"), __filename);\n`,
    { flag: "wx" });
  return output.replaceAll("\\", "/");
}
export function sourceHashes() {
  const files = ["package.json", "tsconfig.json", "src/contracts.ts", "test/contracts.test.ts",
    "test/regression.test.ts", "qa/harness.ts", "qa/command.mjs", "qa/probe.mjs",
    "qa/owned.ts", "qa/pins.mjs", "qa/reader.ps1", "qa/audit.mjs", "bun.lock"];
  return Object.fromEntries(files.filter((f) => fs.existsSync(path.join(product, f)))
    .map((f) => [f, sha256(path.join(product, f))]));
}
export function isolatedEnv(sandbox) {
  /** @type {Record<string, string>} */
  const env = {};
  for (const key of ["PATH", "Path", "SystemRoot", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT"]) {
    if (process.env[key] !== undefined) env[key] = process.env[key];
  }
  for (const [name, suffix] of Object.entries({
    HOME: "home", USERPROFILE: "home", APPDATA: "appdata", LOCALAPPDATA: "localappdata",
    TMP: "tmp", TEMP: "tmp", TMPDIR: "tmp", XDG_CONFIG_HOME: "config",
    XDG_CACHE_HOME: "cache", XDG_STATE_HOME: "state", BUN_INSTALL_CACHE_DIR: "bun-cache",
    OMO_CODING_AGENT_DIR: "agent", SENPI_CODING_AGENT_DIR: "agent", PI_CODING_AGENT_DIR: "agent",
    OMO_TASK_STATE_DIR: "senpi-task", OMO_RPC_DIR: "endpoints", SENPI_RPC_DIR: "endpoints",
  })) {
    const dir = path.join(sandbox, suffix);
    fs.mkdirSync(dir, { recursive: true });
    env[name] = dir;
  }
  env.OMO_SENPI_QA = "1";
  env.OMO_TELEMETRY_DISABLED = "1";
  env.DO_NOT_TRACK = "1";
  const pathKey = Object.keys(env).find((key) => key.toLowerCase() === "path") ?? "PATH";
  const powershell = path.join(process.env.SystemRoot, "System32/WindowsPowerShell/v1.0");
  env[pathKey] = `C:/Users/USER/.bun/bin;C:/Users/USER/.bun/install/global/node_modules/.bin;${powershell};${env[pathKey] ?? ""}`;
  env.COORDINATION_ATTEMPT_DIR = attempt;
  return env;
}

/**
 * @param {string[]} argv
 * @param {{cwd?: string, timeoutMs?: number,
 * onStdout?: (data: Buffer) => void,
 * onSpawn?: (child: import("node:child_process").ChildProcess) => void}} options
 */
export async function command(argv, { cwd = product, timeoutMs = 120_000, onStdout, onSpawn } = {}) {
  fs.mkdirSync(evidence, { recursive: true });
  const id = `${new Date().toISOString().replaceAll(/[:.]/g, "-")}-${process.pid}`;
  const receiptPath = path.join(evidence, `command-${id}.json`);
  const raw = path.join(evidence, `command-${id}.raw.txt`);
  const receipt = {
    argv, cwd, start_time: new Date().toISOString(), end_time: null, exit_code: null,
    timeout_ms: timeoutMs, source_sha256: sourceHashes(),
    module_sha256: { [bundle]: sha256(bundle), [senpi + "/dist/core/agent-session.js"]: sha256(senpi + "/dist/core/agent-session.js") },
    raw_output_path: raw, fixture_identity: path.relative(product, evidence), status: "running",
  };
  const resolved = argv[0] === "bun" ? ["C:/Users/USER/.bun/bin/bun.exe", ...argv.slice(1)]
    : argv[0] === "bunx" ? ["C:/Users/USER/.bun/bin/bun.exe", "x", ...argv.slice(1)] : argv;
  receipt.resolved_argv = resolved;
  const save = () => fs.writeFileSync(receiptPath, JSON.stringify(receipt, null, 2) + "\n");
  save();
  const fd = fs.openSync(raw, "wx");
  const child = spawn(resolved[0], resolved.slice(1), {
    cwd, env: isolatedEnv(path.join(attempt, "commands", id)),
    windowsHide: true, stdio: [onSpawn ? "pipe" : "ignore", "pipe", "pipe"],
  });
  receipt.pid = child.pid;
  save();
  onSpawn?.(child);
  child.stdout.on("data", (data) => onStdout?.(data));
  for (const stream of [child.stdout, child.stderr]) stream.on("data", (data) => {
    fs.writeSync(fd, data);
    process.stdout.write(data);
  });
  const timer = setTimeout(() => {
    receipt.timed_out = true;
    child.kill();
  }, timeoutMs);
  const code = await new Promise((resolve) => {
    child.once("error", (error) => { receipt.error = String(error); resolve(127); });
    child.once("close", (code, signal) => { receipt.signal = signal; resolve(code ?? 1); });
  });
  clearTimeout(timer);
  fs.closeSync(fd);
  receipt.exit_code = code;
  receipt.end_time = new Date().toISOString();
  receipt.status = "finished";
  save();
  console.log(JSON.stringify({ receipt: receiptPath, exit_code: code }));
  return code;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv[2] === "--repeat") {
    const count = Number(process.argv[3]);
    if (!Number.isInteger(count) || count < 1 || count > 3) throw new Error("repeat count must be 1..3");
    const exits = [];
    for (let n = 0; n < count; n++) exits.push(await command(process.argv.slice(4)));
    console.log(JSON.stringify({ repeat_exit_codes: exits }));
    process.exitCode = exits.find((exit) => exit !== 0) ?? 0;
  } else {
    process.exitCode = await command(process.argv.slice(2));
  }
}

import { expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";
import { attempt, isolatedEnv, product } from "../qa/command.mjs";

test("native model loading uses a removed disposable copy outside evidence and leaves the live file unchanged", async () => {
  const sandbox = path.join(attempt, "task-6", "runs", `model-config-regression-${randomUUID()}`);
  const directory = path.join(sandbox, "question-package");
  fs.mkdirSync(directory, { recursive: true });
  const built = await Bun.build({ entrypoints: ["src/extension.ts", "qa/cases/question-fixture.ts",
    "qa/cases/question-consumer.ts"].map(file => path.join(product, file)), outdir: directory,
    naming: "[name].js", target: "node", format: "esm",
    external: ["@code-yeongyu/senpi", "@earendil-works/pi-ai"] });
  expect(built.success).toBe(true);
  const api = await Bun.build({ entrypoints: [path.join(product, "src/public-api.ts")],
    outdir: path.join(directory, "dist"), naming: "public-api.js", target: "node", format: "esm" });
  expect(api.success).toBe(true);
  fs.writeFileSync(path.join(directory, "package.json"), JSON.stringify({ type: "module",
    name: "coordination-question-owned-candidate", exports: { "./api": "./dist/public-api.js" },
    pi: { extensions: ["./extension.js", "./question-fixture.js", "./question-consumer.js"] } }));
  const link = path.join(directory, "node_modules"), reportPath = path.join(sandbox, "result.json");
  fs.symlinkSync("C:/Users/USER/.bun/install/global/node_modules", link, "junction");
  let output = "";
  try {
    const child = spawn("node", ["--import", pathToFileURL(path.join(product, "qa/model-config-probe.ts")).href,
      path.join(product, "qa/cases/question-native.ts"), "--native-case", "question-model-inventory",
      "--sandbox", sandbox, "--result", reportPath], { cwd: product, env: isolatedEnv(sandbox),
      windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
    child.stdout.on("data", bytes => { output += bytes; });
    child.stderr.on("data", bytes => { output += bytes; });
    const timer = setTimeout(() => child.kill(), 55_000);
    let exit;
    try { exit = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject); child.once("close", resolve);
    }); } finally { clearTimeout(timer); }
    expect(exit, output).toBe(0);
    const report = JSON.parse(fs.readFileSync(reportPath, "utf8"));
    expect(report.passed).toBe(true);
    expect(report.model_config.live_after).toEqual(report.model_config.live_before);
    expect(report.model_config.disposable_removed).toBe(true);
    expect(fs.existsSync(report.model_config.disposable_root)).toBe(false);
    expect(path.dirname(report.model_config.loader_models_path)).toBe(report.model_config.disposable_root);
  } finally { fs.unlinkSync(link); }
}, 60_000);

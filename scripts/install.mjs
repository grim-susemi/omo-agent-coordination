import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { BASELINE, BASELINES, buildAdapter } from "./build-adapter.mjs";
import { attempt, product } from "../qa/command.mjs";

const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const json = (value) => Buffer.from(JSON.stringify(value, null, 2) + "\n");
const packageName = "piomo-agent-coordination-mod";
function inside(root, file) {
  const relative = path.relative(root, file);
  return relative !== ".." && !relative.startsWith(".." + path.sep) && !path.isAbsolute(relative);
}
function safe(root, file) {
  const absolute = path.resolve(file);
  if (!inside(root, absolute)) throw new Error("PATH_OUTSIDE_AGENT_DIR");
  let current = absolute;
  while (inside(root, current)) {
    if (fs.existsSync(current) && fs.lstatSync(current).isSymbolicLink()) throw new Error("SYMLINK_REFUSED");
    if (current === root) break;
    current = path.dirname(current);
  }
  return absolute;
}
function locations(agentDir) {
  if (!agentDir) throw new Error("AGENT_DIR_REQUIRED");
  const root = path.resolve(agentDir);
  // This candidate is authorized for disposable installation only. A future
  // live installation is a separate reviewed action, not a default CLI path.
  if (root === path.resolve(attempt) || !inside(path.resolve(attempt), root)) throw new Error("DISPOSABLE_AGENT_DIR_REQUIRED");
  safe(path.resolve(attempt), root);
  return { root, settings: safe(root, path.join(root, "settings.json")),
    directory: safe(root, path.join(root, "packages", packageName)),
    state: safe(root, path.join(root, ".agent-coordination-install")) };
}
function tree(directory) {
  if (!fs.existsSync(directory)) return [];
  const files = [];
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error("SYMLINK_REFUSED");
    if (entry.isDirectory()) files.push(...tree(full));
    else if (entry.isFile()) files.push(full);
    else throw new Error("NON_REGULAR_FILE_REFUSED");
  }
  return files.sort();
}
function expectHash(file, hash, reason) {
  if (!fs.existsSync(file) || !fs.statSync(file).isFile() || digest(fs.readFileSync(file)) !== hash) throw new Error(reason);
}
function expectBaseline(file, reason) {
  if (!fs.existsSync(file) || !fs.statSync(file).isFile()) throw new Error(reason);
  const hash = digest(fs.readFileSync(file));
  if (!BASELINES[hash]) throw new Error(reason);
  return hash;
}
function installedPlan(paths) {
  const receiptPath = safe(paths.root, path.join(paths.state, "receipt.json"));
  const seal = safe(paths.root, path.join(paths.state, "receipt.sha256"));
  if (!fs.existsSync(receiptPath) || !fs.existsSync(seal)) throw new Error("RECEIPT_MISSING_REFUSED");
  expectHash(receiptPath, fs.readFileSync(seal, "utf8").trim(), "RECEIPT_HASH_REFUSED");
  const receipt = JSON.parse(fs.readFileSync(receiptPath, "utf8"));
  if (receipt.version !== 1 || receipt.agent_dir !== paths.root || receipt.package_dir !== paths.directory) throw new Error("RECEIPT_IDENTITY_REFUSED");
  const files = Object.keys(receipt.package_sha256).map((relative) => safe(paths.root, path.join(paths.directory, relative)));
  if (JSON.stringify(tree(paths.directory)) !== JSON.stringify([...files].sort())) throw new Error("PACKAGE_INVENTORY_DRIFT_REFUSED");
  for (const [relative, hash] of Object.entries(receipt.package_sha256)) {
    expectHash(safe(paths.directory, path.join(paths.directory, relative)), hash, "PACKAGE_DRIFT_REFUSED");
  }
  expectHash(paths.settings, receipt.settings_after_sha256, "SETTINGS_DRIFT_REFUSED");
  const settingsBackup = safe(paths.root, path.join(paths.state, "settings.original"));
  if (receipt.settings_before_sha256 !== null) expectHash(settingsBackup, receipt.settings_before_sha256, "SETTINGS_BACKUP_REFUSED");
  if (receipt.adapter) {
    const target = safe(paths.root, receipt.adapter.target);
    const backup = safe(paths.root, path.join(paths.state, "adapter", "omo-task.js.baseline"));
    expectHash(target, receipt.adapter.candidate_sha256, "ADAPTER_DRIFT_REFUSED");
    expectHash(backup, receipt.adapter.baseline_sha256 ?? BASELINE, "ADAPTER_BACKUP_REFUSED");
    const manifest = safe(paths.root, path.join(paths.state, "adapter", "omo-task.js.manifest.json"));
    expectHash(manifest, receipt.adapter.manifest_sha256, "ADAPTER_MANIFEST_REFUSED");
  }
  return receipt;
}

/** Validate every restore source and every removal target before the first
 * write. Missing/corrupt backups are errors, never permission to delete.
 * @param {{agentDir?: string, action?: string, dryRun?: boolean, adapterTarget?: string}} options
 */
export async function install({ agentDir, action = "install", dryRun = false, adapterTarget } = {}) {
  if (!["install", "uninstall", "rollback"].includes(action)) throw new Error("INVALID_ACTION");
  const paths = locations(agentDir);
  if (action !== "install") {
    const receipt = installedPlan(paths);
    const result = { action, dry_run: dryRun, agent_dir: paths.root, package_dir: paths.directory,
      restored_adapter_sha256: receipt.adapter ? receipt.adapter.baseline_sha256 ?? BASELINE : null, restored_settings_sha256: receipt.settings_before_sha256 };
    if (dryRun) return result;
    // All input bytes are read before destructive work, including settings that
    // originally did not exist. The receipt stays until restoration completes.
    const settings = receipt.settings_before_sha256 === null ? null
      : fs.readFileSync(path.join(paths.state, "settings.original"));
    const adapter = receipt.adapter ? fs.readFileSync(path.join(paths.state, "adapter", "omo-task.js.baseline")) : null;
    if (adapter) fs.writeFileSync(receipt.adapter.target, adapter);
    if (settings) fs.writeFileSync(paths.settings, settings);
    else fs.unlinkSync(paths.settings);
    fs.rmSync(paths.directory, { recursive: true });
    fs.rmSync(paths.state, { recursive: true });
    return result;
  }
  if (fs.existsSync(paths.state) || fs.existsSync(paths.directory)) throw new Error("EXISTING_INSTALL_REFUSED");
  const before = fs.existsSync(paths.settings) ? fs.readFileSync(paths.settings) : null;
  const settings = before ? JSON.parse(before.toString()) : {};
  if (!settings || typeof settings !== "object" || Array.isArray(settings) ||
      settings.packages !== undefined && !Array.isArray(settings.packages)) throw new Error("SETTINGS_SHAPE_REFUSED");
  const target = adapterTarget ? safe(paths.root, path.resolve(adapterTarget)) : null;
  const baseline = target ? expectBaseline(target, "BUNDLE_DRIFT_REFUSED") : null;
  const after = json({ ...settings, packages: [...(settings.packages ?? []), paths.directory] });
  const manifest = { name: packageName, version: "0.0.0", type: "module", private: true,
    peerDependencies: { "@code-yeongyu/senpi": "*" },
    pi: { extensions: ["./dist/extension.js"] },
    exports: { "./api": { types: "./src/public-api.ts", import: "./dist/public-api.js" } } };
  const result = { action, dry_run: dryRun, agent_dir: paths.root, package_dir: paths.directory,
    pi: manifest.pi, adapter_target: target, adapter_baseline_sha256: baseline,
    settings_before_sha256: before ? digest(before) : null, settings_after_sha256: digest(after) };
  if (dryRun) return result;
  if (typeof Bun === "undefined") throw new Error("BUN_REQUIRED");
  const output = await Bun.build({
    entrypoints: [path.join(product, "src/extension.ts"), path.join(product, "src/public-api.ts")],
    target: "node", format: "esm", naming: "[name].js", external: ["@code-yeongyu/senpi"], write: false,
  });
  if (!output.success || output.outputs.length !== 2) throw new Error(`BUILD_FAILED: ${output.logs.join("\n")}`);
  const bytes = new Map([["package.json", json(manifest)]]);
  for (const file of output.outputs) bytes.set("dist/" + path.basename(file.path), Buffer.from(await file.arrayBuffer()));
  for (const file of ["src/public-api.ts", "src/contracts.ts", "README.md", "INSTALL.md", "LICENSE"]) {
    bytes.set(file, fs.readFileSync(path.join(product, file)));
  }
  fs.mkdirSync(paths.state, { recursive: true });
  if (before) fs.writeFileSync(path.join(paths.state, "settings.original"), before, { flag: "wx" });
  let adapter = null;
  if (target) {
    const built = buildAdapter({ input: target, output: path.join(paths.state, "adapter", "omo-task.js") });
    // The real loader resolves SDK imports; no dependency copy/junction is shipped.
    fs.unlinkSync(path.join(paths.state, "adapter", "node_modules"));
    adapter = { target, candidate_sha256: built.candidate_sha256, baseline_sha256: baseline,
      manifest_sha256: digest(fs.readFileSync(built.output + ".manifest.json")) };
  }
  for (const [file, content] of bytes) {
    const destination = safe(paths.root, path.join(paths.directory, file));
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    fs.writeFileSync(destination, content, { flag: "wx" });
  }
  const receipt = { version: 1, agent_dir: paths.root, package_dir: paths.directory,
    installed_at: new Date().toISOString(), settings_before_sha256: result.settings_before_sha256,
    settings_after_sha256: result.settings_after_sha256, adapter,
    package_sha256: Object.fromEntries([...bytes].map(([f, data]) => [f, digest(data)])) };
  const receiptBytes = json(receipt);
  fs.writeFileSync(path.join(paths.state, "receipt.json"), receiptBytes, { flag: "wx" });
  fs.writeFileSync(path.join(paths.state, "receipt.sha256"), digest(receiptBytes) + "\n", { flag: "wx" });
  // Recheck the external boundaries after building, before their first write.
  if (before) expectHash(paths.settings, digest(before), "SETTINGS_CHANGED_DURING_BUILD");
  else if (fs.existsSync(paths.settings)) throw new Error("SETTINGS_CHANGED_DURING_BUILD");
  if (target) {
    if (expectBaseline(target, "BUNDLE_DRIFT_REFUSED") !== baseline) throw new Error("BUNDLE_DRIFT_REFUSED");
    fs.copyFileSync(path.join(paths.state, "adapter", "omo-task.js"), target);
  }
  fs.writeFileSync(paths.settings, after);
  return { ...result, receipt: path.join(paths.state, "receipt.json"), package_sha256: receipt.package_sha256 };
}

if (path.resolve(process.argv[1] ?? "") === fileURLToPath(import.meta.url)) {
  try {
    const args = process.argv.slice(2);
    const value = (flag) => args.includes(flag) ? args[args.indexOf(flag) + 1] : undefined;
    const action = args.find((arg) => ["install", "uninstall", "rollback"].includes(arg)) ?? "install";
    const result = await install({ action, agentDir: value("--agent-dir"),
      adapterTarget: value("--adapter-target"), dryRun: args.includes("--dry-run") });
    console.log(JSON.stringify({ ok: true, ...result }));
  } catch (error) {
    console.error(JSON.stringify({ ok: false, reason: error.message }));
    process.exitCode = 1;
  }
}

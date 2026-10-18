import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";
import { install } from "../../scripts/install.mjs";
import { product, bundle, senpi, sha256 } from "../command.mjs";
import { task9, runCommand, save, hashes } from "./install-receipts.ts";

export function installationFixture() {
  const directory = path.join(task9, "runs", `install-${randomUUID()}`);
  const agentDir = path.join(directory, "agent");
  const target = path.join(agentDir, "native/omo-task.js");
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.copyFileSync(bundle, target);
  const settings = Buffer.from(JSON.stringify({
    packages: [], extensions: [], defaultModel: "unmodified-user-model", retry: { enabled: false },
    compaction: { enabled: false }, sessionTitle: { enabled: false },
  }, null, 3) + "\r\n");
  fs.writeFileSync(path.join(agentDir, "settings.json"), settings);
  fs.mkdirSync(path.join(agentDir, "tasks"));
  fs.writeFileSync(path.join(agentDir, "tasks", "unrelated.json"), '{"sentinel":"not mod-owned"}\n');
  return { directory, agentDir, target, settings, original: sha256(target) };
}
export function snapshot(directory: string): Record<string, string> {
  return Object.fromEntries(fs.readdirSync(directory, { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile()).map((entry) => {
      const full = path.join(entry.parentPath, entry.name);
      return [path.relative(directory, full), sha256(full)];
    }).sort(([a], [b]) => a!.localeCompare(b!)));
}
export async function runInstallCase(name: "install-roundtrip" | "install-drift"): Promise<Record<string, any>> {
  if (name === "install-drift") return (await import("./install-drift.ts")).runDrift();
  const fixture = installationFixture();
  const before = snapshot(fixture.agentDir);
  const args = { agentDir: fixture.agentDir, adapterTarget: fixture.target };
  const dry = await install({ ...args, dryRun: true });
  assert.deepEqual(snapshot(fixture.agentDir), before, "DRY_RUN_MUTATED_AGENT");
  const cli = (action: string, flags: string[] = []) => runCommand(["bun", "scripts/install.mjs", action,
    "--agent-dir", fixture.agentDir, ...(action === "install" ? ["--adapter-target", fixture.target] : []), ...flags], action);
  const installed = await cli("install");
  assert.equal(installed.code, 0, installed.output);
  assert.notEqual(sha256(fixture.target), fixture.original);
  const packageDir = path.join(fixture.agentDir, "packages/piomo-agent-coordination-mod");
  const manifest = JSON.parse(fs.readFileSync(path.join(packageDir, "package.json"), "utf8"));
  assert.deepEqual(manifest.pi.extensions, ["./dist/extension.js"]);
  assert.equal(manifest.exports["./api"].types, "./src/public-api.ts");
  const result = path.join(fixture.directory, "loader.json");
  const loaded = await runCommand(["node", fileURLToPath(import.meta.url), "--native",
    fixture.agentDir, result], "installed-loader");
  assert.equal(loaded.code, 0, loaded.output);
  const loader = JSON.parse(fs.readFileSync(result, "utf8"));
  assert(loader.passed && loader.pure_helper && loader.single_esm_entry, JSON.stringify(loader));
  const current = snapshot(fixture.agentDir);
  const removalDry = await cli("uninstall", ["--dry-run"]);
  assert.equal(removalDry.code, 0, removalDry.output);
  assert.deepEqual(snapshot(fixture.agentDir), current);
  const removed = await cli("uninstall");
  assert.equal(removed.code, 0, removed.output);
  assert.deepEqual(snapshot(fixture.agentDir), before, "UNINSTALL_DID_NOT_RESTORE_ORIGINAL_BYTES");
  const report = { case: name, passed: true, sandbox: fixture.directory, source_sha256: hashes(),
    candidate_extracted: true, dry_run: dry, manifest, single_esm_entry: true,
    pure_helper: loader.pure_helper, actual_loader: loader,
    settings_restored: fs.readFileSync(path.join(fixture.agentDir, "settings.json")).equals(fixture.settings),
    adapter_restored: sha256(fixture.target) === fixture.original,
    receipts: [installed.receipt, loaded.receipt, removalDry.receipt, removed.receipt],
    cleanup: { installed_package_removed: !fs.existsSync(packageDir), native_sessions_disposed: loader.disposed } };
  save("package-manifest", { manifest, package_sha256: JSON.parse(installed.output.trim()).package_sha256 });
  save("install", report);
  return report;
}

async function nativeLoader(agentDir: string, artifact: string) {
  assert(path.resolve(agentDir).startsWith(path.resolve(task9) + path.sep));
  const sdk = await import(pathToFileURL(senpi + "/dist/index.js").href);
  const key = Symbol.for("omo.agent-coordination.capture.v1");
  const global = globalThis as any;
  const before = sdk.AgentSession.prototype.bindExtensions;
  const nativeBefore = global[Symbol.for("omo.agent-coordination.native.v1")];
  const packageDir = path.join(agentDir, "packages/piomo-agent-coordination-mod");
  const helperDirectory = path.join(path.dirname(agentDir), "helper-consumer");
  fs.mkdirSync(path.join(helperDirectory, "node_modules"), { recursive: true });
  const link = path.join(helperDirectory, "node_modules/piomo-agent-coordination-mod");
  fs.symlinkSync(packageDir, link, "junction");
  const consumer = path.join(helperDirectory, "consumer.mjs");
  fs.writeFileSync(consumer, 'export { getCoordinationBroker, getCoordinationClient } from "piomo-agent-coordination-mod/api";\n');
  const helper = await import(pathToFileURL(consumer).href);
  assert.equal(sdk.AgentSession.prototype.bindExtensions, before);
  assert.equal(global[key], undefined);
  assert.equal(global[Symbol.for("omo.agent-coordination.native.v1")], nativeBefore);
  assert.deepEqual(helper.getCoordinationBroker(), { ok: false, reason: "coordination_unavailable" });
  const cwd = path.join(path.dirname(agentDir), "work"); fs.mkdirSync(cwd);
  const loader = new sdk.DefaultResourceLoader({ cwd, agentDir, noSkills: true, noThemes: true,
    noPromptTemplates: true, noContextFiles: true, settingsManager: sdk.SettingsManager.create(cwd, agentDir) });
  let session: any;
  try {
    await loader.reload();
    const loaded = loader.getExtensions();
    assert.deepEqual(loaded.errors, []);
    const entry = path.join(packageDir, "dist/extension.js");
    assert(loaded.extensions.some((e: any) => path.resolve(e.path) === path.resolve(entry)));
    const { AuthStorage } = await import(pathToFileURL(senpi + "/dist/core/auth-storage.js").href);
    const { ModelRegistry } = await import(pathToFileURL(senpi + "/dist/core/model-registry.js").href);
    const auth = AuthStorage.inMemory(), registry = ModelRegistry.inMemory(auth);
    session = (await sdk.createAgentSession({ cwd, agentDir, resourceLoader: loader, authStorage: auth,
      modelRegistry: registry, sessionManager: sdk.SessionManager.inMemory(cwd) })).session;
    await session.bindExtensions({ mode: "print" });
    const state: any = Reflect.get(global, key);
    assert(state.capture.status.ok);
    assert.equal(state.capture.forRuntime(session).ok, true);
    assert.deepEqual(state.tools.map((tool: any) => tool.name).sort(),
      ["coordination_ask", "coordination_inspect", "coordination_notify"]);
    await session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
    session.dispose();
    assert.equal(sdk.AgentSession.prototype.bindExtensions, before);
    assert.deepEqual(helper.getCoordinationBroker(), { ok: false, reason: "coordination_unavailable" });
    fs.writeFileSync(artifact, JSON.stringify({ passed: true, pure_helper: true,
      single_esm_entry: true, entry_sha256: sha256(entry), loader_paths: loaded.extensions.map((e: any) => e.path),
      disposed: session._disposed, provider_calls: 0 }, null, 2) + "\n");
  } finally {
    await Reflect.get(global, key)?.dispose();
    if (session && !session._disposed) session.dispose();
    fs.unlinkSync(link);
  }
}
export const cases = { "install-roundtrip": () => runInstallCase("install-roundtrip") };
if (process.argv.includes("--native")) {
  const index = process.argv.indexOf("--native");
  await nativeLoader(process.argv[index + 1]!, process.argv[index + 2]!);
}

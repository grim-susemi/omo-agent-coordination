import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";
import { attempt } from "./command.mjs";

export const realModelProfile = "C:/Users/USER/.omo-stable-500/home/.omo/agent";
const hash = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
const inside = (parent: string, child: string) => {
  const relative = path.relative(parent, child);
  return relative === "" || (!relative.startsWith(".." + path.sep) && relative !== ".." && !path.isAbsolute(relative));
};

export function disposableModelConfig() {
  const source = path.join(realModelProfile, "models.json");
  const bytes = fs.readFileSync(source);
  const liveBefore = { path: source, sha256: hash(bytes),
    mtime_ns: String(fs.statSync(source, { bigint: true }).mtimeNs) };
  // isolatedEnv deliberately redirects TMP/HOME into evidence. Use the OS
  // account's real temporary parent, not those redirected environment variables.
  const parent = process.platform === "win32"
    ? path.join(os.userInfo().homedir, "AppData", "Local", "Temp") : "/tmp";
  const root = fs.realpathSync(fs.mkdtempSync(path.join(parent, "coordination-models-")));
  assert(!inside(fs.realpathSync(attempt), root), "MODEL_CONFIG_INSIDE_EVIDENCE");
  const modelsPath = path.join(root, "models.json");
  try { fs.writeFileSync(modelsPath, bytes, { flag: "wx", mode: 0o600 }); }
  catch (error) { fs.rmSync(root, { recursive: true, force: true }); throw error; }
  let loaderModelsPath: string | undefined;
  return {
    modelsPath,
    observe(nativeModelsPath: string) {
      loaderModelsPath = fs.realpathSync(nativeModelsPath);
      assert(inside(root, loaderModelsPath), "NATIVE_MODELS_PATH_OUTSIDE_DISPOSABLE_ROOT");
      assert.equal(loaderModelsPath, fs.realpathSync(modelsPath));
    },
    dispose() {
      let liveAfter;
      const disposableSha256 = hash(fs.readFileSync(modelsPath));
      try {
        liveAfter = { path: source, sha256: hash(fs.readFileSync(source)),
          mtime_ns: String(fs.statSync(source, { bigint: true }).mtimeNs) };
      } finally { fs.rmSync(root, { recursive: true, force: true }); }
      assert.deepEqual(liveAfter, liveBefore, "LIVE_MODEL_CONFIG_CHANGED");
      return { live_before: liveBefore, live_after: liveAfter, disposable_root: root,
        loader_models_path: loaderModelsPath, copy_sha256: liveBefore.sha256,
        loaded_copy_sha256: disposableSha256, version_cache_path: path.join(root, "claude-code-version.json"),
        disposable_removed: !fs.existsSync(root) };
    },
  };
}

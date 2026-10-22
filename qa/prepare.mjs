import fs from "node:fs";
import path from "node:path";
import { attempt, bundle, senpi, installedBundle, sha256 } from "./command.mjs";

// Previously verified 5.1.40 stock and 19-2 SDK, not freshly blessed live bytes.
const stockHash = "d41bc807b6f91768db84b7f8f76092107aebf9934888cc749174d88a1c00ac6b";
const archive = "C:/Users/USER/orca/projects/기타 작업/.omo/evidence/omo-maintenance-20261021/m5/coordination/omo-task.5140-adapter.js.baseline";
const files = [
  { path: senpi + "/dist/core/agent-session.js", sha256: "694c3b4fb71de08097bfd887f8950afc95dc8ad599042d073096faba02bb07a8" },
  { path: senpi + "/dist/core/agent-session.d.ts", sha256: "c2422d9e21339bfd2ce18b7dc9a9ecc2da6c4374452a01673646259d42785f9e" },
];
for (const file of files) {
  if (sha256(file.path) !== file.sha256) throw new Error("VERIFIED_5140_SDK_DRIFT");
}
if (!fs.existsSync(bundle)) {
  if (sha256(archive) !== stockHash) throw new Error("ARCHIVED_STOCK_DRIFT");
  fs.mkdirSync(path.dirname(bundle), { recursive: true });
  fs.copyFileSync(archive, bundle, fs.constants.COPYFILE_EXCL);
}
if (sha256(bundle) !== stockHash) throw new Error("EXISTING_STOCK_DRIFT");
// Stock/no-mode cases load the unmodified bundle itself, not an adapted copy.
// Resolve its external dependencies to the same installed modules as candidates.
const modules = path.join(path.dirname(bundle), "node_modules");
const installedModules = path.resolve(path.dirname(installedBundle), "../../..");
if (!fs.existsSync(modules)) fs.symlinkSync(installedModules, modules, "junction");
if (fs.realpathSync(modules) !== fs.realpathSync(installedModules)) throw new Error("BASELINE_MODULES_DRIFT");
const pins = path.join(attempt, "task-1/source-pins-5140.json");
if (!fs.existsSync(pins)) {
  fs.mkdirSync(path.dirname(pins), { recursive: true });
  fs.writeFileSync(pins, JSON.stringify({ files }, null, 2) + "\n", { flag: "wx" });
}
const recorded = JSON.parse(fs.readFileSync(pins, "utf8"));
if (JSON.stringify(recorded.files) !== JSON.stringify(files)) throw new Error("EXISTING_SDK_PINS_DRIFT");
console.log(JSON.stringify({ bundle, stock_sha256: sha256(bundle), pins }));

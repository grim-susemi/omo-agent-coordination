import fs from "node:fs";
import path from "node:path";
import { evidence, senpi, omo, bundle, sha256 } from "./command.mjs";

const profile = "C:/Users/USER/.omo-stable-500/home/.omo/agent";
const paths = [
  senpi + "/package.json", senpi + "/dist/core/agent-session.js",
  senpi + "/dist/core/agent-session.d.ts", senpi + "/dist/bundle/cli.js",
  senpi + "/dist/index.d.ts", senpi + "/dist/core/extensions/runner.js",
  senpi + "/dist/core/extensions/types.d.ts", senpi + "/dist/core/extensions/eval-handle-host.d.ts",
  senpi + "/dist/core/extensions/session-control-types.d.ts",
  senpi + "/dist/core/model-runtime.js", senpi + "/dist/core/model-runtime.d.ts",
  senpi + "/dist/core/extensions/builtin/compaction/speculative-summary.js",
  omo + "/package.json", omo + "/plugin/package.json",
  omo + "/bin/omo.js", omo + "/bin/lib/launcher.js",
  omo + "/plugin/extensions/omo.js", bundle, omo + "/plugin/daemon-launch-spec.json",
  profile + "/settings.json", profile + "/extensions/omo-compaction-model.ts",
  profile + "/extensions/omo-task-record-publish-retry.js",
  profile + "/task-record-publish-retry/extension.mjs",
  profile + "/task-no-fallback-pkg/package.json",
  profile + "/task-no-fallback-pkg/omo-task-no-fallback.js",
  omo + "/plugin/LICENSE", omo + "/plugin/NOTICE",
];
const pkg = JSON.parse(fs.readFileSync(senpi + "/package.json", "utf8"));
const result = {
  observed_at: new Date().toISOString(),
  product_absent_before_start: true,
  node: { version: process.versions.bun ? null : process.version, executable: process.versions.bun ? null : process.execPath },
  bun: { version: process.versions.bun ?? "1.4.0", executable: "C:/Users/USER/.bun/bin/bun.exe",
    evidence: "Native export probe records Bun 1.4.0 (34cbb9a40); pins script runs on Node for its distinct Node version." },
  senpi: { root: senpi, version: pkg.version, gitHead: pkg.gitHead ?? null },
  omo_version: JSON.parse(fs.readFileSync(omo + "/package.json", "utf8")).version,
  references: {
    commit: "d7e224e478a4a77dba0d2104c336fdc72ad301e3",
    R6: { path: "packages/omo-senpi/src/components/task/index.ts",
      sha256: "65696bec4145e00e9d7cca5f78d8b80379c3504ffbb37699e9aae662c31e474a" },
    R7: { path: "packages/senpi-task/src/manager/manager-outcome.ts",
      sha256: "47d31b4f76698412aa6db10bb6b166cc70ecde8dd91d1ff699bcaec77756c8c8" },
    relation: "Pinned source explains the reached native stack; no whole-bundle source equivalence is claimed.",
  },
  task_bundle: {
    path: bundle, realpath: fs.realpathSync(bundle), sha256: sha256(bundle),
    resolution: "omo bin/lib/launcher.js:144 --extension <global omo-ai>/plugin -> plugin/package.json pi.extensions -> extensions/omo.js -> #omo-task-runtime -> extensions/omo-task.js",
    settings_note: "settings.json loads task-no-fallback-pkg; native omo plugin is launcher-injected, not a settings packages entry",
  },
  files: paths.map((file) => ({ path: file, realpath: fs.realpathSync(file), sha256: sha256(file) })),
  license: { senpi: "MIT", omo: "plugin/LICENSE and plugin/NOTICE retained at installed source; no engine source vendored" },
  instrumentation_scope: "Only sandbox task-record write/rename paths; no live filesystem instrumentation",
};
fs.mkdirSync(evidence, { recursive: true });
fs.writeFileSync(path.join(evidence, "source-pins.json"), JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result));

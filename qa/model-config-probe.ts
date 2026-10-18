import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { attempt, senpi } from "./command.mjs";

// Observe the real native loader boundary, refusing the old live path before
// ModelConfig.load can migrate it. No provider request is made by this probe.
const { ModelRuntime } = await import(pathToFileURL(senpi + "/dist/core/model-runtime.js").href);
const create = ModelRuntime.create;
ModelRuntime.create = async function (options: any) {
  const modelsPath = fs.realpathSync(options.modelsPath);
  const disposableRoot = fs.realpathSync(path.dirname(modelsPath));
  assert(path.basename(disposableRoot).startsWith("coordination-models-"), "MODEL_CONFIG_NOT_DISPOSABLE");
  const relative = path.relative(attempt, disposableRoot);
  assert(relative === ".." || relative.startsWith(".." + path.sep) || path.isAbsolute(relative),
    "MODEL_CONFIG_INSIDE_EVIDENCE");
  assert.equal(modelsPath, path.join(disposableRoot, "models.json"));
  const runtime = await create.call(this, options);
  assert.equal(fs.realpathSync(runtime.modelsPath), modelsPath);
  return runtime;
};

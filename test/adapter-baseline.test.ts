import { expect, test } from "bun:test";
import fs from "node:fs";
import path from "node:path";
import { attempt, sha256 } from "../qa/command.mjs";
import { BASELINE, BASELINE_5140, BASELINES, buildAdapter, removeAdapter } from "../scripts/build-adapter.mjs";

// The 5.1.40 stock bytes live beside the 5.1.38 fixture in the disposable attempt
// evidence. Both are refused unless their exact digest is in the baseline table.
const stock = path.join(attempt, "baseline-5140/omo-task.js");

function sandbox() {
  const directory = fs.mkdtempSync(path.join(attempt, "test-adapter-baseline-"));
  return { directory, output: path.join(directory, "omo-task.js") };
}

test("the 5.1.40 baseline builds with twelve unique anchors and removes back to its exact bytes", () => {
  expect(fs.existsSync(stock)).toBe(true);
  const { directory, output } = sandbox();
  try {
    const manifest = buildAdapter({ input: stock, output });
    expect(manifest.baseline_sha256).toBe(BASELINE_5140);
    expect(manifest.anchors).toHaveLength(12);
    expect(manifest.anchors.every((anchor: { count: number }) => anchor.count === 1)).toBe(true);
    const text = fs.readFileSync(output, "utf8");
    for (const anchor of manifest.anchors) {
      // Every re-derived site exists once, and the injected delta replaced it exactly once.
      expect(text.split(anchor.before).length - 1).toBe(0);
      expect(text.split(anchor.after).length - 1).toBe(1);
    }
    expect(sha256(output)).toBe(manifest.candidate_sha256);
    expect(removeAdapter(manifest).restored_sha256).toBe(BASELINE_5140);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
}, 60_000);

test("the table has both pinned baselines and unknown bundle bytes are refused before writing", () => {
  expect(Object.keys(BASELINES).sort()).toEqual([BASELINE, BASELINE_5140].sort());
  expect(BASELINES[BASELINE].deltas).toHaveLength(12);
  expect(BASELINES[BASELINE_5140].deltas).toHaveLength(12);
  const { directory, output } = sandbox();
  try {
    const unknown = path.join(directory, "unknown.js");
    fs.writeFileSync(unknown, fs.readFileSync(stock, "utf8") + "\n// isolated byte drift\n");
    expect(() => buildAdapter({ input: unknown, output })).toThrow("BUNDLE_DRIFT_REFUSED");
    expect(fs.existsSync(output)).toBe(false);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
}, 60_000);

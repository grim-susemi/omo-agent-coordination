import fs from "node:fs";
import path from "node:path";
import { attempt, product, priorPreload } from "../command.mjs";
import { command, hashes } from "../../.omo/evidence/agent-coordination/attempt-20261017-e1/task-6/command.mjs";

export const questionEvidence = path.join(attempt, "task-6");
export const questionHashes = hashes;
export async function questionCommand(argv: string[], timeoutMs = 180_000) {
  priorPreload("task-6/write-guard.cjs");
  const nativeCase = argv.indexOf("--native-case");
  const caseName = nativeCase >= 0 ? argv[nativeCase + 1]! : "";
  const real = ["max-reasoning-nonce", "deep-ask-long"].includes(caseName);
  // The live case writes its disposable models.json outside the evidence tree (like the
  // real cases) and makes two asks of up to 60 s each, so it needs the model-config guard
  // and the full timeout. It counts its own backend-api requests, so it stays out of the
  // `real` list and its Ollama target pins.
  const live = caseName === "codex-quick-live";
  const modelConfig = real || live || caseName === "question-model-inventory";
  if (real) priorPreload("task-6/owner-target-pin.cjs");
  return command(argv, { timeoutMs: real || live ? timeoutMs : Math.min(timeoutMs, 60_000),
    ...(modelConfig ? { env: { NODE_OPTIONS: [
      `--require="${priorPreload("task-10/attempt-2/write-guard.cjs")}"`,
      ...(real ? [`--require="${priorPreload("task-6/owner-target-pin-attempt-2.cjs")}"`] : []),
    ].join(" ") } } : {}) });
}
export function questionReceipt(name: string, data: unknown) {
  fs.mkdirSync(questionEvidence, { recursive: true });
  const destination = path.join(questionEvidence, `${name}.json`);
  fs.writeFileSync(destination, JSON.stringify(data, null, 2) + "\n");
  return destination;
}

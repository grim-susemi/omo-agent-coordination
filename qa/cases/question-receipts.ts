import fs from "node:fs";
import path from "node:path";
import { attempt, product } from "../command.mjs";
import { command, hashes } from "../../.omo/evidence/agent-coordination/attempt-20261017-e1/task-6/command.mjs";

export const questionEvidence = path.join(attempt, "task-6");
export const questionHashes = hashes;
export async function questionCommand(argv: string[], timeoutMs = 180_000) {
  const nativeCase = argv.indexOf("--native-case");
  const real = nativeCase >= 0 && ["max-reasoning-nonce", "deep-ask-long"].includes(argv[nativeCase + 1]!);
  const modelConfig = real || (nativeCase >= 0 && argv[nativeCase + 1] === "question-model-inventory");
  return command(argv, { timeoutMs: real ? timeoutMs : Math.min(timeoutMs, 60_000),
    ...(modelConfig ? { env: { NODE_OPTIONS: [
      `--require="${path.join(attempt, "task-10/attempt-2/write-guard.cjs").replaceAll("\\", "/")}"`,
      ...(real ? [`--require="${path.join(questionEvidence, "owner-target-pin-attempt-2.cjs").replaceAll("\\", "/")}"`] : []),
    ].join(" ") } } : {}) });
}
export function questionReceipt(name: string, data: unknown) {
  fs.mkdirSync(questionEvidence, { recursive: true });
  const destination = path.join(questionEvidence, `${name}.json`);
  fs.writeFileSync(destination, JSON.stringify(data, null, 2) + "\n");
  return destination;
}

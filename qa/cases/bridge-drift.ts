import fs from "node:fs";
import path from "node:path";
import { runBridgeCase } from "./native-bridge.ts";
import { taskEvidence } from "./receipts.mjs";

export const cases = {
  "bridge-drift": async () => {
    const result = await runBridgeCase("bridge-drift");
    fs.writeFileSync(path.join(taskEvidence, "drift.json"), JSON.stringify(result, null, 2) + "\n");
    return result;
  },
};

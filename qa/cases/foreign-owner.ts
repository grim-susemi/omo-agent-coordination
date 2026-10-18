import fs from "node:fs";
import path from "node:path";
import { runBridgeCase } from "./native-bridge.ts";
import { taskEvidence } from "./receipts.mjs";

export const cases = {
  "foreign-owner": async () => {
    const result = await runBridgeCase("foreign-owner");
    fs.writeFileSync(path.join(taskEvidence, "foreign-owner.json"), JSON.stringify(result, null, 2) + "\n");
    return result;
  },
};

import fs from "node:fs";
import path from "node:path";
import { runBridgeCase } from "./native-bridge.ts";
import { taskEvidence } from "./receipts.mjs";

export const cases = {
  "engine-bridge": async () => {
    const result = await runBridgeCase("engine-bridge");
    fs.writeFileSync(path.join(taskEvidence, "bridge.json"), JSON.stringify(result, null, 2) + "\n");
    return result;
  },
};

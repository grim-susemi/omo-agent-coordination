import { runLiveCase, finalRun, type FinalCase } from "../live.ts";
import { cases as quick } from "./max-reasoning-nonce.ts";
import { cases as deep } from "./deep-ask-long.ts";
import { cases as abort } from "./deep-abort.ts";
import { cases as policy } from "./ask-policy.ts";
import { cases as inspect } from "./inspect-api.ts";

// The final integration registry takes ownership of these five named reruns.
// Earlier case modules and their receipts remain byte-for-byte unchanged.
// qa:owned imports this alphabetically before enumerating those module maps.
if (finalRun) {
  for (const entries of [quick, deep, abort, policy, inspect]) {
    for (const name of Object.keys(entries)) {
      (entries as Record<string, () => Promise<unknown>>)[name] = async () => {
        const enabled = await runLiveCase(name as FinalCase);
        if (name === "inspect-api") return { enabled, disabled: await runLiveCase("inspect-api", true) };
        return enabled;
      };
    }
  }
}
export const cases = { acceptance: () => runLiveCase("acceptance") };

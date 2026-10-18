import path from "node:path";
import { fileURLToPath } from "node:url";
import { attempt } from "./command.mjs";

const flag = process.argv.indexOf("--case");
const name = flag < 0 ? undefined : process.argv[flag + 1];
const attemptFlag = process.argv.indexOf("--attempt");
if (attemptFlag >= 0 && path.resolve(process.argv[attemptFlag + 1] ?? "") !== path.resolve(attempt)) {
  throw new Error("QA_ATTEMPT_MISMATCH");
}
const directory = path.join(path.dirname(fileURLToPath(import.meta.url)), "cases");
const cases = new Map<string, () => Promise<unknown>>();
for (const file of [...new Bun.Glob("*.ts").scanSync(directory)].sort()) {
  const module = await import(path.join(directory, file));
  for (const [key, run] of Object.entries(module.cases ?? {})) {
    if (cases.has(key) || typeof run !== "function") throw new Error(`QA_CASE_INVALID: ${key}`);
    cases.set(key, run as () => Promise<unknown>);
  }
}
if (!name || !cases.has(name)) throw new Error(`QA_CASE_NOT_IMPLEMENTED: ${name ?? "(missing --case)"}`);
console.log(JSON.stringify(await cases.get(name)!()));

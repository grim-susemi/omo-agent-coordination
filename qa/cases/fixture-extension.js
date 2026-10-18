import { pathToFileURL } from "node:url";

export default async function bridgeFixture(pi) {
  const state = { registered: false, context: undefined };
  globalThis[Symbol.for("coordination.qa.bridge.fixture")] = state;
  const native = await import(pathToFileURL(process.env.COORDINATION_CANDIDATE_BUNDLE).href);
  await native.createTaskComponent({ resolveCwd: () => pi.cwd }).register(pi, {
    logger: { info() {}, debug() {}, warn(...args) { console.error(...args); }, error(...args) { console.error(...args); } },
    getCapturedTools: () => [],
  });
  state.registered = true;
  pi.on("session_start", (_event, ctx) => { state.context = ctx; });
}

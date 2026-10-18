import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

/** Scripted provider only; task engine, store, runner, session, and tool loader are native. */
export default async function fixture(pi: any) {
  const ai = "@earendil-works/pi-ai";
  const { AssistantMessageEventStream } = await import(ai);
  const bus = (globalThis as any)[Symbol.for("coordination.inspect.qa.bus")];
  const processChild = process.env.OMO_SENPI_TASK_RPC_CHILD === "1";
  pi.registerProvider("coordination-inspect", {
    api: "openai-completions", apiKey: "offline-not-a-credential", baseUrl: "http://offline.invalid",
    models: [{ id: "offline", name: "Inspect offline gate", reasoning: false, input: ["text"],
      contextWindow: 262144, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple: (_model: unknown, context: { messages: unknown[] }) => {
      if (bus) bus.calls++;
      const stream = new AssistantMessageEventStream();
      const label = [...JSON.stringify(context.messages).matchAll(/INSPECT_NATIVE_GATE:(INSPECT_CHILD_[A-Z]+)/g)]
        .at(-1)?.[1];
      const finish = () => {
        const message = { role: "assistant", api: "openai-completions", provider: "coordination-inspect",
          model: "offline", content: [{ type: "text", text: label ? `native-result-${label.slice(-1)}` : "owner completion" }],
          stopReason: "stop", timestamp: Date.now(),
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
        stream.push({ type: "start", partial: message });
        stream.push({ type: "done", reason: "stop", message });
        stream.end(message);
      };
      if (label && processChild) {
        const gates = process.env.COORDINATION_INSPECT_GATES!;
        const release = path.join(gates, "release-rpc");
        let released = false;
        const complete = () => {
          if (released || !fs.existsSync(release)) return;
          released = true;
          watcher.close();
          finish();
        };
        const watcher = fs.watch(gates, (_event, file) => {
          if (String(file) === "release-rpc") complete();
        });
        const state = (globalThis as any)[Symbol.for("omo.agent-coordination.capture.v1")];
        const capture = state.capture.list().find((item: any) => item.read().ok);
        assert(capture, "RPC_LOCAL_NATIVE_CAPTURE_MISSING");
        const snapshot = capture.read();
        assert(snapshot.ok);
        const ready = path.join(gates, "rpc-start.json");
        fs.writeFileSync(ready + ".tmp", JSON.stringify({ pid: process.pid,
          session_id: snapshot.value.session_id, branch: snapshot.value.branch }));
        fs.renameSync(ready + ".tmp", ready);
        complete();
      } else if (label) {
        bus.once(`release-${label}`, finish);
        bus.emit("provider-start", { label });
      } else queueMicrotask(finish);
      return stream;
    },
  });
  if (processChild) return;
  const native = await import(pathToFileURL(process.env.COORDINATION_CANDIDATE_BUNDLE!).href);
  await native.createTaskComponent({ resolveCwd: () => pi.cwd }).register(pi, {
    logger: { info() {}, debug() {}, warn(...args: unknown[]) { console.error(...args); },
      error(...args: unknown[]) { console.error(...args); } }, getCapturedTools: () => [],
  });
}

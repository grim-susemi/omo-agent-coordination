import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import type { CaptureExtensionState } from "../../src/extension.ts";

export default async function fixture(pi: any) {
  const sdkName = "@code-yeongyu/senpi";
  const aiName = "@earendil-works/pi-ai";
  const sdk = await import(sdkName);
  const { AssistantMessageEventStream } = await import(aiName);
  const global = globalThis as unknown as Record<symbol, any>;
  const bus = global[Symbol.for("coordination.capture.qa.bus")];
  const isProcessChild = process.env.OMO_SENPI_TASK_RPC_CHILD === "1";
  pi.registerProvider("coordination-capture", {
    api: "openai-completions", apiKey: "offline-not-a-credential", baseUrl: "http://offline.invalid",
    models: ["offline", "changed"].map((id) => ({
      id, name: `Capture ${id}`, reasoning: false, input: ["text"],
      contextWindow: 262144, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    })),
    streamSimple: (_model: unknown, context: { messages: unknown[] }) => {
      const stream = new AssistantMessageEventStream();
      const child = JSON.stringify(context.messages).includes("CAPTURE_FIXTURE_CHILD");
      const finish = () => {
        let text = child ? "capture-native-result" : "owner received completion";
        if (isProcessChild && child) {
          const state = global[Symbol.for("omo.agent-coordination.capture.v1")] as CaptureExtensionState;
          assert(state?.capture.status.ok, "PROCESS_LOCAL_CAPTURE_UNAVAILABLE");
          const capture = state.capture.list().find((item) => item.read().ok);
          assert(capture, "PROCESS_LOCAL_RUNTIME_MISSING");
          const actual = capture.runtime()!;
          const snapshot = capture.read();
          assert(snapshot.ok);
          assert.equal(path.resolve(sdk.getPackageDir()), path.resolve(process.env.SENPI_PACKAGE_DIR!),
            "CHILD_SDK_PACKAGE_OVERRIDE_NOT_APPLIED");
          assert(actual instanceof sdk.AgentSession, "PROCESS_CLASS_IDENTITY_MISMATCH");
          assert.equal(snapshot.value.session_id, actual.sessionId);
          assert.equal(snapshot.value.model_runtime, actual.modelRuntime);
          assert.deepEqual(snapshot.value.messages, actual.messages);
          assert.equal(snapshot.value.system_prompt, actual.systemPrompt);
          const entry = fileURLToPath(state.entry_url);
          text = JSON.stringify({
            pid: process.pid, session_id: actual.sessionId,
            session_file: Reflect.get(actual, "sessionFile"), branch: snapshot.value.branch,
            model: `${snapshot.value.model.provider}/${snapshot.value.model.id}`,
            revision: snapshot.value.revision, message_count: snapshot.value.messages.length,
            same_AgentSession: true, local_snapshot_matches_actual: true,
            entry, entry_sha256: createHash("sha256").update(fs.readFileSync(entry)).digest("hex"),
          });
        }
        const message = {
          role: "assistant", api: "openai-completions", provider: "coordination-capture", model: "offline",
          content: [{ type: "text", text }], stopReason: "stop", timestamp: Date.now(),
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
        };
        stream.push({ type: "start", partial: message });
        stream.push({ type: "done", reason: "stop", message });
        stream.end(message);
      };
      if (child && bus) {
        bus.once("release-child", finish);
        bus.emit("provider-start", { child: true });
      } else queueMicrotask(finish);
      return stream;
    },
  });
  if (!isProcessChild) {
    const native = await import(pathToFileURL(process.env.COORDINATION_CANDIDATE_BUNDLE!).href);
    await native.createTaskComponent({ resolveCwd: () => pi.cwd }).register(pi, {
      logger: { info() {}, debug() {}, warn(...args: unknown[]) { console.error(...args); },
        error(...args: unknown[]) { console.error(...args); } },
      getCapturedTools: () => [],
    });
  }
}

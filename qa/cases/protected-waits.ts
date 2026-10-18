import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
import { runAutoCase } from "./auto-terminal.ts";

/** Only the provider and controlled tool are scripted. The loader, task
 * admission, runner, store, outcome, notification and capacity are native. */
export default async function protectedFixture(pi: any) {
  const bus = (globalThis as any)[Symbol.for("coordination.auto.qa.bus")];
  const packageName = "@earendil-works/pi-ai";
  const { AssistantMessageEventStream } = await import(packageName);
  pi.registerProvider("coordination-auto", {
    api: "openai-completions", apiKey: "offline-not-a-credential", baseUrl: "http://offline.invalid",
    models: [{ id: "offline", name: "Automatic repair event gate", reasoning: false, input: ["text"],
      contextWindow: 262144, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple: (_model: unknown, context: { messages: any[] }) => {
      const stream = new AssistantMessageEventStream();
      const label = [...JSON.stringify(context.messages).matchAll(/AUTO_NATIVE_GATE:([a-z0-9-]+)/g)].at(-1)?.[1];
      if (label) bus.calls++;
      const alreadyHeld = context.messages.some((message) => message.role === "toolResult" && message.toolName === "read");
      const tool = label === "tool" && !alreadyHeld;
      const finish = () => {
        const content = tool ? [{ type: "toolCall", id: "native-auto-hold", name: "read", arguments: { path: "owned-gate" } }]
          : [{ type: "text", text: label ? `native-auto-result:${label}` : "owner result" }];
        const message = { role: "assistant", api: "openai-completions", provider: "coordination-auto",
          model: "offline", content, stopReason: tool ? "toolUse" : "stop", timestamp: Date.now(),
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
        stream.push({ type: "start", partial: message });
        stream.push({ type: "done", reason: tool ? "toolUse" : "stop", message }); stream.end(message);
      };
      if (label && !alreadyHeld) { bus.once(`release:${label}`, finish); bus.emit("provider-start", { label }); }
      else queueMicrotask(finish);
      return stream;
    },
  });
  const holdTool = { name: "read", label: "Native protected tool gate",
    description: "Wait for the owned QA event.", parameters: { type: "object",
      properties: { path: { type: "string" } }, required: ["path"] },
    async execute() {
      const released = new Promise<void>((resolve) => bus.once("release-tool", resolve));
      bus.emit("tool-start"); await released;
      return { content: [{ type: "text", text: "protected tool finished" }] };
    },
  };
  pi.registerTool(holdTool);
  const native = await import(pathToFileURL(process.env.COORDINATION_CANDIDATE_BUNDLE!).href);
  await native.createTaskComponent({ resolveCwd: () => pi.cwd }).register(pi, {
    logger: { info() {}, debug() {}, warn(...args: unknown[]) { console.error(...args); },
      error(...args: unknown[]) { console.error(...args); } },
    getCapturedTools: () => [holdTool],
  });
  pi.on("session_start", (_event: unknown, context: any) => {
    if (process.env.OMO_SENPI_TASK_RPC_CHILD === "1") assert.fail("UNEXPECTED_PROCESS_CHILD");
    bus.emit("fixture-session", context.sessionManager.getSessionId());
  });
}

export const cases = { "protected-waits": async () => runAutoCase("protected-waits") };

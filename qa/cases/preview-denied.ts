import { runLiveCase } from "../live.ts";

export const cases = {
  "preview-denied": () => runLiveCase("preview-denied"),
};

/** Script only model output; native input tracking, launch, role identity and
 * collaboration policy remain the real loader/engine implementations. */
export default async function reviewerFixture(pi: any) {
  const aiName = "@earendil-works/pi-ai";
  const ai = await import(aiName);
  const plan = process.env.COORDINATION_PREVIEW_PLAN!;
  const stream = (model: any, context: any) => {
    const output = new ai.AssistantMessageEventStream();
    const messages = JSON.stringify(context.messages);
    const review = messages.includes("Review the work plan at ");
    const label = review ? "planreviewer" : "preview-plan-read";
    const global = globalThis as any;
    const counts = global[Symbol.for("coordination.question.qa.counts")];
    counts.main++;
    counts.main_tools[label] ??= ai.getCurrentTools(context.messages).map((tool: any) => tool.name);
    const consumed = review ? messages.includes("main-latch-released:planreviewer")
      : context.messages.some((message: any) => message.role === "toolResult" && !message.isError);
    const content = consumed ? [{ type: "text", text: `native-main-result:${label}` }]
      : [{ type: "toolCall", id: `preview-artifact-${label}`, name: review ? "read" : "write",
        arguments: review ? { path: "planreviewer" } : { path: plan,
          content: "# Disposable collaboration policy QA\n\nReview this isolated fixture only.\n" } }];
    const message: any = { role: "assistant", api: model.api, provider: model.provider, model: model.id,
      content, stopReason: consumed ? "stop" : "toolUse", timestamp: Date.now(),
      usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    queueMicrotask(() => {
      output.push({ type: "start", partial: message });
      output.push({ type: "done", reason: message.stopReason, message });
      output.end(message);
    });
    return output;
  };
  pi.registerProvider("coordination-preview-review", {
    api: "openai-completions", apiKey: "offline-test-no-credential", baseUrl: "http://offline.invalid",
    models: [{ id: "offline", name: "Disposable reviewer controller", reasoning: false, input: ["text"],
      contextWindow: 262144, maxTokens: 32768, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    stream, streamSimple: stream,
  });
}

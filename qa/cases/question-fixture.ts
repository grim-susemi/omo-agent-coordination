import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { getCoordinationClient } from "../../src/public-api.ts";

export default async function questionFixture(pi: any) {
  const aiName = "@earendil-works/pi-ai";
  const ai = await import(aiName);
  const global = globalThis as any;
  const processChild = process.env.OMO_SENPI_TASK_RPC_CHILD === "1";
  const bus = global[Symbol.for("coordination.question.qa.bus")] ?? new EventEmitter();
  let closeGate = () => {};
  if (!global[Symbol.for("coordination.question.qa.counts")]) global[Symbol.for("coordination.question.qa.counts")] =
    { main: 0, side: 0, payloads: [], main_tools: {} };
  let ownerContext: any;
  pi.on("session_start", (_event: unknown, context: unknown) => { ownerContext = context; });
  pi.on("session_shutdown", () => { closeGate(); });
  const holdTool = {
    name: "read", label: "Owned event latch", description: "Disposable QA override of read; await the named controlled event.",
    parameters: { type: "object", properties: { path: { type: "string" } }, required: ["path"] },
    async execute(_id: string, input: { path: string }) {
      if (processChild) {
        const directory = process.env.COORDINATION_QUESTION_PROCESS_GATE!;
        const released = new Promise<void>((resolve) => {
          const watcher = fs.watch(directory, (_event, filename) => {
            if (String(filename) === "release-main") { watcher.close(); resolve(); }
          });
          closeGate = () => { watcher.close(); };
        });
        const state = global[Symbol.for("omo.agent-coordination.capture.v1")];
        const capture = state.capture.list().find((row: any) =>
          row.runtime()?.sessionManager === ownerContext.sessionManager);
        assert(capture, "PROCESS_ACTUAL_CAPTURE_MISSING");
        const got = getCoordinationClient(ownerContext);
        assert(got.ok, JSON.stringify(got));
        const client = got.value, peers = client.peers();
        assert(peers.ok);
        const root = peers.value.peers.find((peer) => peer.role === "root");
        assert(root, "PROCESS_REAL_ROOT_MISSING");
        const sibling = peers.value.peers.find((peer) => peer.role === "oracle");
        assert(sibling, "PROCESS_REAL_SIBLING_MISSING");
        const request = { target_ref: root.target_ref, request_id: "rpc-child-to-root", question: "NONCE:rpc-root-nonce" };
        const rootAnswer = (await capture.runtime().extensionRunner.getToolDefinition("coordination_ask")
          .execute("rpc-child-to-root", request, undefined, undefined, ownerContext)).details;
        assert(rootAnswer.ok && rootAnswer.value.reply === "rpc-root-nonce", JSON.stringify(rootAnswer));
        assert.deepEqual(await client.ask(request), rootAnswer);
        const siblingAnswer = await client.ask({ target_ref: sibling.target_ref,
          request_id: "rpc-child-to-sibling", question: "NONCE:rpc-sibling-nonce" });
        assert(siblingAnswer.ok && siblingAnswer.value.reply === "rpc-sibling-nonce", JSON.stringify(siblingAnswer));
        const rootInspection = await client.inspect(root.target_ref);
        assert(rootInspection.ok && rootInspection.value.live === true, JSON.stringify(rootInspection));
        assert.deepEqual(getCoordinationClient({ sessionManager: { getSessionId: () => capture.runtime().sessionId } }),
          { ok: false, reason: "scope_denied" });
        const outbound = { child_to_root: rootAnswer, child_to_sibling: siblingAnswer,
          child_to_root_tool_api_deduplicated: true, root_inspect: rootInspection,
          copied_id_context_refused: true };
        const loaded = capture.runtime().resourceLoader.getExtensions();
        fs.writeFileSync(path.join(directory, "child-proof.json"), JSON.stringify({
          pid: process.pid, argv: process.argv, session_id: capture.runtime().sessionId,
          branch: capture.read().value.branch, capture_entry_url: state.entry_url,
          loaded_extensions: loaded.extensions.map((extension: any) =>
            ({ path: extension.path, resolved_path: extension.resolvedPath })),
          extension_errors: loaded.errors, outbound, fixture_handshake_calls: 0,
        }, null, 2) + "\n");
        fs.writeFileSync(path.join(directory, "child-ready"), "registered tools and API succeeded\n");
        client.dispose();
        await released;
        return { content: [{ type: "text", text: `main-latch-released:${input.path}` }] };
      }
      const released = new Promise<void>((resolve) => bus.once(`release-main:${input.path}`, resolve));
      bus.emit("main-latch", { label: input.path });
      await released;
      return { content: [{ type: "text", text: `main-latch-released:${input.path}` }] };
    },
  };
  pi.registerTool(holdTool);
  const providerStream = (model: any, context: any, options: any) => {
    const stream = new ai.AssistantMessageEventStream();
    const side = String(options?.sessionId ?? "").includes(":peer:");
    const text = JSON.stringify(context.messages);
    const label = [...text.matchAll(/QUESTION_NATIVE_CHILD:([a-zA-Z0-9_-]+)/g)].at(-1)?.[1];
    const nonce = text.match(/NONCE:([a-zA-Z0-9_-]+)/)?.[1] ?? "peer-nonce";
    const control = text.match(/CONTROL:([a-zA-Z0-9_-]+)/)?.[1] ?? "normal";
    const payload = { model: model.id, messages: context.messages, max_tokens: options?.maxTokens ?? 32768,
      reasoning_effort: options?.reasoningEffort ?? options?.reasoning ?? "high", ...(options?.extraBody ?? {}) };
    const finish = (content: any[], stopReason = "stop") => {
      const message: any = { role: "assistant", api: "openai-completions", provider: "coordination-question",
        model: "offline", content, stopReason, timestamp: Date.now(),
        usage: { input: 7, output: 11, reasoning: 3, cacheRead: 2, cacheWrite: 0, totalTokens: 18,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      if (stopReason === "error") message.errorMessage = "controlled provider/quota error";
      stream.push({ type: "start", partial: message });
      stream.push(stopReason === "error" ? { type: "error", reason: "error", error: message }
        : { type: "done", reason: stopReason as any, message });
      stream.end(message);
    };
    void (async () => {
      const finalPayload = options?.onPayload ? await options.onPayload(payload, model) : payload;
      const counters = global[Symbol.for("coordination.question.qa.counts")];
      if (side) {
        counters.side++;
        if (processChild) fs.writeFileSync(path.join(process.env.COORDINATION_QUESTION_PROCESS_GATE!,
          "side-provider-count.json"), JSON.stringify({ side_provider_calls: counters.side }) + "\n");
        counters.payloads.push({ max_tokens: finalPayload.max_tokens,
          reasoning_effort: finalPayload.reasoning_effort, has_tools: !!finalPayload.tools,
          pending_peer_context: JSON.stringify(context.messages).includes("<pending-peer-context>"),
          unfinished_pair_repaired: JSON.stringify(context.messages).includes("still running in the main task"),
          controls: { maxTokens: options.maxTokens, reasoningEffort: options.reasoningEffort, maxRetries: options.maxRetries } });
        const complete = () => {
          const content = control === "empty" ? [] : control === "tool" ?
            [{ type: "toolCall", id: "forbidden-side-tool", name: "read", arguments: { path: "must-not-run" } }]
            : [{ type: "text", text: control === "long" ? nonce + "\n" + "Native deep peer information. ".repeat(3000) : nonce }];
          finish(content, control === "error" || control === "quota" ? "error" : control === "length" ? "length" : "stop");
        };
        if (control === "gate") {
          bus.once(`release-side:${nonce}`, complete);
          options.signal?.addEventListener("abort", () => bus.emit("side-aborted", { nonce }), { once: true });
        }
        bus.emit("side-start", { nonce, control });
        if (control !== "gate") complete();
        return;
      }
      counters.main++;
      if (label && !counters.main_tools[label]) counters.main_tools[label] =
        ai.getCurrentTools(context.messages).map((tool: any) => tool.name);
      if (label && !text.includes(`main-latch-released:${label}`) && !text.includes("not found")) {
        finish([{ type: "toolCall", id: `hold-${label}`, name: "read", arguments: { path: label } }], "toolUse");
      } else finish([{ type: "text", text: label ? `native-main-result:${label}` : "native-root-result" }]);
    })().catch((error) => {
      console.error("QUESTION_PROVIDER_FIXTURE_ERROR", String(error));
      finish([], "error");
    });
    return stream;
  };
  pi.registerProvider("coordination-question", {
    api: "openai-completions", apiKey: "offline-test-no-credential", baseUrl: "http://offline.invalid",
    models: ["offline", "offline-alt"].map((id) => ({ id, name: "Native event-gated question fixture", reasoning: true, input: ["text"],
      contextWindow: 262144, maxTokens: 32768,
      thinkingLevelMap: { off: "none", minimal: "minimal", low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" },
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })),
    streamSimple: providerStream, stream: providerStream,
  });
  if (!processChild) {
    const native = await import(pathToFileURL(process.env.COORDINATION_CANDIDATE_BUNDLE!).href);
    await native.createTaskComponent({ resolveCwd: () => pi.cwd }).register(pi, {
    logger: { info() {}, debug() {}, warn(...args: unknown[]) { console.error(...args); },
      error(...args: unknown[]) { console.error(...args); } },
    getCapturedTools: () => {
      const state = global[Symbol.for("omo.agent-coordination.capture.v1")];
      return [holdTool, ...(state?.tools ?? [])];
    },
    });
  }
}

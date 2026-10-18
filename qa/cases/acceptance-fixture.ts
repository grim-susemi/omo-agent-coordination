import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { EventEmitter } from "node:events";
import { pathToFileURL } from "node:url";
import { getCoordinationClient } from "../../src/public-api.ts";
import { createHash } from "node:crypto";

export default async function acceptanceFixture(pi: any) {
  const ai = await import("@earendil-works/pi-ai" as string);
  const global = globalThis as any;
  const remote = process.env.OMO_SENPI_TASK_RPC_CHILD === "1";
  const bus = global[Symbol.for("coordination.acceptance.bus")] ?? new EventEmitter();
  const counts = global[Symbol.for("coordination.acceptance.counts")] ??= {
    main: 0, side: 0, payloads: [], emitted: [], main_tools: {}, events: [],
  };
  let context: any, stopGate = () => {};
  pi.on("session_start", (_event: unknown, ctx: any) => { context = ctx; });
  pi.on("session_shutdown", () => stopGate());
  const held = {
    name: "read", label: "Acceptance event latch", description: "Await the owned QA release event.",
    parameters: { type: "object", properties: { path: { type: "string" } }, required: ["path"] },
    async execute(_id: string, args: { path: string }) {
      if (remote) {
        const dir = process.env.COORDINATION_ACCEPTANCE_GATE!;
        const released = new Promise<void>((resolve, reject) => {
          const watcher = fs.watch(dir, (_event, file) => {
            if (String(file) === "release") { watcher.close(); resolve(); }
          });
          watcher.on("error", reject);
          stopGate = () => { watcher.close(); resolve(); };
        });
        const state = global[Symbol.for("omo.agent-coordination.capture.v1")];
        const capture = state.capture.list().find((row: any) => row.runtime()?.sessionManager === context.sessionManager);
        assert(capture);
        const peers = getCoordinationClient(context); assert(peers.ok);
        const roster = peers.value.peers(); assert(roster.ok);
        const sibling = roster.value.peers.find((peer: any) => peer.role === "oracle");
        assert(sibling);
        const duplicate = await peers.value.ask({ target_ref: sibling.target_ref,
          request_id: "process-sibling-ask", question: "NONCE:process-sibling-nonce" });
        assert(duplicate.ok && duplicate.value.state === "replied");
        fs.writeFileSync(path.join(dir, "ready.json"), JSON.stringify({
          pid: process.pid, argv: process.argv, session_id: capture.runtime().sessionId,
          runtime_instance_id: capture.runtime_instance_id, branch: capture.read().value.branch, peers: roster.value,
          fixture_bootstrap_calls: 0,
          loaded: capture.runtime().resourceLoader.getExtensions().extensions.map((entry: any) => entry.resolvedPath),
          model_issued: counts.emitted, tool_events: counts.events, side: counts.side,
          sibling_duplicate_replied: true,
        }, null, 2) + "\n");
        peers.value.dispose();
        await released;
      } else {
        const release = new Promise<void>(resolve => bus.once(`release:${args.path}`, resolve));
        bus.emit("latch", { label: args.path }); await release;
      }
      return { content: [{ type: "text", text: `accepted-release:${args.path}` }] };
    },
  };
  pi.registerTool(held);
  pi.on("tool_execution_end", (event: any) => {
    counts.events.push({ tool: event.toolName, id: event.toolCallId, details: event.result?.details ?? event.details });
  });
  const streamProvider = (model: any, ctx: any, options: any) => {
    const stream = new ai.AssistantMessageEventStream();
    const side = String(options?.sessionId ?? "").includes(":peer:");
    const messages = JSON.stringify(ctx.messages);
    const users = ctx.messages.filter((message: any) => message.role === "user");
    const latest = JSON.stringify(users.at(-1) ?? {});
    const label = [...messages.matchAll(/ACCEPTANCE_HOLD:([\w-]+)/g)].at(-1)?.[1];
    const command = latest.match(/ACCEPTANCE_CALL:([A-Za-z0-9+/=]+)/)?.[1];
    const finish = (content: any[], reason = "stop") => {
      const message: any = { role: "assistant", api: "openai-completions", provider: model.provider, model: model.id,
        content, stopReason: reason, timestamp: Date.now(),
        usage: { input: 7, output: 11, reasoning: 3, cacheRead: 2, cacheWrite: 0, totalTokens: 18,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      if (reason === "error") message.errorMessage = "controlled provider quota error";
      stream.push({ type: "start", partial: message });
      stream.push(reason === "error" ? { type: "error", reason: "error", error: message } : { type: "done", reason, message });
      stream.end(message);
    };
    void (async () => {
      const payload = { model: model.id, messages: ctx.messages, max_tokens: options.maxTokens ?? 32768,
        reasoning_effort: options.reasoningEffort ?? options.reasoning ?? "high", ...(options.extraBody ?? {}) };
      const final = options.onPayload ? await options.onPayload(payload, model) : payload;
      if (side) {
        counts.side++;
        const question = [...ctx.messages].reverse().find((message: any) => message.role === "user");
        const text = JSON.stringify(question);
        const nonce = [...text.matchAll(/NONCE:([\w-]+)/g)].at(-1)?.[1] ?? "missing-nonce";
        const control = [...text.matchAll(/CONTROL:([\w-]+)/g)].at(-1)?.[1] ?? "normal";
        counts.payloads.push({ max_tokens: final.max_tokens, reasoning_effort: final.reasoning_effort,
          tools: final.tools?.length ?? 0, pending_peer_context: messages.includes("<pending-peer-context>") });
        const complete = () => finish(control === "empty" ? [] : [{ type: "text", text: nonce }],
          control === "quota" ? "error" : "stop");
        if (control === "gate") {
          bus.once(`side-release:${nonce}`, complete);
          options.signal.addEventListener("abort", () => bus.emit("side-aborted", { nonce }), { once: true });
        }
        bus.emit("side-start", { nonce, control });
        if (remote) fs.writeFileSync(path.join(process.env.COORDINATION_ACCEPTANCE_GATE!, "side.json"), JSON.stringify({ count: counts.side }));
        if (control !== "gate") complete();
        return;
      }
      counts.main++;
      const promptText = [ctx.systemPrompt, ...ctx.messages.map((message: any) => message.content)]
        .flatMap((content: any) => typeof content === "string" ? [content] : Array.isArray(content)
          ? content.flatMap((part: any) => typeof part === "string" ? [part] : typeof part?.text === "string" ? [part.text] : []) : [])
        .join("\n");
      const sections = [...promptText.matchAll(/<omo-agent-coordination-roster-v1>\n([\s\S]*?)\n<\/omo-agent-coordination-roster-v1>/g)];
      (counts.roster_observations ??= []).push({ sections: sections.length,
        revision: sections[0] ? JSON.parse(sections[0][1]!).revision : null,
        hash: sections[0] ? createHash("sha256").update(sections[0][1]!).digest("hex") : null });
      if (label) counts.main_tools[label] = ai.getCurrentTools(ctx.messages).map((tool: any) => tool.name);
      if (command) {
        const spec = JSON.parse(Buffer.from(command, "base64").toString("utf8"));
        const done = ctx.messages.some((message: any) => message.role === "toolResult" && message.toolCallId === spec.id);
        if (!done) {
          const rootRef = global[Symbol.for("coordination.acceptance.root_ref")];
          const expected = global[Symbol.for("coordination.acceptance.peer_refs")] ?? [rootRef];
          if (rootRef) (counts.roster_context ??= {})[spec.id] = expected.every((ref: string) =>
            (messages + String(ctx.systemPrompt ?? "")).includes(ref));
          assert(ai.getCurrentTools(ctx.messages).some((tool: any) => tool.name === spec.name), `MODEL_TOOL_NOT_VISIBLE:${spec.name}`);
          counts.emitted.push({ ...spec, caller_session: options.sessionId, at: new Date().toISOString() });
          finish([{ type: "toolCall", id: spec.id, name: spec.name, arguments: spec.args }], "toolUse");
          return;
        }
      }
      if (label && !messages.includes(`accepted-release:${label}`)) {
        if (remote && !ctx.messages.some((message: any) => message.role === "toolResult" && message.toolCallId === "process-sibling-note")) {
          const id = "process-model-inspect";
          const inspected = ctx.messages.some((message: any) => message.role === "toolResult" && message.toolCallId === id);
          if (!inspected) {
            counts.emitted.push({ id, name: "coordination_inspect", args: {} });
            finish([{ type: "toolCall", id, name: "coordination_inspect", arguments: {} }], "toolUse"); return;
          }
          const client = getCoordinationClient(context); assert(client.ok, `PROCESS_COORD_CLIENT:${JSON.stringify(client)}`);
          const peers = client.value.peers(); assert(peers.ok, `PROCESS_COORD_PEERS:${JSON.stringify(peers)}`);
          const root = peers.value.peers.find((peer: any) => peer.role === "root");
          const sibling = peers.value.peers.find((peer: any) => peer.role === "oracle");
          assert(root, `PROCESS_ROOT_PEER_MISSING:${JSON.stringify(peers.value)}`);
          assert(sibling);
          const asked = ctx.messages.some((message: any) => message.role === "toolResult" && message.toolCallId === "process-model-ask");
          const noted = ctx.messages.some((message: any) => message.role === "toolResult" && message.toolCallId === "process-model-note");
          const siblingAsked = ctx.messages.some((message: any) => message.role === "toolResult" && message.toolCallId === "process-sibling-ask");
          const name = !asked || noted && !siblingAsked ? "coordination_ask" : "coordination_notify";
          const callId = !asked ? "process-model-ask" : !noted ? "process-model-note" :
            !siblingAsked ? "process-sibling-ask" : "process-sibling-note";
          const args = !asked ? { target_ref: root.target_ref, question: "NONCE:process-root-nonce", request_id: callId }
            : !noted ? { target_ref: root.target_ref, message: "process-model-note", request_id: callId }
            : !siblingAsked ? { target_ref: sibling.target_ref, question: "NONCE:process-sibling-nonce", request_id: callId }
            : { target_ref: sibling.target_ref, message: "process-sibling-note", request_id: callId };
          client.value.dispose();
          counts.emitted.push({ id: callId, name, args });
          finish([{ type: "toolCall", id: callId, name, arguments: args }], "toolUse"); return;
        }
        finish([{ type: "toolCall", id: `hold-${label}`, name: "read", arguments: { path: label } }], "toolUse");
      } else finish([{ type: "text", text: label ? `acceptance-main-result:${label}` : "acceptance-root-result" }]);
    })().catch(error => {
      console.error("ACCEPTANCE_PROVIDER_ERROR", String(error));
      if (remote) fs.writeFileSync(path.join(process.env.COORDINATION_ACCEPTANCE_GATE ?? process.env.OMO_CODING_AGENT_DIR!,
        "provider-error.json"), JSON.stringify({ message: String(error), stack: error.stack,
        argv: process.argv, pid: process.pid, ppid: process.ppid, executable: process.execPath,
        capture_status: global[Symbol.for("omo.agent-coordination.capture.v1")]?.capture.status,
        captures: global[Symbol.for("omo.agent-coordination.capture.v1")]?.capture.list().length,
        unsupported: [...(global[Symbol.for("omo.agent-coordination.capture.v1")]?.unsupported ?? [])],
        tool_events: counts.events,
        gate_configured: !!process.env.COORDINATION_ACCEPTANCE_GATE, label,
        main_tools: ai.getCurrentTools(ctx.messages).map((tool: any) => tool.name) }, null, 2) + "\n");
      finish([], "error");
    });
    return stream;
  };
  pi.registerProvider("coordination-acceptance", {
    api: "openai-completions", apiKey: "offline-fixture", baseUrl: "http://offline.invalid",
    models: ["offline", "offline-alt"].map(id => ({ id, name: "Acceptance scripted tool issuer", reasoning: true,
      input: ["text"], contextWindow: 262144, maxTokens: 32768,
      thinkingLevelMap: { off: "none", minimal: "minimal", low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" },
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })),
    streamSimple: streamProvider, stream: streamProvider,
  });
  if (!remote) {
    const native = await import(pathToFileURL(process.env.COORDINATION_CANDIDATE_BUNDLE!).href);
    await native.createTaskComponent({ resolveCwd: () => pi.cwd }).register(pi, {
      logger: { info() {}, debug() {}, warn(...args: any[]) { console.error(...args); }, error(...args: any[]) { console.error(...args); } },
      getCapturedTools: () => [held, ...(global[Symbol.for("omo.agent-coordination.capture.v1")]?.tools ?? [])],
    });
  }
}

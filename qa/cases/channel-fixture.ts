import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { connectRemoteRuntime } from "../../src/peer-channel.ts";

export default async function fixture(pi: any) {
  const aiName = "@earendil-works/pi-ai";
  const { AssistantMessageEventStream } = await import(aiName);
  const global = globalThis as any;
  const bus = global[Symbol.for("coordination.channel.qa.bus")];
  const processChild = process.env.OMO_SENPI_TASK_RPC_CHILD === "1";
  let context: any;
  let remote: any;
  let closeGate = () => {};
  pi.on("session_start", (_event: unknown, ctx: unknown) => { context = ctx; });
  pi.on("session_shutdown", () => { closeGate(); remote?.dispose(); });
  pi.registerTool({
    name: "channel_probe", label: "Owned channel probe", description: "Exercise the owned transport without a model side request.",
    parameters: { type: "object", properties: {
      target_ref: { type: "string" }, request_id: { type: "string" },
      operation: { type: "string" }, payload: {},
    }, required: ["target_ref", "request_id", "operation", "payload"] },
    async execute(_id: string, args: any, signal: AbortSignal | undefined, _update: unknown, ctx: unknown) {
      const channel = global[Symbol.for("coordination.channel.qa.owner")]?.channel;
      assert(channel, "REGISTERED_CHANNEL_OWNER_MISSING");
      const result = await channel.request(ctx, { ...args, signal });
      return { content: [{ type: "text", text: JSON.stringify(result) }], details: result };
    },
  });
  pi.registerProvider("coordination-channel", {
    api: "openai-completions", apiKey: "offline-not-a-credential", baseUrl: "http://offline.invalid",
    models: [{ id: "offline", name: "Channel offline event gate", reasoning: false, input: ["text"],
      contextWindow: 262144, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple: (_model: unknown, providerContext: { messages: unknown[] }) => {
      const counter = Symbol.for("coordination.channel.qa.provider-count");
      global[counter] = (global[counter] ?? 0) + 1;
      const stream = new AssistantMessageEventStream();
      const label = JSON.stringify(providerContext.messages).match(/CHANNEL_FIXTURE_CHILD:([A-Za-z0-9_-]+)/)?.[1];
      const finish = () => {
        closeGate();
        const message = {
          role: "assistant", api: "openai-completions", provider: "coordination-channel", model: "offline",
          content: [{ type: "text", text: label ? `channel-native-result:${label}` : "owner received completion" }],
          stopReason: "stop", timestamp: Date.now(),
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
        };
        stream.push({ type: "start", partial: message });
        stream.push({ type: "done", reason: "stop", message }); stream.end(message);
      };
      if (!label || label === "denied") { queueMicrotask(finish); return stream; }
      if (!processChild) {
        bus.once(`release-main:${label}`, finish);
        bus.emit("provider-start", { label });
        return stream;
      }
      void (async () => {
        const state = global[Symbol.for("omo.agent-coordination.capture.v1")];
        const capture = state.capture.list().find((item: any) => item.runtime()?.sessionManager === context.sessionManager);
        assert(capture, "REMOTE_ACTUAL_RUNTIME_MISSING");
        const runtime = capture.runtime();
        const gateDirectory = process.env.COORDINATION_CHANNEL_GATE!;
        const gateName = `release-${process.pid}`;
        // Subscribe before hello: the owner cannot release a not-yet-announced child.
        const watcher = fs.watch(gateDirectory, (_event, file) => {
          if (String(file) === gateName && fs.existsSync(path.join(gateDirectory, gateName))) finish();
        });
        closeGate = () => { watcher.close(); };
        let identityCalls = 0;
        const connected = await connectRemoteRuntime({
          invitation: JSON.parse(process.env.COORDINATION_CHANNEL_INVITATION!), capture, context,
          handlers: {
            identity(call) {
              identityCalls++;
              return { ok: true, value: {
                session_id: runtime.sessionId, runtime_instance_id: capture.runtime_instance_id,
                caller_ref: call.caller.target_ref, target_ref: call.target.target_ref,
                calls: identityCalls,
                body: call.payload === "large" ? "deep:\uD83D\uDE80\uD55C\uAE00\u00E9\n".repeat(18_000) : call.payload,
              } };
            },
            disconnect() { remote.dispose(); return { ok: true, value: "disconnected" }; },
          },
        });
        assert(connected.ok, JSON.stringify(connected));
        remote = connected.value;
        const root = await remote.request({ target_ref: remote.root_ref, request_id: "remote-root",
          operation: "identity", payload: "child-to-root" });
        assert(root.ok, JSON.stringify(root));
        const siblingRef = process.env.COORDINATION_CHANNEL_SIBLING;
        if (siblingRef) {
          const sibling = await remote.request({ target_ref: siblingRef, request_id: "remote-sibling",
            operation: "identity", payload: "child-to-sibling" });
          assert(sibling.ok, JSON.stringify(sibling));
        }
        const proof = await remote.request({ target_ref: remote.root_ref, request_id: "remote-proof",
          operation: "remoteProof", payload: {
            child_to_root: true, child_to_sibling: !!siblingRef, pid: process.pid,
            session_id: runtime.sessionId, branch: capture.read().value.branch,
          } });
        assert(proof.ok, JSON.stringify(proof));
      })().catch((error) => {
        console.error("REMOTE_CHANNEL_FIXTURE_FAILED", error);
        stream.end({
          role: "assistant", api: "openai-completions", provider: "coordination-channel", model: "offline",
          content: [], stopReason: "error", errorMessage: String(error), timestamp: Date.now(),
          usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
        });
        closeGate();
      });
      return stream;
    },
  });
  if (!processChild) {
    const native = await import(pathToFileURL(process.env.COORDINATION_CANDIDATE_BUNDLE!).href);
    await native.createTaskComponent({ resolveCwd: () => pi.cwd }).register(pi, {
      logger: { info() {}, debug() {}, warn(...args: unknown[]) { console.error(...args); },
        error(...args: unknown[]) { console.error(...args); } },
      getCapturedTools: () => [],
    });
  }
}

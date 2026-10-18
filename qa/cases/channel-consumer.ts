import assert from "node:assert/strict";

export default async function consumer(pi: any) {
  const global = globalThis as any;
  const state = global[Symbol.for("omo.agent-coordination.capture.v1")];
  const before = state?.capture.list().length ?? 0;
  const beforeProvider = global[Symbol.for("coordination.channel.qa.provider-count")] ?? 0;
  const sdkName = "@code-yeongyu/senpi";
  const sdk = await import(sdkName);
  const beforeHook = sdk.AgentSession.prototype.bindExtensions;
  const apiName = "coordination-channel-owned-candidate/api";
  const helper = await import(apiName);
  const after = state?.capture.list().length ?? 0;
  const afterProvider = global[Symbol.for("coordination.channel.qa.provider-count")] ?? 0;
  global[Symbol.for("coordination.channel.qa.consumer")] = {
    pure_import_capture_delta: after - before,
    pure_import_provider_delta: afterProvider - beforeProvider,
    pure_import_hook_unchanged: sdk.AgentSession.prototype.bindExtensions === beforeHook,
    built_api_imported: true,
  };
  pi.registerTool({
    name: "channel_api_probe", label: "Owned API consumer", description: "Consume only Coordination API v1.",
    parameters: { type: "object", properties: { target_ref: { type: "string" } }, required: ["target_ref"] },
    async execute(_id: string, input: any, _signal: unknown, _update: unknown, ctx: unknown) {
      const connected = helper.getCoordinationClient(ctx);
      assert(connected.ok, JSON.stringify(connected));
      const client = connected.value;
      const peers = client.peers();
      assert(peers.ok);
      const results = [
        await client.ask({ target_ref: input.target_ref, question: "unwired", request_id: "api-ask" }),
        await client.notify({ target_ref: input.target_ref, message: "unwired", request_id: "api-notify" }),
        await client.inspect(input.target_ref),
        client.setAskPolicy({ target_ref: input.target_ref, policy: "deny_peer_asks" }),
      ];
      assert(results.every((result) => !result.ok && result.reason === "coordination_unavailable"));
      const result = { ports: Object.keys(client).sort(), unwired_ports_refused: true,
        ...global[Symbol.for("coordination.channel.qa.consumer")] };
      client.dispose();
      return { content: [{ type: "text", text: JSON.stringify(result) }], details: result };
    },
  });
}
